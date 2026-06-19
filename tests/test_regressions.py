from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import call, patch
from zipfile import ZIP_DEFLATED, ZipFile
from pathlib import Path

os.environ.setdefault("AUTO_SYNC_ON_STARTUP", "0")
os.environ.setdefault("LIVE_SYNC_BACKGROUND_ENABLED", "0")

PROJECT_DIR = Path(__file__).resolve().parent.parent / "ecommerce-dashboard"
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from app.services.analytics import build_analytics
from app.services import bookkeeping_full, exports as exports_service, source_sync
from app.services.importers import kaufland_live
from app.services import live_sync
from app.services.bookkeeping_full import _calculate_amount_net_cents, _calculate_vat_amount_cents
from app.services.exports import _resolve_backup_manifest_path
from app.services.google_ads import _parse_ads_report_csv
from app.services.invoices import _build_draft_internal, _build_fallback_pdf
from app.services.order_shipping import build_shipment_capabilities
from app.services.orders import _kaufland_summary_from_row, _shopify_summary_from_row, _to_kaufland_cents
from app.services.importers.shopify_live import sync_shopify_live
from app.uploads import UploadTooLargeError, stream_fileobj_to_path


def build_sqlite_row(columns: list[str], values: list[object]) -> sqlite3.Row:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    column_sql = ", ".join(f'"{column}"' for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    connection.execute(f"CREATE TABLE sample ({column_sql})")
    connection.execute(f"INSERT INTO sample VALUES ({placeholders})", values)
    row = connection.execute("SELECT * FROM sample").fetchone()
    assert row is not None
    return row


def _create_stub_combined_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE combined_orders (id TEXT)")
    conn.commit()
    return conn


class GuardedChunkStream:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.offset = 0
        self.read_sizes: list[int] = []

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 0:
            self.offset = offset
        elif whence == 1:
            self.offset += offset
        elif whence == 2:
            self.offset = len(self.payload) + offset
        else:
            raise ValueError("unsupported whence")
        return self.offset

    def read(self, size: int = -1) -> bytes:
        if size <= 0:
            raise AssertionError("expected chunked reads")
        self.read_sizes.append(size)
        if self.offset >= len(self.payload):
            return b""
        chunk = self.payload[self.offset:self.offset + size]
        self.offset += len(chunk)
        return chunk


class RegressionTests(unittest.TestCase):
    def test_invoice_fallback_pdf_builder_outputs_valid_pdf_header(self) -> None:
        payload = _build_fallback_pdf([
            "Rechnung",
            "Bestellnummer: #1152",
            "Gesamtbetrag EUR 169,90",
        ])

        self.assertTrue(payload.startswith(b"%PDF-1.4"))
        self.assertIn(b"%%EOF", payload)

    def test_stream_fileobj_to_path_reads_in_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            payload = b"abcdefghij"
            stream = GuardedChunkStream(payload)
            target_path = Path(temp_dir) / "invoice.pdf"

            written = stream_fileobj_to_path(stream, target_path, max_bytes=32, chunk_size=4)
            stored = target_path.read_bytes()

        self.assertEqual(written, len(payload))
        self.assertEqual(stored, payload)
        self.assertGreaterEqual(len(stream.read_sizes), 3)
        self.assertTrue(all(size == 4 for size in stream.read_sizes))

    def test_stream_fileobj_to_path_cleans_up_temp_file_on_oversize(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            stream = GuardedChunkStream(b"abcdefghijk")
            target_path = Path(temp_dir) / "invoice.pdf"

            with self.assertRaises(UploadTooLargeError):
                stream_fileobj_to_path(stream, target_path, max_bytes=10, chunk_size=4)

            leftover_parts = list(Path(temp_dir).glob(".invoice.pdf*.upload"))
            target_exists = target_path.exists()

        self.assertFalse(target_exists)
        self.assertEqual(leftover_parts, [])

    def test_kaufland_cents_handle_decimal_eur_and_integral_real_values(self) -> None:
        self.assertEqual(_to_kaufland_cents(12.34), 1234)
        self.assertEqual(_to_kaufland_cents(10000.0), 10000)
        self.assertEqual(_to_kaufland_cents("12.34"), 1234)

    def test_shopify_partial_refund_scales_stored_estimates(self) -> None:
        row = build_sqlite_row(
            [
                "raw_json",
                "total_price",
                "financial_status",
                "refund_amount_sum",
                "fee_total",
                "estimated_paypal_fee",
                "payment_method",
                "net_total",
                "estimated_net_after_fee",
                "customer_first_name",
                "customer_last_name",
                "customer_email",
                "email",
                "first_article",
                "created_at",
                "currency",
                "fulfillment_status",
                "line_items_count",
                "id",
                "name",
            ],
            [
                '{"payment_gateway_names":["paypal"]}',
                "100.00",
                "partially_refunded",
                "25.00",
                None,
                "3.50",
                "PayPal",
                None,
                "96.50",
                "Max",
                "Mustermann",
                "max@example.com",
                "",
                "Artikel A",
                "2026-01-01T00:00:00Z",
                "EUR",
                "paid",
                1,
                "1",
                "#1001",
            ],
        )

        summary = _shopify_summary_from_row(row)

        self.assertEqual(summary["total_cents"], 7500)
        self.assertEqual(summary["fees_cents"], 262)
        self.assertEqual(summary["after_fees_cents"], 7238)

    def test_kaufland_refunds_ignore_cancelled_units(self) -> None:
        row = build_sqlite_row(
            [
                "raw_json",
                "units_price_sum",
                "revenue_gross_sum",
                "shipping_sum",
                "customer_name",
                "first_article",
                "ts_created_iso",
                "unit_status",
                "line_items_count",
                "id_order",
            ],
            [
                '{"currency":"EUR"}',
                10000,
                9000,
                0,
                "Kunde A",
                "Artikel A",
                "2026-01-01T00:00:00Z",
                "shipped",
                1,
                "ORDER-1",
            ],
        )

        summary = _kaufland_summary_from_row(row)

        self.assertEqual(summary["total_cents"], 10000)
        self.assertEqual(summary["after_fees_cents"], 9000)
        self.assertEqual(summary["fees_cents"], 1000)

    def test_kaufland_summary_converts_decimal_real_values_to_cents(self) -> None:
        row = build_sqlite_row(
            [
                "raw_json",
                "units_price_sum",
                "revenue_gross_sum",
                "shipping_sum",
                "customer_name",
                "first_article",
                "ts_created_iso",
                "unit_status",
                "line_items_count",
                "id_order",
            ],
            [
                '{"currency":"EUR"}',
                12.34,
                9.99,
                3.5,
                "Kunde B",
                "Artikel B",
                "2026-01-02T00:00:00Z",
                "shipped",
                1,
                "ORDER-2",
            ],
        )

        summary = _kaufland_summary_from_row(row)

        self.assertEqual(summary["total_cents"], 1234)
        self.assertEqual(summary["after_fees_cents"], 999)
        self.assertEqual(summary["fees_cents"], 235)
        self.assertEqual(summary["shipping_cents"], 350)

    def test_kaufland_invoice_draft_uses_integer_real_values_as_cents_and_adds_shipping_once(self) -> None:
        detail_payload = {
            "summary": {
                "marketplace": "kaufland",
                "order_id": "ORDER-1",
                "external_order_id": "ORDER-1",
                "order_date": "2026-05-31T10:00:00Z",
                "customer": "Alice Example",
                "total_cents": 10000,
                "shipping_cents": 350,
                "currency": "EUR",
                "fulfillment_status": "sent",
                "financial_status": "",
            },
            "customer": {"name": "Alice Example", "email": "alice@example.com"},
            "billing_address": {
                "name": "Alice Example",
                "street": "Musterweg 9",
                "postcode": "10115",
                "city": "Berlin",
                "country": "DE",
            },
            "shipping_address": {
                "name": "Alice Example",
                "street": "Musterweg 9",
                "postcode": "10115",
                "city": "Berlin",
                "country": "DE",
            },
            "units": [
                {
                    "id_order_unit": "unit-1",
                    "product_id_product": "prod-1",
                    "product_title": "Alpha Product",
                    "price": 10000.0,
                    "shipping_rate": 350.0,
                    "vat": None,
                    "status": "sent",
                }
            ],
        }

        seller_profile = {
            "legal_name": "Demo Shop",
            "street": "Demo Street 1",
            "address_line2": "",
            "postcode": "12345",
            "city": "Berlin",
            "country": "DE",
            "email": "demo@example.com",
            "phone": "",
            "vat_id": "DE123456789",
            "tax_number": "",
            "tax_mode": "small_business",
            "invoice_prefix": "RE",
            "default_template": "clean",
            "footer_note": "",
            "payment_note": "",
            "eu_invoicing_enabled": False,
        }

        combined = sqlite3.connect(":memory:")
        combined.row_factory = sqlite3.Row
        combined.execute("CREATE TABLE seller_profiles (id TEXT PRIMARY KEY, legal_name TEXT, street TEXT, address_line2 TEXT, postcode TEXT, city TEXT, country TEXT, email TEXT, phone TEXT, vat_id TEXT, tax_number TEXT, tax_mode TEXT, invoice_prefix TEXT, default_template TEXT, footer_note TEXT, payment_note TEXT, eu_invoicing_enabled INTEGER, created_at TEXT, updated_at TEXT)")
        combined.execute("CREATE TABLE sales_invoices (id TEXT PRIMARY KEY, marketplace TEXT, source_order_id TEXT, source_external_order_id TEXT, invoice_number TEXT, invoice_date TEXT, delivery_date TEXT, currency TEXT, customer_name TEXT, customer_country TEXT, tax_country TEXT, tax_treatment TEXT, template_key TEXT, total_gross_cents INTEGER, seller_snapshot_json TEXT, customer_snapshot_json TEXT, totals_snapshot_json TEXT, validation_snapshot_json TEXT, notes TEXT, pdf_path TEXT, created_at TEXT, updated_at TEXT)")

        try:
            with patch("app.services.invoices.get_order_detail", return_value=detail_payload), \
                 patch("app.services.invoices.get_seller_profile", return_value=seller_profile), \
                 patch("app.services.invoices.connect_combined_db", return_value=combined):
                draft = _build_draft_internal("kaufland", "ORDER-1", "clean")
        finally:
            combined.close()

        items = draft["items"]
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["title"], "Alpha Product")
        self.assertEqual(items[0]["line_total_gross_cents"], 10000)
        self.assertEqual(items[1]["title"], "Versand")
        self.assertEqual(items[1]["line_total_gross_cents"], 350)
        self.assertEqual(draft["totals"]["gross_cents"], 10350)
        self.assertEqual(draft["totals"]["shipping_cents"], 350)

    def test_kaufland_invoice_draft_aggregates_shipping_across_multiple_units_without_quantity_multiplication(self) -> None:
        detail_payload = {
            "summary": {
                "marketplace": "kaufland",
                "order_id": "ORDER-2",
                "external_order_id": "ORDER-2",
                "order_date": "2026-05-31T10:00:00Z",
                "customer": "Bob Example",
                "total_cents": 2500,
                "shipping_cents": 500,
                "currency": "EUR",
                "fulfillment_status": "sent",
                "financial_status": "",
            },
            "customer": {"name": "Bob Example", "email": "bob@example.com"},
            "billing_address": {
                "name": "Bob Example",
                "street": "Musterweg 10",
                "postcode": "10115",
                "city": "Berlin",
                "country": "DE",
            },
            "shipping_address": {
                "name": "Bob Example",
                "street": "Musterweg 10",
                "postcode": "10115",
                "city": "Berlin",
                "country": "DE",
            },
            "units": [
                {
                    "id_order_unit": "unit-1",
                    "product_id_product": "prod-1",
                    "product_title": "Alpha",
                    "price": 1000,
                    "shipping_rate": 200,
                    "vat": None,
                    "status": "sent",
                },
                {
                    "id_order_unit": "unit-2",
                    "product_id_product": "prod-1",
                    "product_title": "Alpha",
                    "price": 1500,
                    "shipping_rate": 300,
                    "vat": None,
                    "status": "sent",
                },
            ],
        }

        seller_profile = {
            "legal_name": "Demo Shop",
            "street": "Demo Street 1",
            "address_line2": "",
            "postcode": "12345",
            "city": "Berlin",
            "country": "DE",
            "email": "demo@example.com",
            "phone": "",
            "vat_id": "DE123456789",
            "tax_number": "",
            "tax_mode": "small_business",
            "invoice_prefix": "RE",
            "default_template": "clean",
            "footer_note": "",
            "payment_note": "",
            "eu_invoicing_enabled": False,
        }

        combined = sqlite3.connect(":memory:")
        combined.row_factory = sqlite3.Row
        combined.execute("CREATE TABLE seller_profiles (id TEXT PRIMARY KEY, legal_name TEXT, street TEXT, address_line2 TEXT, postcode TEXT, city TEXT, country TEXT, email TEXT, phone TEXT, vat_id TEXT, tax_number TEXT, tax_mode TEXT, invoice_prefix TEXT, default_template TEXT, footer_note TEXT, payment_note TEXT, eu_invoicing_enabled INTEGER, created_at TEXT, updated_at TEXT)")
        combined.execute("CREATE TABLE sales_invoices (id TEXT PRIMARY KEY, marketplace TEXT, source_order_id TEXT, source_external_order_id TEXT, invoice_number TEXT, invoice_date TEXT, delivery_date TEXT, currency TEXT, customer_name TEXT, customer_country TEXT, tax_country TEXT, tax_treatment TEXT, template_key TEXT, total_gross_cents INTEGER, seller_snapshot_json TEXT, customer_snapshot_json TEXT, totals_snapshot_json TEXT, validation_snapshot_json TEXT, notes TEXT, pdf_path TEXT, created_at TEXT, updated_at TEXT)")

        try:
            with patch("app.services.invoices.get_order_detail", return_value=detail_payload), \
                 patch("app.services.invoices.get_seller_profile", return_value=seller_profile), \
                 patch("app.services.invoices.connect_combined_db", return_value=combined):
                draft = _build_draft_internal("kaufland", "ORDER-2", "clean")
        finally:
            combined.close()

        items = draft["items"]
        self.assertEqual([item["line_total_gross_cents"] for item in items], [1000, 1500, 500])
        self.assertEqual(draft["totals"]["gross_cents"], 3000)
        self.assertEqual(draft["totals"]["shipping_cents"], 500)

    def test_init_combined_db_migrates_legacy_combined_orders_columns(self) -> None:
        from app import db as app_db

        with tempfile.TemporaryDirectory() as temp_dir:
            combined_path = Path(temp_dir) / "combined.sqlite3"
            original_combined_path = app_db.COMBINED_DB_PATH
            app_db.COMBINED_DB_PATH = combined_path
            try:
                connection = sqlite3.connect(combined_path)
                connection.execute(
                    """
                    CREATE TABLE combined_orders (
                        id TEXT PRIMARY KEY,
                        marketplace TEXT NOT NULL,
                        order_id TEXT NOT NULL,
                        external_order_id TEXT NOT NULL,
                        order_date TEXT,
                        customer TEXT,
                        article TEXT,
                        line_items_count INTEGER NOT NULL DEFAULT 1,
                        total_cents INTEGER NOT NULL DEFAULT 0,
                        fees_cents INTEGER NOT NULL DEFAULT 0,
                        after_fees_cents INTEGER NOT NULL DEFAULT 0,
                        shipping_cents INTEGER NOT NULL DEFAULT 0,
                        currency TEXT NOT NULL DEFAULT 'EUR',
                        fulfillment_status TEXT,
                        payment_method TEXT,
                        fee_source TEXT,
                        financial_status TEXT,
                        raw_status TEXT,
                        raw_json TEXT
                    )
                    """
                )
                connection.commit()
                connection.close()

                app_db.init_combined_db()

                connection = sqlite3.connect(combined_path)
                try:
                    columns = {row[1] for row in connection.execute("PRAGMA table_info(combined_orders)").fetchall()}
                finally:
                    connection.close()
            finally:
                app_db.COMBINED_DB_PATH = original_combined_path

        self.assertTrue({
            "purchase_cost_cents",
            "purchase_currency",
            "purchase_supplier",
            "purchase_notes",
            "profit_cents",
            "has_invoice",
            "invoice_document_id",
        }.issubset(columns))

    def test_sync_all_sources_reports_combined_orders_errors(self) -> None:
        with patch.object(source_sync, "_pick_shopify_bootstrap_source", return_value=Path("/missing-shopify.sqlite3")), \
             patch.object(source_sync, "KAUFLAND_BOOTSTRAP_DB_PATH", Path("/missing-kaufland.sqlite3")), \
             patch.object(source_sync, "populate_combined_orders", side_effect=RuntimeError("boom")):
            summary = source_sync.sync_all_sources(force=False, include_documents=False)

        self.assertEqual(summary["results"]["combined_orders"]["status"], "error")
        self.assertIn("RuntimeError: boom", summary["results"]["combined_orders"]["reason"])

    def test_create_monthly_invoice_rejects_overlapping_period_for_same_provider(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "dashboard.sqlite3"
            connection = sqlite3.connect(db_path)
            connection.row_factory = sqlite3.Row
            try:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute(
                    """
                    CREATE TABLE monthly_invoices (
                        id TEXT PRIMARY KEY,
                        provider TEXT NOT NULL,
                        period_from TEXT NOT NULL,
                        period_to TEXT NOT NULL,
                        invoice_amount_cents INTEGER NOT NULL,
                        currency TEXT NOT NULL DEFAULT 'EUR',
                        calculated_sum_cents INTEGER,
                        difference_cents INTEGER,
                        document_id TEXT,
                        notes TEXT,
                        status TEXT NOT NULL DEFAULT 'draft',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE monthly_invoice_transactions (
                        invoice_id TEXT NOT NULL,
                        transaction_id TEXT NOT NULL,
                        PRIMARY KEY (invoice_id, transaction_id)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE transactions (
                        id TEXT PRIMARY KEY,
                        booking_class TEXT,
                        provider TEXT,
                        type TEXT,
                        direction TEXT,
                        date TEXT,
                        amount_gross INTEGER,
                        source TEXT
                    )
                    """
                )
                connection.execute(
                    "INSERT INTO monthly_invoices (id, provider, period_from, period_to, invoice_amount_cents, currency, calculated_sum_cents, difference_cents, document_id, notes, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "existing",
                        "paypal",
                        "2026-01-01T00:00:00Z",
                        "2026-01-31T23:59:59Z",
                        1000,
                        "EUR",
                        1000,
                        0,
                        None,
                        None,
                        "matched",
                        "2026-01-01T00:00:00Z",
                        "2026-01-01T00:00:00Z",
                    ),
                )
                connection.commit()
            finally:
                connection.close()

            original_path = bookkeeping_full.BOOKKEEPING_DB_PATH
            bookkeeping_full.BOOKKEEPING_DB_PATH = db_path
            try:
                with self.assertRaises(bookkeeping_full.BookkeepingServiceError) as exc:
                    bookkeeping_full.create_monthly_invoice({
                        "provider": "paypal",
                        "period_from": "2026-01-15",
                        "period_to": "2026-01-20",
                        "invoice_amount_cents": 1000,
                        "currency": "EUR",
                    })
            finally:
                bookkeeping_full.BOOKKEEPING_DB_PATH = original_path

        self.assertEqual(exc.exception.status_code, 409)

    def test_full_backup_includes_sales_invoice_storage_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            combined = root / "combined.sqlite3"
            combined.write_bytes(b"sqlite")
            invoices_dir = root / "invoices"
            sales_invoices_dir = root / "sales_invoices"
            documents_dir = root / "documents"
            invoices_dir.mkdir()
            sales_invoices_dir.mkdir()
            documents_dir.mkdir()
            (sales_invoices_dir / "invoice.pdf").write_bytes(b"%PDF-1.4 sales")

            originals = (
                exports_service.COMBINED_DB_PATH,
                exports_service.SHOPIFY_DB_PATH,
                exports_service.KAUFLAND_DB_PATH,
                exports_service.BOOKKEEPING_DB_PATH,
                exports_service.EBAY_DB_PATH,
                exports_service.INVOICES_DIR,
                exports_service.SALES_INVOICES_DIR,
                exports_service.BOOKKEEPING_DOCUMENTS_DIR,
            )
            exports_service.COMBINED_DB_PATH = combined
            exports_service.SHOPIFY_DB_PATH = root / "shopify.sqlite3"
            exports_service.KAUFLAND_DB_PATH = root / "kaufland.sqlite3"
            exports_service.BOOKKEEPING_DB_PATH = root / "bookkeeping.sqlite3"
            exports_service.EBAY_DB_PATH = root / "ebay.sqlite3"
            exports_service.INVOICES_DIR = invoices_dir
            exports_service.SALES_INVOICES_DIR = sales_invoices_dir
            exports_service.BOOKKEEPING_DOCUMENTS_DIR = documents_dir
            try:
                archive = exports_service.create_full_backup_archive()
                try:
                    with ZipFile(archive.file_path, "r") as zf:
                        names = set(zf.namelist())
                finally:
                    exports_service.cleanup_temp_export(archive.file_path)
            finally:
                (
                    exports_service.COMBINED_DB_PATH,
                    exports_service.SHOPIFY_DB_PATH,
                    exports_service.KAUFLAND_DB_PATH,
                    exports_service.BOOKKEEPING_DB_PATH,
                    exports_service.EBAY_DB_PATH,
                    exports_service.INVOICES_DIR,
                    exports_service.SALES_INVOICES_DIR,
                    exports_service.BOOKKEEPING_DOCUMENTS_DIR,
                ) = originals

        self.assertIn("storage/sales_invoices/invoice.pdf", names)

    def test_manual_vat_values_survive_amount_gross_only_change_without_vat_rate(self) -> None:
        existing = {
            "amount_gross": 10000,
            "vat_rate": None,
            "vat_amount": 1900,
            "amount_net": 8100,
        }
        updates = {"amount_gross": 12000}

        effective_amount_gross = int(updates.get("amount_gross", existing["amount_gross"]))
        if "vat_rate" in updates:
            effective_vat_rate = updates["vat_rate"]
        elif existing["vat_rate"] is None:
            effective_vat_rate = None
        else:
            effective_vat_rate = float(existing["vat_rate"])

        should_recalculate_vat_amount = False
        if "vat_amount" not in updates:
            if "vat_rate" in updates:
                should_recalculate_vat_amount = True
            elif "amount_gross" in updates and existing["vat_rate"] is not None:
                should_recalculate_vat_amount = True

        if should_recalculate_vat_amount:
            updates["vat_amount"] = _calculate_vat_amount_cents(effective_amount_gross, effective_vat_rate)

        effective_vat_amount = updates.get("vat_amount", existing["vat_amount"])
        if "amount_net" not in updates and ("amount_gross" in updates or "vat_amount" in updates or "vat_rate" in updates):
            updates["amount_net"] = _calculate_amount_net_cents(
                effective_amount_gross,
                None if effective_vat_amount is None else int(effective_vat_amount),
            )

        self.assertNotIn("vat_amount", updates)
        self.assertEqual(effective_vat_amount, 1900)
        self.assertEqual(updates["amount_net"], 10100)

    def test_backup_manifest_resolution_accepts_single_nested_prefix(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".zip") as temp_file:
            with ZipFile(temp_file.name, mode="w", compression=ZIP_DEFLATED) as zf:
                zf.writestr("backup-2026-05-05/manifest.json", "{}")

            with ZipFile(temp_file.name, mode="r") as zf:
                manifest_path, archive_prefix = _resolve_backup_manifest_path(zf)

        self.assertEqual(manifest_path, "backup-2026-05-05/manifest.json")
        self.assertEqual(archive_prefix, "backup-2026-05-05/")

    def test_google_ads_report_rejects_non_eur_currency(self) -> None:
        content = (
            "Artikel-ID,Currency,2026-01-01_Cost\n"
            "sku-1,USD,12.34\n"
        ).encode("utf-8")

        with self.assertRaisesRegex(ValueError, "Nur EUR"):
            _parse_ads_report_csv(content)

    def test_analytics_previous_period_uses_same_refund_normalization(self) -> None:
        current_order = {
            "marketplace": "shopify",
            "order_date": "2026-01-10T10:00:00Z",
            "customer": "Alice",
            "article": "Artikel A",
            "total_cents": 10000,
            "fees_cents": 500,
            "after_fees_cents": 9500,
            "purchase_cost_cents": 4000,
            "profit_cents": 5500,
            "shipping_cents": 0,
            "fulfillment_status": "fulfilled",
            "financial_status": "paid",
            "raw_status": "fulfilled",
            "payment_method": "Shopify",
        }
        refunded_prev_order = {
            "marketplace": "shopify",
            "order_date": "2026-01-09T10:00:00Z",
            "customer": "Alice",
            "article": "Artikel A",
            "total_cents": 10000,
            "fees_cents": 500,
            "after_fees_cents": 9500,
            "purchase_cost_cents": 4000,
            "profit_cents": 5500,
            "shipping_cents": 0,
            "fulfillment_status": "fulfilled",
            "financial_status": "refunded",
            "raw_status": "refunded",
            "payment_method": "Shopify",
        }

        def fake_orders(*, from_date: str | None, to_date: str | None, marketplace: str | None, query: str | None, status_filter: str | None = None):
            del to_date, marketplace, query, status_filter
            if from_date == "2026-01-10":
                return [current_order]
            if from_date == "2026-01-09":
                return [refunded_prev_order]
            return []

        with patch("app.services.analytics.list_all_orders_without_pagination", side_effect=fake_orders):
            payload = build_analytics(
                from_date="2026-01-10",
                to_date="2026-01-10",
                marketplace=None,
                query=None,
            )

        self.assertEqual(payload["revenue_total_cents"], 10000)
        self.assertIsNotNone(payload["previous_period"])
        self.assertEqual(payload["previous_period"]["revenue_total_cents"], 0)
        self.assertEqual(payload["previous_period"]["profit_total_cents"], 0)

    def test_kaufland_order_detail_summary_matches_list_summary_when_cancelled_units_exist(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "kaufland.sqlite3"
            connection = sqlite3.connect(db_path)
            try:
                connection.execute("CREATE TABLE orders (id_order TEXT PRIMARY KEY, ts_created_iso TEXT, raw_json TEXT)")
                connection.execute(
                    """
                    CREATE TABLE order_units (
                        id_order_unit TEXT PRIMARY KEY,
                        id_order TEXT NOT NULL,
                        status TEXT,
                        price TEXT,
                        revenue_gross TEXT,
                        shipping_rate TEXT,
                        product_title TEXT,
                        ts_created_iso TEXT,
                        shipping_first_name TEXT,
                        shipping_last_name TEXT,
                        billing_first_name TEXT,
                        billing_last_name TEXT,
                        buyer_id_buyer TEXT,
                        raw_json TEXT
                    )
                    """
                )
                connection.execute(
                    "CREATE TABLE order_unit_refunds (id_order_unit TEXT NOT NULL, amount TEXT)"
                )
                connection.execute(
                    "INSERT INTO orders (id_order, ts_created_iso, raw_json) VALUES (?, ?, ?)",
                    ("ORDER-DET-1", "2026-01-03T00:00:00Z", '{"currency":"EUR"}'),
                )
                connection.executemany(
                    """
                    INSERT INTO order_units (
                        id_order_unit, id_order, status, price, revenue_gross, shipping_rate,
                        product_title, ts_created_iso, shipping_first_name, shipping_last_name,
                        billing_first_name, billing_last_name, buyer_id_buyer, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            "unit-1", "ORDER-DET-1", "shipped", "12.34", "9.99", "3.50",
                            "Artikel A", "2026-01-03T00:00:00Z", "Max", "Mustermann",
                            "Max", "Mustermann", "buyer-1", "{}",
                        ),
                        (
                            "unit-2", "ORDER-DET-1", "cancelled", "99.99", "88.88", "7.77",
                            "Artikel B", "2026-01-03T00:01:00Z", "Max", "Mustermann",
                            "Max", "Mustermann", "buyer-1", "{}",
                        ),
                    ],
                )
                connection.commit()
            finally:
                connection.close()

            from app.services import orders as orders_service

            _stub_combined = _create_stub_combined_db()
            with patch.object(orders_service, "KAUFLAND_DB_PATH", db_path), \
                 patch("app.services.orders.fetch_enrichment_map", return_value={}), \
                 patch("app.services.orders.connect_combined_db", return_value=_stub_combined):
                list_payload = orders_service.list_orders(
                    from_date=None,
                    to_date=None,
                    marketplace="kaufland",
                    query=None,
                    limit=10,
                    offset=0,
                )
                detail_payload = orders_service.get_order_detail("kaufland", "ORDER-DET-1")

        self.assertEqual(list_payload["total"], 1)
        assert detail_payload is not None
        list_summary = list_payload["items"][0]
        detail_summary = detail_payload["summary"]
        self.assertEqual(detail_summary["total_cents"], list_summary["total_cents"])
        self.assertEqual(detail_summary["after_fees_cents"], list_summary["after_fees_cents"])
        self.assertEqual(detail_summary["shipping_cents"], list_summary["shipping_cents"])

    def test_shopify_order_detail_summary_includes_refund_amount_sum(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "shopify.sqlite3"
            connection = sqlite3.connect(db_path)
            try:
                connection.execute(
                    """
                    CREATE TABLE orders (
                        id TEXT PRIMARY KEY,
                        order_number INTEGER,
                        name TEXT,
                        email TEXT,
                        created_at TEXT,
                        updated_at TEXT,
                        financial_status TEXT,
                        fulfillment_status TEXT,
                        total_price TEXT,
                        subtotal_price TEXT,
                        total_tax TEXT,
                        total_discounts TEXT,
                        currency TEXT,
                        tags TEXT,
                        note TEXT,
                        customer_id TEXT,
                        customer_email TEXT,
                        customer_first_name TEXT,
                        customer_last_name TEXT,
                        shipping_country TEXT,
                        shipping_city TEXT,
                        line_items_count INTEGER,
                        fulfillments_count INTEGER,
                        refunds_count INTEGER,
                        raw_json TEXT NOT NULL,
                        synced_at TEXT NOT NULL,
                        estimated_paypal_fee TEXT,
                        estimated_net_after_fee TEXT,
                        fee_estimation_note TEXT,
                        payment_method TEXT
                    )
                    """
                )
                connection.execute(
                    "CREATE TABLE order_line_items (id TEXT PRIMARY KEY, order_id TEXT NOT NULL, title TEXT, product_id TEXT, raw_json TEXT)"
                )
                connection.execute(
                    "CREATE TABLE order_refunds (id TEXT PRIMARY KEY, order_id TEXT NOT NULL, created_at TEXT, transactions_json TEXT, raw_json TEXT)"
                )
                connection.execute(
                    "CREATE TABLE order_fulfillments (id TEXT PRIMARY KEY, order_id TEXT NOT NULL, created_at TEXT, raw_json TEXT)"
                )
                connection.execute(
                    "CREATE TABLE order_transactions (id TEXT PRIMARY KEY, order_id TEXT NOT NULL, fee_amount TEXT, net_amount TEXT, processed_at TEXT, raw_json TEXT)"
                )
                connection.execute(
                    "INSERT INTO orders (id, name, created_at, updated_at, financial_status, fulfillment_status, total_price, currency, customer_email, customer_first_name, customer_last_name, line_items_count, fulfillments_count, refunds_count, raw_json, synced_at, estimated_paypal_fee, estimated_net_after_fee, payment_method) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "7643540750675",
                        "#1001",
                        "2026-01-01T00:00:00Z",
                        "2026-01-01T00:00:00Z",
                        "partially_refunded",
                        "fulfilled",
                        "100.00",
                        "EUR",
                        "max@example.com",
                        "Max",
                        "Mustermann",
                        1,
                        0,
                        1,
                        '{"payment_gateway_names":["paypal"]}',
                        "2026-01-01T00:00:00Z",
                        "3.50",
                        "96.50",
                        "PayPal",
                    ),
                )
                connection.execute(
                    "INSERT INTO order_line_items (id, order_id, title, product_id, raw_json) VALUES (?, ?, ?, ?, ?)",
                    ("li-1", "7643540750675", "Artikel A", "prod-1", "{}"),
                )
                connection.execute(
                    "INSERT INTO order_refunds (id, order_id, created_at, transactions_json, raw_json) VALUES (?, ?, ?, ?, ?)",
                    (
                        "ref-1",
                        "7643540750675",
                        "2026-01-02T00:00:00Z",
                        '[{"kind":"refund","status":"success","amount":"25.00"}]',
                        "{}",
                    ),
                )
                connection.commit()
            finally:
                connection.close()

            from app.services import orders as orders_service

            with patch.object(orders_service, "SHOPIFY_DB_PATH", db_path):
                detail_payload = orders_service.get_order_detail("shopify", "7643540750675")

        assert detail_payload is not None
        self.assertEqual(detail_payload["summary"]["total_cents"], 7500)
        self.assertEqual(detail_payload["summary"]["after_fees_cents"], 7238)

    def test_list_orders_falls_back_when_combined_orders_partial(self) -> None:
        from app.services import orders as orders_service

        shopify_rows = [
            {
                "marketplace": "shopify",
                "order_id": "shop-1",
                "external_order_id": "#1001",
                "order_date": "2026-01-02T00:00:00Z",
                "customer": "Alice",
                "article": "Artikel A",
                "line_items_count": 1,
                "total_cents": 10000,
                "fees_cents": 350,
                "after_fees_cents": 9650,
                "shipping_cents": 0,
                "currency": "EUR",
                "fulfillment_status": "fulfilled",
                "payment_method": "PayPal",
                "fee_source": "estimated",
                "financial_status": "paid",
                "raw_status": "fulfilled",
            },
            {
                "marketplace": "shopify",
                "order_id": "shop-2",
                "external_order_id": "#1002",
                "order_date": "2026-01-01T00:00:00Z",
                "customer": "Bob",
                "article": "Artikel B",
                "line_items_count": 1,
                "total_cents": 20000,
                "fees_cents": 500,
                "after_fees_cents": 19500,
                "shipping_cents": 0,
                "currency": "EUR",
                "fulfillment_status": "fulfilled",
                "payment_method": "Shopify Payments",
                "fee_source": "estimated",
                "financial_status": "paid",
                "raw_status": "fulfilled",
            },
        ]

        combined = sqlite3.connect(":memory:")
        combined.row_factory = sqlite3.Row
        combined.execute(
            """
            CREATE TABLE combined_orders (
                id TEXT PRIMARY KEY,
                marketplace TEXT NOT NULL,
                order_id TEXT NOT NULL,
                external_order_id TEXT NOT NULL,
                order_date TEXT,
                customer TEXT,
                article TEXT,
                line_items_count INTEGER NOT NULL DEFAULT 1,
                total_cents INTEGER NOT NULL DEFAULT 0,
                fees_cents INTEGER NOT NULL DEFAULT 0,
                after_fees_cents INTEGER NOT NULL DEFAULT 0,
                shipping_cents INTEGER NOT NULL DEFAULT 0,
                currency TEXT NOT NULL DEFAULT 'EUR',
                fulfillment_status TEXT,
                payment_method TEXT,
                fee_source TEXT,
                financial_status TEXT,
                raw_status TEXT,
                raw_json TEXT,
                purchase_cost_cents INTEGER NOT NULL DEFAULT 0,
                purchase_currency TEXT NOT NULL DEFAULT 'EUR',
                purchase_supplier TEXT,
                purchase_notes TEXT,
                profit_cents INTEGER NOT NULL DEFAULT 0,
                has_invoice INTEGER NOT NULL DEFAULT 0,
                invoice_document_id TEXT
            )
            """
        )
        combined.execute(
            "INSERT INTO combined_orders (id, marketplace, order_id, external_order_id, order_date) VALUES (?, ?, ?, ?, ?)",
            ("shopify:shop-1", "shopify", "shop-1", "#1001", "2026-01-02T00:00:00Z"),
        )
        combined.commit()

        with patch.object(orders_service, "_load_shopify_orders", return_value=shopify_rows), \
             patch.object(orders_service, "_load_kaufland_orders", return_value=[]), \
             patch.object(orders_service, "SHOPIFY_DB_PATH", Path("/virtual/shopify.sqlite3")), \
             patch.object(orders_service, "KAUFLAND_DB_PATH", Path("/virtual/kaufland.sqlite3")), \
             patch.object(orders_service.Path, "exists", autospec=True) as exists_mock, \
             patch("app.services.orders.fetch_enrichment_map", return_value={}), \
             patch("app.services.orders.connect_combined_db", return_value=combined), \
             patch("app.services.orders._source_order_counts", return_value={"shopify": 2, "kaufland": 0}):
            exists_mock.side_effect = lambda path_obj: str(path_obj) in {"/virtual/shopify.sqlite3", "/virtual/kaufland.sqlite3"}
            payload = orders_service.list_orders(
                from_date=None,
                to_date=None,
                marketplace="shopify",
                query=None,
                limit=10,
                offset=0,
            )

        self.assertEqual(payload["total"], 2)
        self.assertEqual([item["order_id"] for item in payload["items"]], ["shop-1", "shop-2"])

    def test_populate_combined_orders_shopify_matches_source_summary(self) -> None:
        from app import db as app_db

        with tempfile.TemporaryDirectory() as temp_dir:
            combined_path = Path(temp_dir) / "combined.sqlite3"
            shopify_path = Path(temp_dir) / "shopify.sqlite3"

            connection = sqlite3.connect(shopify_path)
            try:
                connection.execute(
                    """
                    CREATE TABLE orders (
                        id TEXT PRIMARY KEY,
                        order_number INTEGER,
                        name TEXT,
                        email TEXT,
                        created_at TEXT,
                        updated_at TEXT,
                        financial_status TEXT,
                        fulfillment_status TEXT,
                        total_price TEXT,
                        subtotal_price TEXT,
                        total_tax TEXT,
                        total_discounts TEXT,
                        currency TEXT,
                        tags TEXT,
                        note TEXT,
                        customer_id TEXT,
                        customer_email TEXT,
                        customer_first_name TEXT,
                        customer_last_name TEXT,
                        shipping_country TEXT,
                        shipping_city TEXT,
                        line_items_count INTEGER,
                        fulfillments_count INTEGER,
                        refunds_count INTEGER,
                        raw_json TEXT NOT NULL,
                        synced_at TEXT NOT NULL,
                        estimated_paypal_fee TEXT,
                        estimated_net_after_fee TEXT,
                        fee_estimation_note TEXT,
                        payment_method TEXT
                    )
                    """
                )
                connection.execute("CREATE TABLE order_line_items (id TEXT PRIMARY KEY, order_id TEXT NOT NULL, title TEXT, product_id TEXT, raw_json TEXT)")
                connection.execute("CREATE TABLE order_refunds (id TEXT PRIMARY KEY, order_id TEXT NOT NULL, created_at TEXT, transactions_json TEXT, raw_json TEXT)")
                connection.execute("CREATE TABLE order_transactions (id TEXT PRIMARY KEY, order_id TEXT NOT NULL, fee_amount TEXT, net_amount TEXT, processed_at TEXT, raw_json TEXT)")
                connection.execute(
                    "INSERT INTO orders (id, name, created_at, updated_at, financial_status, fulfillment_status, total_price, currency, customer_email, customer_first_name, customer_last_name, line_items_count, fulfillments_count, refunds_count, raw_json, synced_at, estimated_paypal_fee, estimated_net_after_fee, payment_method) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "7643540750675",
                        "#1001",
                        "2026-01-01T00:00:00Z",
                        "2026-01-01T00:00:00Z",
                        "partially_refunded",
                        "fulfilled",
                        "100.00",
                        "EUR",
                        "max@example.com",
                        "Max",
                        "Mustermann",
                        1,
                        0,
                        1,
                        '{"payment_gateway_names":["paypal"]}',
                        "2026-01-01T00:00:00Z",
                        "3.50",
                        "96.50",
                        "PayPal",
                    ),
                )
                connection.execute(
                    "INSERT INTO order_line_items (id, order_id, title, product_id, raw_json) VALUES (?, ?, ?, ?, ?)",
                    ("li-1", "7643540750675", "Artikel A", "prod-1", "{}"),
                )
                connection.execute(
                    "INSERT INTO order_refunds (id, order_id, created_at, transactions_json, raw_json) VALUES (?, ?, ?, ?, ?)",
                    (
                        "ref-1",
                        "7643540750675",
                        "2026-01-02T00:00:00Z",
                        '[{"kind":"refund","status":"success","amount":"25.00"}]',
                        "{}",
                    ),
                )
                connection.commit()
            finally:
                connection.close()

            with patch.object(app_db, "COMBINED_DB_PATH", combined_path), \
                 patch.object(app_db, "SHOPIFY_DB_PATH", shopify_path), \
                 patch.object(app_db, "KAUFLAND_DB_PATH", Path(temp_dir) / "missing-kaufland.sqlite3"):
                app_db.init_combined_db()
                rows_written = app_db.populate_combined_orders()

                combined_connection = sqlite3.connect(combined_path)
                combined_connection.row_factory = sqlite3.Row
                try:
                    stored = combined_connection.execute(
                        "SELECT fees_cents, after_fees_cents, profit_cents, fee_source FROM combined_orders WHERE marketplace = 'shopify' AND order_id = ?",
                        ("7643540750675",),
                    ).fetchone()
                finally:
                    combined_connection.close()

        self.assertEqual(rows_written, 1)
        assert stored is not None
        self.assertEqual(stored["fees_cents"], 262)
        self.assertEqual(stored["after_fees_cents"], 7238)
        self.assertEqual(stored["profit_cents"], 7238)
        self.assertEqual(stored["fee_source"], "stored_estimate")

    def test_shopify_live_sync_reports_changed_order_ids(self) -> None:
        class FakeShopifyClient:
            def __init__(self, _config: object) -> None:
                pass

            def get_orders_page(self, *, status: str, limit: int, cursor_url: str | None = None, updated_at_min: str | None = None):
                if cursor_url is not None:
                    return ([], None)
                return ([{
                    "id": "shop-1",
                    "order_number": 1001,
                    "name": "#1001",
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T00:00:00Z",
                    "financial_status": "paid",
                    "fulfillment_status": "fulfilled",
                    "total_price": "100.00",
                    "currency": "EUR",
                    "customer": {"first_name": "Alice", "last_name": "Example", "email": "alice@example.com"},
                    "shipping_address": {"country": "Germany", "city": "Berlin"},
                    "line_items": [{"id": "li-1", "title": "Artikel A", "price": "100.00", "quantity": 1}],
                    "refunds": [],
                    "transactions": [{"id": "tx-1", "kind": "sale", "status": "success", "gateway": "paypal", "amount": "100.00", "currency": "EUR", "processed_at": "2026-01-01T00:00:00Z"}],
                    "payment_gateway_names": ["paypal"],
                }], None)

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "shopify.sqlite3"
            config = object()
            with patch("app.services.importers.shopify_live.load_shopify_live_config", return_value=(config, [], {"shop_domain": "demo"})), \
                 patch("app.services.importers.shopify_live.SHOPIFY_DB_PATH", db_path), \
                 patch("app.services.importers.shopify_live.ShopifyLiveClient", return_value=FakeShopifyClient(config)), \
                 patch("app.services.importers.shopify_live._record_sync_run"):
                result = sync_shopify_live(include_transactions=True)

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["_changed_order_ids"], ["shop-1"])

    def test_kaufland_delta_sync_rechecks_recent_incomplete_orders(self) -> None:
        class FakeKauflandClient:
            def get_json(self, url: str, uri: str) -> object:
                if uri.startswith("/v2/orders/?"):
                    return {"data": []}
                if uri == "/v2/orders/ORDER-RECHECK-1/":
                    return {
                        "data": {
                            "id_order": "ORDER-RECHECK-1",
                            "ts_created_iso": "2026-06-16T11:35:00Z",
                            "storefront": "de",
                            "order_units": [{"id_order_unit": "unit-1", "status": "need_to_be_sent"}],
                        }
                    }
                if uri == "/v2/order-units/unit-1/":
                    return {
                        "data": {
                            "id_order_unit": "unit-1",
                            "id_order": "ORDER-RECHECK-1",
                            "status": "need_to_be_sent",
                            "price": "1999",
                            "revenue_gross": "1799",
                            "shipping_rate": "300",
                            "product": {"title": "Alpha"},
                            "billing_address": {
                                "first_name": "Alice",
                                "last_name": "Example",
                                "street": "Billing St",
                                "house_number": "1",
                                "postcode": "10115",
                                "city": "Berlin",
                                "country": "DE",
                            },
                            "shipping_address": {
                                "first_name": "Alice",
                                "last_name": "Example",
                                "street": "Shipping St",
                                "house_number": "2",
                                "postcode": "10115",
                                "city": "Berlin",
                                "country": "DE",
                            },
                            "buyer": {"id_buyer": "buyer-1", "email": "alice@example.com"},
                            "tracking_numbers": [],
                        }
                    }
                raise AssertionError(f"unexpected request: {uri}")

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "kaufland.sqlite3"
            config = kaufland_live.KauflandLiveConfig(
                client_key="key",
                secret_key="secret",
                base_url="https://sellerapi.kaufland.com/v2",
                verify_ssl=True,
            )

            with patch.object(kaufland_live, "KAUFLAND_DB_PATH", db_path), \
                 patch.object(kaufland_live, "load_kaufland_live_config", return_value=(config, [], {"base_url": config.base_url})), \
                 patch.object(kaufland_live, "KauflandLiveClient", return_value=FakeKauflandClient()), \
                 patch.object(kaufland_live, "_utc_now_iso", return_value="2026-06-16T12:00:00Z"):
                kaufland_live.init_kaufland_db()
                connection = sqlite3.connect(db_path)
                try:
                    connection.execute(
                        "INSERT INTO orders (id_order, order_units_count, ts_created_iso, storefront, is_marketplace_deemed_supplier, raw_json, synced_at_iso) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            "ORDER-RECHECK-1",
                            1,
                            "2026-06-16T11:35:00Z",
                            "de",
                            0,
                            '{"id_order":"ORDER-RECHECK-1"}',
                            "2026-06-16T11:40:00Z",
                        ),
                    )
                    connection.execute(
                        """
                        INSERT INTO order_units (
                            id_order_unit, id_order, ts_created_iso, status, cancel_reason, price,
                            revenue_gross, revenue_net, vat, shipping_rate, id_offer, storefront,
                            is_marketplace_deemed_supplier, buyer_id_buyer, billing_first_name,
                            billing_last_name, billing_company_name, billing_street, billing_house_number,
                            billing_postcode, billing_city, billing_additional_field, billing_phone,
                            billing_country, shipping_first_name, shipping_last_name,
                            shipping_company_name, shipping_street, shipping_house_number,
                            shipping_postcode, shipping_city, shipping_additional_field,
                            shipping_phone, shipping_country, product_id_product, product_title,
                            product_eans_json, product_id_category, product_main_picture, product_url,
                            carrier_code, tracking_numbers_json, raw_json, synced_at_iso
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "unit-1",
                            "ORDER-RECHECK-1",
                            "2026-06-16T11:35:30Z",
                            "unknown",
                            None,
                            "1999",
                            "1799",
                            None,
                            None,
                            "300",
                            None,
                            "de",
                            0,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            '{"id_order_unit":"unit-1","status":"unknown"}',
                            "2026-06-16T11:40:00Z",
                        ),
                    )
                    connection.commit()
                finally:
                    connection.close()

                result = kaufland_live.sync_kaufland_live(
                    storefront="de",
                    page_limit=50,
                    max_pages=1,
                    include_returns=False,
                    include_order_unit_details=True,
                    ts_created_from_iso="2026-06-16T11:55:00Z",
                )

            self.assertEqual(result["status"], "success")
            summary = result["summary"]
            self.assertEqual(summary["orders_seen"], 0)
            self.assertEqual(summary["recent_incomplete_rechecks_considered"], 1)
            self.assertEqual(summary["recent_incomplete_rechecks_attempted"], 1)
            self.assertEqual(summary["recent_incomplete_rechecks_completed"], 1)
            self.assertEqual(summary["recent_incomplete_rechecks_updated"], 1)
            self.assertEqual(summary["recent_incomplete_rechecks_failed"], 0)

            connection = sqlite3.connect(db_path)
            connection.row_factory = sqlite3.Row
            try:
                row = connection.execute(
                    "SELECT status, product_title, shipping_first_name, shipping_street, billing_first_name, billing_street, synced_at_iso FROM order_units WHERE id_order_unit = ?",
                    ("unit-1",),
                ).fetchone()
            finally:
                connection.close()

        assert row is not None
        self.assertEqual(row["status"], "need_to_be_sent")
        self.assertEqual(row["product_title"], "Alpha")
        self.assertEqual(row["shipping_first_name"], "Alice")
        self.assertEqual(row["shipping_street"], "Shipping St")
        self.assertEqual(row["billing_first_name"], "Alice")
        self.assertEqual(row["billing_street"], "Billing St")
        self.assertEqual(row["synced_at_iso"], "2026-06-16T12:00:00Z")

    def test_run_live_sync_refreshes_combined_orders_for_changed_live_orders(self) -> None:
        with patch.object(live_sync, "sync_shopify_live", return_value={
            "status": "success",
            "provider": "shopify",
            "summary": {"total_inserted": 1, "total_updated": 1},
            "_changed_order_ids": ["shop-2", "shop-1"],
        }), patch.object(live_sync, "sync_kaufland_live", return_value={
            "status": "success",
            "provider": "kaufland",
            "summary": {"orders_saved": 1, "order_units_saved": 1},
            "_changed_order_ids": ["kau-1"],
        }), patch.object(live_sync, "init_combined_db"), \
             patch.object(live_sync, "populate_combined_orders", side_effect=[1, 1, 1]) as populate_mock, \
             patch.object(live_sync, "build_live_sync_status", return_value={"background": {}}):
            result = live_sync.run_live_sync(
                run_shopify=True,
                run_kaufland=True,
                shopify_status="any",
                shopify_page_limit=50,
                shopify_max_pages=2,
                shopify_include_line_items=True,
                shopify_include_fulfillments=True,
                shopify_include_refunds=True,
                shopify_include_transactions=True,
                kaufland_storefront="de",
                kaufland_page_limit=50,
                kaufland_max_pages=2,
                kaufland_include_returns=True,
                kaufland_include_order_unit_details=True,
            )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["combined_orders"]["status"], "refreshed")
        self.assertEqual(result["combined_orders"]["orders_considered"], 3)
        self.assertEqual(result["combined_orders"]["orders_refreshed"], 3)
        populate_mock.assert_has_calls(
            [
                call(marketplace="kaufland", order_id="kau-1"),
                call(marketplace="shopify", order_id="shop-1"),
                call(marketplace="shopify", order_id="shop-2"),
            ]
        )
        self.assertNotIn("_changed_order_ids", result["results"]["shopify"])
        self.assertNotIn("_changed_order_ids", result["results"]["kaufland"])

    def test_kaufland_shipment_capabilities_only_allow_need_to_be_sent_units(self) -> None:
        capabilities = build_shipment_capabilities(
            {
                "summary": {"marketplace": "kaufland", "order_id": "ORDER-1"},
                "units": [
                    {"id_order_unit": "unit-1", "product_title": "Alpha", "status": "need_to_be_sent"},
                    {"id_order_unit": "unit-2", "product_title": "Beta", "status": "sent"},
                ],
            }
        )

        self.assertTrue(capabilities["available"])
        self.assertEqual(capabilities["pending_units_count"], 1)
        self.assertEqual(capabilities["pending_units"][0]["id_order_unit"], "unit-1")

    def test_shopify_shipment_capabilities_hide_already_fulfilled_orders(self) -> None:
        capabilities = build_shipment_capabilities(
            {
                "summary": {
                    "marketplace": "shopify",
                    "order_id": "shop-1",
                    "fulfillment_status": "fulfilled",
                    "financial_status": "paid",
                },
                "line_items": [{"id": "line-1", "title": "Alpha", "fulfillment_status": "fulfilled"}],
            }
        )

        self.assertFalse(capabilities["available"])
        self.assertIn("versendet", str(capabilities["reason"]).lower())

    def test_shopify_list_orders_light_mode_does_not_require_raw_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "shopify.sqlite3"
            connection = sqlite3.connect(db_path)
            try:
                connection.execute(
                    """
                    CREATE TABLE orders (
                        id TEXT PRIMARY KEY,
                        order_number INTEGER,
                        name TEXT,
                        email TEXT,
                        created_at TEXT,
                        updated_at TEXT,
                        financial_status TEXT,
                        fulfillment_status TEXT,
                        total_price TEXT,
                        subtotal_price TEXT,
                        total_tax TEXT,
                        total_discounts TEXT,
                        currency TEXT,
                        tags TEXT,
                        note TEXT,
                        customer_id TEXT,
                        customer_email TEXT,
                        customer_first_name TEXT,
                        customer_last_name TEXT,
                        shipping_country TEXT,
                        shipping_city TEXT,
                        line_items_count INTEGER,
                        fulfillments_count INTEGER,
                        refunds_count INTEGER,
                        raw_json TEXT NOT NULL,
                        synced_at TEXT NOT NULL,
                        estimated_paypal_fee TEXT,
                        estimated_net_after_fee TEXT,
                        fee_estimation_note TEXT,
                        payment_method TEXT
                    )
                    """
                )
                connection.execute(
                    "CREATE TABLE order_line_items (id TEXT PRIMARY KEY, order_id TEXT NOT NULL, title TEXT, product_id TEXT, raw_json TEXT)"
                )
                connection.execute(
                    "CREATE TABLE order_refunds (id TEXT PRIMARY KEY, order_id TEXT NOT NULL, created_at TEXT, transactions_json TEXT, raw_json TEXT)"
                )
                connection.execute(
                    "CREATE TABLE order_fulfillments (id TEXT PRIMARY KEY, order_id TEXT NOT NULL, created_at TEXT, raw_json TEXT)"
                )
                connection.execute(
                    "CREATE TABLE order_transactions (id TEXT PRIMARY KEY, order_id TEXT NOT NULL, fee_amount TEXT, net_amount TEXT, processed_at TEXT, raw_json TEXT)"
                )
                connection.execute(
                    "INSERT INTO orders (id, name, created_at, updated_at, financial_status, fulfillment_status, total_price, currency, customer_email, customer_first_name, customer_last_name, line_items_count, fulfillments_count, refunds_count, raw_json, synced_at, estimated_paypal_fee, estimated_net_after_fee, payment_method) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "light-1",
                        "#2001",
                        "2026-01-01T00:00:00Z",
                        "2026-01-01T00:00:00Z",
                        "paid",
                        "fulfilled",
                        "100.00",
                        "EUR",
                        "light@example.com",
                        "Light",
                        "Mode",
                        1,
                        0,
                        0,
                        "not-json",
                        "2026-01-01T00:00:00Z",
                        "3.50",
                        "96.50",
                        "PayPal",
                    ),
                )
                connection.execute(
                    "INSERT INTO order_line_items (id, order_id, title, product_id, raw_json) VALUES (?, ?, ?, ?, ?)",
                    ("li-light-1", "light-1", "Artikel Light", "prod-1", "{}"),
                )
                connection.commit()
            finally:
                connection.close()

            from app.services import orders as orders_service

            _stub_combined = _create_stub_combined_db()
            with patch.object(orders_service, "SHOPIFY_DB_PATH", db_path), \
                 patch.object(orders_service, "KAUFLAND_DB_PATH", Path(temp_dir) / "missing-kaufland.sqlite3"), \
                 patch("app.services.orders.fetch_enrichment_map", return_value={}), \
                 patch("app.services.orders.connect_combined_db", return_value=_stub_combined):
                light_payload = orders_service.list_orders(
                    from_date=None,
                    to_date=None,
                    marketplace="shopify",
                    query=None,
                    limit=10,
                    offset=0,
                    include_raw_fallbacks=False,
                )
                full_payload = orders_service.list_orders(
                    from_date=None,
                    to_date=None,
                    marketplace="shopify",
                    query=None,
                    limit=10,
                    offset=0,
                    include_raw_fallbacks=True,
                )

        self.assertEqual(light_payload["total"], 1)
        self.assertEqual(full_payload["total"], 1)
        light_summary = light_payload["items"][0]
        full_summary = full_payload["items"][0]
        self.assertEqual(light_summary["order_id"], "light-1")
        self.assertEqual(light_summary["customer"], "Light Mode")
        self.assertEqual(light_summary["fees_cents"], 350)
        self.assertEqual(light_summary["after_fees_cents"], 9650)
        self.assertEqual(light_summary["shipping_cents"], 0)
        self.assertEqual(full_summary["fees_cents"], 350)
        self.assertEqual(full_summary["after_fees_cents"], 9650)

    def test_list_orders_status_filter_matches_supported_status_tokens(self) -> None:
        from app.services import orders as orders_service

        shopify_rows = [
            {
                "marketplace": "shopify",
                "order_id": "shop-paid",
                "external_order_id": "#3001",
                "order_date": "2026-01-03T00:00:00Z",
                "customer": "Paid Customer",
                "article": "Artikel Paid",
                "total_cents": 10000,
                "fees_cents": 500,
                "after_fees_cents": 9500,
                "shipping_cents": 0,
                "currency": "EUR",
                "fulfillment_status": "fulfilled",
                "financial_status": "paid",
                "raw_status": "fulfilled",
                "payment_method": "Shopify Payments",
                "fee_source": "api",
            },
            {
                "marketplace": "shopify",
                "order_id": "shop-refund",
                "external_order_id": "#3002",
                "order_date": "2026-01-02T00:00:00Z",
                "customer": "Refund Customer",
                "article": "Artikel Refund",
                "total_cents": 10000,
                "fees_cents": 500,
                "after_fees_cents": 9500,
                "shipping_cents": 0,
                "currency": "EUR",
                "fulfillment_status": "",
                "financial_status": "partially_refunded",
                "raw_status": "refunded",
                "payment_method": "PayPal",
                "fee_source": "api",
            },
        ]
        kaufland_rows = [
            {
                "marketplace": "kaufland",
                "order_id": "kau-sent",
                "external_order_id": "ORDER-3003",
                "order_date": "2026-01-01T00:00:00Z",
                "customer": "Sent Customer",
                "article": "Artikel Sent",
                "total_cents": 10000,
                "fees_cents": 500,
                "after_fees_cents": 9500,
                "shipping_cents": 0,
                "currency": "EUR",
                "fulfillment_status": "sent_and_autopaid",
                "financial_status": "",
                "raw_status": "sent_and_autopaid",
                "payment_method": "Kaufland Settlement",
                "fee_source": "api",
            },
        ]

        with patch.object(orders_service, "_load_shopify_orders", return_value=shopify_rows), \
             patch.object(orders_service, "_load_kaufland_orders", return_value=kaufland_rows), \
             patch("app.services.orders.fetch_enrichment_map", return_value={}), \
             patch("app.services.orders.connect_combined_db", return_value=_create_stub_combined_db()):
            paid_payload = orders_service.list_orders(
                from_date=None,
                to_date=None,
                marketplace=None,
                query=None,
                status_filter="paid",
                limit=10,
                offset=0,
            )
            refunded_payload = orders_service.list_orders(
                from_date=None,
                to_date=None,
                marketplace=None,
                query=None,
                status_filter="refunded",
                limit=10,
                offset=0,
            )
            sent_payload = orders_service.list_orders(
                from_date=None,
                to_date=None,
                marketplace=None,
                query=None,
                status_filter="sent",
                limit=10,
                offset=0,
            )

        self.assertEqual([item["order_id"] for item in paid_payload["items"]], ["shop-paid"])
        self.assertEqual([item["order_id"] for item in refunded_payload["items"]], ["shop-refund"])
        self.assertEqual([item["order_id"] for item in sent_payload["items"]], ["kau-sent"])


if __name__ == "__main__":
    unittest.main()
