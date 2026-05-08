from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch
from zipfile import ZIP_DEFLATED, ZipFile
from pathlib import Path

os.environ.setdefault("AUTO_SYNC_ON_STARTUP", "0")
os.environ.setdefault("LIVE_SYNC_BACKGROUND_ENABLED", "0")

PROJECT_DIR = Path(__file__).resolve().parent.parent / "ecommerce-dashboard"
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from app.services.analytics import build_analytics
from app.services.bookkeeping_full import _calculate_amount_net_cents, _calculate_vat_amount_cents
from app.services.exports import _resolve_backup_manifest_path
from app.services.google_ads import _parse_ads_report_csv
from app.services.orders import _kaufland_summary_from_row, _shopify_summary_from_row, _to_kaufland_cents


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


class RegressionTests(unittest.TestCase):
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

            with patch.object(orders_service, "KAUFLAND_DB_PATH", db_path), \
                 patch("app.services.orders.fetch_enrichment_map", return_value={}):
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


if __name__ == "__main__":
    unittest.main()
