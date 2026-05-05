from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("AUTO_SYNC_ON_STARTUP", "0")
os.environ.setdefault("LIVE_SYNC_BACKGROUND_ENABLED", "0")
os.environ.setdefault("APP_ADMIN_TOKEN", "test-admin-token")

PROJECT_DIR = Path(__file__).resolve().parent.parent / "ecommerce-dashboard"
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from fastapi.testclient import TestClient

from app import db as app_db
from app.main import app
from app.services import bookings, bookkeeping_full, source_sync


def _init_bookkeeping_schema(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.executescript(
        """
        CREATE TABLE orders (
            id TEXT PRIMARY KEY,
            provider TEXT NOT NULL,
            external_order_id TEXT NOT NULL,
            order_date TEXT NOT NULL,
            currency TEXT NOT NULL,
            revenue_gross INTEGER NOT NULL CHECK (revenue_gross > 0),
            revenue_net INTEGER,
            vat_amount INTEGER,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(provider, external_order_id)
        );
        CREATE INDEX idx_orders_order_date ON orders(order_date);

        CREATE TABLE documents (
            id TEXT PRIMARY KEY,
            original_filename TEXT NOT NULL,
            stored_filename TEXT NOT NULL,
            file_path TEXT NOT NULL,
            mime_type TEXT,
            uploaded_at TEXT NOT NULL,
            notes TEXT
        );

        CREATE TABLE recurring_templates (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            schedule TEXT,
            start_date TEXT
        );

        CREATE TABLE payment_accounts (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            provider TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT '2026-01-01T00:00:00Z',
            updated_at TEXT NOT NULL DEFAULT '2026-01-01T00:00:00Z'
        );

        CREATE TABLE transactions (
            id TEXT PRIMARY KEY,
            date TEXT NOT NULL,
            type TEXT NOT NULL,
            direction TEXT NOT NULL,
            amount_gross INTEGER NOT NULL CHECK (amount_gross > 0),
            currency TEXT NOT NULL,
            vat_rate REAL,
            vat_amount INTEGER,
            amount_net INTEGER,
            provider TEXT NOT NULL,
            counterparty_name TEXT,
            category TEXT,
            reference TEXT,
            notes TEXT,
            order_id TEXT,
            document_id TEXT,
            template_id TEXT,
            payment_account_id TEXT,
            period_key TEXT,
            source TEXT NOT NULL,
            source_key TEXT UNIQUE,
            status TEXT,
            booking_class TEXT DEFAULT 'single',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE SET NULL,
            FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE SET NULL
        );
        """
    )
    connection.commit()
    connection.close()


class BookkeepingSyncTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "dashboard.sqlite3"
        _init_bookkeeping_schema(self.db_path)

        self.original_bookings_db = bookings.BOOKKEEPING_DB_PATH
        self.original_bf_db = bookkeeping_full.BOOKKEEPING_DB_PATH
        bookings.BOOKKEEPING_DB_PATH = self.db_path
        bookkeeping_full.BOOKKEEPING_DB_PATH = self.db_path

    def tearDown(self) -> None:
        bookings.BOOKKEEPING_DB_PATH = self.original_bookings_db
        bookkeeping_full.BOOKKEEPING_DB_PATH = self.original_bf_db
        self.temp_dir.cleanup()

    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)
        cls.admin_headers = {"X-Admin-Token": "test-admin-token"}

    def _fetchone(self, sql: str, params: tuple[object, ...] = ()) -> sqlite3.Row | None:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            return connection.execute(sql, params).fetchone()
        finally:
            connection.close()

    def _fetchall(self, sql: str, params: tuple[object, ...] = ()) -> list[sqlite3.Row]:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            return connection.execute(sql, params).fetchall()
        finally:
            connection.close()

    def test_sync_keeps_zero_value_order_and_removes_auto_transactions_for_full_refund(self) -> None:
        initial_order = {
            "marketplace": "shopify",
            "order_id": "1",
            "external_order_id": "#1001",
            "order_date": "2026-01-10T10:00:00Z",
            "currency": "EUR",
            "total_cents": 10000,
            "fees_cents": 500,
            "after_fees_cents": 9500,
            "purchase_cost_cents": 4000,
            "customer": "Alice",
            "fulfillment_status": "fulfilled",
            "financial_status": "paid",
            "raw_status": "fulfilled",
            "payment_method": "Shopify",
        }
        refunded_order = {
            **initial_order,
            "fulfillment_status": "fulfilled",
            "financial_status": "refunded",
            "raw_status": "refunded",
            "total_cents": 10000,
            "fees_cents": 500,
            "after_fees_cents": 9500,
        }

        with patch("app.db.fetch_enrichment_map", return_value={}):
            with patch("app.services.orders.list_all_orders_without_pagination", return_value=[initial_order]):
                first_summary = bookings.sync_combined_orders_into_bookkeeping()

            with patch("app.services.orders.list_all_orders_without_pagination", return_value=[refunded_order]):
                second_summary = bookings.sync_combined_orders_into_bookkeeping()

        self.assertEqual(first_summary["transactions_inserted"], 3)
        self.assertGreaterEqual(second_summary["transactions_deleted"], 3)

        order_row = self._fetchone(
            "SELECT revenue_gross, revenue_net, status FROM orders WHERE provider = ? AND external_order_id = ?",
            ("shopify", "#1001"),
        )
        assert order_row is not None
        self.assertEqual(order_row["revenue_gross"], 0)
        self.assertEqual(order_row["revenue_net"], 0)
        self.assertEqual(order_row["status"], "refunded")

        tx_rows = self._fetchall("SELECT source_key FROM transactions ORDER BY source_key")
        self.assertEqual(tx_rows, [])

    def test_sync_preserves_positive_rows_for_partial_refund(self) -> None:
        partial_refund_order = {
            "marketplace": "shopify",
            "order_id": "2",
            "external_order_id": "#1002",
            "order_date": "2026-01-11T10:00:00Z",
            "currency": "EUR",
            "total_cents": 7500,
            "fees_cents": 262,
            "after_fees_cents": 7238,
            "purchase_cost_cents": 3000,
            "customer": "Bob",
            "fulfillment_status": "fulfilled",
            "financial_status": "partially_refunded",
            "raw_status": "partially_refunded",
            "payment_method": "PayPal",
        }

        with patch("app.db.fetch_enrichment_map", return_value={}):
            with patch("app.services.orders.list_all_orders_without_pagination", return_value=[partial_refund_order]):
                summary = bookings.sync_combined_orders_into_bookkeeping()

        self.assertEqual(summary["transactions_inserted"], 3)

        order_row = self._fetchone(
            "SELECT revenue_gross, revenue_net FROM orders WHERE provider = ? AND external_order_id = ?",
            ("shopify", "#1002"),
        )
        assert order_row is not None
        self.assertEqual(order_row["revenue_gross"], 7500)
        self.assertEqual(order_row["revenue_net"], 7238)

        amounts = {
            str(row["source_key"]): int(row["amount_gross"])
            for row in self._fetchall("SELECT source_key, amount_gross FROM transactions")
        }
        self.assertEqual(amounts["combined:shopify:#1002:sale"], 7500)
        self.assertEqual(amounts["combined:shopify:#1002:fee"], 262)
        self.assertEqual(amounts["combined:shopify:#1002:cogs"], 3000)

    def test_schema_migration_allows_zero_revenue_orders(self) -> None:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            changed = bookkeeping_full._ensure_schema(connection)
            connection.commit()
            sql = connection.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'orders'"
            ).fetchone()["sql"]
        finally:
            connection.close()

        self.assertTrue(changed)
        self.assertIn("revenue_gross >= 0", sql)

    def test_transaction_marketplace_filter_matches_joined_order_provider(self) -> None:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute(
                """
                INSERT INTO orders (
                    id, provider, external_order_id, order_date, currency,
                    revenue_gross, revenue_net, vat_amount, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "order-shopify",
                    "shopify",
                    "#2001",
                    "2026-02-01T10:00:00Z",
                    "EUR",
                    10000,
                    9000,
                    None,
                    "paid",
                    "2026-02-01T10:00:00Z",
                    "2026-02-01T10:00:00Z",
                ),
            )
            connection.execute(
                """
                INSERT INTO transactions (
                    id, date, type, direction, amount_gross, currency,
                    vat_rate, vat_amount, amount_net, provider, counterparty_name, category,
                    reference, notes, order_id, document_id, template_id, payment_account_id, period_key,
                    source, source_key, status, booking_class, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, 'api', ?, ?, ?, ?, ?)
                """,
                (
                    "tx-shopify-fee",
                    "2026-02-01T10:01:00Z",
                    "FEE",
                    "OUT",
                    500,
                    "EUR",
                    "paypal",
                    "PayPal Fees",
                    "fees",
                    "#2001-fees",
                    "Auto-sync",
                    "order-shopify",
                    "combined:shopify:#2001:fee",
                    "confirmed",
                    "automatic",
                    "2026-02-01T10:01:00Z",
                    "2026-02-01T10:01:00Z",
                ),
            )
            connection.execute(
                """
                INSERT INTO transactions (
                    id, date, type, direction, amount_gross, currency,
                    vat_rate, vat_amount, amount_net, provider, counterparty_name, category,
                    reference, notes, order_id, document_id, template_id, payment_account_id, period_key,
                    source, source_key, status, booking_class, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, 'api', ?, ?, ?, ?, ?)
                """,
                (
                    "tx-kaufland-fee",
                    "2026-02-01T12:00:00Z",
                    "FEE",
                    "OUT",
                    700,
                    "EUR",
                    "kaufland",
                    "Kaufland Fees",
                    "fees",
                    "kaufland-fees",
                    "Auto-sync",
                    "google_ads:2026-02-01:fee",
                    "confirmed",
                    "automatic",
                    "2026-02-01T12:00:00Z",
                    "2026-02-01T12:00:00Z",
                ),
            )
            connection.commit()
        finally:
            connection.close()

        shopify_rows = bookkeeping_full.list_bookkeeping_transactions({"marketplace": "shopify"})
        self.assertEqual(shopify_rows["total"], 1)
        self.assertEqual(shopify_rows["items"][0]["provider"], "paypal")
        self.assertEqual(shopify_rows["items"][0]["order"]["provider"], "shopify")

        kaufland_rows = bookkeeping_full.list_bookkeeping_transactions({"marketplace": "kaufland"})
        self.assertEqual(kaufland_rows["total"], 1)
        self.assertEqual(kaufland_rows["items"][0]["provider"], "kaufland")
        self.assertIsNone(kaufland_rows["items"][0]["order"])

    def test_source_sync_skips_bookkeeping_bootstrap_without_force(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_db = root / "bookkeeping-bootstrap.sqlite3"
            target_db = root / "bookkeeping-runtime.sqlite3"
            shopify_source = root / "shopify-bootstrap.sqlite3"
            shopify_target = root / "shopify-runtime.sqlite3"
            kaufland_source = root / "kaufland-bootstrap.sqlite3"
            kaufland_target = root / "kaufland-runtime.sqlite3"
            source_docs = root / "bootstrap-docs"
            target_docs = root / "runtime-docs"
            source_docs.mkdir(parents=True, exist_ok=True)
            target_docs.mkdir(parents=True, exist_ok=True)
            (source_docs / "sample.txt").write_text("source", encoding="utf-8")

            for path, value in (
                (source_db, 1),
                (target_db, 2),
                (shopify_source, 10),
                (shopify_target, 10),
                (kaufland_source, 20),
                (kaufland_target, 20),
            ):
                connection = sqlite3.connect(path)
                connection.execute("CREATE TABLE sample (value INTEGER)")
                connection.execute("INSERT INTO sample (value) VALUES (?)", (value,))
                connection.commit()
                connection.close()

            with patch.object(source_sync, "SHOPIFY_BOOTSTRAP_DB_PATH", shopify_source), \
                 patch.object(source_sync, "SHOPIFY_BOOTSTRAP_DB_PATH_FALLBACK", shopify_source), \
                 patch.object(source_sync, "SHOPIFY_DB_PATH", shopify_target), \
                 patch.object(source_sync, "KAUFLAND_BOOTSTRAP_DB_PATH", kaufland_source), \
                 patch.object(source_sync, "KAUFLAND_DB_PATH", kaufland_target), \
                 patch.object(source_sync, "BOOKKEEPING_BOOTSTRAP_DB_PATH", source_db), \
                 patch.object(source_sync, "BOOKKEEPING_DB_PATH", target_db), \
                 patch.object(source_sync, "BOOKKEEPING_BOOTSTRAP_DOCUMENTS_DIR", source_docs), \
                 patch.object(source_sync, "BOOKKEEPING_DOCUMENTS_DIR", target_docs):
                summary = source_sync.sync_all_sources(
                    force=False,
                    include_documents=True,
                    include_bookkeeping_bootstrap=True,
                )

            self.assertEqual(summary["results"]["bookkeeping_db"]["status"], "skipped")
            self.assertIn("already exists", summary["results"]["bookkeeping_db"]["reason"])
            self.assertEqual(summary["results"]["bookkeeping_documents"]["status"], "skipped")
            self.assertIn("already exists", summary["results"]["bookkeeping_documents"]["reason"])

            connection = sqlite3.connect(target_db)
            try:
                value = connection.execute("SELECT value FROM sample").fetchone()[0]
            finally:
                connection.close()
            self.assertEqual(value, 2)

    def test_regular_source_sync_disables_bookkeeping_bootstrap_even_with_force(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_db = root / "bookkeeping-bootstrap.sqlite3"
            target_db = root / "bookkeeping-runtime.sqlite3"

            for path, value in ((source_db, 1), (target_db, 2)):
                connection = sqlite3.connect(path)
                connection.execute("CREATE TABLE sample (value INTEGER)")
                connection.execute("INSERT INTO sample (value) VALUES (?)", (value,))
                connection.commit()
                connection.close()

            with patch.object(source_sync, "SHOPIFY_BOOTSTRAP_DB_PATH", source_db), \
                 patch.object(source_sync, "SHOPIFY_BOOTSTRAP_DB_PATH_FALLBACK", source_db), \
                 patch.object(source_sync, "SHOPIFY_DB_PATH", source_db), \
                 patch.object(source_sync, "KAUFLAND_BOOTSTRAP_DB_PATH", source_db), \
                 patch.object(source_sync, "KAUFLAND_DB_PATH", source_db), \
                 patch.object(source_sync, "BOOKKEEPING_BOOTSTRAP_DB_PATH", source_db), \
                 patch.object(source_sync, "BOOKKEEPING_DB_PATH", target_db):
                summary = source_sync.sync_all_sources(force=True, include_documents=False)

            self.assertEqual(summary["results"]["bookkeeping_db"]["status"], "skipped")
            self.assertIn("disabled for regular source sync", summary["results"]["bookkeeping_db"]["reason"])

    def test_delete_cogs_transaction_does_not_clear_purchase_enrichment(self) -> None:
        combined_db_path = Path(self.temp_dir.name) / "combined.sqlite3"
        original_combined_db = app_db.COMBINED_DB_PATH
        app_db.COMBINED_DB_PATH = combined_db_path
        deleted_payload = {
            "id": "tx-1",
            "document_id": None,
            "source": "api",
            "source_key": "combined:shopify:#1001:cogs",
        }

        try:
            app_db.init_combined_db()
            app_db.upsert_purchase_enrichment(
                marketplace="shopify",
                order_id="shopify-order-1",
                purchase_cost_cents=4200,
                purchase_currency="EUR",
                supplier_name="Supplier A",
                purchase_notes="keep me",
            )

            with patch("app.routers.bookings.delete_bookkeeping_transaction", return_value=deleted_payload):
                response = self.client.delete("/api/bookings/transactions/tx-1", headers=self.admin_headers)

            self.assertEqual(response.status_code, 200)

            with app_db.connect_combined_db() as connection:
                row = connection.execute(
                    "SELECT purchase_cost_cents, supplier_name, purchase_notes FROM order_enrichments WHERE marketplace = ? AND order_id = ?",
                    ("shopify", "shopify-order-1"),
                ).fetchone()

            assert row is not None
            self.assertEqual(row["purchase_cost_cents"], 4200)
            self.assertEqual(row["supplier_name"], "Supplier A")
            self.assertEqual(row["purchase_notes"], "keep me")
        finally:
            app_db.COMBINED_DB_PATH = original_combined_db


if __name__ == "__main__":
    unittest.main()
