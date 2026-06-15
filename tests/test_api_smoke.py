from __future__ import annotations

import os
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

from app import changestamp
from app.main import app


class ApiSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)
        cls.admin_headers = {"X-Admin-Token": "test-admin-token"}

    def test_health_endpoint(self) -> None:
        response = self.client.get("/api/health")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("status", payload)
        self.assertIn("combined_db", payload)

    def test_analytics_endpoint(self) -> None:
        response = self.client.get("/api/analytics/kpis")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("order_count", payload)
        self.assertIn("revenue_total_cents", payload)

    def test_orders_endpoint(self) -> None:
        response = self.client.get("/api/orders?limit=5")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("items", payload)
        self.assertIn("total", payload)

    def test_order_aliexpress_mappings_endpoints(self) -> None:
        expected_mappings = [
            {
                "id": "map-1",
                "marketplace": "shopify",
                "order_id": "order-1",
                "aliexpress_order_id": "C:3073240755170418",
                "match_status": "matched",
            }
        ]

        with patch(
            "app.routers.orders.fetch_aliexpress_order_mappings_for_marketplace_order",
            return_value=expected_mappings,
        ) as fetch_mock:
            response = self.client.get(
                "/api/orders/shopify/order-1/aliexpress-mappings",
                headers=self.admin_headers,
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["mappings"], expected_mappings)
        fetch_mock.assert_called_once_with(marketplace="shopify", order_id="order-1")

        with patch(
            "app.routers.orders.replace_aliexpress_order_mappings",
            return_value=expected_mappings,
        ) as replace_mock:
            response = self.client.put(
                "/api/orders/shopify/order-1/aliexpress-mappings",
                headers=self.admin_headers,
                json={
                    "mappings": [
                        {
                            "aliexpress_order_id": "C:3073240755170418",
                            "match_status": "matched",
                            "match_confidence": 0.98,
                            "match_method": "address+date+product",
                            "source": "manual",
                            "note": "Confirmed split order",
                        }
                    ]
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["mappings"], expected_mappings)
        replace_mock.assert_called_once()

    def test_bookings_orders_endpoint_forwards_pagination_query(self) -> None:
        expected_payload = {
            "items": [
                {
                    "marketplace": "shopify",
                    "order_id": "order-151",
                    "external_order_id": "#2151",
                }
            ],
            "total": 305,
            "limit": 150,
            "offset": 150,
        }

        with patch("app.routers.bookings.list_booking_orders", return_value=expected_payload) as list_booking_orders_mock:
            response = self.client.get(
                "/api/bookings/orders",
                params={
                    "from": "2026-02-01",
                    "to": "2026-02-29",
                    "marketplace": "shopify",
                    "q": "alice",
                    "limit": 150,
                    "offset": 150,
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), expected_payload)
        list_booking_orders_mock.assert_called_once_with(
            from_date="2026-02-01",
            to_date="2026-02-29",
            marketplace="shopify",
            query="alice",
            limit=150,
            offset=150,
        )

    def test_order_invoice_upload_stores_streamed_file(self) -> None:
        invoice_content = b"%PDF-1.4 invoice"

        with tempfile.TemporaryDirectory() as temp_dir:
            target_path = Path(temp_dir) / "stored-invoice.pdf"

            def fake_create_invoice_document(*, marketplace: str, order_id: str, original_filename: str, stored_filename: str, mime_type: str, file_path: Path):
                self.assertEqual(marketplace, "shopify")
                self.assertEqual(order_id, "order-1")
                self.assertEqual(file_path, target_path)
                self.assertEqual(file_path.read_bytes(), invoice_content)
                self.assertEqual(mime_type, "application/pdf")
                self.assertEqual(stored_filename, target_path.name)
                self.assertTrue(original_filename.endswith(".pdf"))
                return {
                    "purchase_cost_cents": None,
                    "purchase_currency": "EUR",
                    "supplier_name": None,
                    "purchase_notes": None,
                }

            with patch("app.routers.orders.build_invoice_storage_path", return_value=target_path), \
                 patch("app.routers.orders.create_invoice_document", side_effect=fake_create_invoice_document), \
                 patch("app.routers.orders.upsert_purchase_enrichment", return_value={"invoice_document_id": "doc-1"}), \
                 patch("app.routers.orders.sync_combined_orders_into_bookkeeping", return_value={"orders_inserted": 0}):
                response = self.client.post(
                    "/api/orders/shopify/order-1/invoice",
                    headers=self.admin_headers,
                    files={"file": ("invoice.pdf", invoice_content, "application/pdf")},
                )
                stored = target_path.read_bytes()

            self.assertEqual(response.status_code, 200)
            self.assertEqual(stored, invoice_content)

    def test_customers_endpoint(self) -> None:
        response = self.client.get("/api/customers?limit=5")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("items", payload)

    def test_invoice_profile_endpoint(self) -> None:
        with patch("app.routers.invoices.get_seller_profile", return_value={"legal_name": "Demo Shop", "tax_mode": "small_business"}):
            response = self.client.get("/api/invoices/profile")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["profile"]["legal_name"], "Demo Shop")

    def test_invoice_draft_endpoint(self) -> None:
        expected = {
            "invoice": {"invoice_number_preview": "RE-2026-000001"},
            "validation": {"blockers": [], "warnings": [], "ready": True},
        }

        with patch("app.routers.invoices.build_invoice_draft", return_value=expected) as draft_mock:
            response = self.client.get("/api/invoices/draft", params={"marketplace": "shopify", "order_id": "order-1", "template_key": "clean"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), expected)
        draft_mock.assert_called_once_with("shopify", "order-1", "clean")

    def test_invoice_create_endpoint_requires_admin_and_returns_invoice(self) -> None:
        expected_invoice = {"id": "inv-1", "invoice_number": "RE-2026-000001"}

        with patch("app.routers.invoices.create_invoice", return_value=expected_invoice) as create_mock:
            response = self.client.post(
                "/api/invoices",
                headers=self.admin_headers,
                json={"marketplace": "shopify", "order_id": "order-1", "template_key": "clean"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True, "invoice": expected_invoice})
        create_mock.assert_called_once_with("shopify", "order-1", "clean")

    def test_invoice_pdf_download_endpoint(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            pdf_path = Path(temp_dir) / "invoice.pdf"
            pdf_path.write_bytes(b"%PDF-1.4 test invoice")

            with patch("app.routers.invoices.get_invoice_pdf_response_payload", return_value=(pdf_path, "RE-2026-000001.pdf")):
                response = self.client.get("/api/invoices/inv-1/pdf?disposition=inline")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "application/pdf")
        self.assertIn("inline", response.headers.get("content-disposition", ""))

    def test_sync_run_bumps_changestamp_when_data_changes(self) -> None:
        before = changestamp.get()

        with patch("app.routers.sync.sync_all_sources", return_value={
            "results": {
                "shopify_db": {"copied": True, "status": "copied"},
                "kaufland_db": {"copied": False, "status": "up-to-date"},
                "bookkeeping_db": {"copied": False, "status": "skipped"},
                "bookkeeping_documents": {"copied_files": 0, "status": "skipped"},
            }
        }), patch("app.routers.sync.sync_combined_orders_into_bookkeeping", return_value={
            "orders_inserted": 0,
            "orders_updated": 0,
            "transactions_inserted": 0,
            "transactions_updated": 0,
            "transactions_deleted": 0,
            "documents_inserted": 0,
            "documents_updated": 0,
        }):
            response = self.client.post("/api/sync/run", json={"force": False, "include_documents": True}, headers=self.admin_headers)

        self.assertEqual(response.status_code, 200)
        self.assertGreater(changestamp.get(), before)

    def test_sync_run_does_not_bump_changestamp_when_nothing_changes(self) -> None:
        before = changestamp.get()

        with patch("app.routers.sync.sync_all_sources", return_value={
            "results": {
                "shopify_db": {"copied": False, "status": "up-to-date"},
                "kaufland_db": {"copied": False, "status": "up-to-date"},
                "bookkeeping_db": {"copied": False, "status": "skipped"},
                "bookkeeping_documents": {"copied_files": 0, "status": "skipped"},
            }
        }), patch("app.routers.sync.sync_combined_orders_into_bookkeeping", return_value={
            "orders_inserted": 0,
            "orders_updated": 0,
            "transactions_inserted": 0,
            "transactions_updated": 0,
            "transactions_deleted": 0,
            "documents_inserted": 0,
            "documents_updated": 0,
        }):
            response = self.client.post("/api/sync/run", json={"force": False, "include_documents": True}, headers=self.admin_headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(changestamp.get(), before)

    def test_sync_background_trigger_does_not_bump_changestamp_immediately(self) -> None:
        before = changestamp.get()

        with patch("app.routers.sync.trigger_live_sync_background_now", return_value={
            "enabled": True,
            "thread_started": True,
            "thread_alive": True,
            "in_flight": False,
        }), patch("app.routers.sync.build_live_sync_status", return_value={"timestamp": "2026-05-05T20:00:00Z"}):
            response = self.client.post("/api/sync/live/background/trigger", json={"reason": "test"}, headers=self.admin_headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(changestamp.get(), before)

    def test_live_sync_run_bumps_changestamp_when_live_data_changes(self) -> None:
        before = changestamp.get()

        with patch("app.routers.sync.run_live_sync", return_value={
            "results": {
                "shopify": {
                    "status": "success",
                    "summary": {
                        "total_inserted": 2,
                        "total_updated": 0,
                    },
                },
                "kaufland": {
                    "status": "skipped",
                    "summary": {},
                },
            }
        }), patch("app.routers.sync.sync_combined_orders_into_bookkeeping", return_value={
            "orders_inserted": 0,
            "orders_updated": 0,
            "transactions_inserted": 0,
            "transactions_updated": 0,
            "transactions_deleted": 0,
            "documents_inserted": 0,
            "documents_updated": 0,
        }):
            response = self.client.post("/api/sync/live/run", json={"shopify": True, "kaufland": False}, headers=self.admin_headers)

        self.assertEqual(response.status_code, 200)
        self.assertGreater(changestamp.get(), before)

    def test_live_sync_run_does_not_bump_changestamp_when_nothing_changes(self) -> None:
        before = changestamp.get()

        with patch("app.routers.sync.run_live_sync", return_value={
            "results": {
                "shopify": {
                    "status": "success",
                    "summary": {
                        "total_inserted": 0,
                        "total_updated": 0,
                    },
                },
                "kaufland": {
                    "status": "success",
                    "summary": {
                        "orders_saved": 0,
                        "order_units_saved": 0,
                        "returns_saved": 0,
                        "return_units_saved": 0,
                    },
                },
            }
        }), patch("app.routers.sync.sync_combined_orders_into_bookkeeping", return_value={
            "orders_inserted": 0,
            "orders_updated": 0,
            "transactions_inserted": 0,
            "transactions_updated": 0,
            "transactions_deleted": 0,
            "documents_inserted": 0,
            "documents_updated": 0,
        }):
            response = self.client.post("/api/sync/live/run", json={"shopify": True, "kaufland": True}, headers=self.admin_headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(changestamp.get(), before)

    def test_sync_run_requires_admin_auth(self) -> None:
        with patch.dict(os.environ, {"APP_ADMIN_TOKEN": "locked-token"}, clear=False):
            response = self.client.post("/api/sync/run", json={"force": False, "include_documents": True})

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["detail"], "admin auth required")

    def test_sync_run_is_open_when_admin_token_is_not_configured(self) -> None:
        with patch.dict(os.environ, {"APP_ADMIN_TOKEN": ""}, clear=False), \
             patch("app.routers.sync.sync_all_sources", return_value={
                 "results": {
                     "shopify_db": {"copied": False, "status": "up-to-date"},
                     "kaufland_db": {"copied": False, "status": "up-to-date"},
                     "bookkeeping_db": {"copied": False, "status": "skipped"},
                     "bookkeeping_documents": {"copied_files": 0, "status": "skipped"},
                 }
             }), patch("app.routers.sync.sync_combined_orders_into_bookkeeping", return_value={
                 "orders_inserted": 0,
                 "orders_updated": 0,
                 "transactions_inserted": 0,
                 "transactions_updated": 0,
                 "transactions_deleted": 0,
                 "documents_inserted": 0,
                 "documents_updated": 0,
             }):
            response = self.client.post("/api/sync/run", json={"force": False, "include_documents": True})

        self.assertEqual(response.status_code, 200)


if __name__ == "__main__":
    unittest.main()
