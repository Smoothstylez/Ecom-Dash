from __future__ import annotations

import os
import sys
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

    def test_customers_endpoint(self) -> None:
        response = self.client.get("/api/customers?limit=5")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("items", payload)

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
