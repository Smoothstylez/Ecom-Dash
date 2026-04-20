from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

os.environ.setdefault("AUTO_SYNC_ON_STARTUP", "0")
os.environ.setdefault("LIVE_SYNC_BACKGROUND_ENABLED", "0")

PROJECT_DIR = Path(__file__).resolve().parent.parent / "ecommerce-dashboard"
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from fastapi.testclient import TestClient

from app.main import app


class ApiSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

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


if __name__ == "__main__":
    unittest.main()
