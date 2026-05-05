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


class AppRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

    def test_dashboard_routes_return_html(self) -> None:
        for path in ["/", "/analytics", "/orders", "/customers", "/bookings", "/bookings/full", "/google-ads", "/ebay"]:
            with self.subTest(path=path):
                response = self.client.get(path)

                self.assertEqual(response.status_code, 200)
                self.assertIn("text/html", response.headers.get("content-type", ""))

    def test_preview_routes_redirect_to_main_routes(self) -> None:
        expected_locations = {
            "/app-preview": "/analytics",
            "/app-preview/analytics": "/analytics",
            "/app-preview/orders": "/orders",
            "/app-preview/customers": "/customers",
            "/app-preview/bookings": "/bookings",
            "/app-preview/bookings/full?subtab=transactions": "/bookings/full?subtab=transactions",
            "/app-preview/google-ads": "/google-ads",
            "/app-preview/ebay": "/ebay",
        }
        for path, expected_location in expected_locations.items():
            with self.subTest(path=path):
                response = self.client.get(path, follow_redirects=False)

                self.assertEqual(response.status_code, 307)
                self.assertEqual(response.headers.get("location"), expected_location)


if __name__ == "__main__":
    unittest.main()
