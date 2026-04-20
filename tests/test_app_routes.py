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

    def test_primary_frontend_routes_return_html(self) -> None:
        for path in ["/", "/analytics", "/orders", "/customers", "/bookings", "/google-ads", "/ebay"]:
            with self.subTest(path=path):
                response = self.client.get(path)

                self.assertEqual(response.status_code, 200)
                self.assertIn("text/html", response.headers.get("content-type", ""))

    def test_legacy_dashboard_fallback_route_returns_html(self) -> None:
        for path in ["/legacy", "/legacy?tab=orders", "/legacy?tab=bookings&subtab=transactions&full=1"]:
            with self.subTest(path=path):
                response = self.client.get(path)

                self.assertEqual(response.status_code, 200)
                self.assertIn("text/html", response.headers.get("content-type", ""))

    def test_preview_route_is_available(self) -> None:
        for path in ["/app-preview", "/app-preview/orders", "/app-preview/analytics", "/app-preview/customers", "/app-preview/bookings", "/app-preview/google-ads", "/app-preview/ebay"]:
            with self.subTest(path=path):
                response = self.client.get(path)

                self.assertEqual(response.status_code, 200)
                self.assertIn("text/html", response.headers.get("content-type", ""))


if __name__ == "__main__":
    unittest.main()
