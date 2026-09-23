"""API-Tests fuer den monatlichen USt-Report (Task 9+10)."""
from __future__ import annotations

import io
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

from app import db as combined_db
from app.main import app
from app.services import amazon_tax_import, ust_documents, ust_report


class _IsolatedDbMixin:
    """Jeder Test bekommt eine eigene Combined-DB, damit Laeufe reproduzierbar bleiben."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._original_db_path = combined_db.COMBINED_DB_PATH
        combined_db.COMBINED_DB_PATH = Path(self._tmp.name) / "combined.sqlite3"
        combined_db.init_combined_db()
        self.client = TestClient(app)
        self.admin = {"X-Admin-Token": "test-admin-token"}

    def tearDown(self) -> None:
        combined_db.COMBINED_DB_PATH = self._original_db_path
        self._tmp.cleanup()


class UstApiTests(_IsolatedDbMixin, unittest.TestCase):

    def test_report_requires_admin_token(self) -> None:
        response = self.client.get("/api/ust-report", params={"month": "2026-02"})
        self.assertIn(response.status_code, (401, 403))

    def test_get_report_returns_sections_and_totals(self) -> None:
        response = self.client.get("/api/ust-report", params={"month": "2026-02"}, headers=self.admin)
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        for key in ("sections", "totals", "blockers", "warnings", "business_rules"):
            self.assertIn(key, payload)
        self.assertIn("kaufland", payload["sections"])
        self.assertIn("amazon", payload["sections"])
        self.assertIn("input_vat", payload["sections"])

    def test_get_report_rejects_invalid_month(self) -> None:
        response = self.client.get("/api/ust-report", params={"month": "nope"}, headers=self.admin)
        self.assertEqual(response.status_code, 400)

    def test_months_endpoint_lists_entries(self) -> None:
        response = self.client.get("/api/ust-report/months", headers=self.admin)
        self.assertEqual(response.status_code, 200)
        self.assertIsInstance(response.json().get("items"), list)

    def test_file_is_rejected_with_409_when_blocked(self) -> None:
        with patch.object(ust_report, "build_ust_report", return_value={
            "month": "2026-02",
            "blockers": [{"code": "AMAZON_UNRESOLVED", "count": 1, "hint": "x"}],
            "warnings": [],
        }), patch.object(ust_report, "_latest_filed", return_value=None):
            response = self.client.post("/api/ust-report/2026-02/file", headers=self.admin)
        self.assertEqual(response.status_code, 409)

    def test_file_succeeds_when_clean(self) -> None:
        clean = {
            "month": "2026-02", "revision": None, "kind": None, "supersedes_id": None,
            "status": "ready", "settings": {}, "business_rules": {}, "sections": {},
            "totals": {"output_vat_cents": 0, "input_vat_cents": 0, "vat_payable_cents": 0},
            "blockers": [], "warnings": [],
        }
        with patch.object(ust_report, "build_ust_report", return_value=clean), \
             patch.object(ust_report, "_latest_filed", return_value=None), \
             patch.object(ust_report, "_persist_report", return_value={
                 "id": "r1", "month": "2026-02", "revision": 1, "kind": "original",
                 "supersedes_id": None, "status": "filed", "snapshot_json": "{}",
             }):
            response = self.client.post("/api/ust-report/2026-02/file", headers=self.admin)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["report"]["status"], "filed")

    def test_amend_returns_new_revision(self) -> None:
        with patch.object(ust_report, "amend_report", return_value={
            "id": "r2", "month": "2026-02", "revision": 2, "kind": "amendment",
            "supersedes_id": "r1", "status": "filed", "snapshot_json": "{}",
        }):
            response = self.client.post("/api/ust-report/2026-02/amend", headers=self.admin)
        self.assertEqual(response.status_code, 200)
        payload = response.json()["report"]
        self.assertEqual(payload["revision"], 2)
        self.assertEqual(payload["kind"], "amendment")

    def test_document_upload_persists_invoice_and_file(self) -> None:
        with tempfile.TemporaryDirectory() as folder, \
             patch.object(ust_documents, "BOOKKEEPING_DOCUMENTS_DIR", Path(folder)):
            response = self.client.post(
                "/api/ust-report/documents",
                headers=self.admin,
                data={
                    "provider": "kaufland", "doc_type": "fee",
                    "invoice_number": "R0226-23464200", "invoice_date": "2026-03-01",
                    "received_date": "2026-03-01",
                    "period_from": "2026-02-01", "period_to": "2026-02-28",
                    "gross_cents": "231899", "net_cents": "194873",
                    "vat_cents": "37026", "deductible_vat_cents": "37026",
                },
                files={"file": ("rechnung.pdf", io.BytesIO(b"%PDF-1.4 test"), "application/pdf")},
            )
            self.assertEqual(response.status_code, 200)
            invoice = response.json()["invoice"]
            self.assertEqual(invoice["deduction_month"], "2026-03")
            self.assertTrue(invoice["sha256"])

            listed = self.client.get(
                "/api/ust-report/documents", headers=self.admin, params={"month": "2026-03"},
            )
            self.assertEqual(listed.status_code, 200)
            self.assertEqual(listed.json()["total"], 1)

            download = self.client.get(
                f"/api/ust-report/documents/{invoice['id']}/download", headers=self.admin
            )
            self.assertEqual(download.status_code, 200)

    def test_document_upload_rejects_inconsistent_amounts(self) -> None:
        response = self.client.post(
            "/api/ust-report/documents", headers=self.admin,
            data={"provider": "kaufland", "doc_type": "fee", "invoice_number": "X-2",
                  "invoice_date": "2026-03-01", "gross_cents": "100", "net_cents": "50",
                  "vat_cents": "10", "deductible_vat_cents": "0"},
            files={"file": ("a.pdf", io.BytesIO(b"%PDF-1.4"), "application/pdf")},
        )
        self.assertEqual(response.status_code, 400)

    def test_patch_document_edits_received_date_and_status(self) -> None:
        row = ust_documents.save_input_vat_invoice({
            "provider": "kaufland", "doc_type": "fee", "invoice_number": "PATCH-API-1",
            "invoice_date": "2026-08-28", "received_date": "2026-08-28",
            "service_date": "2026-09-03",
            "gross_cents": 100, "net_cents": 84, "vat_cents": 16, "deductible_vat_cents": 16,
        })
        self.assertEqual(row["deduction_month"], "2026-09")
        response = self.client.patch(
            f"/api/ust-report/documents/{row['id']}", headers=self.admin,
            json={"input_vat_status": "confirmed", "received_date": "2026-10-05"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["invoice"]["deduction_month"], "2026-10")

    def test_kaufland_override_endpoint(self) -> None:
        with patch.object(ust_report, "save_kaufland_override", return_value={
            "id_order_unit": "u1", "from_rate": 0.0, "to_rate": 19.0,
            "gross_cents": 26990, "net_cents": 22681, "vat_cents": 4309, "reason": "Falschfeld",
        }):
            response = self.client.post(
                "/api/ust-report/kaufland-overrides", headers=self.admin,
                json={"id_order_unit": "u1", "to_rate": 19.0, "reason": "Falschfeld"},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["override"]["to_rate"], 19.0)

    def test_kaufland_override_requires_reason(self) -> None:
        response = self.client.post(
            "/api/ust-report/kaufland-overrides", headers=self.admin,
            json={"id_order_unit": "u1", "to_rate": 19.0, "reason": ""},
        )
        self.assertEqual(response.status_code, 400)

    def test_kaufland_bulk_override_endpoint(self) -> None:
        with patch.object(ust_report, "bulk_override_zero_rates", return_value={
            "overridden": 37, "month": "2026-02", "to_rate": 19.0,
        }):
            response = self.client.post(
                "/api/ust-report/kaufland-overrides/bulk", headers=self.admin,
                json={"month": "2026-02", "reason": "alle 0-%-Felder", "to_rate": 19.0},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["overridden"], 37)

    def test_settings_validates_eu_tax_regime(self) -> None:
        with patch.object(ust_report, "set_eu_tax_settings", return_value={
            "eu_tax_regime": "home_rate_under_threshold",
            "eu_distance_prior_year_cents": 0, "eu_distance_current_year_cents": 0,
        }):
            response = self.client.post(
                "/api/ust-report/settings", headers=self.admin,
                json={"eu_tax_regime": "home_rate_under_threshold"},
            )
        self.assertEqual(response.status_code, 200)
        bad = self.client.post(
            "/api/ust-report/settings", headers=self.admin, json={"eu_tax_regime": "nonsense"},
        )
        self.assertEqual(bad.status_code, 400)

    def test_refresh_recomputes_without_filing(self) -> None:
        with patch.object(ust_report, "build_ust_report", return_value={
            "month": "2026-02", "status": "ready", "blockers": [], "warnings": [],
        }):
            response = self.client.post("/api/ust-report/2026-02/refresh", headers=self.admin)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["report"]["status"], "ready")


class AmazonTaxReportApiTests(_IsolatedDbMixin, unittest.TestCase):

    def test_request_tax_report_requires_admin(self) -> None:
        response = self.client.post("/api/amazon/tax-report/request")
        self.assertIn(response.status_code, (401, 403))

    def test_request_tax_report_returns_skipped_without_credentials(self) -> None:
        with patch("app.services.amazon_tax_import.request_sc_vat_tax_report",
                   return_value={"status": "skipped", "missing": ["client_id"]}):
            response = self.client.post("/api/amazon/tax-report/request", headers=self.admin)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "skipped")

    def test_import_tax_report_classifies_and_persists(self) -> None:
        with patch("app.services.amazon_tax_import.import_report_document",
                   return_value={"status": "imported", "inserted": 180, "skipped": 0,
                                 "blockers": 0, "warnings": 13}):
            response = self.client.post("/api/amazon/tax-report/r-1/import", headers=self.admin)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["inserted"], 180)

    def test_import_tax_report_reports_pending_status(self) -> None:
        with patch("app.services.amazon_tax_import.import_report_document",
                   return_value={"status": "pending", "report_id": "r-1"}):
            response = self.client.post("/api/amazon/tax-report/r-1/import", headers=self.admin)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "pending")

    def test_legacy_pool_tax_report_still_stores_rows(self) -> None:
        payload = "Transaction ID,Order ID,SKU,Tax Rate\nT1,O1,S1,0.1900\n"
        with patch("app.services.amazon_tax_import.parse_sc_vat_tax_report",
                   return_value=[{"Transaction ID": "T1", "Order ID": "O1", "SKU": "S1"}]), \
             patch("app.services.amazon_tax_import.link_and_inherit", return_value=[]), \
             patch("app.services.amazon_tax_import.import_sc_vat_tax_rows",
                   return_value={"inserted": 0, "skipped": 0, "total": 0}):
            response = self.client.post(
                "/api/amazon/pool/tax-report", headers=self.admin,
                files={"file": ("taxReport.csv", io.BytesIO(payload.encode()), "text/csv")},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["rows"], 1)
