from __future__ import annotations

import io
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from zipfile import ZIP_DEFLATED, ZipFile

os.environ.setdefault("AUTO_SYNC_ON_STARTUP", "0")
os.environ.setdefault("LIVE_SYNC_BACKGROUND_ENABLED", "0")
os.environ.setdefault("APP_ADMIN_TOKEN", "test-admin-token")

PROJECT_DIR = Path(__file__).resolve().parent.parent / "ecommerce-dashboard"
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from fastapi.testclient import TestClient

from app.main import app
from app.routers import exports as exports_router
from app.services import exports as exports_service


def _build_full_backup_zip(entries: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with ZipFile(buffer, mode="w", compression=ZIP_DEFLATED) as zf:
        zf.writestr(
            "manifest.json",
            (
                '{"kind":"full_backup","generated_at":"2026-05-05T00:00:00Z",'
                '"app_version":"0.3.0","schema_version":1}'
            ).encode("utf-8"),
        )
        for name, content in entries.items():
            zf.writestr(name, content)
    return buffer.getvalue()


class RestoreSecurityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)
        cls.admin_headers = {"X-Admin-Token": "test-admin-token"}

    def test_restore_archive_rejects_path_traversal(self) -> None:
        payload = _build_full_backup_zip(
            {
                "storage/invoices/../../evil.txt": b"oops",
                "databases/combined.sqlite3": b"not-a-real-db",
            }
        )

        with tempfile.NamedTemporaryFile(suffix=".zip") as temp_file:
            temp_file.write(payload)
            temp_file.flush()

            result = exports_service.restore_from_backup_archive(Path(temp_file.name))

        self.assertFalse(result.success)
        self.assertIn("unsicheren Pfad", str(result.summary.get("error", "")))

    def test_restore_endpoint_rejects_oversized_upload(self) -> None:
        original_limit = exports_router.MAX_RESTORE_UPLOAD_BYTES
        try:
            exports_router.MAX_RESTORE_UPLOAD_BYTES = 16
            oversized = b"x" * 17
            response = self.client.post(
                "/api/exports/restore",
                files={"file": ("backup.zip", oversized, "application/zip")},
                headers=self.admin_headers,
            )
        finally:
            exports_router.MAX_RESTORE_UPLOAD_BYTES = original_limit

        self.assertEqual(response.status_code, 413)
        self.assertEqual(response.json()["detail"], "backup file too large")

    def test_restore_archive_rejects_oversized_uncompressed_payload(self) -> None:
        original_limit = exports_service.MAX_RESTORE_TOTAL_UNCOMPRESSED_BYTES
        try:
            exports_service.MAX_RESTORE_TOTAL_UNCOMPRESSED_BYTES = 32
            payload = _build_full_backup_zip(
                {
                    "storage/documents/sample.txt": b"a" * 64,
                }
            )
            with tempfile.NamedTemporaryFile(suffix=".zip") as temp_file:
                temp_file.write(payload)
                temp_file.flush()

                result = exports_service.restore_from_backup_archive(Path(temp_file.name))
        finally:
            exports_service.MAX_RESTORE_TOTAL_UNCOMPRESSED_BYTES = original_limit

        self.assertFalse(result.success)
        self.assertIn("zu gross nach dem Entpacken", str(result.summary.get("error", "")))

    def test_restore_archive_validation_happens_before_safety_backup(self) -> None:
        payload = _build_full_backup_zip(
            {
                "storage/documents/../../escape.txt": b"bad",
            }
        )

        with tempfile.NamedTemporaryFile(suffix=".zip") as temp_file:
            temp_file.write(payload)
            temp_file.flush()

            with patch("app.services.exports._create_pre_restore_safety_backup") as safety_backup:
                result = exports_service.restore_from_backup_archive(Path(temp_file.name))

        self.assertFalse(result.success)
        safety_backup.assert_not_called()

    def test_restore_runs_runtime_reconcile_after_successful_restore(self) -> None:
        payload = _build_full_backup_zip({})

        with tempfile.NamedTemporaryFile(suffix=".zip") as temp_file:
            temp_file.write(payload)
            temp_file.flush()

            with patch("app.services.exports._create_pre_restore_safety_backup", return_value=Path("/tmp/safety.zip")), \
                 patch("app.services.exports._restore_databases_from_zip", return_value={}), \
                 patch("app.services.exports._restore_storage_from_zip", return_value={}), \
                 patch("app.services.exports.reconcile_runtime_state", return_value={"ok": True, "status": "ok"}) as reconcile_runtime_state, \
                 patch("app.services.live_sync.stop_live_sync_background_worker"), \
                 patch("app.services.live_sync.start_live_sync_background_worker"):
                result = exports_service.restore_from_backup_archive(Path(temp_file.name))

        self.assertTrue(result.success)
        reconcile_runtime_state.assert_called_once()

    def test_restore_requires_admin_auth(self) -> None:
        with patch.dict(os.environ, {"APP_ADMIN_TOKEN": "locked-token"}, clear=False):
            response = self.client.post(
                "/api/exports/restore",
                files={"file": ("backup.zip", b"x", "application/zip")},
            )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["detail"], "admin auth required")


if __name__ == "__main__":
    unittest.main()
