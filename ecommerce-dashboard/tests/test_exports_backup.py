from __future__ import annotations

from pathlib import Path
from zipfile import ZipFile


def test_full_backup_archive_includes_amazon_database(monkeypatch, tmp_path) -> None:
    import app.services.exports as exports

    amazon_db = tmp_path / "amazon_fba.sqlite3"
    amazon_db.write_bytes(b"sqlite-fake-bytes")
    monkeypatch.setattr(exports, "AMAZON_FBA_DB_PATH", amazon_db)

    archive = exports.create_full_backup_archive()
    try:
        with ZipFile(archive.file_path) as zf:
            names = zf.namelist()
            assert "databases/amazon_fba.sqlite3" in names
    finally:
        Path(archive.file_path).unlink(missing_ok=True)


def test_db_restore_map_includes_amazon_database() -> None:
    import app.services.exports as exports
    from app.config import AMAZON_FBA_DB_PATH

    assert exports._DB_RESTORE_MAP["amazon_fba"] == AMAZON_FBA_DB_PATH


def test_pre_restore_safety_backup_includes_amazon_database(monkeypatch, tmp_path) -> None:
    import app.services.exports as exports

    amazon_db = tmp_path / "amazon_fba.sqlite3"
    amazon_db.write_bytes(b"sqlite-fake-bytes")
    monkeypatch.setattr(exports, "AMAZON_FBA_DB_PATH", amazon_db)
    monkeypatch.setitem(exports._DB_RESTORE_MAP, "amazon_fba", amazon_db)
    monkeypatch.setattr(exports, "DATA_DIR", tmp_path)

    safety_zip_path = exports._create_pre_restore_safety_backup()
    try:
        with ZipFile(safety_zip_path) as zf:
            assert "databases/amazon_fba.sqlite3" in zf.namelist()
    finally:
        safety_zip_path.unlink(missing_ok=True)
