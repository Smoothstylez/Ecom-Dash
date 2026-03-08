from __future__ import annotations

import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import (
    BOOKKEEPING_BOOTSTRAP_DB_PATH,
    BOOKKEEPING_BOOTSTRAP_DOCUMENTS_DIR,
    BOOKKEEPING_DB_PATH,
    BOOKKEEPING_DOCUMENTS_DIR,
    KAUFLAND_BOOTSTRAP_DB_PATH,
    KAUFLAND_DB_PATH,
    SHOPIFY_BOOTSTRAP_DB_PATH,
    SHOPIFY_BOOTSTRAP_DB_PATH_FALLBACK,
    SHOPIFY_DB_PATH,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _should_copy_source_to_target(source: Path, target: Path, *, force: bool = False) -> bool:
    if force:
        return True
    if not target.exists() or not target.is_file():
        return True

    source_stat = source.stat()
    target_stat = target.stat()

    # Keep newer runtime files. This prevents startup bootstrap sync from
    # overwriting data that was fetched live into runtime DBs.
    return int(source_stat.st_mtime) > int(target_stat.st_mtime)


def _atomic_copy(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target.with_suffix(f"{target.suffix}.tmp")
    try:
        shutil.copy2(source, temp_path)
        os.replace(temp_path, target)
    except PermissionError:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass
        shutil.copy2(source, target)


def _sync_db(source: Path, target: Path, force: bool = False) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "source": str(source),
        "target": str(target),
        "copied": False,
        "status": "skipped",
        "reason": "up-to-date",
    }

    if not source.exists() or not source.is_file():
        payload["status"] = "missing_source"
        payload["reason"] = "source database not found"
        return payload

    if not _should_copy_source_to_target(source, target, force=force):
        return payload

    try:
        _atomic_copy(source, target)
    except Exception as exc:
        payload["status"] = "error"
        payload["reason"] = f"{type(exc).__name__}: {exc}"
        return payload

    payload["copied"] = True
    payload["status"] = "copied"
    payload["reason"] = "forced" if force else "source-newer"
    payload["target_bytes"] = target.stat().st_size if target.exists() else None
    return payload


def _iter_files(root: Path) -> list[Path]:
    if not root.exists() or not root.is_dir():
        return []
    return [path for path in root.rglob("*") if path.is_file()]


def _sync_documents_dir(source_dir: Path, target_dir: Path, force: bool = False) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "source": str(source_dir),
        "target": str(target_dir),
        "status": "skipped",
        "copied_files": 0,
        "total_source_files": 0,
    }

    if not source_dir.exists() or not source_dir.is_dir():
        payload["status"] = "missing_source"
        return payload

    source_files = _iter_files(source_dir)
    payload["total_source_files"] = len(source_files)
    copied = 0

    for source_file in source_files:
        rel = source_file.relative_to(source_dir)
        target_file = target_dir / rel
        if not _should_copy_source_to_target(source_file, target_file, force=force):
            continue
        target_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_file, target_file)
        copied += 1

    payload["copied_files"] = copied
    payload["status"] = "copied" if copied > 0 else "up-to-date"
    return payload


def _pick_shopify_bootstrap_source() -> Path:
    if SHOPIFY_BOOTSTRAP_DB_PATH.exists():
        return SHOPIFY_BOOTSTRAP_DB_PATH
    return SHOPIFY_BOOTSTRAP_DB_PATH_FALLBACK


def build_sync_status() -> dict[str, Any]:
    shopify_source = _pick_shopify_bootstrap_source()
    return {
        "runtime": {
            "shopify_db": {"path": str(SHOPIFY_DB_PATH), "exists": SHOPIFY_DB_PATH.exists()},
            "kaufland_db": {"path": str(KAUFLAND_DB_PATH), "exists": KAUFLAND_DB_PATH.exists()},
            "bookkeeping_db": {"path": str(BOOKKEEPING_DB_PATH), "exists": BOOKKEEPING_DB_PATH.exists()},
            "bookkeeping_documents": {
                "path": str(BOOKKEEPING_DOCUMENTS_DIR),
                "exists": BOOKKEEPING_DOCUMENTS_DIR.exists(),
            },
        },
        "bootstrap_sources": {
            "shopify_db": {"path": str(shopify_source), "exists": shopify_source.exists()},
            "kaufland_db": {"path": str(KAUFLAND_BOOTSTRAP_DB_PATH), "exists": KAUFLAND_BOOTSTRAP_DB_PATH.exists()},
            "bookkeeping_db": {"path": str(BOOKKEEPING_BOOTSTRAP_DB_PATH), "exists": BOOKKEEPING_BOOTSTRAP_DB_PATH.exists()},
            "bookkeeping_documents": {
                "path": str(BOOKKEEPING_BOOTSTRAP_DOCUMENTS_DIR),
                "exists": BOOKKEEPING_BOOTSTRAP_DOCUMENTS_DIR.exists(),
            },
        },
    }


def sync_all_sources(*, force: bool = False, include_documents: bool = True) -> dict[str, Any]:
    shopify_source = _pick_shopify_bootstrap_source()
    shopify = _sync_db(shopify_source, SHOPIFY_DB_PATH, force=force)
    kaufland = _sync_db(KAUFLAND_BOOTSTRAP_DB_PATH, KAUFLAND_DB_PATH, force=force)
    bookkeeping = _sync_db(BOOKKEEPING_BOOTSTRAP_DB_PATH, BOOKKEEPING_DB_PATH, force=force)

    documents: dict[str, Any]
    if include_documents:
        documents = _sync_documents_dir(
            BOOKKEEPING_BOOTSTRAP_DOCUMENTS_DIR,
            BOOKKEEPING_DOCUMENTS_DIR,
            force=force,
        )
    else:
        documents = {
            "source": str(BOOKKEEPING_BOOTSTRAP_DOCUMENTS_DIR),
            "target": str(BOOKKEEPING_DOCUMENTS_DIR),
            "status": "skipped",
            "copied_files": 0,
            "total_source_files": 0,
        }

    summary = {
        "timestamp": _utc_now(),
        "force": force,
        "include_documents": include_documents,
        "results": {
            "shopify_db": shopify,
            "kaufland_db": kaufland,
            "bookkeeping_db": bookkeeping,
            "bookkeeping_documents": documents,
        },
        "status": build_sync_status(),
    }
    return summary
