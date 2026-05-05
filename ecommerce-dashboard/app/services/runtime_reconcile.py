from __future__ import annotations

import logging
from typing import Any

from app import changestamp
from app.config import ensure_runtime_dirs
from app.db import init_combined_db
from app.services.bookings import (
    migrate_bookkeeping_document_paths,
    sync_combined_orders_into_bookkeeping,
    sync_google_ads_into_bookkeeping,
)


LOGGER = logging.getLogger(__name__)


def _bookkeeping_sync_has_mutation(summary: dict[str, Any] | None) -> bool:
    if not isinstance(summary, dict):
        return False
    for key in (
        "orders_inserted",
        "orders_updated",
        "transactions_inserted",
        "transactions_updated",
        "transactions_deleted",
        "documents_inserted",
        "documents_updated",
    ):
        if int(summary.get(key) or 0) > 0:
            return True
    return False


def reconcile_runtime_state() -> dict[str, Any]:
    errors: list[dict[str, str]] = []
    summary: dict[str, Any] = {
        "ok": False,
        "changestamp_bumped": False,
        "migrated_document_paths": 0,
        "order_sync": None,
        "google_ads_sync": None,
        "errors": errors,
    }
    should_bump = False

    try:
        ensure_runtime_dirs()
    except Exception as exc:  # pragma: no cover - robustness
        LOGGER.exception("runtime directory initialization failed: %s", exc)
        errors.append({"step": "ensure_runtime_dirs", "error": f"{type(exc).__name__}: {exc}"})

    try:
        init_combined_db()
    except Exception as exc:  # pragma: no cover - robustness
        LOGGER.exception("combined database initialization failed: %s", exc)
        errors.append({"step": "init_combined_db", "error": f"{type(exc).__name__}: {exc}"})

    try:
        migrated = migrate_bookkeeping_document_paths()
        summary["migrated_document_paths"] = int(migrated or 0)
        if migrated:
            LOGGER.info("migrated %d bookkeeping document paths to relative", migrated)
    except Exception as exc:  # pragma: no cover - robustness
        LOGGER.exception("bookkeeping document path migration failed: %s", exc)
        errors.append({"step": "migrate_bookkeeping_document_paths", "error": f"{type(exc).__name__}: {exc}"})

    try:
        order_sync = sync_combined_orders_into_bookkeeping()
        summary["order_sync"] = order_sync
        should_bump = _bookkeeping_sync_has_mutation(order_sync) or should_bump
        LOGGER.info(
            "runtime bookkeeping order sync: selected=%s inserted=%s updated=%s",
            order_sync.get("selected_orders"),
            order_sync.get("orders_inserted"),
            order_sync.get("orders_updated"),
        )
    except Exception as exc:  # pragma: no cover - robustness
        LOGGER.exception("runtime bookkeeping order sync failed: %s", exc)
        errors.append({"step": "sync_combined_orders_into_bookkeeping", "error": f"{type(exc).__name__}: {exc}"})

    try:
        gads_sync = sync_google_ads_into_bookkeeping()
        summary["google_ads_sync"] = gads_sync
        should_bump = _bookkeeping_sync_has_mutation(gads_sync) or should_bump
        LOGGER.info(
            "runtime google ads bookkeeping sync: days=%s inserted=%s updated=%s deleted=%s",
            gads_sync.get("days_total"),
            gads_sync.get("transactions_inserted"),
            gads_sync.get("transactions_updated"),
            gads_sync.get("transactions_deleted"),
        )
    except Exception as exc:  # pragma: no cover - robustness
        LOGGER.exception("runtime google ads bookkeeping sync failed: %s", exc)
        errors.append({"step": "sync_google_ads_into_bookkeeping", "error": f"{type(exc).__name__}: {exc}"})

    if should_bump:
        changestamp.bump()
        summary["changestamp_bumped"] = True

    summary["ok"] = not errors
    summary["status"] = "ok" if not errors else "error"
    return summary
