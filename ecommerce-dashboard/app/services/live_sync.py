from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any, Optional

from app.services.importers import (
    build_kaufland_live_status,
    build_shopify_live_status,
    get_kaufland_last_sync_time,
    get_shopify_last_sync_time,
    sync_kaufland_live,
    sync_shopify_live,
)


LOGGER = logging.getLogger(__name__)

_LIVE_SYNC_RUN_LOCK = threading.Lock()
_BACKGROUND_STATE_LOCK = threading.Lock()
_BACKGROUND_STOP_EVENT = threading.Event()
_BACKGROUND_TRIGGER_EVENT = threading.Event()
_BACKGROUND_THREAD: Optional[threading.Thread] = None

_BACKGROUND_STATE: dict[str, Any] = {
    "enabled": False,
    "thread_started": False,
    "thread_name": "",
    "thread_alive": False,
    "interval_seconds": 150,
    "run_on_startup": True,
    "shopify_enabled": True,
    "kaufland_enabled": True,
    "reconcile_every_cycles": 144,
    "cycle_count": 0,
    "in_flight": False,
    "last_attempt_at": None,
    "last_finished_at": None,
    "last_success_at": None,
    "last_status": "never_started",
    "last_mode": None,
    "last_reason": None,
    "last_error": None,
    "pending_trigger_reason": None,
    "last_live_result": None,
    "last_bookkeeping_sync": None,
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _env_bool(env_name: str, default: bool) -> bool:
    raw = str(os.getenv(env_name, "1" if default else "0")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _env_int(env_name: str, default: int, *, min_value: int, max_value: int) -> int:
    raw = str(os.getenv(env_name, str(default))).strip()
    try:
        value = int(raw)
    except ValueError:
        value = default
    return min(max(value, min_value), max_value)


def _env_text(env_name: str, default: str) -> str:
    value = str(os.getenv(env_name, default)).strip()
    return value or default


def _read_background_config() -> dict[str, Any]:
    return {
        "enabled": _env_bool("LIVE_SYNC_BACKGROUND_ENABLED", True),
        "run_on_startup": _env_bool("LIVE_SYNC_BACKGROUND_RUN_ON_STARTUP", True),
        "interval_seconds": _env_int("LIVE_SYNC_INTERVAL_SECONDS", 150, min_value=30, max_value=86_400),
        "shopify_enabled": _env_bool("LIVE_SYNC_BACKGROUND_SHOPIFY", True),
        "kaufland_enabled": _env_bool("LIVE_SYNC_BACKGROUND_KAUFLAND", True),
        "shopify_status": _env_text("LIVE_SYNC_BACKGROUND_SHOPIFY_STATUS", "any"),
        "shopify_page_limit": _env_int("LIVE_SYNC_BACKGROUND_SHOPIFY_PAGE_LIMIT", 100, min_value=1, max_value=250),
        "shopify_delta_max_pages": _env_int("LIVE_SYNC_BACKGROUND_SHOPIFY_DELTA_MAX_PAGES", 4, min_value=1, max_value=20_000),
        "shopify_reconcile_max_pages": _env_int(
            "LIVE_SYNC_BACKGROUND_SHOPIFY_RECONCILE_MAX_PAGES", 500, min_value=1, max_value=20_000
        ),
        "shopify_include_line_items": _env_bool("LIVE_SYNC_BACKGROUND_SHOPIFY_INCLUDE_LINE_ITEMS", True),
        "shopify_include_fulfillments": _env_bool("LIVE_SYNC_BACKGROUND_SHOPIFY_INCLUDE_FULFILLMENTS", True),
        "shopify_include_refunds": _env_bool("LIVE_SYNC_BACKGROUND_SHOPIFY_INCLUDE_REFUNDS", True),
        "shopify_include_transactions": _env_bool("LIVE_SYNC_BACKGROUND_SHOPIFY_INCLUDE_TRANSACTIONS", True),
        "kaufland_storefront": _env_text("LIVE_SYNC_BACKGROUND_KAUFLAND_STOREFRONT", "de"),
        "kaufland_page_limit": _env_int("LIVE_SYNC_BACKGROUND_KAUFLAND_PAGE_LIMIT", 100, min_value=1, max_value=200),
        "kaufland_delta_max_pages": _env_int("LIVE_SYNC_BACKGROUND_KAUFLAND_DELTA_MAX_PAGES", 4, min_value=1, max_value=200_000),
        "kaufland_reconcile_max_pages": _env_int(
            "LIVE_SYNC_BACKGROUND_KAUFLAND_RECONCILE_MAX_PAGES", 5000, min_value=1, max_value=200_000
        ),
        "kaufland_include_returns": _env_bool("LIVE_SYNC_BACKGROUND_KAUFLAND_INCLUDE_RETURNS", True),
        "kaufland_include_order_unit_details": _env_bool("LIVE_SYNC_BACKGROUND_KAUFLAND_INCLUDE_ORDER_UNIT_DETAILS", True),
        "reconcile_every_cycles": _env_int("LIVE_SYNC_BACKGROUND_RECONCILE_EVERY_CYCLES", 144, min_value=1, max_value=10_000),
        "bookkeeping_sync": _env_bool("LIVE_SYNC_BACKGROUND_BOOKKEEPING_SYNC", True),
    }


def _derive_overall_status(results: dict[str, Any]) -> str:
    statuses: list[str] = []
    for payload in results.values():
        if not isinstance(payload, dict):
            continue
        statuses.append(str(payload.get("status") or "").strip().lower())

    if not statuses:
        return "skipped"
    if any(status == "error" for status in statuses):
        if any(status in {"success", "partial"} for status in statuses):
            return "partial"
        return "error"
    if any(status == "partial" for status in statuses):
        return "partial"
    if all(status == "skipped" for status in statuses):
        return "skipped"
    return "success"


def _compact_live_result(payload: dict[str, Any]) -> dict[str, Any]:
    results_raw = payload.get("results")
    results = results_raw if isinstance(results_raw, dict) else {}
    provider_statuses: dict[str, Any] = {}
    for provider, data in results.items():
        if not isinstance(data, dict):
            continue
        summary_raw = data.get("summary")
        summary = summary_raw if isinstance(summary_raw, dict) else {}
        provider_statuses[str(provider)] = {
            "status": data.get("status"),
            "orders_seen": summary.get("orders_seen"),
            "orders_inserted": summary.get("orders_inserted"),
            "orders_updated": summary.get("orders_updated"),
            "orders_unchanged": summary.get("orders_unchanged"),
            "error_count": summary.get("error_count"),
            "updated_at_min": summary.get("updated_at_min"),
            "ts_created_from_iso": summary.get("ts_created_from_iso"),
            "duration_seconds": summary.get("duration_seconds"),
        }

    return {
        "timestamp": payload.get("timestamp"),
        "status": payload.get("status"),
        "requested": payload.get("requested"),
        "providers": provider_statuses,
    }


def _run_live_sync_inner(
    *,
    run_shopify: bool,
    run_kaufland: bool,
    shopify_status: str,
    shopify_page_limit: int,
    shopify_max_pages: int,
    shopify_include_line_items: bool,
    shopify_include_fulfillments: bool,
    shopify_include_refunds: bool,
    shopify_include_transactions: bool,
    shopify_updated_at_min: Optional[str] = None,
    kaufland_storefront: str,
    kaufland_page_limit: int,
    kaufland_max_pages: int,
    kaufland_include_returns: bool,
    kaufland_include_order_unit_details: bool,
    kaufland_ts_created_from_iso: Optional[str] = None,
) -> dict[str, Any]:
    results: dict[str, Any] = {}

    if run_shopify:
        results["shopify"] = sync_shopify_live(
            status=shopify_status,
            page_limit=shopify_page_limit,
            max_pages=shopify_max_pages,
            include_line_items=shopify_include_line_items,
            include_fulfillments=shopify_include_fulfillments,
            include_refunds=shopify_include_refunds,
            include_transactions=shopify_include_transactions,
            updated_at_min=shopify_updated_at_min,
        )
    else:
        results["shopify"] = {"status": "skipped", "provider": "shopify"}

    if run_kaufland:
        results["kaufland"] = sync_kaufland_live(
            storefront=kaufland_storefront,
            page_limit=kaufland_page_limit,
            max_pages=kaufland_max_pages,
            include_returns=kaufland_include_returns,
            include_order_unit_details=kaufland_include_order_unit_details,
            ts_created_from_iso=kaufland_ts_created_from_iso,
        )
    else:
        results["kaufland"] = {"status": "skipped", "provider": "kaufland"}

    return {
        "timestamp": _utc_now(),
        "requested": {
            "shopify": bool(run_shopify),
            "kaufland": bool(run_kaufland),
        },
        "status": _derive_overall_status(results),
        "results": results,
        "live_status": build_live_sync_status(),
    }


def run_live_sync(
    *,
    run_shopify: bool,
    run_kaufland: bool,
    shopify_status: str,
    shopify_page_limit: int,
    shopify_max_pages: int,
    shopify_include_line_items: bool,
    shopify_include_fulfillments: bool,
    shopify_include_refunds: bool,
    shopify_include_transactions: bool,
    shopify_updated_at_min: Optional[str] = None,
    kaufland_storefront: str,
    kaufland_page_limit: int,
    kaufland_max_pages: int,
    kaufland_include_returns: bool,
    kaufland_include_order_unit_details: bool,
    kaufland_ts_created_from_iso: Optional[str] = None,
) -> dict[str, Any]:
    with _LIVE_SYNC_RUN_LOCK:
        return _run_live_sync_inner(
            run_shopify=run_shopify,
            run_kaufland=run_kaufland,
            shopify_status=shopify_status,
            shopify_page_limit=shopify_page_limit,
            shopify_max_pages=shopify_max_pages,
            shopify_include_line_items=shopify_include_line_items,
            shopify_include_fulfillments=shopify_include_fulfillments,
            shopify_include_refunds=shopify_include_refunds,
            shopify_include_transactions=shopify_include_transactions,
            shopify_updated_at_min=shopify_updated_at_min,
            kaufland_storefront=kaufland_storefront,
            kaufland_page_limit=kaufland_page_limit,
            kaufland_max_pages=kaufland_max_pages,
            kaufland_include_returns=kaufland_include_returns,
            kaufland_include_order_unit_details=kaufland_include_order_unit_details,
            kaufland_ts_created_from_iso=kaufland_ts_created_from_iso,
        )


def _background_mode_for_cycle(cycle_index: int, cfg: dict[str, Any]) -> str:
    every = int(cfg.get("reconcile_every_cycles") or 0)
    if every > 0 and cycle_index > 0 and (cycle_index % every) == 0:
        return "reconcile"
    return "delta"


def _run_background_cycle(mode: str, cfg: dict[str, Any]) -> dict[str, Any]:
    shopify_max_pages = int(cfg["shopify_reconcile_max_pages"] if mode == "reconcile" else cfg["shopify_delta_max_pages"])
    kaufland_max_pages = int(cfg["kaufland_reconcile_max_pages"] if mode == "reconcile" else cfg["kaufland_delta_max_pages"])

    # --- Delta mode: read last successful sync timestamps and use as filters ---
    shopify_updated_at_min: Optional[str] = None
    kaufland_ts_created_from_iso: Optional[str] = None

    if mode == "delta":
        if bool(cfg["shopify_enabled"]):
            try:
                shopify_updated_at_min = get_shopify_last_sync_time()
                if shopify_updated_at_min:
                    # When we have a delta anchor, allow more pages (safety cap)
                    # since the result set is already filtered by timestamp
                    shopify_max_pages = max(shopify_max_pages, 100)
                    LOGGER.info("Shopify delta sync: updated_at_min=%s", shopify_updated_at_min)
            except Exception:
                LOGGER.warning("Failed to read Shopify last sync time, falling back to unfiltered delta")

        if bool(cfg["kaufland_enabled"]):
            try:
                kaufland_ts_created_from_iso = get_kaufland_last_sync_time()
                if kaufland_ts_created_from_iso:
                    kaufland_max_pages = max(kaufland_max_pages, 100)
                    LOGGER.info("Kaufland delta sync: ts_created_from_iso=%s", kaufland_ts_created_from_iso)
            except Exception:
                LOGGER.warning("Failed to read Kaufland last sync time, falling back to unfiltered delta")
    else:
        LOGGER.info("Reconcile sync: full pull (no date filter)")

    live_result = run_live_sync(
        run_shopify=bool(cfg["shopify_enabled"]),
        run_kaufland=bool(cfg["kaufland_enabled"]),
        shopify_status=str(cfg["shopify_status"]),
        shopify_page_limit=int(cfg["shopify_page_limit"]),
        shopify_max_pages=shopify_max_pages,
        shopify_include_line_items=bool(cfg["shopify_include_line_items"]),
        shopify_include_fulfillments=bool(cfg["shopify_include_fulfillments"]),
        shopify_include_refunds=bool(cfg["shopify_include_refunds"]),
        shopify_include_transactions=bool(cfg["shopify_include_transactions"]),
        shopify_updated_at_min=shopify_updated_at_min,
        kaufland_storefront=str(cfg["kaufland_storefront"]),
        kaufland_page_limit=int(cfg["kaufland_page_limit"]),
        kaufland_max_pages=kaufland_max_pages,
        kaufland_include_returns=bool(cfg["kaufland_include_returns"]),
        kaufland_include_order_unit_details=bool(cfg["kaufland_include_order_unit_details"]),
        kaufland_ts_created_from_iso=kaufland_ts_created_from_iso,
    )

    bookkeeping_payload: dict[str, Any] = {"ok": False, "error": None, "summary": None}
    if bool(cfg["bookkeeping_sync"]):
        try:
            from app.services.bookings import sync_combined_orders_into_bookkeeping

            bookkeeping_payload["summary"] = sync_combined_orders_into_bookkeeping()
            bookkeeping_payload["ok"] = True
        except Exception as exc:  # pragma: no cover - robustness
            LOGGER.exception("background bookkeeping sync failed: %s", exc)
            bookkeeping_payload["error"] = str(exc)
    else:
        bookkeeping_payload["ok"] = True
        bookkeeping_payload["summary"] = {"status": "skipped"}

    return {
        "live_result": live_result,
        "bookkeeping": bookkeeping_payload,
    }


def _background_worker_loop() -> None:
    first_iteration = True

    with _BACKGROUND_STATE_LOCK:
        _BACKGROUND_STATE["thread_alive"] = True

    while not _BACKGROUND_STOP_EVENT.is_set():
        cfg = _read_background_config()

        with _BACKGROUND_STATE_LOCK:
            _BACKGROUND_STATE["enabled"] = bool(cfg["enabled"])
            _BACKGROUND_STATE["interval_seconds"] = int(cfg["interval_seconds"])
            _BACKGROUND_STATE["run_on_startup"] = bool(cfg["run_on_startup"])
            _BACKGROUND_STATE["shopify_enabled"] = bool(cfg["shopify_enabled"])
            _BACKGROUND_STATE["kaufland_enabled"] = bool(cfg["kaufland_enabled"])
            _BACKGROUND_STATE["reconcile_every_cycles"] = int(cfg["reconcile_every_cycles"])

        if first_iteration and not bool(cfg["run_on_startup"]):
            first_iteration = False
            triggered = _BACKGROUND_TRIGGER_EVENT.wait(timeout=float(cfg["interval_seconds"]))
            _BACKGROUND_TRIGGER_EVENT.clear()
            if _BACKGROUND_STOP_EVENT.is_set():
                break
            reason = "trigger"
            if triggered:
                with _BACKGROUND_STATE_LOCK:
                    pending_reason = str(_BACKGROUND_STATE.get("pending_trigger_reason") or "trigger").strip() or "trigger"
                    _BACKGROUND_STATE["pending_trigger_reason"] = None
                reason = pending_reason
            else:
                reason = "interval"
        elif first_iteration:
            first_iteration = False
            reason = "startup"
        else:
            triggered = _BACKGROUND_TRIGGER_EVENT.wait(timeout=float(cfg["interval_seconds"]))
            _BACKGROUND_TRIGGER_EVENT.clear()
            if _BACKGROUND_STOP_EVENT.is_set():
                break
            reason = "trigger"
            if triggered:
                with _BACKGROUND_STATE_LOCK:
                    pending_reason = str(_BACKGROUND_STATE.get("pending_trigger_reason") or "trigger").strip() or "trigger"
                    _BACKGROUND_STATE["pending_trigger_reason"] = None
                reason = pending_reason
            else:
                reason = "interval"

        attempt_at = _utc_now()
        with _BACKGROUND_STATE_LOCK:
            cycle_index = int(_BACKGROUND_STATE.get("cycle_count") or 0) + 1
        mode = _background_mode_for_cycle(cycle_index, cfg)

        with _BACKGROUND_STATE_LOCK:
            _BACKGROUND_STATE["in_flight"] = True
            _BACKGROUND_STATE["last_attempt_at"] = attempt_at
            _BACKGROUND_STATE["last_mode"] = mode
            _BACKGROUND_STATE["last_reason"] = reason
            _BACKGROUND_STATE["last_error"] = None

        try:
            cycle_result = _run_background_cycle(mode, cfg)
            live_result = cycle_result["live_result"]
            bookkeeping = cycle_result["bookkeeping"]
            live_status = str(live_result.get("status") or "error").lower()
            bookkeeping_ok = bool(bookkeeping.get("ok"))
            bookkeeping_error = bookkeeping.get("error")

            status = live_status
            if not bookkeeping_ok:
                status = "partial" if live_status in {"success", "partial", "skipped"} else "error"

            finished_at = _utc_now()

            with _BACKGROUND_STATE_LOCK:
                _BACKGROUND_STATE["cycle_count"] = cycle_index
                _BACKGROUND_STATE["in_flight"] = False
                _BACKGROUND_STATE["last_finished_at"] = finished_at
                _BACKGROUND_STATE["last_status"] = status
                _BACKGROUND_STATE["last_live_result"] = _compact_live_result(live_result)
                _BACKGROUND_STATE["last_bookkeeping_sync"] = bookkeeping.get("summary")
                _BACKGROUND_STATE["last_error"] = str(bookkeeping_error) if bookkeeping_error else None
                if status in {"success", "partial", "skipped"} and not bookkeeping_error:
                    _BACKGROUND_STATE["last_success_at"] = finished_at
        except Exception as exc:  # pragma: no cover - robustness
            LOGGER.exception("background live sync cycle failed: %s", exc)
            with _BACKGROUND_STATE_LOCK:
                _BACKGROUND_STATE["cycle_count"] = cycle_index
                _BACKGROUND_STATE["in_flight"] = False
                _BACKGROUND_STATE["last_finished_at"] = _utc_now()
                _BACKGROUND_STATE["last_status"] = "error"
                _BACKGROUND_STATE["last_error"] = str(exc)

    with _BACKGROUND_STATE_LOCK:
        _BACKGROUND_STATE["thread_started"] = False
        _BACKGROUND_STATE["thread_alive"] = False
        _BACKGROUND_STATE["in_flight"] = False


def start_live_sync_background_worker() -> dict[str, Any]:
    global _BACKGROUND_THREAD

    cfg = _read_background_config()
    with _BACKGROUND_STATE_LOCK:
        _BACKGROUND_STATE["enabled"] = bool(cfg["enabled"])
        _BACKGROUND_STATE["interval_seconds"] = int(cfg["interval_seconds"])
        _BACKGROUND_STATE["run_on_startup"] = bool(cfg["run_on_startup"])
        _BACKGROUND_STATE["shopify_enabled"] = bool(cfg["shopify_enabled"])
        _BACKGROUND_STATE["kaufland_enabled"] = bool(cfg["kaufland_enabled"])
        _BACKGROUND_STATE["reconcile_every_cycles"] = int(cfg["reconcile_every_cycles"])

    if not bool(cfg["enabled"]):
        with _BACKGROUND_STATE_LOCK:
            _BACKGROUND_STATE["thread_started"] = False
            _BACKGROUND_STATE["thread_alive"] = False
        return build_live_sync_background_status()

    already_running = False
    thread: Optional[threading.Thread] = None
    with _BACKGROUND_STATE_LOCK:
        already_running = _BACKGROUND_THREAD is not None and _BACKGROUND_THREAD.is_alive()
        if not already_running:
            _BACKGROUND_STOP_EVENT.clear()
            _BACKGROUND_TRIGGER_EVENT.clear()
            thread = threading.Thread(target=_background_worker_loop, name="combined-live-sync-worker", daemon=True)
            _BACKGROUND_THREAD = thread
            _BACKGROUND_STATE["thread_started"] = True
            _BACKGROUND_STATE["thread_name"] = thread.name
            _BACKGROUND_STATE["thread_alive"] = False

    if already_running:
        return build_live_sync_background_status()

    assert thread is not None
    thread.start()
    return build_live_sync_background_status()


def stop_live_sync_background_worker(timeout_seconds: float = 5.0) -> dict[str, Any]:
    global _BACKGROUND_THREAD

    _BACKGROUND_STOP_EVENT.set()
    _BACKGROUND_TRIGGER_EVENT.set()

    thread = _BACKGROUND_THREAD
    if thread is not None and thread.is_alive():
        thread.join(timeout=timeout_seconds)

    with _BACKGROUND_STATE_LOCK:
        _BACKGROUND_STATE["thread_started"] = False
        _BACKGROUND_STATE["thread_alive"] = bool(thread is not None and thread.is_alive())

    if thread is None or not thread.is_alive():
        _BACKGROUND_THREAD = None

    return build_live_sync_background_status()


def trigger_live_sync_background_now(reason: str = "api") -> dict[str, Any]:
    normalized_reason = str(reason or "api").strip() or "api"
    with _BACKGROUND_STATE_LOCK:
        _BACKGROUND_STATE["pending_trigger_reason"] = normalized_reason
    _BACKGROUND_TRIGGER_EVENT.set()
    return build_live_sync_background_status()


def build_live_sync_background_status() -> dict[str, Any]:
    cfg = _read_background_config()
    with _BACKGROUND_STATE_LOCK:
        state = dict(_BACKGROUND_STATE)

    thread = _BACKGROUND_THREAD
    thread_alive = bool(thread is not None and thread.is_alive())

    cycle_count = int(state.get("cycle_count") or 0)
    reconcile_every = int(cfg["reconcile_every_cycles"])
    if reconcile_every > 0 and cycle_count > 0:
        next_reconcile_in = reconcile_every - (cycle_count % reconcile_every)
    else:
        next_reconcile_in = reconcile_every

    return {
        "enabled": bool(cfg["enabled"]),
        "thread_started": bool(state.get("thread_started")),
        "thread_alive": thread_alive,
        "thread_name": state.get("thread_name"),
        "in_flight": bool(state.get("in_flight")),
        "interval_seconds": int(cfg["interval_seconds"]),
        "run_on_startup": bool(cfg["run_on_startup"]),
        "shopify_enabled": bool(cfg["shopify_enabled"]),
        "kaufland_enabled": bool(cfg["kaufland_enabled"]),
        "shopify_page_limit": int(cfg["shopify_page_limit"]),
        "kaufland_page_limit": int(cfg["kaufland_page_limit"]),
        "shopify_delta_max_pages": int(cfg["shopify_delta_max_pages"]),
        "shopify_reconcile_max_pages": int(cfg["shopify_reconcile_max_pages"]),
        "kaufland_delta_max_pages": int(cfg["kaufland_delta_max_pages"]),
        "kaufland_reconcile_max_pages": int(cfg["kaufland_reconcile_max_pages"]),
        "reconcile_every_cycles": reconcile_every,
        "bookkeeping_sync": bool(cfg["bookkeeping_sync"]),
        "cycle_count": cycle_count,
        "next_reconcile_in_cycles": next_reconcile_in,
        "last_attempt_at": state.get("last_attempt_at"),
        "last_finished_at": state.get("last_finished_at"),
        "last_success_at": state.get("last_success_at"),
        "last_status": state.get("last_status"),
        "last_mode": state.get("last_mode"),
        "last_reason": state.get("last_reason"),
        "last_error": state.get("last_error"),
        "last_live_result": state.get("last_live_result"),
        "last_bookkeeping_sync": state.get("last_bookkeeping_sync"),
    }


def build_live_sync_status() -> dict[str, Any]:
    return {
        "timestamp": _utc_now(),
        "providers": {
            "shopify": build_shopify_live_status(),
            "kaufland": build_kaufland_live_status(),
        },
        "background": build_live_sync_background_status(),
    }
