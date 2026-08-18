from __future__ import annotations

import logging
import os
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from app import changestamp
from app.services.importers.amazon_sp_api import AmazonSpApiError, _connect, init_amazon_fba_db, sync_amazon_fba


LOGGER = logging.getLogger(__name__)
TASKS = {
    "orders": {"interval_seconds": 300, "lookback_minutes": 20, "scopes": {"include_orders": True, "include_inventory": False, "include_finances": False, "include_inbound": False, "include_settlement_reports": False, "include_catalog_images": False}},
    "finance": {"interval_seconds": 900, "lookback_minutes": 2880, "scopes": {"include_orders": False, "include_inventory": False, "include_finances": True, "include_inbound": False, "include_settlement_reports": False, "include_catalog_images": False}},
    "inventory_inbound": {"interval_seconds": 1800, "lookback_minutes": 20, "scopes": {"include_orders": False, "include_inventory": True, "include_finances": False, "include_inbound": True, "include_settlement_reports": False, "include_catalog_images": False}},
    "reconcile": {"interval_seconds": 86400, "lookback_days": 30, "scopes": {"include_orders": True, "include_inventory": True, "include_finances": True, "include_inbound": True, "include_settlement_reports": True, "include_catalog_images": False}},
}
BACKOFF_SECONDS = (300, 600, 1200, 2400, 3600)

_CYCLE_LOCK = threading.Lock()
_WORKER_STOP = threading.Event()
_WORKER_TRIGGER = threading.Event()
_WORKER_THREAD: threading.Thread | None = None
_WORKER_STATE_LOCK = threading.Lock()
_WORKER_STATE: dict[str, Any] = {"thread_alive": False, "in_flight": False, "pending_trigger_reason": None, "last_cycle": None}
_PROCESS_OWNER_ID = f"{os.getpid()}:{uuid.uuid4()}"


class AmazonAutoRefreshBusyError(RuntimeError):
    pass


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _task_row(task_name: str) -> dict[str, Any]:
    init_amazon_fba_db()
    with _connect() as connection:
        connection.execute("INSERT OR IGNORE INTO amazon_auto_refresh_tasks(task_name) VALUES (?)", (task_name,))
        row = connection.execute("SELECT * FROM amazon_auto_refresh_tasks WHERE task_name = ?", (task_name,)).fetchone()
        connection.commit()
    return dict(row)


def _task_rows() -> dict[str, dict[str, Any]]:
    return {task_name: _task_row(task_name) for task_name in TASKS}


def acquire_database_lease(owner_id: str, now: datetime | None = None) -> bool:
    current = now or _now()
    expires_at = _iso(current + timedelta(minutes=5))
    with _connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT owner_id, expires_at FROM amazon_auto_refresh_lease WHERE lease_name='amazon-sync'"
        ).fetchone()
        expired = row is None or (_parse(str(row["expires_at"])) or current) <= current
        if row is not None and str(row["owner_id"]) != owner_id and not expired:
            connection.rollback()
            return False
        connection.execute(
            "INSERT INTO amazon_auto_refresh_lease(lease_name, owner_id, expires_at) VALUES ('amazon-sync', ?, ?) ON CONFLICT(lease_name) DO UPDATE SET owner_id=excluded.owner_id, expires_at=excluded.expires_at",
            (owner_id, expires_at),
        )
        connection.commit()
    return True


def renew_database_lease(owner_id: str, now: datetime | None = None) -> bool:
    current = now or _now()
    with _connect() as connection:
        cursor = connection.execute(
            "UPDATE amazon_auto_refresh_lease SET expires_at=? WHERE lease_name='amazon-sync' AND owner_id=?",
            (_iso(current + timedelta(minutes=5)), owner_id),
        )
        connection.commit()
    return cursor.rowcount == 1


def release_database_lease(owner_id: str) -> None:
    with _connect() as connection:
        connection.execute(
            "DELETE FROM amazon_auto_refresh_lease WHERE lease_name='amazon-sync' AND owner_id=?", (owner_id,)
        )
        connection.commit()


def initialize_task_state(now: datetime | None = None) -> None:
    current = now or _now()
    reconcile = _task_row("reconcile")
    if reconcile.get("next_eligible_at") is None:
        with _connect() as connection:
            connection.execute(
                "UPDATE amazon_auto_refresh_tasks SET last_status='scheduled', last_success_at=NULL, next_eligible_at=? WHERE task_name='reconcile'",
                (_iso(current + timedelta(seconds=int(TASKS["reconcile"]["interval_seconds"]))),),
            )
            connection.commit()


def select_due_tasks(now: datetime | None = None, *, force: bool = False) -> list[str]:
    current = now or _now()
    due: list[str] = []
    for task_name, config in TASKS.items():
        if force and task_name == "reconcile":
            continue
        state = _task_row(task_name)
        next_eligible = _parse(state.get("next_eligible_at"))
        if state.get("last_status") == "backoff" and next_eligible and next_eligible > current:
            continue
        if force:
            due.append(task_name)
            continue
        if next_eligible and next_eligible > current:
            continue
        last_success = _parse(state.get("last_success_at"))
        if last_success is None or current >= last_success + timedelta(seconds=int(config["interval_seconds"])):
            due.append(task_name)
    return due


def record_task_success(task_name: str, now: datetime | None = None) -> dict[str, Any]:
    current = now or _now()
    next_eligible = _iso(current + timedelta(seconds=int(TASKS[task_name]["interval_seconds"])))
    with _connect() as connection:
        connection.execute(
            """
            INSERT INTO amazon_auto_refresh_tasks(task_name, last_started_at, last_finished_at, last_success_at, last_status, last_error, backoff_level, next_eligible_at)
            VALUES (?, ?, ?, ?, 'success', NULL, 0, ?)
            ON CONFLICT(task_name) DO UPDATE SET
                last_finished_at=excluded.last_finished_at, last_success_at=excluded.last_success_at,
                last_status='success', last_error=NULL, backoff_level=0, next_eligible_at=excluded.next_eligible_at
            """,
            (task_name, _iso(current), _iso(current), _iso(current), next_eligible),
        )
        row = connection.execute("SELECT * FROM amazon_auto_refresh_tasks WHERE task_name = ?", (task_name,)).fetchone()
        connection.commit()
    return dict(row)


def record_task_failure(task_name: str, error: str, now: datetime | None = None) -> dict[str, Any]:
    current = now or _now()
    previous = _task_row(task_name)
    throttle = "SP-API 429" in error or "SP-API 503" in error
    level = min(int(previous.get("backoff_level") or 0) + 1, len(BACKOFF_SECONDS)) if throttle else 0
    backoff = BACKOFF_SECONDS[level - 1] if level else 0
    next_eligible = _iso(current + timedelta(seconds=backoff or int(TASKS[task_name]["interval_seconds"])))
    with _connect() as connection:
        connection.execute(
            """
            UPDATE amazon_auto_refresh_tasks
            SET last_finished_at=?, last_status=?, last_error=?, backoff_level=?, next_eligible_at=?
            WHERE task_name=?
            """,
            (_iso(current), "backoff" if throttle else "error", error[:500], level, next_eligible, task_name),
        )
        row = connection.execute("SELECT * FROM amazon_auto_refresh_tasks WHERE task_name = ?", (task_name,)).fetchone()
        connection.commit()
    result = dict(row)
    result["backoff_seconds"] = backoff
    return result


def _mark_task_started(task_name: str, now: datetime) -> None:
    with _connect() as connection:
        connection.execute(
            "UPDATE amazon_auto_refresh_tasks SET last_started_at=?, last_status='running', last_error=NULL WHERE task_name=?",
            (_iso(now), task_name),
        )
        connection.commit()


def run_amazon_task(task_name: str, now: datetime | None = None) -> dict[str, Any]:
    config = TASKS[task_name]
    summary = sync_amazon_fba(lookback_days=int(config.get("lookback_days") or 1), lookback_minutes=config.get("lookback_minutes"), **config["scopes"])
    errors = summary.get("errors") or []
    throttle_error = next((str(error) for error in errors if "SP-API 429" in str(error) or "SP-API 503" in str(error)), "")
    if throttle_error:
        raise AmazonSpApiError(throttle_error)
    if str(summary.get("status")) == "error":
        raise AmazonSpApiError(str(errors[0] if errors else "Amazon sync failed"))
    changed = any(int(summary.get(key) or 0) > 0 for key in (
        "orders", "inventory_items", "inbound_shipments", "inbound_items", "inbound_costs",
        "modern_transactions", "modern_order_events", "financial_events", "settlement_report_rows",
    ))
    return {"changed": changed, "summary": summary, "status": str(summary.get("status") or "success")}


def run_manual_amazon_sync(**kwargs: Any) -> dict[str, Any]:
    if not _CYCLE_LOCK.acquire(blocking=False):
        raise AmazonAutoRefreshBusyError("Amazon auto refresh is already running")
    owner_id = f"manual:{_PROCESS_OWNER_ID}"
    if not acquire_database_lease(owner_id):
        _CYCLE_LOCK.release()
        raise AmazonAutoRefreshBusyError("Amazon sync is already running in another worker")
    try:
        return sync_amazon_fba(**kwargs)
    finally:
        release_database_lease(owner_id)
        _CYCLE_LOCK.release()


def run_amazon_auto_refresh_cycle(now: datetime | None = None, reason: str = "interval", *, force: bool = False) -> dict[str, Any]:
    if not _CYCLE_LOCK.acquire(blocking=False):
        return {"status": "already_running", "reason": reason, "tasks": {}}
    owner_id = f"auto:{_PROCESS_OWNER_ID}"
    if not acquire_database_lease(owner_id):
        _CYCLE_LOCK.release()
        return {"status": "already_running", "reason": reason, "tasks": {}}
    heartbeat_stop = threading.Event()

    def heartbeat() -> None:
        while not heartbeat_stop.wait(timeout=60):
            if not renew_database_lease(owner_id):
                return

    heartbeat_thread = threading.Thread(target=heartbeat, name="amazon-sync-lease-heartbeat", daemon=True)
    heartbeat_thread.start()
    current = now or _now()
    try:
        with _WORKER_STATE_LOCK:
            _WORKER_STATE["in_flight"] = True
        results: dict[str, Any] = {}
        for task_name in select_due_tasks(current, force=force):
            if not renew_database_lease(owner_id):
                return {"status": "lease_lost", "reason": reason, "tasks": results}
            task_started = _now()
            _mark_task_started(task_name, task_started)
            try:
                result = run_amazon_task(task_name, task_started)
            except AmazonSpApiError as exc:
                state = record_task_failure(task_name, str(exc), _now())
                results[task_name] = {"status": state["last_status"], "error": state["last_error"], "backoff_seconds": state["backoff_seconds"]}
                continue
            except Exception as exc:  # pragma: no cover - worker safety boundary
                state = record_task_failure(task_name, str(exc), _now())
                results[task_name] = {"status": state["last_status"], "error": state["last_error"], "backoff_seconds": state["backoff_seconds"]}
                continue
            record_task_success(task_name, _now())
            results[task_name] = {"status": str(result.get("status") or "success"), "summary": result.get("summary")}
            if result["changed"]:
                changestamp.bump()
        output = {"status": "success", "reason": reason, "tasks": results}
        with _WORKER_STATE_LOCK:
            _WORKER_STATE["last_cycle"] = output
        return output
    finally:
        with _WORKER_STATE_LOCK:
            _WORKER_STATE["in_flight"] = False
        heartbeat_stop.set()
        heartbeat_thread.join(timeout=1.0)
        release_database_lease(owner_id)
        _CYCLE_LOCK.release()


def _config() -> dict[str, Any]:
    def boolean(name: str, default: bool) -> bool:
        return str(os.getenv(name, "1" if default else "0")).strip().lower() in {"1", "true", "yes", "on"}

    try:
        tick = int(os.getenv("AMAZON_AUTO_REFRESH_TICK_SECONDS", "30"))
    except ValueError:
        tick = 30
    return {"enabled": boolean("AMAZON_AUTO_REFRESH_ENABLED", True), "run_on_startup": boolean("AMAZON_AUTO_REFRESH_RUN_ON_STARTUP", True), "tick_seconds": min(max(tick, 10), 300)}


def _worker_loop() -> None:
    first = True
    with _WORKER_STATE_LOCK:
        _WORKER_STATE["thread_alive"] = True
    while not _WORKER_STOP.is_set():
        cfg = _config()
        triggered = _WORKER_TRIGGER.wait(timeout=0 if first and cfg["run_on_startup"] else cfg["tick_seconds"])
        _WORKER_TRIGGER.clear()
        if _WORKER_STOP.is_set():
            break
        first = False
        with _WORKER_STATE_LOCK:
            reason = _WORKER_STATE.pop("pending_trigger_reason", None) or ("trigger" if triggered else "interval")
        if cfg["enabled"]:
            try:
                run_amazon_auto_refresh_cycle(reason=reason, force=reason != "interval")
            except Exception:  # pragma: no cover - worker safety boundary
                LOGGER.exception("Amazon auto refresh cycle failed")
    with _WORKER_STATE_LOCK:
        _WORKER_STATE["thread_alive"] = False


def start_amazon_auto_refresh_worker() -> dict[str, Any]:
    global _WORKER_THREAD
    cfg = _config()
    if not cfg["enabled"]:
        return get_amazon_auto_refresh_status()
    initialize_task_state()
    if _WORKER_THREAD is None or not _WORKER_THREAD.is_alive():
        _WORKER_STOP.clear()
        _WORKER_THREAD = threading.Thread(target=_worker_loop, name="amazon-auto-refresh", daemon=True)
        _WORKER_THREAD.start()
    return get_amazon_auto_refresh_status()


def stop_amazon_auto_refresh_worker(timeout_seconds: float = 5.0) -> dict[str, Any]:
    global _WORKER_THREAD
    _WORKER_STOP.set()
    _WORKER_TRIGGER.set()
    if _WORKER_THREAD is not None:
        _WORKER_THREAD.join(timeout_seconds)
    if _WORKER_THREAD is None or not _WORKER_THREAD.is_alive():
        _WORKER_THREAD = None
    return get_amazon_auto_refresh_status()


def trigger_amazon_auto_refresh_now(reason: str = "api") -> dict[str, Any]:
    with _WORKER_STATE_LOCK:
        _WORKER_STATE["pending_trigger_reason"] = str(reason or "api")
    _WORKER_TRIGGER.set()
    return get_amazon_auto_refresh_status()


def get_amazon_auto_refresh_status() -> dict[str, Any]:
    cfg = _config()
    states = _task_rows()
    for task_name, state in states.items():
        state["interval_seconds"] = TASKS[task_name]["interval_seconds"]
        state["backoff_seconds"] = BACKOFF_SECONDS[int(state.get("backoff_level") or 0) - 1] if int(state.get("backoff_level") or 0) else 0
    with _WORKER_STATE_LOCK:
        worker = dict(_WORKER_STATE)
    return {"enabled": cfg["enabled"], "run_on_startup": cfg["run_on_startup"], "tick_seconds": cfg["tick_seconds"], "thread_alive": bool(_WORKER_THREAD and _WORKER_THREAD.is_alive()), "in_flight": worker["in_flight"], "pending_trigger_reason": worker.get("pending_trigger_reason"), "last_cycle": worker.get("last_cycle"), "tasks": states}
