# Amazon Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the four confirmed production-review defects from
`docs/superpowers/reports/2026-08-19-amazon-production-review.md` (items A, B, C, D):
Amazon database missing from backup/restore, a crashing inbound-cost endpoint,
partial Amazon syncs silently recorded as full success, and a scheduler lock that
can be permanently leaked on a database error.

**Architecture:** Each fix is independent and additive. No existing schema rows are
altered destructively; the inbound-cost fix adds one nullable column via the
existing `PRAGMA table_info` / `ALTER TABLE ADD COLUMN` migration pattern already
used in `init_amazon_fba_db()`. The scheduler fixes only restructure control flow
(try/finally boundaries and a new task-state branch) — no new tables.

**Tech Stack:** Python 3.12, FastAPI, SQLite, pytest.

## Global Constraints

- No destructive schema changes; only additive `ALTER TABLE ADD COLUMN`.
- Preserve all currently passing tests (`63 passed` before this plan).
- Every task ends with its own focused test run before moving to the next task.
- Do not touch unrelated code (no opportunistic refactors).

---

### Task 1: Include the Amazon database in backup, restore, and safety-backup

**Files:**
- Modify: `ecommerce-dashboard/app/services/exports.py:16-31` (imports),
  `:203-214` (`create_full_backup_archive`), `:677-683` (`_DB_RESTORE_MAP`),
  `:824-838` (`_create_pre_restore_safety_backup`)
- Test: `ecommerce-dashboard/tests/test_exports_backup.py` (new file)

**Interfaces:**
- Consumes: `AMAZON_FBA_DB_PATH` from `app.config` (already defined,
  `ecommerce-dashboard/app/config.py:49`).
- Produces: no new public functions; existing `create_full_backup_archive()`,
  `_DB_RESTORE_MAP`, and `_create_pre_restore_safety_backup()` now include an
  `"amazon_fba"` entry alongside `combined`/`shopify`/`kaufland`/`bookkeeping`/`ebay`.

- [ ] **Step 1: Write the failing test**

Create `ecommerce-dashboard/tests/test_exports_backup.py`:

```python
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
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_exports_backup.py -v`

Expected: FAIL — `databases/amazon_fba.sqlite3` not in the archive; `"amazon_fba"`
not a key in `_DB_RESTORE_MAP`.

- [ ] **Step 3: Add the Amazon database to all three maps**

In `ecommerce-dashboard/app/services/exports.py`, add `AMAZON_FBA_DB_PATH` to the
`app.config` import block (`:16-31`), then:

```python
# create_full_backup_archive(), inside db_entries:
db_entries = [
    ("combined", COMBINED_DB_PATH),
    ("shopify", SHOPIFY_DB_PATH),
    ("kaufland", KAUFLAND_DB_PATH),
    ("bookkeeping", BOOKKEEPING_DB_PATH),
    ("ebay", EBAY_DB_PATH),
    ("amazon_fba", AMAZON_FBA_DB_PATH),
]
```

```python
# _DB_RESTORE_MAP
_DB_RESTORE_MAP: dict[str, Path] = {
    "combined": COMBINED_DB_PATH,
    "shopify": SHOPIFY_DB_PATH,
    "kaufland": KAUFLAND_DB_PATH,
    "bookkeeping": BOOKKEEPING_DB_PATH,
    "ebay": EBAY_DB_PATH,
    "amazon_fba": AMAZON_FBA_DB_PATH,
}
```

`_create_pre_restore_safety_backup()` already iterates `_DB_RESTORE_MAP.items()`,
so no separate change is needed there once the map is updated.

- [ ] **Step 4: Run the tests and verify they pass**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_exports_backup.py -v`

Expected: PASS.

- [ ] **Step 5: Run the full backend suite**

Run: `cd ecommerce-dashboard && python3 -m pytest`

Expected: all tests pass (previous count + 3 new).

- [ ] **Step 6: Commit**

```bash
git add ecommerce-dashboard/app/services/exports.py ecommerce-dashboard/tests/test_exports_backup.py
git commit -m "fix: include Amazon FBA database in backup and restore"
```

---

### Task 2: Fix the crashing inbound-cost endpoint (`notes` support)

**Files:**
- Modify: `ecommerce-dashboard/app/services/importers/amazon_sp_api.py:585-593`
  (schema), `:740-832` (migration block, add a new column check)
- Modify: `ecommerce-dashboard/app/services/amazon_fba.py:368-396` (`add_inbound_cost`)
- Test: `ecommerce-dashboard/tests/test_amazon_fba.py` (append new test)

**Interfaces:**
- Produces: `add_inbound_cost(*, shipment_id, cost_type, amount_cents,
  currency="EUR", allocation_method="value", status="manual",
  source_event_id=None, raw_json="{}", notes="")` — adds one new keyword-only
  parameter `notes: str = ""`, persisted in a new `notes TEXT NOT NULL DEFAULT ''`
  column on `amazon_inbound_costs`.
- Consumes: existing `_connect()`, `init_amazon_fba_db()` from `amazon_sp_api.py`.

- [ ] **Step 1: Write the failing test**

Append to `ecommerce-dashboard/tests/test_amazon_fba.py`:

```python
def test_add_inbound_cost_persists_notes(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-NOTES", "ShipmentStatus": "CLOSED"},
            items=[{"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 5, "QuantityReceived": 5}],
        )

    cost = amazon_fba.add_inbound_cost(
        shipment_id="FBA-NOTES",
        cost_type="supplier_product",
        amount_cents=5_000,
        notes="Zollgebuehr fuer Los 3",
    )

    with importer._connect() as connection:
        row = connection.execute("SELECT notes FROM amazon_inbound_costs WHERE id = ?", (cost["id"],)).fetchone()
    assert row["notes"] == "Zollgebuehr fuer Los 3"


def test_inbound_cost_router_request_model_matches_service_signature() -> None:
    import inspect

    from app.routers.amazon import InboundCostRequest
    from app.services.amazon_fba import add_inbound_cost

    service_params = set(inspect.signature(add_inbound_cost).parameters) - {"shipment_id"}
    request_fields = set(InboundCostRequest.model_fields)
    assert request_fields.issubset(service_params), (
        f"InboundCostRequest fields not accepted by add_inbound_cost: {request_fields - service_params}"
    )
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -k inbound_cost_persists_notes -v`
Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -k router_request_model_matches -v`

Expected: FAIL — first test fails with `sqlite3.OperationalError: no such column: notes`
or a `TypeError` from `add_inbound_cost`; second test fails asserting the `notes`
field is missing from `add_inbound_cost`'s parameters.

- [ ] **Step 3: Add the `notes` column via migration**

In `ecommerce-dashboard/app/services/importers/amazon_sp_api.py`, update the
`CREATE TABLE IF NOT EXISTS amazon_inbound_costs` statement to add
`notes TEXT NOT NULL DEFAULT ''` after `raw_json`, and add a migration check
next to the existing shipment/lot column migrations (near line 763):

```python
inbound_cost_columns = {
    str(row[1]) for row in connection.execute("PRAGMA table_info(amazon_inbound_costs)").fetchall()
}
if "notes" not in inbound_cost_columns:
    connection.execute("ALTER TABLE amazon_inbound_costs ADD COLUMN notes TEXT NOT NULL DEFAULT ''")
```

- [ ] **Step 4: Accept and persist `notes` in `add_inbound_cost`**

In `ecommerce-dashboard/app/services/amazon_fba.py`, update the function:

```python
def add_inbound_cost(
    *,
    shipment_id: str,
    cost_type: str,
    amount_cents: int,
    currency: str = "EUR",
    allocation_method: str = "value",
    status: str = "manual",
    source_event_id: Optional[str] = None,
    raw_json: str = "{}",
    notes: str = "",
) -> dict[str, Any]:
    if amount_cents < 0:
        raise ValueError("amount_cents must be non-negative")
    init_amazon_fba_db()
    cost_id = str(uuid.uuid4())
    with _connect() as connection:
        if connection.execute("SELECT 1 FROM amazon_inbound_shipments WHERE shipment_id = ?", (shipment_id,)).fetchone() is None:
            raise ValueError("Amazon FBA shipment not found")
        connection.execute(
            """
            INSERT INTO amazon_inbound_costs(
                id, shipment_id, source_event_id, cost_type, amount_cents,
                currency, status, allocation_method, raw_json, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (cost_id, shipment_id, source_event_id, cost_type.strip(), amount_cents, currency.upper(), status, allocation_method, raw_json, notes.strip()),
        )
        connection.commit()
    return {"id": cost_id, "shipment_id": shipment_id, "amount_cents": amount_cents, "currency": currency.upper(), "status": status}
```

- [ ] **Step 5: Run the tests and verify they pass**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -k "inbound_cost_persists_notes or router_request_model_matches" -v`

Expected: PASS.

- [ ] **Step 6: Run the full backend suite**

Run: `cd ecommerce-dashboard && python3 -m pytest`

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add ecommerce-dashboard/app/services/importers/amazon_sp_api.py ecommerce-dashboard/app/services/amazon_fba.py ecommerce-dashboard/tests/test_amazon_fba.py
git commit -m "fix: persist notes on Amazon inbound costs to fix crashing endpoint"
```

---

### Task 3: Record partial Amazon syncs as `partial`, not `success`

**Files:**
- Modify: `ecommerce-dashboard/app/services/amazon_auto_refresh.py:138-269`
- Test: `ecommerce-dashboard/tests/test_amazon_auto_refresh.py` (append new test)

**Interfaces:**
- Produces: `record_task_partial(task_name: str, error: str, now: datetime | None = None) -> dict[str, Any]`
  — sets `last_status='partial'`, keeps `last_error`, `backoff_level=0`,
  `next_eligible_at = now + TASKS[task_name]["interval_seconds"]`, and does
  **not** modify `last_success_at`.
- Consumes: existing `_connect()`, `_now()`, `_iso()`, `TASKS` in the same module.
- `run_amazon_auto_refresh_cycle` now branches on `result["status"] == "partial"`
  before calling `record_task_success`.

- [ ] **Step 1: Write the failing test**

Append to `ecommerce-dashboard/tests/test_amazon_auto_refresh.py`:

```python
def test_partial_sync_is_recorded_as_partial_not_success(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh

    _configure_db(monkeypatch, tmp_path)
    now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)

    def fake_run(task_name, _now):
        return {
            "changed": True,
            "status": "partial",
            "summary": {"errors": [{"scope": "inventory", "error": "SP-API 500 internal error"}]},
        }

    monkeypatch.setattr(auto_refresh, "run_amazon_task", fake_run)
    result = auto_refresh.run_amazon_auto_refresh_cycle(now)

    assert result["tasks"]["orders"]["status"] == "partial"
    assert "SP-API 500" in result["tasks"]["orders"]["error"]

    state = auto_refresh.get_amazon_auto_refresh_status()["tasks"]["orders"]
    assert state["last_status"] == "partial"
    assert state["backoff_level"] == 0
    assert state["last_success_at"] is None


def test_partial_sync_still_bumps_changestamp(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh
    from app import changestamp

    _configure_db(monkeypatch, tmp_path)
    now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)
    calls = []
    monkeypatch.setattr(changestamp, "bump", lambda: calls.append(1))

    def fake_run(task_name, _now):
        return {"changed": True, "status": "partial", "summary": {"errors": [{"scope": "x", "error": "SP-API 500"}]}}

    monkeypatch.setattr(auto_refresh, "run_amazon_task", fake_run)
    auto_refresh.run_amazon_auto_refresh_cycle(now)

    assert len(calls) == len(auto_refresh.select_due_tasks(now))
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_auto_refresh.py -k partial_sync -v`

Expected: FAIL — `result["tasks"]["orders"]["status"]` is `"success"`, and
`state["last_status"]` is `"success"` with `backoff_level` reset and
`last_success_at` populated (the current, buggy behavior).

- [ ] **Step 3: Add `record_task_partial` and branch on it in the cycle**

In `ecommerce-dashboard/app/services/amazon_auto_refresh.py`, add a new function
right after `record_task_failure` (after line 177):

```python
def record_task_partial(task_name: str, error: str, now: datetime | None = None) -> dict[str, Any]:
    current = now or _now()
    next_eligible = _iso(current + timedelta(seconds=int(TASKS[task_name]["interval_seconds"])))
    with _connect() as connection:
        connection.execute(
            """
            UPDATE amazon_auto_refresh_tasks
            SET last_finished_at=?, last_status='partial', last_error=?, backoff_level=0, next_eligible_at=?
            WHERE task_name=?
            """,
            (_iso(current), error[:500], next_eligible, task_name),
        )
        row = connection.execute("SELECT * FROM amazon_auto_refresh_tasks WHERE task_name = ?", (task_name,)).fetchone()
        connection.commit()
    result = dict(row)
    result["backoff_seconds"] = 0
    return result
```

Then replace the success-handling block inside `run_amazon_auto_refresh_cycle`
(currently lines 255-258):

```python
            record_task_success(task_name, _now())
            results[task_name] = {"status": str(result.get("status") or "success"), "summary": result.get("summary")}
            if result["changed"]:
                changestamp.bump()
```

with:

```python
            if result["status"] == "partial":
                error_items = (result.get("summary") or {}).get("errors") or []
                error_text = "; ".join(str(item.get("error") or item) for item in error_items) or "partial sync"
                state = record_task_partial(task_name, error_text, _now())
                results[task_name] = {"status": state["last_status"], "error": state["last_error"], "summary": result.get("summary")}
            else:
                record_task_success(task_name, _now())
                results[task_name] = {"status": str(result.get("status") or "success"), "summary": result.get("summary")}
            if result["changed"]:
                changestamp.bump()
```

- [ ] **Step 4: Run the tests and verify they pass**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_auto_refresh.py -k partial_sync -v`

Expected: PASS.

- [ ] **Step 5: Run the full auto-refresh test file**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_auto_refresh.py -v`

Expected: all pass, including previously existing tests (no regression in
`test_throttled_task_does_not_block_other_due_tasks` or the backoff tests).

- [ ] **Step 6: Commit**

```bash
git add ecommerce-dashboard/app/services/amazon_auto_refresh.py ecommerce-dashboard/tests/test_amazon_auto_refresh.py
git commit -m "fix: record partial Amazon syncs as partial instead of success"
```

---

### Task 4: Fix scheduler lock leak on lease-acquisition failure

**Files:**
- Modify: `ecommerce-dashboard/app/services/amazon_auto_refresh.py:205-269`
  (`run_manual_amazon_sync`, `run_amazon_auto_refresh_cycle`)
- Test: `ecommerce-dashboard/tests/test_amazon_auto_refresh.py` (append new test)

**Interfaces:**
- No signature changes. `run_manual_amazon_sync(**kwargs)` and
  `run_amazon_auto_refresh_cycle(now=None, reason="interval", *, force=False)`
  keep their existing return/raise contracts; only the internal try/finally
  boundary moves earlier so `_CYCLE_LOCK` is always released.

- [ ] **Step 1: Write the failing test**

Append to `ecommerce-dashboard/tests/test_amazon_auto_refresh.py`:

```python
def test_manual_sync_releases_cycle_lock_when_lease_acquisition_raises(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh

    _configure_db(monkeypatch, tmp_path)

    def raise_lease_error(owner_id, now=None):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(auto_refresh, "acquire_database_lease", raise_lease_error)

    try:
        auto_refresh.run_manual_amazon_sync(include_orders=False)
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected RuntimeError to propagate")

    assert auto_refresh._CYCLE_LOCK.acquire(blocking=False)
    auto_refresh._CYCLE_LOCK.release()


def test_auto_cycle_releases_cycle_lock_when_lease_acquisition_raises(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh

    _configure_db(monkeypatch, tmp_path)

    def raise_lease_error(owner_id, now=None):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(auto_refresh, "acquire_database_lease", raise_lease_error)

    try:
        auto_refresh.run_amazon_auto_refresh_cycle()
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected RuntimeError to propagate")

    assert auto_refresh._CYCLE_LOCK.acquire(blocking=False)
    auto_refresh._CYCLE_LOCK.release()
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_auto_refresh.py -k releases_cycle_lock -v`

Expected: FAIL — `auto_refresh._CYCLE_LOCK.acquire(blocking=False)` returns
`False` because the lock was never released after the raised exception.

- [ ] **Step 3: Move lease acquisition inside the try/finally in both functions**

In `ecommerce-dashboard/app/services/amazon_auto_refresh.py`, replace
`run_manual_amazon_sync` (currently lines 205-216) with:

```python
def run_manual_amazon_sync(**kwargs: Any) -> dict[str, Any]:
    if not _CYCLE_LOCK.acquire(blocking=False):
        raise AmazonAutoRefreshBusyError("Amazon auto refresh is already running")
    owner_id = f"manual:{_PROCESS_OWNER_ID}"
    try:
        if not acquire_database_lease(owner_id):
            raise AmazonAutoRefreshBusyError("Amazon sync is already running in another worker")
        return sync_amazon_fba(**kwargs)
    finally:
        release_database_lease(owner_id)
        _CYCLE_LOCK.release()
```

Replace `run_amazon_auto_refresh_cycle` (currently lines 219-269) with:

```python
def run_amazon_auto_refresh_cycle(now: datetime | None = None, reason: str = "interval", *, force: bool = False) -> dict[str, Any]:
    if not _CYCLE_LOCK.acquire(blocking=False):
        return {"status": "already_running", "reason": reason, "tasks": {}}
    owner_id = f"auto:{_PROCESS_OWNER_ID}"
    heartbeat_stop = threading.Event()
    heartbeat_thread: threading.Thread | None = None
    try:
        if not acquire_database_lease(owner_id):
            return {"status": "already_running", "reason": reason, "tasks": {}}

        def heartbeat() -> None:
            while not heartbeat_stop.wait(timeout=60):
                if not renew_database_lease(owner_id):
                    return

        heartbeat_thread = threading.Thread(target=heartbeat, name="amazon-sync-lease-heartbeat", daemon=True)
        heartbeat_thread.start()
        current = now or _now()
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
            if result["status"] == "partial":
                error_items = (result.get("summary") or {}).get("errors") or []
                error_text = "; ".join(str(item.get("error") or item) for item in error_items) or "partial sync"
                state = record_task_partial(task_name, error_text, _now())
                results[task_name] = {"status": state["last_status"], "error": state["last_error"], "summary": result.get("summary")}
            else:
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
        if heartbeat_thread is not None:
            heartbeat_thread.join(timeout=1.0)
        release_database_lease(owner_id)
        _CYCLE_LOCK.release()
```

(This step supersedes Task 3 Step 3's edit to the same function body — apply
Task 3's `partial` branch as part of this same replacement since both tasks
touch the same function. If executed in order, the code above already includes
the Task 3 change.)

- [ ] **Step 4: Run the tests and verify they pass**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_auto_refresh.py -k releases_cycle_lock -v`

Expected: PASS.

- [ ] **Step 5: Run the full backend suite**

Run: `cd ecommerce-dashboard && python3 -m pytest`

Expected: all tests pass, no regressions.

- [ ] **Step 6: Commit**

```bash
git add ecommerce-dashboard/app/services/amazon_auto_refresh.py ecommerce-dashboard/tests/test_amazon_auto_refresh.py
git commit -m "fix: release scheduler lock when lease acquisition raises"
```

---

### Task 5: Final verification

**Files:** none (verification only)

- [ ] **Step 1: Run the complete backend suite**

Run: `cd ecommerce-dashboard && python3 -m pytest`

Expected: all tests pass (original 63 + 3 backup + 2 inbound-cost + 2 partial +
2 lock-release = 72).

- [ ] **Step 2: Run frontend typecheck and build (no frontend files changed, confirms nothing broke)**

Run: `cd frontend && npm run typecheck && npx vite build`

Expected: no errors.

- [ ] **Step 3: Commit is already done per-task; no additional commit needed.**
