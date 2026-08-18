# Amazon Quota-Safe Auto Refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep Amazon operational data current through independent, quota-safe scheduled delta syncs and refresh open dashboard views when data changes.

**Architecture:** Add a dedicated Amazon scheduler in `app/services/amazon_auto_refresh.py` with its own thread, task state, lock, and persistent task-state table in the Amazon source database. It invokes existing idempotent Amazon import functions using bounded lookback windows; a 429 or 503 delays only the failed task. The existing changestamp client polling remains the browser refresh mechanism.

**Tech Stack:** Python 3.12, FastAPI, SQLite, threading, Amazon SP-API, React, TypeScript, Vite, pytest.

## Global Constraints

- Preserve existing Tailscale Serve configuration and dashboard ports.
- Do not change existing Shopify/Kaufland worker cadence or behavior.
- Do not automatically run a 730-day Amazon history import.
- Never manufacture missing Amazon financial data for pending orders.
- Keep all Amazon SP-API secrets out of status responses, logs, and UI.
- Orders run every 5 minutes with a 20-minute overlap.
- Finance runs every 15 minutes with a 48-hour lookback.
- Inventory and FBA inbound run every 30 minutes.
- A 30-day reconciliation runs at most once per 24 hours.
- A 429 or 503 backoff is 5, 10, 20, 40, then 60 minutes maximum per affected task.

---

### Task 1: Add Persistent Amazon Scheduler State and Task Selection

**Files:**
- Modify: `ecommerce-dashboard/app/services/importers/amazon_sp_api.py`
- Create: `ecommerce-dashboard/app/services/amazon_auto_refresh.py`
- Create: `ecommerce-dashboard/tests/test_amazon_auto_refresh.py`

**Interfaces:**
- Produces `AmazonAutoRefreshTask = Literal['orders', 'finance', 'inventory_inbound', 'reconcile']`.
- Produces `get_amazon_auto_refresh_status() -> dict[str, Any]`.
- Produces `run_amazon_auto_refresh_cycle(now: datetime | None = None, reason: str = 'interval') -> dict[str, Any]`.
- Persists `amazon_auto_refresh_tasks` rows keyed by task name with `last_started_at`, `last_finished_at`, `last_success_at`, `last_status`, `last_error`, `backoff_level`, and `next_eligible_at`.

- [ ] **Step 1: Write failing task-due and persistent-state tests.**

```python
def test_orders_task_is_due_every_five_minutes(tmp_path, monkeypatch):
    configure_db(monkeypatch, tmp_path)
    now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)
    assert select_due_tasks(now) == ['orders', 'finance', 'inventory_inbound', 'reconcile']
    record_task_success('orders', now)
    assert 'orders' not in select_due_tasks(now + timedelta(minutes=4, seconds=59))
    assert 'orders' in select_due_tasks(now + timedelta(minutes=5))

def test_reconcile_is_not_due_again_until_24_hours(tmp_path, monkeypatch):
    configure_db(monkeypatch, tmp_path)
    now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)
    record_task_success('reconcile', now)
    assert 'reconcile' not in select_due_tasks(now + timedelta(hours=23, minutes=59))
    assert 'reconcile' in select_due_tasks(now + timedelta(hours=24))
```

- [ ] **Step 2: Run the focused tests and verify they fail because the scheduler module and table do not exist.**

Run: `python3 -m pytest tests/test_amazon_auto_refresh.py -k 'due' -v`

Expected: FAIL with an import or missing function error.

- [ ] **Step 3: Add the schema and scheduler state helpers.**

Add `amazon_auto_refresh_tasks` in `init_amazon_fba_db()` with a primary-key task name. In `amazon_auto_refresh.py`, define immutable task configuration:

```python
TASKS = {
    'orders': {'interval_seconds': 300, 'lookback_seconds': 1200},
    'finance': {'interval_seconds': 900, 'lookback_seconds': 172800},
    'inventory_inbound': {'interval_seconds': 1800, 'lookback_seconds': None},
    'reconcile': {'interval_seconds': 86400, 'lookback_seconds': 2592000},
}
```

Implement ISO timestamp parsing, task-state reads/writes, and `select_due_tasks(now)` which excludes tasks whose `next_eligible_at` is in the future.

- [ ] **Step 4: Run the focused tests and verify task scheduling passes.**

Run: `python3 -m pytest tests/test_amazon_auto_refresh.py -k 'due' -v`

Expected: PASS.

### Task 2: Run Isolated Delta Tasks With Backoff and Changelog Updates

**Files:**
- Modify: `ecommerce-dashboard/app/services/amazon_auto_refresh.py`
- Modify: `ecommerce-dashboard/app/services/importers/amazon_sp_api.py`
- Test: `ecommerce-dashboard/tests/test_amazon_auto_refresh.py`

**Interfaces:**
- `run_amazon_auto_refresh_cycle` serializes execution with a dedicated `_AMAZON_AUTO_REFRESH_LOCK`.
- `run_amazon_task(task_name, now) -> dict[str, Any]` invokes existing SP-API import work with bounded scopes.
- On changed data, `app.changestamp.bump()` is invoked once after each completed task.
- On `AmazonSpApiError` that includes `SP-API 429` or `SP-API 503`, the failed task gets the next backoff duration and all other due tasks continue.

- [ ] **Step 1: Write failing tests for independent tasks, backoff, and coalescing.**

```python
def test_inventory_throttle_does_not_block_orders(monkeypatch, tmp_path):
    configure_db(monkeypatch, tmp_path)
    monkeypatch.setattr(module, 'run_amazon_task', fake_task_that_throttles_inventory)
    result = module.run_amazon_auto_refresh_cycle(fixed_now)
    assert result['tasks']['orders']['status'] == 'success'
    assert result['tasks']['inventory_inbound']['status'] == 'backoff'

def test_backoff_grows_and_success_resets_it(monkeypatch, tmp_path):
    configure_db(monkeypatch, tmp_path)
    assert record_task_failure('finance', 'SP-API 429', fixed_now)['backoff_seconds'] == 300
    assert record_task_failure('finance', 'SP-API 429', fixed_now)['backoff_seconds'] == 600
    assert record_task_success('finance', fixed_now)['backoff_level'] == 0

def test_concurrent_trigger_is_coalesced(monkeypatch, tmp_path):
    configure_db(monkeypatch, tmp_path)
    acquire_scheduler_lock_for_test()
    assert module.run_amazon_auto_refresh_cycle(fixed_now)['status'] == 'already_running'
```

- [ ] **Step 2: Run the focused tests and verify they fail.**

Run: `python3 -m pytest tests/test_amazon_auto_refresh.py -k 'throttle or backoff or coalesced' -v`

Expected: FAIL because task execution and backoff do not yet exist.

- [ ] **Step 3: Implement bounded task calls without using `sync_amazon_fba` for every interval.**

Use the existing `AmazonSpApiClient`, `_upsert_order`, `_upsert_order_items`, `sync_modern_financial_transactions`, `sync_modern_inbound_costs`, `sync_inbound_shipments`, and `_upsert_inventory_snapshot` functions. Implement each task exactly as follows:

```python
# orders: now - 20 minutes; order items and Catalog requests only for
# new/order items missing image_url.
# finance: now - 48 hours; legacy events, settlements, modern transactions.
# inventory_inbound: all current inventory pages and account-level inbound shipments.
# reconcile: orders and finance with a 30-day lookback, then inventory/inbound.
```

Do not call settlement-report creation/import in ordinary delta tasks. It remains part of reconciliation only and must reuse existing report idempotency.

- [ ] **Step 4: Implement throttling and generic failures.**

Detect `SP-API 429` and `SP-API 503` from `AmazonSpApiError`. Set `next_eligible_at` to 5/10/20/40/60 minutes based on `backoff_level`. For other exceptions, record `last_error` but leave the normal interval unchanged. Continue processing other selected tasks.

- [ ] **Step 5: Run focused task tests.**

Run: `python3 -m pytest tests/test_amazon_auto_refresh.py -k 'throttle or backoff or coalesced' -v`

Expected: PASS.

### Task 3: Start and Observe the Dedicated Amazon Worker

**Files:**
- Modify: `ecommerce-dashboard/app/services/amazon_auto_refresh.py`
- Modify: `ecommerce-dashboard/app/main.py`
- Modify: `ecommerce-dashboard/app/routers/amazon.py`
- Modify: `docs/dashboard-backend-api.md`
- Test: `ecommerce-dashboard/tests/test_amazon_auto_refresh.py`

**Interfaces:**
- Produces `start_amazon_auto_refresh_worker()`, `stop_amazon_auto_refresh_worker(timeout_seconds=5.0)`, `trigger_amazon_auto_refresh_now(reason='api')`, and `get_amazon_auto_refresh_status()`.
- Extends `GET /api/amazon/status` with `auto_refresh`.
- Adds admin-only `POST /api/amazon/auto-refresh/trigger` accepting `{"reason": "manual"}`.

- [ ] **Step 1: Write failing worker lifecycle and trigger tests.**

```python
def test_worker_status_reports_scheduler_state(monkeypatch, tmp_path):
    configure_db(monkeypatch, tmp_path)
    status = module.get_amazon_auto_refresh_status()
    assert status['enabled'] is True
    assert set(status['tasks']) == {'orders', 'finance', 'inventory_inbound', 'reconcile'}

def test_manual_trigger_uses_same_non_overlapping_cycle(monkeypatch, tmp_path):
    configure_db(monkeypatch, tmp_path)
    monkeypatch.setattr(module, 'run_amazon_auto_refresh_cycle', fake_cycle)
    result = module.trigger_amazon_auto_refresh_now('api')
    assert result['pending_trigger_reason'] == 'api'
```

- [ ] **Step 2: Run the focused lifecycle tests and verify they fail.**

Run: `python3 -m pytest tests/test_amazon_auto_refresh.py -k 'worker_status or manual_trigger' -v`

Expected: FAIL with missing worker APIs.

- [ ] **Step 3: Implement the daemon worker and FastAPI lifecycle.**

Read `AMAZON_AUTO_REFRESH_ENABLED` (default true), `AMAZON_AUTO_REFRESH_RUN_ON_STARTUP` (default true), and `AMAZON_AUTO_REFRESH_TICK_SECONDS` (default 30, bounded 10 to 300). Start the worker in `on_startup`, stop it in `on_shutdown`, and leave `live_sync.py` unchanged. The worker wakes each tick, processes due tasks, and handles one coalesced manual trigger.

- [ ] **Step 4: Add status and trigger API endpoints.**

Extend `api_amazon_status` with `auto_refresh=get_amazon_auto_refresh_status()`. Add a Pydantic `AmazonAutoRefreshTriggerRequest` containing optional `reason`, and make the trigger endpoint admin-only. Document both response fields and route in `docs/dashboard-backend-api.md`.

- [ ] **Step 5: Run worker tests.**

Run: `python3 -m pytest tests/test_amazon_auto_refresh.py -k 'worker_status or manual_trigger' -v`

Expected: PASS.

### Task 4: Display Amazon Refresh Health and Reuse Existing Client Reloading

**Files:**
- Modify: `frontend/src/features/amazon/amazon-page.tsx`
- Modify: `frontend/src/features/orders/orders-page.tsx` only if it does not already reload on `refreshRequestToken`.
- Test: `frontend/src/features/amazon/amazon-page.test.tsx` if the repository has frontend test support; otherwise verify through TypeScript build.

**Interfaces:**
- Amazon status response includes `auto_refresh.tasks.orders|finance|inventory_inbound|reconcile` with last result, next eligible time, and backoff.
- `AmazonPage` reloads its status and data on `refreshRequestToken` from `useDashboardShellState()`.

- [ ] **Step 1: Add the expanded frontend types and a status formatter.**

```ts
type AmazonAutoRefreshTask = {
  last_status?: string;
  last_success_at?: string | null;
  next_eligible_at?: string | null;
  backoff_seconds?: number;
  last_error?: string | null;
};
```

Show a compact card for Orders, Finance, and Inventory/FBA with the latest success and either its next run or active backoff.

- [ ] **Step 2: Make AmazonPage respond to the existing refresh token.**

Read `refreshRequestToken` from `useDashboardShellState()`, factor the existing initial `Promise.all` fetch into `refreshAmazonData`, and re-run it when the page is active and the token changes. Preserve the selected shipment where it still exists.

- [ ] **Step 3: Verify browser refresh behavior through types and build.**

Run: `npm run typecheck && npx vite build`

Expected: TypeScript reports no errors and Vite completes its production build.

### Task 5: Final Verification and Controlled Live Enablement

**Files:**
- Modify: `docs/superpowers/specs/2026-08-17-amazon-quota-safe-auto-refresh-design.md` only if implementation decisions differ.
- Test: `ecommerce-dashboard/tests/test_amazon_auto_refresh.py`

- [ ] **Step 1: Run complete automated verification.**

Run: `python3 -m pytest`

Run: `npm run typecheck && npx vite build` from `frontend/`.

Expected: all backend tests pass; typecheck and production build pass.

- [ ] **Step 2: Restart the dedicated Ecom-Dash backend and inspect worker status.**

Call `GET /api/amazon/status` and verify `auto_refresh.enabled`, all four task states, and that no secret value appears.

- [ ] **Step 3: Trigger one delta cycle through the admin endpoint and inspect task isolation.**

Call `POST /api/amazon/auto-refresh/trigger` with `{"reason":"verification"}`. Confirm the response marks the trigger pending or starts a cycle, then read the status endpoint until it reports task completion. Verify the changestamp advances only if data changed.

- [ ] **Step 4: Verify the Tailnet dashboard URL remains reachable.**

Run: `curl --fail-with-body -k -sS https://desktop-nq2nv1f-1.tail7a6430.ts.net:10001/api/amazon/status`

Expected: HTTP 200 and the `auto_refresh` status object; existing Tailscale Serve entries remain unchanged.
