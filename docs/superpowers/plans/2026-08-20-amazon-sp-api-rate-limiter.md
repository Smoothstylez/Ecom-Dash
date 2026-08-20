# Amazon SP-API Rate Limiter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pace every Amazon SP-API request through persistent quota buckets so Amazon order items reliably import without HTTP 429 bursts.

**Architecture:** Add a small, SQLite-backed token-bucket component inside the Amazon importer. `AmazonSpApiClient.request_json()` reserves capacity before every API call and records cooldowns after 429/503 responses. The existing order sync will only request items absent from the local database, newest missing orders first. The Amazon status endpoint and existing Amazon page expose pending item counts and bucket wait state.

**Tech Stack:** Python 3.12, SQLite, urllib, FastAPI, pytest, React 19, TypeScript, Vite.

## Global Constraints

- Do not delete or replace existing Amazon order-item records during a backfill.
- Do not make parallel Amazon item requests; sequential pacing is intentional.
- Keep manual marketplace selection and `include_all_marketplaces` behavior unchanged.
- Use one persisted bucket for every dynamic `orderItems` URL, not one bucket per order ID.
- Commit only source, tests, generated `ecommerce-dashboard/frontend_dist`, documentation, and version files; never stage runtime databases or local tool files.

---

### Task 1: Persisted quota-bucket primitives

**Files:**
- Modify: `ecommerce-dashboard/app/services/importers/amazon_sp_api.py`
- Test: `ecommerce-dashboard/tests/test_amazon_fba.py`

**Interfaces:**
- Produces: `amazon_api_bucket_key(path: str) -> str` returning `orders`, `order_items`, `catalog`, or `default`.
- Produces: `reserve_amazon_api_token(bucket_key: str, *, now: datetime | None = None) -> float`, returning `0.0` after reserving a token or the exact seconds to wait before retrying.
- Produces: `record_amazon_api_throttle(bucket_key: str, *, retry_after_seconds: float | None, error: str, now: datetime | None = None) -> None`.
- Produces: `update_amazon_api_rate_limit(bucket_key: str, rate_per_second: float) -> None`.
- Produces: `get_amazon_api_rate_limit_status(now: datetime | None = None) -> dict[str, dict[str, Any]]` for status presentation.

- [ ] **Step 1: Write the failing bucket-classification and reservation tests**

Append these tests to `ecommerce-dashboard/tests/test_amazon_fba.py`:

```python
def test_amazon_api_bucket_key_groups_dynamic_order_item_paths() -> None:
    from app.services.importers.amazon_sp_api import amazon_api_bucket_key

    assert amazon_api_bucket_key("/orders/v0/orders/111-222/orderItems") == "order_items"
    assert amazon_api_bucket_key("/orders/v0/orders/333-444/orderItems") == "order_items"
    assert amazon_api_bucket_key("/orders/v0/orders") == "orders"
    assert amazon_api_bucket_key("/catalog/2022-04-01/items/B0TEST") == "catalog"
    assert amazon_api_bucket_key("/fba/inventory/v1/summaries") == "default"


def test_amazon_api_bucket_reservation_refills_and_calculates_wait(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer
    from datetime import datetime, timedelta, timezone

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    start = datetime(2026, 8, 20, tzinfo=timezone.utc)

    assert importer.reserve_amazon_api_token("catalog", now=start) == 0.0
    assert importer.reserve_amazon_api_token("catalog", now=start) == 0.0
    assert importer.reserve_amazon_api_token("catalog", now=start) == 0.5
    assert importer.reserve_amazon_api_token("catalog", now=start + timedelta(seconds=0.5)) == 0.0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
cd ecommerce-dashboard && python3 -m pytest -q tests/test_amazon_fba.py -k 'bucket_key or bucket_reservation'
```

Expected: FAIL because the bucket functions do not exist.

- [ ] **Step 3: Add the additive SQLite table and constants**

In `init_amazon_fba_db()` add this table:

```sql
CREATE TABLE IF NOT EXISTS amazon_api_rate_limits (
    bucket_key TEXT PRIMARY KEY,
    rate_per_second REAL NOT NULL,
    burst_capacity REAL NOT NULL,
    tokens REAL NOT NULL,
    updated_at TEXT NOT NULL,
    blocked_until TEXT,
    last_throttle_at TEXT,
    last_throttle_error TEXT NOT NULL DEFAULT ''
);
```

Near the importer helpers, define the bucket mapping and conservative defaults:

```python
_AMAZON_API_BUCKETS = {
    "orders": (0.0167, 20.0),
    "order_items": (0.5, 30.0),
    "catalog": (2.0, 2.0),
    "default": (1.0, 1.0),
}
```

Implement `amazon_api_bucket_key()` with exact route matching: the bare
`/orders/v0/orders` route maps to `orders`; paths ending in `/orderItems` map
to `order_items`; paths starting with `/catalog/` map to `catalog`; all others
map to `default`. During initialization, insert each configured bucket with its
full burst capacity as the initial token count. This makes status available
before the first external request.

- [ ] **Step 4: Implement atomic token reservation and throttle recording**

Use `BEGIN IMMEDIATE` only for reading/upserting the one bucket row. Refill
with elapsed UTC seconds, cap at `burst_capacity`, decrement on reservation,
commit, and return the wait instead of sleeping while the SQLite transaction is
open. For `blocked_until` in the future, return its remaining wait.

Use this normalization shape in the status function:

```python
{
    "catalog": {
        "rate_per_second": 2.0,
        "available_tokens": 0.0,
        "blocked_until": "2026-08-20T12:00:00Z",
        "last_throttle_at": "2026-08-20T11:59:55Z",
        "last_throttle_error": "SP-API 429 for /catalog/...",
    },
}
```

`update_amazon_api_rate_limit()` accepts only positive rates, updates the
stored refill rate, and never modifies `burst_capacity`. `record_amazon_api_throttle()` uses the supplied `Retry-After` delay when it
is positive; otherwise use 1.5, 3.0, then 6.0 seconds based on the existing
cooldown duration, capped at 60 seconds. Set `blocked_until`, throttle fields,
and `tokens=0` in one committed transaction.

- [ ] **Step 5: Run the focused tests to verify they pass**

Run:

```bash
cd ecommerce-dashboard && python3 -m pytest -q tests/test_amazon_fba.py -k 'bucket_key or bucket_reservation'
```

Expected: PASS.

- [ ] **Step 6: Commit the persisted limiter primitives**

```bash
git add ecommerce-dashboard/app/services/importers/amazon_sp_api.py ecommerce-dashboard/tests/test_amazon_fba.py
git commit -m "feat: add persistent Amazon API rate buckets"
```

### Task 2: Route all SP-API calls through the limiter

**Files:**
- Modify: `ecommerce-dashboard/app/services/importers/amazon_sp_api.py`
- Test: `ecommerce-dashboard/tests/test_amazon_fba.py`

**Interfaces:**
- Consumes: `amazon_api_bucket_key`, `reserve_amazon_api_token`, and `record_amazon_api_throttle` from Task 1.
- Produces: paced `AmazonSpApiClient.request_json()` that preserves its existing return type and error format.

- [ ] **Step 1: Write the failing request-pacing and 429-cooldown tests**

Append tests that stub the LWA token and `urlopen` response:

```python
import io
from urllib.error import HTTPError


class FakeJsonResponse:
    def __init__(self, payload: dict, headers: dict[str, str] | None = None) -> None:
        self._payload = payload
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        return False

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


def quota_error(code: int, headers: dict[str, str]) -> HTTPError:
    return HTTPError(
        "https://sellingpartnerapi-eu.amazon.com/test",
        code,
        "quota exceeded",
        headers,
        io.BytesIO(b'{"errors":[{"code":"QuotaExceeded"}]}'),
    )


def test_request_json_waits_for_bucket_before_opening_request(monkeypatch) -> None:
    import app.services.importers.amazon_sp_api as importer
    from app.services.importers.amazon_sp_api import AmazonSpApiClient, AmazonSpApiConfig

    client = AmazonSpApiClient(AmazonSpApiConfig("client", "secret", "refresh"))
    waits: list[float] = []
    monkeypatch.setattr(client, "_lwa_access_token", lambda: "token")
    monkeypatch.setattr(importer, "reserve_amazon_api_token", lambda bucket: 2.0 if not waits else 0.0)
    monkeypatch.setattr(importer.time, "sleep", lambda seconds: waits.append(seconds))
    monkeypatch.setattr(importer, "urlopen", lambda request, timeout: FakeJsonResponse({"payload": {}}))

    assert client.request_json("/orders/v0/orders/ORDER-1/orderItems") == {"payload": {}}
    assert waits == [2.0]


def test_request_json_records_retry_after_on_quota_error(monkeypatch) -> None:
    import app.services.importers.amazon_sp_api as importer
    from app.services.importers.amazon_sp_api import AmazonSpApiClient, AmazonSpApiConfig, AmazonSpApiError

    client = AmazonSpApiClient(AmazonSpApiConfig("client", "secret", "refresh"))
    recorded: list[tuple[str, float | None, str]] = []
    monkeypatch.setattr(client, "_lwa_access_token", lambda: "token")
    monkeypatch.setattr(importer, "reserve_amazon_api_token", lambda bucket: 0.0)
    monkeypatch.setattr(importer, "record_amazon_api_throttle", lambda bucket, *, retry_after_seconds, error: recorded.append((bucket, retry_after_seconds, error)))
    monkeypatch.setattr(importer, "urlopen", lambda request, timeout: (_ for _ in ()).throw(quota_error(429, {"Retry-After": "7"})))

    with pytest.raises(AmazonSpApiError):
        client.request_json("/orders/v0/orders/ORDER-1/orderItems")

    assert recorded[0][0] == "order_items"
    assert recorded[0][1] == 7.0
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run:

```bash
cd ecommerce-dashboard && python3 -m pytest -q tests/test_amazon_fba.py -k 'request_json_waits or request_json_records'
```

Expected: FAIL because `request_json()` currently opens the request before any reservation and does not inspect `Retry-After`.

- [ ] **Step 3: Pace `request_json()` before every HTTP call**

At the start of `request_json()` determine `bucket_key = amazon_api_bucket_key(path)`.
Loop until `reserve_amazon_api_token(bucket_key)` returns `0.0`; call
`time.sleep(wait_seconds)` outside database transactions for every positive
wait. Preserve the existing request construction and successful JSON response
format.

On successful HTTP responses, parse `x-amzn-RateLimit-Limit` as a positive
float and call a small `update_amazon_api_rate_limit(bucket_key, rate)` helper
that persists only the rate and keeps the configured burst capacity.

On an `HTTPError` with code 429 or 503, parse `Retry-After` as a float when
valid, call `record_amazon_api_throttle(bucket_key, retry_after_seconds=..., error=...)`, then raise the existing `AmazonSpApiError` message format.

- [ ] **Step 4: Run the focused tests and current retry tests**

Run:

```bash
cd ecommerce-dashboard && python3 -m pytest -q tests/test_amazon_fba.py -k 'request_json_waits or request_json_records or retries_quota_exceeded'
```

Expected: PASS.

- [ ] **Step 5: Commit paced requests**

```bash
git add ecommerce-dashboard/app/services/importers/amazon_sp_api.py ecommerce-dashboard/tests/test_amazon_fba.py
git commit -m "fix: pace Amazon API requests before sending"
```

### Task 3: Fetch only missing order items, newest first

**Files:**
- Modify: `ecommerce-dashboard/app/services/importers/amazon_sp_api.py`
- Test: `ecommerce-dashboard/tests/test_amazon_fba.py`

**Interfaces:**
- Produces: `_orders_missing_items(connection: sqlite3.Connection, orders: list[dict[str, Any]]) -> list[dict[str, Any]]`.
- Changes: `sync_amazon_fba()` invokes `client.order_items()` only for returned orders.

- [ ] **Step 1: Write the failing sync behavior test**

Append a test that creates one previously complete order and one missing-items
order, then stubs the client:

```python
def test_sync_amazon_fba_fetches_only_missing_order_items_newest_first(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer
    from app.services.importers.amazon_sp_api import AmazonSpApiClient

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    monkeypatch.setattr(importer, "load_amazon_sp_api_config", lambda: (importer.AmazonSpApiConfig("c", "s", "r"), []))
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_order(connection, {"AmazonOrderId": "COMPLETE", "MarketplaceId": "DE", "PurchaseDate": "2026-08-18T10:00:00Z"})
        importer._upsert_order_items(connection, "COMPLETE", [{"ASIN": "B0COMPLETE", "SellerSKU": "SKU-C"}])
        connection.commit()

    monkeypatch.setattr(AmazonSpApiClient, "marketplace_participations", lambda self: {"payload": [{"marketplace": {"id": "DE"}, "participation": {"isParticipating": True}}]})
    monkeypatch.setattr(AmazonSpApiClient, "orders", lambda self, *args, **kwargs: ([
        {"AmazonOrderId": "OLD", "MarketplaceId": "DE", "LastUpdateDate": "2026-08-18T10:00:00Z"},
        {"AmazonOrderId": "NEW", "MarketplaceId": "DE", "LastUpdateDate": "2026-08-19T10:00:00Z"},
        {"AmazonOrderId": "COMPLETE", "MarketplaceId": "DE", "LastUpdateDate": "2026-08-20T10:00:00Z"},
    ], []))
    requested: list[str] = []
    monkeypatch.setattr(AmazonSpApiClient, "order_items", lambda self, order_id: requested.append(order_id) or [])

    importer.sync_amazon_fba(include_orders=True, include_inventory=False, include_finances=False, include_inbound=False)

    assert requested == ["NEW", "OLD"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cd ecommerce-dashboard && python3 -m pytest -q tests/test_amazon_fba.py -k fetches_only_missing_order_items
```

Expected: FAIL because the current loop requests every returned order in response order.

- [ ] **Step 3: Implement missing-item selection**

After upserting all returned order headers in `sync_amazon_fba()`, query
`amazon_order_items` for the fetched order IDs in one parameterized SQL query.
Return only IDs whose item count is zero. Sort the associated order payloads
by `_text(order.get("LastUpdateDate"))`, then `_text(order.get("PurchaseDate"))`, descending.

Use the returned list for the current order-item and optional catalog-image
loop. Keep the per-order `AmazonSpApiError` handling and raw-record writes
unchanged, so one permanently failed item request does not stop later orders.

- [ ] **Step 4: Run focused tests**

Run:

```bash
cd ecommerce-dashboard && python3 -m pytest -q tests/test_amazon_fba.py -k 'fetches_only_missing_order_items or retries_quota_exceeded'
```

Expected: PASS.

- [ ] **Step 5: Commit item-first backfill selection**

```bash
git add ecommerce-dashboard/app/services/importers/amazon_sp_api.py ecommerce-dashboard/tests/test_amazon_fba.py
git commit -m "fix: prioritize missing Amazon order items"
```

### Task 4: Expose limiter diagnostics in backend status and Amazon page

**Files:**
- Modify: `ecommerce-dashboard/app/services/importers/amazon_sp_api.py`
- Modify: `frontend/src/features/amazon/amazon-page.tsx`
- Test: `ecommerce-dashboard/tests/test_amazon_fba.py`

**Interfaces:**
- Consumes: `get_amazon_api_rate_limit_status()` from Task 1.
- Changes: `build_amazon_fba_status()` adds `pending_order_items: int` and `rate_limits: dict[str, dict[str, Any]]`.
- Changes: frontend `AmazonStatus` includes those optional fields.

- [ ] **Step 1: Write the failing backend status test**

Append:

```python
def test_amazon_status_reports_pending_items_and_rate_limits(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_order(connection, {"AmazonOrderId": "MISSING", "MarketplaceId": "DE"})
        importer._upsert_order(connection, {"AmazonOrderId": "COMPLETE", "MarketplaceId": "DE"})
        importer._upsert_order_items(connection, "COMPLETE", [{"ASIN": "B0COMPLETE", "SellerSKU": "SKU-C"}])
        connection.commit()

    status = importer.build_amazon_fba_status()

    assert status["pending_order_items"] == 1
    assert "order_items" in status["rate_limits"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cd ecommerce-dashboard && python3 -m pytest -q tests/test_amazon_fba.py -k status_reports_pending_items
```

Expected: FAIL because the status payload lacks both fields.

- [ ] **Step 3: Add backend fields and minimal frontend presentation**

In `build_amazon_fba_status()`, count Amazon orders for which no matching
`amazon_order_items` record exists, then include that value as
`pending_order_items`. Include every initialized persisted bucket in
`rate_limits` via `get_amazon_api_rate_limit_status()`.

In `frontend/src/features/amazon/amazon-page.tsx`:

```ts
type AmazonStatus = {
  // existing fields
  pending_order_items?: number;
  rate_limits?: Record<string, {
    blocked_until?: string | null;
    last_throttle_at?: string | null;
  }>;
};
```

Below the existing auto-refresh subtitle, display `Ausstehende Artikelpositionen:
N` when `pending_order_items > 0`. Derive a list of buckets with a non-empty
`blocked_until` or `last_throttle_at`; if non-empty, display a compact line
`Amazon-Limit aktiv: order_items, catalog`. Do not display endpoint paths,
errors, credentials, or token counts.

- [ ] **Step 4: Run backend test and frontend checks**

Run:

```bash
cd ecommerce-dashboard && python3 -m pytest -q tests/test_amazon_fba.py -k status_reports_pending_items
cd ../frontend && npm run typecheck && npm run build
```

Expected: all commands exit 0.

- [ ] **Step 5: Sync the production frontend artifact and commit**

Run:

```bash
rm -rf ecommerce-dashboard/frontend_dist/assets
cp -r frontend/dist/assets ecommerce-dashboard/frontend_dist/assets
cp frontend/dist/index.html ecommerce-dashboard/frontend_dist/index.html
git add ecommerce-dashboard/app/services/importers/amazon_sp_api.py ecommerce-dashboard/tests/test_amazon_fba.py frontend/src/features/amazon/amazon-page.tsx ecommerce-dashboard/frontend_dist
git commit -m "feat: show Amazon rate limit status"
```

### Task 5: Release, verify, and backfill

**Files:**
- Modify: `ecommerce-dashboard/config.yaml`
- Modify: `ecommerce-dashboard/Dockerfile`
- Modify: `ecommerce-dashboard/app/config.py`

**Interfaces:**
- Produces: add-on release version `0.5.7`.

- [ ] **Step 1: Bump the release version**

Change all three version values from `0.5.6` to `0.5.7`:

```yaml
version: "0.5.7"
```

```dockerfile
io.hass.version="0.5.7"
```

```python
APP_VERSION = "0.5.7"
```

- [ ] **Step 2: Run full verification**

Run:

```bash
cd ecommerce-dashboard && python3 -m pytest -q
cd ../frontend && npm run typecheck && npm run build
cd .. && git diff --check && git status --short
```

Expected: backend suite passes, frontend commands exit 0, no whitespace errors,
and only intended source, test, documentation, version, and frontend artifact
files are staged for the release.

- [ ] **Step 3: Commit and push the release**

```bash
git add ecommerce-dashboard/config.yaml ecommerce-dashboard/Dockerfile ecommerce-dashboard/app/config.py
git commit -m "chore: bump addon version to 0.5.7"
git push origin master
```

- [ ] **Step 4: Deploy and perform the production smoke test**

After the operator updates the Home Assistant add-on to `0.5.7`, verify:

```bash
curl --fail-with-body -sS http://192.168.178.197:8012/api/amazon/status
curl --fail-with-body -sS -X POST http://192.168.178.197:8012/api/amazon/sync \
  -H 'Content-Type: application/json' \
  -d '{"include_orders":true,"include_inventory":false,"include_finances":false,"include_inbound":false,"lookback_days":30}'
curl --fail-with-body -sS http://192.168.178.197:8012/api/orders/amazon/303-2546340-3457930
```

Expected: the sync response shows controlled pacing rather than order-item 429
errors, and the final detail response contains one or more `line_items` for
`303-2546340-3457930`.
