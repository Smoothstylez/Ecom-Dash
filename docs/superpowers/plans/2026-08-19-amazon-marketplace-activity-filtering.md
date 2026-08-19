# Amazon Marketplace Activity Filtering Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Only query Amazon SP-API orders/inventory for marketplaces the
seller is actually enrolled in (`participation.isParticipating == true`),
instead of every marketplace the account is merely eligible for.

**Architecture:** `_upsert_marketplaces()` gains an `active_only` filter
parameter (default `True`) applied to its returned marketplace-ID list
(all marketplaces are still persisted to the DB regardless). `sync_amazon_fba()`
and the manual sync API gain a pass-through `include_all_marketplaces`
override (default `False`). The scheduler is unaffected in code — it
never sets this new parameter, so it gets the filtered behavior by default.

**Tech Stack:** Python 3.12, FastAPI, SQLite, pytest.

## Global Constraints

- No destructive schema changes.
- Missing/absent `isParticipating` is treated as not participating (fail
  safe toward fewer API calls).
- Preserve all currently passing tests (78 passed before this plan).

---

### Task 1: Filter `_upsert_marketplaces()` by participation status

**Files:**
- Modify: `ecommerce-dashboard/app/services/importers/amazon_sp_api.py:1277-1310`
- Test: `ecommerce-dashboard/tests/test_amazon_fba.py` (append)

**Interfaces:**
- Produces: `_upsert_marketplaces(connection, payload, *, active_only: bool = True) -> list[str]`

- [ ] **Step 1: Write the failing test**

```python
def test_upsert_marketplaces_filters_to_participating_by_default(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    payload = {"payload": [
        {"marketplace": {"id": "DE", "name": "DE", "countryCode": "DE", "defaultCurrencyCode": "EUR", "domainName": "amazon.de"}, "participation": {"isParticipating": True}},
        {"marketplace": {"id": "FR", "name": "FR", "countryCode": "FR", "defaultCurrencyCode": "EUR", "domainName": "amazon.fr"}, "participation": {"isParticipating": False}},
        {"marketplace": {"id": "IT", "name": "IT", "countryCode": "IT", "defaultCurrencyCode": "EUR", "domainName": "amazon.it"}, "participation": {}},
    ]}

    with importer._connect() as connection:
        active = importer._upsert_marketplaces(connection, payload)
        connection.commit()
        all_ids = importer._upsert_marketplaces(connection, payload, active_only=False)
        connection.commit()
        stored = connection.execute("SELECT marketplace_id FROM amazon_marketplaces ORDER BY marketplace_id").fetchall()

    assert active == ["DE"]
    assert sorted(all_ids) == ["DE", "FR", "IT"]
    assert [row["marketplace_id"] for row in stored] == ["DE", "FR", "IT"]
```

- [ ] **Step 2: Run the test and verify it fails**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -k upsert_marketplaces_filters -v`

Expected: FAIL — `_upsert_marketplaces()` takes no `active_only` keyword and currently returns all three IDs.

- [ ] **Step 3: Implement the filter**

Replace the function body in `amazon_sp_api.py`:

```python
def _upsert_marketplaces(connection: sqlite3.Connection, payload: dict[str, Any], *, active_only: bool = True) -> list[str]:
    result: list[str] = []
    for item in _as_list(payload.get("payload")):
        participation = _as_dict(item)
        marketplace = _as_dict(participation.get("marketplace"))
        marketplace_id = _text(marketplace.get("id"))
        # Seller participation also exposes non-marketplace service entries.
        # Inventory endpoints reject these with a misleading regional 403.
        if (
            not marketplace_id
            or _text(marketplace.get("name")).lower().startswith("non-amazon")
            or _text(marketplace.get("domainName")).lower() == "non-amazon"
        ):
            continue
        connection.execute(
            """
            INSERT INTO amazon_marketplaces(marketplace_id, name, country_code, domain_name, default_currency, participation_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(marketplace_id) DO UPDATE SET
                name=excluded.name, country_code=excluded.country_code, domain_name=excluded.domain_name,
                default_currency=excluded.default_currency, participation_json=excluded.participation_json, updated_at=excluded.updated_at
            """,
            (
                marketplace_id,
                _text(marketplace.get("name")),
                _text(marketplace.get("countryCode")),
                _text(marketplace.get("domainName")),
                _text(marketplace.get("defaultCurrencyCode")),
                _json_dumps(participation),
                _utc_now(),
            ),
        )
        is_participating = bool(_as_dict(participation.get("participation")).get("isParticipating"))
        if active_only and not is_participating:
            continue
        result.append(marketplace_id)
    return result
```

- [ ] **Step 4: Run the test and verify it passes**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -k upsert_marketplaces_filters -v`

Expected: PASS.

- [ ] **Step 5: Run the full amazon_fba test file to check for regressions**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -v`

Expected: all pass (existing callers of `_upsert_marketplaces` inside `sync_amazon_fba()` are unaffected until Task 2 wires the new parameter through — they'll keep getting `active_only=True` default, which is safe since existing tests' fixtures either have no `participation` key, i.e. filtered out by default. Check any existing test relying on marketplaces flowing through with an empty `participation: {}` fixture — those existing tests use `participation: {}` at lines like 1247-1248 which will now resolve to `is_participating=False` and be excluded from the returned list by default. If any existing sync-level test asserts marketplace count or requires those IDs, update its fixture to `{"isParticipating": True}` to keep the same behavior.)

- [ ] **Step 6: Fix any existing test fixtures broken by the new default filter**

Search for other test fixtures that build a `marketplace_participations` payload without `isParticipating: True` and update them to include it, so pre-existing tests continue observing all marketplace IDs flowing through (matching their original intent, unaffected by this feature).

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py tests/test_amazon_auto_refresh.py -v`

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add ecommerce-dashboard/app/services/importers/amazon_sp_api.py ecommerce-dashboard/tests/test_amazon_fba.py
git commit -m "feat: filter Amazon marketplaces to isParticipating by default"
```

---

### Task 2: Thread `include_all_marketplaces` through `sync_amazon_fba()` and the manual sync API

**Files:**
- Modify: `ecommerce-dashboard/app/services/importers/amazon_sp_api.py:2068-2116` (`sync_amazon_fba` signature + `_upsert_marketplaces` call)
- Modify: `ecommerce-dashboard/app/routers/amazon.py` (`AmazonSyncRequest`, `api_amazon_sync`)
- Test: `ecommerce-dashboard/tests/test_amazon_fba.py` (append)

**Interfaces:**
- Produces: `sync_amazon_fba(..., include_all_marketplaces: bool = False) -> dict[str, Any]`
- Produces: `AmazonSyncRequest.include_all_marketplaces: bool = False`

- [ ] **Step 1: Write the failing test**

```python
def test_sync_amazon_fba_excludes_non_participating_marketplace_by_default(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer
    from app.services.importers.amazon_sp_api import AmazonSpApiClient

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    monkeypatch.setattr(importer, "load_amazon_sp_api_config", lambda: (importer.AmazonSpApiConfig("c", "s", "r"), []))

    def fake_marketplace_participations(self):
        return {"payload": [
            {"marketplace": {"id": "DE", "name": "DE", "countryCode": "DE", "defaultCurrencyCode": "EUR", "domainName": "amazon.de"}, "participation": {"isParticipating": True}},
            {"marketplace": {"id": "FR", "name": "FR", "countryCode": "FR", "defaultCurrencyCode": "EUR", "domainName": "amazon.fr"}, "participation": {"isParticipating": False}},
        ]}

    queried_marketplaces: list[list[str]] = []

    def fake_orders(self, marketplace_ids, created_after, *, updated_after=None):
        queried_marketplaces.append(list(marketplace_ids))
        return [], []

    monkeypatch.setattr(AmazonSpApiClient, "marketplace_participations", fake_marketplace_participations)
    monkeypatch.setattr(AmazonSpApiClient, "orders", fake_orders)

    importer.sync_amazon_fba(include_orders=True, include_inventory=False, include_finances=False, include_inbound=False)
    assert queried_marketplaces == [["DE"]]

    queried_marketplaces.clear()
    importer.sync_amazon_fba(include_orders=True, include_inventory=False, include_finances=False, include_inbound=False, include_all_marketplaces=True)
    assert queried_marketplaces == [["DE", "FR"]]
```

- [ ] **Step 2: Run the test and verify it fails**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -k excludes_non_participating -v`

Expected: FAIL — `sync_amazon_fba()` has no `include_all_marketplaces` parameter yet, and currently always calls `_upsert_marketplaces` with the default (now-filtering) behavior with no way to disable it.

- [ ] **Step 3: Add the parameter and thread it through**

In `sync_amazon_fba()`'s signature (around line 2068-2078), add:

```python
    include_all_marketplaces: bool = False,
```

right after `include_catalog_images: bool = True,`. Then update the `_upsert_marketplaces` call site (inside the function, where `participations = client.marketplace_participations()` and `marketplaces = _upsert_marketplaces(connection, participations)` currently appear) to:

```python
            marketplaces = _upsert_marketplaces(connection, participations, active_only=not include_all_marketplaces)
```

Also add `"include_all_marketplaces": include_all_marketplaces` to the `scopes` dict used for the sync-run's `requested_scopes_json` logging, alongside the other scope flags, for observability.

- [ ] **Step 4: Add the field to the manual sync request model and route**

In `ecommerce-dashboard/app/routers/amazon.py`, update `AmazonSyncRequest`:

```python
class AmazonSyncRequest(BaseModel):
    include_orders: bool = True
    include_inventory: bool = True
    include_finances: bool = True
    include_inbound: bool = True
    include_all_marketplaces: bool = False
    lookback_days: int = Field(default=30, ge=1, le=730)
```

Update `api_amazon_sync()`'s call to `run_manual_amazon_sync(...)` to pass
`include_all_marketplaces=request.include_all_marketplaces` alongside the
existing keyword arguments.

- [ ] **Step 5: Run the test and verify it passes**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -k excludes_non_participating -v`

Expected: PASS.

- [ ] **Step 6: Run the full backend suite**

Run: `cd ecommerce-dashboard && python3 -m pytest`

Expected: all tests pass, no regressions.

- [ ] **Step 7: Commit**

```bash
git add ecommerce-dashboard/app/services/importers/amazon_sp_api.py ecommerce-dashboard/app/routers/amazon.py ecommerce-dashboard/tests/test_amazon_fba.py
git commit -m "feat: add include_all_marketplaces override for manual Amazon sync"
```

---

### Task 3: Version bump and push

- [ ] **Step 1: Bump version in all three locations**

`ecommerce-dashboard/config.yaml` (`version:`), `ecommerce-dashboard/Dockerfile`
(`io.hass.version=`), `ecommerce-dashboard/app/config.py` (`APP_VERSION =`) —
bump from `0.5.2` to `0.5.3`.

- [ ] **Step 2: Run the full backend suite one more time**

Run: `cd ecommerce-dashboard && python3 -m pytest`

Expected: all pass.

- [ ] **Step 3: Commit and push**

```bash
git add ecommerce-dashboard/config.yaml ecommerce-dashboard/Dockerfile ecommerce-dashboard/app/config.py
git commit -m "chore: bump addon version to 0.5.3"
git push origin master
```
