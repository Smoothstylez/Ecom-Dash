# Amazon Marketplace Manual Selection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persisted setting letting the operator manually restrict which
Amazon marketplaces are synced, exposed via a new Settings-panel section.

**Architecture:** New `amazon_sync_settings` table (single row) in the
Amazon FBA SQLite database. `sync_amazon_fba()` consults it to decide the
marketplace filtering strategy (auto/`isParticipating` vs manual explicit
list vs `include_all_marketplaces` override). Two new endpoints expose
read/write. A new Settings-panel section in the frontend lets the operator
toggle mode and pick marketplaces from a checkbox list.

**Tech Stack:** Python 3.12, FastAPI, SQLite, pytest, React, TypeScript.

## Global Constraints

- No destructive schema changes; additive table only.
- Preserve all currently passing tests (80 passed before this plan).
- `include_all_marketplaces=True` remains the highest-priority override
  (bypasses both auto and manual filtering).

---

### Task 1: Persisted marketplace-sync settings (backend storage)

**Files:**
- Modify: `ecommerce-dashboard/app/services/importers/amazon_sp_api.py`
  (schema in `init_amazon_fba_db()`, new functions)
- Test: `ecommerce-dashboard/tests/test_amazon_fba.py` (append)

**Interfaces:**
- Produces: `get_amazon_marketplace_settings() -> dict[str, Any]` returning
  `{"marketplace_mode": str, "selected_marketplace_ids": list[str]}`.
- Produces: `set_amazon_marketplace_settings(*, marketplace_mode: str,
  selected_marketplace_ids: list[str]) -> dict[str, Any]` — raises
  `ValueError` for an invalid `marketplace_mode`.

- [ ] **Step 1: Write the failing test**

```python
def test_amazon_marketplace_settings_default_to_auto(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()

    settings = importer.get_amazon_marketplace_settings()

    assert settings == {"marketplace_mode": "auto", "selected_marketplace_ids": []}


def test_amazon_marketplace_settings_round_trip(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()

    saved = importer.set_amazon_marketplace_settings(
        marketplace_mode="manual", selected_marketplace_ids=["A1PA6795UKMFR9"]
    )
    assert saved == {"marketplace_mode": "manual", "selected_marketplace_ids": ["A1PA6795UKMFR9"]}

    reloaded = importer.get_amazon_marketplace_settings()
    assert reloaded == saved


def test_amazon_marketplace_settings_rejects_invalid_mode(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()

    try:
        importer.set_amazon_marketplace_settings(marketplace_mode="bogus", selected_marketplace_ids=[])
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for invalid marketplace_mode")
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -k amazon_marketplace_settings -v`

Expected: FAIL — functions don't exist yet.

- [ ] **Step 3: Add the schema and functions**

In `init_amazon_fba_db()`'s `executescript(...)` block, add:

```sql
CREATE TABLE IF NOT EXISTS amazon_sync_settings (
    id TEXT PRIMARY KEY,
    marketplace_mode TEXT NOT NULL DEFAULT 'auto',
    selected_marketplace_ids TEXT NOT NULL DEFAULT '[]',
    updated_at TEXT NOT NULL
);
```

Add near `_upsert_marketplaces`:

```python
_VALID_MARKETPLACE_MODES = {"auto", "manual"}


def get_amazon_marketplace_settings() -> dict[str, Any]:
    init_amazon_fba_db()
    with _connect() as connection:
        connection.execute(
            "INSERT OR IGNORE INTO amazon_sync_settings(id, marketplace_mode, selected_marketplace_ids, updated_at) VALUES ('default', 'auto', '[]', ?)",
            (_utc_now(),),
        )
        connection.commit()
        row = connection.execute(
            "SELECT marketplace_mode, selected_marketplace_ids FROM amazon_sync_settings WHERE id = 'default'"
        ).fetchone()
    try:
        selected = json.loads(str(row["selected_marketplace_ids"]))
    except (TypeError, ValueError, json.JSONDecodeError):
        selected = []
    return {
        "marketplace_mode": str(row["marketplace_mode"]),
        "selected_marketplace_ids": [str(m) for m in selected] if isinstance(selected, list) else [],
    }


def set_amazon_marketplace_settings(*, marketplace_mode: str, selected_marketplace_ids: list[str]) -> dict[str, Any]:
    mode = str(marketplace_mode or "").strip().lower()
    if mode not in _VALID_MARKETPLACE_MODES:
        raise ValueError(f"marketplace_mode must be one of {sorted(_VALID_MARKETPLACE_MODES)}")
    normalized_ids = [str(m).strip() for m in selected_marketplace_ids if str(m).strip()]
    init_amazon_fba_db()
    with _connect() as connection:
        connection.execute(
            """
            INSERT INTO amazon_sync_settings(id, marketplace_mode, selected_marketplace_ids, updated_at)
            VALUES ('default', ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                marketplace_mode=excluded.marketplace_mode,
                selected_marketplace_ids=excluded.selected_marketplace_ids,
                updated_at=excluded.updated_at
            """,
            (mode, _json_dumps(normalized_ids), _utc_now()),
        )
        connection.commit()
    return {"marketplace_mode": mode, "selected_marketplace_ids": normalized_ids}
```

- [ ] **Step 4: Run the tests and verify they pass**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -k amazon_marketplace_settings -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ecommerce-dashboard/app/services/importers/amazon_sp_api.py ecommerce-dashboard/tests/test_amazon_fba.py
git commit -m "feat: add persisted Amazon marketplace sync settings"
```

---

### Task 2: Wire the setting into `sync_amazon_fba()`'s marketplace selection

**Files:**
- Modify: `ecommerce-dashboard/app/services/importers/amazon_sp_api.py`
  (`sync_amazon_fba()` marketplace-selection block)
- Test: `ecommerce-dashboard/tests/test_amazon_fba.py` (append)

**Interfaces:**
- Consumes: `get_amazon_marketplace_settings()` from Task 1.
- No signature changes to `sync_amazon_fba()`.

- [ ] **Step 1: Write the failing test**

```python
def test_sync_amazon_fba_uses_manual_marketplace_selection(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer
    from app.services.importers.amazon_sp_api import AmazonSpApiClient

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    monkeypatch.setattr(importer, "load_amazon_sp_api_config", lambda: (importer.AmazonSpApiConfig("c", "s", "r"), []))
    importer.init_amazon_fba_db()
    importer.set_amazon_marketplace_settings(marketplace_mode="manual", selected_marketplace_ids=["FR"])

    def fake_marketplace_participations(self):
        return {"payload": [
            {"marketplace": {"id": "DE", "name": "DE", "countryCode": "DE", "defaultCurrencyCode": "EUR", "domainName": "amazon.de"}, "participation": {"isParticipating": True}},
            {"marketplace": {"id": "FR", "name": "FR", "countryCode": "FR", "defaultCurrencyCode": "EUR", "domainName": "amazon.fr"}, "participation": {"isParticipating": False}},
        ]}

    queried: list[list[str]] = []

    def fake_orders(self, marketplace_ids, created_after, *, updated_after=None):
        queried.append(list(marketplace_ids))
        return [], []

    monkeypatch.setattr(AmazonSpApiClient, "marketplace_participations", fake_marketplace_participations)
    monkeypatch.setattr(AmazonSpApiClient, "orders", fake_orders)

    importer.sync_amazon_fba(include_orders=True, include_inventory=False, include_finances=False, include_inbound=False)

    assert queried == [["FR"]]


def test_sync_amazon_fba_include_all_marketplaces_bypasses_manual_selection(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer
    from app.services.importers.amazon_sp_api import AmazonSpApiClient

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    monkeypatch.setattr(importer, "load_amazon_sp_api_config", lambda: (importer.AmazonSpApiConfig("c", "s", "r"), []))
    importer.init_amazon_fba_db()
    importer.set_amazon_marketplace_settings(marketplace_mode="manual", selected_marketplace_ids=["FR"])

    def fake_marketplace_participations(self):
        return {"payload": [
            {"marketplace": {"id": "DE", "name": "DE", "countryCode": "DE", "defaultCurrencyCode": "EUR", "domainName": "amazon.de"}, "participation": {"isParticipating": True}},
            {"marketplace": {"id": "FR", "name": "FR", "countryCode": "FR", "defaultCurrencyCode": "EUR", "domainName": "amazon.fr"}, "participation": {"isParticipating": False}},
        ]}

    queried: list[list[str]] = []

    def fake_orders(self, marketplace_ids, created_after, *, updated_after=None):
        queried.append(list(marketplace_ids))
        return [], []

    monkeypatch.setattr(AmazonSpApiClient, "marketplace_participations", fake_marketplace_participations)
    monkeypatch.setattr(AmazonSpApiClient, "orders", fake_orders)

    importer.sync_amazon_fba(include_orders=True, include_inventory=False, include_finances=False, include_inbound=False, include_all_marketplaces=True)

    assert queried == [["DE", "FR"]]
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -k "manual_marketplace_selection or bypasses_manual_selection" -v`

Expected: FAIL — the first test fails because manual mode isn't wired in
yet (marketplace filtering still uses only `isParticipating`, so `FR`
with `isParticipating: False` would currently be excluded rather than
included via manual override).

- [ ] **Step 3: Wire the setting into the marketplace-selection block**

Locate the line in `sync_amazon_fba()`:

```python
            marketplaces = _upsert_marketplaces(connection, participations, active_only=not include_all_marketplaces)
```

Replace with:

```python
            all_marketplaces = _upsert_marketplaces(connection, participations, active_only=False)
            if include_all_marketplaces:
                marketplaces = all_marketplaces
            else:
                sync_settings = get_amazon_marketplace_settings()
                if sync_settings["marketplace_mode"] == "manual":
                    allowed = set(sync_settings["selected_marketplace_ids"])
                    marketplaces = [m for m in all_marketplaces if m in allowed]
                else:
                    marketplaces = _upsert_marketplaces(connection, participations, active_only=True)
```

(The second `_upsert_marketplaces` call in the `auto` branch is a cheap
re-run of the same already-persisted upsert — it re-reads
`participations` and returns the `isParticipating`-filtered subset without
any additional network calls; this keeps the auto-path identical to the
existing tested behavior instead of duplicating the `isParticipating`
parsing logic inline.)

- [ ] **Step 4: Run the tests and verify they pass**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -k "manual_marketplace_selection or bypasses_manual_selection" -v`

Expected: PASS.

- [ ] **Step 5: Run the full amazon_fba test file**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -v`

Expected: all pass, no regressions to the earlier `isParticipating`-filter tests.

- [ ] **Step 6: Commit**

```bash
git add ecommerce-dashboard/app/services/importers/amazon_sp_api.py ecommerce-dashboard/tests/test_amazon_fba.py
git commit -m "feat: apply manual marketplace selection in sync_amazon_fba"
```

---

### Task 3: API endpoints

**Files:**
- Modify: `ecommerce-dashboard/app/routers/amazon.py`
- Test: `ecommerce-dashboard/tests/test_amazon_fba.py` (append, using FastAPI TestClient)

**Interfaces:**
- Produces: `GET /api/amazon/marketplace-settings` (unauthenticated read)
- Produces: `POST /api/amazon/marketplace-settings` (admin-only write)

- [ ] **Step 1: Write the failing test**

```python
def test_marketplace_settings_endpoints_round_trip(monkeypatch, tmp_path) -> None:
    import importlib

    from fastapi.testclient import TestClient

    monkeypatch.setenv("AUTO_SYNC_ON_STARTUP", "0")
    monkeypatch.setenv("AMAZON_AUTO_REFRESH_ENABLED", "0")
    monkeypatch.setenv("LIVE_SYNC_BACKGROUND_ENABLED", "0")
    monkeypatch.setenv("COMBINED_DB_PATH", str(tmp_path / "combined.sqlite3"))
    monkeypatch.setenv("AMAZON_FBA_DB_PATH", str(tmp_path / "amazon.sqlite3"))
    monkeypatch.setenv("APP_ADMIN_TOKEN", "test-token")

    import app.config as config_module
    import app.services.importers.amazon_sp_api as importer_module
    import app.routers.amazon as router_module
    import app.main as main_module

    importlib.reload(config_module)
    importlib.reload(importer_module)
    importlib.reload(router_module)
    importlib.reload(main_module)

    importer_module.init_amazon_fba_db()
    with importer_module._connect() as connection:
        importer_module._upsert_marketplaces(
            connection,
            {"payload": [
                {"marketplace": {"id": "DE", "name": "Amazon.de", "countryCode": "DE", "defaultCurrencyCode": "EUR", "domainName": "amazon.de"}, "participation": {"isParticipating": True}},
            ]},
            active_only=False,
        )
        connection.commit()

    client = TestClient(main_module.app)

    response = client.get("/api/amazon/marketplace-settings")
    assert response.status_code == 200
    payload = response.json()
    assert payload["marketplace_mode"] == "auto"
    assert payload["marketplaces"] == [
        {"marketplace_id": "DE", "name": "Amazon.de", "country_code": "DE", "domain_name": "amazon.de", "is_participating": True}
    ]

    unauthorized = client.post("/api/amazon/marketplace-settings", json={"marketplace_mode": "manual", "selected_marketplace_ids": ["DE"]})
    assert unauthorized.status_code == 401

    saved = client.post(
        "/api/amazon/marketplace-settings",
        json={"marketplace_mode": "manual", "selected_marketplace_ids": ["DE"]},
        headers={"X-Admin-Token": "test-token"},
    )
    assert saved.status_code == 200
    assert saved.json()["marketplace_mode"] == "manual"

    rejected = client.post(
        "/api/amazon/marketplace-settings",
        json={"marketplace_mode": "manual", "selected_marketplace_ids": ["NOT-A-REAL-ID"]},
        headers={"X-Admin-Token": "test-token"},
    )
    assert rejected.status_code == 400
```

- [ ] **Step 2: Run the test and verify it fails**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -k marketplace_settings_endpoints -v`

Expected: FAIL — routes don't exist (404).

- [ ] **Step 3: Add the request model and routes**

In `app/routers/amazon.py`, add near the other request models:

```python
class AmazonMarketplaceSettingsRequest(BaseModel):
    marketplace_mode: str
    selected_marketplace_ids: list[str] = Field(default_factory=list)
```

Add the import for `get_amazon_marketplace_settings`, `set_amazon_marketplace_settings`
to the existing `from app.services.importers.amazon_sp_api import (...)` block.

Add the routes (near `api_amazon_status`):

```python
@router.get("/marketplace-settings")
def api_get_amazon_marketplace_settings() -> dict[str, Any]:
    settings = get_amazon_marketplace_settings()
    with _connect() as connection:  # reuse existing import from amazon_sp_api if _connect is exported; otherwise import it explicitly
        rows = connection.execute("SELECT marketplace_id, name, country_code, domain_name, participation_json FROM amazon_marketplaces ORDER BY name").fetchall()
    marketplaces = []
    for row in rows:
        try:
            participation = json.loads(str(row["participation_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            participation = {}
        marketplaces.append({
            "marketplace_id": row["marketplace_id"],
            "name": row["name"],
            "country_code": row["country_code"],
            "domain_name": row["domain_name"],
            "is_participating": bool(_as_dict(participation.get("participation")).get("isParticipating")) if isinstance(participation, dict) else False,
        })
    return {"ok": True, **settings, "marketplaces": marketplaces}


@router.post("/marketplace-settings", dependencies=ADMIN_ONLY)
def api_set_amazon_marketplace_settings(payload: AmazonMarketplaceSettingsRequest) -> dict[str, Any]:
    with _connect() as connection:
        known_ids = {row["marketplace_id"] for row in connection.execute("SELECT marketplace_id FROM amazon_marketplaces").fetchall()}
    unknown = [m for m in payload.selected_marketplace_ids if m not in known_ids]
    if unknown:
        raise HTTPException(status_code=400, detail=f"Unknown marketplace_id(s): {unknown}")
    try:
        result = set_amazon_marketplace_settings(
            marketplace_mode=payload.marketplace_mode,
            selected_marketplace_ids=payload.selected_marketplace_ids,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, **result}
```

Note: `_connect`, `_as_dict`, and `json` need to be imported/available in
`amazon.py` — check the existing imports; if `_connect`/`_as_dict` aren't
already imported from `amazon_sp_api`, add them to the existing import
block, and add `import json` at the top of the file alongside the other
stdlib imports.

- [ ] **Step 4: Run the test and verify it passes**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -k marketplace_settings_endpoints -v`

Expected: PASS.

- [ ] **Step 5: Run the full backend suite**

Run: `cd ecommerce-dashboard && python3 -m pytest`

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add ecommerce-dashboard/app/routers/amazon.py ecommerce-dashboard/tests/test_amazon_fba.py
git commit -m "feat: add Amazon marketplace settings API endpoints"
```

---

### Task 4: Frontend Settings-panel section

**Files:**
- Modify: `frontend/src/app/dashboard-controls-api.ts` (new fetch helpers)
- Modify: `frontend/src/app/dashboard-controls.tsx` (new settings section)

**Interfaces:**
- Produces: `fetchAmazonMarketplaceSettings(): Promise<AmazonMarketplaceSettings>`
- Produces: `saveAmazonMarketplaceSettings(mode, selectedIds): Promise<AmazonMarketplaceSettings>`
- Consumes: `GET`/`POST /api/amazon/marketplace-settings` from Task 3.

- [ ] **Step 1: Add the API helper types and functions**

In `dashboard-controls-api.ts`, add:

```typescript
export type AmazonMarketplaceOption = {
  marketplace_id: string;
  name: string;
  country_code: string;
  domain_name: string;
  is_participating: boolean;
};

export type AmazonMarketplaceSettings = {
  marketplace_mode: "auto" | "manual";
  selected_marketplace_ids: string[];
  marketplaces: AmazonMarketplaceOption[];
};

export async function fetchAmazonMarketplaceSettings(): Promise<AmazonMarketplaceSettings> {
  return fetchJson<AmazonMarketplaceSettings>(buildDashboardApiUrl("/api/amazon/marketplace-settings"));
}

export async function saveAmazonMarketplaceSettings(
  mode: "auto" | "manual",
  selectedIds: string[],
): Promise<AmazonMarketplaceSettings> {
  return fetchJson<AmazonMarketplaceSettings>(buildDashboardApiUrl("/api/amazon/marketplace-settings"), {
    method: "POST",
    headers: { "Content-Type": "application/json", ...adminAuthHeaders() },
    body: JSON.stringify({ marketplace_mode: mode, selected_marketplace_ids: selectedIds }),
  });
}
```

(Check the existing `dashboard-controls-api.ts` for the actual admin-auth
header helper name used by other admin-protected calls like
`runLiveSyncRequest` — reuse that exact helper instead of inventing
`adminAuthHeaders()` if a differently-named one already exists.)

- [ ] **Step 2: Add state and a settings section in `dashboard-controls.tsx`**

Add local state near the other settings state:

```typescript
const [amazonMarketplaces, setAmazonMarketplaces] = useState<AmazonMarketplaceSettings | null>(null);
const [amazonMarketplaceDraftMode, setAmazonMarketplaceDraftMode] = useState<"auto" | "manual">("auto");
const [amazonMarketplaceDraftSelection, setAmazonMarketplaceDraftSelection] = useState<string[]>([]);
const [amazonMarketplaceStatus, setAmazonMarketplaceStatus] = useState("");
```

Load it when the settings panel opens (alongside the existing
`openSettingsPanel` logic), and add a `handleAmazonMarketplaceSave`
callback that calls `saveAmazonMarketplaceSettings` and updates status.

Add the section markup after the "Admin Zugriff" `settings-section` block:

```tsx
<div className="settings-section">
  <h3>Amazon Marketplaces</h3>
  <div className="credentials-form">
    <label className="settings-inline-row">
      <span className="settings-toggle-label">Modus</span>
      <select
        value={amazonMarketplaceDraftMode}
        onChange={(event) => setAmazonMarketplaceDraftMode(event.target.value as "auto" | "manual")}
      >
        <option value="auto">Automatisch (empfohlen)</option>
        <option value="manual">Manuell</option>
      </select>
    </label>
    {amazonMarketplaceDraftMode === "manual" ? (
      <div className="settings-status-line">
        {(amazonMarketplaces?.marketplaces || []).map((marketplace) => (
          <label key={marketplace.marketplace_id} className="settings-toggle-row">
            <span className="settings-toggle-label">
              {marketplace.name} ({marketplace.country_code}){marketplace.is_participating ? "" : " – nicht aktiv laut Amazon"}
            </span>
            <input
              type="checkbox"
              checked={amazonMarketplaceDraftSelection.includes(marketplace.marketplace_id)}
              onChange={(event) => {
                setAmazonMarketplaceDraftSelection((current) =>
                  event.target.checked
                    ? [...current, marketplace.marketplace_id]
                    : current.filter((id) => id !== marketplace.marketplace_id),
                );
              }}
            />
          </label>
        ))}
      </div>
    ) : null}
    <button className="btn-inline primary" type="button" onClick={() => void handleAmazonMarketplaceSave()}>
      Speichern
    </button>
    <div className="settings-status-line">{amazonMarketplaceStatus}</div>
  </div>
</div>
```

- [ ] **Step 3: Run typecheck and build**

Run: `cd frontend && npm run typecheck && npx vite build`

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/app/dashboard-controls-api.ts frontend/src/app/dashboard-controls.tsx
git commit -m "feat: add Amazon marketplace selection UI to Settings panel"
```

---

### Task 5: Smoke tests, version bump, push

- [ ] **Step 1: Run the complete backend suite**

Run: `cd ecommerce-dashboard && python3 -m pytest`

Expected: all pass (80 + ~9 new = ~89).

- [ ] **Step 2: Run frontend typecheck and production build**

Run: `cd frontend && npm run typecheck && npx vite build`

Expected: no errors.

- [ ] **Step 3: Smoke test the new endpoints against a locally running instance**

Start a local backend, then:

```bash
curl -sS http://127.0.0.1:8012/api/amazon/marketplace-settings | python3 -m json.tool
curl -sS -X POST http://127.0.0.1:8012/api/amazon/marketplace-settings \
  -H "Content-Type: application/json" \
  -d '{"marketplace_mode": "auto", "selected_marketplace_ids": []}'
```

Expected: both return 200 with the expected JSON shape; no tracebacks in
the server log.

- [ ] **Step 4: Bump version**

`config.yaml`, `Dockerfile`, `app/config.py` — bump from `0.5.3` to `0.5.4`.

- [ ] **Step 5: Commit and push**

```bash
git add ecommerce-dashboard/config.yaml ecommerce-dashboard/Dockerfile ecommerce-dashboard/app/config.py
git commit -m "chore: bump addon version to 0.5.4"
git push origin master
```
