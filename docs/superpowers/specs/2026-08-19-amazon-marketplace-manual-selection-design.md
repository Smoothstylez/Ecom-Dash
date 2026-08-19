# Amazon Marketplace Manual Selection Setting Design

## Goal

Let the operator explicitly choose which Amazon marketplaces get synced,
as an alternative to the automatic `isParticipating`-based filter, to
avoid account-wide Orders/Inventory rate-limit exhaustion when the seller
account is enrolled (but not actually selling) in several marketplaces.

## Background

The `isParticipating`-based filter (implemented previously) reduced the
queried marketplace count from 10 to 6 for this account, but Amazon's SP-API
rate limits for `getOrders` (0.0167 req/s, burst 20) and similar operations
are shared per (selling-partner account, application) pair — not per
marketplace. Querying even 6 marketplaces back-to-back can still exhaust
the shared token bucket, especially combined with manual syncs and the
auto-refresh scheduler running close together. The operator wants a hard,
explicit override: restrict sync to a manually chosen subset (typically
just the one marketplace they actually sell on).

## Design

### Backend: persisted setting

New table in the Amazon FBA SQLite database (`init_amazon_fba_db()`):

```sql
CREATE TABLE IF NOT EXISTS amazon_sync_settings (
    id TEXT PRIMARY KEY,
    marketplace_mode TEXT NOT NULL DEFAULT 'auto',
    selected_marketplace_ids TEXT NOT NULL DEFAULT '[]',
    updated_at TEXT NOT NULL
);
```

Single row, keyed by the literal id `'default'`. `marketplace_mode` is
either `'auto'` (default — matches current `isParticipating`-based
filtering) or `'manual'` (use only `selected_marketplace_ids`, a JSON array
of marketplace ID strings).

New functions in `amazon_sp_api.py`:

- `get_amazon_marketplace_settings() -> dict[str, Any]` — reads the single
  settings row (creating a default `auto` row if missing), returns
  `{"marketplace_mode": str, "selected_marketplace_ids": list[str]}`.
- `set_amazon_marketplace_settings(*, marketplace_mode: str,
  selected_marketplace_ids: list[str]) -> dict[str, Any]` — validates
  `marketplace_mode` is one of `auto`/`manual`, upserts the row, returns the
  saved state.

### Backend: wiring into the sync

In `sync_amazon_fba()`, after fetching `participations` from Amazon, decide
which filter to apply based on the saved setting. When `marketplace_mode ==
'manual'` (and `include_all_marketplaces` is not set), the manual selection
is authoritative and completely bypasses the `isParticipating` filter —
this lets the operator select a marketplace even if Amazon's own flag is
stale or wrong. Otherwise, the existing `isParticipating`-based filter
applies exactly as before:

```python
all_marketplaces = _upsert_marketplaces(connection, participations, active_only=False)
settings = get_amazon_marketplace_settings()
if include_all_marketplaces:
    marketplaces = all_marketplaces
elif settings["marketplace_mode"] == "manual":
    allowed = set(settings["selected_marketplace_ids"])
    marketplaces = [m for m in all_marketplaces if m in allowed]
else:
    marketplaces = [m for m in all_marketplaces if _is_participating(m)]
```

`include_all_marketplaces=True` (the existing manual-sync override) still
bypasses both the `isParticipating` filter and the manual selection —
it remains the explicit "query literally everything" escape hatch.

### Backend: API endpoints

In `app/routers/amazon.py`:

- `GET /api/amazon/marketplace-settings` — no auth required (read-only,
  matches the pattern of `GET /api/amazon/status`). Returns:
  ```json
  {
    "ok": true,
    "marketplace_mode": "auto",
    "selected_marketplace_ids": [],
    "marketplaces": [
      {"marketplace_id": "A1PA6795UKMFR9", "name": "Amazon.de", "country_code": "DE", "domain_name": "amazon.de", "is_participating": true},
      ...
    ]
  }
  ```
  The `marketplaces` list is read from `amazon_marketplaces` (already
  populated by every sync), each annotated with `is_participating` parsed
  from its stored `participation_json`.
- `POST /api/amazon/marketplace-settings` (admin-only, matches the pattern
  of `POST /api/amazon/sync`) — request body `{"marketplace_mode": "auto"
  | "manual", "selected_marketplace_ids": ["A1PA6795UKMFR9", ...]}`.
  Rejects unknown marketplace IDs (not present in `amazon_marketplaces`)
  with HTTP 400.

### Frontend: Settings panel section

In `dashboard-controls.tsx`, add a new `settings-section` (matching the
"API Credentials" / "Admin Zugriff" sections already there) titled "Amazon
Marketplaces", placed after the "Admin Zugriff" section:

- A mode toggle (radio buttons or a two-option toggle): "Automatisch
  (empfohlen)" vs "Manuell".
- When "Manuell" is selected: a checkbox list of all known marketplaces
  (name + country code), pre-checked according to the currently saved
  selection.
- A "Speichern" button that POSTs the current mode + selected IDs.
- A status line showing the last save result (matches existing
  `credentialsStatus` pattern).

New API helper functions in `dashboard-controls-api.ts`:
`fetchAmazonMarketplaceSettings()` and `saveAmazonMarketplaceSettings(mode,
selectedIds)`.

## Non-goals

- No automatic detection of "has this marketplace ever had a real order" —
  the operator makes this choice explicitly; `isParticipating` from Amazon
  remains the only automatic signal.
- No change to the existing `include_all_marketplaces` manual-sync
  override — it still means "ignore all filters, query everything Amazon
  returns."
- No proactive rate-limit pacing/delay between sequential per-marketplace
  calls — deferred; manual restriction to fewer marketplaces is the chosen
  mitigation for now.

## Testing

- `get_amazon_marketplace_settings()` / `set_amazon_marketplace_settings()`
  round-trip correctly, default to `auto`/`[]` when no row exists yet,
  reject invalid `marketplace_mode` values.
- `sync_amazon_fba()` with `marketplace_mode='manual'` and a selection of
  one marketplace ID only queries that one marketplace for orders, even
  when more are `isParticipating`.
- `include_all_marketplaces=True` still bypasses manual selection.
- Router: `GET`/`POST /api/amazon/marketplace-settings` round-trip via
  FastAPI `TestClient`; `POST` without an admin token is rejected when
  `APP_ADMIN_TOKEN` is set; unknown marketplace IDs are rejected with 400.

## Verification

- Full backend test suite passes.
- Frontend typecheck and production build pass.
- Manual smoke test against the live production instance: set mode to
  `manual` with only the real marketplace selected, trigger a sync,
  confirm via the sync response that only that one marketplace was
  queried for orders.
