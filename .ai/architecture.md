# Architecture: E-Commerce Dashboard

## Tech Stack
- **Backend**: Python 3.11, FastAPI 0.116.1
- **Frontend**: Vite, React 19, TypeScript, hybrid React shell plus remaining legacy dashboard modules
- **UI / Viz**: Chart.js 4.4.2, Leaflet 1.9.4, Globe.gl 2.41.6
- **Testing**: Playwright for route and interaction smoke coverage, `unittest` for backend route checks
- **Database**: SQLite 3 (multiple databases, combined at runtime)
- **Server**: Uvicorn ASGI
- **Deployment**: Docker, Home Assistant add-on (ingress port 8012)

## Folder Structure
```
ecommerce-dashboard/
├── app/
│   ├── main.py              # FastAPI app factory, routers, startup hooks
│   ├── config.py            # Path resolution, environment variables, version
│   ├── db.py                # Combined DB operations, invoice documents, enrichments
│   ├── changestamp.py       # Cross-session change detection (polling)
│   ├── routers/             # API endpoints by domain
│   │   ├── orders.py        # Order listing, details, purchase enrichment, invoices
│   │   ├── analytics.py     # Aggregated metrics, trends, heatmaps
│   │   ├── customers.py     # Customer merging, geo-data, repeat analytics
│   │   ├── bookings.py      # Bookkeeping transactions, templates, accounts
│   │   ├── google_ads.py    # Ads cost import, product assignments, ROAS
│   │   ├── ebay.py          # Legacy eBay data display
│   │   ├── sync.py          # Source sync, live sync triggers
│   │   └── exports.py       # Data export, backup, restore
│   ├── services/            # Business logic layer
│   │   ├── orders.py        # Order loading, merging, filtering, enrichment
│   │   ├── analytics.py     # Metric aggregation, trend computation
│   │   ├── customers.py     # Customer deduplication, geo-resolution
│   │   ├── bookings.py      # Order-bookkeeping sync
│   │   ├── bookkeeping_full.py  # Full bookkeeping service (transactions, templates, accounts, documents)
│   │   ├── google_ads.py    # Ads cost tracking, product mapping
│   │   ├── ebay.py          # Legacy eBay data queries
│   │   ├── source_sync.py   # Bootstrap DB copy-sync
│   │   ├── live_sync.py     # Background worker for API polling
│   │   ├── exports.py       # Backup/restore, CSV export
│   │   └── importers/       # Live API importers
│   │       ├── shopify_live.py  # Shopify REST API sync
│   │       └── kaufland_live.py # Kaufland API sync
│   └── static/
│       └── css/             # themes.css, main.css
frontend/
├── src/
│   ├── main.tsx             # Vite entry
│   ├── app/                 # React shell, route selection, shared shell state
│   ├── features/            # Route-owned React screens (analytics, orders, customers, bookings, google-ads, ebay)
│   └── shared/              # Runtime helpers, theme provider, route resolution
├── data/
│   ├── combined.sqlite3     # Runtime DB (enrichments, invoices, ads costs)
│   └── sources/             # Runtime source DBs (copied from bootstrap)
│       ├── shopify/shopify_data.sqlite3
│       ├── kaufland/kaufland_data.sqlite3
│       ├── bookkeeping/dashboard.sqlite3
│       └── ebay/ebay_data.sqlite3
├── storage/
│   ├── invoices/            # Order purchase invoice uploads
│   └── documents/           # Bookkeeping document uploads
├── scripts/                 # CLI utilities (sync_sources.py, sync_live_sources.py, import_ebay.py)
├── config.yaml              # Home Assistant add-on config
├── Dockerfile               # Container definition
└── run.sh                   # Entry point script
```

## Database Schema

### combined.sqlite3
- `order_purchase_documents`: id, marketplace, order_id, original_filename, stored_filename, file_path, mime_type, uploaded_at
- `order_enrichments`: marketplace, order_id, purchase_cost_cents, purchase_currency, supplier_name, purchase_notes, invoice_document_id
- `google_ads_import_batches`: id, import_kind, source_filename, imported_at
- `google_ads_daily_costs`: article_id, day, cost_cents, currency
- `google_ads_product_assignments`: article_id, product_title, product_key, product_label, product_detail

### bookkeeping.sqlite3 (managed by bookkeeping_full.py)
- `transactions`: id, date, type, direction, amount_gross, currency, vat_rate, vat_amount, amount_net, provider, counterparty_name, category, reference, order_id, document_id, template_id, payment_account_id, period_key, notes, source, source_key, status, booking_class
- `orders`: id, provider, external_order_id, order_date, currency, revenue_gross, revenue_net, vat_amount, status
- `documents`: id, original_filename, stored_filename, file_path, mime_type, uploaded_at, notes
- `recurring_templates`: id, name, type, direction, default_amount_gross, currency, provider, counterparty_name, schedule, day_of_month, start_date, active
- `payment_accounts`: id, name, provider, is_active
- `monthly_invoices`: id, provider, period_from, period_to, invoice_amount_cents, calculated_sum_cents, difference_cents, document_id, status

## Frontend Runtime Layout
- `frontend/src/app/app.tsx` mounts the React app shell, the shared `DashboardSharedModals`, the shared `OrderDetailRuntime`, and the shared `BookingsGlobalRuntime`; it no longer mounts any hidden legacy dashboard host.
- `frontend/src/app/app-shell.tsx` owns the outer dashboard chrome, top-level navigation, search, marketplace/date controls, and shell-to-legacy event publication.
- `frontend/src/shared/runtime/dashboard-route.ts` maps `/analytics`, `/orders`, `/customers`, `/bookings/full`, `/google-ads`, and `/ebay` to route shells.
- The former `frontend/src/template/*-markup.ts` fragments and `ecommerce-dashboard/app/static/js/*.js` legacy boot bundle were removed in Phase 2; the active app boots only the Vite bundle plus shared CSS.

## Route Ownership Snapshot
- `analytics`: React-owned route; no route-specific bridge, no `LegacyDashboardHost`, no active legacy JS loader path.
- `orders`: React-owned route; `orders.js`, `react-orders-bridge.js`, and the old legacy JS boot bundle are removed; shared order-detail/modal coordination now runs through `DashboardRuntimeProvider`.
- `customers`: React-owned route; `customers.js`, `react-customers-bridge.js`, and the old legacy JS boot bundle are removed; only the external geo libs remain outside the normal React tree.
- `google-ads`: React-owned route; direct React API/data/actions, `google-ads.js` removed, no active legacy loader path.
- `ebay`: React-owned route; direct React API/data/filters, `ebay.js` removed, no active legacy loader path.
- `bookings`: React-owned route; React owns the `/bookings` panel markup, reads, visible mutations for transactions/templates/accounts/documents/monthly invoices, the transactions booking-class bar, unified new-button/tool toggles, monthly-invoice month picker/preview, Bookings table click ownership, and the booking transaction/monthly invoice detail content and mutations inside `#detailsContent`. Shared details, preview, order-detail return paths, and bookings refresh/UI state now flow through `DashboardRuntimeProvider`, `DashboardSharedModals`, `OrderDetailRuntime`, and `BookingsGlobalRuntime` without React-only globals or `ecomdash:*` events.

## Data Flow
1. **Bootstrap Sync**: On startup, `source_sync.py` copies source DBs from bootstrap paths (Shopify-API, Kaufland-API, Buchungen-Dashboard) to runtime paths.
2. **Live Sync**: Background worker (`live_sync.py`) polls Shopify/Kaufland APIs and writes to runtime source DBs.
3. **Order Loading**: `orders.py` service reads from runtime source DBs and merges with enrichments from `combined.sqlite3`.
4. **Analytics**: `analytics.py` aggregates merged order data and computes trends, heatmaps, and marketplace comparisons.
5. **Bookkeeping Sync**: `bookings.py` syncs combined orders into the bookkeeping DB as transactions.
6. **Frontend Shell**: FastAPI serves `frontend/dist/index.html`; React selects the route shell and renders the shared dashboard chrome.
7. **Shell State**: Shared shell filters and explicit refresh requests are owned by `DashboardShellStateProvider`; the shell no longer exposes React-only globals or `ecomdash:*` refresh events.
8. **Shared Runtime Integration**: `DashboardRuntimeProvider` owns the shared details/preview/order/bookings runtime. `DashboardSharedModals`, `OrderDetailRuntime`, and `BookingsGlobalRuntime` register and consume APIs there instead of coordinating through `window.__ECOM_DASH_REACT_*` globals or `ecomdash:*` events.
9. **Bookings Integration**: On `/bookings`, React owns the route panel markup, visible table click/detail/preview flows, bookings UI filter state, and explicit bookings refresh requests. The order-detail return path back from booking details is covered by Playwright.
10. **Verification Note**: Because FastAPI serves `frontend/dist/index.html`, frontend `build` and Playwright should be run sequentially when verifying route boots; running them in parallel can transiently serve an incomplete bundle and produce false 500s.

## External Dependencies
- **Shopify-API** project: Bootstrap DB at `WORKSPACE_ROOT/Shopify-API/shopify-dashboard/shopify_data.sqlite3`
- **Kaufland-API** project: Bootstrap DB at `WORKSPACE_ROOT/Kaufland-API/kaufland_data.sqlite3`
- **Buchungen-Dashboard** project: Bootstrap DB at `WORKSPACE_ROOT/Buchungen-Dashboard/data/dashboard.sqlite3`
