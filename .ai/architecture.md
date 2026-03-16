# Architecture: E-Commerce Dashboard

## Tech Stack
- **Backend**: Python 3.11, FastAPI 0.116.1
- **Frontend**: Vanilla JavaScript (no framework), Chart.js 4.4.2, Leaflet 1.9.4, Globe.gl 2.41.6
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
│       ├── dashboard.html   # Single-page application shell
│       ├── css/             # themes.css, main.css
│       └── js/              # core.js (state, utils), analytics.js, orders.js, customers.js, bookings.js, google-ads.js, ebay.js, theme.js, init.js
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

## Data Flow
1. **Bootstrap Sync**: On startup, `source_sync.py` copies source DBs from bootstrap paths (Shopify-API, Kaufland-API, Buchungen-Dashboard) to runtime paths
2. **Live Sync**: Background worker (`live_sync.py`) polls Shopify/Kaufland APIs, writes to runtime source DBs
3. **Order Loading**: `orders.py` service reads from runtime source DBs, merges with enrichments from combined.sqlite3
4. **Analytics**: `analytics.py` aggregates merged order data, computes trends, heatmaps, marketplace comparisons
5. **Bookkeeping Sync**: `bookings.py` syncs combined orders into bookkeeping DB as transactions
6. **Frontend**: Single HTML page, JavaScript modules fetch via REST API, render charts/tables

## External Dependencies
- **Shopify-API** project: Bootstrap DB at `WORKSPACE_ROOT/Shopify-API/shopify-dashboard/shopify_data.sqlite3`
- **Kaufland-API** project: Bootstrap DB at `WORKSPACE_ROOT/Kaufland-API/kaufland_data.sqlite3`
- **Buchungen-Dashboard** project: Bootstrap DB at `WORKSPACE_ROOT/Buchungen-Dashboard/data/dashboard.sqlite3`
