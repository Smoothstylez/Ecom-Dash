# Current State: E-Commerce Dashboard

## Project Status
- **Version**: 0.3.0 (config.py: `APP_VERSION = "0.3.0"`)
- **Last Updated**: 2026-03-16
- **Deployment**: Home Assistant add-on, Docker container

## Recently Completed Features
- Integrated bookkeeping module with transaction management, recurring templates, payment accounts
- Monthly invoice (Sammelrechnung) reconciliation for provider fee invoices
- Customer geo-visualization with Leaflet map and Globe.gl 3D hexbin
- Purchase cost enrichment per order with invoice document uploads
- Live sync background worker for Shopify and Kaufland API polling
- Google Ads cost tracking with product-level ROAS analysis
- Cross-device polling via changestamp mechanism
- Data backup/restore functionality with safety snapshots
- Theme support (light/dark mode)
- **Converted horizontal tabbar to vertical sidebar navigation**
- **Moved controls (Channel, Zeitraum, Suche) into sidebar**
- **Moved Buchungen sub-navigation into sidebar**

## Known Issues
- eBay module is legacy/read-only (no live sync)
- Customer geo-data requires external geocoding (postcodes resolved to coordinates)
- Shopify fee estimation used when transaction data unavailable
- Live sync requires external API projects (Shopify-API, Kaufland-API) for bootstrap data

## Active Work Areas
- UI Layout: Complete sidebar navigation implementation done

## Immediate Next Steps
1. None - all features working as expected
2. Review test coverage in `/home/luis/projects/Ecom-Dash/tests/`

## Configuration
- Port: 8012 (config.yaml ingress_port)
- Auto-sync on startup: enabled (`AUTO_SYNC_ON_STARTUP=1`)
- Live sync interval: configurable via `LIVE_SYNC_INTERVAL_SECONDS` env var (default 150s)
- Max upload size: 12MB (`MAX_UPLOAD_BYTES = 12 * 1024 * 1024`)

## External Project Dependencies
- Shopify-API project must exist at `WORKSPACE_ROOT/Shopify-API/` for bootstrap sync
- Kaufland-API project must exist at `WORKSPACE_ROOT/Kaufland-API/` for bootstrap sync
- Buchungen-Dashboard project must exist at `WORKSPACE_ROOT/Buchungen-Dashboard/` for bookkeeping bootstrap

## Pending Decisions
- None currently tracked
