# Dashboard Backend API

This document is the agent-facing operational reference for the production
dashboard backend. It exists so agents can work directly against the backend
API instead of clicking through the UI.

## Maintenance Contract

This file must be updated in the same commit whenever any of the following
change:

- a route under `ecommerce-dashboard/app/routers/`
- a request or response shape used by those routes
- validation rules in services that affect agent calls
- a new dashboard capability that should be automatable via backend calls
- a destructive endpoint is newly allowed or disallowed for automation

Treat documentation drift here as a bug.

Primary source files:

- `ecommerce-dashboard/app/auth.py`
- `ecommerce-dashboard/app/routers/orders.py`
- `ecommerce-dashboard/app/routers/invoices.py`
- `ecommerce-dashboard/app/routers/bookings.py`
- `ecommerce-dashboard/app/routers/sync.py`
- `ecommerce-dashboard/app/routers/analytics.py`
- `ecommerce-dashboard/app/routers/customers.py`
- `ecommerce-dashboard/app/routers/google_ads.py`
- `ecommerce-dashboard/app/routers/exports.py`
- `ecommerce-dashboard/app/routers/kaufland_tickets.py`
- `ecommerce-dashboard/app/services/order_shipping.py`
- `ecommerce-dashboard/app/services/invoices.py`
- `ecommerce-dashboard/app/services/kaufland_tickets.py`
- `ecommerce-dashboard/app/services/importers/kaufland_tickets.py`
- `ecommerce-dashboard/app/services/bookkeeping_full.py`

## Runtime Target

- Production target: Home server dashboard backend
- Canonical production URL at the time of writing: `http://192.168.178.197:8012`
- Agents should prefer `DASHBOARD_BASE_URL` over a hardcoded URL.

Recommended environment variables:

```bash
export DASHBOARD_BASE_URL="http://192.168.178.197:8012"
export DASHBOARD_ADMIN_TOKEN="..."
```

## Authentication

Admin endpoints accept either of these headers:

- `X-Admin-Token: <token>`
- `Authorization: Bearer <token>`

Behavior:

- If `APP_ADMIN_TOKEN` is configured on the server, admin auth is required.
- If `APP_ADMIN_TOKEN` is empty on the server, admin endpoints are open.
- Agents should still send the token when available.

Agent rules:

- Never print the token in logs or summaries.
- Prefer `X-Admin-Token` because it is simpler to construct.
- If `DASHBOARD_ADMIN_TOKEN` is empty, omit auth headers instead of sending an empty token.

## Call Conventions

Preferred shell pattern:

```bash
curl --fail-with-body -sS
```

JSON request pattern:

```bash
curl --fail-with-body -sS \
  -H "Content-Type: application/json" \
  -H "X-Admin-Token: $DASHBOARD_ADMIN_TOKEN" \
  "$DASHBOARD_BASE_URL/api/..."
```

Multipart upload pattern:

```bash
curl --fail-with-body -sS \
  -H "X-Admin-Token: $DASHBOARD_ADMIN_TOKEN" \
  -F "file=@/absolute/path/to/file.pdf" \
  "$DASHBOARD_BASE_URL/api/..."
```

Operational rules:

- Read first, then mutate.
- After every successful mutation, perform a follow-up read to verify the change.
- Use backend endpoints only. Do not use browser/UI automation when an API exists.
- Use the documented route instead of direct DB edits.
- For file uploads, always use `multipart/form-data`.
- For JSON endpoints, always use `application/json`.

Helper scripts available in this repository:

- `scripts/dashboard-api/order-search.sh`
- `scripts/dashboard-api/set-tracking.sh`
- `scripts/dashboard-api/upload-order-invoice.sh`
- `scripts/dashboard-api/create-sales-invoice.sh`

Shared shell helper:

- `scripts/dashboard-api/_lib.sh`

## Allowed Automation Scope

This reference supports full non-destructive automation for:

- orders and filters
- order detail inspection
- shipment submission and tracking numbers
- purchase cost and supplier metadata
- order invoice PDF upload
- AliExpress mapping maintenance
- seller profile maintenance
- sales invoice draft, preview, creation, listing, detail, and PDF download
- bookkeeping lists and non-destructive writes
- transaction creation and patching
- payment accounts
- recurring templates
- document uploads
- monthly invoice create and patch
- sync status and sync execution
- analytics, customers, eBay, and Google Ads reads
- Amazon FBA SKU inventory reads
- Google Ads upload
- backup export download
- support ticket status, list/detail reads, sync, replies, close/open actions, attachment preview, and local note management

Blocked for this automation scope:

- `DELETE /api/bookings/transactions/{transaction_id}`
- `DELETE /api/bookings/monthly-invoices/{invoice_id}`
- `POST /api/exports/restore`
- `DELETE /api/google-ads/reset`

These endpoints exist, but this agent should not use them unless the policy in
this file is explicitly changed later.

## Orders Domain Model

Important order summary fields:

- `marketplace`: currently `shopify` or `kaufland`
- `order_id`: internal source order key used for `/api/orders/...`
- `external_order_id`: customer-facing order number when available
- `order_date`: ISO timestamp
- `customer`: display customer string
- `article`: first or representative article title
- `total_cents`: gross order value in cents
- `fees_cents`: fee total in cents
- `after_fees_cents`: net after fees in cents
- `sales_gross_cents`: steuerlich relevanter Brutto-Umsatz in cents
- `sales_net_cents`: steuerlich relevanter Netto-Umsatz in cents
- `sales_vat_cents`: enthaltene Ausgangs-USt in cents
- `purchase_cost_cents`: stored purchase cost in cents
- `purchase_vat_cents`: enthaltene Einkaufs-/Vorsteuer in cents
- `purchase_is_vat_deductible`: whether that purchase VAT is deductible
- `vat_applicable`: whether the order falls into the manually configured regular-VAT period
- `profit_cents`: `after_fees_cents - purchase_cost_cents`
- `fulfillment_status`: operational shipment state
- `financial_status`: payment/refund state if available
- `raw_status`: raw source-like status token
- `payment_method`: normalized payment label
- `invoice`: uploaded purchase invoice metadata if present

Important detail sections:

- `summary`: normalized order summary
- `order`: stored row payload
- `order_raw`: parsed raw source object
- `line_items`: Shopify line items
- `fulfillments`: Shopify fulfillments
- `refunds`: Shopify refunds
- `transactions`: Shopify transactions
- `units`: Kaufland order units
- `shipping_address`
- `billing_address`
- `customer`
- `bookkeeping_breakdown`
- `shipment_capabilities`

Order ID rule:

- Use `order_id` for `/api/orders/{marketplace}/{order_id}` routes.
- Use `external_order_id` for `/api/bookings/orders/{marketplace}/{external_order_id}/detail`.

## Orders API

### List Orders

- Method: `GET`
- Path: `/api/orders`
- Auth: no

Query params:

- `from`
- `to`
- `marketplace`
- `q`
- `status`
- `payment` (repeatable)
- `hide_canceled` (`true` or omitted)
- `has_purchase_cost` (`true` or omitted)
- `no_purchase_cost` (`true` or omitted)
- `has_invoice` (`true` or omitted)
- `no_invoice` (`true` or omitted)
- `limit` default `200`, max `5000`
- `offset` default `0`

Example:

```bash
curl --fail-with-body -sS \
  "$DASHBOARD_BASE_URL/api/orders?marketplace=kaufland&status=need_to_be_sent&has_purchase_cost=true&limit=100"
```

Response shape:

```json
{
  "total": 1,
  "items": [
    {
      "marketplace": "kaufland",
      "order_id": "ORDER-123",
      "external_order_id": "ORDER-123",
      "order_date": "2026-06-16T10:00:00Z",
      "customer": "Alice Example",
      "article": "Produkt A",
      "total_cents": 12990,
      "fees_cents": 1190,
      "after_fees_cents": 11800,
      "purchase_cost_cents": 5400,
      "profit_cents": 6400,
      "fulfillment_status": "need_to_be_sent",
      "financial_status": "",
      "raw_status": "need_to_be_sent",
      "payment_method": "Kaufland Settlement",
      "currency": "EUR"
    }
  ],
  "limit": 100,
  "offset": 0
}
```

### Get Order Detail

- Method: `GET`
- Path: `/api/orders/{marketplace}/{order_id}`
- Auth: no

Example:

```bash
curl --fail-with-body -sS \
  "$DASHBOARD_BASE_URL/api/orders/kaufland/ORDER-123"
```

Agent use:

- Always fetch detail before shipment writes.
- Read `shipment_capabilities` before attempting shipment.
- For Kaufland, read `units`.
- For Shopify, read `line_items`, `fulfillments`, and `transactions` if needed.

### Submit Shipment / Tracking Number

- Method: `PATCH`
- Path: `/api/orders/{marketplace}/{order_id}/shipment`
- Auth: admin
- Content-Type: `application/json`

Request body:

```json
{
  "carrier": "DHL",
  "tracking_number": "00340434161094000000"
}
```

Rules enforced by backend:

- `marketplace` must be `shopify` or `kaufland`
- `carrier` must exactly match an allowed carrier option list
- tracking number is required for almost all carriers
- for Kaufland, tracking number may be empty only for `Other` and `Other Hauler`
- tracking number must not contain line breaks
- backend refreshes the underlying source data and returns updated detail

Agent rules:

- Never write tracking data through any route other than this endpoint.
- Never invent or normalize carrier names beyond exact allowed values.
- Read `detail.shipment_capabilities.carrier_options` from order detail first and choose from that list.

Representative preferred carriers:

- Kaufland examples: `DHL`, `DHL Express`, `DPD`, `GLS`, `Hermes`, `UPS`, `Fedex`, `Deutsche Post`, `Other`, `Other Hauler`
- Shopify examples: `DHL`, `DHL Express`, `DPD`, `GLS`, `Hermes`, `UPS`, `FedEx`, `USPS`, `Deutsche Post`

Example:

```bash
curl --fail-with-body -sS \
  -X PATCH \
  -H "Content-Type: application/json" \
  -H "X-Admin-Token: $DASHBOARD_ADMIN_TOKEN" \
  -d '{"carrier":"DHL","tracking_number":"00340434161094000000"}' \
  "$DASHBOARD_BASE_URL/api/orders/kaufland/ORDER-123/shipment"
```

### Update Purchase Metadata

- Method: `PATCH`
- Path: `/api/orders/{marketplace}/{order_id}/purchase`
- Auth: admin
- Content-Type: `application/json`

Request body fields:

- `purchase_cost_eur`: float or `null`
- `purchase_vat_eur`: float or `null`
- `purchase_is_vat_deductible`: boolean
- `purchase_currency`: string, defaults to `EUR`
- `supplier_name`: string or `null`
- `purchase_notes`: string or `null`

Example:

```json
{
  "purchase_cost_eur": 54.90,
  "purchase_vat_eur": 0,
  "purchase_is_vat_deductible": false,
  "purchase_currency": "EUR",
  "supplier_name": "AliExpress Supplier",
  "purchase_notes": "Express line, June batch"
}
```

Agent rules:

- Send purchase cost in EUR units, not cents.
- After writing, re-read the order detail and verify `summary.purchase_cost_cents`, `summary.purchase_supplier`, and `summary.purchase_notes`.

### Upload Purchase Invoice PDF

- Method: `POST`
- Path: `/api/orders/{marketplace}/{order_id}/invoice`
- Auth: admin
- Content-Type: multipart

Form fields:

- `file` required
- `notes` optional
- `purchase_cost_eur` optional float
- `purchase_vat_eur` optional float
- `purchase_is_vat_deductible` optional boolean
- `purchase_currency` optional string
- `supplier_name` optional string

Important behavior:

- backend renames and stores the file
- backend also updates purchase enrichment in the same flow
- response is `{"ok": true, "enrichment": ..., "bookkeeping_sync": ...}`
- to inspect the resulting invoice metadata, re-read order detail and inspect `summary.invoice`

Example:

```bash
curl --fail-with-body -sS \
  -H "X-Admin-Token: $DASHBOARD_ADMIN_TOKEN" \
  -F "file=@/absolute/path/to/purchase-invoice.pdf" \
  -F "purchase_cost_eur=54.90" \
  -F "purchase_vat_eur=0" \
  -F "purchase_is_vat_deductible=false" \
  -F "purchase_currency=EUR" \
  -F "supplier_name=AliExpress Supplier" \
  -F "notes=Supplier invoice June batch" \
  "$DASHBOARD_BASE_URL/api/orders/kaufland/ORDER-123/invoice"
```

### Download Purchase Invoice

- Method: `GET`
- Path: `/api/orders/{marketplace}/{order_id}/invoice/{document_id}/download`
- Auth: no

Query param:

- `disposition=attachment|inline|preview`

### AliExpress Mappings

Read mappings:

- Method: `GET`
- Path: `/api/orders/{marketplace}/{order_id}/aliexpress-mappings`
- Auth: admin

Replace mappings:

- Method: `PUT`
- Path: `/api/orders/{marketplace}/{order_id}/aliexpress-mappings`
- Auth: admin

Request body:

```json
{
  "mappings": [
    {
      "aliexpress_order_id": "8192736455463621",
      "match_status": "matched",
      "match_confidence": 0.95,
      "match_method": "manual",
      "source": "manual",
      "note": "Confirmed against supplier invoice"
    }
  ]
}
```

## Sales Invoice API

### Seller Profile

Read:

- `GET /api/invoices/profile`

Write:

- `PUT /api/invoices/profile`

Payload fields:

- `legal_name`
- `street`
- `address_line2`
- `postcode`
- `city`
- `country`
- `email`
- `phone`
- `vat_id`
- `tax_number`
- `tax_mode`
- `vat_effective_from` ISO datetime string. Manual cutoff from which incoming orders are treated as VAT-applicable for reporting.
- `invoice_prefix`
- `default_template`
- `footer_note`
- `payment_note`
- `eu_invoicing_enabled`

Agent rule:

- Do not partially guess missing legal or tax data. If user requests profile changes, apply only what was explicitly given.

### VAT Report

- Method: `GET`
- Path: `/api/invoices/tax-report`
- Auth: admin

Query params:

- `month` in `YYYY-MM`

Behavior:

- refreshes `combined_orders` from source data before calculation
- uses `order_date` as the inclusion basis for order revenue within the month
- uses the manual seller profile field `vat_effective_from` as the VAT cutoff
- returns order output VAT, deductible purchase VAT, deductible monthly fee invoice VAT, deductible manual transaction VAT, and the resulting payable amount
- includes a read-only threshold candidate: the first order where cumulative gross turnover reaches `100000 EUR`

### List Sales Invoices

- Method: `GET`
- Path: `/api/invoices`
- Auth: admin

Query params:

- `from`
- `to`
- `marketplace`
- `q`
- `limit` default `120`, max `5000`
- `offset`

### Build Invoice Draft

- Method: `GET`
- Path: `/api/invoices/draft`
- Auth: admin

Query params:

- `marketplace` required
- `order_id` required
- `template_key` optional

Agent rules:

- Always read draft before creating a sales invoice.
- Inspect `validation.blockers`. If blockers are present, do not call create.

### Preview Invoice PDF

- Method: `GET`
- Path: `/api/invoices/preview.pdf`
- Auth: admin

Query params:

- `marketplace`
- `order_id`
- `template_key`

Returns PDF bytes.

### Create Sales Invoice

- Method: `POST`
- Path: `/api/invoices`
- Auth: admin
- Content-Type: `application/json`

Request body:

```json
{
  "marketplace": "kaufland",
  "order_id": "ORDER-123",
  "template_key": "clean"
}
```

Important behavior:

- backend rejects creation if draft has blockers
- backend rejects duplicates with HTTP `409`
- backend generates and stores a PDF
- response contains the created invoice object

Agent workflow:

1. `GET /api/invoices/draft`
2. optionally `GET /api/invoices/preview.pdf`
3. `POST /api/invoices`
4. `GET /api/invoices/{invoice_id}` to verify persistence when needed

### Get Sales Invoice Detail

- Method: `GET`
- Path: `/api/invoices/{invoice_id}`
- Auth: admin

### Download Sales Invoice PDF

- Method: `GET`
- Path: `/api/invoices/{invoice_id}/pdf`
- Auth: admin

Query param:

- `disposition=attachment|inline|preview`

## Bookings API

### List Booking Summary Rows

- Method: `GET`
- Path: `/api/bookings`
- Auth: no

Query params:

- `from`
- `to`
- `q`
- `limit` max `1000`
- `offset`

### Patch Booking Summary Row

- Method: `PATCH`
- Path: `/api/bookings/{booking_id}`
- Auth: admin

Payload:

```json
{
  "status": "confirmed",
  "reference": "Bank payout 2026-06-16",
  "notes": "Checked against source export"
}
```

### List Transactions

- Method: `GET`
- Path: `/api/bookings/transactions`
- Auth: no

Query params:

- `dateFrom`
- `dateTo`
- `marketplace`
- `q`
- `category`
- `type`
- `provider`
- `direction`
- `hasDocument`
- `orderId`
- `templateId`
- `paymentAccountId`
- `bookingClass`
- `limit` max `1000`
- `offset`

Transaction enums:

- `type`: `SALE`, `COGS`, `FEE`, `SHIPPING`, `SUBSCRIPTION`, `EXPENSE`, `REFUND`, `PAYOUT`, `ADJUSTMENT`
- `direction`: `IN`, `OUT`
- `source`: `api`, `manual`
- `status`: `pending`, `confirmed`, `reconciled`
- `booking_class`: `automatic`, `monthly`, `single`

### Create Transaction

- Method: `POST`
- Path: `/api/bookings/transactions`
- Auth: admin

Minimum practical payload:

```json
{
  "date": "2026-06-16T12:00:00Z",
  "type": "EXPENSE",
  "direction": "OUT",
  "amount_gross": 1299,
  "currency": "EUR",
  "provider": "OpenAI",
  "counterparty_name": "OpenAI",
  "category": "software",
  "reference": "invoice-2026-06",
  "notes": "Monthly tooling",
  "source": "manual",
  "status": "pending",
  "booking_class": "single"
}
```

Supported optional write fields:

- `vat_rate`
- `vat_amount`
- `amount_net`
- `is_vat_deductible`
- `order_id`
- `document_id`
- `template_id`
- `payment_account_id`
- `period_key`
- `source_key`

Important rules:

- `amount_gross` is cents, not float EUR.
- `provider` is required.
- linked IDs must be valid UUIDs and already exist.
- if `type` is `SUBSCRIPTION`, backend forces `booking_class` to `monthly`.

### Patch Transaction

- Method: `PATCH`
- Path: `/api/bookings/transactions/{transaction_id}`
- Auth: admin

Patchable fields:

- `date`
- `type`
- `direction`
- `amount_gross`
- `currency`
- `vat_rate`
- `vat_amount`
- `amount_net`
- `is_vat_deductible`
- `provider`
- `counterparty_name`
- `category`
- `reference`
- `order_id`
- `document_id`
- `template_id`
- `payment_account_id`
- `period_key`
- `notes`
- `status`
- `booking_class`

### Get Transaction

- Method: `GET`
- Path: `/api/bookings/transactions/{transaction_id}`
- Auth: no

### Sum Automatic Transactions

- Method: `GET`
- Path: `/api/bookings/transactions/sum`
- Auth: no

Query params:

- `provider`
- `periodFrom`
- `periodTo`

Useful providers for monthly invoice reconciliation:

- `paypal`
- `shopify_payments`
- `kaufland`
- `google_ads`
- `ebay`

### Payment Accounts

Read:

- `GET /api/bookings/payment-accounts`

Create:

- `POST /api/bookings/payment-accounts`

Patch:

- `PATCH /api/bookings/payment-accounts/{payment_account_id}`

Payload fields:

- `name` required on create
- `provider` optional nullable
- `is_active` boolean

### Recurring Templates

Read:

- `GET /api/bookings/templates`

Create:

- `POST /api/bookings/templates`

Patch:

- `PATCH /api/bookings/templates/{template_id}`

Generate transaction:

- `POST /api/bookings/templates/{template_id}/generate-transaction`

Template payload fields:

- `name`
- `type`
- `direction`
- `default_amount_gross`
- `currency`
- `provider`
- `counterparty_name`
- `category`
- `vat_rate`
- `payment_account_id`
- `schedule` in `monthly|quarterly|yearly`
- `day_of_month`
- `start_date`
- `active`
- `notes_default`

Generate payload:

```json
{
  "period_key": "2026-06",
  "date": "2026-06-30T12:00:00Z",
  "status": "pending"
}
```

### Documents

Read list:

- `GET /api/bookings/documents`

Upload:

- `POST /api/bookings/documents/upload`

Upload form fields:

- `file` required
- `notes` optional
- `transaction_id` optional
- `provider` optional
- `transaction_type` optional
- `booking_date` optional
- `amount_cents` optional integer string
- `currency` optional

Rules:

- if `transaction_id` is supplied, backend links the document to that transaction
- `amount_cents` is cents, not float EUR

Download:

- `GET /api/bookings/documents/{document_id}/download`

### Booking Orders Views

Read list:

- `GET /api/bookings/orders`

Read order breakdown:

- `GET /api/bookings/orders/{marketplace}/{external_order_id}/detail`

Use this when the task is about bookkeeping treatment of an order, not the raw order detail.

### Monthly Invoices

Read list:

- `GET /api/bookings/monthly-invoices`

Read single:

- `GET /api/bookings/monthly-invoices/{invoice_id}`

Create:

- `POST /api/bookings/monthly-invoices`

Patch:

- `PATCH /api/bookings/monthly-invoices/{invoice_id}`

Allowed provider values:

- `paypal`
- `shopify_payments`
- `kaufland`
- `google_ads`
- `ebay`

Create payload:

```json
{
  "provider": "kaufland",
  "period_from": "2026-06-01",
  "period_to": "2026-06-30",
  "invoice_amount_cents": 129900,
  "vat_amount_cents": 24700,
  "currency": "EUR",
  "document_id": "uuid-or-null",
  "notes": "June Kaufland fee invoice"
}
```

Behavior:

- backend computes `calculated_sum_cents`
- backend computes `difference_cents`
- status becomes `matched` or `mismatch`
- overlapping periods for the same provider are rejected with `409`

## Sync API

### Read Sync State

- `GET /api/sync/changestamp`
- `GET /api/sync/status`
- `GET /api/sync/live/status`
- `GET /api/sync/live/background/status`
- `GET /api/sync/credentials`

### Amazon Auto Refresh

- `GET /api/amazon/status` includes `auto_refresh` with worker state and independent `orders`, `finance`, `inventory_inbound`, and `reconcile` task states.
- `POST /api/amazon/auto-refresh/trigger` is admin-only and queues one quota-safe Amazon delta cycle.

Payload:

```json
{
  "reason": "manual"
}
```

The scheduler uses short delta windows and per-task backoff on Amazon `429`/`503` responses. It never runs the historical 730-day import automatically.

### Amazon FBA SKU Inventory

- `GET /api/amazon/inventory/skus?include_hidden=false` — no auth. Returns
  `{"ok": true, "items": [...]}`, one entry per SKU (`sku_key`, `seller_sku`,
  `asin`, `title`, `image_url`, `quantity_sold`, `sales_cents` (gross item
  price), `tax_cents`, `sales_net_cents` (`sales_cents` minus `tax_cents`),
  `fees_cents` (allocated Amazon fees — see below), `cogs_cents`,
  `margin_cents` (real profit: `sales_net_cents - cogs_cents - fees_cents`,
  not just revenue minus purchase cost), `margin_percent` (relative to
  `sales_net_cents`), `fulfillable_quantity`, `inbound_working_quantity`,
  `inbound_shipped_quantity`, `reserved_quantity`, `hidden`). Includes SKUs
  with stock but no sales yet. Amazon reports fees per order, not per line
  item, so each order's fees are split across its items proportionally by
  item revenue share — exact for single-SKU orders, an approximation for
  multi-SKU orders. By default, excludes SKUs the operator explicitly hid
  and "dormant" SKUs with zero stock AND zero sales (e.g. a stale order-item
  row for a discontinued product); pass `include_hidden=true` to see
  everything (dormant SKUs are always included once a SKU has any stock or
  sales history — the dormant filter only ever hides fully-zero-activity
  rows and has no separate toggle).
- `GET /api/amazon/inventory/skus/{sku_key}` — no auth. Same fields plus
  `fee_per_unit_cents` (`fees_cents / quantity_sold`), `quantity_sold_last_30_days`,
  `days_of_stock` (`null` when there is no recent sales velocity), and
  `shipments` (associated inbound shipments with quantity/status). Always
  resolves regardless of hidden or dormant status. Returns `404` when
  `sku_key` is unknown.
- `POST /api/amazon/inventory/skus/{sku_key}/hidden` — admin-only. Body
  `{"hidden": true|false}`. Persists an explicit show/hide preference for
  that SKU in the default listing.

### Source Sync

- Method: `POST`
- Path: `/api/sync/run`
- Auth: admin

Payload:

```json
{
  "force": false,
  "include_documents": true,
  "bookkeeping_bootstrap": false
}
```

### Live Sync Run

- Method: `POST`
- Path: `/api/sync/live/run`
- Auth: admin

Payload fields:

- `shopify`
- `kaufland`
- `shopify_status`
- `shopify_page_limit`
- `shopify_max_pages`
- `shopify_include_line_items`
- `shopify_include_fulfillments`
- `shopify_include_refunds`
- `shopify_include_transactions`
- `kaufland_storefront`
- `kaufland_page_limit`
- `kaufland_max_pages`
- `kaufland_include_returns`
- `kaufland_include_order_unit_details`

Recommended normal payload:

```json
{
  "shopify": true,
  "kaufland": true,
  "shopify_status": "any",
  "shopify_page_limit": 250,
  "shopify_max_pages": 500,
  "shopify_include_line_items": true,
  "shopify_include_fulfillments": true,
  "shopify_include_refunds": true,
  "shopify_include_transactions": true,
  "kaufland_storefront": "de",
  "kaufland_page_limit": 100,
  "kaufland_max_pages": 5000,
  "kaufland_include_returns": true,
  "kaufland_include_order_unit_details": true
}
```

### Trigger Background Live Sync

- Method: `POST`
- Path: `/api/sync/live/background/trigger`
- Auth: admin

Payload:

```json
{
  "reason": "api"
}
```

## Analytics API

- Method: `GET`
- Path: `/api/analytics/kpis`
- Auth: no

Query params:

- `from`
- `to`
- `marketplace`
- `q`
- `trendGranularity`

## Customers API

List customers:

- `GET /api/customers`

Query params:

- `from`
- `to`
- `marketplace`
- `q`
- `status`
- `limit`
- `offset`

Location map:

- `GET /api/customers/locations`

Additional query param:

- `refresh`

## eBay API

- `GET /api/ebay/orders`
- `GET /api/ebay/summary`

eBay orders query params:

- `shop`
- `category`
- `includeReturns`
- `limit`
- `offset`

## Kaufland Support Agent API

This is the canonical operational contract for autonomous Kaufland DE support
work. The agent may read tickets, manage local notes, send messages, open
tickets, and close tickets directly. Use API calls, not browser automation.

### Required Operating Sequence

1. Read `GET /api/kaufland-tickets/status`.
2. Run `POST /api/kaufland-tickets/sync/poll` when `last_sync` is missing,
   stale, or before selecting a working queue.
3. List the queue with `GET /api/kaufland-tickets?filter=todo`.
4. Read `GET /api/kaufland-tickets/{id_ticket}` immediately before every
   remote mutation.
5. After a successful message, open, or close action, poll again and re-read
   the relevant ticket detail before reporting completion.

Send `X-Admin-Token` on every support call, including reads, even though some
local read routes remain open when no dashboard token is configured.

### Status and Synchronization

#### Read local state

```bash
dashboard_api_get "/api/kaufland-tickets/status"
```

The response includes `configured`, `counts`, `runtime_db`, and `last_sync`.
Treat `last_sync` as the local freshness source of truth. A missing value, an
old completion timestamp, or a prior error requires a poll before making an
operational decision.

#### Incremental poll

```bash
dashboard_api_json POST "/api/kaufland-tickets/sync/poll" \
  '{"storefront":"de","include_closed":true,"page_limit":30,"max_pages":50,"lookback_minutes":60}'
```

`page_limit` must be between `1` and `30`. A successful HTTP response can
still contain `{"status":"partial"}`; treat that as incomplete and operate
only on tickets whose required detail is present. HTTP `502` means the
Kaufland provider or ticket sync failed; do not report a successful refresh.

#### Backfill

```bash
dashboard_api_json POST "/api/kaufland-tickets/sync/backfill" \
  '{"storefront":"de","include_closed":true,"page_limit":30,"max_pages":1000}'
```

Backfill is for initial loading or history repair. Do not use it as the normal
per-ticket refresh mechanism.

### Inbox and Detail Reads

#### List tickets

```bash
dashboard_api_get "/api/kaufland-tickets?filter=todo&limit=200&offset=0"
```

Supported `filter` values:

- `todo`: `status` is `opened` and `is_seller_responsible` is `true`.
- `waiting`: `status` is `opened` and `is_seller_responsible` is `false`.
- `closed`: every ticket whose status is not `opened`.
- `all`: every locally synchronized ticket.

Optional `q` searches ticket IDs, topic, reason, linked order-unit IDs, and
stored message text. The response contains `total`, `items`, `limit`, and
`offset`; each item includes local message/note counts and linked
`order_unit_ids`.

#### Read complete ticket context

```bash
ticket_id="T-100"
dashboard_api_get "/api/kaufland-tickets/${ticket_id}"
```

Read detail immediately before every mutation. The response has:

- `ticket`: normalized ticket row, including `status`,
  `is_seller_responsible`, timestamps, and linked order-unit count.
- `ticket_raw`: parsed Kaufland payload for fields not promoted below.
- `order_unit_ids`: linked Kaufland order-unit IDs.
- `messages`: complete locally synchronized conversation history in ascending
  timestamp order. `direction` is `outbound` for seller messages and `inbound`
  otherwise.
- `attachments`: metadata with `filename`, `uri`, and timestamp.
- `notes`: local-only internal notes.
- `order_context`: available dashboard order context for the linked Kaufland
  order, or `null` if it is unavailable locally.

Use normalized fields first. Do not decide from a cached list row, and do not
treat local `first_response_due_at` as an authoritative Kaufland SLA.

#### Preview an attachment

```bash
ticket_id="T-100"
filename="example.pdf"
encoded_filename="$(dashboard_urlencode "$filename")"
base_url="$(dashboard_api_base_url)"
dashboard_api_curl "$base_url/api/kaufland-tickets/${ticket_id}/attachments/${encoded_filename}/preview" \
  --output "$filename"
```

Preview fetches the remote Kaufland attachment only on demand. A `404` can mean
the local metadata is absent or the remote attachment URL is no longer
available. Never expose a returned attachment URL in notes or summaries.

### Local Internal Notes

Notes are visible only in the dashboard. They never reach Kaufland and must
not be reported as customer-visible actions.

```bash
ticket_id="T-100"
dashboard_api_get "/api/kaufland-tickets/${ticket_id}/notes"
dashboard_api_json POST "/api/kaufland-tickets/${ticket_id}/notes" \
  '{"note_text":"Checked current order context before replying."}'
dashboard_api_json PATCH "/api/kaufland-tickets/${ticket_id}/notes/note-1" \
  '{"note_text":"Updated internal handoff."}'
dashboard_api_json DELETE "/api/kaufland-tickets/${ticket_id}/notes/note-1"
```

Use a note before closing to record why the request is resolved. For a timeout
or uncertain remote write outcome, re-read ticket detail before retrying; add a
note only when it records a real operational fact, not speculative reasoning.

### Send a Customer Message

```bash
ticket_id="T-100"
base_url="$(dashboard_api_base_url)"
dashboard_api_curl \
  -X POST \
  -F 'text=Your shipment is being checked.' \
  -F 'interim_notice=true' \
  "$base_url/api/kaufland-tickets/${ticket_id}/messages"
```

`text` is required. `interim_notice` defaults to `false`; set it to `true`
only for an acknowledgement where seller responsibility must intentionally stay
open for a later follow-up. Before sending, compare the proposed answer with
the latest seller messages to avoid duplicates.

Attach one or more files with repeated `files` fields:

```bash
dashboard_api_curl \
  -X POST \
  -F 'text=Please find the requested document attached.' \
  -F 'interim_notice=false' \
  -F 'files=@/absolute/path/example.pdf;type=application/pdf' \
  "$base_url/api/kaufland-tickets/${ticket_id}/messages"
```

Each file must be at most 12 MiB and use one of these MIME types:

- `text/plain`
- `image/png`
- `image/jpeg`
- `image/gif`
- `image/tiff`
- `application/pdf`
- `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`
- `application/vnd.openxmlformats-officedocument.wordprocessingml.document`
- `application/msword`

After a successful send, poll and re-read the ticket. Kaufland normally moves
responsibility to the waiting side after a non-interim seller message.

### Open or Close a Ticket

#### Open

```bash
dashboard_api_json POST "/api/kaufland-tickets" \
  '{
    "id_order_unit":[314568008668014],
    "reason":"product_return",
    "message":"Please provide a return option."
  }'
```

All `id_order_unit` values must be numeric and belong to the same order. The
allowed `reason` values are:

- `product_not_as_described`
- `product_defect`
- `product_not_delivered`
- `product_return`
- `contact_other`

The message is customer-visible. Poll and read the created ticket after the
request succeeds.

#### Close

```bash
ticket_id="T-100"
dashboard_api_json PATCH "/api/kaufland-tickets/${ticket_id}/close"
```

Close only after the customer request is resolved or the seller is no longer
expected to act. Create a local rationale note first, then poll and re-read
the ticket to verify the resulting status.

### Error Handling

- `401`: token missing or invalid. Do not retry unchanged credentials.
- `404`: ticket, note, attachment metadata, or remote preview is unavailable.
  Re-poll before treating a ticket as absent.
- `422`: request shape, ticket reason, or attachment validation failed. Correct
  the payload before retrying.
- `502`: Kaufland provider or synchronization failed. Do not report success;
  wait for provider recovery, poll, and re-read before a retry.
- Successful HTTP with `status: partial`: incomplete sync. Report the partial
  result and do not assume unreturned ticket details are current.

## Google Ads API

Upload:

- Method: `POST`
- Path: `/api/google-ads/upload`
- Auth: admin

Multipart fields:

- `file` or `report_file`
- `assignment_file`

Read analytics:

- `GET /api/google-ads/analytics`

Read product detail:

- `GET /api/google-ads/product-detail?product_key=...`

Blocked for this automation scope:

- `DELETE /api/google-ads/reset`

## Exports API

Allowed:

- `GET /api/exports/backup`
- `GET /api/exports/period`

Blocked for this automation scope:

- `POST /api/exports/restore`

## Recommended Agent Workflows

## Helper Scripts

These shell helpers are intended for direct agent use against production.

### Search Orders

```bash
scripts/dashboard-api/order-search.sh --marketplace kaufland --status need_to_be_sent --limit 100
```

### Set Tracking

```bash
scripts/dashboard-api/set-tracking.sh kaufland ORDER-123 DHL 00340434161094000000
```

### Upload Purchase Invoice

```bash
scripts/dashboard-api/upload-order-invoice.sh kaufland ORDER-123 /absolute/path/to/invoice.pdf 54.90 EUR "AliExpress Supplier" "June batch"
```

### Draft or Create Sales Invoice

Draft only:

```bash
scripts/dashboard-api/create-sales-invoice.sh kaufland ORDER-123 clean --preview-only
```

Create:

```bash
scripts/dashboard-api/create-sales-invoice.sh kaufland ORDER-123 clean
```

### Set Tracking Number Correctly

1. `GET /api/orders/{marketplace}/{order_id}`
2. inspect `shipment_capabilities.available`
3. choose exact carrier from `shipment_capabilities.carrier_options`
4. `PATCH /api/orders/{marketplace}/{order_id}/shipment`
5. verify returned `detail` and optionally re-read the order

### Add Purchase Cost and Supplier

1. `GET /api/orders/{marketplace}/{order_id}`
2. `PATCH /api/orders/{marketplace}/{order_id}/purchase`
3. re-read order detail

### Upload Purchase Invoice PDF

1. verify the local file path exists
2. `POST /api/orders/{marketplace}/{order_id}/invoice`
3. `GET /api/orders/{marketplace}/{order_id}`
4. confirm `summary.invoice` and purchase enrichment fields

### Create Sales Invoice Safely

1. `GET /api/orders/{marketplace}/{order_id}`
2. `GET /api/invoices/draft`
3. if needed `GET /api/invoices/preview.pdf`
4. if no blockers, `POST /api/invoices`
5. `GET /api/invoices/{invoice_id}`

### Investigate Order Accounting

1. `GET /api/orders/{marketplace}/{order_id}`
2. extract `external_order_id`
3. `GET /api/bookings/orders/{marketplace}/{external_order_id}/detail`

## Error Expectations

Common backend error patterns:

- `400`: validation error, unsupported field, missing/invalid value
- `401`: admin auth required
- `404`: resource not found
- `409`: duplicate invoice, overlapping monthly invoice period, or other unique conflict
- `413`: uploaded file too large
- `500`: backend processing failure

Agent error handling rules:

- Report the exact backend error message when possible.
- Do not retry the same invalid payload blindly.
- For `409`, read the relevant resource and explain the conflict.
- For uploads, confirm the local path and file size before retrying.

## Update Checklist For Future Backend Changes

When adding or changing a dashboard capability, update all of these together:

- this file: `docs/dashboard-backend-api.md`
- skill summary: `.opencode/skills/dashboard-backend-api/SKILL.md`
- agent operating rules: `.opencode/agents/dashboard-production-api-operator.md`
- shell helpers under `scripts/dashboard-api/` if the capability is scriptable

Minimum review checklist:

- route path still correct
- auth requirement still correct
- query params still correct
- JSON or multipart payload still correct
- important response fields still correct
- any new validations or blocked actions documented
- any new automatable feature added to the allowed scope
