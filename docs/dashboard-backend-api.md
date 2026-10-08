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

### Amazon FBA Inbound Shipments and Supplier Invoices

- `GET /api/amazon/inbound/shipments` — no auth. Returns
  `{"ok": true, "items": [...]}`. The default listing excludes shipments with
  Amazon status `CANCELLED`; use `?status=CANCELLED` to retrieve them explicitly.
  Each item includes the normalized shipment status plus `invoice_count`,
  `allocation_count`, and `cost_status`: `missing` when no invoice is recorded,
  `entered` when at least one invoice exists but no product-cost allocation has
  been confirmed, and `confirmed` when allocations exist for the shipment.
- `GET /api/amazon/inbound/shipments/{shipment_id}` — no auth. Returns the
  shipment, items, boxes, transport options, costs, cost allocations, invoice
  headers, and invoice lines. Every invoice header includes `supplier_name`,
  `invoice_number`, `invoice_date`, `currency`, `gross_cents`, `net_cents`,
  `vat_cents`, `document_path`, and `notes`. Every invoice line includes its
  `invoice_id`, SKU/FNSKU identity, quantity, and `gross_cents`, `net_cents`,
  and `vat_cents`.
- `POST /api/amazon/inbound/invoices/{invoice_id}/lines` — admin-only. Upserts
  the line identified by its invoice and SKU/FNSKU. The JSON body requires
  `quantity` (at least 1), `gross_cents`, `net_cents`, and `vat_cents` (all
  non-negative); optional fields are `seller_sku`, `fnsku`, `asin`, and `title`.
  `gross_cents` must equal `net_cents + vat_cents`, otherwise the endpoint
  returns `400`. One line covers one shipment SKU/FNSKU, and all invoice lines
  selected for an invoice must sum exactly to that invoice header's
  `gross_cents`, `net_cents`, and `vat_cents`.
- Cost confirmation accepts either one combined supplier invoice covering the
  shipment or multiple invoices that are specific to individual shipment SKUs.
  Each invoice header and its selected lines must provide gross, net, and VAT
  totals, and the selected lines' sums must match the corresponding header
  before allocations can be confirmed.

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

## USt Report API

Monthly German VAT report, input VAT ledger and Kaufland rate corrections.
All routes require `X-Admin-Token`.

### Independent Finance checks (2026-10)

Original tax reports and confirmed invoices determine tax totals. Finance-only
gaps, mismatches and API-source failures are warnings, never filing blockers
and never a reason to change a confirmed invoice or create extra VAT bookings.
`AMAZON_TAX_DATA_INCOMPLETE` and `AMAZON_SOURCE_UNAVAILABLE` are informational
Finance-check warnings; actual unresolved tax classifications, cutoff dates,
conflicting invoice records and unreadable booked-VAT sources remain blockers.

`sections.finance_reconciliation` is a read-only Amazon fee check:

- `status`: `matched`, `explained` (proven month shifts), `differences`, `incomplete`.
- `comparisons`: native `currency`, fee `category`, `invoice_cents`,
  `finance_cents`, signed `difference_cents` (invoice minus Finance).
- `details`: order/category-level charge or documented credit differences;
  opposing differences are retained even if aggregate sums match.
- `timing`: lifecycle `event_id`, `order_id`, `activity_month`, `release_month`,
  `currency`, `fees_cents`. Released amounts counted once with original
  lifecycle activity; release dates are not substituted for tax periods.
- `issues`: missing coverage/dates, unknown fee types or incompatible lifecycle
  components. `excluded_ads_cents`: separate EUR advertising payments.
- `tax_amounts_changed: false`, `source: stored_finance`.

Modern Finance is not summed with settlement representations. Only separately
identified native subscription evidence fills a missing modern subscription
stream; equal amount/date alone never merges modern economic transactions.
Base/Tax/Promo nodes are subdivisions, not additive fees. GBP is compared in
GBP, not estimated EUR. Confirmed invoice position amounts are for checking;
their original header VAT continues to determine tax totals.

`FINANCE_RECONCILIATION_INCOMPLETE` and `FEE_RECONCILIATION_DIFFERENCE` remain
warnings even when `block_filing_when_input_vat_incomplete=true`. That option
only escalates genuinely missing fee-document findings, not control differences.
The automatic check uses stored data; `matched` does not claim a fresh remote
sync, nor validate the seller's tax-regime settings.

### Periodization (authoritative)

Input VAT is deductible in the first period in which the service has been
performed AND a proper invoice is available (UStAE):

```
service_month = month(service_date) or month(period_to or period_from or invoice_date)
docs_month    = month(received_date or invoice_date)
deduction_month = max(service_month, docs_month)
```

`service_date` / `delivery_date` is a real delivery/performance date and wins
over `period_to` and `invoice_date`. The invoice date is never assumed to be
the service date. Only monthly platform fees may use `period_to` as the
service end.

Output VAT and revenue follow the delivery/transaction month. Returns and
refunds follow their own booking month.

The seller's `vat_effective_from` preserves its full UTC timestamp. The
threshold order itself is included (`order timestamp >= cutoff`). Pre-cutoff
sales carry `pre_vat`, gross equals net, and output VAT is zero, regardless of
marketplace rate fields. Amazon corrections inherit the original shipment's
seller eligibility, even in a later month. Exact Amazon order timestamps take
precedence over date-only tax-report order dates; an ambiguous cutoff-day
order blocks filing. Raw imported tax components remain unchanged.

Kaufland refunds are separate negative rows (`transaction_type: "REFUND"`)
dated from the refund's own raw booking/creation date. A sync timestamp or
return-request date is not used as a substitute. Undated refunds are not
deducted retrospectively from the sale; affected reports remain blocked.

Booked input VAT uses the same deduction-month resolver as the USt invoice
ledger. Approved monthly invoices are counted once, in their deduction month,
not in every service month. Their linked transactions and duplicates of
`(provider, invoice_number)` already in the USt ledger are excluded. Booked
amounts are included in `purchases_cents`, `amazon_fees_cents`,
`kaufland_fees_cents`, or `other_cents`; `booked_fee_vat_cents` is informational
and MUST NOT be added again to those buckets.

Platform-fee eligibility is separately projected against the VAT cutoff.
Consumed pre-cutoff services are not deductible; order-related fees and
credits use the original order's eligibility. Shared monthly services use
their documented service end. Explicit manual partial allocations are
preserved. Original invoice totals/confirmations are not rewritten.
`GET /api/ust-report/documents` additionally exposes
`effective_deductible_vat_cents`, `eligibility_adjustment_cents`, and
`eligibility_review_count`; the report's input-VAT section aggregates the
last two fields. Effective deduction can exceed an invoice's NET signed VAT
when credits of previously non-deductible pre-cutoff fees are excluded.
An unproven original credit or cutoff-day allocation blocks filing with
`INPUT_VAT_ELIGIBILITY_UNRESOLVED`; adjustments are disclosed as
`INPUT_VAT_START_ADJUSTMENT`.

### Classification

Amazon `SC_VAT_TAX_REPORT` rows are classified in this order:

1. `RETURN` / `REFUND` -- inherits the original SHIPMENT's class via
   `(Order ID, Shipment ID, SKU)`
2. `deemed_supplier` (Tax Collection Responsibility = Amazon)
3. `export`
4. `de_b2c` (DE -> DE at 19%, Amazon's own tax components are authoritative)
5. `eu_b2b_intra_community_supply` (DE -> EU, foreign EU VAT country prefix,
   VAT registration type, Taxable, 0%; Amazon's report provides the exemption
   evidence, the classifier does not perform a live VIES validation)
6. `eu_b2c_home_rate` (EU B2C without VAT ID; `net = round_half_up(gross / 1.19)`)
7. `unresolved`

Kaufland `vat` is a PERCENTAGE (19.0 / 0.0), not an amount. The tax base is the
customer gross (`price + shipping_rate`) net of refunds.

### Blocking policy

Hard blockers (prevent `filed`):

- `AMAZON_UNRESOLVED`
- `KAUFLAND_RATE_NEEDS_OVERRIDE`
- `INPUT_VAT_PENDING_REVIEW`
- `TAX_MODE_NOT_REGULAR`
- `NO_VAT_START_DATE`
- `UNRESOLVED_RETURN_LINK`
- `AMAZON_VAT_START_AMBIGUOUS` (no exact timestamp for a cutoff-day order)
- `KAUFLAND_REFUND_DATE_MISSING` (refund cannot be assigned to a month)
- `INPUT_VAT_SOURCE_UNAVAILABLE` (bookkeeping VAT could not be inspected)
- `INPUT_VAT_INVOICE_CONFLICT` (duplicate invoice representations disagree)
- `AMAZON_TRANSACTION_DATE_MISSING` (transaction has no proven booking date)
- `INPUT_VAT_ELIGIBILITY_UNRESOLVED` (fee/credit transition allocation unproven)

`sections.amazon_reconciliation` contains `missing_order_ids`,
`amount_mismatches` (`order_id`, `kind`, `source_gross_cents`,
`tax_gross_cents`), and `source_error`. `AMAZON_TAX_DATA_INCOMPLETE` and
`AMAZON_SOURCE_UNAVAILABLE` are warnings from this independent cross-check,
not hard blockers. A missing order or Finance/API outage does not replace,
rewrite, or prevent filing a valid original Amazon tax report. The original
tax-report rows determine tax totals; Finance figures are completeness evidence
and must never be substituted for missing tax rows or tax classifications.
Actual unresolved classifications, ambiguous cutoff dates, undated corrections,
and conflicting input invoices retain their explicit blockers. Earlier lifecycle
states supply period evidence without contributing amounts twice. Promotional
shipping rebates reduce customer gross; ordinary fees do not. Synthetic
finance-order dates are never treated as actual purchase dates.
Amazon's `Sept` month spelling is supported, including August orders shipped
in September. Corrections without a shipment/transaction date never fall back
to their original order date and are excluded from period totals until resolved.

Contradictory duplicate input invoices (approval, deduction month, or VAT
amount) block filing rather than silently suppressing a deduction. Non-EUR
booked invoices use proven `vat_cents_eur`; non-EUR deductible transactions
without EUR conversion block the source check.

Imports identify economic transactions by `(Transaction ID, Order ID,
Shipment ID, SKU, Transaction Type)`. Changed invoice URLs/metadata update the
existing transaction and do not create another sale. Reimports also resolve
legacy content-hash IDs; reclassification supports both ID generations.

Soft warnings: `MISSING_FEE_INVOICE` (also sets `input_vat_incomplete`), and
`AMAZON_VAT_CALCULATION_MISSING`. A missing fee invoice only means the input
VAT is not yet deductible -- it does not make the output VAT wrong -- so it
does NOT block filing unless the named business rule
`block_filing_when_input_vat_incomplete` is turned on (default `false`).
`FEE_RECONCILIATION_DIFFERENCE` is an informational Finance control warning;
it does not mark input VAT incomplete and is never promoted by the optional
input-VAT blocking rule. It is not proof that a specific invoice is missing.
`FINANCE_RECONCILIATION_INCOMPLETE` means an API source, fee type, or source
link could not be verified; it also remains informational. `FEE_SOURCE_ESTIMATE_DIFFERENCE` only compares a
Kaufland order-derived estimate with an already present full-month statement;
the statement is authoritative and this informational difference does not
mark its input VAT incomplete.

### Filing lifecycle

`filed` snapshots are immutable. Corrections go through `POST .../amend`,
which appends a new revision with `kind: "amendment"` and `supersedes_id`
pointing at the filed original.

### Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/ust-report?month=YYYY-MM` | Report (live, or the latest filed snapshot) |
| GET | `/api/ust-report/months` | `{items, total}` of revisions |
| POST | `/api/ust-report/import` | Multipart `files` (up to 25): automatic content recognition, PDF/CSV pairing, assignment and safe booking |
| GET | `/api/ust-report/imports` | Latest 30 persisted file-import results |
| POST | `/api/ust-report/parse-upload` | Multipart `file`: genuine binary PDF extraction and preview without booking |

PDF extraction uses Poppler's `pdftotext -layout`; the add-on image installs
`poppler-utils` and checks for the executable at build time. A missing
`pdftotext` in an older image returns `503` with an add-on-update hint. A PDF
that exists but cannot be read remains a client-side `400` error.
| POST | `/api/ust-report/{month}/refresh` | Recompute without filing |
| POST | `/api/ust-report/{month}/file` | File; `409` while blockers remain |
| POST | `/api/ust-report/{month}/amend` | Append an amendment revision |
| POST | `/api/ust-report/documents` | `multipart/form-data` input VAT invoice |
| GET | `/api/ust-report/documents?month=&provider=&input_vat_status=` | `{items, total}` |
| GET | `/api/ust-report/documents/{id}/download` | PDF |
| PATCH | `/api/ust-report/documents/{id}` | `input_vat_status`, `received_date`, `service_date`, `notes` |
| POST | `/api/ust-report/kaufland-overrides` | `{id_order_unit, to_rate, reason}` |
| POST | `/api/ust-report/kaufland-overrides/bulk` | `{month, reason, to_rate}` |
| POST | `/api/ust-report/settings` | `eu_tax_regime`, EU distance totals |
| POST | `/api/amazon/tax-report/request` | Request `SC_VAT_TAX_REPORT` |
| POST | `/api/amazon/tax-report/{report_id}/import` | Fetch, classify, persist |

Amazon upstream errors (including missing report permissions / SP-API `403`)
return `502` with `detail`, not an unhandled `500`. This is not a successful
request/import; do not clear completeness blockers. A Seller Central CSV can
be uploaded through `/api/amazon/pool/tax-report` when API permissions are absent.

`POST /api/amazon/pool/tax-report` accepts a UTF-8 CSV/TSV `file` from either
`SC_VAT_TAX_REPORT` or the Seller Central `VAT_TRANSACTION` report (AVTR).
It stores raw evidence in `pool_tax_rows` and additionally classifies and
persists customer sales/corrections into `amazon_tax_rows`. AVTR rows retain
their original fields under `_avtr_raw` and their source type under
`_source_report_type`; fees, stock transfers and other non-customer activities
are not imported as sales. AVTR supplements a missing VCS shipment, never
duplicates a matching VCS transaction. Cross-report correspondence requires
one candidate on each side of the order/SKU/type/date group; amounts are
validation evidence rather than identity, so authoritative amount corrections
replace the supplemental row. Multiple possible shipment matches are retained
as unresolved evidence and block filing instead of silently deleting or
skipping distinct shipments. A later uniquely matching VCS import replaces
the supplemental representation. Refunds inherit the original's computed
standard-rate treatment when their own AVTR VAT calculation is absent.

For pre-VCS domestic DE-to-DE standard goods, a SELLER-responsibility AVTR
sale with proven EUR customer gross but no Amazon VAT calculation is computed
at the German standard rate (19%, `net_source: "computed_home_rate"`), with
`AMAZON_VAT_CALCULATION_MISSING`. Explicit non-standard product tax codes or
unproven foreign exemptions remain unresolved. Seller cutoff eligibility
still applies at report time; synthetic finance purchase dates are excluded.

`MISSING_FEE_INVOICE` checks Kaufland order-derived fees against the invoice's
service month, not its deduction month. A July invoice deducted in August does
not cover August fees. Confirmed USt-ledger invoices and approved bookkeeping
invoices can establish coverage; the warning never creates a deduction by
itself. Amazon invoice/Finance coverage is displayed independently under
`sections.finance_reconciliation` and does not alter the report's tax totals.

### Unified upload workflow

The USt page's **Reports und Belege importieren** accepts original invoice PDFs,
Amazon VCS/AVTR tax CSVs, and invoice-specific fee CSVs. File CONTENT determines
the importer, not a manually chosen provider or the filename. CSV/PDF pairs
are matched by invoice number, including across separate requests and either
upload order. Original files and processing results are persisted by SHA-256.

Each response item includes `id`, `filename`, `kind`, `status`, `reasons`, and
when known `invoice_number`, `provider`, `deduction_month`, `document_id`, or
tax-report `months`. Fee CSVs expose `invoice_numbers` and `pairings` with
`invoice_number`, `pdf_filename`, `document_id`, `deduction_month`, `status`,
and `reasons` per invoice. Statuses: `approved`, `tax_imported`, `paired`, `duplicate`,
`waiting_pdf`, `paired_review`, `needs_review`, `conflict`, `evidence_only`, `error`.
`paired_review` means the original PDF exists but its invoice or the CSV needs
review. `waiting_pdf` names the still-missing invoice numbers; a multi-invoice
CSV retains its already found pairings. Existing bookkeeping PDFs are matched
too, including files uploaded before the unified importer. Duplicate/review
invoice outcomes update the CSV status and month. A repeated PDF/fee CSV is
re-evaluated idempotently; already imported tax reports remain duplicates.
Unknown-position reasons use `unbekannte_position:<label>:<count>` (one reason
per distinct label), preserving the actual labels and number of positions.
HTTP 200 for a processed batch is not a declaration that every file succeeded;
clients MUST display each item's status and reasons. Empty/oversized batches
return 400/413. Total request payload is limited by `MAX_UPLOAD_BYTES`.

An unambiguous supported invoice with matching sums, complete header data,
proven EUR VAT conversion where needed, and clear transition eligibility is
automatically drafted and approved through the existing platform-invoice
services. Unclear invoices remain drafts; unknown files and conflicting
approved invoice data never create guessed bookings. Recognized sales PDFs
are evidence only; tax-report values are not duplicated as input VAT.
Repeated files/invoice identities do not create another document or booking.
An invoice-specific CSV can improve and complete a previous waiting draft;
an approved invoice is never overwritten by changed amounts/periods.

Amazon credits preserve every documented original invoice reference in
`original_invoice_numbers` (stored as `original_invoice_numbers_json`), with
the first reference retained as `original_invoice_number` for compatibility.
Without order-level allocation, only confirmed positive fee originals with
unanimous fully deductible or fully non-deductible treatment allow automatic
inheritance. Missing, mixed, partially allocated, later-dated or credit-chain
references remain review cases. The report uses the same eligibility projection.
`shipping_chargeback` is an order-related FBA shipping-cost charge; it is not
inventory removal. `inventory_removal` is a separate FBA inventory-removal
service, assessed using its service date rather than a customer-order ID.

`GET /documents` presents both legacy USt invoices and bookkeeping platform
invoices in one list. Bookkeeping IDs have `book:` prefix. Their originals
use the same `/documents/{id}/download`; explicit review confirmation uses
`PATCH /documents/{id}` with `input_vat_status: "confirmed"`. Raw invoice
amounts preserve native currency, effective deductible VAT is in EUR.
Legacy invoice identities take precedence to prevent duplicate presentation.

The bookkeeping PDF **Auslesen** action now posts binary PDF to `parse-upload`
instead of treating PDF bytes as plain text. Saving a supported selected file,
or uploading one without an explicit existing-transaction attachment, uses
the same automatic import workflow. Explicit transaction attachments and
manual entries retain their dedicated behavior. The upload never files or
changes an immutable filed USt snapshot.

Helper: `bash scripts/dashboard-api/import-ust-files.sh invoice.pdf fees.csv`.
Set `DASHBOARD_BASE_URL` to the intended LOCAL/Tailscale dashboard.

### Input VAT documents

Form fields for `POST /api/ust-report/documents`:

`file`, `provider` (`amazon`/`kaufland`/`other`), `doc_type`
(`fee`/`purchase`/`damage_compensation`/`other`), `invoice_number`,
`invoice_date`, `received_date`, `service_date`, `period_from`, `period_to`,
`currency`, `gross_cents`, `net_cents`, `vat_cents`, `deductible_vat_cents`,
`notes`.

Validation: `gross_cents == net_cents + vat_cents`, `deductible_vat_cents <=
vat_cents`, no negative amounts, and `damage_compensation` must carry no VAT.
Only `input_vat_status = "confirmed"` documents are deductible. Duplicate
`(provider, invoice_number)` returns `409`; identical file bytes return `409`
and leave no orphan file.

### EU distance-selling threshold

`EU_DISTANCE_SELLING_THRESHOLD_CENTS = 10000 * 100` (10.000 EUR). This is a
separate constant from the section-19 threshold (`VAT_THRESHOLD_CENTS =
100000 * 100`). The home VAT rate is only allowed when `eu_tax_regime` is
`home_rate_under_threshold` AND both the prior and the current year are below
the threshold; otherwise those cases stay `unresolved` and block filing.

### Restricted Data Tokens

`SC_VAT_TAX_REPORT` and the VAT Invoice Data Reports are restricted. For
restricted operations the RDT is sent as `x-amz-access-token` **instead of**
the LWA token -- there is no separate `RestrictedDataToken` header on the
SP-API. `report_requires_rdt()` declares which report types need one.

## Eingangsrechnungen (Plattform-Rechnungen)

Upload, automatisches Auslesen, Abgleich gegen die gebuchten Gebuehren und
ausdrueckliche Freigabe. Deckt Kaufland- und Amazon-Belege ab, spaeter auch
Wareneinkauf. Die Parsing-Regeln stehen in `docs/platform-invoice-parsing.md`.

**Ablauf: parse -> draft -> approve.** Kein Schritt darf uebersprungen werden:

1. `POST /api/bookings/monthly-invoices/parse` liest den Beleg und liefert die
   Vorbefuellung. Es wird **nichts gespeichert**.
2. `POST /api/bookings/monthly-invoices/draft` legt ab, Status `needs_review`.
   Weiterhin **keine Buchung**.
3. `POST /api/bookings/monthly-invoices/{id}/approve` ist der **einzige** Weg,
   der etwas ins Ledger schreibt.

Ein KI-Agent soll bei `parse_confidence < 1` oder nicht-leeren
`needs_review_reasons` dem Menschen zur Freigabe vorlegen, nicht selbst
freigeben.

### Parse (Prefill, ohne Anlegen)

`POST /api/bookings/monthly-invoices/parse`

Request (JSON): `pdf_text` (Belegtext) **oder** `pdf_path` (Datei auf dem
Server), optional `csv_text` (Amazon-Fee-CSV, bevorzugt neben dem PDF).

```json
{
  "pdf_text": "Rechnungs Nr.: R0726-...",
  "csv_text": "\"Transaction Date\",..."
}
```

Antwort: `parsed` mit Kopfdaten, `lines` (je Position Netto/USt/Brutto),
`parse_confidence` (0..1), `needs_review_reasons` (Liste) und `preview`
(`expected_cents`, `invoice_cents`, `difference_cents`, `has_expected`).

Bei einer zugehoerigen Amazon-Gebuehren-CSV werden die PDF-Positionen ersetzt
und positionsbezogene Pruefhinweise neu berechnet. Eine alte, unbekannte
PDF-Sammelposition bleibt nicht als Freigabehindernis stehen, wenn die CSV
eindeutige Positionen liefert und ihre Summen mit dem PDF uebereinstimmen.
Echte Summenabweichungen, fehlende Kopfdaten und unabhaengige Parserhinweise
bleiben pruefpflichtig. `parse` erzeugt weiterhin keine Buchung.

### Draft (ablegen, ohne Buchung)

`POST /api/bookings/monthly-invoices/draft`

Request: `parsed` (das Objekt aus `parse`), optional `document_id`, `notes`,
`invoice_amount_cents`, `vat_amount_cents` (ueberschreiben die geparsten Werte).

Antwort: `invoice` mit Status `needs_review`.

### Approve (die Freigabe)

`POST /api/bookings/monthly-invoices/{id}/approve`

Request (optional): `{"create_missing_bookings": true}`.

Bucht die Positionen ohne Automatik (Grundgebuehr, Werbung, Einzelbelege),
laesst die orderbezogene Verkaufsprovision unangetastet und gleicht eine
Abweichung mit einer eigenen `ADJUSTMENT`-Buchung
(`category = invoice_variance`, Referenz `Abweichung Sammelrechnung …`) aus.
Antwort: `invoice` mit `approved_transaction_ids` und `had_variance`.

**Idempotent** ueber `source_key` -- ein erneuter Aufruf bucht nicht doppelt.

Mehrere Rechnungen desselben Providers im selben Zeitraum werden jeweils
gegen ihre eigenen verknuepften Buchungen abgeglichen. Bereits einer anderen
Rechnung zugeordnete Provisionen werden nicht erneut verwendet; externe
Amazon-Bestellnummern werden ueber `orders.external_order_id` aufgeloest.
Spaeter eintreffende Buchungen erzeugen bei erneutem Abgleich eine eigene,
idempotente Korrekturdifferenz. Der tatsaechliche verknuepfte Saldo wird vor
dem Status-Update geprueft; bestehende Buchungen bleiben unveraendert.

Originale Amazon-Steuergutschriften mit Ursprungsbezug duerfen negative
Rechnungs- und Steuerbetraege tragen. Im Transaktionsledger bleiben Betraege
positiv: Rueckzahlungen und negative Korrekturen werden als `direction=IN`
gebucht; Ausgaben als `OUT`. Rechnung und Vorsteuerkorrektur behalten ihr
Vorzeichen. Nullsummen-Positionsgruppen erzeugen keine Nullbetrag-Buchung.

### Statuswerte

`draft` -> `needs_review` -> `approved`, daneben weiter `matched` / `mismatch`
aus dem bisherigen Abgleich. Alte Zeilen mit `draft` bleiben gueltig.

### Verhalten bei Abweichung

Der Rechnungsbetrag gilt am Ende. Bestehende Transaktionen werden nie
ueberschrieben; die Differenz wird als eigene, sichtbare Korrekturbuchung
erganzt und braucht eine eigene Freigabe.

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

- `scripts/dashboard-api/file-ust-report.sh` is the helper for the USt report.

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

### Amazon Einkaufspool (2026-09-22)

The new entry point is **Amazon → Einkaufspool**. Purchasing is independent of
FBA shipments. A product can have multiple explicit marketplace/Seller-SKU/ASIN
mappings. Never infer equivalence from a title. FIFO is the sales cost method;
weighted average home-stock cost is a display metric, not the sales valuation.

All `/api/amazon/pool` routes use existing admin authentication. Money fields are
integer cents; quantities are positive integers. Every command below, except
reconciliation and file imports, requires a caller-generated `request_id`.
**Reuse the same ID and identical payload after a timeout.** Reuse with changed
payload fails. Commands and their audit/result are committed atomically.
Validation failures return 400; malformed request shapes return 422.

| Method / suffix | Purpose |
|---|---|
| GET (empty suffix) | Products, listing mappings, invoices/lines, receipts, transfers/lines, payments, documents, recent movements, revisions, legacy invoices, listing and shipment metrics, receipt discrepancies |
| POST `/products` | `{request_id,name}` creates an internal product |
| POST `/listings` | `{request_id,product_id,marketplace_id,seller_sku,asin}` maps a listing; existing product identities cannot silently change |
| POST `/invoices` | Creates invoice and product positions; does not create stock or assert payment |
| POST `/receipts` | `{request_id,line_id,quantity,received_at}` records actual partial delivery to own stock |
| POST `/transfers` | Reserves FIFO own stock for Amazon shipment items |
| POST `/transfers/action` | Dispatches or releases a reserved transfer |
| GET `/reconcile-preview` | Current receipt differences and economic listing metrics before reconciliation |
| POST `/reconcile` | Idempotently books Amazon receipt deltas and allocates shipped order quantities; returns `issues` and `cost_issues` for missing history; synchronizes documents/payments to bookkeeping |
| POST `/payments` | `{request_id,invoice_id,paid_at,amount_cents,account_reference}` records an actual payment in invoice currency; partial payments are separate records |
| POST `/adjustments` | `{request_id,receipt_id,quantity,occurred_at,reason}` documents own-stock writeoff; cannot consume reserved stock |
| POST `/revisions` | Preview or apply an audited cost/tax correction, including already allocated sales costs |
| POST `/legacy-migration` | `{request_id,legacy_invoice_id,marketplace_id,received_at}` adopts a complete EUR legacy shipment invoice after product mapping; existing lots/allocations retain their identities |
| POST `/invoices/{id}/documents` | Multipart `file`; stores under shared bookkeeping document root; content hash prevents repeated attachment |
| GET `/documents/{id}` | Authenticated document download |
| POST `/tax-report` | Multipart `file` containing UTF-8 Amazon `SC_VAT_TAX_REPORT` CSV; preserves original rows for later tax review, does not change seller tax settings |

Invoice example (nine units, 107.10 EUR gross, 17.10 deductible VAT):

```json
{
  "request_id": "purchase-supplier-2026-001",
  "supplier": "Supplier GmbH", "number": "2026-001",
  "invoice_date": "2026-09-01", "currency": "EUR",
  "fx_rate": "1", "fx_reference": "",
  "freight_cents": 300,
  "lines": [{
    "product_id": "PRODUCT_UUID", "quantity": 9,
    "gross_cents": 10710, "net_cents": 9000, "vat_cents": 1710,
    "deductible_vat_cents": 1710
  }]
}
```

`deductible_vat_cents=null` means unreviewed, not tax-free. Initial provisional
cost includes that VAT. Explicit zero means no deductible input VAT. A later
review uses `/revisions` with an audit reason. Foreign invoices require a
positive EUR-per-currency-unit `fx_rate` plus `fx_reference`; EUR must use 1.
Original currency values remain intact. `freight_cents` is an **additional
economic EUR cost**, not already included in product lines. Retain its source
invoice as a document. It defaults to a goods-value split; optional
`freight_allocations` supplies integer-cent shares in invoice-line order and
must sum exactly to freight. No silent equal-split fallback with zero values.

Reservation example:

```json
{
  "request_id": "reserve-package-1",
  "shipment_id": "FBA...", "marketplace_id": "A1PA6795UKMFR9",
  "package_reference": "Paket 1",
  "lines": [{"shipment_item_id": "ITEM_ID", "quantity": 7}]
}
```

An optional `receipt_id` per reservation line explicitly chooses a source
receipt instead of the FIFO suggestion. Reservations can span invoices and
consume only available own stock. Multiple reservations/packages can share an
FBA shipment; their total cannot exceed Amazon's shipment quantities.

Dispatch uses `{request_id,transfer_id,action:"dispatch",dispatched_at,
freight_cents,source_cost_id?}`. Optional `freight_allocations` uses transfer-line
**ID sort order**, returned by the pool overview; the sum must match freight.
A `source_cost_id` links existing assigned EUR Amazon transport cost, verifies
amount and shipment, and prevents reuse. Cancel uses `action:"cancel"` and
returns reserved quantities and exact cents to own stock. Dispatch cannot be
cancelled as if it never happened. Amazon receipt decreases remain visible
reconciliation exceptions, never silently create negative stock.

Revisions require `{request_id,line_id,expected_cost_cents,gross_cents,
net_cents,vat_cents,deductible_vat_cents,reason,preview}`. First use
`preview:true`. Apply the reviewed payload with `preview:false` and a new retry
ID. `expected_cost_cents` is the current line's economic product cost (excluding
freight); stale corrections fail. The original values, reason and new values
are retained in `revisions`. Quantities and existing FIFO provenance do not
change. Existing freight allocations remain fixed rather than being silently
redistributed after a price correction.

Only recorded **payments**, never FIFO allocations or invoice upload alone,
are projected as purchasing outflows to bookkeeping. The projection is
idempotent and links the shared documents. VAT-period assignment, final EÜR,
bank imports and future GmbH accounting are separate work. Missing bookkeeping
storage returns an explicit unavailable status; saved pool data can be retried
through reconciliation after the storage becomes available.

Amazon sync now updates shipment items in place and reconciles receipt deltas
and shipped sales costs after source ingestion. For FIFO timing, `CHECKED_IN`,
`RECEIVING`, and `CLOSED` are sellable transitions; `CLOSED` is not required
before a received quantity can be matched to a sale. Repeated runs consume no
extra stock. Insufficient historic costs remain `cost_issues`, rather than
invented zero-cost purchases. No purchases are synthesized from existing
Amazon stock.

Financial projections normalize `Refunded Sales` and `Refunded Expenses` with
signed tax and fee reversals. Original raw events are retained. A zero sale
balance stays zero. `costs_complete`, `margin_complete`, `sales_tax_complete`,
`fee_tax_complete` describe missing valuation/data; unknown fee tax uses the
full fee provisionally, never free fees. SKU/order/shipment contribution uses
net sales less effective fee cost and FIFO cost. Shipment attribution is
**calculated FIFO**, not proof of the physical unit Amazon shipped. The finance
endpoint also returns settlement `reconciliation` differences and an
`incomplete_event_count`. Business classification never chooses a VAT rate.

The legacy shipment-invoice endpoints remain readable/compatible during
migration. Use the pool for new purchases. Do not enter the same acquisition
through both paths. Migrated documents, old raw events and old invoice records
remain available; legacy confirmed costs are retained and flagged for tax
review instead of retroactively assuming deductible VAT.
