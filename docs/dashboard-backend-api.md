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
- `ecommerce-dashboard/app/services/order_shipping.py`
- `ecommerce-dashboard/app/services/invoices.py`
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
- Google Ads upload
- backup export download

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
- `purchase_cost_cents`: stored purchase cost in cents
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
- `purchase_currency`: string, defaults to `EUR`
- `supplier_name`: string or `null`
- `purchase_notes`: string or `null`

Example:

```json
{
  "purchase_cost_eur": 54.90,
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
- `invoice_prefix`
- `default_template`
- `footer_note`
- `payment_note`
- `eu_invoicing_enabled`

Agent rule:

- Do not partially guess missing legal or tax data. If user requests profile changes, apply only what was explicitly given.

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

Minimum review checklist:

- route path still correct
- auth requirement still correct
- query params still correct
- JSON or multipart payload still correct
- important response fields still correct
- any new validations or blocked actions documented
- any new automatable feature added to the allowed scope
