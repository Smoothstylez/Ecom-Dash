---
name: dashboard-backend-api
description: Use when operating the Ecom Dashboard via direct backend API calls instead of the visible UI, especially for orders, support tickets, filters, tracking numbers, purchase costs, PDF uploads, invoices, bookings, sync, analytics, customers, or Google Ads.
---

# Dashboard Backend API

Use this skill when the task is about running the production dashboard through
its backend API rather than clicking the UI.

Primary reference:

- `docs/dashboard-backend-api.md`

Repository helper scripts:

- `scripts/dashboard-api/order-search.sh`
- `scripts/dashboard-api/set-tracking.sh`
- `scripts/dashboard-api/upload-order-invoice.sh`
- `scripts/dashboard-api/create-sales-invoice.sh`

For any Kaufland support task, read `docs/dashboard-backend-api.md` section
`Kaufland Support Agent API` before making a call. It is the canonical contract
for ticket freshness, full history reads, local notes, attachment previews,
direct message sends, ticket opening, ticket closing, allowed attachment types,
and verification after every remote mutation.

Core rules:

- Prefer direct HTTP calls to `GET`, `PATCH`, `POST`, and upload endpoints.
- Do not use browser/UI automation when an API route already exists.
- Read before mutate.
- Verify after mutate.
- Use `DASHBOARD_BASE_URL` and `DASHBOARD_ADMIN_TOKEN` when available.
- Prefer `X-Admin-Token` for admin routes.
- Use `multipart/form-data` for uploads.
- Use `application/json` for JSON routes.
- Do not use blocked destructive endpoints documented in the reference.
- For support work: poll, list, detail, mutate, poll, then detail verification.
- Do not send or close based only on a cached list row.
- Treat a sync result with `status: partial` as incomplete, even if the HTTP response is successful.
- Do not interpret `first_response_due_at` as a confirmed Kaufland SLA.

Maintenance rule:

- If backend routes, payloads, response fields, or allowed automation scope
  change, update this skill, the agent file, `docs/dashboard-backend-api.md`,
  and any affected helper scripts in `scripts/dashboard-api/` in the same
  commit.

## USt Report Agent API

Monthly VAT report, input VAT ledger and Kaufland rate corrections. Read before
mutate; verify after mutate. `X-Admin-Token` required on every route.

- Report: `GET /api/ust-report?month=YYYY-MM`, `GET /api/ust-report/months`
- Filing: `POST /api/ust-report/{month}/refresh` | `file` | `amend`.
  `file` returns `409` while hard blockers remain (`AMAZON_UNRESOLVED`,
  `KAUFLAND_RATE_NEEDS_OVERRIDE`, `INPUT_VAT_PENDING_REVIEW`,
  `TAX_MODE_NOT_REGULAR`, `NO_VAT_START_DATE`, `UNRESOLVED_RETURN_LINK`).
  `MISSING_FEE_INVOICE` is only a warning plus `input_vat_incomplete`, unless
  `block_filing_when_input_vat_incomplete` is enabled (default off).
  Filed snapshots are immutable; corrections use `amend`, never overwrite.
- Input VAT documents: `POST /api/ust-report/documents` (multipart),
  `GET /api/ust-report/documents`, `GET .../{id}/download`,
  `PATCH .../{id}`. Input VAT is deductible in
  `max(service_month, docs_month)` where `service_month` prefers
  `service_date`/`delivery_date`, then `period_to`, `period_from`,
  `invoice_date` and `docs_month` is `received_date` or `invoice_date`.
  Never treat `invoice_date` as the service date.
- Kaufland rate fixes: `POST /api/ust-report/kaufland-overrides` and
  `/bulk`. Kaufland `vat` is a PERCENTAGE (19.0 / 0.0), never an amount.
- Settings: `POST /api/ust-report/settings` with `eu_tax_regime` one of
  `unconfirmed`, `home_rate_under_threshold`, `oss_destination`.
- Amazon: `POST /api/amazon/tax-report/request` and
  `POST /api/amazon/tax-report/{report_id}/import`. Restricted reports use an
  RDT as `x-amz-access-token` replacing the LWA token.

Helper: `scripts/dashboard-api/file-ust-report.sh`.

