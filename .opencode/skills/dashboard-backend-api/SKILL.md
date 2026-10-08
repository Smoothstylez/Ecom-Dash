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

Finance checks are independent: `sections.finance_reconciliation` shows
`matched`/`explained`/`differences`/`incomplete`, native-currency category sums,
order-level differences, lifecycle month shifts and unknown/missing evidence.
Finance-only gaps/mismatches/source failures are warnings, never an extra
manual approval or filing blocker. Original reports/confirmed invoices retain
their tax totals. Do not change tax values to force agreement with payout dates.
`AMAZON_TAX_DATA_INCOMPLETE`/`AMAZON_SOURCE_UNAVAILABLE` now label control
warnings; actual unresolved tax-source records remain blockers. Strict input-VAT
mode does not promote Finance-only warnings into blockers.

- Report: `GET /api/ust-report?month=YYYY-MM`, `GET /api/ust-report/months`
- Normal upload: `POST /api/ust-report/import` with multipart `files` (PDF/CSV,
  max 25). Recognizes content, pairs fee CSVs with PDFs by invoice number,
  persists originals, assigns periods and automatically approves only fully
  unambiguous supported cases. Inspect every returned item's status/reasons;
  HTTP 200 alone is not full success. `GET /api/ust-report/imports` shows history.
  Repeated uploads are idempotent; conflicting approved records are preserved.
  `POST /api/ust-report/parse-upload` previews binary PDF without booking.
  PDF extraction requires `pdftotext`; the add-on installs it through
  `poppler-utils` and verifies the executable during image build. `503` naming
  missing `pdftotext` means update/rebuild the add-on image, not install a tool
  on the HA OS host. Unreadable uploaded PDFs remain `400` client errors.
  Use `scripts/dashboard-api/import-ust-files.sh` with explicit target base URL.
- Filing: `POST /api/ust-report/{month}/refresh` | `file` | `amend`.
  `file` returns `409` while hard blockers remain (`AMAZON_UNRESOLVED`,
  `KAUFLAND_RATE_NEEDS_OVERRIDE`, `INPUT_VAT_PENDING_REVIEW`,
  `TAX_MODE_NOT_REGULAR`, `NO_VAT_START_DATE`, `UNRESOLVED_RETURN_LINK`,
  `AMAZON_VAT_START_AMBIGUOUS`, `KAUFLAND_REFUND_DATE_MISSING`,
  `INPUT_VAT_SOURCE_UNAVAILABLE`, `INPUT_VAT_INVOICE_CONFLICT`,
  `AMAZON_TRANSACTION_DATE_MISSING`). Inspect `sections.amazon_reconciliation`
  for missing order IDs and shipment/refund amount gaps. Never replace missing
  tax rows with financial-event amounts or silently assign undated refunds.
  `INPUT_VAT_ELIGIBILITY_UNRESOLVED` also blocks filing when a fee/credit's
  original eligibility across the cutoff is unproven. Amazon Finance coverage
  warnings `AMAZON_TAX_DATA_INCOMPLETE` and `AMAZON_SOURCE_UNAVAILABLE` do not
  block a valid original tax report and do not change its tax totals; unresolved
  tax classifications and cutoff/date errors remain blockers.
  `MISSING_FEE_INVOICE` is only a warning plus `input_vat_incomplete`, unless
  `block_filing_when_input_vat_incomplete` is enabled (default off).
  `FEE_RECONCILIATION_DIFFERENCE` and `FINANCE_RECONCILIATION_INCOMPLETE` are
  independent Finance warnings; they do not set `input_vat_incomplete` or become
  blockers in strict input-VAT mode. The `sections.finance_reconciliation`
  result reports native-currency fee categories, order differences, timing,
  excluded advertising and missing/unknown evidence. Never alter confirmed
  invoice tax or report amounts merely to force Finance totals to match.
  Filed snapshots are immutable; corrections use `amend`, never overwrite.
- Input VAT documents: `POST /api/ust-report/documents` (multipart),
  `GET /api/ust-report/documents`, `GET .../{id}/download`,
  `PATCH .../{id}`. Input VAT is deductible in
  `max(service_month, docs_month)` where `service_month` prefers
  `service_date`/`delivery_date`, then `period_to`, `period_from`,
  `invoice_date` and `docs_month` is `received_date` or `invoice_date`.
  Never treat `invoice_date` as the service date.
  The shared document list also includes bookkeeping invoices (`book:` IDs);
  originals and explicit confirmation use the same document download/PATCH
  routes. Do not assume all returned invoices live in the legacy USt ledger.
  Booked input VAT is already included in the four detail buckets;
  `booked_fee_vat_cents` is informational, never add it a second time.
  Document responses include `effective_deductible_vat_cents` plus eligibility
  adjustment/review counts. Raw invoice VAT may differ from effective VAT
  where pre-cutoff fees/credits are excluded; do not overwrite the original.
- Cutoff: `PUT /api/invoices/profile` (read-modify-write the full profile)
  preserves the full `vat_effective_from` UTC timestamp. The threshold order
  is included; pre-cutoff sales and their later corrections carry no output
  VAT. Do not shorten an order-based cutoff to midnight.
- Kaufland rate fixes: `POST /api/ust-report/kaufland-overrides` and
  `/bulk`. Kaufland `vat` is a PERCENTAGE (19.0 / 0.0), never an amount.
- Settings: `POST /api/ust-report/settings` with `eu_tax_regime` one of
  `unconfirmed`, `home_rate_under_threshold`, `oss_destination`.
- Amazon: `POST /api/amazon/tax-report/request` and
  `POST /api/amazon/tax-report/{report_id}/import`. Restricted reports use an
  RDT as `x-amz-access-token` replacing the LWA token.
  Upstream permission errors return `502` with the SP-API error in `detail`;
  a successful sync does not imply permission to download VAT reports. If a
  Seller Central CSV is available, use `/api/amazon/pool/tax-report` to import
  either VCS or `VAT_TRANSACTION` (AVTR). AVTR supplies pre-VCS shipment/gross
  evidence without duplicating VCS rows. Standard domestic DE/EUR sales without
  Amazon calculation get a disclosed computed 19% rate; unsupported special
  product codes and foreign exemptions remain unresolved. This import does
  not approve invoices or file reports.

Helper: `scripts/dashboard-api/file-ust-report.sh`.

### Automatic original-file import

- `POST /api/ust-report/import`: multipart `files`, at most 25, accepts original
  PDFs and tax/fee CSVs. Helper: `scripts/dashboard-api/import-ust-files.sh`.
- `GET /api/ust-report/imports`: latest 30 persisted results. Always inspect
  per-file status/reasons; HTTP 200 may include review/conflict/error cases.
- Fee CSV `pairings` preserve invoice number, found PDF filename, document ID,
  month and outcome for each invoice. `paired_review` means PDF found but review
  outstanding; `waiting_pdf` names genuinely missing invoice numbers.
- Pairing includes existing bookkeeping originals. Repeated PDF/fee CSV uploads
  re-evaluate safely without duplicate bookings; approved totals are not changed.
- Credit `original_invoice_numbers` includes all original references. Inheritance
  without order allocation requires confirmed positive fee originals with the
  same fully eligible/ineligible treatment. Mixed/partial/missing/credit-chain
  references remain review cases. Never force-confirm these to clear warnings.
- Shipping Chargeback is customer-order shipping, Inventory Removals is removal
  of FBA stock; use order eligibility for the former, service date for the latter.

For Amazon fee invoices, pair the original PDF with its invoice-specific CSV
in `POST /api/bookings/monthly-invoices/parse`. Replaced PDF-line warnings are
recomputed from the CSV; real checksum/header/currency issues still require
review. Parsing alone never approves or books the invoice.
Platform invoice approval reconciles only its own linked transactions. Signed
supplier credit invoices are supported; ledger credits use IN/positive
amounts, and late reconciliation differences are appended idempotently.
`FEE_RECONCILIATION_DIFFERENCE` is an open finance-vs-invoice reconciliation,
not evidence that a specific invoice is missing.
