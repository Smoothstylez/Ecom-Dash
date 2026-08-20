# Amazon FBA Shipment Invoices and Layout Design

## Goal

Make FBA shipment management usable for real supplier paperwork: support a
single invoice spanning multiple SKUs as well as separate invoices per SKU,
show the full gross/net/VAT trail, and remove the current modal and table
layout failures.

## Scope

### Shipment List

- Hide `CANCELLED` FBA shipments from the normal shipment list. They are not
  open cost work and should not be interpreted as missing-cost records.
- Replace the generic eleven-column `orders-table` styling with a compact,
  shipment-specific table. It shows shipment identity, status, received versus
  shipped quantity, and invoice/cost state without wasting width on separate
  SKU, transport quote, or document-count columns.
- Show a concise cost status in the shipment identity/status cell:
  `Rechnung fehlt`, `Rechnung erfasst`, or `Kosten bestaetigt`. Amazon's
  selected transport quote remains visible as secondary information only when
  Amazon actually reports one; it is never presented as product cost.

### Shared Detail Modal

- Make `detailsContent` a React portal leaf. Remove its static `-` child so
  React no longer owns content in the same node that portal users own.
- Increase the shared modal to a desktop maximum of 1280px and 92vh. The
  header remains fixed within the modal card while only detail content scrolls.
- Add an Amazon shipment detail modifier so its tables use auto layout and fit
  the modal instead of inheriting the global eleven-column order-table widths.
- Preserve the existing shared order and booking detail flows.

### Invoice Workflow

- Keep invoice files attached to exactly one inbound shipment.
- Allow any number of supplier invoices per shipment.
- Each uploaded invoice captures supplier, invoice number, date, currency,
  gross amount, net amount, VAT amount, optional note, and its document file.
- The file picker uses a stable input reference. Its value resets only after
  React has consumed the selected `FileList`, fixing the current lost-file
  race.
- After a file is chosen, its editable invoice draft appears immediately in
  the modal. A supplier is required; financial fields remain editable before
  upload.

### Invoice Lines and Cost Confirmation

- Each invoice can have zero or more SKU/FNSKU lines.
- The line editor presents all shipment SKUs and lets the operator assign each
  line to a selected invoice. Every line records received quantity, gross,
  net, and VAT; gross must equal net plus VAT.
- A combined invoice can contain a line for every shipment SKU. Separate
  supplier invoices can each contain one SKU line. A shipment SKU/FNSKU may
  appear on exactly one invoice line, avoiding ambiguous or partial cost
  allocation.
- Confirmation is permitted only for received shipments (`RECEIVING` or
  `CLOSED`), requires at least one invoice, requires every received SKU to be
  fully allocated, and requires every invoice's line gross/net/VAT totals to
  match that invoice header.
- Confirmation creates FIFO lots from the combined net cost per SKU. VAT is
  stored and displayed but is not included in product COGS.
- Existing confirmed allocations and lots remain immutable. The UI shows their
  final values instead of editable controls.

## Data Model and API Changes

- Extend `amazon_inbound_invoice_lines` with `gross_cents`. Existing rows use
  `gross_cents = net_cents + vat_cents` during schema migration.
- The invoice detail response returns all invoice headers including net, VAT,
  gross, document path, and their own lines grouped by `invoice_id`.
- The invoice-line request accepts `gross_cents`, `net_cents`, and `vat_cents`.
- Add a replace/upsert invoice-line endpoint keyed by invoice plus SKU/FNSKU;
  it rejects a gross amount that does not equal net plus VAT.
- `confirm_inbound_product_costs()` aggregates invoice lines across all
  invoices in the shipment. It validates totals before writing allocations or
  FIFO lots in one transaction.

## Explicit Non-Goals

- Do not create a manual own-shipping/transport-cost feature. The relevant
  own-shipping shipment was cancelled.
- Do not infer product purchase cost from an Amazon transport quote.
- Do not modify already confirmed FIFO lots or allocations.

## Error Handling

- A file upload with missing supplier, invalid amount relationship, empty file,
  or unsupported shipment is rejected with an actionable response.
- A draft that has not uploaded remains in the UI and preserves entered values
  after a validation error.
- Confirmation errors identify the exact SKU, invoice, or total that is
  incomplete or mismatched.
- Missing Amazon transport data renders as no transport quote, not as a cost
  error.

## Verification

- Backend tests cover schema migration, gross/net/VAT validation, a combined
  two-SKU invoice, two single-SKU invoices, incomplete SKU allocation,
  invoice-total mismatch, and immutable confirmed costs.
- Frontend tests/typecheck cover invoice draft selection and invoice-specific
  SKU-line submissions.
- Production build refreshes `ecommerce-dashboard/frontend_dist`.
- Playwright verifies: cancelled shipment hidden; compact shipment table;
  invoice file draft appears after choosing a file; gross/net/VAT fields are
  visible; the larger modal holds the shipment tables without horizontal
  overflow at desktop width.
