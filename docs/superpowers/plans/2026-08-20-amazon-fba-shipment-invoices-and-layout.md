# Amazon FBA Shipment Invoices and Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make FBA shipments support combined or per-SKU supplier invoices with gross/net/VAT validation, while fixing the broken invoice picker and compacting the shipment list and detail modal.

**Architecture:** SQLite keeps one invoice header per uploaded file and one unique SKU/FNSKU line per invoice. Confirmation validates invoice-header totals and all shipment SKU coverage across every invoice, then creates existing FIFO lots from aggregated net SKU cost. The React shipment modal owns draft files through refs, selects an invoice per line, and uses dedicated layout classes rather than the global order-table dimensions.

**Tech Stack:** Python 3, FastAPI, SQLite, React, TypeScript, Vite, pytest, Playwright.

## Global Constraints

- Do not create a manual own-shipping transport-cost feature; the relevant shipment is cancelled.
- Hide `CANCELLED` shipments from the default FBA shipment list.
- Supplier invoice net value is product COGS; VAT remains visible and excluded from FIFO COGS.
- One shipment SKU/FNSKU maps to exactly one invoice line across the shipment; a combined invoice can contain many SKU lines and separate invoices can contain one each.
- Do not mutate confirmed allocations or FIFO lots.
- Keep the user-requested changes local; do not push or bump the add-on version.

---

### Task 1: Extend the Invoice-Line Schema and Service Validation

**Files:**
- Modify: `ecommerce-dashboard/app/services/importers/amazon_sp_api.py:651-664,786-875`
- Modify: `ecommerce-dashboard/app/services/amazon_fba.py:690-736`
- Modify: `ecommerce-dashboard/tests/test_amazon_fba.py:262-339`

**Interfaces:**
- Consumes: `init_amazon_fba_db()` and its existing `PRAGMA table_info` migration pattern.
- Produces: `add_inbound_invoice_line(..., gross_cents: int, net_cents: int, vat_cents: int) -> dict[str, Any]`.
- Produces: `amazon_inbound_invoice_lines.gross_cents INTEGER NOT NULL DEFAULT 0`, populated as `net_cents + vat_cents` for existing rows.

- [ ] **Step 1: Write the failing migration and amount-validation tests**

```python
def test_invoice_line_migration_backfills_gross_from_net_and_vat(monkeypatch, tmp_path) -> None:
    import sqlite3
    import app.services.importers.amazon_sp_api as importer

    db_path = tmp_path / "amazon.sqlite3"
    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", db_path)
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "CREATE TABLE amazon_inbound_invoice_lines "
            "(id TEXT PRIMARY KEY, invoice_id TEXT, seller_sku TEXT, fnsku TEXT, "
            "asin TEXT, title TEXT, quantity INTEGER, net_cents INTEGER, vat_cents INTEGER, raw_json TEXT)"
        )
        connection.execute(
            "INSERT INTO amazon_inbound_invoice_lines VALUES "
            "('LINE-1', 'INV-1', 'SKU-1', 'FNSKU-1', '', '', 1, 1000, 190, '{}')"
        )
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        row = connection.execute(
            "SELECT gross_cents FROM amazon_inbound_invoice_lines WHERE id = 'LINE-1'"
        ).fetchone()
    assert row["gross_cents"] == 1190


def test_invoice_line_rejects_gross_not_equal_to_net_plus_vat(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()

    with pytest.raises(ValueError, match="gross_cents must equal net_cents plus vat_cents"):
        amazon_fba.add_inbound_invoice_line(
            invoice_id="INV-1", seller_sku="SKU-1", fnsku="FNSKU-1", asin="",
            title="Product", quantity=1, gross_cents=1200, net_cents=1000, vat_cents=190,
        )
```

- [ ] **Step 2: Run the focused tests to verify they fail**

Run: `cd ecommerce-dashboard && python3 -m pytest -q tests/test_amazon_fba.py -k 'invoice_line_migration or invoice_line_rejects_gross'`

Expected: FAIL because `gross_cents` does not exist and `add_inbound_invoice_line` does not accept it.

- [ ] **Step 3: Add the schema migration and exact amount validation**

```python
# In init_amazon_fba_db(), after the existing migration checks.
invoice_line_columns = {
    str(row[1]) for row in connection.execute(
        "PRAGMA table_info(amazon_inbound_invoice_lines)"
    ).fetchall()
}
if "gross_cents" not in invoice_line_columns:
    connection.execute(
        "ALTER TABLE amazon_inbound_invoice_lines "
        "ADD COLUMN gross_cents INTEGER NOT NULL DEFAULT 0"
    )
    connection.execute(
        "UPDATE amazon_inbound_invoice_lines "
        "SET gross_cents = net_cents + vat_cents"
    )


def add_inbound_invoice_line(..., gross_cents: int, net_cents: int, vat_cents: int) -> dict[str, Any]:
    if gross_cents != net_cents + vat_cents:
        raise ValueError("gross_cents must equal net_cents plus vat_cents")
```

Include `gross_cents` in the insert statement, upsert update set, and returned line records. Update existing test calls to pass their existing `net_cents + vat_cents` amount.

- [ ] **Step 4: Run the focused tests to verify they pass**

Run: `cd ecommerce-dashboard && python3 -m pytest -q tests/test_amazon_fba.py -k 'invoice_line_migration or invoice_line_rejects_gross or invoice_lines_allocate_exact_single_sku_cost'`

Expected: PASS.

- [ ] **Step 5: Commit the schema and validation change**

```bash
git add ecommerce-dashboard/app/services/importers/amazon_sp_api.py \
  ecommerce-dashboard/app/services/amazon_fba.py \
  ecommerce-dashboard/tests/test_amazon_fba.py
git commit -m "feat: validate gross net VAT on FBA invoice lines"
```

### Task 2: Confirm Combined and Per-SKU Invoice Costs Across Multiple Headers

**Files:**
- Modify: `ecommerce-dashboard/app/services/amazon_fba.py:749-853`
- Modify: `ecommerce-dashboard/tests/test_amazon_fba.py:262-339`

**Interfaces:**
- Consumes: invoice lines with `gross_cents`, `net_cents`, `vat_cents` from Task 1.
- Produces: `confirm_inbound_product_costs(shipment_id: str) -> dict[str, Any]` that validates all shipment invoices rather than requiring exactly one.
- Produces: one allocation and one FIFO lot per shipment SKU/FNSKU with the selected line's net cost.

- [ ] **Step 1: Write failing tests for the supported invoice shapes and rejection paths**

```python
def _fba_services(monkeypatch, tmp_path):
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    return amazon_fba, importer


def _closed_two_sku_shipment(importer, shipment_id: str) -> None:
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": shipment_id, "ShipmentStatus": "CLOSED"},
            items=[
                {"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 1, "QuantityReceived": 1},
                {"SellerSKU": "SKU-2", "FulfillmentNetworkSKU": "FNSKU-2", "QuantityShipped": 1, "QuantityReceived": 1},
            ],
        )


def _invoice(amazon_fba, shipment_id: str, number: str, gross: int, net: int, vat: int):
    return amazon_fba.add_inbound_invoice(
        shipment_id=shipment_id, supplier_name="Supplier", invoice_number=number,
        invoice_date="2026-08-20", currency="EUR", gross_cents=gross,
        net_cents=net, vat_cents=vat, document_path=f"{number}.pdf",
    )


def _line(amazon_fba, invoice_id: str, sku: str, fnsku: str, gross: int, net: int, vat: int) -> None:
    amazon_fba.add_inbound_invoice_line(
        invoice_id=invoice_id, seller_sku=sku, fnsku=fnsku, asin="", title=sku,
        quantity=1, gross_cents=gross, net_cents=net, vat_cents=vat,
    )


def test_confirm_inbound_product_costs_accepts_one_combined_invoice(monkeypatch, tmp_path) -> None:
    amazon_fba, importer = _fba_services(monkeypatch, tmp_path)
    _closed_two_sku_shipment(importer, "FBA-COMBINED")
    invoice = _invoice(amazon_fba, "FBA-COMBINED", "INV-C", gross=3570, net=3000, vat=570)
    _line(amazon_fba, invoice["id"], "SKU-1", "FNSKU-1", gross=1190, net=1000, vat=190)
    _line(amazon_fba, invoice["id"], "SKU-2", "FNSKU-2", gross=2380, net=2000, vat=380)

    result = amazon_fba.confirm_inbound_product_costs("FBA-COMBINED")

    assert {lot["seller_sku"]: lot["unit_cost_cents"] for lot in result["lots"]} == {"SKU-1": 1000, "SKU-2": 2000}


def test_confirm_inbound_product_costs_accepts_one_invoice_per_sku(monkeypatch, tmp_path) -> None:
    amazon_fba, importer = _fba_services(monkeypatch, tmp_path)
    _closed_two_sku_shipment(importer, "FBA-SEPARATE")
    invoice_one = _invoice(amazon_fba, "FBA-SEPARATE", "INV-1", gross=1190, net=1000, vat=190)
    invoice_two = _invoice(amazon_fba, "FBA-SEPARATE", "INV-2", gross=2380, net=2000, vat=380)
    _line(amazon_fba, invoice_one["id"], "SKU-1", "FNSKU-1", gross=1190, net=1000, vat=190)
    _line(amazon_fba, invoice_two["id"], "SKU-2", "FNSKU-2", gross=2380, net=2000, vat=380)

    result = amazon_fba.confirm_inbound_product_costs("FBA-SEPARATE")

    assert len(result["lots"]) == 2


def test_confirm_inbound_product_costs_rejects_invoice_header_line_total_mismatch(monkeypatch, tmp_path) -> None:
    amazon_fba, importer = _fba_services(monkeypatch, tmp_path)
    _closed_two_sku_shipment(importer, "FBA-MISMATCH")
    invoice = _invoice(amazon_fba, "FBA-MISMATCH", "INV-M", gross=1190, net=1000, vat=190)
    _line(amazon_fba, invoice["id"], "SKU-1", "FNSKU-1", gross=1189, net=1000, vat=189)

    with pytest.raises(ValueError, match="invoice line gross total must match invoice gross total"):
        amazon_fba.confirm_inbound_product_costs("FBA-MISMATCH")
```

Keep helpers inside `tests/test_amazon_fba.py` near the existing invoice tests, with explicit construction of the closed shipment and invoice inputs.

- [ ] **Step 2: Run the focused tests to verify they fail**

Run: `cd ecommerce-dashboard && python3 -m pytest -q tests/test_amazon_fba.py -k 'combined_invoice or invoice_per_sku or header_line_total_mismatch'`

Expected: FAIL because confirmation rejects multiple invoices and only checks one invoice net total.

- [ ] **Step 3: Replace one-invoice confirmation with whole-shipment validation**

```python
lines = connection.execute(
    """
    SELECT l.*, i.shipment_id, i.currency, i.gross_cents AS invoice_gross_cents,
           i.net_cents AS invoice_net_cents, i.vat_cents AS invoice_vat_cents
    FROM amazon_inbound_invoice_lines l
    JOIN amazon_inbound_invoices i ON i.id = l.invoice_id
    WHERE i.shipment_id = ?
    ORDER BY l.invoice_id, l.seller_sku, l.fnsku
    """,
    (shipment_id,),
).fetchall()

lines_by_invoice: dict[str, list[sqlite3.Row]] = {}
for line in lines:
    lines_by_invoice.setdefault(str(line["invoice_id"]), []).append(line)
for invoice in invoices:
    invoice_lines = lines_by_invoice.get(str(invoice["id"]), [])
    if sum(int(line["gross_cents"]) for line in invoice_lines) != int(invoice["gross_cents"]):
        raise ValueError("invoice line gross total must match invoice gross total")
    if sum(int(line["net_cents"]) for line in invoice_lines) != int(invoice["net_cents"]):
        raise ValueError("invoice line net total must match invoice net total")
    if sum(int(line["vat_cents"]) for line in invoice_lines) != int(invoice["vat_cents"]):
        raise ValueError("invoice line VAT total must match invoice VAT total")
```

Build `lines_by_sku` over all invoice lines. Require exactly the shipment item key set, reject any key with more than one line, and require received quantity equality. Use each line's invoice currency and net amount when writing allocations/lots. Retain the existing early return when lots already exist.

```python
lines_by_sku: dict[tuple[str, str], list[sqlite3.Row]] = {}
for line in lines:
    key = (str(line["seller_sku"] or ""), str(line["fnsku"] or ""))
    lines_by_sku.setdefault(key, []).append(line)
if set(lines_by_sku) != set(items_by_key):
    raise ValueError("invoice lines must cover every shipment SKU")
if any(len(sku_lines) != 1 for sku_lines in lines_by_sku.values()):
    raise ValueError("each shipment SKU requires exactly one invoice line")
```

- [ ] **Step 4: Run all inbound-cost confirmation tests**

Run: `cd ecommerce-dashboard && python3 -m pytest -q tests/test_amazon_fba.py -k 'invoice or inbound_product_costs or cost_confirmation'`

Expected: PASS, including existing FIFO and immutability behavior.

- [ ] **Step 5: Commit the multi-invoice confirmation change**

```bash
git add ecommerce-dashboard/app/services/amazon_fba.py ecommerce-dashboard/tests/test_amazon_fba.py
```

### Task 3: Expose the Complete Invoice API and Compact Shipment State

**Files:**
- Modify: `ecommerce-dashboard/app/routers/amazon.py:128-136,331-337`
- Modify: `ecommerce-dashboard/app/services/amazon_fba.py:492-525,528-583`
- Modify: `ecommerce-dashboard/tests/test_amazon_fba.py`
- Modify: `docs/dashboard-backend-api.md:935-955`

**Interfaces:**
- Consumes: whole-shipment invoice validation from Task 2.
- Produces: `InboundInvoiceLineRequest(gross_cents, net_cents, vat_cents)`.
- Produces: list shipment records with `cost_status: "missing" | "entered" | "confirmed"` and no `CANCELLED` rows by default.
- Produces: shipment detail invoice lines with `gross_cents` and invoice headers with all gross/net/VAT fields.

- [ ] **Step 1: Write failing API/service tests**

```python
def test_list_inbound_shipments_hides_cancelled_and_projects_cost_status(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-CANCELLED", "ShipmentStatus": "CANCELLED"},
            items=[],
        )
        for shipment_id in ("FBA-MISSING", "FBA-ENTERED"):
            importer._upsert_inbound_shipment(
                connection,
                shipment={"ShipmentId": shipment_id, "ShipmentStatus": "CLOSED"},
                items=[{"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 1, "QuantityReceived": 1}],
            )
    amazon_fba.add_inbound_invoice(
        shipment_id="FBA-ENTERED", supplier_name="Supplier", invoice_number="INV-E",
        invoice_date="2026-08-20", currency="EUR", gross_cents=1190,
        net_cents=1000, vat_cents=190, document_path="entered.pdf",
    )

    shipments = amazon_fba.list_inbound_shipments()

    assert {shipment["shipment_id"] for shipment in shipments} == {"FBA-MISSING", "FBA-ENTERED"}
    assert {shipment["shipment_id"]: shipment["cost_status"] for shipment in shipments} == {
        "FBA-MISSING": "missing", "FBA-ENTERED": "entered",
    }


def test_invoice_line_endpoint_requires_gross_net_vat_consistency(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer
    from fastapi.testclient import TestClient
    from app.main import app

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-API", "ShipmentStatus": "CLOSED"},
            items=[{"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 1, "QuantityReceived": 1}],
        )
    invoice = amazon_fba.add_inbound_invoice(
        shipment_id="FBA-API", supplier_name="Supplier", invoice_number="INV-API",
        invoice_date="2026-08-20", currency="EUR", gross_cents=950,
        net_cents=800, vat_cents=150, document_path="api.pdf",
    )
    client = TestClient(app)

    response = client.post(
        f"/api/amazon/inbound/invoices/{invoice['id']}/lines",
        json={"seller_sku": "SKU-1", "fnsku": "FNSKU-1", "quantity": 1,
              "gross_cents": 1000, "net_cents": 900, "vat_cents": 50},
    )

    assert response.status_code == 400
    assert "gross_cents" in response.json()["detail"]
```

- [ ] **Step 2: Run the focused tests to verify they fail**

Run: `cd ecommerce-dashboard && python3 -m pytest -q tests/test_amazon_fba.py -k 'hides_cancelled or cost_status or requires_gross_net_vat'`

Expected: FAIL because cancelled shipments remain visible, no cost status exists, and the request model omits `gross_cents`.

- [ ] **Step 3: Implement API models and shipment projections**

```python
class InboundInvoiceLineRequest(BaseModel):
    seller_sku: str = ""
    fnsku: str = ""
    asin: str = ""
    title: str = ""
    quantity: int = Field(ge=1)
    gross_cents: int = Field(ge=0)
    net_cents: int = Field(ge=0)
    vat_cents: int = Field(ge=0)
```

Use `WHERE s.status <> 'CANCELLED'` when no explicit status filter is requested; retain `WHERE s.status = ?` for explicit status requests. In the list projection, calculate `cost_status` from invoice count and confirmed allocation count:

```python
if item["allocation_count"]:
    item["cost_status"] = "confirmed"
elif item["invoice_count"]:
    item["cost_status"] = "entered"
else:
    item["cost_status"] = "missing"
```

Pass `gross_cents` to `add_inbound_invoice_line`. Document the new fields and the cancelled-row default in `docs/dashboard-backend-api.md`.

- [ ] **Step 4: Run API/service tests and the full backend suite**

Run: `cd ecommerce-dashboard && python3 -m pytest -q tests/test_amazon_fba.py && python3 -m pytest -q`

Expected: PASS.

- [ ] **Step 5: Commit the API and shipment-list change**

```bash
git add ecommerce-dashboard/app/routers/amazon.py \
  ecommerce-dashboard/app/services/amazon_fba.py \
  ecommerce-dashboard/tests/test_amazon_fba.py \
  docs/dashboard-backend-api.md
git commit -m "feat: show FBA invoice cost state and hide cancelled shipments"
```

### Task 4: Repair the Shared Modal Container and Add Dedicated Amazon Layouts

**Files:**
- Modify: `frontend/src/app/dashboard-shared-modals.tsx:302-318`
- Modify: `ecommerce-dashboard/app/static/css/main.css:3418-3458,3848-3907`
- Modify: `frontend/src/features/amazon/amazon-page.tsx:432-449,492-570`

**Interfaces:**
- Consumes: the shared modal DOM IDs `detailsModal`, `detailsTitle`, `detailsContent`.
- Produces: a portal-safe empty `#detailsContent` and CSS classes `amazon-shipment-modal`, `amazon-shipment-table`, `amazon-shipment-detail-table`.
- Produces: a shipment list that presents `cost_status` and uses compact, purpose-specific columns.

- [ ] **Step 1: Add an implementation-facing UI test assertion or Playwright check**

Add a Playwright test or extend the existing Amazon FBA e2e flow so it asserts the FBA shipment list contains no `Storniert` row by default and the opened detail modal's bounding box is at least 1100px wide at a 1440px desktop viewport.

```ts
await expect(page.getByText("Storniert", { exact: true })).toHaveCount(0);
await page.locator(".amazon-shipment-table tbody tr").first().click();
await expect(page.locator("#detailsModal .modal-card")).toHaveJSProperty("clientWidth", expect.any(Number));
```

Use `page.locator("#detailsModal .modal-card").evaluate((node) => node.clientWidth)` for the explicit `>= 1100` assertion.

- [ ] **Step 2: Run the e2e check to verify it fails**

Run: `cd frontend && npm run test:e2e -- --grep "Amazon FBA shipment"`

Expected: FAIL because the current generic modal is 1020px wide and cancelled shipments are visible.

- [ ] **Step 3: Make the shared portal container a leaf and add layout classes**

```tsx
<div id="detailsContent" className="detail-content" />
```

```css
.modal-card {
  width: min(1280px, 100%);
  max-height: 92vh;
  overflow: hidden;
  grid-template-rows: auto minmax(0, 1fr);
}

.detail-content {
  min-height: 0;
  overflow: auto;
}

.amazon-shipment-table {
  min-width: 680px;
  table-layout: fixed;
}

.amazon-shipment-table thead th:nth-child(1) { width: auto; }
.amazon-shipment-table thead th:nth-child(2) { width: 130px; }
.amazon-shipment-table thead th:nth-child(3) { width: 130px; }
.amazon-shipment-table thead th:nth-child(4) { width: 150px; }
```

Render only `Shipment`, `Status`, `Menge`, and `Kosten` in the list. Put destination and the selected Amazon transport quote below the shipment name as short secondary text. Use `Rechnung fehlt`, `Rechnung erfasst`, and `Kosten bestaetigt` as the cost labels. Use `detail-table-wrap` or the new `amazon-shipment-detail-table` for detail tables; do not use `orders-table` in the shipment modal.

- [ ] **Step 4: Run typecheck, production build, and e2e check**

Run: `cd frontend && npm run typecheck && npm run build && npm run test:e2e -- --grep "Amazon FBA shipment"`

Expected: PASS. The build script refreshes `ecommerce-dashboard/frontend_dist`.

- [ ] **Step 5: Commit the shared modal and table layout changes**

```bash
git add frontend/src/app/dashboard-shared-modals.tsx \
  frontend/src/features/amazon/amazon-page.tsx \
  ecommerce-dashboard/app/static/css/main.css \
  ecommerce-dashboard/frontend_dist \
  frontend/e2e
git commit -m "fix: enlarge FBA shipment modal and compact shipment tables"
```

### Task 5: Build the Multi-Invoice Shipment Editor

**Files:**
- Modify: `frontend/src/features/amazon/amazon-page.tsx:67-95,152-155,236-334,502-569`
- Modify: `frontend/src/features/amazon/use-amazon-detail-modal.ts:12-35` only if a shipment-specific class-name registration is required by Task 4's CSS implementation.
- Modify: `frontend/src/test/` or `frontend/e2e/` with the existing frontend test convention.

**Interfaces:**
- Consumes: `InboundShipmentDetail.invoices[]` and `invoice_lines[]` including gross/net/VAT values from Task 3.
- Consumes: `POST /api/amazon/inbound/shipments/{shipment_id}/invoices` and `POST /api/amazon/inbound/invoices/{invoice_id}/lines`.
- Produces: a stable file-selection flow, per-invoice drafts, and one invoice selector plus gross/net/VAT fields for every unconfirmed shipment SKU.

- [ ] **Step 1: Write a failing UI test for file selection and per-invoice line data**

```tsx
it("shows an invoice draft after choosing a file and submits gross/net/VAT per selected invoice", async () => {
  render(<AmazonPage />);
  await userEvent.click(await screen.findByText("FBA-TEST-1"));
  await userEvent.upload(screen.getByLabelText("Dateien waehlen"), new File(["invoice"], "supplier.pdf", { type: "application/pdf" }));

  expect(await screen.findByText("supplier.pdf")).toBeVisible();
  expect(screen.getByLabelText("Brutto supplier.pdf")).toBeVisible();
  expect(screen.getByLabelText("Netto supplier.pdf")).toBeVisible();
  expect(screen.getByLabelText("USt supplier.pdf")).toBeVisible();
});
```

If the project has no React component-test runner, implement this exact behavior in Playwright instead: upload a file, assert the filename and three fields appear, fill them, and assert the invoice upload request includes all three cent values.

- [ ] **Step 2: Run the UI test to verify it fails**

Run the project’s existing frontend test command for the chosen test location. If only Playwright exists: `cd frontend && npm run test:e2e -- --grep "FBA invoice drafts"`.

Expected: FAIL because the current `event.target.value = ""` reset discards the selected file before the draft is rendered, and SKU rows only expose net input.

- [ ] **Step 3: Implement stable invoice drafts and invoice-specific SKU lines**

```tsx
const invoiceFileInputRef = useRef<HTMLInputElement>(null);

function addInvoiceFiles(files: FileList | null) {
  const selectedFiles = files ? Array.from(files) : [];
  if (!selectedFiles.length) return;
  setInvoiceDrafts((current) => {
    const next = { ...current };
    for (const file of selectedFiles) {
      const key = draftKey(file);
      next[key] ??= { file, supplier: "", invoiceNumber: "", gross: "", net: "", vat: "", status: "idle", error: "" };
    }
    return next;
  });
  if (invoiceFileInputRef.current) invoiceFileInputRef.current.value = "";
}
```

Use a real `button` labelled `Dateien waehlen` that calls `invoiceFileInputRef.current?.click()`, with the file input hidden but separately rendered. Give each draft fields `Lieferant <filename>`, `Rechnungsnummer <filename>`, `Brutto <filename>`, `Netto <filename>`, `USt <filename>` and validate `gross === net + vat` before upload.

Replace `invoiceLineNet` with a line draft keyed by SKU/FNSKU containing `invoiceId`, `gross`, `net`, and `vat`. The invoice selector lists only headers already uploaded for the shipment. Submit all three values with the selected invoice ID. Before confirmation, show saved amount values in editable fields and replace them through the existing invoice/SKU upsert endpoint; keep the chosen invoice fixed once its line is saved so a SKU cannot be accidentally represented on two invoices. After confirmation, show the final allocation values without editable controls.

- [ ] **Step 4: Run the frontend verification cycle**

Run: `cd frontend && npm run typecheck && npm run build && npm run test:e2e -- --grep "FBA invoice"`

Expected: PASS. Verify the build copies the new bundle and runtime CSS to `ecommerce-dashboard/frontend_dist`.

- [ ] **Step 5: Commit the invoice editor**

```bash
git add frontend/src/features/amazon/amazon-page.tsx \
  frontend/src/features/amazon/use-amazon-detail-modal.ts \
  frontend/src/test frontend/e2e ecommerce-dashboard/frontend_dist
```

### Task 6: Verify Documentation, Build Outputs, and Live UI

**Files:**
- Modify: `docs/dashboard-backend-api.md`
- Modify: `ecommerce-dashboard/frontend_dist/`

**Interfaces:**
- Consumes: final backend endpoints and final frontend build.
- Produces: accurate API documentation and deployment-ready static assets.

- [ ] **Step 1: Update API documentation with exact invoice contracts**

Document that invoice headers receive gross/net/VAT, lines receive gross/net/VAT and must sum exactly to their selected invoice, one line covers one shipment SKU/FNSKU, and confirmation accepts either one combined invoice or multiple SKU-specific invoices.

- [ ] **Step 2: Run all automated verification**

Run: `cd ecommerce-dashboard && python3 -m pytest -q`

Run: `cd frontend && npm run typecheck && npm run build`

Expected: all tests pass and build exits zero.

- [ ] **Step 3: Run Playwright live verification against the local API**

At desktop width 1440px:

```ts
await page.goto("http://100.106.27.65:5174/amazon");
await expect(page.getByText("Storniert", { exact: true })).toHaveCount(0);
await page.locator(".amazon-shipment-table tbody tr").first().click();
await page.getByRole("button", { name: "Dateien waehlen" }).click();
await page.setInputFiles("input[type=file]", "test-invoice.pdf");
await expect(page.getByText("test-invoice.pdf")).toBeVisible();
await expect(page.getByLabelText("Brutto test-invoice.pdf")).toBeVisible();
await expect(page.getByLabelText("Netto test-invoice.pdf")).toBeVisible();
await expect(page.getByLabelText("USt test-invoice.pdf")).toBeVisible();
```

Verify the modal is at least 1100px wide and each detail table fits without a horizontal scrollbar at this desktop viewport.

- [ ] **Step 4: Inspect staged changes and commit documentation/build output**

```bash
git status --porcelain
git diff --check
git add docs/dashboard-backend-api.md ecommerce-dashboard/frontend_dist
git commit -m "docs: document FBA multi-invoice workflow"
```

Do not stage runtime databases, `.playwright-mcp/`, or unrelated files. Do not push or version-bump.
