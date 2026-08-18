# Amazon Order Detail and Cost Chain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Amazon order details complete and connect explicitly confirmed FBA shipment product costs to SKU lots and FIFO COGS.

**Architecture:** Keep Amazon raw payloads in the existing FBA SQLite source database. Add normalized detail projections for catalog images and modern Finance breakdowns, then extend the FBA shipment model with invoice lines and an explicit cost-confirmation operation that creates received inventory lots. The order detail API remains the single frontend data source.

**Tech Stack:** FastAPI, SQLite, Python 3.12, Amazon SP-API Orders/Catalog/Finances APIs, React, TypeScript, Vite, pytest.

## Global Constraints

- Amazon Finance transaction amounts are authoritative for Amazon sales and Amazon fees.
- Amazon transport quotes are not final costs until a released Finance transaction exists.
- Supplier invoice net value is the product cost; invoice VAT remains separate and reviewable.
- FBA inbound transportation fees are shipment logistics costs, not product purchase costs.
- No customer name, email, street, phone, or billing address is fabricated when Amazon PII access is unavailable.
- FIFO lots are created only for shipments in `RECEIVING`, `CLOSED`, or `DELIVERED` state.
- A multi-SKU shipment requires explicit per-SKU allocation before FIFO lot creation.
- Do not change existing Procurement Batch behavior while adding the shipment-ledger path.

---

### Task 1: Normalize Complete Amazon Order Detail Data

**Files:**
- Modify: `ecommerce-dashboard/app/services/importers/amazon_sp_api.py`
- Modify: `ecommerce-dashboard/app/services/amazon_fba.py`
- Modify: `frontend/src/features/orders/order-detail-content.tsx`
- Test: `ecommerce-dashboard/tests/test_amazon_fba.py`

**Interfaces:**
- Produces `financial_breakdown` on each modern order finance event with `sales`, `tax`, `fees[]`, `net`, `financial_finality`, `maturity_date`, `order_id`, `shipment_id`, and `settlement_id`.
- Preserves `line_items[].image_url` and `line_items[].image_urls_json` from the existing catalog sync.
- Frontend consumes the existing `/api/orders/{marketplace}/{order_id}` response without a second detail request.

- [x] **Step 1: Write failing backend tests for normalized fee components and image projection.**

```python
def test_modern_finance_event_exposes_fee_breakdown(monkeypatch, tmp_path):
    # Seed one modern transaction with Commission and FBAPerUnitFulfillmentFee,
    # call get_amazon_order_detail(), and assert both fee rows are returned.
    assert detail["financial_events"][0]["financial_breakdown"]["fees"] == [
        {"type": "Commission", "amount_cents": 2659},
        {"type": "FBAPerUnitFulfillmentFee", "amount_cents": 641},
    ]

def test_amazon_detail_projects_catalog_images(monkeypatch, tmp_path):
    # Seed image_url and image_urls_json, call get_amazon_order_detail(),
    # and assert the response includes parsed image_urls.
    assert detail["line_items"][0]["image_urls"] == ["https://example.test/main.jpg"]
```

- [x] **Step 2: Run the focused tests and verify they fail for the missing response fields.**

Run: `python3 -m pytest tests/test_amazon_fba.py -k 'finance_event_exposes_fee_breakdown or detail_projects_catalog_images'`

Expected: FAIL because the detail response currently returns raw finance JSON only and leaves image variants as a JSON string.

- [x] **Step 3: Add backend normalization helpers.**

Implement `extract_modern_financial_breakdown(transaction)` in `amazon_sp_api.py` to recursively walk transaction and item breakdowns, select `Sales`, `Tax`, and `AmazonFees` descendants, normalize each fee amount as positive cents, and extract `maturityDate` from `DeferredContext`. Add `financial_breakdown` to the event dictionaries returned by `get_amazon_order_detail` without changing the persisted raw payload.

Parse `image_urls_json` in `get_amazon_order_detail` into an `image_urls` list while retaining `image_url` for compatibility.

- [x] **Step 4: Add the Amazon line-item and finance breakdown sections to the React detail view.**

Update `ImageCard` to read `item.image_url` and parsed `item.image_urls`. Add an `Amazon-Positionen` table with `Titel`, `ASIN`, `SKU`, `Menge`, `Brutto`, and `USt`. Replace the one-line finance event display with rows for event status, net amount, fee type/amount, maturity date, order ID, Amazon shipment ID, and settlement ID.

- [x] **Step 5: Run focused backend and frontend verification.**

Run: `python3 -m pytest tests/test_amazon_fba.py -k 'finance_event_exposes_fee_breakdown or detail_projects_catalog_images'`

Run: `npm run typecheck` from `frontend/`.

Expected: focused tests pass and TypeScript reports no errors.

### Task 2: Add FBA Supplier Invoice Lines and Explicit Allocation

**Files:**
- Modify: `ecommerce-dashboard/app/services/importers/amazon_sp_api.py`
- Modify: `ecommerce-dashboard/app/services/amazon_fba.py`
- Modify: `ecommerce-dashboard/app/routers/amazon.py`
- Modify: `frontend/src/features/amazon/amazon-page.tsx`
- Test: `ecommerce-dashboard/tests/test_amazon_fba.py`

**Interfaces:**
- Adds `amazon_inbound_invoice_lines` with `invoice_id`, `seller_sku`, `fnsku`, `asin`, `title`, `quantity`, `net_cents`, and `vat_cents`.
- Adds `amazon_inbound_cost_allocations` with `invoice_id`, `shipment_id`, `seller_sku`, `fnsku`, `quantity`, `net_cents`, and `allocation_method`.
- Adds `add_inbound_invoice_line(...)`, `list_inbound_invoice_lines(invoice_id)`, and `confirm_inbound_product_costs(shipment_id)` services.
- Adds `POST /api/amazon/inbound/shipments/{shipment_id}/cost-confirmation` as an admin-only endpoint.

- [x] **Step 1: Write failing tests for invoice-line validation and allocation rules.**

```python
def test_invoice_lines_allocate_exact_single_sku_cost(monkeypatch, tmp_path):
    # Seed one CLOSED shipment with 11 units and one invoice line for 11 units.
    result = amazon_fba.confirm_inbound_product_costs("FBA-1")
    assert result["lots"][0]["seller_sku"] == "SKU-1"
    assert result["lots"][0]["available_quantity"] == 11
    assert result["lots"][0]["unit_cost_cents"] == 1000

def test_multi_sku_cost_confirmation_rejects_missing_invoice_lines(monkeypatch, tmp_path):
    # Seed a CLOSED two-SKU shipment with only a shipment-level invoice total.
    with pytest.raises(ValueError, match="invoice lines"):
        amazon_fba.confirm_inbound_product_costs("FBA-2")

def test_unreceived_shipment_cannot_create_product_lot(monkeypatch, tmp_path):
    # Seed a READY_TO_SHIP shipment with a complete invoice line.
    with pytest.raises(ValueError, match="received"):
        amazon_fba.confirm_inbound_product_costs("FBA-3")
```

- [x] **Step 2: Run the allocation tests and verify they fail because the service/schema do not exist.**

Run: `python3 -m pytest tests/test_amazon_fba.py -k 'invoice_lines_allocate or multi_sku_cost_confirmation or unreceived_shipment'`

Expected: FAIL with missing service/schema behavior.

- [x] **Step 3: Add idempotent invoice-line and allocation tables.**

Extend `init_amazon_fba_db()` with the two tables and indexes. Add migrations using the existing `PRAGMA table_info` pattern. Use a stable ID based on invoice ID, SKU/FNSKU, and line index so repeated syncs or UI retries do not duplicate lines.

- [x] **Step 4: Implement invoice-line services and confirmation validation.**

`confirm_inbound_product_costs` must load the shipment, reject statuses outside `RECEIVING`, `CLOSED`, and `DELIVERED`, require at least one supplier invoice, require invoice lines for multi-SKU shipments, validate line quantities against received quantities, allocate each line's net cents exactly, and return existing lots on an idempotent repeat. For a one-SKU shipment with no line rows, reject rather than silently spreading the total.

- [x] **Step 5: Add admin API payloads and routes.**

Add Pydantic request models for invoice lines and cost confirmation. Add an endpoint to create invoice lines under an invoice and the confirmation endpoint. Convert `ValueError` to HTTP 400 using the existing router pattern.

- [x] **Step 6: Add shipment UI controls.**

In the FBA shipment detail, show invoice lines, allocated product cost, allocation status, received status, and a `Kosten bestaetigen und FIFO-Lots erzeugen` action. Disable the action until the shipment is received and allocation is unambiguous. Display the backend rejection reason inline.

- [x] **Step 7: Run the allocation tests and frontend checks.**

Run: `python3 -m pytest tests/test_amazon_fba.py -k 'invoice_lines_allocate or multi_sku_cost_confirmation or unreceived_shipment'`

Run: `npm run typecheck` from `frontend/`.

Expected: all focused tests pass and the frontend compiles.

### Task 3: Connect Shipment Lots to Existing FIFO and Order Profit

**Files:**
- Modify: `ecommerce-dashboard/app/services/amazon_fba.py`
- Modify: `ecommerce-dashboard/app/routers/amazon.py`
- Modify: `frontend/src/features/orders/order-detail-content.tsx`
- Test: `ecommerce-dashboard/tests/test_amazon_fba.py`

**Interfaces:**
- Reuses `inventory_lots` and `fifo_allocations` rather than creating a second inventory ledger.
- Adds shipment/SKU lot projections to the order detail response.
- Keeps Amazon inbound logistics costs visible on the shipment but excludes them from product COGS unless explicitly classified as product cost.

- [x] **Step 1: Write failing tests for lot projection and FIFO COGS.**

```python
def test_confirmed_fba_lot_is_consumed_by_amazon_order_fifo(monkeypatch, tmp_path):
    # Seed one CLOSED shipment, confirmed 11-unit lot at 1000 cents, and an order
    # for one matching SKU. Allocate FIFO and assert COGS is 1000 cents.
    result = amazon_fba.allocate_order_fifo("ORDER-1")
    assert result["allocated_cogs_cents"] == 1000
    assert amazon_fba.get_amazon_order_detail("ORDER-1")["summary"]["purchase_cost_cents"] == 1000
```

- [x] **Step 2: Run the FIFO test and verify it fails with the current procurement-only join.**

Run: `python3 -m pytest tests/test_amazon_fba.py -k 'confirmed_fba_lot'`

Expected: FAIL because `fifo_allocations` currently joins only `procurement_batch_lines` and cannot see shipment-created lots.

- [x] **Step 3: Extend lot provenance without breaking existing procurement lots.**

Add nullable `inbound_shipment_id` and `inbound_shipment_item_id` columns to `inventory_lots`. Make shipment-created lots use those fields and leave `batch_line_id` nullable only for shipment lots. Update the FIFO allocation query to resolve either procurement provenance or shipment provenance and retain seller SKU matching.

- [x] **Step 4: Project shipment lot provenance in the order detail.**

Return `shipment_id`, `seller_sku`, `quantity`, `unit_cost_cents`, and `received_at` in each FIFO allocation so the UI can show the exact FBA shipment that supplied the order.

- [x] **Step 5: Update the order detail FIFO section.**

Show shipment reference, SKU, quantity, unit cost, and total COGS instead of the generic `Lot 1` label. Keep the existing empty state when no lot has been confirmed.

- [x] **Step 6: Run the complete backend suite.**

Run: `python3 -m pytest`

Expected: all backend tests pass, including legacy procurement FIFO tests.

### Task 4: Live Backfill and End-to-End Verification

**Files:**
- Modify: `docs/superpowers/specs/2026-07-30-amazon-order-detail-cost-chain-design.md` only if implementation decisions changed.
- Test: `ecommerce-dashboard/tests/test_amazon_fba.py`

- [x] **Step 1: Run a read-only live check before mutation.**

Verify order `028-0075286-8561126`, ASIN `B0D95XYL1R`, SKU `YG-GYRO-RGFN`, shipment `FBA15M02LDQF`, and current invoice/lot state through the existing API and source database.

- [x] **Step 2: Run Amazon order/catalog/finance sync.**

Run the existing sync with orders, finances, and inbound enabled. Confirm that catalog images remain present, the modern finance event remains idempotent, and no duplicate financial event or image row is created.

- [x] **Step 3: Verify target detail fields.**

The target order must show the product image, product line item, `26.59 EUR` commission, `6.41 EUR` FBA fulfillment fee, `33.00 EUR` total fees, deferred status and maturity date, available partial address, and no fabricated PII.

- [x] **Step 4: Run frontend production verification.**

Run: `npm run typecheck && npx vite build` from `frontend/`.

Expected: typecheck passes and Vite build completes successfully.
