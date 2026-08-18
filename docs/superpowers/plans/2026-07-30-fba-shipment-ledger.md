# FBA Shipment Ledger Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Amazon FBA procurement batches with shipment-centered inbound records that preserve Amazon API data, support invoice/cost matching, and show unreceived shipments in the dashboard.

**Architecture:** Amazon inbound shipments become the primary procurement and inventory boundary. The sync combines the legacy inbound API for complete shipment discovery and item receipt quantities with the modern inbound API for plans, boxes, transport quotes, and active shipment details. Finance events remain immutable; shipment assignment is explicit when Amazon omits a shipment ID.

**Tech Stack:** FastAPI, SQLite, Python pytest, React, TypeScript, Vite.

## Global Constraints

- `READY_TO_SHIP` is displayed as `Nicht versendet` and does not create inventory or FIFO lots.
- `IN_TRANSIT` is displayed immediately and does not wait for check-in.
- `CLOSED` and `RECEIVING` use Amazon-reported received quantities for inventory/FIFO.
- Amazon Finance costs without a shipment ID are suggestions until manually confirmed.
- Raw Amazon payloads remain available for audit and rematching.
- Do not delete existing unrelated worktree changes.

---

### Task 1: Extend Inbound Persistence and Normalization

**Files:**
- Modify: `ecommerce-dashboard/app/services/importers/amazon_sp_api.py`
- Test: `ecommerce-dashboard/tests/test_amazon_fba.py`

**Interfaces:**
- Add normalized shipment helpers that accept legacy and modern payloads and return stable shipment/line dictionaries.
- Persist shipment details, item quantities, boxes, transport options, and cost candidates idempotently by Amazon shipment ID.

- [ ] **Step 1: Write failing tests**

Add tests for:

```python
def test_ready_to_ship_is_not_received():
    assert normalize_fba_status("READY_TO_SHIP") == {
        "label": "Nicht versendet",
        "received": False,
        "inventory_eligible": False,
    }

def test_shipment_items_are_deduplicated_by_sku_and_fnsku():
    rows = normalize_shipment_items([
        {"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 6, "QuantityReceived": 0},
        {"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 6, "QuantityReceived": 0},
    ])
    assert rows == [{"seller_sku": "SKU-1", "fnsku": "FNSKU-1", "quantity_shipped": 6, "quantity_received": 0}]

def test_inbound_cost_without_shipment_id_is_unassigned():
    assert suggest_shipment_for_inbound_cost(4725, "EUR", [], []) is None
```

- [ ] **Step 2: Run the focused tests and confirm they fail**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -k "ready_to_ship or shipment_items or inbound_cost" -v`

Expected: FAIL because the normalization and matching functions do not exist.

- [ ] **Step 3: Add shipment-focused schema migrations**

Add tables for:

```sql
amazon_inbound_shipment_items(shipment_id, seller_sku, fnsku, asin, title, quantity_shipped, quantity_received, raw_json)
amazon_inbound_shipment_boxes(shipment_id, box_id, weight_value, weight_unit, length, width, height, dimension_unit, raw_json)
amazon_inbound_transport_options(shipment_id, option_id, carrier, shipping_solution, shipping_mode, quote_cents, currency, selected, raw_json)
amazon_inbound_costs(id, shipment_id, source_event_id, cost_type, amount_cents, currency, status, allocation_method, raw_json)
```

Add `shipment_id` to procurement records only as a migration bridge; new code must use `amazon_inbound_shipments` as the primary reference. Existing empty procurement tables are not copied into new data.

- [ ] **Step 4: Implement normalization and idempotent upserts**

Implement the smallest helpers needed by the tests. Deduplicate repeated API pages by `(seller_sku, fnsku)` without summing repeated pages. Use shipment ID as the unique key. Preserve every raw response through the existing raw-record mechanism.

- [ ] **Step 5: Run focused tests and then all backend tests**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -v`

Expected: PASS.

### Task 2: Import All Shipment Statuses and API Enrichment

**Files:**
- Modify: `ecommerce-dashboard/app/services/importers/amazon_sp_api.py`
- Modify: `ecommerce-dashboard/app/routers/amazon.py`
- Test: `ecommerce-dashboard/tests/test_amazon_fba.py`

**Interfaces:**
- `sync_amazon_fba(..., include_inbound=True)` imports inbound data.
- `GET /api/amazon/inbound/shipments` returns all non-deleted shipments with status, counts, costs, and invoice state.
- `GET /api/amazon/inbound/shipments/{shipment_id}` returns detail lines, boxes, transport options, costs, and raw-match metadata.

- [ ] **Step 1: Write failing sync tests**

Test that mocked legacy results include `CLOSED`, `RECEIVING`, `READY_TO_SHIP`, and `IN_TRANSIT`, and that `READY_TO_SHIP` is not treated as received. Test that modern shipment confirmation IDs map to the legacy FBA shipment ID.

- [ ] **Step 2: Run the tests and confirm the expected failure**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -k inbound -v`

Expected: FAIL because `include_inbound` and inbound routes are absent.

- [ ] **Step 3: Implement legacy discovery with pagination**

Request all valid statuses: `WORKING`, `READY_TO_SHIP`, `SHIPPED`, `RECEIVING`, `CANCELLED`, `DELETED`, `CLOSED`, `ERROR`, `IN_TRANSIT`, `DELIVERED`, and `CHECKED_IN`. Continue with `QueryType=NEXT_TOKEN`. Deduplicate by `ShipmentId` across marketplace responses because Amazon can return the same shipment for multiple marketplace IDs.

- [ ] **Step 4: Enrich each shipment safely**

Fetch legacy item lines for shipped/received quantities. When a modern inbound plan maps to the shipment, fetch plan detail, modern shipment detail, boxes, and transportation options. A 403 on optional enrichment must be recorded as a partial enrichment error, not discard the shipment.

- [ ] **Step 5: Add API endpoints and sync summary counters**

Expose shipment list/detail routes and add `inbound_shipments`, `inbound_items`, and `inbound_errors` to the sync summary. Do not expose secret payloads.

- [ ] **Step 6: Run focused and full backend tests**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -v && python3 -m pytest`

Expected: PASS.

### Task 3: Shipment-Centered Costs, Invoices, and FIFO Eligibility

**Files:**
- Modify: `ecommerce-dashboard/app/services/amazon_fba.py`
- Modify: `ecommerce-dashboard/app/routers/amazon.py`
- Modify: `ecommerce-dashboard/app/uploads.py` or the existing document upload service used by the app
- Test: `ecommerce-dashboard/tests/test_amazon_fba.py`

**Interfaces:**
- `POST /api/amazon/inbound/shipments/{shipment_id}/costs` records product, freight, Amazon inbound, or other costs.
- `POST /api/amazon/inbound/shipments/{shipment_id}/invoices` stores a document and invoice metadata.
- `POST /api/amazon/inbound/shipments/{shipment_id}/costs/{cost_id}/confirm` confirms a suggested Amazon cost assignment.

- [ ] **Step 1: Write failing cost and FIFO tests**

Cover:

```python
def test_ready_to_ship_cannot_create_inventory_lot(): ...
def test_receiving_lot_uses_received_quantity_not_shipped_quantity(): ...
def test_confirmed_freight_cost_is_stored_on_shipment(): ...
def test_unlinked_finance_event_stays_unassigned(): ...
```

- [ ] **Step 2: Run tests and verify they fail**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -k "cost or fifo or lot" -v`

Expected: FAIL because shipment cost and receipt-aware lot operations are absent.

- [ ] **Step 3: Implement shipment cost records and invoice attachment**

Use existing document storage conventions. Keep invoice metadata and uploaded file path on the shipment. Allow multiple supplier, freight, and Amazon documents per shipment.

- [ ] **Step 4: Implement explicit finance matching**

Create a candidate record for account-level `FBAInboundTransportationFee` and `FBAInboundTransportationProgramFee`. Match suggestions only by explainable evidence such as user confirmation; never auto-assign solely by amount. On confirmation, retain source finance event ID and source amount.

- [ ] **Step 5: Gate FIFO by receipt status**

`READY_TO_SHIP` and `IN_TRANSIT` cannot create lots. `RECEIVING` creates at most `quantity_received`; `CLOSED` can use all received quantity. Apply confirmed product and allocated inbound costs to unit cost.

- [ ] **Step 6: Run focused tests and all backend tests**

Run: `cd ecommerce-dashboard && python3 -m pytest tests/test_amazon_fba.py -v && python3 -m pytest`

Expected: PASS.

### Task 4: Replace Batch UI with FBA Shipment UI

**Files:**
- Modify: `frontend/src/features/amazon/amazon-page.tsx`
- Modify: `frontend/src/features/amazon/amazon-page.css` if present, otherwise the existing Amazon/global stylesheet
- Test: `frontend/src/features/amazon/amazon-page.test.tsx` if the project test setup supports it; otherwise verify through typecheck/build and API response fixtures.

**Interfaces:**
- The page consumes `/api/amazon/inbound/shipments` and `/api/amazon/inbound/shipments/{shipment_id}`.
- The page renders shipment status labels, cost status, received/shipped counts, and invoice actions.

- [ ] **Step 1: Write the failing UI assertion or fixture contract**

Assert that a shipment with `status="READY_TO_SHIP"` renders `Nicht versendet`, while `IN_TRANSIT` renders `Unterwegs` and `CLOSED` renders `Empfangen`.

- [ ] **Step 2: Run the UI check and confirm failure**

Run: `cd frontend && npm run typecheck`

Expected: the new shipment contract/component is missing.

- [ ] **Step 3: Replace the procurement-batch section**

Render shipments as the primary section. Keep the existing finance table below it, but remove the implication that procurement batches are the source of truth. Add filter buttons for all, not sent, in transit, receiving, and received.

- [ ] **Step 4: Add detail and invoice controls**

Allow opening one shipment, viewing item lines and costs, entering/uploading invoices, and confirming a suggested Amazon cost. Disable inventory/FIFO actions when the shipment has no received quantity.

- [ ] **Step 5: Run frontend verification**

Run: `cd frontend && npm run typecheck && npx vite build`

Expected: PASS.

### Task 5: End-to-End Live Verification

**Files:**
- Modify: `ecommerce-dashboard/tests/test_amazon_fba.py` only if a discovered API edge case needs a regression test.

- [ ] **Step 1: Run the Amazon sync with inbound enabled**

Call `POST /api/amazon/sync` with `{"include_orders": false, "include_inventory": false, "include_finances": true, "include_inbound": true}` and verify that all four shipment IDs are persisted.

- [ ] **Step 2: Verify the shipment API**

Confirm:

```text
FBA15M02LDQF -> 11 shipped, 11 received, CLOSED
FBA15M1Y923F -> 36 shipped, 36 received, RECEIVING
FBA15M3CBF5T -> 12 shipped, 0 received, IN_TRANSIT
FBA15LYJCCXF -> 516 shipped, 0 received, READY_TO_SHIP / Nicht versendet
```

- [ ] **Step 3: Verify cost behavior**

Confirm the `47,25 EUR` finance event is visible as an unassigned suggestion and is not booked into FIFO until explicitly assigned.

- [ ] **Step 4: Verify frontend build and final status**

Run backend tests, frontend typecheck/build, and inspect the working tree to ensure only intended shipment feature files changed.
