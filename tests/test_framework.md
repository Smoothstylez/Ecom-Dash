# Test Framework Overview

## Project Analyzed
- **Dashboard:** `ecommerce-dashboard/`
- **Framework:** FastAPI (Python)
- **Databases:** SQLite (combined, shopify, kaufland, bookkeeping)

## Test Categories

### 1. Orders Management (ORD)
**Feature:** Order CRUD, filtering, search, multi-source merge
**Endpoints:** `/api/orders` (GET, PATCH)
**Data Sources:** Shopify, Kaufland, eBay

| Test ID | Description | Priority |
|---------|-------------|----------|
| ORD-01 | Create Order | HIGH |
| ORD-02 | Filter Orders by Date | HIGH |
| ORD-03 | Update Order Status | HIGH |
| ORD-04 | Search by Customer Name | MEDIUM |
| ORD-05 | Multi-Source Merge | MEDIUM |

---

### 2. Invoices & Bookkeeping (INV)
**Feature:** Invoice generation, fee calculation, reports
**Endpoints:** `/api/orders/{id}/invoice` (POST, GET)
**Bookkeeping:** `/api/bookings/transactions` (GET, POST)

| Test ID | Description | Priority |
|---------|-------------|----------|
| INV-01 | Generate Invoice from Order | HIGH |
| INV-02 | Calculate Platform Fees | HIGH |
| INV-03 | Invoice Overview Report | MEDIUM |
| INV-04 | Link Invoice to Multiple Orders | MEDIUM |

**Fee Structure:**
- Shopify: 3% + payment processor
- eBay: 12% + payment processor
- Kaufland: 8% + payment processor

---

### 3. Customers Management (CUST)
**Feature:** Customer CRUD, validation, export
**Endpoints:** `/api/customers`

| Test ID | Description | Priority |
|---------|-------------|----------|
| CUST-01 | Create/Register Customer | HIGH |
| CUST-02 | Link Customer to Orders | HIGH |
| CUST-03 | Data Validation | HIGH |
| CUST-04 | Export CSV | MEDIUM |

---

### 4. Bookings Management (BOOK)
**Feature:** Booking creation, bookkeeping sync
**Endpoints:** `/api/bookings`, `/api/bookings/transactions`

| Test ID | Description | Priority |
|---------|-------------|----------|
| BOOK-01 | Create Booking from Order | HIGH |
| BOOK-02 | Sync to Bookkeeping | HIGH |
| BOOK-03 | Validate Totals | MEDIUM |

---

### 5. Import / Export (IMPORT)
**Feature:** CSV/JSON import, bulk export
**Endpoints:** `/api/exports`, import via file upload

| Test ID | Description | Priority |
|---------|-------------|----------|
| IMPORT-01 | Import Orders CSV | HIGH |
| IMPORT-02 | Schema Validation | HIGH |
| IMPORT-03 | Export Filtered JSON | MEDIUM |
| IMPORT-04 | Bulk Export | MEDIUM |

---

### 6. Data Management (DATA)
**Feature:** Integrity, duplicates, archiving
**Endpoints:** Internal DB operations

| Test ID | Description | Priority |
|---------|-------------|----------|
| DATA-01 | Duplicate Detection | HIGH |
| DATA-02 | Integrity Check | HIGH |
| DATA-03 | Archive Old Orders | MEDIUM |
| DATA-04 | Restore from Archive | MEDIUM |

---

### 7. Performance (PERFORM)
**Feature:** Load testing, bulk operations

| Test ID | Description | Priority |
|---------|-------------|----------|
| PERFORM-01 | 1000 Orders Query | MEDIUM |
| PERFORM-02 | Bulk Import 500 | LOW |

---

## Fixtures Reference

| File | Records | Purpose |
|------|---------|---------|
| `fixtures/orders.json` | 5 | Order CRUD tests |
| `fixtures/invoices.json` | 5 | Invoice generation |
| `fixtures/customers.json` | 5 | Customer management |
| `fixtures/bookings.json` | 5 | Booking & sync tests |
| `fixtures/bookkeeping.json` | 5 | Bookkeeping entries |
| `fixtures/import_export_samples.json` | 5 samples | Import/Export tests |

---

## Test Execution Workflow

1. **Start Dashboard**
   ```bash
   cd ecommerce-dashboard
   python -m app.main
   ```
   Default: `http://localhost:8012/`

2. **Run Tests Manually**
   - Use `test_cases.json` as test matrix
   - Reference fixture data for inputs
   - Log results in `manual_test_log.md`

3. **Validate Results**
   - Compare actual output vs expected
   - Record issues in Issues Found table

---

## Expected Test Results

- **Total Tests:** 26
- **Expected Pass Rate:** > 90%
- **Critical Failures:** 0

---

## Test Data Schema (Valid Fixtures)

### Order
```json
{
  "id": "ORD-XXX",
  "customer_id": "CUST-XXX",
  "customer_name": "string",
  "source": "shopify|kaufland|ebay",
  "items": [{"sku": "string", "qty": int, "unit_price": float}],
  "total": float,
  "status": "pending|processing|shipped|delivered|cancelled"
}
```

### Customer
```json
{
  "id": "CUST-XXX",
  "name": "string",
  "email": "valid@email.de",
  "phone": "+49...",
  "country": "DE",
  "total_orders": int,
  "lifetime_value": float
}
```

---

**Framework Version:** 1.0  
**Last Updated:** 2026-03-14
