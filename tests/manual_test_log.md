# Manual Test Log

**Date:** 2026-03-14  
**Tester:** [Your Name]  
**Dashboard Version:** [check in UI]  

---

## Test Execution Summary

| Category | Total Tests | Passed | Failed | Blocked | Notes |
|----------|------------|--------|--------|---------|-------|
| Orders | 5 | - | - | - | TBD |
| Invoices | 4 | - | - | - | TBD |
| Customers | 4 | - | - | - | TBD |
| Bookings | 3 | - | - | - | TBD |
| Import/Export | 4 | - | - | - | TBD |
| Data Management | 4 | - | - | - | TBD |
| Performance | 2 | - | - | - | TBD |
| **TOTAL** | **26** | **-** | **-** | **-** | - |

---

## Test Results by Category

### Orders Management (ORD)

#### ORD-01: Create Order
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** Use fixture: `fixtures/orders.json` (ORD-001)
- **Expected:** Order created with ID, status=pending, total calculated
- **Actual Result:** 
- **Notes:** 

#### ORD-02: Filter Orders by Date Range
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** Date from 2026-03-01 to 2026-03-14
- **Expected:** All 5 sample orders returned, ordered correctly
- **Actual Result:** 
- **Notes:** 

#### ORD-03: Update Order Status
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** ORD-001 status → "shipped"
- **Expected:** Status updated, timestamp changed
- **Actual Result:** 
- **Notes:** 

#### ORD-04: Search Orders by Customer Name
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** Search term: "John"
- **Expected:** Returns ORD-001, ORD-003 (John Doe orders)
- **Actual Result:** 
- **Notes:** 

#### ORD-05: Multi-Source Order Merge
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** Query all sources (shopify, ebay, kaufland)
- **Expected:** Combined list with no duplicates
- **Actual Result:** 
- **Notes:** 

---

### Invoices & Bookkeeping (INV)

#### INV-01: Generate Invoice from Order
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** ORD-001
- **Expected:** Invoice created, PDF exists, contains line items
- **Actual Result:** 
- **Notes:** 

#### INV-02: Calculate Fees
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** Order total: 100.00, source: ebay, shipping: 5.00
- **Expected:** eBay fee 12%, total with fees correct
- **Actual Result:** 
- **Notes:** 

#### INV-03: Invoice Overview / Report
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** Period: 2026-03, group by source
- **Expected:** Grouped summary with totals
- **Actual Result:** 
- **Notes:** 

#### INV-04: Link Invoice to Multiple Orders
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** Link ORD-001, ORD-002, ORD-003 to single invoice
- **Expected:** Bulk invoice created, sum = all orders total
- **Actual Result:** 
- **Notes:** 

---

### Customers Management (CUST)

#### CUST-01: Create Customer
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** Use fixture: `fixtures/customers.json` (CUST-004 - Alice)
- **Expected:** Customer created with all fields
- **Actual Result:** 
- **Notes:** 

#### CUST-02: Link Customer to Orders
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** CUST-001 already has ORD-001, ORD-003
- **Expected:** LTV calculated correctly (127.53)
- **Actual Result:** 
- **Notes:** 

#### CUST-03: Validation
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** Invalid email, invalid phone
- **Expected:** Form rejects, shows field errors
- **Actual Result:** 
- **Notes:** 

#### CUST-04: Export CSV
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** Export all customers
- **Expected:** CSV created, headers correct, 5 rows
- **Actual Result:** 
- **Notes:** 

---

### Bookings Management (BOOK)

#### BOOK-01: Create Booking from Order
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** ORD-001
- **Expected:** Booking created, auto-linked, status=confirmed
- **Actual Result:** 
- **Notes:** 

#### BOOK-02: Sync to Bookkeeping
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** BOOK-001, BOOK-002, BOOK-003
- **Expected:** 3 entries created in bookkeeping
- **Actual Result:** 
- **Notes:** 

#### BOOK-03: Validate Totals
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** BOOK-001
- **Expected:** Total = sum of order + fees, math is correct
- **Actual Result:** 
- **Notes:** 

---

### Import / Export (IMPORT)

#### IMPORT-01: Import Orders CSV
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** `fixtures/import_export_samples.json` → orders_sample.csv
- **Expected:** 3 orders imported, no errors
- **Actual Result:** 
- **Notes:** 

#### IMPORT-02: Schema Validation (Strict)
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** orders_invalid.csv
- **Expected:** Import blocked, field errors reported
- **Actual Result:** 
- **Notes:** 

#### IMPORT-03: Export Filtered Orders (JSON)
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** Filter: date_from=2026-03-01, source=shopify
- **Expected:** JSON file with ORD-001, ORD-004 only
- **Actual Result:** 
- **Notes:** 

#### IMPORT-04: Bulk Export
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** Export all (orders, invoices, customers)
- **Expected:** 3 files created, all valid JSON/CSV
- **Actual Result:** 
- **Notes:** 

---

### Data Management (DATA)

#### DATA-01: Duplicate Detection
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** Same customer, same item, 5 min apart
- **Expected:** Detected as duplicate, merge suggested
- **Actual Result:** 
- **Notes:** 

#### DATA-02: Integrity Check
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** Full audit
- **Expected:** No orphaned records, all refs valid
- **Actual Result:** 
- **Notes:** 

#### DATA-03: Archive Old Orders
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** Archive before 2026-01-01
- **Expected:** Archived successfully, still queryable
- **Actual Result:** 
- **Notes:** 

#### DATA-04: Restore from Archive
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Input:** Restore archived set
- **Expected:** Matches original, queryable in main
- **Actual Result:** 
- **Notes:** 

---

### Performance (PERFORM)

#### PERFORM-01: Load Test (1000 Orders)
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Response Time:** _____ ms (target: < 1000 ms)
- **No Timeout:** [ ] Yes [ ] No
- **Notes:** 

#### PERFORM-02: Bulk Import (500 records)
- **Status:** [ ] PASS [ ] FAIL [ ] BLOCKED
- **Import Time:** _____ sec (target: < 10 sec)
- **Success Rate:** _____ %
- **Notes:** 

---

## Issues Found

| ID | Category | Severity | Description | Status |
|---|----------|----------|-------------|--------|
| BUG-001 | | [ ] CRITICAL [ ] HIGH [ ] MEDIUM [ ] LOW | | [ ] Open [ ] Fixed |
| | | | | |

---

## Recommendations

- 
- 
- 

---

**Tester Signature:** _________________ **Date:** _______
