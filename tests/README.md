# Dashboard Test Suite

**Purpose:** Comprehensive testing of ecommerce-dashboard features (Orders, Invoices, Customers, Bookings, Import/Export, Bookkeeping, etc.)

**Test Data:** Combination of mock fixtures + schema validation

## Structure

```
tests/
├── README.md                  ← You are here
├── test_cases.json           ← Test matrix (all features)
├── test_framework.md         ← Feature breakdown
├── fixtures/                 ← Mock data
│   ├── orders.json
│   ├── invoices.json
│   ├── customers.json
│   ├── bookings.json
│   ├── bookkeeping.json
│   └── import_export_samples.json
└── manual_test_log.md        ← Results log (you'll fill this)
```

## How to Use

1. **Review** `test_cases.json` — see all test scenarios
2. **Use fixtures** as reference data when running tests
3. **Log results** in `manual_test_log.md`
4. **Report** findings back to Atlas

## Test Categories

- **Orders**: Create, Read, Update, Delete, Filter, Search
- **Invoices/Bookkeeping**: Generate, View, Link to Orders, Calculate Fees
- **Customers**: Manage, Link to Orders, Export
- **Bookings**: Create, Sync, Validate
- **Import/Export**: Upload CSV/JSON, Export filtered data, Validate schema
- **Data Management**: Archive, Restore, Bulk operations

## Starting Dashboard

```bash
cd ecommerce-dashboard
# Check config.yaml first (ports, DB paths)
python -m app.main
# or: bash run.sh
```

Default: `http://localhost:8000/`

---

Next → `test_cases.json` for detailed test matrix
