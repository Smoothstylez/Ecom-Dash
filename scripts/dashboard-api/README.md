# Dashboard API Helpers

Small shell helpers for direct production backend automation.

Environment:

```bash
export DASHBOARD_BASE_URL="http://192.168.178.197:8012"
export DASHBOARD_ADMIN_TOKEN="..."
```

Helpers:

- `order-search.sh`: list and filter dashboard orders
- `set-tracking.sh`: submit shipment tracking through the backend API
- `upload-order-invoice.sh`: upload a purchase invoice file to an order
- `create-sales-invoice.sh`: read invoice draft and create a sales invoice

Shared library:

- `_lib.sh`

Maintenance rule:

- If a related backend route or payload changes, update these helpers together
  with `docs/dashboard-backend-api.md`, the skill file, and the agent file.
