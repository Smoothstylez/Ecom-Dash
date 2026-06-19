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

Support ticket routes are documented in `docs/dashboard-backend-api.md` and should be preferred over UI clicks when working the Kaufland support inbox.

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

Maintenance rule:

- If backend routes, payloads, response fields, or allowed automation scope
  change, update this skill, the agent file, `docs/dashboard-backend-api.md`,
  and any affected helper scripts in `scripts/dashboard-api/` in the same
  commit.
