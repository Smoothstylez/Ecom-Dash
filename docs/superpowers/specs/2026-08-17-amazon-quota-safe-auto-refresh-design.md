# Amazon Quota-Safe Auto Refresh Design

## Goal

Keep Amazon orders and operational data current without repeatedly running a full SP-API import, triggering avoidable throttling, or disrupting the existing Shopify and Kaufland background worker.

## Scope

- Add a dedicated Amazon background worker; do not put Amazon calls into the existing Shopify/Kaufland live-sync cycle.
- Refresh recent orders every 5 minutes, recent finance data every 15 minutes, and inventory plus FBA inbound data every 30 minutes.
- Run a bounded reconciliation once per day to repair missed data.
- Use the existing changestamp so active dashboard views reload their data after an Amazon mutation without a browser hard reload.
- Expose the Amazon worker status, next run, last successful run, last error, and active backoff through the existing Amazon status API and status UI.

## Scheduling

### Delta Work

- Orders: every 5 minutes, query only an overlap window ending at the current time. The overlap is 20 minutes so late Amazon updates are re-read safely.
- Finance: every 15 minutes, query the last 48 hours. Finance events can be deferred or arrive after shipment.
- Inventory and inbound: every 30 minutes. The inbound call stays account-level and uses the existing EU primary marketplace selection.
- Catalog images: never run as a periodic refresh. Fetch only for a new/order item with no stored image URL, preserving the existing image cache.

### Reconciliation

- Once every 24 hours, run a 30-day Orders/Finance reconciliation and a regular inventory/inbound refresh.
- Historical 730-day imports remain manual-only. A background process never repeats the expensive historical run.
- Each scheduled task is independently due. A Finance failure does not block Orders, and an Inventory throttle does not block Finance.

## Quota and Failure Policy

- Every Amazon API response records its available `x-amzn-RateLimit-Limit` header when present for diagnostics.
- A `429` or `503` applies exponential backoff only to the affected task: 5, 10, 20, 40, then 60 minutes maximum.
- A successful run clears that task's backoff.
- No automatic retry loop runs inside a single scheduled cycle; the worker waits for the next calculated eligible time.
- Multiple triggers are coalesced while an Amazon task is in progress.
- The worker uses a separate lock from the existing live-sync worker and from manual `/api/amazon/sync` requests.

## Data Flow

1. The Amazon worker determines which tasks are due from persistent task state.
2. It runs only those tasks with their small, fixed lookback windows.
3. Existing idempotent upserts update orders, finance events, inbound shipments, and inventory snapshots.
4. If a task writes data, it bumps `changestamp` once after the task completes.
5. The existing dashboard controls poll `/api/sync/changestamp`; when it changes, active pages receive the existing `refreshRequestToken` and reload their API data.
6. The Orders page refreshes its list/detail through its current refresh-token behavior; no full document reload is used.

## Notifications

- Do not implement Amazon Notifications API in this phase.
- Amazon supports push notifications through SQS or EventBridge and recommends a polling fallback for delivery delays/outages.
- The dedicated scheduler is the operational solution now. SQS/EventBridge remains a future enhancement when near-real-time updates justify AWS IAM, SQS/EventBridge resources, subscription lifecycle management, and message processing.

## API and UI

- `GET /api/amazon/status` adds a compact `auto_refresh` object with enabled state, per-task timings, backoff state, and last result.
- Add an admin-only endpoint to trigger one Amazon delta cycle immediately; it uses the same scheduler lock and task policy as the worker.
- The Amazon dashboard shows last successful Order, Finance, and Inventory/Inbound refresh plus any active quota backoff.
- The existing global dashboard polling setting remains the client-side refresh control; no additional browser polling timer is added.

## Constraints

- Preserve existing Tailscale Serve configuration and dashboard ports.
- Do not change existing Shopify/Kaufland worker cadence or behavior.
- Do not automatically run a 730-day Amazon history import.
- Never manufacture missing Amazon financial data for pending orders.
- Keep all Amazon SP-API secrets out of status responses, logs, and UI.

## Verification

- Unit tests cover due-time selection, task isolation, overlapping-window parameters, backoff growth/reset, trigger coalescing, and changestamp behavior.
- Tests prove an Inventory `429` does not prevent the next Orders task.
- Tests prove a manual 730-day sync remains separate from scheduled work.
- Backend suite, frontend typecheck, and production Vite build pass.
