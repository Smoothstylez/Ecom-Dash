# Amazon SP-API Rate Limiter Design

## Goal

Prevent the dashboard's Amazon SP-API syncs from exceeding Amazon quotas while
reliably importing product lines for all Amazon FBA orders. A historical
backfill may take several minutes; correctness and quota safety take priority
over speed.

## Background

Amazon order `303-2546340-3457930` had a valid order header and financial
event, but no `amazon_order_items` row. A production orders-only sync confirmed
that its `GET /orders/v0/orders/{orderId}/orderItems` request received HTTP 429
after an unpaced loop tried to fetch item lists for a 30-day set of 50 orders.

The order-item API advertises a rate of 0.5 requests per second. Existing retry
handling in version 0.5.6 is a safety net, but it sends the first request before
checking capacity, so it does not prevent the initial quota breach. Order-list,
catalog, manual sync, and background sync requests also need one shared policy.

## Design

### Persistent endpoint buckets

Add an additive `amazon_api_rate_limits` table to the existing Amazon FBA
SQLite database. Each row represents one shared API quota bucket, rather than
an individual URL. Dynamic paths such as every order's `/orderItems` URL map to
the same bucket.

Required persisted state per bucket:

- `bucket_key`: primary key.
- `rate_per_second`: current known refill rate.
- `burst_capacity`: conservative maximum token capacity.
- `tokens`: remaining tokens after the last reservation.
- `updated_at`: UTC timestamp used to refill tokens.
- `blocked_until`: optional UTC cooldown after an HTTP 429 or 503.
- `last_throttle_at` and `last_throttle_error`: operator-visible diagnostics.

The initial bucket policy is deliberately conservative. A successful Amazon
response's `x-amzn-RateLimit-Limit` header updates the refill rate but never
raises burst capacity automatically.

| Bucket | Route family | Initial rate | Burst |
| --- | --- | ---: | ---: |
| `orders` | `/orders/v0/orders` | 0.0167/s | 20 |
| `order_items` | `/orders/v0/orders/{id}/orderItems` | 0.5/s | 30 |
| `catalog` | `/catalog/.../items/{asin}` | 2.0/s | 2 |
| `default` | unclassified routes | 1.0/s | 1 |

Existing inbound, inventory, finance, report, and seller routes retain their
current call structure but pass through `default` until an explicit bucket is
needed. This prevents a new unclassified endpoint from being sent in a burst.

### Atomic reservation and cooldown

`AmazonSpApiClient.request_json()` obtains a token before opening an HTTP
request. The limiter:

1. Opens a short SQLite `BEGIN IMMEDIATE` transaction.
2. Refills the selected bucket based on elapsed UTC wall-clock time,
   capped at its burst capacity.
3. Reserves one token when available, or calculates the exact wait time until
   one token is available.
4. Commits immediately before sleeping; it never holds a database lock while
   waiting or doing network I/O.
5. Repeats until it reserves a token.

All background and manual syncs already share the Amazon sync lease. Persistent
bucket state additionally preserves cooldowns and token accounting across
process restarts and protects any future call path that uses the client.

On HTTP 429 or 503, `request_json()` records a cooldown on the relevant bucket.
It uses `Retry-After` when Amazon provides it; otherwise it applies a bounded
exponential delay. The existing caller-level retries remain, but their next
attempt must reserve a new token and respect that cooldown.

### Item-first order synchronization

After receiving order headers, the sync checks the local item count for each
order before calling the order-items API:

- New orders and existing orders with zero stored items are fetched first.
- Orders that already have items are not fetched again during an ordinary
  delta or historical backfill, because Amazon order items are immutable for
  this dashboard's purpose.
- The item loop sorts missing-item orders by `LastUpdateDate` and then
  `PurchaseDate`, descending, so recent missing orders are repaired first.
- If one order-items request ultimately fails, it is recorded in the sync
  summary and the sync continues with the remaining orders.

Catalog image lookup is strictly after successful item retrieval. A catalog
failure must not prevent storing an item's title, ASIN, SKU, quantity, or price.

### Status and UI

`GET /api/amazon/status` gains a `rate_limits` section with one compact record
per active bucket: rate, available-token estimate, `blocked_until`, last
throttle timestamp, and last throttle message. It also reports the current
count of Amazon orders with no stored items.

The existing "Amazon-Aktualisierung" card on the Amazon page displays a concise
rate-limit line when any bucket is waiting or blocked, plus the count of orders
whose item lines are pending. It does not expose raw request URLs or secrets.

## Non-goals

- No parallel item fetching. Sequential reservation is intentional.
- No deletion or replacement of existing order-item data during a backfill.
- No change to the manual marketplace selection or `include_all_marketplaces`
  behavior.
- No automatic item-title synthesis from financial events; financial metadata
  remains supplementary, while the canonical item record comes from the Orders
  API.

## Testing

- Unit tests verify shared dynamic URLs map to one bucket, correct refill/wait
  calculation, atomic reservation, cooldown application, and `Retry-After`
  precedence.
- Client tests verify a 429 waits through the limiter before retrying and that
  successful response headers update the persisted rate.
- Sync tests verify orders with stored items do not call `order_items`, zero-
  item orders are requested, and an item failure does not stop later orders.
- Status tests verify pending item counts and rate-limit diagnostics are safe
  to return from the public status endpoint.
- Frontend typecheck and production build pass.

## Verification

- Full backend test suite passes.
- Deploy the new add-on version.
- Run one 30-day Amazon orders-only backfill with Amazon.de selected.
- Confirm that the sync reports controlled waits rather than 429 errors and
  that `303-2546340-3457930` receives its stored item record.
