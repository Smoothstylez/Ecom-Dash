# Amazon Marketplace Activity Filtering Design

## Goal

Reduce Amazon SP-API quota consumption and 429 errors by only syncing
marketplaces the seller is actually enrolled in, instead of every
marketplace Amazon's account is merely eligible to sell on.

## Problem

`AmazonSpApiClient.marketplace_participations()` returns every marketplace
associated with the seller account — in this account's case 10 — regardless
of whether the seller has ever activated/enrolled in most of them.
`_upsert_marketplaces()` currently passes through every marketplace ID
except explicit "Non-Amazon" service entries. Every downstream call that
loops per marketplace (`client.orders()`, the inventory sync loop in
`sync_amazon_fba()`) queries all 10, even though only one
(`A1PA6795UKMFR9`, Amazon.de) has ever produced real order data for this
account. This multiplies API calls unnecessarily and increases the odds
that at least one marketplace's request gets throttled (`429
QuotaExceeded`) within a single sync pass, which in turn delays or spreads
out data completeness for the one marketplace that actually matters.

## Design

### Data already available

Amazon's `/sellers/v1/marketplaceParticipations` response includes a
`participation.isParticipating` boolean per marketplace, indicating
whether the seller has actually enrolled in that marketplace (as opposed
to merely being eligible to sell there). This field is already stored
verbatim in `amazon_marketplaces.participation_json` but is never read
back out or used for filtering.

### Change

1. `_upsert_marketplaces()` gains an `active_only: bool = True` parameter
   (default `True`, matching the "filter everywhere by default" behavior).
   When `True`, the returned `list[str]` of marketplace IDs excludes any
   marketplace whose `participation.isParticipating` is not `true`
   (missing/absent is treated as not participating, to fail safe toward
   *fewer* API calls rather than more). All marketplaces are still
   persisted to `amazon_marketplaces` regardless of this filter — only the
   returned ID list used for subsequent per-marketplace API calls is
   affected. This preserves marketplace metadata (name, currency, etc.)
   for every marketplace the account is eligible for, useful for display
   or future manual overrides, while only *querying* active ones.

2. `sync_amazon_fba()` gains an `include_all_marketplaces: bool = False`
   parameter, threaded through to `_upsert_marketplaces(connection,
   participations, active_only=not include_all_marketplaces)`.

3. `AmazonSyncRequest` (the manual `/api/amazon/sync` request model) gains
   `include_all_marketplaces: bool = False`, passed through to
   `sync_amazon_fba()`.

4. The scheduled auto-refresh tasks in `amazon_auto_refresh.py` are
   unaffected code-wise (they call `sync_amazon_fba()` without this new
   parameter, so they get the default `active_only=True` filtering
   automatically — this is the desired behavior, no scheduler changes
   needed).

### Self-healing behavior

`isParticipating` is fetched fresh from Amazon on every single sync call
(scheduled or manual) — there is no caching of the eligibility list itself
across syncs beyond the persisted `amazon_marketplaces` table (which is
informational only). If the seller enrolls in a new marketplace on
Amazon's side, the very next sync call (scheduled or manual) will see
`isParticipating: true` for it and automatically include it in that same
call's orders/inventory queries — no manual intervention or code change
required.

### Non-goals

- No per-marketplace backoff tracking (out of scope; the existing
  per-task backoff plus this filtering reduction is expected to be
  sufficient given the account currently has one real marketplace).
- No change to `_primary_inbound_marketplace()` selection logic for
  inbound shipments/modern finance transactions — those already operate
  account-level / single-marketplace and are unaffected by this filter.

## Testing

- `_upsert_marketplaces()`: given a fixture with one `isParticipating:
  true` and one `isParticipating: false` marketplace, `active_only=True`
  returns only the participating ID; `active_only=False` returns both.
  Both marketplaces are persisted to `amazon_marketplaces` in either case.
- `sync_amazon_fba()`: with `include_all_marketplaces` omitted (default),
  a non-participating marketplace present in the fixture is never passed
  to `client.orders()`/inventory sync. With `include_all_marketplaces=True`,
  it is included.
- `AmazonSyncRequest` accepts and defaults `include_all_marketplaces`
  correctly via the existing router test conventions.

## Verification

- Full backend test suite passes.
- Manual full sync on production, without `include_all_marketplaces`,
  confirms via `rate_limits`/`marketplaces` count in the response that
  only the real marketplace(s) were queried.
