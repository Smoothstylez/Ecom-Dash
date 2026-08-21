# Amazon Financial Lifecycle and Fee Accounting Design

## Goal

Correctly model Amazon Deferred/Released transaction lifecycles without
double-counting sales or fees, preserve Amazon's fee VAT breakdowns, and show
separate operational profitability and released cash/accounting views.

## Raw Data

- Persist every Amazon Finance payload unchanged in `raw_json`.
- Persist the source `transactionId`, `transactionStatus`,
  `DEFERRED_TRANSACTION_ID`, `RELEASE_TRANSACTION_ID`, `deferralReason`, and
  `maturityDate` as normalized fields.
- Never delete historical Deferred, Deferred Released, or Released source
  records. They remain available for audit and debugging.

## Lifecycle Identity

- Every Finance event receives `lifecycle_id`:

```python
lifecycle_id = deferred_transaction_id or transaction_id
```

- A normal transaction with no deferred relationship therefore uses its own
  `transactionId`.
- A `RELEASED` event linked with `DEFERRED_TRANSACTION_ID` and its original
  deferred event share the original deferred transaction ID as lifecycle ID.
- Multiple raw records for one lifecycle form exactly one economic sale. They
  must never be summed together for sales, fees, VAT, or cash.

## Canonical Lifecycle Representative

- For a lifecycle with a `RELEASED` record, use that record as the canonical
  released financial representation.
- Otherwise prefer `DEFERRED_RELEASED`, then `DEFERRED`, then the most recent
  remaining record by posted timestamp.
- `DEFERRED_RELEASED` counts as released for the dashboard's released view,
  but does not coexist with a paired `RELEASED` record in aggregates.

## Accounting Views

### Operational Margin

- Includes exactly one representative for every sale lifecycle, including
  `DEFERRED`.
- Labels Deferred amounts as provisional and displays deferral reason plus the
  Amazon-provided planned release `maturityDate`.
- Calculates profitability as:

```text
net sales - Amazon fee net - FIFO purchase cost net
```

### Released Margin and Amazon Cash

- Includes only canonical lifecycles in `RELEASED` or `DEFERRED_RELEASED`
  state.
- Released means available to a settlement/outgoing payout; it is not asserted
  to have arrived in the bank account.
- Cash reporting is separate from margin:

```text
gross sales - Amazon fee gross - other Amazon movements
```

## Fee Breakdown

- Parse nested Amazon fee breakdowns and retain each fee's exact gross, base,
  and tax values. Do not infer net fee by dividing gross by 1.19.
- Aggregate and expose `amazon_fee_gross_cents`, `amazon_fee_net_cents`, and
  `amazon_fee_vat_cents` per lifecycle, order/SKU allocation, and dashboard
  total.
- If Amazon omits Base/Tax for an individual fee, retain its gross value and
  mark fee net/VAT unavailable rather than inventing a tax split.

## FIFO Cost Rule

- FIFO cost must be stored as supplier purchase cost net when deductible input
  VAT applies.
- A price expressed as gross must not silently become a net FIFO cost. The
  invoice workflow retains gross/net/VAT so confirmed lots use the supplied
  net line amount.
- For imports or non-deductible VAT, the operator supplies the economically
  correct net/COGS amount explicitly; the system does not make a country or
  tax-treatment assumption.

## Verification

- Regression fixture with a paired `DEFERRED_RELEASED` and `RELEASED` event
  asserts one lifecycle, one sale, and one fee set.
- Normal unpaired `RELEASED` transaction uses its own transaction ID as
  lifecycle ID.
- Operational aggregate includes Deferred; released aggregate excludes it.
- Carlinkit fixture verifies Commission gross 6.77 EUR, net 5.69 EUR, VAT
  1.08 EUR and FBA fee gross 2.81 EUR, net 2.36 EUR, VAT 0.45 EUR.
- Dashboard exposes DD7/B2B deferral reason and Amazon maturity date without
  estimating a release date.
