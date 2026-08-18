# Amazon Order Detail and Cost Chain Design

## Goal

Make Amazon order details show all available product and finance data, then connect confirmed FBA shipment costs to SKU inventory lots and FIFO cost of goods without inventing restricted or missing data.

## Scope

### Order Detail

- Render Amazon Catalog Items `image_url` and stored image variants.
- Render Amazon order line items with title, ASIN, seller SKU, quantity, price, and tax.
- Render modern Finance API breakdowns by fee type, including commission and FBA fulfillment fees.
- Render finance finality, maturity date, order ID, Amazon outbound shipment ID, and settlement ID.
- Render only address fields Amazon actually supplies; label restricted PII explicitly.
- Keep missing billing address explicit rather than synthesizing one.

### FBA Cost Chain

- Keep inbound FBA shipment costs separate from product purchase costs.
- Attach supplier invoices to an inbound shipment.
- Support SKU/FNSKU-level invoice lines and explicit allocation.
- Require received inventory and confirmed, unambiguous costs before creating FIFO lots.
- Refuse automatic multi-SKU allocation when invoice lines are absent.
- Feed confirmed lots into existing FIFO allocation and order profit calculations.

## Data Rules

- Amazon Finance transaction amounts are authoritative for Amazon sales and Amazon fees.
- Amazon transport quotes are not final costs until a released Finance transaction exists.
- Supplier invoice net value is the product cost; invoice VAT remains separate and reviewable.
- FBA inbound transportation fees are shipment logistics costs, not product purchase costs.
- No customer name, email, street, phone, or billing address is fabricated when Amazon PII access is unavailable.
- FIFO lots are created only for shipments in `RECEIVING`, `CLOSED`, or `DELIVERED` state.
- A multi-SKU shipment requires explicit per-SKU allocation before FIFO lot creation.

## Data Flow

1. Amazon Orders and Catalog APIs populate order items, available address fields, and product image URLs.
2. Amazon Finance transactions are stored with raw payloads and normalized event breakdowns.
3. An inbound FBA shipment stores shipment items, supplier invoice header, invoice lines, and confirmed allocated product cost.
4. A confirmation action creates one inventory lot per SKU/FNSKU with received quantity and unit cost.
5. Existing FIFO allocation consumes lots for shipped Amazon orders and projects COGS and profit.

## Error Handling

- Catalog image failures leave the order usable and retain the ASIN/SKU.
- Restricted Amazon PII responses are shown as an explicit limitation, not an API error.
- Invoice uploads without valid shipment or required supplier data are rejected.
- Ambiguous cost allocation blocks FIFO confirmation and identifies the missing line allocations.
- Repeated syncs remain idempotent by Amazon transaction, shipment, invoice, and SKU keys.
- A real Orders API payload always takes precedence over synthetic finance-only order rows.
- For a given order, modern Finance transactions take precedence over settlement-report rows; settlement-report rows take precedence over legacy event rows.
- Inbound finance costs are attributed per transaction item and shipment; ambiguous multi-shipment items remain unassigned.
- FIFO stores exact line totals, including remainder cents that cannot be represented by an integer unit cost.
- Confirmed invoice lines are immutable while dependent allocations and lots exist.

## Verification

- Backend tests cover image projection, finance breakdown normalization, address limitations, invoice line allocation, received-state gating, and FIFO lot creation.
- Frontend typecheck and production build must pass.
- Live target order `028-0075286-8561126` must show its product image, `26.59 EUR` commission, `6.41 EUR` FBA fulfillment fee, deferred status, and current partial address.
