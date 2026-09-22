"""Read-only economic projection shared by Amazon orders and SKU analytics.

Source payloads remain unchanged; signed refunds are also repaired for historical
records without requiring a new Amazon sync. Unknown tax splits remain explicit.
"""
from collections import defaultdict
import json

from app.services.importers import amazon_sp_api as db


def events(connection):
    from app.services.amazon_fba import _canonical_financial_event_predicate
    rows = connection.execute(f'SELECT e.* FROM amazon_financial_events e WHERE {_canonical_financial_event_predicate("e")}').fetchall()
    result = []
    for row in rows:
        event = dict(row)
        raw = json.loads(event['raw_json'] or '{}')
        if event['event_type'].startswith('ModernTransaction:') and 'breakdowns' in raw:
            breakdown = db.extract_modern_financial_breakdown(raw)
            event['sales_cents'] = breakdown['sales_cents']
            event['fees_cents'] = sum(f['amount_cents'] for f in breakdown['fees'])
            event['sales_vat_cents'] = breakdown['tax_cents']
            event['fees_vat_cents'] = sum(f.get('vat_cents', 0) for f in breakdown['fees'])
            event['fees_net_cents'] = sum(f.get('net_cents', f['amount_cents']) for f in breakdown['fees'])
            event['fee_tax_complete'] = all('vat_cents' in f for f in breakdown['fees'])
            def tax_present(nodes):
                return any(str(n.get('breakdownType', '')).lower() == 'tax' or tax_present(n.get('breakdowns') or []) for n in nodes)
            event['sales_tax_complete'] = tax_present(raw.get('breakdowns') or [])
            event['financial_breakdown'] = breakdown
        else:
            event['sales_vat_cents'] = None
            event['fees_vat_cents'] = None
            event['fees_net_cents'] = event['fees_cents']
            event['fee_tax_complete'] = False
            event['sales_tax_complete'] = False
        event['sales_net_cents'] = event['sales_cents'] - (event['sales_vat_cents'] or 0)
        event['unclassified_cents'] = event['net_cents'] - (event['sales_cents'] - event['fees_cents'])
        result.append(event)
    return result


def order_totals(connection):
    grouped = defaultdict(list)
    for event in events(connection):
        if event['amazon_order_id']:
            grouped[event['amazon_order_id']].append(event)
    output = {}
    for order_id, group in grouped.items():
        total = {key: sum(e[key] or 0 for e in group) for key in ('sales_cents','fees_cents','fees_net_cents','fees_vat_cents','unclassified_cents')}
        currencies = {e['currency'] for e in group}
        tax_known = all(e['sales_tax_complete'] for e in group if e['sales_cents'])
        tax = sum(e['sales_vat_cents'] or 0 for e in group)
        total.update(sales_vat_cents=tax if tax_known else None, sales_tax_complete=tax_known,
                     fee_tax_complete=all(e['fee_tax_complete'] for e in group if e['fees_cents']),
                     currency_complete=len(currencies) == 1, financial_event_count=len(group))
        output[order_id] = total
    return output


def apply_summary(summary, totals, *, shipped=0, allocated=0):
    if totals:
        summary['sales_gross_cents'] = summary['total_cents'] = totals['sales_cents']
        # Never subtract unreversed original tax from a refunded sale. Legacy-only
        # sales may use original item VAT; refunds with absent tax remain unknown.
        tax = totals['sales_vat_cents']
        if tax is None:
            tax = summary['sales_vat_cents'] if totals['sales_cents'] > 0 and not totals['unclassified_cents'] else 0
        summary['sales_vat_cents'] = tax
        summary['sales_net_cents'] = totals['sales_cents'] - tax
        summary['fees_cents'] = totals['fees_cents']
        summary['fees_net_cents'] = totals['fees_net_cents']
        summary['fees_vat_cents'] = totals['fees_vat_cents']
        summary['sales_tax_complete'] = totals['sales_tax_complete']
        summary['fee_tax_complete'] = totals['fee_tax_complete']
        summary['finance_complete'] = totals['currency_complete'] and totals['unclassified_cents'] == 0
    else:
        summary.update(fees_net_cents=summary['fees_cents'], sales_tax_complete=False, fee_tax_complete=False, finance_complete=False)
    summary['costs_complete'] = shipped <= allocated
    summary['after_fees_cents'] = summary['sales_gross_cents'] - summary['fees_cents']
    summary['profit_cents'] = summary['sales_net_cents'] - summary['fees_net_cents'] - summary['purchase_cost_cents']
    summary['margin_complete'] = summary['costs_complete'] and summary['sales_tax_complete'] and summary['fee_tax_complete'] and summary['finance_complete']
    return summary


def listing_metrics():
    from app.services.amazon_fba import load_amazon_order_summaries
    from app.services.amazon_procurement import split_cents
    summaries = {s['order_id']: s for s in load_amazon_order_summaries()}
    with db._connect() as c:
        items = c.execute('''SELECT i.*,o.marketplace_id,
            COALESCE((SELECT SUM(allocated_cost_cents) FROM fifo_allocations a WHERE a.amazon_order_item_id=i.id),0) cogs,
            COALESCE((SELECT SUM(quantity) FROM fifo_allocations a WHERE a.amazon_order_item_id=i.id),0) allocated
            FROM amazon_order_items i JOIN amazon_orders o USING(amazon_order_id) ORDER BY i.id''').fetchall()
    by_order = defaultdict(list)
    for item in items:
        by_order[item['amazon_order_id']].append(item)
    metrics = {}
    for order_id, order_items in by_order.items():
        s = summaries[order_id]
        weights = [max(i['item_price_cents'],0) for i in order_items]
        if not sum(weights):
            weights = [max(i['quantity_shipped'],1) for i in order_items]
        distributed = {}
        for field in ('sales_gross_cents','sales_net_cents','sales_vat_cents','fees_cents','fees_net_cents'):
            value = s[field]
            distributed[field] = [v * (-1 if value < 0 else 1) for v in split_cents(abs(value),weights)]
        for index, item in enumerate(order_items):
            key = (item['marketplace_id'], item['seller_sku'] or item['asin'])
            entry = metrics.setdefault(key, dict(marketplace_id=key[0],seller_sku=key[1],asin=item['asin'],title=item['title'],
                quantity_sold=0,cogs_cents=0,sales_gross_cents=0,sales_net_cents=0,sales_vat_cents=0,fees_cents=0,fees_net_cents=0,
                costs_complete=True,margin_complete=True,allocation_estimated=False))
            for field, amounts in distributed.items():
                entry[field] += amounts[index]
            entry['quantity_sold'] += item['quantity_shipped']
            entry['cogs_cents'] += item['cogs']
            entry['costs_complete'] &= item['allocated'] >= item['quantity_shipped']
            entry['margin_complete'] &= s['margin_complete']
            entry['allocation_estimated'] |= len(order_items) > 1
    for entry in metrics.values():
        entry['margin_cents'] = entry['sales_net_cents'] - entry['fees_net_cents'] - entry['cogs_cents']
    return list(metrics.values())


def shipment_metrics():
    """Economic attribution, not physical Amazon unit provenance."""
    from app.services.amazon_fba import load_amazon_order_summaries
    from app.services.amazon_procurement import split_cents
    summaries = {s['order_id']: s for s in load_amazon_order_summaries()}
    result = {}
    with db._connect() as c:
        for order_id, summary in summaries.items():
            items = c.execute('SELECT * FROM amazon_order_items WHERE amazon_order_id=? ORDER BY id',(order_id,)).fetchall()
            weights = [max(i['item_price_cents'],0) for i in items]
            if not sum(weights):
                weights = [max(i['quantity_shipped'],1) for i in items]
            if not items:
                continue
            amounts = {}
            for field in ('sales_net_cents','fees_net_cents'):
                value = summary[field]
                amounts[field] = [v*(-1 if value<0 else 1) for v in split_cents(abs(value),weights)]
            for index,item in enumerate(items):
                allocations = c.execute('''SELECT a.*,l.inbound_shipment_id FROM fifo_allocations a JOIN inventory_lots l ON l.id=a.inventory_lot_id
                    WHERE amazon_order_item_id=? ORDER BY a.id''',(item['id'],)).fetchall()
                if not allocations:
                    continue
                quantities = [a['quantity'] for a in allocations]
                quantities.append(max(0,item['quantity_shipped']-sum(quantities)))
                distributed = {field:[v*(-1 if values[index]<0 else 1) for v in split_cents(abs(values[index]),quantities)] for field,values in amounts.items()}
                for n,a in enumerate(allocations):
                    if not a['inbound_shipment_id']:
                        continue
                    key = (a['inbound_shipment_id'],summary['currency'])
                    row = result.setdefault(key,dict(shipment_id=key[0],currency=key[1],quantity_sold=0,sales_net_cents=0,fees_net_cents=0,cogs_cents=0,margin_complete=True))
                    for field in distributed:
                        row[field] += distributed[field][n]
                    row['cogs_cents'] += a['allocated_cost_cents']
                    row['quantity_sold'] += a['quantity']
                    row['margin_complete'] &= summary['margin_complete']
    for row in result.values():
        row['margin_cents'] = row['sales_net_cents']-row['fees_net_cents']-row['cogs_cents']
    return list(result.values())
