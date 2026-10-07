"""Read-only native-currency fee checks, never a source of tax or bookings.

Invoice activity and payment release are different clocks. Modern lifecycle
records are selected once; settlement evidence is never added to that stream.
Unprovable coverage is disclosed rather than guessed or made a filing gate.
"""
from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP

from app import config
from app.db import connect_combined_db
from app.services.ust_reconciliation import _read
from app.services.invoice_parser import InvoiceParseError

FEE_TYPES = {
    'Commission': 'selling_fees', 'RefundCommission': 'selling_fees',
    'FBAPerUnitFulfillmentFee': 'fulfillment', 'ShippingChargeback': 'shipping_chargeback',
    'FBARemovalFee': 'inventory_removal', 'FBADisposalFee': 'inventory_removal',
    'FBAStorageFee': 'fulfillment', 'StorageBillingFee': 'fulfillment',
    'FBAInboundTransportationFee': 'fulfillment', 'FBAInboundTransportationProgramFee': 'fulfillment',
    'InboundTransportationFee': 'fulfillment', 'Subscription': 'subscription',
    'SubscriptionFee': 'subscription', 'AdvertisingFee': 'advertising',
}
POSITION_TYPES = {'provision': 'selling_fees', 'provision_storno': 'selling_fees',
                  'refund_admin': 'selling_fees', 'fee_refund': 'selling_fees',
                  'fulfillment': 'fulfillment', 'shipping_chargeback': 'shipping_chargeback',
                  'inventory_removal': 'inventory_removal', 'subscription': 'subscription',
                  'advertising': 'advertising'}
IGNORED = {'Base', 'Tax', 'Promo', 'ProductCharges', 'OurPricePrincipal', 'OurPriceTax',
           'Shipping', 'ShippingPrincipal', 'ShippingTax', 'PromoRebates',
           'ShippingDiscount', 'ShippingTaxDiscount', 'Giftwrap', 'GiftwrapTax'}
CONTAINERS = {'AmazonFees', 'FBAFees', 'Expenses', 'Refunded Expenses'}
RANK = {'RELEASED': 3, 'DEFERRED_RELEASED': 2, 'DEFERRED': 1}


def _cents(money):
    return int((Decimal(str((money or {}).get('currencyAmount') or 0)) * 100).quantize(Decimal('1'), rounding=ROUND_HALF_UP))


def _components(nodes):
    for node in nodes or []:
        name = node.get('breakdownType', '')
        children = node.get('breakdowns') or []
        if name in IGNORED:
            continue
        if name in CONTAINERS and not children:
            value = -_cents(node.get('breakdownAmount'))
            if value:
                yield name, value
            continue
        if name in CONTAINERS or (children and not any(c.get('breakdownType') in {'Base', 'Tax', 'Promo'} for c in children)):
            yield from _components(children)
        else:
            value = -_cents(node.get('breakdownAmount'))
            if value:
                yield name, value


def _invoices():
    """Prefer the VAT ledger for duplicate invoice identities, as the report does."""
    identities = {}
    with connect_combined_db() as c:
        for raw in c.execute("SELECT * FROM input_vat_invoices WHERE provider='amazon' AND doc_type='fee' AND input_vat_status='confirmed'"):
            row = dict(raw)
            identities[row['invoice_number']] = row
    if config.BOOKKEEPING_DB_PATH.exists():
        with _read(config.BOOKKEEPING_DB_PATH) as c:
            tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if 'monthly_invoices' in tables:
                for raw in c.execute("SELECT * FROM monthly_invoices WHERE provider IN ('amazon','amazon_fba') AND status='approved'"):
                    row = dict(raw)
                    if row.get('doc_kind') == 'sales':
                        continue
                    row.update(gross_cents=row.get('invoice_amount_cents'), lines=json.loads(row.get('lines_json') or '[]'))
                    identities.setdefault(row.get('invoice_number') or row['id'], row)
    return list(identities.values())


def reconcile_amazon_fees(month):
    result = {'status': 'matched', 'comparisons': [], 'details': [], 'timing': [],
              'issues': [], 'excluded_ads_cents': 0, 'source': 'stored_finance',
              'tax_amounts_changed': False}
    api = defaultdict(int)
    invoice_sums = defaultdict(int)
    positive_api = defaultdict(int)
    positive_invoice = defaultdict(int)
    issues = result['issues']
    try:
        invoices = _invoices()
        for row in invoices:
            if str(row.get('service_date') or row.get('period_to') or row.get('period_from') or row.get('invoice_date'))[:7] != month:
                continue
            currency = row.get('currency') or 'EUR'
            lines = row.get('lines') or []
            if not lines and row.get('document_path'):
                from app.services.invoice_parser import parse_invoice_file
                lines = parse_invoice_file(row['document_path']).to_dict()['lines']
            if not lines:
                issues.append({'code': 'INVOICE_DETAILS_MISSING', 'invoice_number': row.get('invoice_number')})
                continue
            category_sums = defaultdict(int)
            for line in lines:
                category = POSITION_TYPES.get(line.get('position_key'))
                if not category:
                    issues.append({'code': 'UNKNOWN_INVOICE_FEE', 'invoice_number': row.get('invoice_number'), 'label': line.get('label')})
                    continue
                if category == 'advertising':
                    continue
                value = int(line.get('gross_cents') or 0)
                category_sums[category] += value
                reference = str(line.get('order_ref') or '')
                if reference not in {'', '-', 'None'}:
                    # Positive components are checked independently from credits.
                    key = (currency, reference, line.get('position_key'), 'charge' if value > 0 else 'credit')
                    if line.get('position_key') != 'refund_admin':
                        positive_invoice[key] += value
            for category, value in category_sums.items():
                invoice_sums[(currency, category)] += value
            line_total = sum(category_sums.values())
            gross = int(row.get('gross_cents') or 0)
            if line_total != gross:
                issues.append({'code': 'INVOICE_POSITION_TOTAL_DIFFERENCE', 'invoice_number': row.get('invoice_number'), 'difference_cents': gross-line_total})

        if not config.AMAZON_FBA_DB_PATH.exists():
            issues.append({'code': 'FINANCE_SOURCE_MISSING'})
            events = []
        else:
            with _read(config.AMAZON_FBA_DB_PATH) as c:
                events = [dict(r) for r in c.execute('SELECT * FROM amazon_financial_events')]
        groups = defaultdict(list)
        settlement_subscriptions = {}
        modern_in_month = any(e['event_type'].startswith('ModernTransaction:') and str(e.get('posted_date') or '')[:7] == month for e in events)
        modern_orders = {(e.get('amazon_order_id'), e['currency']) for e in events if e['event_type'].startswith('ModernTransaction:') and str(e.get('posted_date') or '')[:7] == month}
        for event in events:
            raw = json.loads(event.get('raw_json') or '{}')
            if event['event_type'].startswith('ModernTransaction:'):
                identifiers = {r['relatedIdentifierName']: r['relatedIdentifierValue'] for r in raw.get('relatedIdentifiers') or []}
                lifecycle = identifiers.get('DEFERRED_TRANSACTION_ID') or event.get('lifecycle_id') or event.get('transaction_id') or event['id']
                groups[(event['currency'], event['event_type'], lifecycle)].append((event, raw))
            elif event['event_type'] == 'SettlementReportLine' and raw.get('transaction-type') == 'Subscription Fee':
                key = (event['currency'], event.get('settlement_id'), event.get('posted_date'), raw.get('other-amount'))
                settlement_subscriptions.setdefault(key, event)
            elif event['event_type'] == 'SettlementReportLine' and str(event.get('posted_date') or '')[:7] == month and event.get('fees_cents') and not modern_in_month:
                issues.append({'code': 'LEGACY_FINANCE_COVERAGE', 'event_id': event['id']})
            elif str(event.get('posted_date') or '')[:7] == month and event.get('fees_cents') and event['event_type'] != 'SettlementReportLine':
                if (event.get('amazon_order_id'), event['currency']) not in modern_orders and not event['event_type'].startswith('ModernTransaction:'):
                    issues.append({'code': 'LEGACY_FINANCE_COVERAGE', 'event_id': event['id']})

        modern_subscription_currencies = set()
        for (currency, kind, lifecycle), members in groups.items():
            selected, raw = max(members, key=lambda p: (RANK.get(p[1].get('transactionStatus'), 0), str(p[0].get('posted_date') or ''), p[0]['id']))
            original_dates = [str(e.get('posted_date') or '') for e, r in members if r.get('transactionStatus') in {'DEFERRED', 'DEFERRED_RELEASED'} or (e.get('transaction_id') or e['id']) == lifecycle]
            activity = min(original_dates) if original_dates else str(selected.get('posted_date') or '')
            if not activity:
                issues.append({'code': 'FINANCE_DATE_MISSING', 'event_id': selected['id']})
                continue
            if activity[:7] != month:
                continue
            if kind in {'ModernTransaction:Transfer', 'ModernTransaction:DebtRecovery'}:
                continue  # movements of money, not charges for supplier services
            if not original_dates:
                issues.append({'code': 'ORIGINAL_LIFECYCLE_MISSING', 'event_id': selected['id']})
            known_payloads = {json.dumps(list(_components([n for item in r.get('items') or [] for n in item.get('breakdowns') or []])), sort_keys=True) for e, r in members}
            if len(known_payloads) > 1:
                issues.append({'code': 'LIFECYCLE_COMPONENTS_CHANGED', 'event_id': selected['id']})
            if selected.get('posted_date', '')[:7] != activity[:7]:
                result['timing'].append({'event_id': selected['id'], 'order_id': selected.get('amazon_order_id'),
                    'activity_month': activity[:7], 'release_month': str(selected['posted_date'])[:7], 'currency': currency,
                    'fees_cents': int(selected.get('fees_cents') or 0)})
            nodes = [node for item in raw.get('items') or [] for node in item.get('breakdowns') or []]
            if not nodes or (kind == 'ModernTransaction:ProductAdsPayment' and raw.get('breakdowns')):
                nodes = raw.get('breakdowns') or []
            for name, value in _components(nodes):
                category = FEE_TYPES.get(name)
                if not category:
                    issues.append({'code': 'UNKNOWN_FINANCE_FEE', 'event_id': selected['id'], 'label': name, 'amount_cents': value, 'currency': currency})
                    continue
                if category == 'advertising' or kind == 'ModernTransaction:ProductAdsPayment':
                    if currency == 'EUR':
                        result['excluded_ads_cents'] += value
                    continue
                if category == 'subscription':
                    modern_subscription_currencies.add(currency)
                api[(currency, category)] += value
                order = selected.get('amazon_order_id')
                if kind in {'ModernTransaction:Shipment', 'ModernTransaction:Refund'} and order:
                    position = {'Commission': 'provision', 'FBAPerUnitFulfillmentFee': 'fulfillment', 'ShippingChargeback': 'shipping_chargeback'}.get(name)
                    if position:
                        positive_api[(currency, order, position, 'charge' if value > 0 else 'credit')] += value

        # GBP subscription is absent from modern DE-marketplace data in the
        # audited source. Use the uniquely identified settlement evidence only.
        for event in settlement_subscriptions.values():
            currency = event['currency']
            if str(event.get('posted_date') or '')[:7] == month and currency not in modern_subscription_currencies:
                api[(currency, 'subscription')] += int(event.get('fees_cents') or 0)
        if not events:
            issues.append({'code': 'FINANCE_COVERAGE_EMPTY'})
        for key in sorted(set(invoice_sums) | set(api)):
            result['comparisons'].append({'currency': key[0], 'category': key[1], 'invoice_cents': invoice_sums[key],
                'finance_cents': api[key], 'difference_cents': invoice_sums[key]-api[key]})
        # Only compare order-level credits if the original provides that
        # allocation; aggregate credit PDFs cannot establish an order split.
        credit_kinds = {key[2] for key in positive_invoice if key[3] == 'credit'}
        detail_keys = {key for key in set(positive_invoice) | set(positive_api) if key[3] == 'charge' or key[2] in credit_kinds}
        for key in sorted(detail_keys):
            delta = positive_invoice[key]-positive_api[key]
            if delta:
                result['details'].append({'currency': key[0], 'order_id': key[1], 'category': key[2],
                    'invoice_cents': positive_invoice[key], 'finance_cents': positive_api[key], 'difference_cents': delta})
    except (sqlite3.Error, ValueError, TypeError, OSError, InvoiceParseError) as exc:
        issues.append({'code': 'FINANCE_CHECK_FAILED', 'message': str(exc)})
    result['status'] = ('incomplete' if issues else 'differences' if result['details'] or any(r['difference_cents'] for r in result['comparisons'])
                        else 'explained' if result['timing'] else 'matched')
    return result
