"""Finance checks never change report tax or demand manual approval."""
import json
import sqlite3
import pytest
from app import config
from app.services import ust_report as report, ust_documents
from test_ust_report_reliability import isolated, import_rows, tax_row
from test_platform_invoices import DDL


def reconcile(month='2026-09'):
    from app.services.finance_reconciliation import reconcile_amazon_fees
    return reconcile_amazon_fees(month)


def event(id='d1', date='2026-09-28', status='DEFERRED', lifecycle='d1', amount=119, order='O1', fee='Commission', currency='EUR', kind='Shipment'):
    raw={'transactionId':id,'transactionStatus':status,'transactionType':kind,
         'relatedIdentifiers':[{'relatedIdentifierName':'DEFERRED_TRANSACTION_ID','relatedIdentifierValue':lifecycle}] if lifecycle!=id else [],
         'items':[{'breakdowns':[{'breakdownType':'AmazonFees','breakdownAmount':{'currencyAmount':-amount/100},'breakdowns':[
             {'breakdownType':fee,'breakdownAmount':{'currencyAmount':-amount/100},'breakdowns':[
                 {'breakdownType':'Base','breakdownAmount':{'currencyAmount':-amount/119}},
                 {'breakdownType':'Tax','breakdownAmount':{'currencyAmount':-amount/100+amount/119}}]}]}]}]}
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        c.execute('INSERT INTO amazon_financial_events(id,event_type,amazon_order_id,posted_date,currency,transaction_id,lifecycle_id,fees_cents,raw_json) VALUES (?,?,?,?,?,?,?,?,?)',
                  (id,'ModernTransaction:'+kind,order,date,currency,id,lifecycle,amount,json.dumps(raw)))


def invoice(number='TEST-I1', amount=119, order='O1', category='provision', currency='EUR'):
    with sqlite3.connect(config.BOOKKEEPING_DB_PATH) as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name='monthly_invoices'").fetchone():c.executescript(DDL)
        c.execute("INSERT INTO monthly_invoices(id,provider,invoice_number,status,period_from,period_to,invoice_date,invoice_amount_cents,currency,doc_category,lines_json) VALUES (?, 'amazon',?,'approved','2026-09-01','2026-09-30','2026-09-30',?,?,'fee',?)",
                  (number,number,amount,currency,json.dumps([{'position_key':category,'label':'Referral Fee','gross_cents':amount,'order_ref':order}])))


def test_lifecycle_month_shift_is_explained_once(isolated):
    event();event('r1','2026-10-05','RELEASED');invoice()
    r=reconcile()
    assert r['status']=='explained'
    assert r['comparisons'][0]['finance_cents']==119
    assert r['comparisons'][0]['difference_cents']==0
    assert r['timing'][0]['release_month']=='2026-10'


def test_no_order_settlement_does_not_double_modern_service(isolated):
    event(order=None,fee='FBARemovalFee',kind='ServiceFee');invoice(order=None,category='inventory_removal')
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        c.execute("INSERT INTO amazon_financial_events(id,event_type,posted_date,fees_cents,raw_json) VALUES ('s','SettlementReportLine','2026-09-28',119,'{}')")
    assert reconcile()['comparisons'][0]['finance_cents']==119


def test_equal_real_fees_survive_and_opposite_differences_are_visible(isolated):
    event(order='O1');event(id='d2',lifecycle='d2',order='O2');invoice(amount=109);invoice('TEST-I2',129,'O2')
    r=reconcile()
    assert r['comparisons'][0]['difference_cents']==0
    assert r['status']=='differences'
    assert sorted(x['difference_cents'] for x in r['details'])==[-10,10]


def test_ads_excluded_and_currency_never_mixed(isolated):
    event();invoice();event('ad',lifecycle='ad',order=None,kind='ProductAdsPayment',fee='AdvertisingFee',amount=500)
    event('gb',lifecycle='gb',order=None,kind='ServiceFee',fee='Subscription',amount=2975,currency='GBP')
    invoice('TEST-GBP',2975,None,'subscription','GBP')
    r=reconcile()
    assert {x['currency']:x['difference_cents'] for x in r['comparisons']}=={'EUR':0,'GBP':0}
    assert r['excluded_ads_cents']==500


def test_unknown_fee_and_missing_lifecycle_are_incomplete_not_matched(isolated):
    event('r1',status='RELEASED',lifecycle='missing',fee='FutureUnknownFee');invoice()
    r=reconcile()
    assert r['status']=='incomplete'
    assert r['issues']


def test_missing_api_does_not_claim_matched(isolated):
    invoice();config.AMAZON_FBA_DB_PATH.unlink()
    assert reconcile()['status']=='incomplete'


def test_refund_components_and_multiple_partials_are_signed_once(isolated):
    event(amount=238);invoice(amount=238)
    event('ref1',lifecycle='ref1',amount=-60,kind='Refund');event('ref2',lifecycle='ref2',amount=-59,kind='Refund')
    invoice('TEST-CREDIT',-119,None,'fee_refund')
    assert reconcile()['comparisons'][0]['finance_cents']==119
    assert reconcile()['comparisons'][0]['difference_cents']==0


def test_finance_difference_is_warning_even_with_strict_input_option(isolated):
    import_rows(tax_row(**{'Shipment Date':'2026-09-28'}));event(amount=238);invoice()
    before=report.build_ust_report('2026-09')['totals']
    r=report.build_ust_report('2026-09',block_filing_when_input_vat_incomplete=True)
    assert r['totals']==before
    assert not any(b['code'] in {'FEE_RECONCILIATION_DIFFERENCE','AMAZON_TAX_DATA_INCOMPLETE'} for b in r['blockers'])
    assert r['sections']['finance_reconciliation']['status']=='differences'


def test_transfers_are_not_unknown_fees_and_ads_root_is_excluded(isolated):
    event();invoice()
    event('transfer',lifecycle='transfer',kind='Transfer',fee='FundTransfer',amount=10000,order=None)
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        raw={'transactionStatus':'RELEASED','items':[{'breakdowns':[{'breakdownType':'Base','breakdownAmount':{'currencyAmount':-2.1}},{'breakdownType':'Tax','breakdownAmount':{'currencyAmount':-0.4}}]}],
             'breakdowns':[{'breakdownType':'Expenses','breakdowns':[{'breakdownType':'AdvertisingFee','breakdownAmount':{'currencyAmount':-2.5}}]}]}
        c.execute("INSERT INTO amazon_financial_events(id,event_type,posted_date,transaction_id,lifecycle_id,fees_cents,raw_json) VALUES ('ad','ModernTransaction:ProductAdsPayment','2026-09-15','ad','ad',250,?)",(json.dumps(raw),))
    r=reconcile()
    assert r['status']=='matched'
    assert r['excluded_ads_cents']==250


def test_multiple_shipments_same_order_do_not_collapse_equal_fees(isolated):
    event();event('d2',lifecycle='d2');invoice(amount=238)
    first=reconcile()
    assert first['status']=='matched'
    assert first['comparisons'][0]['finance_cents']==238
    assert reconcile()==first


def test_api_failure_preserves_tax_totals_and_allows_filing(isolated):
    import_rows(tax_row())
    before=report.build_ust_report('2026-07')['totals']
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        c.execute('DROP TABLE amazon_financial_events')
    after=report.build_ust_report('2026-07')
    assert after['totals']==before
    assert after['status']=='ready'
    assert after['sections']['finance_reconciliation']['status']=='incomplete'
    report.file_report('2026-07')


def test_tax_report_excess_is_disclosed_but_does_not_change_tax(isolated):
    import_rows(tax_row())
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        c.execute("INSERT INTO amazon_financial_events(id,event_type,amazon_order_id,posted_date,sales_cents,raw_json) VALUES ('s','ShipmentEventList','O1','2026-07-07',10000,'{}')")
    r=report.build_ust_report('2026-07')
    assert r['totals']['output_vat_cents']==1900
    assert r['sections']['amazon_reconciliation']['amount_mismatches'][0]['tax_gross_cents']==11900
    assert 'AMAZON_TAX_DATA_INCOMPLETE' in {w['code'] for w in r['warnings']}
    assert r['status']=='ready'


def test_opposing_order_credit_errors_do_not_disappear_in_total(isolated):
    event('rf1',lifecycle='rf1',kind='Refund',amount=-119,order='O1')
    event('rf2',lifecycle='rf2',kind='Refund',amount=-119,order='O2')
    invoice('TEST-C1',-109,'O1','provision');invoice('TEST-C2',-129,'O2','provision')
    r=reconcile()
    assert r['comparisons'][0]['difference_cents']==0
    assert r['status']=='differences'
    assert sorted(x['difference_cents'] for x in r['details'])==[-10,10]


def test_unknown_container_amount_without_children_is_visible(isolated):
    event();invoice()
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        raw={'transactionStatus':'RELEASED','breakdowns':[{'breakdownType':'Expenses','breakdownAmount':{'currencyAmount':-5}}]}
        c.execute("INSERT INTO amazon_financial_events(id,event_type,posted_date,transaction_id,lifecycle_id,raw_json) VALUES ('unknown','ModernTransaction:ServiceFee','2026-09-15','unknown','unknown',?)",(json.dumps(raw),))
    assert reconcile()['status']=='incomplete'


def test_old_only_fees_are_incomplete_even_if_other_month_has_modern_data(isolated):
    event(date='2026-10-05');invoice()
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        c.execute("INSERT INTO amazon_financial_events(id,event_type,posted_date,fees_cents,raw_json) VALUES ('legacy','ShipmentEventList','2026-09-28',119,'{}')")
    assert reconcile()['status']=='incomplete'


def test_settlement_only_fee_cannot_be_silently_marked_matched(isolated):
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        c.execute("INSERT INTO amazon_financial_events(id,event_type,posted_date,fees_cents,raw_json) VALUES ('settle','SettlementReportLine','2026-09-28',119,'{}')")
    assert reconcile()['status']=='incomplete'


def test_duplicate_invoice_ledgers_count_original_only_once(isolated):
    event();invoice()
    ust_documents.save_input_vat_invoice({'provider':'amazon','doc_type':'fee','invoice_number':'TEST-I1',
        'invoice_date':'2026-09-30','period_to':'2026-09-30','gross_cents':119,'net_cents':100,
        'vat_cents':19,'deductible_vat_cents':19,'input_vat_status':'confirmed'})
    # A manual confirmed ledger entry without original line evidence must be
    # reported as uncheckable, never silently added to its bookkeeping duplicate.
    r=reconcile()
    assert r['status']=='incomplete'
    assert sum(x['invoice_cents'] for x in r['comparisons'])==0
