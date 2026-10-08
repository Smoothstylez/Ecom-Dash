"""Credit-note CSVs and originals must work on the first ordinary upload."""
import json
import sqlite3

from app import config, db
from app.services import platform_invoices as invoices
from test_invoice_parser import AMAZON_GUTSCHRIFT
from test_ust_upload_flow import client, upload, pdf_bytes, KNOWN_AMAZON


CREDIT_CSV = (
    'Transaction Date,Transaction ID,Order ID,Credit Note Number,Original Invoice Number,'
    'Marketplace,Fee ID,Total Fees (VAT-Inclusive)\n'
    '08/19/2026,T1,TEST-ORDER,TEST-CN-00000001,TEST-AEU-00000001,'
    'Amazon.de,Referral Fee,-98.07\n'
).encode()


def credit_pdf():
    return ('credit.pdf', pdf_bytes(AMAZON_GUTSCHRIFT), 'application/pdf')


def original_pdf():
    return ('original.pdf', pdf_bytes(KNOWN_AMAZON), 'application/pdf')


def add_order(tmp_path, monkeypatch, date='2026-08-01T12:00:00Z'):
    path = tmp_path / 'amazon.sqlite3'
    with sqlite3.connect(path) as c:
        c.execute('CREATE TABLE amazon_orders(amazon_order_id TEXT,purchase_date TEXT,is_synthetic INTEGER)')
        c.execute('INSERT INTO amazon_orders VALUES (?,?,0)', ('TEST-ORDER', date))
    monkeypatch.setattr(config, 'AMAZON_FBA_DB_PATH', path)


def test_credit_uploaded_before_original_in_same_batch_is_approved(client):
    results = upload(client, credit_pdf(), original_pdf())
    credit = next(r for r in results if r['invoice_number'] == 'TEST-CN-00000001')
    assert credit['status'] == 'approved'
    docs = client.get('/api/ust-report/documents?month=2026-08').json()['items']
    assert next(r for r in docs if r['id'] == credit['document_id'])['effective_deductible_vat_cents'] == -1566


def test_later_original_upload_completes_waiting_credit_without_reupload(client):
    credit = upload(client, credit_pdf())[0]
    assert credit['status'] == 'needs_review'
    results = upload(client, original_pdf())
    completed = next((r for r in results if r.get('invoice_number') == 'TEST-CN-00000001'), None)
    assert completed is not None and completed['status'] == 'approved'
    assert completed['document_id'] == credit['document_id']
    assert invoices.get_invoice(credit['document_id'].removeprefix('book:'))['status'] == 'approved'


def test_credit_csv_pairs_with_credit_number_and_preserves_pdf_tax(client, tmp_path, monkeypatch):
    add_order(tmp_path, monkeypatch)
    results = upload(client, ('credit.csv', CREDIT_CSV, 'text/csv'), credit_pdf(), original_pdf())
    csv_result = next(r for r in results if r['filename'] == 'credit.csv')
    assert csv_result['status'] == 'paired'
    assert csv_result['invoice_numbers'] == ['TEST-CN-00000001']
    assert csv_result['pairings'][0]['pdf_filename'] == 'credit.pdf'
    invoice = invoices.get_invoice(csv_result['document_id'].removeprefix('book:'))
    assert invoice['invoice_amount_cents'] == -9807 and invoice['vat_amount_cents'] == -1566
    assert invoice['lines'][0]['order_ref'] == 'TEST-ORDER'
    with sqlite3.connect(config.BOOKKEEPING_DB_PATH) as c:
        counts = tuple(c.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0] for t in ['documents', 'monthly_invoices', 'transactions'])
    assert upload(client, ('credit.csv', CREDIT_CSV, 'text/csv'))[0]['status'] == 'paired'
    with sqlite3.connect(config.BOOKKEEPING_DB_PATH) as c:
        assert tuple(c.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0] for t in ['documents', 'monthly_invoices', 'transactions']) == counts


def test_reupload_reclassifies_previously_unknown_credit_csv(client, tmp_path, monkeypatch):
    add_order(tmp_path, monkeypatch)
    result = upload(client, ('credit.csv', CREDIT_CSV, 'text/csv'))[0]
    with db.connect_combined_db() as c:
        c.execute("UPDATE ust_import_files SET kind='unknown',invoice_numbers_json='[]',result_json=? WHERE id=?",
                  (json.dumps({**result, 'kind': 'unknown', 'status': 'error', 'reasons': ['old unsupported format']}), result['id']))
    upload(client, original_pdf(), credit_pdf())
    repaired = upload(client, ('credit.csv', CREDIT_CSV, 'text/csv'))[0]
    assert repaired['kind'] == 'fee_csv' and repaired['status'] == 'paired'


def test_credit_csv_missing_order_date_remains_review_despite_eligible_original(client):
    upload(client, original_pdf())
    results = upload(client, ('credit.csv', CREDIT_CSV, 'text/csv'), credit_pdf())
    assert next(r for r in results if r['filename'] == 'credit.csv')['status'] == 'paired_review'
    credit = next(r for r in results if r['filename'] == 'credit.pdf')
    assert credit['status'] == 'needs_review'
    assert invoices.get_invoice(credit['document_id'].removeprefix('book:'))['transactions'] == []
