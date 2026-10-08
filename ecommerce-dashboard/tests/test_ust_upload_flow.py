"""Ordinary multipart uploads exercise extraction, assignment and booking."""
import csv
import io
import json
import sqlite3

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app import config, db
from app.auth import require_admin_access
from app.routers import ust_report as router
from app.services import bookkeeping_full as books, platform_invoices as invoices
from test_platform_invoices import DDL, RECHNUNG
from test_invoice_parser import AMAZON_GEBUEHR, AMAZON_FEE_CSV, AMAZON_GUTSCHRIFT
from test_ust_report import KAUFLAND_SCHEMA
from test_ust_report_reliability import tax_row


def pdf_bytes(text):
    """A real extractable Courier PDF, not text masquerading as a PDF."""
    commands = ["BT /F1 10 Tf 12 TL 20 1700 Td"]
    for line in text.splitlines():
        safe = line.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
        commands.append(f"({safe}) Tj T*")
    commands.append("ET")
    stream = "\n".join(commands).encode("latin-1")
    objects = [b"<< /Type /Catalog /Pages 2 0 R >>", b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
               b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 1800 1800] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
               b"<< /Type /Font /Subtype /Type1 /BaseFont /Courier /Encoding /WinAnsiEncoding >>",
               b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream"]
    data, offsets = b"%PDF-1.4\n", [0]
    for index, obj in enumerate(objects, 1):
        offsets.append(len(data))
        data += str(index).encode() + b" 0 obj\n" + obj + b"\nendobj\n"
    xref = len(data)
    data += b"xref\n0 6\n0000000000 65535 f \n"
    data += b"".join(f"{offset:010d} 00000 n \n".encode() for offset in offsets[1:])
    return data + f"trailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF".encode()


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "COMBINED_DB_PATH", tmp_path / "combined.sqlite3")
    db.init_combined_db()
    book_path = tmp_path / "books.sqlite3"
    with sqlite3.connect(book_path) as c:
        c.executescript(DDL + "CREATE TABLE recurring_templates(id TEXT PRIMARY KEY,start_date TEXT);")
    monkeypatch.setattr(config, "BOOKKEEPING_DB_PATH", book_path)
    monkeypatch.setattr(books, "BOOKKEEPING_DB_PATH", book_path)
    monkeypatch.setattr(invoices, "BOOKKEEPING_DB_PATH", book_path)
    monkeypatch.setattr(books, "BOOKKEEPING_DOCUMENTS_DIR", tmp_path / "documents")
    monkeypatch.setattr(books, "BOOKKEEPING_PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(config, "BOOKKEEPING_DOCUMENTS_DIR", tmp_path / "documents")
    monkeypatch.setattr(config, "KAUFLAND_DB_PATH", tmp_path / "kaufland.sqlite3")
    with sqlite3.connect(config.KAUFLAND_DB_PATH) as c:
        c.executescript(KAUFLAND_SCHEMA)
        for unit, order in [("123456789012345", "TEST-A"), ("234567890123456", "TEST-B")]:
            c.execute("INSERT INTO order_units(id_order_unit,id_order,ts_created_iso) VALUES (?,?,'2026-06-01T12:00:00Z')", (unit, order))
    monkeypatch.setattr(config, "AMAZON_FBA_DB_PATH", tmp_path / "absent-amazon.sqlite3")
    with db.connect_combined_db() as c:
        c.execute("INSERT INTO seller_profiles(id,legal_name,tax_mode,vat_effective_from,created_at,updated_at) VALUES ('default','Test','regular','2026-01-01T00:00:00Z','t','t')")
    app = FastAPI()
    app.include_router(router.router)
    app.dependency_overrides[require_admin_access] = lambda: None
    return TestClient(app)


def upload(client, *files):
    response = client.post("/api/ust-report/import", files=[("files", f) for f in files])
    assert response.status_code == 200, response.text
    return response.json()["items"]


def test_real_pdf_upload_extracts_and_assigns_without_entering_amounts(client):
    result = upload(client, ("arbitrary-name.pdf", pdf_bytes(RECHNUNG), "application/pdf"))[0]
    assert result["status"] == "approved"
    assert (result["provider"], result["invoice_number"], result["deduction_month"]) == ("kaufland", "TEST-R0001-00000001", "2026-07")
    assert (result["gross_cents"], result["vat_cents"]) == (6987, 1116)
    listed = client.get("/api/ust-report/documents?month=2026-07").json()["items"]
    assert len(listed) == 1 and listed[0]["input_vat_status"] == "confirmed"
    assert listed[0]["effective_deductible_vat_cents"] == 1116


def test_repeated_pdf_upload_does_not_duplicate_documents_or_bookings(client):
    file = ("bill.pdf", pdf_bytes(RECHNUNG), "application/pdf")
    upload(client, file)
    assert upload(client, file)[0]["status"] == "duplicate"
    with sqlite3.connect(config.BOOKKEEPING_DB_PATH) as c:
        assert c.execute("SELECT COUNT(*) FROM monthly_invoices").fetchone()[0] == 1
        assert c.execute("SELECT COUNT(*) FROM documents").fetchone()[0] == 1


def test_tax_csv_is_classified_by_content_not_filename(client):
    r = tax_row()
    text = io.StringIO()
    writer = csv.DictWriter(text, fieldnames=list(r))
    writer.writeheader()
    writer.writerow(r)
    file = ("export.csv", text.getvalue().encode(), "text/csv")
    assert upload(client, file)[0]["status"] == "tax_imported"
    upload(client, file)
    with db.connect_combined_db() as c:
        assert c.execute("SELECT COUNT(*) FROM amazon_tax_rows").fetchone()[0] == 1


def test_fee_csv_uploaded_first_is_automatically_paired_with_later_pdf(client):
    fee_csv = ('Transaction Date,Transaction ID,Order ID,Fees Invoice Number,Marketplace,Fee ID,Total Fees (VAT-Inclusive)\n08/15/2026,T1,,TEST-AEU-00000001,Amazon.de,MCF FBA Pick & Pack Fee,163.58\n').encode()
    first = upload(client, ("rows.csv", fee_csv, "text/csv"))[0]
    assert first["status"] == "waiting_pdf"
    second = upload(client, ("invoice.pdf", pdf_bytes(AMAZON_GEBUEHR), "application/pdf"))[0]
    assert second["status"] == "approved"
    assert second["vat_cents"] == 2612


def test_invoice_pdf_then_csv_completes_existing_draft_without_duplicate(client):
    text = AMAZON_GEBUEHR.replace("Gebuehren im Zusammenhang mit", "Unbekannte Abrechnungsposition").replace('"Versand durch Amazon"', '"Unbekannte Leistung"')
    assert upload(client, ("invoice.pdf", pdf_bytes(text), "application/pdf"))[0]["status"] == "needs_review"
    fee_csv = ('Transaction Date,Transaction ID,Order ID,Fees Invoice Number,Marketplace,Fee ID,Total Fees (VAT-Inclusive)\n08/15/2026,T1,,TEST-AEU-00000001,Amazon.de,MCF FBA Pick & Pack Fee,163.58\n').encode()
    results = upload(client, ("rows.csv", fee_csv, "text/csv"))
    assert any(r["status"] == "approved" and r["invoice_number"] == "TEST-AEU-00000001" for r in results)
    with sqlite3.connect(config.BOOKKEEPING_DB_PATH) as c:
        assert c.execute("SELECT COUNT(*) FROM monthly_invoices").fetchone()[0] == 1


def test_changed_approved_invoice_is_a_visible_conflict_not_overwritten(client):
    upload(client, ("bill.pdf", pdf_bytes(RECHNUNG), "application/pdf"))
    different = RECHNUNG.replace("69,87", "79,87")
    result = upload(client, ("changed.pdf", pdf_bytes(different), "application/pdf"))[0]
    assert result["status"] == "conflict"
    with sqlite3.connect(config.BOOKKEEPING_DB_PATH) as c:
        assert c.execute("SELECT invoice_amount_cents FROM monthly_invoices").fetchone()[0] == 6987


def test_unknown_file_is_visible_and_never_silently_booked(client):
    result = upload(client, ("mystery.csv", b"one,two\n1,2\n", "text/csv"))[0]
    assert result["status"] == "error" and result["reasons"]
    with sqlite3.connect(config.BOOKKEEPING_DB_PATH) as c:
        assert c.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0


def test_pdf_preview_endpoint_extracts_binary_without_booking(client):
    response = client.post("/api/ust-report/parse-upload", files={"file": ("invoice.pdf", pdf_bytes(RECHNUNG), "application/pdf")})
    assert response.status_code == 200
    assert response.json()["parsed"]["gross_cents"] == 6987
    with sqlite3.connect(config.BOOKKEEPING_DB_PATH) as c:
        assert c.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0


def test_same_batch_pairs_pdf_and_csv_in_either_order(client):
    text = AMAZON_GEBUEHR.replace("Gebuehren im Zusammenhang mit", "Unbekannte Abrechnungsposition").replace('"Versand durch Amazon"', '"Unbekannte Leistung"')
    fee_csv = b'Transaction Date,Transaction ID,Order ID,Fees Invoice Number,Marketplace,Fee ID,Total Fees (VAT-Inclusive)\n08/15/2026,T1,,TEST-AEU-00000001,Amazon.de,MCF FBA Pick & Pack Fee,163.58\n'
    results = upload(client, ("invoice.pdf", pdf_bytes(text), "application/pdf"), ("rows.csv", fee_csv, "text/csv"))
    assert {r["status"] for r in results} == {"approved", "paired"}
    doc = next(r for r in results if r["status"] == "approved")
    original = client.get('/api/ust-report/documents/' + doc['document_id'] + '/download')
    assert original.status_code == 200 and original.content.startswith(b'%PDF')


def test_invalid_pdf_preview_is_a_visible_client_error(client):
    r = client.post('/api/ust-report/parse-upload', files={'file': ('bad.pdf', b'%PDF-broken', 'application/pdf')})
    assert r.status_code == 400


def test_missing_pdf_text_utility_is_reported_as_service_unavailable(client, monkeypatch):
    from app.services import invoice_parser

    def missing_binary(*args, **kwargs):
        raise FileNotFoundError("pdftotext")

    monkeypatch.setattr(invoice_parser.subprocess, "run", missing_binary)
    response = client.post('/api/ust-report/parse-upload', files={'file': ('invoice.pdf', pdf_bytes(RECHNUNG), 'application/pdf')})
    assert response.status_code == 503
    assert 'pdftotext' in response.json()['detail']


def test_upload_finishes_matching_legacy_review_entry_without_second_invoice(client):
    from app.services import ust_documents
    stored = ust_documents.save_input_vat_invoice({'provider': 'kaufland', 'doc_type': 'fee',
        'invoice_number': 'TEST-R0001-00000001', 'invoice_date': '2026-07-01',
        'period_from': '2026-06-01', 'period_to': '2026-06-06', 'gross_cents': 6987,
        'net_cents': 5871, 'vat_cents': 1116, 'deductible_vat_cents': 1116})
    result = upload(client, ('invoice.pdf', pdf_bytes(RECHNUNG), 'application/pdf'))[0]
    assert result['status'] == 'approved'
    assert ust_documents.get_input_vat_invoice(stored['id'])['input_vat_status'] == 'confirmed'
    with sqlite3.connect(config.BOOKKEEPING_DB_PATH) as c:
        assert c.execute('SELECT COUNT(*) FROM monthly_invoices').fetchone()[0] == 0


def fee_csv(number='TEST-AEU-00000001', label='MCF FBA Pick & Pack Fee'):
    return (f'Transaction Date,Transaction ID,Order ID,Fees Invoice Number,Marketplace,Fee ID,Total Fees (VAT-Inclusive)\n08/15/2026,T1,,{number},Amazon.de,{label},163.58\n').encode()


KNOWN_AMAZON = AMAZON_GEBUEHR.replace('Gebuehren im Zusammenhang mit', 'FBA Pick & Pack Fee')


def test_csv_after_approved_pdf_updates_pairing_without_new_bookings(client):
    original = upload(client, ('invoice.pdf', pdf_bytes(KNOWN_AMAZON), 'application/pdf'))[0]
    assert original['status'] == 'approved'
    with sqlite3.connect(config.BOOKKEEPING_DB_PATH) as c:
        before = c.execute('SELECT COUNT(*) FROM transactions').fetchone()[0]
    result = upload(client, ('rows.csv', fee_csv(), 'text/csv'))[0]
    assert result['status'] == 'paired'
    assert result['document_id'] == original['document_id']
    assert result['deduction_month'] == '2026-08'
    assert result['pairings'][0]['invoice_number'] == 'TEST-AEU-00000001'
    upload(client, ('rows.csv', fee_csv(), 'text/csv'))
    with sqlite3.connect(config.BOOKKEEPING_DB_PATH) as c:
        assert c.execute('SELECT COUNT(*) FROM transactions').fetchone()[0] == before


def test_csv_resolves_original_pdf_from_preexisting_bookkeeping_document(client):
    document = books.upload_document(filename='original.pdf', content=pdf_bytes(AMAZON_GEBUEHR), mime_type='application/pdf')
    invoice = invoices.create_platform_invoice(parsed=invoices.parse_uploaded_document(pdf_path=books.get_document_resolved_path(document['file_path'])), document_id=document['id'])
    invoices.approve_platform_invoice(invoice['id'])
    result = upload(client, ('rows.csv', fee_csv(), 'text/csv'))[0]
    assert result['status'] == 'paired'
    assert result['document_id'] == 'book:' + invoice['id']
    with sqlite3.connect(config.BOOKKEEPING_DB_PATH) as c:
        assert c.execute('SELECT COUNT(*) FROM monthly_invoices').fetchone()[0] == 1
        assert c.execute('SELECT COUNT(*) FROM documents').fetchone()[0] == 1


def test_paired_csv_reports_review_not_missing_pdf_when_fee_is_unknown(client):
    results = upload(client, ('rows.csv', fee_csv(label='Mystery fee'), 'text/csv'), ('invoice.pdf', pdf_bytes(AMAZON_GEBUEHR), 'application/pdf'))
    csv_result = next(r for r in results if r['kind'] == 'fee_csv')
    assert csv_result['status'] == 'paired_review'
    assert csv_result['document_id'].startswith('book:')
    assert 'Mystery fee' in ' '.join(csv_result['reasons'])


def test_credit_inherits_all_original_invoices_and_is_counted_once_in_report(client):
    for number in ['TEST-AEU-00000001', 'TEST-AEU-00000003']:
        assert upload(client, (number + '.pdf', pdf_bytes(KNOWN_AMAZON.replace('TEST-AEU-00000001', number)), 'application/pdf'))[0]['status'] == 'approved'
    credit = AMAZON_GUTSCHRIFT.replace('TEST-AEU-00000001\n', 'TEST-AEU-00000001\nTEST-AEU-00000003\n')
    file = ('credit.pdf', pdf_bytes(credit), 'application/pdf')
    result = upload(client, file)[0]
    assert result['status'] == 'approved'
    invoice = invoices.get_invoice(result['document_id'].removeprefix('book:'))
    assert invoice['original_invoice_numbers'] == ['TEST-AEU-00000001', 'TEST-AEU-00000003']
    docs = client.get('/api/ust-report/documents?month=2026-08').json()['items']
    credit_doc = next(r for r in docs if r['id'] == result['document_id'])
    assert credit_doc['effective_deductible_vat_cents'] == -1566
    from app.services import ust_report
    assert ust_report.build_ust_report('2026-08')['totals']['input_vat_cents'] == 3658
    assert upload(client, file)[0]['status'] == 'duplicate'
    assert ust_report.build_ust_report('2026-08')['totals']['input_vat_cents'] == 3658


def test_credit_with_mixed_original_eligibility_remains_unbooked(client):
    with db.connect_combined_db() as c:
        c.execute("UPDATE seller_profiles SET vat_effective_from='2026-07-02T16:27:11Z'")
    upload(client, ('august.pdf', pdf_bytes(KNOWN_AMAZON), 'application/pdf'))
    june = KNOWN_AMAZON.replace('31/08/2026', '30/06/2026').replace('01/08/2026', '01/06/2026').replace('TEST-AEU-00000001', 'TEST-AEU-00000003')
    upload(client, ('june.pdf', pdf_bytes(june), 'application/pdf'))
    credit = AMAZON_GUTSCHRIFT.replace('TEST-AEU-00000001\n', 'TEST-AEU-00000001\nTEST-AEU-00000003\n')
    result = upload(client, ('credit.pdf', pdf_bytes(credit), 'application/pdf'))[0]
    assert result['status'] == 'needs_review'
    invoice = invoices.get_invoice(result['document_id'].removeprefix('book:'))
    assert invoice['transactions'] == []


@pytest.mark.parametrize('reference', ['MISSING-AEU-1', 'TEST-CN-AEU-00000001'])
def test_missing_or_circular_credit_reference_cannot_auto_approve(client, reference):
    credit = AMAZON_GUTSCHRIFT.replace('TEST-CN-00000001', 'TEST-CN-AEU-00000001').replace('TEST-AEU-00000001\n', reference + '\n')
    result = upload(client, ('credit.pdf', pdf_bytes(credit), 'application/pdf'))[0]
    assert result['status'] == 'needs_review'
    with sqlite3.connect(config.BOOKKEEPING_DB_PATH) as c:
        assert c.execute('SELECT COUNT(*) FROM transactions').fetchone()[0] == 0


def test_credit_for_pre_vat_original_is_approved_but_not_deducted(client):
    with db.connect_combined_db() as c:
        c.execute("UPDATE seller_profiles SET vat_effective_from='2026-07-02T16:27:11Z'")
    original = KNOWN_AMAZON.replace('31/08/2026', '30/06/2026').replace('01/08/2026', '01/06/2026')
    assert upload(client, ('june.pdf', pdf_bytes(original), 'application/pdf'))[0]['status'] == 'approved'
    result = upload(client, ('credit.pdf', pdf_bytes(AMAZON_GUTSCHRIFT), 'application/pdf'))[0]
    assert result['status'] == 'approved'
    docs = client.get('/api/ust-report/documents?month=2026-08').json()['items']
    assert docs[0]['effective_deductible_vat_cents'] == 0


def test_removal_uses_service_date_not_a_customer_order_reference(client):
    from app.services.ust_input_vat import FeeTaxContext
    from app.services import ust_report
    with db.connect_combined_db() as c:
        c.execute("UPDATE seller_profiles SET vat_effective_from='2026-07-02T16:27:11Z'")
    context = FeeTaxContext(ust_report.get_vat_effective_from())
    row = {'provider': 'amazon', 'vat_cents': 19, 'deductible_vat_cents': 19, 'period_to': '2026-08-31',
        'lines': [{'position_key': 'inventory_removal', 'order_ref': 'removal-request-id', 'line_date': '2026-08-15', 'vat_cents': 19}]}
    assert context.assess(row)['effective_deductible_vat_cents'] == 19
    row['lines'][0]['position_key'] = 'shipping_chargeback'
    assert context.assess(row)['eligibility_review_count'] == 1


def test_csv_with_multiple_invoices_preserves_each_pairing_and_missing_number(client):
    upload(client, ('invoice.pdf', pdf_bytes(KNOWN_AMAZON), 'application/pdf'))
    text = fee_csv() + fee_csv('TEST-AEU-00000003').split(b'\n', 1)[1]
    result = upload(client, ('rows.csv', text, 'text/csv'))[0]
    assert result['status'] == 'waiting_pdf'
    assert result['pairings'][0]['invoice_number'] == 'TEST-AEU-00000001'
    assert 'TEST-AEU-00000003' in ' '.join(result['reasons'])
    upload(client, ('second.pdf', pdf_bytes(KNOWN_AMAZON.replace('TEST-AEU-00000001', 'TEST-AEU-00000003')), 'application/pdf'))
    history = client.get('/api/ust-report/imports').json()['items']
    updated = next(r for r in history if r['id'] == result['id'])
    assert updated['status'] == 'paired'
    assert {p['invoice_number'] for p in updated['pairings']} == {'TEST-AEU-00000001', 'TEST-AEU-00000003'}


def test_new_csv_mismatch_does_not_change_approved_invoice_or_hide_review(client):
    original = upload(client, ('invoice.pdf', pdf_bytes(KNOWN_AMAZON), 'application/pdf'))[0]
    result = upload(client, ('wrong.csv', fee_csv().replace(b'163.58', b'200.00'), 'text/csv'))[0]
    assert result['status'] == 'paired_review'
    assert 'kontrollsumme_brutto_verfehlt' in result['reasons']
    assert invoices.get_invoice(original['document_id'].removeprefix('book:'))['invoice_amount_cents'] == 16358
