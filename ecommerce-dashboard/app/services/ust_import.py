"""One upload workflow for VAT reports and platform invoice PDF/CSV pairs."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import sqlite3
import threading
from pathlib import Path
from tempfile import TemporaryDirectory

from app import config
from app.db import connect_combined_db
from app.services import amazon_tax_import as tax, bookkeeping_full as books, platform_invoices as invoices
from app.services import ust_documents, ust_report, invoice_parser
from app.services.ust_input_vat import FeeTaxContext
from app.services.ust_reconciliation import _read, _provider

_LOCK = threading.RLock()


def _schema():
    with connect_combined_db() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS ust_import_files(
            id TEXT PRIMARY KEY, filename TEXT NOT NULL, kind TEXT NOT NULL,
            stored_path TEXT NOT NULL, invoice_numbers_json TEXT NOT NULL DEFAULT '[]',
            result_json TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL)""")


def _records(text):
    delimiter = '\t' if '\t' in text.partition('\n')[0] else ','
    reader = csv.DictReader(io.StringIO(text.lstrip('\ufeff')), delimiter=delimiter)
    fields, records = list(reader.fieldnames or []), list(reader)
    # Amazon uses a different identity column for fee credit-note exports.
    # Keep the original-reference column; pairing uses the CREDIT, not its origin.
    if 'Credit Note Number' in fields and 'Fees Invoice Number' not in fields:
        fields = ['Fees Invoice Number' if f == 'Credit Note Number' else f for f in fields]
        records = [{('Fees Invoice Number' if k == 'Credit Note Number' else k): v
                    for k, v in row.items()} for row in records]
    return fields, records


def _decode(data):
    try:
        return data.decode('utf-8-sig')
    except UnicodeDecodeError:
        return data.decode('iso-8859-2')


def _save_result(file_id, result):
    csv_reasons = result.pop('csv_review_reasons', [])
    with connect_combined_db() as c:
        c.execute("UPDATE ust_import_files SET result_json=? WHERE id=?", (json.dumps(result), file_id))
    if result.get('paired_csv_id'):
        _pair_csv(result, csv_reasons)
    return result


def _pair_csv(invoice_result, csv_reasons):
    """Project every invoice outcome, including duplicate/review, to its CSV."""
    with connect_combined_db() as c:
        row = c.execute('SELECT invoice_numbers_json,result_json FROM ust_import_files WHERE id=?', (invoice_result['paired_csv_id'],)).fetchone()
    previous = json.loads(row['result_json'])
    numbers = json.loads(row['invoice_numbers_json'])
    pairs = {p['invoice_number']: p for p in previous.get('pairings', [])}
    status = invoice_result['status']
    paired_status = 'paired' if status in {'approved', 'duplicate'} and not csv_reasons else ('conflict' if status == 'conflict' else 'paired_review')
    pairs[invoice_result['invoice_number']] = {key: invoice_result.get(key) for key in ('invoice_number', 'document_id', 'deduction_month')}
    pairs[invoice_result['invoice_number']]['pdf_filename'] = invoice_result['filename']
    pairs[invoice_result['invoice_number']].update(status=paired_status, reasons=csv_reasons or invoice_result.get('reasons', []))
    missing = [number for number in numbers if number not in pairs]
    statuses = {p['status'] for p in pairs.values()}
    overall = 'waiting_pdf' if missing else ('conflict' if 'conflict' in statuses else ('paired_review' if 'paired_review' in statuses else 'paired'))
    reasons = [f'Original-PDF fehlt: {number}' for number in missing]
    reasons.extend(reason for p in pairs.values() for reason in p['reasons'])
    updated = {**previous, 'invoice_numbers': numbers, 'pairings': list(pairs.values()), 'status': overall, 'reasons': list(dict.fromkeys(reasons)),
               'months': sorted({p['deduction_month'] for p in pairs.values() if p.get('deduction_month')})}
    if len(numbers) == 1:
        updated.update({key: invoice_result.get(key) for key in ('invoice_number', 'document_id', 'deduction_month')})
    _save_result(invoice_result['paired_csv_id'], updated)


def _stored_invoice_pdf(number):
    """Include originals uploaded through bookkeeping before the unified flow."""
    candidates = []
    with connect_combined_db() as c:
        candidates.extend((r['document_path'], number + '.pdf') for r in c.execute(
            "SELECT document_path FROM input_vat_invoices WHERE provider='amazon' AND invoice_number=?", (number,)) if r['document_path'])
    if config.BOOKKEEPING_DB_PATH.exists():
        with _read(config.BOOKKEEPING_DB_PATH) as c:
            candidates.extend((books.get_document_resolved_path(r['file_path']), r['original_filename']) for r in c.execute(
                "SELECT d.file_path,d.original_filename FROM monthly_invoices m JOIN documents d ON d.id=m.document_id WHERE m.provider='amazon' AND m.invoice_number=?", (number,)))
    for raw_path, filename in candidates:
        path = Path(raw_path)
        if not path.is_file():
            continue
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        base = {'id': digest, 'filename': filename, 'kind': 'invoice_pdf', 'status': 'queued', 'reasons': []}
        with connect_combined_db() as c:
            c.execute('INSERT OR IGNORE INTO ust_import_files VALUES (?,?,?,?,?,?,?)',
                      (digest, filename, 'invoice_pdf', str(path), json.dumps([number]), json.dumps(base), tax._utc_now()))
            return dict(c.execute('SELECT * FROM ust_import_files WHERE id=?', (digest,)).fetchone())
    return None


def list_imports():
    _schema()
    with connect_combined_db() as c:
        rows = c.execute("SELECT result_json FROM ust_import_files ORDER BY created_at DESC,rowid DESC LIMIT 30").fetchall()
    return [json.loads(r['result_json']) for r in rows]


def _matching_csv(number):
    with connect_combined_db() as c:
        rows = c.execute("SELECT * FROM ust_import_files WHERE kind='fee_csv' ORDER BY rowid DESC").fetchall()
    for row in rows:
        if number not in json.loads(row['invoice_numbers_json']):
            continue
        fields, records = _records(_decode(Path(row['stored_path']).read_bytes()))
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        writer.writerows(r for r in records if str(r.get('Fees Invoice Number') or '').strip() == number)
        return output.getvalue(), row['id']
    return None, None


def preview_pdf(data):
    if not data.startswith(b'%PDF'):
        raise books.BookkeepingServiceError(400, 'Bitte eine echte PDF-Datei hochladen')
    with TemporaryDirectory(prefix='ust-preview-') as folder:
        path = Path(folder) / 'invoice.pdf'
        path.write_bytes(data)
        return invoices.parse_uploaded_document(pdf_path=path)


def _existing_invoice(provider, number):
    if not config.BOOKKEEPING_DB_PATH.exists():
        return None
    with _read(config.BOOKKEEPING_DB_PATH) as c:
        row = c.execute("SELECT id FROM monthly_invoices WHERE provider=? AND invoice_number=?", (provider, number)).fetchone()
    return invoices.get_invoice(row['id']) if row else None


def _invoice(file_row):
    result = {'id': file_row['id'], 'filename': file_row['filename'], 'kind': 'invoice_pdf', 'reasons': []}
    parsed = invoices.parse_uploaded_document(pdf_path=file_row['stored_path'])
    number = str(parsed.get('invoice_number') or '')
    if number:
        csv_text, csv_id = _matching_csv(number)
        if csv_text:
            parsed = invoices.parse_uploaded_document(pdf_path=file_row['stored_path'], csv_text=csv_text)
            result['paired_csv_id'] = csv_id
    result.update({key: parsed.get(key) for key in ('invoice_number', 'provider', 'invoice_date', 'currency', 'gross_cents', 'vat_cents', 'parse_confidence')})
    result['csv_review_reasons'] = list(parsed.get('needs_review_reasons') or [])
    result['deduction_month'] = ust_documents.resolve_deduction_month(invoice_date=parsed.get('invoice_date'), period_from=parsed.get('period_from'), period_to=parsed.get('period_to'))
    with connect_combined_db() as c:
        c.execute("UPDATE ust_import_files SET invoice_numbers_json=? WHERE id=?", (json.dumps([number] if number else []), file_row['id']))
    if not number or not parsed.get('gross_cents'):
        return _save_result(file_row['id'], {**result, 'status': 'needs_review', 'reasons': ['Rechnungsnummer oder Betrag konnte nicht sicher erkannt werden.']})
    if parsed.get('doc_kind') == 'sales':
        return _save_result(file_row['id'], {**result, 'status': 'evidence_only', 'reasons': ['Umsatzbeleg: Verkaufswerte werden aus dem Steuerreport übernommen, nicht erneut als Vorsteuer.']})
    with connect_combined_db() as c:
        legacy = c.execute("SELECT * FROM input_vat_invoices WHERE provider=? AND invoice_number=?", (parsed['provider'], number)).fetchone()
    existing = _existing_invoice(parsed['provider'], number)
    def same(row):
        return (int(row.get('invoice_amount_cents', row.get('gross_cents', 0))) == parsed['gross_cents']
                and int(row.get('vat_amount_cents', row.get('vat_cents', 0))) == parsed['vat_cents']
                and row.get('currency') == parsed['currency']
                and all(not row.get(key) or row[key][:10] == str(parsed.get(key) or '')[:10]
                        for key in ('invoice_date', 'period_from', 'period_to')))
    if legacy:
        legacy_row = dict(legacy)
        if same(legacy_row) and legacy_row['input_vat_status'] == 'review_required':
            assessment = FeeTaxContext(ust_report.get_vat_effective_from()).assess({**legacy_row, 'lines': parsed.get('lines') or []})
            if legacy_row['currency'] == 'EUR' and parsed['parse_confidence'] == 1 and not parsed['needs_review_reasons'] and not assessment['eligibility_review_count']:
                with connect_combined_db() as c:
                    c.execute('UPDATE input_vat_invoices SET document_path=?,sha256=? WHERE id=?', (file_row['stored_path'], file_row['id'], legacy_row['id']))
                ust_documents.set_input_vat_status(legacy_row['id'], 'confirmed')
                return _save_result(file_row['id'], {**result, 'status': 'approved', 'document_id': legacy_row['id']})
            return _save_result(file_row['id'], {**result, 'status': 'needs_review', 'document_id': legacy_row['id'], 'reasons': ['Vorhandene Rechnung bleibt prüfpflichtig; Zuordnung ist noch nicht eindeutig.']})
        return _save_result(file_row['id'], {**result, 'status': 'duplicate' if same(dict(legacy)) else 'conflict', 'document_id': legacy['id'], 'reasons': [] if same(dict(legacy)) else ['Diese Rechnungsnummer ist bereits mit anderen Beträgen erfasst.']})
    if existing and (existing['status'] == 'approved' or not same(existing)):
        return _save_result(file_row['id'], {**result, 'status': 'duplicate' if same(existing) else 'conflict', 'document_id': 'book:'+existing['id'], 'reasons': [] if same(existing) else ['Freigegebene Rechnung wird nicht durch abweichende Daten überschrieben.']})
    if existing:
        with invoices._open_db() as c:
            invoices._store_parsed_fields(c, existing['id'], parsed, status='needs_review')
        invoice = invoices.get_invoice(existing['id'])
    else:
        document = books.upload_document(filename=file_row['filename'], content=Path(file_row['stored_path']).read_bytes(), mime_type='application/pdf', provider=parsed['provider'], notes='Automatischer USt-Import: '+number)
        invoice = invoices.create_platform_invoice(parsed=parsed, document_id=document['id'], notes='Automatisch aus Original-PDF und zugehöriger CSV erkannt.')
    reasons = list(parsed.get('needs_review_reasons') or [])
    if parsed.get('parse_confidence', 0) < 1 and not reasons:
        reasons.append('Beleg ist nicht vollständig sicher erkannt.')
    if parsed.get('currency') != 'EUR' and parsed.get('vat_cents') and parsed.get('vat_cents_eur') is None:
        reasons.append('Belegte Euro-Umrechnung der Vorsteuer fehlt.')
    assessment = FeeTaxContext(ust_report.get_vat_effective_from()).assess({**parsed, 'doc_type': 'fee', 'deductible_vat_cents': parsed.get('vat_cents_eur') if parsed.get('currency') != 'EUR' else parsed.get('vat_cents')})
    if assessment['eligibility_review_count']:
        reasons.append('USt-Abgrenzung zum ursprünglichen Geschäft ist noch ungeklärt.')
    if reasons:
        return _save_result(file_row['id'], {**result, 'status': 'needs_review', 'document_id': 'book:'+invoice['id'], 'reasons': reasons})
    invoices.approve_platform_invoice(invoice['id'])
    result.update(status='approved', document_id='book:'+invoice['id'])
    return _save_result(file_row['id'], result)


def _processing_priority(row):
    """Confirm positive originals before credits, independent of upload order."""
    if row['kind'] != 'invoice_pdf':
        return 0
    try:
        parsed = invoices.parse_uploaded_document(pdf_path=row['stored_path'])
        return 2 if parsed.get('gross_cents', 0) < 0 else 1
    except (books.BookkeepingServiceError, invoice_parser.InvoiceParseError, ValueError, OSError):
        return 1  # The normal processing loop persists the extraction error.


def import_files(files):
    """Stage all files before processing, so PDF/CSV upload order is irrelevant."""
    with _LOCK:
        _schema()
        staged, results, csv_numbers = [], [], set()
        for filename, data in files:
            digest = hashlib.sha256(data).hexdigest()
            with connect_combined_db() as c:
                old = c.execute('SELECT * FROM ust_import_files WHERE id=?', (digest,)).fetchone()
            if old and old['kind'] != 'unknown':
                result = json.loads(old['result_json'])
                if old['kind'] == 'amazon_tax' and result.get('status') == 'tax_imported':
                    results.append({**result, 'status': 'duplicate'})
                    continue
                staged.append(dict(old))
                if old['kind'] == 'fee_csv':
                    csv_numbers.update(json.loads(old['invoice_numbers_json']))
                continue
            kind, numbers = 'unknown', []
            if data.startswith(b'%PDF'):
                kind = 'invoice_pdf'
            else:
                headers, records = _records(_decode(data))
                if set(tax.REQUIRED_COLUMNS).issubset(headers) or {'TRANSACTION_EVENT_ID', 'ACTIVITY_TRANSACTION_ID', 'SELLER_SKU'}.issubset(headers):
                    kind = 'amazon_tax'
                elif {'Fees Invoice Number', 'Fee ID', 'Total Fees (VAT-Inclusive)'}.issubset(headers):
                    kind = 'fee_csv'
                    numbers = sorted({str(r.get('Fees Invoice Number') or '').strip() for r in records} - {''})
                    csv_numbers.update(numbers)
            folder = config.BOOKKEEPING_DOCUMENTS_DIR / 'ust-imports'
            folder.mkdir(parents=True, exist_ok=True)
            path = folder / (digest + ('.pdf' if kind == 'invoice_pdf' else '.csv'))
            path.write_bytes(data)
            base = {'id': digest, 'filename': Path(filename).name, 'kind': kind, 'status': 'queued', 'reasons': []}
            with connect_combined_db() as c:
                if old:
                    # Earlier releases cached unsupported classifications forever.
                    c.execute('UPDATE ust_import_files SET kind=?,stored_path=?,invoice_numbers_json=?,result_json=? WHERE id=?',
                              (kind, str(path), json.dumps(numbers), json.dumps(base), digest))
                else:
                    c.execute('INSERT INTO ust_import_files VALUES (?,?,?,?,?,?,?)', (digest, base['filename'], kind, str(path), json.dumps(numbers), json.dumps(base), tax._utc_now()))
            staged.append({**base, 'stored_path': str(path), 'invoice_numbers_json': json.dumps(numbers)})
        # Fee CSVs arrive independently too: revisit matching waiting PDFs.
        if csv_numbers:
            with connect_combined_db() as c:
                pdfs = c.execute("SELECT * FROM ust_import_files WHERE kind='invoice_pdf'").fetchall()
            ids = {r['id'] for r in staged}
            staged.extend(dict(r) for r in pdfs if r['id'] not in ids and set(json.loads(r['invoice_numbers_json'])) & csv_numbers)
            represented = {number for r in staged if r['kind'] == 'invoice_pdf' for number in json.loads(r['invoice_numbers_json'])}
            for number in csv_numbers - represented:
                original = _stored_invoice_pdf(number)
                if original and original['id'] not in {r['id'] for r in staged}:
                    staged.append(original)
        if any(r['kind'] == 'invoice_pdf' for r in staged):
            # An original can arrive in a later request. Revisit stored waiting
            # credits too, without requiring another upload or manual approval.
            with connect_combined_db() as c:
                waiting = c.execute("SELECT * FROM ust_import_files WHERE kind='invoice_pdf'").fetchall()
            ids = {r['id'] for r in staged}
            for row in waiting:
                previous = json.loads(row['result_json'])
                if row['id'] not in ids and previous.get('status') == 'needs_review' and int(previous.get('gross_cents') or 0) < 0:
                    staged.append(dict(row))
        for row in sorted(staged, key=_processing_priority):
            result = {'id': row['id'], 'filename': row['filename'], 'kind': row['kind'], 'reasons': []}
            try:
                if row['kind'] == 'invoice_pdf':
                    result = _invoice(row)
                elif row['kind'] == 'amazon_tax':
                    raw = tax.parse_sc_vat_tax_report(_decode(Path(row['stored_path']).read_bytes()))
                    settings = ust_report.get_eu_tax_settings()
                    classified = tax.link_and_inherit(raw, **settings)
                    stats = tax.import_sc_vat_tax_rows(classified)
                    tax.reclassify_all_rows(**settings)
                    result.update(status='tax_imported', **stats, months=sorted({r['booking_date'][:7] for r in classified if r['booking_date']}))
                    _save_result(row['id'], result)
                elif row['kind'] == 'fee_csv':
                    result.update(status='waiting_pdf', invoice_numbers=json.loads(row['invoice_numbers_json']), reasons=['Gebühren-CSV gespeichert; die passende Originalrechnung wird automatisch zugeordnet.'])
                    _save_result(row['id'], result)
                else:
                    result.update(status='error', reasons=['Dateityp nicht erkannt. Unterstützt werden Plattformrechnungen als PDF und Amazon-Steuer-/Gebührenreports als CSV.'])
                    _save_result(row['id'], result)
            except (books.BookkeepingServiceError, tax.AmazonTaxImportError, invoice_parser.InvoiceParseError, ValueError, OSError, sqlite3.Error) as exc:
                result.update(status='error', reasons=[str(exc)])
                _save_result(row['id'], result)
            results.append(result)
        # Return final CSV pairing status, not the earlier staging state.
        with connect_combined_db() as c:
            final = {r['id']: json.loads(r['result_json']) for r in c.execute('SELECT id,result_json FROM ust_import_files')}
        return [r if r.get('status') == 'duplicate' else final.get(r['id'], r) for r in results]


def list_report_documents(month=None, provider=None, status=None):
    context = FeeTaxContext(ust_report.get_vat_effective_from())
    legacy = ust_documents.list_input_vat_invoices()
    items = [{**row, **context.assess(row)} if row['input_vat_status'] == 'confirmed' else {**row, 'effective_deductible_vat_cents': 0} for row in legacy]
    identities = {(row['provider'], row['invoice_number']) for row in legacy}
    if config.BOOKKEEPING_DB_PATH.exists():
        with _read(config.BOOKKEEPING_DB_PATH) as c:
            rows = c.execute('SELECT * FROM monthly_invoices WHERE invoice_number IS NOT NULL').fetchall()
        for raw in rows:
            row = dict(raw)
            name = _provider(row['provider'])
            if (name, row['invoice_number']) in identities or row.get('doc_kind') == 'sales':
                continue
            native_vat = int(row.get('vat_amount_cents') or 0)
            deductible = row.get('vat_cents_eur') if row.get('currency') != 'EUR' else native_vat
            entry = {**row, 'id': 'book:'+row['id'], 'provider': name, 'doc_type': 'damage_compensation' if row.get('doc_category') == 'cancellation_fee' else 'fee',
                     'invoice_date': row.get('invoice_date') or '', 'received_date': row.get('invoice_date') or '', 'service_date': '',
                     'gross_cents': int(row['invoice_amount_cents']), 'net_cents': int(row['invoice_amount_cents'])-native_vat, 'vat_cents': native_vat,
                     'deductible_vat_cents': int(deductible or 0), 'input_vat_status': 'confirmed' if row['status'] == 'approved' else 'review_required',
                     'deduction_month': ust_documents.resolve_deduction_month(invoice_date=row.get('invoice_date'), period_from=row.get('period_from'), period_to=row.get('period_to')),
                     'source': 'bookkeeping', 'sha256': '', 'lines': json.loads(row.get('lines_json') or '[]')}
            if entry['input_vat_status'] == 'confirmed':
                entry.update(context.assess(entry))
            else:
                entry['effective_deductible_vat_cents'] = 0
            items.append(entry)
    return sorted([r for r in items if (not month or r['deduction_month'] == month) and (not provider or r['provider'] == provider) and (not status or r['input_vat_status'] == status)], key=lambda r: (r['deduction_month'], r['invoice_number']))
