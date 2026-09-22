from __future__ import annotations

import csv
import hashlib
import io
import json
import sqlite3
from typing import Literal
from uuid import uuid4

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.auth import require_admin_access
from app.config import BOOKKEEPING_DOCUMENTS_DIR, MAX_UPLOAD_BYTES
from app.db import sanitize_filename
from app.services import amazon_procurement as service
from app.services.importers import amazon_sp_api as db
from app.uploads import stream_fileobj_to_path, EmptyUploadError, UploadTooLargeError

router = APIRouter(prefix='/api/amazon/pool', tags=['amazon-pool'], dependencies=[Depends(require_admin_access)])


class Command(BaseModel):
    request_id: str = Field(min_length=1, max_length=200)


class Product(Command):
    name: str = Field(min_length=1, max_length=300)


class Listing(Command):
    product_id: str
    marketplace_id: str = Field(min_length=1)
    seller_sku: str = Field(min_length=1)
    asin: str = ''


class InvoiceLine(BaseModel):
    product_id: str
    quantity: int = Field(gt=0)
    gross_cents: int = Field(ge=0)
    net_cents: int = Field(ge=0)
    vat_cents: int = Field(ge=0)
    deductible_vat_cents: int | None = Field(default=None, ge=0)


class Invoice(Command):
    supplier: str = Field(min_length=1)
    number: str = Field(min_length=1)
    invoice_date: str
    currency: str = Field(default='EUR', min_length=3, max_length=3)
    fx_rate: str = '1'
    fx_reference: str = ''
    notes: str = ''
    freight_cents: int = Field(default=0, ge=0)
    freight_allocations: list[int] | None = None
    lines: list[InvoiceLine] = Field(min_length=1)


class Receipt(Command):
    line_id: str
    quantity: int = Field(gt=0)
    received_at: str


class TransferLine(BaseModel):
    shipment_item_id: str
    quantity: int = Field(gt=0)
    receipt_id: str | None = None


class Reservation(Command):
    shipment_id: str
    marketplace_id: str = Field(min_length=1)
    package_reference: str = ''
    lines: list[TransferLine] = Field(min_length=1)


class TransferAction(Command):
    transfer_id: str
    action: Literal['dispatch', 'cancel']
    dispatched_at: str = ''
    freight_cents: int = Field(default=0, ge=0)
    freight_allocations: list[int] | None = None
    source_cost_id: str | None = None


class Payment(Command):
    invoice_id: str
    paid_at: str
    amount_cents: int = Field(gt=0)
    account_reference: str = Field(min_length=1)


class Adjustment(Command):
    receipt_id: str
    quantity: int = Field(gt=0)
    occurred_at: str
    reason: str = Field(min_length=1)


def invoke(fn, payload):
    try:
        return fn(payload.model_dump())
    except (ValueError, sqlite3.IntegrityError) as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get('')
def get_pool():
    return service.overview()


@router.post('/products')
def create_product(payload: Product):
    return invoke(service.create_product, payload)


@router.post('/listings')
def map_listing(payload: Listing):
    return invoke(service.map_listing, payload)


@router.post('/invoices')
def create_invoice(payload: Invoice):
    return invoke(service.create_invoice, payload)


@router.post('/receipts')
def receive_purchase(payload: Receipt):
    return invoke(service.receive_purchase, payload)


@router.post('/transfers')
def reserve_transfer(payload: Reservation):
    return invoke(service.reserve_transfer, payload)


@router.post('/transfers/action')
def change_transfer(payload: TransferAction):
    return invoke(service.change_transfer, payload)


@router.post('/payments')
def record_payment(payload: Payment):
    result = invoke(service.record_payment, payload)
    result['bookkeeping'] = service.sync_bookkeeping()
    return result


@router.post('/adjustments')
def adjust_stock(payload: Adjustment):
    return invoke(service.adjust_stock, payload)


@router.post('/reconcile')
def reconcile():
    result = service.reconcile_all()
    result['bookkeeping'] = service.sync_bookkeeping()
    return result


@router.post('/invoices/{invoice_id}/documents')
def upload_document(invoice_id: str, file: UploadFile = File(...)):
    service.initialize()
    with db._connect() as c:
        if not c.execute('SELECT 1 FROM pool_invoices WHERE id=?', (invoice_id,)).fetchone():
            raise HTTPException(404, 'Rechnung nicht gefunden')
    filename = sanitize_filename(file.filename or 'invoice')
    target = BOOKKEEPING_DOCUMENTS_DIR / 'amazon-pool' / invoice_id / (str(uuid4()) + '-' + filename)
    try:
        stream_fileobj_to_path(file.file, target, max_bytes=MAX_UPLOAD_BYTES)
    except (EmptyUploadError, UploadTooLargeError) as exc:
        raise HTTPException(400, str(exc)) from exc
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    document_id = db._stable_id('pool-document', invoice_id + ':' + digest)
    try:
        with db._connect() as c:
            old = c.execute('SELECT id FROM pool_documents WHERE id=?', (document_id,)).fetchone()
            if old:
                target.unlink(missing_ok=True)
            else:
                c.execute('INSERT INTO pool_documents VALUES (?,?,?,?,?)', (document_id, invoice_id, filename, str(target), db._utc_now()))
    except Exception:
        target.unlink(missing_ok=True)
        raise
    return {'id': document_id, 'bookkeeping': service.sync_bookkeeping()}


@router.get('/documents/{document_id}')
def download_document(document_id: str):
    service.initialize()
    with db._connect() as c:
        doc = c.execute('SELECT * FROM pool_documents WHERE id=?', (document_id,)).fetchone()
    if not doc:
        raise HTTPException(404, 'Beleg nicht gefunden')
    return FileResponse(doc['document_path'], filename=doc['filename'])


@router.post('/tax-report')
def import_tax_report(file: UploadFile = File(...)):
    data = file.file.read(MAX_UPLOAD_BYTES + 1)
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, 'Datei zu groß')
    try:
        text = data.decode('utf-8-sig')
        delimiter = '\t' if '\t' in text.partition('\n')[0] else ','
        rows = list(csv.DictReader(io.StringIO(text), delimiter=delimiter))
        if not rows or not {'Transaction ID', 'Order ID', 'SKU'}.issubset(rows[0]):
            raise ValueError('SC_VAT_TAX_REPORT mit Transaction ID, Order ID und SKU erforderlich')
        service.initialize()
        with db._connect() as c:
            for row in rows:
                if not row['Transaction ID']:
                    raise ValueError('Transaction ID fehlt')
                raw = json.dumps(row, sort_keys=True)
                row_id = hashlib.sha256(raw.encode()).hexdigest()
                c.execute('INSERT OR IGNORE INTO pool_tax_rows VALUES (?,?,?,?,?,?)',
                          (row_id, row['Transaction ID'], row['Order ID'], row['SKU'], raw, db._utc_now()))
        return {'rows': len(rows), 'status': 'stored_for_review'}
    except (ValueError, UnicodeError, csv.Error) as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get('/reconcile-preview')
def reconciliation_preview():
    from app.services.amazon_financials import listing_metrics
    data = service.overview()
    return {'receipt_differences': data['issues'], 'listings': listing_metrics(),
            'note': 'Abgleich ergänzt nur belegte Mengen; fehlende Einkäufe werden nicht erfunden.'}


class Revision(Command):
    line_id: str
    expected_cost_cents: int
    gross_cents: int = Field(ge=0)
    net_cents: int = Field(ge=0)
    vat_cents: int = Field(ge=0)
    deductible_vat_cents: int = Field(ge=0)
    reason: str = Field(min_length=1)
    preview: bool = True


class LegacyMigration(Command):
    legacy_invoice_id: str
    marketplace_id: str
    received_at: str


@router.post('/revisions')
def revise_line(payload: Revision):
    return invoke(service.revise_line, payload)


@router.post('/legacy-migration')
def migrate_legacy(payload: LegacyMigration):
    return invoke(service.migrate_legacy_invoice, payload)


class FreightRevision(Command):
    transfer_id: str
    expected_freight_cents: int
    freight_cents: int = Field(ge=0)
    source_cost_id: str | None = None
    freight_allocations: list[int] | None = None
    reason: str = Field(min_length=1)
    preview: bool = True


@router.post('/transfer-cost-revisions')
def revise_transfer_cost(payload: FreightRevision):
    return invoke(service.revise_transfer_cost, payload)
