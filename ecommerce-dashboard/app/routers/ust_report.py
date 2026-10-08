"""API fuer den monatlichen USt-Report, Eingangsrechnungen und Rate-Korrekturen."""
from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.auth import require_admin_access
from app.config import MAX_UPLOAD_BYTES
from app.services import ust_documents, ust_report

router = APIRouter(
    prefix="/api/ust-report", tags=["ust-report"], dependencies=[Depends(require_admin_access)]
)


class DocumentPatch(BaseModel):
    input_vat_status: Optional[str] = Field(default=None)
    received_date: Optional[str] = Field(default=None)
    service_date: Optional[str] = Field(default=None)
    delivery_date: Optional[str] = Field(default=None)
    notes: Optional[str] = Field(default=None)


class KauflandOverrideRequest(BaseModel):
    id_order_unit: str = Field(min_length=1)
    to_rate: float = Field(gt=0, le=100)
    reason: str = Field(default="")
    created_by: str = Field(default="admin")


class KauflandBulkOverrideRequest(BaseModel):
    month: str = Field(min_length=7, max_length=7)
    reason: str = Field(default="")
    to_rate: float = Field(default=19.0, gt=0, le=100)
    created_by: str = Field(default="admin")


class SettingsRequest(BaseModel):
    eu_tax_regime: Optional[str] = Field(default=None)
    eu_distance_prior_year_cents: Optional[int] = Field(default=None)
    eu_distance_current_year_cents: Optional[int] = Field(default=None)
    block_filing_when_input_vat_incomplete: Optional[bool] = Field(default=None)


class ReportOptions(BaseModel):
    block_filing_when_input_vat_incomplete: bool = Field(default=False)


def _raise(exc: ust_documents.UstDocumentError) -> None:
    raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


def _require_month(month: str) -> str:
    token = str(month or "").strip()
    if len(token) != 7 or token[4] != "-":
        raise HTTPException(status_code=400, detail="month must be YYYY-MM")
    try:
        int(token[:4])
        int(token[5:7])
    except ValueError:
        raise HTTPException(status_code=400, detail="month must be YYYY-MM") from None
    return token


# Statische Routen vor /{month}/...
@router.post('/import')
async def api_import_files(files: list[UploadFile] = File(...)) -> dict[str, Any]:
    from app.services import ust_import
    if len(files) > 25:
        raise HTTPException(400, 'Hoechstens 25 Dateien pro Import')
    payload, total = [], 0
    for file in files:
        data = await file.read(MAX_UPLOAD_BYTES + 1)
        total += len(data)
        if total > MAX_UPLOAD_BYTES:
            raise HTTPException(413, 'Import ist zu gross')
        if not data:
            raise HTTPException(400, 'Leere Datei: ' + (file.filename or 'Datei'))
        payload.append((file.filename or 'Datei', data))
    # PDF extraction/SQLite processing is blocking, not work for the ASGI loop.
    from starlette.concurrency import run_in_threadpool
    items = await run_in_threadpool(ust_import.import_files, payload)
    return {'items': items, 'total': len(items)}


@router.post('/parse-upload')
async def api_preview_file(file: UploadFile = File(...)) -> dict[str, Any]:
    from app.services import ust_import
    from app.services.bookkeeping_full import BookkeepingServiceError
    data = await file.read(MAX_UPLOAD_BYTES + 1)
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, 'Datei ist zu gross')
    try:
        from starlette.concurrency import run_in_threadpool
        return {'parsed': await run_in_threadpool(ust_import.preview_pdf, data)}
    except BookkeepingServiceError as exc:
        raise HTTPException(exc.status_code, exc.detail) from exc
    except ust_import.invoice_parser.InvoiceParseError as exc:
        # Invalid user PDFs remain client errors; a missing image dependency is
        # a service failure and tells operators to update the add-on.
        status_code = 400 if exc.status_code == 422 else exc.status_code
        raise HTTPException(status_code, exc.detail) from exc


@router.get('/imports')
def api_import_history() -> dict[str, Any]:
    from app.services import ust_import
    items = ust_import.list_imports()
    return {'items': items, 'total': len(items)}


@router.get("/months")
def api_list_report_months() -> dict[str, Any]:
    return {"items": ust_report.list_report_months(), "total": len(ust_report.list_report_months())}


@router.get("/documents")
def api_list_documents(
    month: Optional[str] = Query(default=None),
    provider: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None, alias="input_vat_status"),
) -> dict[str, Any]:
    from app.services import ust_import
    items = ust_import.list_report_documents(month=month, provider=provider, status=status)
    return {"items": items, "total": len(items), "limit": len(items), "offset": 0}


@router.post("/documents")
async def api_upload_document(
    file: UploadFile = File(...),
    provider: str = Form(...),
    doc_type: str = Form(...),
    invoice_number: str = Form(...),
    invoice_date: str = Form(...),
    received_date: str = Form(default=""),
    service_date: str = Form(default=""),
    delivery_date: str = Form(default=""),
    period_from: str = Form(default=""),
    period_to: str = Form(default=""),
    currency: str = Form(default="EUR"),
    gross_cents: int = Form(...),
    net_cents: int = Form(...),
    vat_cents: int = Form(...),
    deductible_vat_cents: int = Form(default=0),
    notes: str = Form(default=""),
) -> dict[str, Any]:
    data = await file.read(MAX_UPLOAD_BYTES + 1)
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Datei zu gross")
    if not data:
        raise HTTPException(status_code=400, detail="Beleg-Datei ist leer")
    payload = {
        "provider": provider,
        "doc_type": doc_type,
        "invoice_number": invoice_number,
        "invoice_date": invoice_date,
        "received_date": received_date,
        "service_date": service_date,
        "delivery_date": delivery_date,
        "period_from": period_from,
        "period_to": period_to,
        "currency": currency,
        "gross_cents": gross_cents,
        "net_cents": net_cents,
        "vat_cents": vat_cents,
        "deductible_vat_cents": deductible_vat_cents,
        "source": "manual",
        "notes": notes,
    }
    try:
        invoice = ust_documents.save_input_vat_invoice(
            payload, file_bytes=data, filename=file.filename or "beleg"
        )
    except ust_documents.UstDocumentError as exc:
        _raise(exc)
    return {"ok": True, "invoice": invoice}


@router.get("/documents/{document_id}/download")
def api_download_document(document_id: str) -> FileResponse:
    if document_id.startswith('book:'):
        from app.services import platform_invoices, bookkeeping_full
        from pathlib import Path
        invoice = platform_invoices.get_invoice(document_id[5:])
        with platform_invoices._open_db() as c:
            row = c.execute('SELECT file_path FROM documents WHERE id=?', (invoice.get('document_id'),)).fetchone()
        if row is None:
            raise HTTPException(404, 'Kein Originalbeleg vorhanden')
        path = bookkeeping_full.get_document_resolved_path(row['file_path'])
        if not path.is_file():
            raise HTTPException(404, 'Belegdatei fehlt')
        return FileResponse(path, filename=invoice['invoice_number'] + Path(path).suffix)
    try:
        path, filename = ust_documents.get_document_file(document_id)
    except ust_documents.UstDocumentError as exc:
        _raise(exc)
    return FileResponse(path, filename=filename)


@router.patch("/documents/{document_id}")
def api_patch_document(document_id: str, payload: DocumentPatch) -> dict[str, Any]:
    if document_id.startswith('book:'):
        from app.services import platform_invoices
        from app.services.bookkeeping_full import BookkeepingServiceError
        try:
            if payload.input_vat_status != 'confirmed' or any((payload.service_date, payload.received_date, payload.delivery_date, payload.notes)):
                raise HTTPException(400, 'Buchhaltungsbelege hier nur freigeben; Bearbeitung im Buchungsbereich')
            invoice = platform_invoices.approve_platform_invoice(document_id[5:])
            return {'ok': True, 'invoice': invoice}
        except BookkeepingServiceError as exc:
            raise HTTPException(exc.status_code, exc.detail) from exc
    fields = {key: value for key, value in payload.model_dump().items() if value is not None}
    try:
        invoice = ust_documents.update_input_vat_invoice(document_id, fields)
    except ust_documents.UstDocumentError as exc:
        _raise(exc)
    return {"ok": True, "invoice": invoice}


@router.post("/kaufland-overrides")
def api_kaufland_override(payload: KauflandOverrideRequest) -> dict[str, Any]:
    try:
        result = ust_report.save_kaufland_override(
            payload.id_order_unit,
            to_rate=payload.to_rate,
            reason=payload.reason,
            created_by=payload.created_by,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "override": result}


@router.post("/kaufland-overrides/bulk")
def api_kaufland_bulk_override(payload: KauflandBulkOverrideRequest) -> dict[str, Any]:
    month = _require_month(payload.month)
    try:
        result = ust_report.bulk_override_zero_rates(
            month, reason=payload.reason, to_rate=payload.to_rate, created_by=payload.created_by
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, **result}


@router.post("/settings")
def api_set_settings(payload: SettingsRequest) -> dict[str, Any]:
    try:
        settings = ust_report.set_eu_tax_settings(
            eu_tax_regime=payload.eu_tax_regime,
            eu_distance_prior_year_cents=payload.eu_distance_prior_year_cents,
            eu_distance_current_year_cents=payload.eu_distance_current_year_cents,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "settings": settings}


@router.get("")
def api_get_report(
    month: str = Query(...),
    block_filing_when_input_vat_incomplete: bool = Query(default=False),
) -> dict[str, Any]:
    token = _require_month(month)
    try:
        return ust_report.get_ust_report(token)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{month}/refresh")
def api_refresh_report(month: str, payload: ReportOptions | None = None) -> dict[str, Any]:
    token = _require_month(month)
    options = payload or ReportOptions()
    report = ust_report.build_ust_report(
        token,
        block_filing_when_input_vat_incomplete=options.block_filing_when_input_vat_incomplete,
    )
    return {"ok": True, "report": report}


@router.post("/{month}/file")
def api_file_report(month: str, payload: ReportOptions | None = None) -> dict[str, Any]:
    token = _require_month(month)
    options = payload or ReportOptions()
    try:
        report = ust_report.file_report(
            token,
            block_filing_when_input_vat_incomplete=options.block_filing_when_input_vat_incomplete,
        )
    except ust_documents.UstDocumentError as exc:
        _raise(exc)
    return {"ok": True, "report": report}


@router.post("/{month}/amend")
def api_amend_report(month: str, payload: ReportOptions | None = None) -> dict[str, Any]:
    token = _require_month(month)
    options = payload or ReportOptions()
    try:
        report = ust_report.amend_report(
            token,
            block_filing_when_input_vat_incomplete=options.block_filing_when_input_vat_incomplete,
        )
    except ust_documents.UstDocumentError as exc:
        _raise(exc)
    return {"ok": True, "report": report}
