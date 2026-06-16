from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field

from app.auth import require_admin_access
from app.services.invoices import (
    InvoiceServiceError,
    build_invoice_draft,
    build_preview_pdf,
    create_invoice,
    get_invoice,
    get_invoice_pdf_response_payload,
    get_seller_profile,
    list_invoices,
    save_seller_profile,
)


router = APIRouter(prefix="/api/invoices", tags=["invoices"])
ADMIN_ONLY = [Depends(require_admin_access)]


class SellerProfilePayload(BaseModel):
    legal_name: str = Field(default="")
    street: str = Field(default="")
    address_line2: str = Field(default="")
    postcode: str = Field(default="")
    city: str = Field(default="")
    country: str = Field(default="DE")
    email: str = Field(default="")
    phone: str = Field(default="")
    vat_id: str = Field(default="")
    tax_number: str = Field(default="")
    tax_mode: str = Field(default="small_business")
    invoice_prefix: str = Field(default="RE")
    default_template: str = Field(default="clean")
    footer_note: str = Field(default="")
    payment_note: str = Field(default="")
    eu_invoicing_enabled: bool = Field(default=False)


class CreateInvoiceRequest(BaseModel):
    marketplace: str = Field(...)
    order_id: str = Field(...)
    template_key: Optional[str] = Field(default=None)


def _raise_service_error(exc: InvoiceServiceError) -> None:
    detail: Any = exc.detail
    if exc.details is not None:
        detail = {"message": exc.detail, "details": exc.details}
    raise HTTPException(status_code=exc.status_code, detail=detail) from exc


@router.get("/profile", dependencies=ADMIN_ONLY)
def api_get_invoice_profile() -> dict[str, Any]:
    return {"profile": get_seller_profile()}


@router.put("/profile", dependencies=ADMIN_ONLY)
def api_put_invoice_profile(payload: SellerProfilePayload) -> dict[str, Any]:
    return {"ok": True, "profile": save_seller_profile(payload.model_dump())}


@router.get("", dependencies=ADMIN_ONLY)
def api_list_invoices(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
    marketplace: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    limit: int = Query(default=120, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    return list_invoices(
        from_date=from_date,
        to_date=to_date,
        marketplace=marketplace,
        query=q,
        limit=limit,
        offset=offset,
    )


@router.get("/draft", dependencies=ADMIN_ONLY)
def api_get_invoice_draft(
    marketplace: str = Query(...),
    order_id: str = Query(...),
    template_key: Optional[str] = Query(default=None),
) -> dict[str, Any]:
    try:
        return build_invoice_draft(marketplace, order_id, template_key)
    except InvoiceServiceError as exc:
        _raise_service_error(exc)


@router.get("/preview.pdf", dependencies=ADMIN_ONLY)
def api_preview_invoice_pdf(
    marketplace: str = Query(...),
    order_id: str = Query(...),
    template_key: Optional[str] = Query(default=None),
) -> Response:
    try:
        payload = build_preview_pdf(marketplace, order_id, template_key)
    except InvoiceServiceError as exc:
        _raise_service_error(exc)
    return Response(content=payload, media_type="application/pdf")


@router.post("", dependencies=ADMIN_ONLY)
def api_create_invoice(payload: CreateInvoiceRequest) -> dict[str, Any]:
    try:
        return {"ok": True, "invoice": create_invoice(payload.marketplace, payload.order_id, payload.template_key)}
    except InvoiceServiceError as exc:
        _raise_service_error(exc)


@router.get("/{invoice_id}", dependencies=ADMIN_ONLY)
def api_get_invoice(invoice_id: str) -> dict[str, Any]:
    invoice = get_invoice(invoice_id)
    if invoice is None:
        raise HTTPException(status_code=404, detail="invoice not found")
    return {"invoice": invoice}


@router.get("/{invoice_id}/pdf", dependencies=ADMIN_ONLY)
def api_download_invoice_pdf(
    invoice_id: str,
    disposition: Optional[str] = Query(default="attachment"),
) -> FileResponse:
    try:
        file_path, filename = get_invoice_pdf_response_payload(invoice_id)
    except InvoiceServiceError as exc:
        _raise_service_error(exc)
    requested = str(disposition or "attachment").strip().lower()
    content_disposition_type = "inline" if requested in {"inline", "preview"} else "attachment"
    return FileResponse(
        path=file_path,
        media_type="application/pdf",
        filename=filename,
        content_disposition_type=content_disposition_type,
    )
