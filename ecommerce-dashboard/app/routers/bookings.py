from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.config import BOOKKEEPING_DB_PATH
from app.db import clear_purchase_enrichment
from app.services.bookings import (
    fetch_bookkeeping_document,
    get_order_bookkeeping_breakdown,
    list_booking_orders,
    list_bookings,
    update_booking,
)
from app.services.bookkeeping_full import (
    BookkeepingServiceError,
    create_bookkeeping_transaction,
    create_monthly_invoice,
    delete_bookkeeping_transaction,
    delete_monthly_invoice,
    create_payment_account,
    create_template,
    generate_template_transaction,
    get_bookkeeping_transaction,
    get_monthly_invoice,
    list_bookkeeping_orders,
    list_bookkeeping_transactions,
    list_documents,
    list_monthly_invoices,
    list_payment_accounts,
    list_templates,
    sum_automatic_transactions,
    update_bookkeeping_transaction,
    update_monthly_invoice,
    update_payment_account,
    update_template,
    upload_document,
)


router = APIRouter(prefix="/api/bookings", tags=["bookings"])


class BookingUpdateRequest(BaseModel):
    status: Optional[str] = Field(default=None)
    reference: Optional[str] = Field(default=None)
    notes: Optional[str] = Field(default=None)


def _sanitize_download_name(value: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "-" for ch in value.strip())
    while "--" in safe:
        safe = safe.replace("--", "-")
    safe = safe.strip("-._")
    return safe or "beleg"


def _raise_service_error(exc: BookkeepingServiceError) -> None:
    detail: Any = exc.detail
    if exc.details is not None:
        detail = {"message": exc.detail, "details": exc.details}
    raise HTTPException(status_code=exc.status_code, detail=detail) from exc


@router.get("")
def api_list_bookings(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
    q: Optional[str] = Query(default=None),
    limit: int = Query(default=120, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    payload = list_bookings(
        limit=limit,
        offset=offset,
        from_date=from_date,
        to_date=to_date,
        query=q,
    )
    return {
        "total": payload["total"],
        "items": payload["items"],
        "db_available": payload["db_available"],
        "bookkeeping_db_path": str(BOOKKEEPING_DB_PATH),
    }


@router.patch("/{booking_id}")
def api_update_booking(booking_id: str, payload: BookingUpdateRequest) -> dict[str, Any]:
    try:
        row = update_booking(
            booking_id=booking_id,
            status=payload.status,
            reference=payload.reference,
            notes=payload.notes,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if row is None:
        raise HTTPException(status_code=404, detail="booking not found")

    return {"ok": True, "booking": row}


@router.get("/transactions")
def api_list_bookkeeping_transactions(
    date_from: Optional[str] = Query(default=None, alias="dateFrom"),
    date_to: Optional[str] = Query(default=None, alias="dateTo"),
    tx_type: Optional[str] = Query(default=None, alias="type"),
    provider: Optional[str] = Query(default=None),
    direction: Optional[str] = Query(default=None),
    has_document: Optional[str] = Query(default=None, alias="hasDocument"),
    order_id: Optional[str] = Query(default=None, alias="orderId"),
    template_id: Optional[str] = Query(default=None, alias="templateId"),
    payment_account_id: Optional[str] = Query(default=None, alias="paymentAccountId"),
    booking_class: Optional[str] = Query(default=None, alias="bookingClass"),
) -> dict[str, Any]:
    try:
        return list_bookkeeping_transactions(
            {
                "dateFrom": date_from,
                "dateTo": date_to,
                "type": tx_type,
                "provider": provider,
                "direction": direction,
                "hasDocument": has_document,
                "orderId": order_id,
                "templateId": template_id,
                "paymentAccountId": payment_account_id,
                "bookingClass": booking_class,
            }
        )
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.post("/transactions")
def api_create_bookkeeping_transaction(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        created = create_bookkeeping_transaction(payload)
        return {"ok": True, "transaction": created}
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.get("/transactions/sum")
def api_sum_automatic_transactions(
    provider: str = Query(...),
    period_from: str = Query(..., alias="periodFrom"),
    period_to: str = Query(..., alias="periodTo"),
) -> dict[str, Any]:
    try:
        return sum_automatic_transactions(
            provider=provider,
            period_from=period_from,
            period_to=period_to,
        )
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.patch("/transactions/{transaction_id}")
def api_patch_bookkeeping_transaction(transaction_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    try:
        updated = update_bookkeeping_transaction(transaction_id, payload)
        return {"ok": True, "transaction": updated}
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.get("/transactions/{transaction_id}")
def api_get_bookkeeping_transaction(transaction_id: str) -> dict[str, Any]:
    try:
        transaction = get_bookkeeping_transaction(transaction_id)
        return {"ok": True, "transaction": transaction}
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.delete("/transactions/{transaction_id}")
def api_delete_bookkeeping_transaction(transaction_id: str) -> dict[str, Any]:
    try:
        deleted = delete_bookkeeping_transaction(transaction_id)

        # Cascade: if this was an auto-synced COGS transaction, clear the
        # order enrichment so the purchase price disappears from the order
        # and the next sync won't re-create the transaction.
        source_key = str(deleted.get("source_key") or "")
        if source_key.startswith("combined:") and source_key.endswith(":cogs"):
            # source_key format: "combined:{marketplace}:{external_order_id}:cogs"
            parts = source_key.split(":")
            if len(parts) == 4:
                marketplace, external_order_id = parts[1], parts[2]
                clear_purchase_enrichment(
                    marketplace=marketplace,
                    order_id=external_order_id,
                )

        return {"ok": True, "deleted": deleted}
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.get("/ledger/orders")
def api_list_bookkeeping_orders() -> dict[str, Any]:
    try:
        return list_bookkeeping_orders()
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.get("/payment-accounts")
def api_list_payment_accounts() -> dict[str, Any]:
    try:
        return list_payment_accounts()
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.post("/payment-accounts")
def api_create_payment_account(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        created = create_payment_account(payload)
        return {"ok": True, "payment_account": created}
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.patch("/payment-accounts/{payment_account_id}")
def api_patch_payment_account(payment_account_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    try:
        updated = update_payment_account(payment_account_id, payload)
        return {"ok": True, "payment_account": updated}
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.get("/templates")
def api_list_templates() -> dict[str, Any]:
    try:
        return list_templates()
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.post("/templates")
def api_create_template(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        created = create_template(payload)
        return {"ok": True, "template": created}
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.patch("/templates/{template_id}")
def api_patch_template(template_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    try:
        updated = update_template(template_id, payload)
        return {"ok": True, "template": updated}
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.post("/templates/{template_id}/generate-transaction")
def api_generate_template_transaction(
    template_id: str,
    payload: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    try:
        created = generate_template_transaction(template_id, payload or {})
        return {"ok": True, "transaction": created}
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.get("/documents")
def api_list_documents() -> dict[str, Any]:
    try:
        return list_documents()
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.post("/documents/upload")
async def api_upload_document(
    file: UploadFile = File(...),
    notes: Optional[str] = Form(default=None),
    transaction_id: Optional[str] = Form(default=None),
    provider: Optional[str] = Form(default=None),
    transaction_type: Optional[str] = Form(default=None),
    booking_date: Optional[str] = Form(default=None),
    amount_cents: Optional[str] = Form(default=None),
    currency: Optional[str] = Form(default=None),
) -> dict[str, Any]:
    content = await file.read()

    parsed_amount: Optional[int] = None
    if amount_cents is not None and str(amount_cents).strip():
        try:
            parsed_amount = int(str(amount_cents).strip())
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="amount_cents must be integer") from exc

    try:
        created = upload_document(
            filename=file.filename or "document",
            content=content,
            mime_type=file.content_type,
            notes=notes,
            transaction_id=transaction_id,
            provider=provider,
            transaction_type=transaction_type,
            booking_date=booking_date,
            amount_cents=parsed_amount,
            currency=currency,
        )
        return {"ok": True, "document": created}
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.get("/orders")
def api_list_booking_orders(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
    marketplace: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
) -> dict[str, Any]:
    payload = list_booking_orders(
        from_date=from_date,
        to_date=to_date,
        marketplace=marketplace,
        query=q,
    )
    return payload


@router.get("/orders/{marketplace}/{external_order_id}/detail")
def api_booking_order_detail(marketplace: str, external_order_id: str) -> dict[str, Any]:
    payload = get_order_bookkeeping_breakdown(
        marketplace=marketplace,
        external_order_id=external_order_id,
    )
    return payload


@router.get("/documents/{document_id}/download")
def api_download_booking_document(
    document_id: str,
    disposition: Optional[str] = Query(default="attachment"),
) -> FileResponse:
    row = fetch_bookkeeping_document(document_id)
    if row is None:
        raise HTTPException(status_code=404, detail="document not found")

    path = Path(str(row.get("resolved_path") or ""))
    if not row.get("exists") or not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="document file not found")

    original = str(row.get("original_filename") or row.get("stored_filename") or path.name)
    download_name = _sanitize_download_name(original)

    requested = str(disposition or "attachment").strip().lower()
    content_disposition_type = "inline" if requested in {"inline", "preview"} else "attachment"

    return FileResponse(
        path=path,
        media_type=str(row.get("mime_type") or "application/octet-stream"),
        filename=download_name,
        content_disposition_type=content_disposition_type,
    )


# ---------------------------------------------------------------------------
# Monthly Invoices (Sammelrechnungen)
# ---------------------------------------------------------------------------

@router.get("/monthly-invoices")
def api_list_monthly_invoices() -> dict[str, Any]:
    try:
        return list_monthly_invoices()
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.get("/monthly-invoices/{invoice_id}")
def api_get_monthly_invoice(invoice_id: str) -> dict[str, Any]:
    try:
        invoice = get_monthly_invoice(invoice_id)
        return {"ok": True, "invoice": invoice}
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.post("/monthly-invoices")
def api_create_monthly_invoice(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        created = create_monthly_invoice(payload)
        return {"ok": True, "invoice": created}
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.patch("/monthly-invoices/{invoice_id}")
def api_update_monthly_invoice(invoice_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    try:
        updated = update_monthly_invoice(invoice_id, payload)
        return {"ok": True, "invoice": updated}
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)


@router.delete("/monthly-invoices/{invoice_id}")
def api_delete_monthly_invoice(invoice_id: str) -> dict[str, Any]:
    try:
        deleted = delete_monthly_invoice(invoice_id)
        return {"ok": True, **deleted}
    except BookkeepingServiceError as exc:
        _raise_service_error(exc)
