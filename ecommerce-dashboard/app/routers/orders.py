from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.auth import require_admin_access
from app.config import ALLOWED_MARKETPLACES, MAX_UPLOAD_BYTES
from app.db import (
    build_invoice_storage_path,
    create_invoice_document,
    fetch_aliexpress_order_mappings_for_marketplace_order,
    fetch_invoice_document,
    replace_aliexpress_order_mappings,
    resolve_invoice_path,
    sanitize_filename,
    upsert_purchase_enrichment,
)
from app.services.bookings import get_order_bookkeeping_breakdown, sync_combined_orders_into_bookkeeping
from app.services.orders import get_order_detail, list_orders
from app.uploads import EmptyUploadError, UploadTooLargeError, stream_fileobj_to_path


router = APIRouter(prefix="/api/orders", tags=["orders"])
ADMIN_ONLY = [Depends(require_admin_access)]


class PurchaseUpdateRequest(BaseModel):
    purchase_cost_eur: Optional[float] = Field(default=None)
    purchase_currency: Optional[str] = Field(default="EUR")
    supplier_name: Optional[str] = Field(default=None)
    purchase_notes: Optional[str] = Field(default=None)


class AliExpressOrderMappingItemRequest(BaseModel):
    aliexpress_order_id: str = Field(min_length=1)
    match_status: Optional[str] = Field(default="matched")
    match_confidence: Optional[float] = Field(default=None, ge=0, le=1)
    match_method: Optional[str] = Field(default=None)
    source: Optional[str] = Field(default="manual")
    note: Optional[str] = Field(default=None)


class AliExpressOrderMappingsUpdateRequest(BaseModel):
    mappings: list[AliExpressOrderMappingItemRequest] = Field(default_factory=list)


def _validate_marketplace(marketplace: str) -> str:
    normalized = (marketplace or "").strip().lower()
    if normalized not in ALLOWED_MARKETPLACES:
        raise HTTPException(status_code=400, detail="marketplace must be shopify or kaufland")
    return normalized


def _to_cents(value: Optional[float]) -> Optional[int]:
    if value is None:
        return None
    if value < 0:
        raise HTTPException(status_code=400, detail="purchase_cost_eur must be >= 0")
    return int(round(value * 100))


def _resolve_invoice_order_token(marketplace: str, order_id: str) -> str:
    token = str(order_id or "").strip()
    try:
        detail = get_order_detail(marketplace, order_id)
    except Exception:
        detail = None

    if isinstance(detail, dict):
        summary = detail.get("summary") if isinstance(detail.get("summary"), dict) else {}
        preferred = str(
            summary.get("external_order_id")
            or summary.get("order_id")
            or order_id
        ).strip()
        if preferred:
            token = preferred

    safe = sanitize_filename(token)
    return safe or sanitize_filename(order_id) or "order"


def _build_invoice_filename(marketplace: str, order_token: str, source_filename: str) -> str:
    ext = Path(str(source_filename or "invoice")).suffix.lower() or ".bin"
    market_token = sanitize_filename(str(marketplace or "order").lower()) or "order"
    order_safe = sanitize_filename(order_token) or "order"
    return sanitize_filename(f"{market_token}_{order_safe}_invoice{ext}")


@router.get("")
def api_list_orders(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
    marketplace: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    status_filter: Optional[str] = Query(default=None, alias="status"),
    payment_filters: list[str] = Query(default_factory=list, alias="payment"),
    hide_canceled: bool = Query(default=False),
    has_purchase_cost: bool = Query(default=False),
    no_purchase_cost: bool = Query(default=False),
    has_invoice: bool = Query(default=False),
    no_invoice: bool = Query(default=False),
    limit: int = Query(default=200, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    if marketplace is not None:
        normalized = (marketplace or "").strip().lower()
        if normalized not in ALLOWED_MARKETPLACES:
            raise HTTPException(status_code=400, detail="marketplace must be shopify or kaufland")
    payload = list_orders(
        from_date=from_date,
        to_date=to_date,
        marketplace=marketplace,
        query=q,
        status_filter=status_filter,
        payment_filters=payment_filters,
        hide_canceled=hide_canceled,
        has_purchase_cost=has_purchase_cost,
        no_purchase_cost=no_purchase_cost,
        has_invoice=has_invoice,
        no_invoice=no_invoice,
        limit=limit,
        offset=offset,
        include_raw_fallbacks=False,
    )
    return {
        "total": payload["total"],
        "items": payload["items"],
        "limit": limit,
        "offset": offset,
    }


@router.get("/{marketplace}/{order_id}")
def api_get_order_detail(marketplace: str, order_id: str) -> dict[str, Any]:
    market = _validate_marketplace(marketplace)
    detail = get_order_detail(market, order_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="order not found")

    summary = detail.get("summary") if isinstance(detail.get("summary"), dict) else {}
    external_order_id = str(
        summary.get("external_order_id")
        or summary.get("order_id")
        or order_id
    ).strip()
    detail["bookkeeping_breakdown"] = get_order_bookkeeping_breakdown(
        marketplace=market,
        external_order_id=external_order_id,
    )
    return detail


@router.patch("/{marketplace}/{order_id}/purchase", dependencies=ADMIN_ONLY)
def api_update_purchase(
    marketplace: str,
    order_id: str,
    payload: PurchaseUpdateRequest,
) -> dict[str, Any]:
    market = _validate_marketplace(marketplace)
    purchase_cents = _to_cents(payload.purchase_cost_eur)

    updated = upsert_purchase_enrichment(
        marketplace=market,
        order_id=order_id,
        purchase_cost_cents=purchase_cents,
        purchase_currency=payload.purchase_currency,
        supplier_name=payload.supplier_name,
        purchase_notes=payload.purchase_notes,
    )
    booking_sync = sync_combined_orders_into_bookkeeping(marketplace=market, order_id=order_id)
    return {"ok": True, "enrichment": updated, "bookkeeping_sync": booking_sync}


@router.get("/{marketplace}/{order_id}/aliexpress-mappings", dependencies=ADMIN_ONLY)
def api_get_aliexpress_mappings(
    marketplace: str,
    order_id: str,
) -> dict[str, Any]:
    market = _validate_marketplace(marketplace)
    return {
        "ok": True,
        "marketplace": market,
        "order_id": order_id,
        "mappings": fetch_aliexpress_order_mappings_for_marketplace_order(
            marketplace=market,
            order_id=order_id,
        ),
    }


@router.put("/{marketplace}/{order_id}/aliexpress-mappings", dependencies=ADMIN_ONLY)
def api_replace_aliexpress_mappings(
    marketplace: str,
    order_id: str,
    payload: AliExpressOrderMappingsUpdateRequest,
) -> dict[str, Any]:
    market = _validate_marketplace(marketplace)
    try:
        mappings = replace_aliexpress_order_mappings(
            marketplace=market,
            order_id=order_id,
            mappings=[item.model_dump() for item in payload.mappings],
            default_source="manual",
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "ok": True,
        "marketplace": market,
        "order_id": order_id,
        "mappings": mappings,
    }


@router.post("/{marketplace}/{order_id}/invoice", dependencies=ADMIN_ONLY)
async def api_upload_invoice(
    marketplace: str,
    order_id: str,
    file: UploadFile = File(...),
    notes: Optional[str] = Form(default=None),
    purchase_cost_eur: Optional[float] = Form(default=None),
    purchase_currency: Optional[str] = Form(default=None),
    supplier_name: Optional[str] = Form(default=None),
) -> dict[str, Any]:
    market = _validate_marketplace(marketplace)
    upload_filename = sanitize_filename(file.filename or "invoice")
    order_token = _resolve_invoice_order_token(market, order_id)
    renamed_filename = _build_invoice_filename(market, order_token, upload_filename)
    purchase_cents = _to_cents(purchase_cost_eur) if purchase_cost_eur is not None else None

    target_path = build_invoice_storage_path(market, order_token, renamed_filename)
    try:
        stream_fileobj_to_path(file.file, target_path, max_bytes=MAX_UPLOAD_BYTES)
    except EmptyUploadError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="file is empty") from exc
    except UploadTooLargeError as exc:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="file too large") from exc

    row = create_invoice_document(
        marketplace=market,
        order_id=order_id,
        original_filename=renamed_filename,
        stored_filename=target_path.name,
        mime_type=file.content_type or "application/octet-stream",
        file_path=target_path,
    )

    existing_purchase = row.get("purchase_cost_cents") if isinstance(row.get("purchase_cost_cents"), int) else None
    existing_currency = str(row.get("purchase_currency") or "EUR")
    existing_supplier = str(row.get("supplier_name") or "").strip() or None
    existing_notes = str(row.get("purchase_notes") or "").strip() or None

    enrichment = upsert_purchase_enrichment(
        marketplace=market,
        order_id=order_id,
        purchase_cost_cents=purchase_cents if purchase_cost_eur is not None else existing_purchase,
        purchase_currency=purchase_currency if purchase_currency is not None else existing_currency,
        supplier_name=supplier_name if supplier_name is not None else existing_supplier,
        purchase_notes=notes if notes is not None else existing_notes,
    )

    booking_sync = sync_combined_orders_into_bookkeeping(marketplace=market, order_id=order_id)
    return {"ok": True, "enrichment": enrichment, "bookkeeping_sync": booking_sync}


@router.get("/{marketplace}/{order_id}/invoice/{document_id}/download")
def api_download_invoice(
    marketplace: str,
    order_id: str,
    document_id: str,
    disposition: Optional[str] = Query(default="attachment"),
) -> FileResponse:
    market = _validate_marketplace(marketplace)
    row = fetch_invoice_document(document_id)
    if row is None:
        raise HTTPException(status_code=404, detail="document not found")

    if row.get("marketplace") != market or row.get("order_id") != order_id:
        raise HTTPException(status_code=404, detail="document not found for order")

    file_path = resolve_invoice_path(str(row.get("file_path") or ""))
    if file_path is None:
        raise HTTPException(status_code=404, detail="file not found")

    download_name = sanitize_filename(str(row.get("original_filename") or file_path.name))
    requested_disposition = str(disposition or "attachment").strip().lower()
    content_disposition_type = "inline" if requested_disposition in {"inline", "preview"} else "attachment"
    return FileResponse(
        path=file_path,
        media_type=str(row.get("mime_type") or "application/octet-stream"),
        filename=download_name,
        content_disposition_type=content_disposition_type,
    )
