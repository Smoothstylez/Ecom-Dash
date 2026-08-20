from __future__ import annotations

import json
import uuid
from typing import Any, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from pydantic import BaseModel, Field

from app.auth import require_admin_access
from app.config import BOOKKEEPING_DOCUMENTS_DIR, MAX_UPLOAD_BYTES
from app.db import sanitize_filename
from app.services.amazon_fba import (
    add_supplier_invoice,
    add_inbound_cost,
    add_inbound_invoice,
    add_inbound_invoice_line,
    allocate_order_fifo,
    assign_inbound_cost,
    confirm_inbound_product_costs,
    create_inventory_lot,
    create_procurement_batch,
    get_amazon_finance_overview,
    get_amazon_inventory_summary,
    get_amazon_sku_detail,
    get_inbound_shipment,
    list_amazon_sku_inventory,
    list_inbound_shipments,
    list_inbound_costs,
    list_procurement_batches,
    list_settlement_suggestions,
)
from app.uploads import EmptyUploadError, UploadTooLargeError, stream_fileobj_to_path
from app.services.importers.amazon_sp_api import (
    AmazonSpApiError,
    _as_dict,
    _connect,
    build_amazon_fba_status,
    get_amazon_marketplace_settings,
    import_settlement_report,
    request_settlement_report,
    set_amazon_marketplace_settings,
    sync_amazon_fba,
)
from app.services.amazon_auto_refresh import (
    AmazonAutoRefreshBusyError,
    get_amazon_auto_refresh_status,
    run_manual_amazon_sync,
    trigger_amazon_auto_refresh_now,
)


router = APIRouter(prefix="/api/amazon", tags=["amazon"])
ADMIN_ONLY = [Depends(require_admin_access)]


class AmazonSyncRequest(BaseModel):
    include_orders: bool = True
    include_inventory: bool = True
    include_finances: bool = True
    include_inbound: bool = True
    include_all_marketplaces: bool = False
    lookback_days: int = Field(default=30, ge=1, le=730)


class AmazonAutoRefreshTriggerRequest(BaseModel):
    reason: str = "api"


class AmazonMarketplaceSettingsRequest(BaseModel):
    marketplace_mode: str
    selected_marketplace_ids: list[str] = Field(default_factory=list)


class SettlementReportRequest(BaseModel):
    lookback_days: int = Field(default=730, ge=1, le=730)


class ProcurementLineRequest(BaseModel):
    seller_sku: str = ""
    fnsku: str = ""
    asin: str = ""
    title: str = ""
    quantity: int = Field(ge=1)
    allocation_basis: str = "value"


class ProcurementBatchRequest(BaseModel):
    reference: str = Field(min_length=1)
    name: str = Field(min_length=1)
    lines: list[ProcurementLineRequest] = Field(min_length=1)
    received_at: Optional[str] = None
    notes: str = ""


class SupplierInvoiceRequest(BaseModel):
    supplier_name: str = Field(min_length=1)
    invoice_number: str = ""
    invoice_date: Optional[str] = None
    currency: str = "EUR"
    gross_cents: int = Field(default=0, ge=0)
    net_cents: int = Field(default=0, ge=0)
    vat_cents: int = Field(default=0, ge=0)
    document_path: str = ""
    notes: str = ""
    input_vat_status: str = "review_required"


class InventoryLotRequest(BaseModel):
    unit_cost_cents: int = Field(ge=0)
    received_at: str = Field(min_length=1)
    quantity: Optional[int] = Field(default=None, ge=1)


class InboundCostRequest(BaseModel):
    cost_type: str = Field(min_length=1)
    amount_cents: int = Field(ge=0)
    currency: str = "EUR"
    allocation_method: str = "value"
    notes: str = ""


class InboundInvoiceLineRequest(BaseModel):
    seller_sku: str = ""
    fnsku: str = ""
    asin: str = ""
    title: str = ""
    quantity: int = Field(ge=1)
    net_cents: int = Field(ge=0)
    vat_cents: int = Field(default=0, ge=0)


@router.get("/status")
def api_amazon_status() -> dict[str, Any]:
    return {"ok": True, **build_amazon_fba_status(), "auto_refresh": get_amazon_auto_refresh_status()}


@router.get("/marketplace-settings")
def api_get_amazon_marketplace_settings() -> dict[str, Any]:
    settings = get_amazon_marketplace_settings()
    with _connect() as connection:
        rows = connection.execute(
            "SELECT marketplace_id, name, country_code, domain_name, participation_json FROM amazon_marketplaces ORDER BY name"
        ).fetchall()
    marketplaces = []
    for row in rows:
        try:
            participation = json.loads(str(row["participation_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            participation = {}
        is_participating = bool(_as_dict(participation.get("participation")).get("isParticipating")) if isinstance(participation, dict) else False
        marketplaces.append({
            "marketplace_id": row["marketplace_id"],
            "name": row["name"],
            "country_code": row["country_code"],
            "domain_name": row["domain_name"],
            "is_participating": is_participating,
        })
    return {"ok": True, **settings, "marketplaces": marketplaces}


@router.post("/marketplace-settings", dependencies=ADMIN_ONLY)
def api_set_amazon_marketplace_settings(payload: AmazonMarketplaceSettingsRequest) -> dict[str, Any]:
    with _connect() as connection:
        known_ids = {row["marketplace_id"] for row in connection.execute("SELECT marketplace_id FROM amazon_marketplaces").fetchall()}
    unknown = [m for m in payload.selected_marketplace_ids if m not in known_ids]
    if unknown:
        raise HTTPException(status_code=400, detail=f"Unknown marketplace_id(s): {unknown}")
    try:
        result = set_amazon_marketplace_settings(
            marketplace_mode=payload.marketplace_mode,
            selected_marketplace_ids=payload.selected_marketplace_ids,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, **result}


@router.post("/sync", dependencies=ADMIN_ONLY)
def api_amazon_sync(payload: Optional[AmazonSyncRequest] = None) -> dict[str, Any]:
    request = payload or AmazonSyncRequest()
    try:
        return run_manual_amazon_sync(
            include_orders=request.include_orders,
            include_inventory=request.include_inventory,
            include_finances=request.include_finances,
            include_inbound=request.include_inbound,
            include_all_marketplaces=request.include_all_marketplaces,
            lookback_days=request.lookback_days,
        )
    except AmazonAutoRefreshBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/auto-refresh/trigger", dependencies=ADMIN_ONLY)
def api_trigger_amazon_auto_refresh(payload: Optional[AmazonAutoRefreshTriggerRequest] = None) -> dict[str, Any]:
    request = payload or AmazonAutoRefreshTriggerRequest()
    return {"ok": True, "auto_refresh": trigger_amazon_auto_refresh_now(request.reason)}


@router.post("/reports/settlement", dependencies=ADMIN_ONLY)
def api_request_settlement_report(payload: Optional[SettlementReportRequest] = None) -> dict[str, Any]:
    request = payload or SettlementReportRequest()
    try:
        return request_settlement_report(lookback_days=request.lookback_days)
    except AmazonSpApiError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/reports/settlement/{report_id}/import", dependencies=ADMIN_ONLY)
def api_import_settlement_report(report_id: str) -> dict[str, Any]:
    try:
        return import_settlement_report(report_id)
    except AmazonSpApiError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/inventory")
def api_amazon_inventory() -> dict[str, Any]:
    return {"ok": True, **get_amazon_inventory_summary()}


@router.get("/inventory/skus")
def api_amazon_sku_inventory() -> dict[str, Any]:
    return {"ok": True, "items": list_amazon_sku_inventory()}


@router.get("/inventory/skus/{sku_key}")
def api_amazon_sku_detail(sku_key: str) -> dict[str, Any]:
    detail = get_amazon_sku_detail(sku_key)
    if detail is None:
        raise HTTPException(status_code=404, detail="SKU not found")
    return {"ok": True, **detail}


@router.get("/finance")
def api_amazon_finance() -> dict[str, Any]:
    return {"ok": True, **get_amazon_finance_overview()}


@router.get("/inbound/shipments")
def api_amazon_inbound_shipments(status: Optional[str] = None) -> dict[str, Any]:
    return {"ok": True, "items": list_inbound_shipments(status=status)}


@router.get("/inbound/shipments/{shipment_id}")
def api_amazon_inbound_shipment(shipment_id: str) -> dict[str, Any]:
    shipment = get_inbound_shipment(shipment_id)
    if shipment is None:
        raise HTTPException(status_code=404, detail="Amazon FBA shipment not found")
    return {"ok": True, **shipment}


@router.get("/inbound/costs")
def api_amazon_inbound_costs(shipment_id: Optional[str] = None) -> dict[str, Any]:
    return {"ok": True, "items": list_inbound_costs(shipment_id=shipment_id)}


@router.post("/inbound/shipments/{shipment_id}/costs", dependencies=ADMIN_ONLY)
def api_add_amazon_inbound_cost(shipment_id: str, payload: InboundCostRequest) -> dict[str, Any]:
    try:
        cost = add_inbound_cost(shipment_id=shipment_id, **payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "cost": cost}


@router.post("/inbound/costs/{cost_id}/confirm", dependencies=ADMIN_ONLY)
def api_confirm_amazon_inbound_cost(cost_id: str, shipment_id: str) -> dict[str, Any]:
    try:
        cost = assign_inbound_cost(cost_id=cost_id, shipment_id=shipment_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "cost": cost}


@router.post("/inbound/shipments/{shipment_id}/invoices", dependencies=ADMIN_ONLY)
async def api_upload_amazon_inbound_invoice(
    shipment_id: str,
    file: UploadFile = File(...),
    supplier_name: str = Form(...),
    invoice_number: str = Form(default=""),
    invoice_date: Optional[str] = Form(default=None),
    currency: str = Form(default="EUR"),
    gross_cents: int = Form(default=0),
    net_cents: int = Form(default=0),
    vat_cents: int = Form(default=0),
    notes: str = Form(default=""),
) -> dict[str, Any]:
    folder = BOOKKEEPING_DOCUMENTS_DIR / "amazon-fba" / sanitize_filename(shipment_id)
    filename = sanitize_filename(file.filename or "invoice")
    target_path = folder / filename
    if target_path.exists():
        target_path = folder / f"{target_path.stem}-{uuid.uuid4().hex[:8]}{target_path.suffix}"
    try:
        stream_fileobj_to_path(file.file, target_path, max_bytes=MAX_UPLOAD_BYTES)
    except EmptyUploadError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="file is empty") from exc
    except UploadTooLargeError as exc:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="file too large") from exc
    try:
        invoice = add_inbound_invoice(
            shipment_id=shipment_id,
            supplier_name=supplier_name,
            invoice_number=invoice_number,
            invoice_date=invoice_date,
            currency=currency,
            gross_cents=gross_cents,
            net_cents=net_cents,
            vat_cents=vat_cents,
            document_path=str(target_path),
            notes=notes,
        )
    except ValueError as exc:
        target_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "invoice": invoice}


@router.post("/inbound/invoices/{invoice_id}/lines", dependencies=ADMIN_ONLY)
def api_add_amazon_inbound_invoice_line(invoice_id: str, payload: InboundInvoiceLineRequest) -> dict[str, Any]:
    try:
        line = add_inbound_invoice_line(invoice_id=invoice_id, **payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "line": line}


@router.post("/inbound/shipments/{shipment_id}/cost-confirmation", dependencies=ADMIN_ONLY)
def api_confirm_amazon_inbound_product_costs(shipment_id: str) -> dict[str, Any]:
    try:
        result = confirm_inbound_product_costs(shipment_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, **result}


@router.get("/settlement-suggestions", dependencies=ADMIN_ONLY)
def api_amazon_settlement_suggestions() -> dict[str, Any]:
    return {"ok": True, "items": list_settlement_suggestions()}


@router.get("/procurement/batches", dependencies=ADMIN_ONLY)
def api_list_procurement_batches() -> dict[str, Any]:
    return {"ok": True, "items": list_procurement_batches()}


@router.post("/procurement/batches", dependencies=ADMIN_ONLY)
def api_create_procurement_batch(payload: ProcurementBatchRequest) -> dict[str, Any]:
    try:
        batch = create_procurement_batch(
            reference=payload.reference,
            name=payload.name,
            lines=[line.model_dump() for line in payload.lines],
            received_at=payload.received_at,
            notes=payload.notes,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "batch": batch}


@router.post("/procurement/batches/{batch_id}/invoices", dependencies=ADMIN_ONLY)
def api_add_supplier_invoice(batch_id: str, payload: SupplierInvoiceRequest) -> dict[str, Any]:
    try:
        invoice = add_supplier_invoice(batch_id=batch_id, **payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "invoice": invoice}


@router.post("/procurement/lines/{batch_line_id}/lots", dependencies=ADMIN_ONLY)
def api_create_inventory_lot(batch_line_id: str, payload: InventoryLotRequest) -> dict[str, Any]:
    try:
        lot = create_inventory_lot(batch_line_id=batch_line_id, **payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "lot": lot}


@router.post("/orders/{order_id}/fifo", dependencies=ADMIN_ONLY)
def api_allocate_order_fifo(order_id: str) -> dict[str, Any]:
    try:
        result = allocate_order_fifo(order_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, **result}
