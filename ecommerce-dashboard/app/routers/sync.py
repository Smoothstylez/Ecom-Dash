from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app import changestamp
from app.services.bookings import sync_combined_orders_into_bookkeeping
from app.services.live_sync import (
    build_live_sync_background_status,
    build_live_sync_status,
    run_live_sync,
    trigger_live_sync_background_now,
)
from app.services.source_sync import build_sync_status, sync_all_sources


router = APIRouter(prefix="/api/sync", tags=["sync"])


class SyncRunRequest(BaseModel):
    force: Optional[bool] = Field(default=False)
    include_documents: Optional[bool] = Field(default=True)


class LiveSyncRunRequest(BaseModel):
    shopify: Optional[bool] = Field(default=True)
    kaufland: Optional[bool] = Field(default=True)

    shopify_status: Optional[str] = Field(default="any")
    shopify_page_limit: Optional[int] = Field(default=250, ge=1, le=250)
    shopify_max_pages: Optional[int] = Field(default=500, ge=1, le=20000)
    shopify_include_line_items: Optional[bool] = Field(default=True)
    shopify_include_fulfillments: Optional[bool] = Field(default=True)
    shopify_include_refunds: Optional[bool] = Field(default=True)
    shopify_include_transactions: Optional[bool] = Field(default=True)

    kaufland_storefront: Optional[str] = Field(default="de")
    kaufland_page_limit: Optional[int] = Field(default=100, ge=1, le=200)
    kaufland_max_pages: Optional[int] = Field(default=5000, ge=1, le=200000)
    kaufland_include_returns: Optional[bool] = Field(default=True)
    kaufland_include_order_unit_details: Optional[bool] = Field(default=True)


class LiveSyncTriggerRequest(BaseModel):
    reason: Optional[str] = Field(default="api")


@router.get("/changestamp")
def api_sync_changestamp() -> dict[str, Any]:
    """Return the current changestamp for cross-device polling."""
    return {"stamp": changestamp.get()}


@router.get("/status")
def api_sync_status() -> dict[str, Any]:
    return build_sync_status()


@router.post("/run")
def api_sync_run(payload: Optional[SyncRunRequest] = None) -> dict[str, Any]:
    request = payload or SyncRunRequest()
    result = sync_all_sources(
        force=bool(request.force),
        include_documents=bool(request.include_documents),
    )
    result["bookkeeping_order_sync"] = sync_combined_orders_into_bookkeeping()
    return result


@router.get("/live/status")
def api_sync_live_status() -> dict[str, Any]:
    return build_live_sync_status()


@router.get("/live/background/status")
def api_sync_live_background_status() -> dict[str, Any]:
    return build_live_sync_background_status()


@router.post("/live/background/trigger")
def api_sync_live_background_trigger(payload: Optional[LiveSyncTriggerRequest] = None) -> dict[str, Any]:
    request = payload or LiveSyncTriggerRequest()
    background = trigger_live_sync_background_now(reason=str(request.reason or "api"))
    return {
        "ok": True,
        "timestamp": build_live_sync_status().get("timestamp"),
        "background": background,
    }


@router.post("/live/run")
def api_sync_live_run(payload: Optional[LiveSyncRunRequest] = None) -> dict[str, Any]:
    request = payload or LiveSyncRunRequest()
    result = run_live_sync(
        run_shopify=bool(request.shopify),
        run_kaufland=bool(request.kaufland),
        shopify_status=str(request.shopify_status or "any"),
        shopify_page_limit=int(request.shopify_page_limit or 250),
        shopify_max_pages=int(request.shopify_max_pages or 500),
        shopify_include_line_items=bool(request.shopify_include_line_items),
        shopify_include_fulfillments=bool(request.shopify_include_fulfillments),
        shopify_include_refunds=bool(request.shopify_include_refunds),
        shopify_include_transactions=bool(request.shopify_include_transactions),
        kaufland_storefront=str(request.kaufland_storefront or "de"),
        kaufland_page_limit=int(request.kaufland_page_limit or 100),
        kaufland_max_pages=int(request.kaufland_max_pages or 5000),
        kaufland_include_returns=bool(request.kaufland_include_returns),
        kaufland_include_order_unit_details=bool(request.kaufland_include_order_unit_details),
    )
    result["bookkeeping_order_sync"] = sync_combined_orders_into_bookkeeping()
    return result
