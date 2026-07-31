from __future__ import annotations

import os
from typing import Any, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app import changestamp
from app.auth import require_admin_access
from app.services.bookings import sync_combined_orders_into_bookkeeping
from app.services.live_sync import (
    build_live_sync_background_status,
    build_live_sync_status,
    run_live_sync,
    trigger_live_sync_background_now,
)
from app.services.source_sync import build_sync_status, sync_all_sources
from app.services.importers.amazon_sp_api import build_amazon_fba_status


router = APIRouter(prefix="/api/sync", tags=["sync"])
ADMIN_ONLY = [Depends(require_admin_access)]


def _source_sync_has_mutation(summary: dict[str, Any]) -> bool:
    results_raw = summary.get("results") if isinstance(summary, dict) else None
    results = results_raw if isinstance(results_raw, dict) else {}
    for payload in results.values():
        if not isinstance(payload, dict):
            continue
        if bool(payload.get("copied")):
            return True
        if int(payload.get("copied_files") or 0) > 0:
            return True
    return False


def _bookkeeping_sync_has_mutation(summary: dict[str, Any]) -> bool:
    if not isinstance(summary, dict):
        return False
    for key in (
        "orders_inserted",
        "orders_updated",
        "transactions_inserted",
        "transactions_updated",
        "transactions_deleted",
        "documents_inserted",
        "documents_updated",
    ):
        if int(summary.get(key) or 0) > 0:
            return True
    return False


def _live_sync_has_mutation(summary: dict[str, Any]) -> bool:
    results_raw = summary.get("results") if isinstance(summary, dict) else None
    results = results_raw if isinstance(results_raw, dict) else {}
    for payload in results.values():
        if not isinstance(payload, dict):
            continue
        provider_summary_raw = payload.get("summary")
        provider_summary = provider_summary_raw if isinstance(provider_summary_raw, dict) else {}
        for key in (
            "total_inserted",
            "total_updated",
            "orders_saved",
            "order_units_saved",
            "returns_saved",
            "return_units_saved",
        ):
            if int(provider_summary.get(key) or 0) > 0:
                return True
    return False


class SyncRunRequest(BaseModel):
    force: Optional[bool] = Field(default=False)
    include_documents: Optional[bool] = Field(default=True)
    bookkeeping_bootstrap: Optional[bool] = Field(default=False)


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


@router.post("/run", dependencies=ADMIN_ONLY)
def api_sync_run(payload: Optional[SyncRunRequest] = None) -> dict[str, Any]:
    request = payload or SyncRunRequest()
    result = sync_all_sources(
        force=bool(request.force),
        include_documents=bool(request.include_documents),
        include_bookkeeping_bootstrap=bool(request.bookkeeping_bootstrap),
    )
    result["bookkeeping_order_sync"] = sync_combined_orders_into_bookkeeping()
    if _source_sync_has_mutation(result) or _bookkeeping_sync_has_mutation(result["bookkeeping_order_sync"]):
        changestamp.bump()
    return result


@router.get("/live/status")
def api_sync_live_status() -> dict[str, Any]:
    return build_live_sync_status()


@router.get("/live/background/status")
def api_sync_live_background_status() -> dict[str, Any]:
    return build_live_sync_background_status()


@router.post("/live/background/trigger", dependencies=ADMIN_ONLY)
def api_sync_live_background_trigger(payload: Optional[LiveSyncTriggerRequest] = None) -> dict[str, Any]:
    request = payload or LiveSyncTriggerRequest()
    background = trigger_live_sync_background_now(reason=str(request.reason or "api"))
    return {
        "ok": True,
        "timestamp": build_live_sync_status().get("timestamp"),
        "background": background,
    }


@router.post("/live/run", dependencies=ADMIN_ONLY)
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
    if _live_sync_has_mutation(result) or _bookkeeping_sync_has_mutation(result["bookkeeping_order_sync"]):
        changestamp.bump()
    return result


@router.get("/credentials")
def api_get_credentials() -> dict[str, Any]:
    shopify_configured = bool(os.getenv("SHOPIFY_SHOP_DOMAIN") and os.getenv("SHOPIFY_CLIENT_ID") and os.getenv("SHOPIFY_CLIENT_SECRET"))
    kaufland_configured = bool(os.getenv("SHOP_CLIENT_KEY") and os.getenv("SHOP_SECRET_KEY"))
    amazon_status = build_amazon_fba_status()
    amazon_configured = bool(amazon_status.get("configured"))
    return {
        "ok": True,
        "has_credentials": shopify_configured or kaufland_configured or amazon_configured,
        "shopify_configured": shopify_configured,
        "kaufland_configured": kaufland_configured,
        "amazon_configured": amazon_configured,
        "storage": "environment and local ignored secret file",
    }
