from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from app import changestamp
from app.config import PROJECT_ROOT
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


class CredentialsSaveRequest(BaseModel):
    shopify_domain: Optional[str] = Field(default=None)
    shopify_client_id: Optional[str] = Field(default=None)
    shopify_client_secret: Optional[str] = Field(default=None)
    shopify_api_version: Optional[str] = Field(default="2025-01")
    kaufland_client_key: Optional[str] = Field(default=None)
    kaufland_secret_key: Optional[str] = Field(default=None)


@router.post("/credentials")
def api_save_credentials(payload: CredentialsSaveRequest) -> dict[str, Any]:
    creds_file = PROJECT_ROOT / "data" / "credentials.json"
    creds_file.parent.mkdir(parents=True, exist_ok=True)
    
    creds = {}
    if creds_file.exists():
        try:
            creds = json.loads(creds_file.read_text())
        except Exception:
            pass
    
    if payload.shopify_domain:
        creds["SHOPIFY_SHOP_DOMAIN"] = payload.shopify_domain
    if payload.shopify_client_id:
        creds["SHOPIFY_CLIENT_ID"] = payload.shopify_client_id
    if payload.shopify_client_secret:
        creds["SHOPIFY_CLIENT_SECRET"] = payload.shopify_client_secret
    if payload.shopify_api_version:
        creds["SHOPIFY_API_VERSION"] = payload.shopify_api_version
    if payload.kaufland_client_key:
        creds["SHOP_CLIENT_KEY"] = payload.kaufland_client_key
    if payload.kaufland_secret_key:
        creds["SHOP_SECRET_KEY"] = payload.kaufland_secret_key
    
    creds_file.write_text(json.dumps(creds, indent=2))
    
    return {"ok": True, "message": "Credentials gespeichert"}


@router.get("/credentials")
def api_get_credentials() -> dict[str, Any]:
    creds_file = PROJECT_ROOT / "data" / "credentials.json"
    
    if not creds_file.exists():
        return {"ok": True, "has_credentials": False}
    
    try:
        creds = json.loads(creds_file.read_text())
        has_shopify = bool(creds.get("SHOPIFY_SHOP_DOMAIN"))
        has_kaufland = bool(creds.get("SHOP_CLIENT_KEY"))
        return {
            "ok": True,
            "has_credentials": has_shopify or has_kaufland,
            "shopify_configured": has_shopify,
            "kaufland_configured": has_kaufland,
        }
    except Exception:
        return {"ok": True, "has_credentials": False}
