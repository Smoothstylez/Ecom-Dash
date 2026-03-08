"""eBay legacy data router — read-only endpoints for imported eBay orders."""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Query

from app.services.ebay import get_ebay_orders, get_ebay_summary


router = APIRouter(prefix="/api/ebay", tags=["ebay"])


@router.get("/orders")
def api_ebay_orders(
    shop: Optional[str] = Query(default=None),
    category: Optional[str] = Query(default=None),
    include_returns: bool = Query(default=True, alias="includeReturns"),
) -> dict[str, Any]:
    orders = get_ebay_orders(shop=shop, category=category, include_returns=include_returns)
    return {"orders": orders, "total": len(orders)}


@router.get("/summary")
def api_ebay_summary() -> dict[str, Any]:
    return get_ebay_summary()
