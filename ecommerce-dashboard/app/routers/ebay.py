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
    limit: int = Query(default=150, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    payload = get_ebay_orders(
        shop=shop,
        category=category,
        include_returns=include_returns,
        limit=limit,
        offset=offset,
    )
    return {"orders": payload["items"], "total": payload["total"], "limit": limit, "offset": offset}


@router.get("/summary")
def api_ebay_summary() -> dict[str, Any]:
    return get_ebay_summary()
