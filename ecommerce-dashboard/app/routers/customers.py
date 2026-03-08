from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Query

from app.services.customers import build_customer_locations_map, build_customers_overview


router = APIRouter(prefix="/api/customers", tags=["customers"])


@router.get("")
def api_list_customers(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
    marketplace: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    status_filter: Optional[str] = Query(default=None, alias="status"),
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    return build_customers_overview(
        from_date=from_date,
        to_date=to_date,
        marketplace=marketplace,
        query=q,
        status_filter=status_filter,
        limit=limit,
        offset=offset,
    )


@router.get("/locations")
def api_customer_locations(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
    marketplace: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    status_filter: Optional[str] = Query(default=None, alias="status"),
    refresh: bool = Query(default=False),
) -> dict[str, Any]:
    return build_customer_locations_map(
        from_date=from_date,
        to_date=to_date,
        marketplace=marketplace,
        query=q,
        status_filter=status_filter,
        force_refresh=refresh,
    )
