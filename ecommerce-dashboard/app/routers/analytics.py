from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Query

from app.services.analytics import build_analytics


router = APIRouter(prefix="/api/analytics", tags=["analytics"])


@router.get("/kpis")
def api_kpis(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
    marketplace: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    trend_granularity: Optional[str] = Query(default=None, alias="trendGranularity"),
) -> dict[str, Any]:
    return build_analytics(
        from_date=from_date,
        to_date=to_date,
        marketplace=marketplace,
        query=q,
        trend_granularity=trend_granularity,
    )
