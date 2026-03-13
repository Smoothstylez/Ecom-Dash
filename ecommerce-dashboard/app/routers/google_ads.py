from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, File, HTTPException, Query, UploadFile

from app.services.google_ads import build_google_ads_analytics, build_google_ads_product_detail, import_google_ads_data, reset_google_ads_data
from app.services.bookings import sync_google_ads_into_bookkeeping


router = APIRouter(prefix="/api/google-ads", tags=["google-ads"])


@router.post("/upload")
async def api_google_ads_upload(
    file: Optional[UploadFile] = File(default=None),
    report_file: Optional[UploadFile] = File(default=None),
    assignment_file: Optional[UploadFile] = File(default=None),
) -> dict[str, Any]:
    selected_report = report_file or file
    selected_assignment = assignment_file

    report_content = await selected_report.read() if selected_report is not None else None
    assignment_content = await selected_assignment.read() if selected_assignment is not None else None

    if not report_content and not assignment_content:
        raise HTTPException(status_code=400, detail="Bitte mindestens eine CSV-Datei hochladen.")

    try:
        result = import_google_ads_data(
            report_content=report_content,
            report_filename=selected_report.filename if selected_report is not None else None,
            assignment_content=assignment_content,
            assignment_filename=selected_assignment.filename if selected_assignment is not None else None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Google Ads Import fehlgeschlagen: {exc}") from exc

    # Sync imported daily costs into bookkeeping transactions
    bookkeeping_sync = sync_google_ads_into_bookkeeping()
    result["bookkeeping_sync"] = bookkeeping_sync

    return {"ok": True, **result}


@router.get("/analytics")
def api_google_ads_analytics(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
    q: Optional[str] = Query(default=None),
) -> dict[str, Any]:
    try:
        return build_google_ads_analytics(from_date=from_date, to_date=to_date, query=q)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Google Ads Analytics fehlgeschlagen: {exc}") from exc


@router.get("/product-detail")
def api_google_ads_product_detail(
    product_key: str = Query(..., alias="product_key"),
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
) -> dict[str, Any]:
    try:
        return build_google_ads_product_detail(
            product_key=product_key,
            from_date=from_date,
            to_date=to_date,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Google Ads Produkt-Detail fehlgeschlagen: {exc}") from exc


@router.delete("/reset")
def api_google_ads_reset() -> dict[str, Any]:
    try:
        result = reset_google_ads_data()
        bookkeeping_sync = sync_google_ads_into_bookkeeping()
        result["bookkeeping_sync"] = bookkeeping_sync
        return {"ok": True, **result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Google Ads Reset fehlgeschlagen: {exc}") from exc
