from __future__ import annotations

import logging
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from app import changestamp

from app.config import (
    APP_VERSION,
    AUTO_SYNC_ON_STARTUP,
    BOOKKEEPING_DB_PATH,
    COMBINED_DB_PATH,
    KAUFLAND_DB_PATH,
    SHOPIFY_DB_PATH,
    ensure_runtime_dirs,
)
from app.db import init_combined_db
from app.routers.analytics import router as analytics_router
from app.routers.bookings import router as bookings_router
from app.routers.customers import router as customers_router
from app.routers.ebay import router as ebay_router
from app.routers.exports import router as exports_router
from app.routers.google_ads import router as google_ads_router
from app.routers.orders import router as orders_router
from app.routers.sync import router as sync_router
from app.services.bookings import sync_combined_orders_into_bookkeeping, sync_google_ads_into_bookkeeping, migrate_bookkeeping_document_paths
from app.services.live_sync import (
    build_live_sync_status,
    start_live_sync_background_worker,
    stop_live_sync_background_worker,
)
from app.services.source_sync import build_sync_status, sync_all_sources


BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "app" / "static"
LOGGER = logging.getLogger("combined_dashboard")


app = FastAPI(title="Combined Dropshipping Dashboard", version=APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(orders_router)
app.include_router(analytics_router)
app.include_router(customers_router)
app.include_router(bookings_router)
app.include_router(ebay_router)
app.include_router(exports_router)
app.include_router(google_ads_router)
app.include_router(sync_router)


@app.middleware("http")
async def changestamp_middleware(request: Request, call_next) -> Response:
    """Bump the changestamp on any successful mutating request."""
    response: Response = await call_next(request)
    if request.method not in ("GET", "HEAD", "OPTIONS") and 200 <= response.status_code < 300:
        changestamp.bump()
    return response


@app.on_event("startup")
def on_startup() -> None:
    ensure_runtime_dirs()
    if AUTO_SYNC_ON_STARTUP:
        try:
            sync_summary = sync_all_sources(force=False, include_documents=True)
            LOGGER.info("startup source sync finished: %s", sync_summary.get("timestamp"))
        except Exception as exc:  # pragma: no cover - startup robustness
            LOGGER.exception("startup source sync failed: %s", exc)
    init_combined_db()
    try:
        migrated = migrate_bookkeeping_document_paths()
        if migrated:
            LOGGER.info("migrated %d bookkeeping document paths to relative", migrated)
    except Exception as exc:  # pragma: no cover - startup robustness
        LOGGER.exception("bookkeeping document path migration failed: %s", exc)
    try:
        order_sync = sync_combined_orders_into_bookkeeping()
        LOGGER.info(
            "startup bookkeeping order sync: selected=%s inserted=%s updated=%s",
            order_sync.get("selected_orders"),
            order_sync.get("orders_inserted"),
            order_sync.get("orders_updated"),
        )
    except Exception as exc:  # pragma: no cover - startup robustness
        LOGGER.exception("startup bookkeeping order sync failed: %s", exc)
    try:
        gads_sync = sync_google_ads_into_bookkeeping()
        LOGGER.info(
            "startup google ads bookkeeping sync: days=%s inserted=%s updated=%s deleted=%s",
            gads_sync.get("days_total"),
            gads_sync.get("transactions_inserted"),
            gads_sync.get("transactions_updated"),
            gads_sync.get("transactions_deleted"),
        )
    except Exception as exc:  # pragma: no cover - startup robustness
        LOGGER.exception("startup google ads bookkeeping sync failed: %s", exc)
    try:
        background_status = start_live_sync_background_worker()
        LOGGER.info(
            "live sync background worker: enabled=%s running=%s interval=%ss",
            background_status.get("enabled"),
            background_status.get("thread_alive"),
            background_status.get("interval_seconds"),
        )
    except Exception as exc:  # pragma: no cover - startup robustness
        LOGGER.exception("startup live sync background worker failed: %s", exc)
    STATIC_DIR.mkdir(parents=True, exist_ok=True)


@app.on_event("shutdown")
def on_shutdown() -> None:
    try:
        stop_live_sync_background_worker(timeout_seconds=5.0)
    except Exception as exc:  # pragma: no cover - shutdown robustness
        LOGGER.exception("shutdown live sync background worker failed: %s", exc)


@app.get("/", include_in_schema=False)
def root() -> FileResponse:
    return FileResponse(STATIC_DIR / "dashboard.html")


@app.get("/bookings", include_in_schema=False)
@app.get("/bookings/full", include_in_schema=False)
@app.get("/orders", include_in_schema=False)
@app.get("/analytics", include_in_schema=False)
@app.get("/ebay", include_in_schema=False)
@app.get("/google-ads", include_in_schema=False)
@app.get("/customers", include_in_schema=False)
def dashboard_alias() -> FileResponse:
    return FileResponse(STATIC_DIR / "dashboard.html")


@app.get("/bookings/module", include_in_schema=False)
def deprecated_bookings_module() -> RedirectResponse:
    return RedirectResponse(url="/bookings/full?subtab=transactions", status_code=307)


@app.get("/api/health")
def health() -> dict[str, object]:
    sync_status = build_sync_status()
    live_sync_status = build_live_sync_status()
    integrated_bookkeeping_status = {
        "enabled": True,
        "running": BOOKKEEPING_DB_PATH.exists(),
        "mode": "integrated",
        "managed": False,
        "pid": None,
        "database_path": str(BOOKKEEPING_DB_PATH),
        "database_exists": BOOKKEEPING_DB_PATH.exists(),
        "last_error": None,
        "last_mode": "integrated",
    }
    return {
        "status": "ok",
        "combined_db": {
            "path": str(COMBINED_DB_PATH),
            "exists": COMBINED_DB_PATH.exists(),
        },
        "source_databases": {
            "shopify": {"path": str(SHOPIFY_DB_PATH), "exists": SHOPIFY_DB_PATH.exists()},
            "kaufland": {"path": str(KAUFLAND_DB_PATH), "exists": KAUFLAND_DB_PATH.exists()},
            "bookkeeping": {"path": str(BOOKKEEPING_DB_PATH), "exists": BOOKKEEPING_DB_PATH.exists()},
        },
        "sync_status": sync_status,
        "live_sync_status": live_sync_status,
        "bookkeeping_module": integrated_bookkeeping_status,
        "legacy_bookkeeping": integrated_bookkeeping_status,
    }


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
