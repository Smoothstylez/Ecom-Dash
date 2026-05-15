from __future__ import annotations

import json
import logging
from html import escape
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
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
from app.routers.analytics import router as analytics_router
from app.routers.bookings import router as bookings_router
from app.routers.customers import router as customers_router
from app.routers.ebay import router as ebay_router
from app.routers.exports import router as exports_router
from app.routers.google_ads import router as google_ads_router
from app.routers.orders import router as orders_router
from app.routers.sync import router as sync_router
from app.services.live_sync import (
    build_live_sync_status,
    start_live_sync_background_worker,
    stop_live_sync_background_worker,
)
from app.services.runtime_reconcile import reconcile_runtime_state
from app.services.source_sync import build_sync_status, sync_all_sources


BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "app" / "static"
WORKSPACE_DIR = BASE_DIR.parent

# Frontend dist: check in-container path first, then workspace-relative (dev)
_FRONTEND_DIST_CONTAINER = BASE_DIR / "frontend_dist"
_FRONTEND_DIST_WORKSPACE = WORKSPACE_DIR / "frontend" / "dist"
FRONTEND_DIST_DIR = (
    _FRONTEND_DIST_CONTAINER if _FRONTEND_DIST_CONTAINER.is_dir() else _FRONTEND_DIST_WORKSPACE
)
LOGGER = logging.getLogger("combined_dashboard")


def _normalize_external_base_path(value: str | None) -> str:
    text = str(value or "").strip()
    if not text or text == "/":
        return ""
    if "://" in text:
        text = urlsplit(text).path or ""
    if not text:
        return ""
    if not text.startswith("/"):
        text = f"/{text}"
    return text[:-1] if text.endswith("/") else text


def _dashboard_external_base_path(request: Request) -> str:
    return _normalize_external_base_path(
        request.headers.get("x-ingress-path")
        or request.headers.get("x-forwarded-prefix")
        or request.scope.get("root_path")
    )


def _with_dashboard_external_base_path(request: Request, path: str) -> str:
    normalized_path = str(path or "").strip() or "/"
    if not normalized_path.startswith("/"):
        normalized_path = f"/{normalized_path}"
    base_path = _dashboard_external_base_path(request)
    if not base_path:
        return normalized_path
    if normalized_path == "/":
        return f"{base_path}/"
    if normalized_path == base_path or normalized_path.startswith(f"{base_path}/"):
        return normalized_path
    return f"{base_path}{normalized_path}"


def _rewrite_html_attribute_prefix(html: str, attribute: str, source_prefix: str, replacement_prefix: str) -> str:
    html = html.replace(f'{attribute}="{source_prefix}', f'{attribute}="{replacement_prefix}')
    return html.replace(f"{attribute}='{source_prefix}", f"{attribute}='{replacement_prefix}")


def _inject_dashboard_shell_bootstrap(request: Request, html: str) -> str:
    base_path = _dashboard_external_base_path(request)
    bootstrap = (
        f'    <base href="{escape(f"{base_path}/" if base_path else "/", quote=True)}">\n'
        f'    <script>window.__DASHBOARD_BASE_PATH__ = {json.dumps(base_path)};</script>\n'
    )
    if "window.__DASHBOARD_BASE_PATH__" in html or "<base " in html:
        return html
    return html.replace("<head>", f"<head>\n{bootstrap}", 1)


def _source_sync_has_mutation(summary: dict[str, object] | None) -> bool:
    if not isinstance(summary, dict):
        return False
    results_raw = summary.get("results")
    results = results_raw if isinstance(results_raw, dict) else {}
    for payload in results.values():
        if not isinstance(payload, dict):
            continue
        if bool(payload.get("copied")):
            return True
        if int(payload.get("copied_files") or 0) > 0:
            return True
    return False


app = FastAPI(title="E-Commerce Dashboard", version=APP_VERSION)

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
    """Bump the changestamp on successful mutating requests.

    Sync endpoints manage changestamp updates based on whether work actually
    changed data, so the generic middleware must stay out of their way.
    """
    response: Response = await call_next(request)
    path = request.url.path
    if (
        request.method not in ("GET", "HEAD", "OPTIONS")
        and not path.startswith("/api/sync")
        and 200 <= response.status_code < 300
    ):
        changestamp.bump()
    return response


@app.on_event("startup")
def on_startup() -> None:
    ensure_runtime_dirs()
    should_bump_source_sync = False
    if AUTO_SYNC_ON_STARTUP:
        try:
            sync_summary = sync_all_sources(
                force=False,
                include_documents=True,
                include_bookkeeping_bootstrap=True,
            )
            should_bump_source_sync = _source_sync_has_mutation(sync_summary)
            LOGGER.info("startup source sync finished: %s", sync_summary.get("timestamp"))
        except Exception as exc:  # pragma: no cover - startup robustness
            LOGGER.exception("startup source sync failed: %s", exc)
    reconcile_summary: dict[str, object] = {}
    try:
        reconcile_summary = reconcile_runtime_state()
        if not bool(reconcile_summary.get("ok")):
            LOGGER.warning("startup runtime reconcile finished with errors: %s", reconcile_summary.get("errors"))
    except Exception as exc:  # pragma: no cover - startup robustness
        LOGGER.exception("startup runtime reconcile failed: %s", exc)
    if should_bump_source_sync and not bool(reconcile_summary.get("changestamp_bumped")):
        changestamp.bump()
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


@app.on_event("shutdown")
def on_shutdown() -> None:
    try:
        stop_live_sync_background_worker(timeout_seconds=5.0)
    except Exception as exc:  # pragma: no cover - shutdown robustness
        LOGGER.exception("shutdown live sync background worker failed: %s", exc)


@app.get("/", include_in_schema=False)
def root(request: Request) -> Response:
    return _dashboard_shell_response(request)


@app.get("/bookings", include_in_schema=False)
@app.get("/bookings/full", include_in_schema=False)
@app.get("/orders", include_in_schema=False)
@app.get("/analytics", include_in_schema=False)
@app.get("/ebay", include_in_schema=False)
@app.get("/google-ads", include_in_schema=False)
@app.get("/customers", include_in_schema=False)
def dashboard_alias(request: Request) -> Response:
    return _dashboard_shell_response(request)


@app.get("/bookings/module", include_in_schema=False)
def deprecated_bookings_module(request: Request) -> RedirectResponse:
    return RedirectResponse(
        url=_with_dashboard_external_base_path(request, "/bookings/full?subtab=transactions"),
        status_code=307,
    )


def _dashboard_shell_response(request: Request) -> Response:
    index_path = FRONTEND_DIST_DIR / "index.html"
    if not index_path.is_file():
        return Response(
            content="Frontend not built. Run 'npm run build' in frontend/ directory.",
            media_type="text/plain",
            status_code=503,
        )
    html = index_path.read_text(encoding="utf-8")
    base_path = _dashboard_external_base_path(request)
    version_suffix = f"?v={APP_VERSION}"
    themes_path = _with_dashboard_external_base_path(request, f"/static/css/themes.css{version_suffix}")
    main_path = _with_dashboard_external_base_path(request, f"/static/css/main.css{version_suffix}")
    html = html.replace('href="/static/css/themes.css"', f'href="{themes_path}"')
    html = html.replace("href='/static/css/themes.css'", f"href='{themes_path}'")
    html = html.replace('href="/static/css/main.css"', f'href="{main_path}"')
    html = html.replace("href='/static/css/main.css'", f"href='{main_path}'")
    html = _rewrite_html_attribute_prefix(
        html,
        "src",
        "/assets/",
        _with_dashboard_external_base_path(request, "/assets/"),
    )
    html = _rewrite_html_attribute_prefix(
        html,
        "href",
        "/assets/",
        _with_dashboard_external_base_path(request, "/assets/"),
    )
    if base_path:
        html = _rewrite_html_attribute_prefix(
            html,
            "src",
            "/static/",
            f"{base_path}/static/",
        )
        html = _rewrite_html_attribute_prefix(
            html,
            "href",
            "/static/",
            f"{base_path}/static/",
        )
    html = _inject_dashboard_shell_bootstrap(request, html)
    return HTMLResponse(content=html)


@app.get("/app-preview", include_in_schema=False, response_model=None)
@app.head("/app-preview", include_in_schema=False, response_model=None)
@app.get("/app-preview/{frontend_path:path}", include_in_schema=False, response_model=None)
@app.head("/app-preview/{frontend_path:path}", include_in_schema=False, response_model=None)
def frontend_preview(request: Request, frontend_path: str = "") -> RedirectResponse:
    normalized = frontend_path.strip("/")
    target = "/analytics"
    if normalized:
        target = f"/{normalized}"
    query_string = str(request.url.query or "").strip()
    if query_string:
        separator = "&" if "?" in target else "?"
        target = f"{target}{separator}{query_string}"
    return RedirectResponse(url=_with_dashboard_external_base_path(request, target), status_code=307)


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
_frontend_assets_dir = FRONTEND_DIST_DIR / "assets"
if _frontend_assets_dir.is_dir():
    app.mount("/assets", StaticFiles(directory=_frontend_assets_dir), name="frontend-assets")
else:
    LOGGER.warning("Frontend assets directory not found: %s — /assets route disabled", _frontend_assets_dir)
