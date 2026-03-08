from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
WORKSPACE_ROOT = PROJECT_ROOT.parent

DATA_DIR = PROJECT_ROOT / "data"
STORAGE_DIR = PROJECT_ROOT / "storage"
INVOICES_DIR = STORAGE_DIR / "invoices"
BOOKKEEPING_DOCUMENTS_DIR = STORAGE_DIR / "documents"
SOURCES_DIR = DATA_DIR / "sources"
SHOPIFY_SOURCE_DIR = SOURCES_DIR / "shopify"
KAUFLAND_SOURCE_DIR = SOURCES_DIR / "kaufland"
BOOKKEEPING_SOURCE_DIR = SOURCES_DIR / "bookkeeping"


def _resolve_env_path(env_name: str, default_path: Path) -> Path:
    resolved = Path(os.getenv(env_name, default_path)).expanduser()
    if not resolved.is_absolute():
        resolved = (PROJECT_ROOT / resolved).resolve()
    return resolved


def _env_flag(env_name: str, default: str = "1") -> bool:
    raw = str(os.getenv(env_name, default)).strip().lower()
    return raw in {"1", "true", "yes", "on"}


COMBINED_DB_PATH = _resolve_env_path("COMBINED_DB_PATH", DATA_DIR / "combined.sqlite3")

# Local runtime source DBs used by the app.
SHOPIFY_DB_PATH = _resolve_env_path("SHOPIFY_DB_PATH", SHOPIFY_SOURCE_DIR / "shopify_data.sqlite3")
KAUFLAND_DB_PATH = _resolve_env_path("KAUFLAND_DB_PATH", KAUFLAND_SOURCE_DIR / "kaufland_data.sqlite3")
BOOKKEEPING_DB_PATH = _resolve_env_path("BOOKKEEPING_DB_PATH", BOOKKEEPING_SOURCE_DIR / "dashboard.sqlite3")

# Bootstrap source DBs used for local copy-sync into the runtime source DBs.
SHOPIFY_BOOTSTRAP_DB_PATH = _resolve_env_path(
    "SHOPIFY_BOOTSTRAP_DB_PATH",
    WORKSPACE_ROOT / "Shopify-API" / "shopify-dashboard" / "shopify_data.sqlite3",
)
SHOPIFY_BOOTSTRAP_DB_PATH_FALLBACK = (
    WORKSPACE_ROOT / "Shopify-API" / "shopify_data.sqlite3"
).resolve()
KAUFLAND_BOOTSTRAP_DB_PATH = _resolve_env_path(
    "KAUFLAND_BOOTSTRAP_DB_PATH",
    WORKSPACE_ROOT / "Kaufland-API" / "kaufland_data.sqlite3",
)
SHOPIFY_BOOTSTRAP_ENV_PATH = _resolve_env_path(
    "SHOPIFY_BOOTSTRAP_ENV_PATH",
    WORKSPACE_ROOT / "Shopify-API" / "shopify-dashboard" / ".env",
)
KAUFLAND_BOOTSTRAP_ENV_PATH = _resolve_env_path(
    "KAUFLAND_BOOTSTRAP_ENV_PATH",
    WORKSPACE_ROOT / "Kaufland-API" / ".env",
)
BOOKKEEPING_BOOTSTRAP_DB_PATH = _resolve_env_path(
    "BOOKKEEPING_BOOTSTRAP_DB_PATH",
    WORKSPACE_ROOT / "Buchungen-Dashboard" / "data" / "dashboard.sqlite3",
)
BOOKKEEPING_BOOTSTRAP_DOCUMENTS_DIR = _resolve_env_path(
    "BOOKKEEPING_BOOTSTRAP_DOCUMENTS_DIR",
    WORKSPACE_ROOT / "Buchungen-Dashboard" / "storage" / "documents",
)

# Base path to resolve relative bookkeeping document paths like "storage/documents/...".
BOOKKEEPING_PROJECT_ROOT = _resolve_env_path("BOOKKEEPING_PROJECT_ROOT", PROJECT_ROOT)

MAX_UPLOAD_BYTES = 12 * 1024 * 1024
ALLOWED_MARKETPLACES = {"shopify", "kaufland"}
AUTO_SYNC_ON_STARTUP = _env_flag("AUTO_SYNC_ON_STARTUP", "1")


def ensure_runtime_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    INVOICES_DIR.mkdir(parents=True, exist_ok=True)
    BOOKKEEPING_DOCUMENTS_DIR.mkdir(parents=True, exist_ok=True)
    SOURCES_DIR.mkdir(parents=True, exist_ok=True)
    SHOPIFY_SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    KAUFLAND_SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    BOOKKEEPING_SOURCE_DIR.mkdir(parents=True, exist_ok=True)
