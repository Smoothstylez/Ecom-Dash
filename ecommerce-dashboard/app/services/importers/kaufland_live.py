from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import ssl
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib import error as urlerror
from urllib import request as urlrequest
from urllib.parse import quote, urlencode, urlparse

from app.config import KAUFLAND_BOOTSTRAP_ENV_PATH, KAUFLAND_DB_PATH


LOGGER = logging.getLogger(__name__)


DEFAULT_BASE_URL = "https://sellerapi.kaufland.com/v2"
ADDRESS_KEYS = (
    "first_name",
    "last_name",
    "company_name",
    "street",
    "house_number",
    "postcode",
    "city",
    "additional_field",
    "phone",
    "country",
)


class KauflandLiveError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        payload: Any = None,
        response_text: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload
        self.response_text = response_text


@dataclass
class KauflandLiveConfig:
    client_key: str
    secret_key: str
    base_url: str
    verify_ssl: Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except TypeError:
        return json.dumps(str(value), ensure_ascii=False, sort_keys=True)


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_bool_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return 0 if value == 0 else 1
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return 1
        if lowered in {"0", "false", "no", "off"}:
            return 0
    return None


def _scalar_or_json(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple)):
        return _json_dumps(value)
    return str(value)


def _load_env_file_values(path: Path) -> dict[str, str]:
    if not path.exists() or not path.is_file():
        return {}

    parsed: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[len("export ") :].strip()
        if "=" not in stripped:
            continue
        key, raw_value = stripped.split("=", 1)
        key = key.strip()
        value = raw_value.strip().strip("\"").strip("'")
        if key:
            parsed[key] = value
    return parsed


def _get_env_value(key: str, defaults: dict[str, str]) -> str:
    direct = os.getenv(key)
    if direct is not None and str(direct).strip() != "":
        return str(direct).strip()
    return str(defaults.get(key) or "").strip()


def _resolve_ssl_verify(defaults: dict[str, str]) -> Any:
    skip_raw = _get_env_value("KAUFLAND_INSECURE_SKIP_VERIFY", defaults).lower()
    if skip_raw in {"1", "true", "yes", "on"}:
        return False

    ca_bundle = _get_env_value("REQUESTS_CA_BUNDLE", defaults)
    if not ca_bundle:
        return True

    path = Path(ca_bundle).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    if not path.exists() or not path.is_file():
        raise ValueError(f"Configured REQUESTS_CA_BUNDLE was not found: {path}")
    return str(path)


def load_kaufland_live_config() -> tuple[Optional[KauflandLiveConfig], list[str], dict[str, Any]]:
    fallback_env = _load_env_file_values(KAUFLAND_BOOTSTRAP_ENV_PATH)

    client_key = _get_env_value("SHOP_CLIENT_KEY", fallback_env)
    secret_key = _get_env_value("SHOP_SECRET_KEY", fallback_env)
    base_url = _get_env_value("KAUFLAND_API_BASE_URL", fallback_env) or DEFAULT_BASE_URL

    missing: list[str] = []
    if not client_key:
        missing.append("SHOP_CLIENT_KEY")
    if not secret_key:
        missing.append("SHOP_SECRET_KEY")

    summary = {
        "base_url": base_url,
        "bootstrap_env_path": str(KAUFLAND_BOOTSTRAP_ENV_PATH),
        "bootstrap_env_exists": KAUFLAND_BOOTSTRAP_ENV_PATH.exists(),
    }

    if missing:
        return None, missing, summary

    verify_ssl = _resolve_ssl_verify(fallback_env)

    cfg = KauflandLiveConfig(
        client_key=client_key,
        secret_key=secret_key,
        base_url=base_url,
        verify_ssl=verify_ssl,
    )
    return cfg, [], summary


def _connect() -> sqlite3.Connection:
    KAUFLAND_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(KAUFLAND_DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def init_kaufland_db() -> str:
    with _connect() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id_order TEXT PRIMARY KEY,
                order_units_count INTEGER,
                ts_created_iso TEXT,
                storefront TEXT,
                is_marketplace_deemed_supplier INTEGER,
                raw_json TEXT NOT NULL,
                synced_at_iso TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS order_units (
                id_order_unit TEXT PRIMARY KEY,
                id_order TEXT,
                ts_created_iso TEXT,
                status TEXT,
                cancel_reason TEXT,
                price TEXT,
                revenue_gross TEXT,
                revenue_net TEXT,
                vat REAL,
                shipping_rate TEXT,
                id_offer TEXT,
                storefront TEXT,
                is_marketplace_deemed_supplier INTEGER,
                buyer_id_buyer TEXT,
                billing_first_name TEXT,
                billing_last_name TEXT,
                billing_company_name TEXT,
                billing_street TEXT,
                billing_house_number TEXT,
                billing_postcode TEXT,
                billing_city TEXT,
                billing_additional_field TEXT,
                billing_phone TEXT,
                billing_country TEXT,
                shipping_first_name TEXT,
                shipping_last_name TEXT,
                shipping_company_name TEXT,
                shipping_street TEXT,
                shipping_house_number TEXT,
                shipping_postcode TEXT,
                shipping_city TEXT,
                shipping_additional_field TEXT,
                shipping_phone TEXT,
                shipping_country TEXT,
                product_id_product TEXT,
                product_title TEXT,
                product_eans_json TEXT,
                product_id_category TEXT,
                product_main_picture TEXT,
                product_url TEXT,
                carrier_code TEXT,
                tracking_numbers_json TEXT,
                raw_json TEXT NOT NULL,
                synced_at_iso TEXT NOT NULL,
                FOREIGN KEY (id_order) REFERENCES orders(id_order) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS order_unit_tracking_numbers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_order_unit TEXT NOT NULL,
                position INTEGER NOT NULL,
                carrier_code TEXT,
                tracking_number TEXT,
                raw_value TEXT,
                synced_at_iso TEXT NOT NULL,
                UNIQUE(id_order_unit, position),
                FOREIGN KEY (id_order_unit) REFERENCES order_units(id_order_unit) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS order_unit_refunds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_order_unit TEXT NOT NULL,
                position INTEGER NOT NULL,
                amount TEXT,
                reason TEXT,
                raw_json TEXT NOT NULL,
                synced_at_iso TEXT NOT NULL,
                UNIQUE(id_order_unit, position),
                FOREIGN KEY (id_order_unit) REFERENCES order_units(id_order_unit) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS returns (
                id_return TEXT PRIMARY KEY,
                ts_created_iso TEXT,
                tracking_provider TEXT,
                tracking_code TEXT,
                status TEXT,
                storefront TEXT,
                buyer_id_buyer TEXT,
                buyer_email TEXT,
                return_units_json TEXT,
                raw_json TEXT NOT NULL,
                synced_at_iso TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS return_units (
                id_return_unit TEXT PRIMARY KEY,
                id_return TEXT NOT NULL,
                id_order_unit TEXT,
                ts_created_iso TEXT,
                status TEXT,
                note TEXT,
                reason TEXT,
                storefront TEXT,
                raw_json TEXT NOT NULL,
                synced_at_iso TEXT NOT NULL,
                FOREIGN KEY (id_return) REFERENCES returns(id_return) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sync_runs (
                id_sync_run INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_started_iso TEXT NOT NULL,
                ts_finished_iso TEXT NOT NULL,
                storefront TEXT NOT NULL,
                status TEXT NOT NULL,
                error_count INTEGER NOT NULL,
                summary_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(ts_created_iso);
            CREATE INDEX IF NOT EXISTS idx_order_units_order ON order_units(id_order);
            CREATE INDEX IF NOT EXISTS idx_order_units_status ON order_units(status);
            CREATE INDEX IF NOT EXISTS idx_tracking_unit ON order_unit_tracking_numbers(id_order_unit);
            CREATE INDEX IF NOT EXISTS idx_refunds_unit ON order_unit_refunds(id_order_unit);
            CREATE INDEX IF NOT EXISTS idx_returns_created ON returns(ts_created_iso);
            CREATE INDEX IF NOT EXISTS idx_return_units_return ON return_units(id_return);
            CREATE INDEX IF NOT EXISTS idx_return_units_order_unit ON return_units(id_order_unit);
            """
        )
        connection.commit()

    return str(KAUFLAND_DB_PATH)


def _upsert(connection: sqlite3.Connection, table: str, key_column: str, values: dict[str, Any]) -> None:
    columns = list(values.keys())
    placeholders = ", ".join("?" for _ in columns)
    update_clause = ", ".join(f"{column}=excluded.{column}" for column in columns if column != key_column)
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT({key_column}) DO UPDATE SET {update_clause}"
    )
    connection.execute(sql, tuple(values[column] for column in columns))


def _fetch_existing_raw_json(connection: sqlite3.Connection, table_name: str, key_column: str, key_value: str) -> Optional[str]:
    row = connection.execute(
        f"SELECT raw_json FROM {table_name} WHERE {key_column} = ?",
        (key_value,),
    ).fetchone()
    if row is None:
        return None
    raw = row["raw_json"]
    return str(raw) if raw is not None else None


def _decode_json_text(raw: Optional[str]) -> Any:
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return None


def _top_level_changed_fields(old_raw: Optional[str], new_raw: Optional[str]) -> list[str]:
    old_obj = _decode_json_text(old_raw)
    new_obj = _decode_json_text(new_raw)
    if not isinstance(old_obj, dict) or not isinstance(new_obj, dict):
        return []

    changed: list[str] = []
    for key in sorted(set(old_obj.keys()) | set(new_obj.keys())):
        if old_obj.get(key) != new_obj.get(key):
            changed.append(str(key))
    return changed


def _detect_change(existing_raw: Optional[str], new_raw: str) -> tuple[str, list[str]]:
    if existing_raw is None:
        return "inserted", []
    if existing_raw == new_raw:
        return "unchanged", []
    return "updated", _top_level_changed_fields(existing_raw, new_raw)


def _extract_address_fields(address: Any, prefix: str) -> dict[str, Any]:
    address_obj = address if isinstance(address, dict) else {}
    return {f"{prefix}_{key}": _clean_text(address_obj.get(key)) for key in ADDRESS_KEYS}


def _normalize_tracking_numbers(raw_value: Any) -> list[str]:
    if raw_value is None:
        return []
    if isinstance(raw_value, str):
        split = [piece.strip() for piece in raw_value.split(",") if piece.strip()]
        if split:
            return split
        single = raw_value.strip()
        return [single] if single else []
    if isinstance(raw_value, list):
        normalized: list[str] = []
        for item in raw_value:
            normalized.extend(_normalize_tracking_numbers(item))
        return normalized
    if isinstance(raw_value, dict):
        tracking_number = _clean_text(raw_value.get("tracking_number"))
        if tracking_number:
            return [tracking_number]
        nested = raw_value.get("tracking_numbers")
        if nested is not None:
            return _normalize_tracking_numbers(nested)
        return [_json_dumps(raw_value)]
    return [str(raw_value)]


def _normalize_refunds(raw_value: Any) -> list[dict[str, Any]]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        normalized: list[dict[str, Any]] = []
        for item in raw_value:
            if isinstance(item, dict):
                normalized.append(item)
            elif item is not None:
                normalized.append({"amount": item})
        return normalized
    if isinstance(raw_value, dict):
        for nested_key in ("refunds", "items", "data"):
            nested = raw_value.get(nested_key)
            if isinstance(nested, list):
                return _normalize_refunds(nested)
        return [raw_value]
    return [{"amount": raw_value}]


def _extract_order_id(order: dict[str, Any]) -> Optional[str]:
    for key in ("id_order", "order_id", "id", "orderId", "order_number", "orderNumber"):
        value = _clean_text(order.get(key))
        if value:
            return value
    return None


def _extract_return_id(return_data: dict[str, Any]) -> Optional[str]:
    for key in ("id_return", "return_id", "id", "returnId"):
        value = _clean_text(return_data.get(key))
        if value:
            return value
    return None


def upsert_order(connection: sqlite3.Connection, order: dict[str, Any]) -> dict[str, Any]:
    id_order = _extract_order_id(order)
    if not id_order:
        return {"ok": False, "id_order": None, "change": "skipped", "changed_fields": []}

    raw_json = _json_dumps(order)
    existing_raw = _fetch_existing_raw_json(connection, "orders", "id_order", id_order)
    change, changed_fields = _detect_change(existing_raw, raw_json)
    if change == "unchanged":
        return {"ok": True, "id_order": id_order, "change": "unchanged", "changed_fields": []}

    values = {
        "id_order": id_order,
        "order_units_count": _to_int(order.get("order_units_count")),
        "ts_created_iso": _clean_text(order.get("ts_created_iso")),
        "storefront": _clean_text(order.get("storefront")),
        "is_marketplace_deemed_supplier": _to_bool_int(order.get("is_marketplace_deemed_supplier")),
        "raw_json": raw_json,
        "synced_at_iso": _utc_now_iso(),
    }
    _upsert(connection, "orders", "id_order", values)
    return {"ok": True, "id_order": id_order, "change": change, "changed_fields": changed_fields}


def replace_order_unit_tracking_numbers(
    connection: sqlite3.Connection,
    id_order_unit: str,
    carrier_code: Optional[str],
    tracking_numbers: list[str],
) -> int:
    connection.execute("DELETE FROM order_unit_tracking_numbers WHERE id_order_unit = ?", (id_order_unit,))
    synced_at = _utc_now_iso()
    inserted = 0
    for position, number in enumerate(tracking_numbers):
        connection.execute(
            """
            INSERT INTO order_unit_tracking_numbers (
                id_order_unit, position, carrier_code, tracking_number, raw_value, synced_at_iso
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (id_order_unit, position, carrier_code, number, number, synced_at),
        )
        inserted += 1
    return inserted


def replace_order_unit_refunds(connection: sqlite3.Connection, id_order_unit: str, raw_refunds: Any) -> int:
    refunds = _normalize_refunds(raw_refunds)
    connection.execute("DELETE FROM order_unit_refunds WHERE id_order_unit = ?", (id_order_unit,))
    synced_at = _utc_now_iso()
    inserted = 0
    for position, refund in enumerate(refunds):
        amount_value = _scalar_or_json(refund.get("amount"))
        reason = _clean_text(refund.get("reason"))
        connection.execute(
            """
            INSERT INTO order_unit_refunds (
                id_order_unit, position, amount, reason, raw_json, synced_at_iso
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (id_order_unit, position, amount_value, reason, _json_dumps(refund), synced_at),
        )
        inserted += 1
    return inserted


def upsert_order_unit(connection: sqlite3.Connection, order_unit: dict[str, Any], parent_order_id: Optional[str] = None) -> dict[str, Any]:
    id_order_unit = _clean_text(order_unit.get("id_order_unit"))
    if not id_order_unit:
        return {"ok": False, "id_order_unit": None, "id_order": _clean_text(parent_order_id), "change": "skipped", "changed_fields": []}

    raw_json = _json_dumps(order_unit)
    existing_raw = _fetch_existing_raw_json(connection, "order_units", "id_order_unit", id_order_unit)
    change, changed_fields = _detect_change(existing_raw, raw_json)
    if change == "unchanged":
        return {
            "ok": True,
            "id_order_unit": id_order_unit,
            "id_order": _clean_text(order_unit.get("id_order")) or _clean_text(parent_order_id),
            "change": "unchanged",
            "changed_fields": [],
        }

    buyer = order_unit.get("buyer") if isinstance(order_unit.get("buyer"), dict) else {}
    billing = order_unit.get("billing_address")
    shipping = order_unit.get("shipping_address")
    product = order_unit.get("product") if isinstance(order_unit.get("product"), dict) else {}
    tracking_numbers = _normalize_tracking_numbers(order_unit.get("tracking_numbers"))
    carrier_code = _clean_text(order_unit.get("carrier_code"))
    id_order = _clean_text(order_unit.get("id_order")) or _clean_text(parent_order_id)

    values = {
        "id_order_unit": id_order_unit,
        "id_order": id_order,
        "ts_created_iso": _clean_text(order_unit.get("ts_created_iso")),
        "status": _clean_text(order_unit.get("status")),
        "cancel_reason": _clean_text(order_unit.get("cancel_reason")),
        "price": _scalar_or_json(order_unit.get("price")),
        "revenue_gross": _scalar_or_json(order_unit.get("revenue_gross")),
        "revenue_net": _scalar_or_json(order_unit.get("revenue_net")),
        "vat": _to_float(order_unit.get("vat")),
        "shipping_rate": _scalar_or_json(order_unit.get("shipping_rate")),
        "id_offer": _clean_text(order_unit.get("id_offer")),
        "storefront": _clean_text(order_unit.get("storefront")),
        "is_marketplace_deemed_supplier": _to_bool_int(order_unit.get("is_marketplace_deemed_supplier")),
        "buyer_id_buyer": _clean_text(buyer.get("id_buyer")),
        **_extract_address_fields(billing, "billing"),
        **_extract_address_fields(shipping, "shipping"),
        "product_id_product": _clean_text(product.get("id_product")),
        "product_title": _clean_text(product.get("title")),
        "product_eans_json": _scalar_or_json(product.get("eans")),
        "product_id_category": _clean_text(product.get("id_category")),
        "product_main_picture": _clean_text(product.get("main_picture")),
        "product_url": _clean_text(product.get("url")),
        "carrier_code": carrier_code,
        "tracking_numbers_json": _json_dumps(tracking_numbers) if tracking_numbers else None,
        "raw_json": raw_json,
        "synced_at_iso": _utc_now_iso(),
    }

    _upsert(connection, "order_units", "id_order_unit", values)
    replace_order_unit_tracking_numbers(connection, id_order_unit, carrier_code, tracking_numbers)
    replace_order_unit_refunds(connection, id_order_unit, order_unit.get("refund") or order_unit.get("refunds"))

    return {
        "ok": True,
        "id_order_unit": id_order_unit,
        "id_order": id_order,
        "change": change,
        "changed_fields": changed_fields,
    }


def upsert_return(connection: sqlite3.Connection, return_data: dict[str, Any]) -> dict[str, Any]:
    id_return = _extract_return_id(return_data)
    if not id_return:
        return {"ok": False, "id_return": None, "change": "skipped", "changed_fields": []}

    raw_json = _json_dumps(return_data)
    existing_raw = _fetch_existing_raw_json(connection, "returns", "id_return", id_return)
    change, changed_fields = _detect_change(existing_raw, raw_json)
    if change == "unchanged":
        return {"ok": True, "id_return": id_return, "change": "unchanged", "changed_fields": []}

    buyer = return_data.get("buyer") if isinstance(return_data.get("buyer"), dict) else {}
    return_units = return_data.get("return_units")

    values = {
        "id_return": id_return,
        "ts_created_iso": _clean_text(return_data.get("ts_created_iso")),
        "tracking_provider": _clean_text(return_data.get("tracking_provider")),
        "tracking_code": _clean_text(return_data.get("tracking_code")),
        "status": _clean_text(return_data.get("status")),
        "storefront": _clean_text(return_data.get("storefront")),
        "buyer_id_buyer": _clean_text(buyer.get("id_buyer")),
        "buyer_email": _clean_text(buyer.get("email")),
        "return_units_json": _json_dumps(return_units) if isinstance(return_units, list) else None,
        "raw_json": raw_json,
        "synced_at_iso": _utc_now_iso(),
    }
    _upsert(connection, "returns", "id_return", values)
    return {"ok": True, "id_return": id_return, "change": change, "changed_fields": changed_fields}


def replace_return_units(connection: sqlite3.Connection, id_return: str, return_units: list[dict[str, Any]]) -> int:
    connection.execute("DELETE FROM return_units WHERE id_return = ?", (id_return,))
    inserted = 0
    synced_at = _utc_now_iso()

    for position, unit in enumerate(return_units):
        if not isinstance(unit, dict):
            continue
        id_return_unit = _clean_text(unit.get("id_return_unit")) or f"{id_return}:{position}"
        values = {
            "id_return_unit": id_return_unit,
            "id_return": id_return,
            "id_order_unit": _clean_text(unit.get("id_order_unit")),
            "ts_created_iso": _clean_text(unit.get("ts_created_iso")),
            "status": _clean_text(unit.get("status")),
            "note": _clean_text(unit.get("note")),
            "reason": _clean_text(unit.get("reason")),
            "storefront": _clean_text(unit.get("storefront")),
            "raw_json": _json_dumps(unit),
            "synced_at_iso": synced_at,
        }
        _upsert(connection, "return_units", "id_return_unit", values)
        inserted += 1

    return inserted


def _record_sync_run(
    connection: sqlite3.Connection,
    *,
    storefront: str,
    status: str,
    started_at_iso: str,
    finished_at_iso: str,
    error_count: int,
    summary: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO sync_runs (
            ts_started_iso, ts_finished_iso, storefront, status, error_count, summary_json
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (started_at_iso, finished_at_iso, storefront, status, int(error_count), _json_dumps(summary)),
    )


def _read_last_sync(connection: sqlite3.Connection) -> Optional[dict[str, Any]]:
    row = connection.execute(
        """
        SELECT id_sync_run, ts_started_iso, ts_finished_iso, storefront, status, error_count, summary_json
        FROM sync_runs
        ORDER BY id_sync_run DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None

    payload = dict(row)
    try:
        payload["summary"] = json.loads(str(payload.get("summary_json") or "{}"))
    except (TypeError, ValueError):
        payload["summary"] = {}
    return payload


def get_last_successful_sync_time() -> Optional[str]:
    """Return the ``ts_finished_iso`` timestamp of the most recent successful
    (or partial) Kaufland sync run, or *None* if no qualifying run exists.
    Used by the background worker to supply ``ts_created_from_iso`` for delta syncs.
    """
    if not KAUFLAND_DB_PATH.exists():
        return None
    try:
        with _connect() as connection:
            row = connection.execute(
                """
                SELECT ts_finished_iso
                FROM sync_runs
                WHERE status IN ('success', 'partial')
                ORDER BY id_sync_run DESC
                LIMIT 1
                """
            ).fetchone()
            return str(row["ts_finished_iso"]) if row else None
    except Exception:
        return None


def build_kaufland_live_status() -> dict[str, Any]:
    config, missing, summary = load_kaufland_live_config()
    db_exists = KAUFLAND_DB_PATH.exists()

    last_sync = None
    if db_exists:
        try:
            with _connect() as connection:
                last_sync = _read_last_sync(connection)
        except sqlite3.Error:
            last_sync = None

    return {
        "configured": config is not None,
        "missing_env": missing,
        "config": {
            "base_url": summary.get("base_url"),
            "bootstrap_env_path": summary.get("bootstrap_env_path"),
            "bootstrap_env_exists": summary.get("bootstrap_env_exists"),
        },
        "runtime_db": {
            "path": str(KAUFLAND_DB_PATH),
            "exists": db_exists,
        },
        "last_sync": last_sync,
    }


def _parse_base_api(base_url: str) -> tuple[str, str, str]:
    parsed = urlparse(base_url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(
            "KAUFLAND_API_BASE_URL must be an absolute URL, for example https://sellerapi.kaufland.com/v2"
        )

    base_path = parsed.path.rstrip("/")
    if not base_path:
        base_path = "/v2"

    return parsed.scheme, parsed.netloc, base_path


def _build_endpoint_url(base_url: str, endpoint_path: str, query: Optional[dict[str, Any]] = None) -> tuple[str, str]:
    scheme, netloc, base_path = _parse_base_api(base_url)
    endpoint = endpoint_path.lstrip("/")
    uri = f"{base_path}/{endpoint}"

    if query:
        clean_query = {k: v for k, v in query.items() if v is not None and v != ""}
        if clean_query:
            uri = f"{uri}?{urlencode(clean_query, doseq=True)}"

    url = f"{scheme}://{netloc}{uri}"
    return url, uri


def _build_orders_url(
    base_url: str,
    storefront: str,
    limit: int,
    offset: int,
    ts_created_from_iso: Optional[str] = None,
) -> tuple[str, str]:
    query: dict[str, Any] = {
        "storefront": storefront,
        "limit": limit,
        "offset": offset,
    }
    if ts_created_from_iso:
        query["ts_created_from_iso"] = ts_created_from_iso
    return _build_endpoint_url(
        base_url,
        "orders/",
        query,
    )


def _build_order_detail_url(base_url: str, order_id: str) -> tuple[str, str]:
    safe_order_id = quote(order_id, safe="")
    return _build_endpoint_url(base_url, f"orders/{safe_order_id}/")


def _build_order_unit_detail_url(base_url: str, id_order_unit: str) -> tuple[str, str]:
    safe_unit_id = quote(str(id_order_unit), safe="")
    return _build_endpoint_url(base_url, f"order-units/{safe_unit_id}/")


def _build_returns_url(
    base_url: str,
    storefront: str,
    limit: int,
    offset: int,
    ts_created_from_iso: Optional[str] = None,
) -> tuple[str, str]:
    query: dict[str, Any] = {
        "storefront": storefront,
        "limit": limit,
        "offset": offset,
    }
    if ts_created_from_iso:
        query["ts_created_from_iso"] = ts_created_from_iso
    return _build_endpoint_url(
        base_url,
        "returns/",
        query,
    )


def _build_return_detail_url(base_url: str, return_id: str) -> tuple[str, str]:
    safe_return_id = quote(return_id, safe="")
    return _build_endpoint_url(
        base_url,
        f"returns/{safe_return_id}/",
        {"embedded": ["return_units", "buyer"]},
    )


def _extract_array_loose(payload: Any) -> Optional[list[Any]]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return None

    keys = ("orders", "data", "items", "results", "content", "returns", "tickets", "invoices")
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return value

    nested_data = payload.get("data")
    if isinstance(nested_data, dict):
        for key in keys:
            value = nested_data.get(key)
            if isinstance(value, list):
                return value

    return None


def _extract_data_object(payload: Any) -> Optional[dict[str, Any]]:
    if not isinstance(payload, dict):
        return None

    data = payload.get("data")
    if isinstance(data, dict):
        return data

    return payload


def _payload_error_message(payload: Any, fallback: str = "") -> str:
    if not isinstance(payload, dict):
        return fallback
    return str(payload.get("message") or payload.get("error") or payload.get("error_message") or fallback).strip()


def _get_order_identifier(order: dict[str, Any]) -> str:
    for key in ("id_order", "order_id", "id", "orderId", "order_number", "orderNumber"):
        value = order.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _get_return_identifier(return_data: dict[str, Any]) -> str:
    for key in ("id_return", "return_id", "id", "returnId"):
        value = return_data.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


class KauflandLiveClient:
    def __init__(self, config: KauflandLiveConfig) -> None:
        self.client_key = config.client_key
        self.secret_key = config.secret_key
        self.base_url = config.base_url
        self.verify_ssl = config.verify_ssl
        self._requests = None

    def _requests_module(self):
        if self._requests is None:
            try:
                import requests  # type: ignore
            except ImportError:
                self._requests = False
            else:
                self._requests = requests
        return self._requests

    def _ssl_context(self):
        if self.verify_ssl is False:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            return context
        if isinstance(self.verify_ssl, str):
            return ssl.create_default_context(cafile=self.verify_ssl)
        return None

    def _build_signature(self, method: str, request_target: str, body: str, timestamp: int) -> str:
        plaintext = f"{method}\n{request_target}\n{body}\n{timestamp}"
        return hmac.new(self.secret_key.encode("utf-8"), plaintext.encode("utf-8"), hashlib.sha256).hexdigest()

    def _sign_headers(self, method: str, request_target: str, body: str) -> dict[str, str]:
        timestamp = int(time.time())
        signature = self._build_signature(method, request_target, body, timestamp)
        return {
            "Accept": "application/json",
            "Shop-Client-Key": self.client_key,
            "Shop-Timestamp": str(timestamp),
            "Shop-Signature": signature,
            "User-Agent": "dashboard-combined/1.0",
        }

    def _request_with_urllib(self, url: str, uri: str) -> Any:
        headers = self._sign_headers("GET", url, "")
        request = urlrequest.Request(url, headers=headers, method="GET")
        context = self._ssl_context()

        try:
            with urlrequest.urlopen(request, timeout=25, context=context) as response:
                status_code = int(getattr(response, "status", 200))
                text = response.read().decode("utf-8", errors="replace")
                header_map = dict(response.headers.items())
        except urlerror.HTTPError as exc:
            status_code = int(getattr(exc, "code", 500))
            body = b""
            try:
                body = exc.read()
            except Exception:
                body = b""
            text = body.decode("utf-8", errors="replace")
            header_map = dict(exc.headers.items()) if exc.headers is not None else {}
        except urlerror.URLError as exc:
            raise KauflandLiveError(f"Request to Kaufland API failed: {exc}", status_code=502) from exc
        except TimeoutError as exc:
            raise KauflandLiveError("Request to Kaufland API timed out", status_code=504) from exc
        except OSError as exc:
            raise KauflandLiveError(
                f"Kaufland request failed due to local SSL/CA configuration: {exc}",
                status_code=502,
            ) from exc

        content_type = str(header_map.get("Content-Type") or "application/json")
        if "json" not in content_type.lower():
            raise KauflandLiveError(
                "Kaufland API returned a non-JSON response",
                status_code=status_code,
                response_text=text[:1000],
            )

        try:
            payload = json.loads(text) if text else {}
        except ValueError as exc:
            raise KauflandLiveError(
                "Kaufland API returned invalid JSON",
                status_code=status_code,
                response_text=text[:1000],
            ) from exc

        if not (200 <= status_code < 300):
            raise KauflandLiveError(
                _payload_error_message(payload, f"HTTP {status_code}"),
                status_code=status_code,
                payload=payload,
                response_text=text[:1000],
            )

        return payload

    def get_json(self, url: str, uri: str) -> Any:
        requests = self._requests_module()
        if not requests:
            return self._request_with_urllib(url, uri)

        headers = self._sign_headers("GET", url, "")

        try:
            response = requests.get(url, headers=headers, timeout=25, verify=self.verify_ssl)
        except requests.exceptions.Timeout as exc:
            raise KauflandLiveError("Request to Kaufland API timed out", status_code=504) from exc
        except requests.exceptions.RequestException as exc:
            raise KauflandLiveError(f"Request to Kaufland API failed: {exc}", status_code=502) from exc
        except OSError as exc:
            raise KauflandLiveError(f"Kaufland request failed due to local SSL/CA configuration: {exc}", status_code=502) from exc

        content_type = str(response.headers.get("Content-Type") or "application/json")
        if "json" not in content_type.lower():
            raise KauflandLiveError(
                "Kaufland API returned a non-JSON response",
                status_code=response.status_code,
                response_text=(response.text or "")[:1000],
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise KauflandLiveError(
                "Kaufland API returned invalid JSON",
                status_code=response.status_code,
                response_text=(response.text or "")[:1000],
            ) from exc

        if not (200 <= response.status_code < 300):
            raise KauflandLiveError(
                _payload_error_message(payload, f"HTTP {response.status_code}"),
                status_code=response.status_code,
                payload=payload,
                response_text=(response.text or "")[:1000],
            )

        return payload


def sync_kaufland_live(
    *,
    storefront: str = "de",
    page_limit: int = 100,
    max_pages: int = 5000,
    include_returns: bool = True,
    include_order_unit_details: bool = True,
    ts_created_from_iso: Optional[str] = None,
) -> dict[str, Any]:
    started_at = _utc_now_iso()
    started_monotonic = time.monotonic()

    config, missing, config_summary = load_kaufland_live_config()
    if config is None:
        return {
            "status": "error",
            "provider": "kaufland",
            "error": "Kaufland credentials are not configured.",
            "missing_env": missing,
            "config": config_summary,
        }

    db_path = init_kaufland_db()

    normalized_storefront = str(storefront or "de").strip().lower() or "de"
    normalized_page_limit = min(max(int(page_limit), 1), 200)
    normalized_max_pages = min(max(int(max_pages), 1), 200000)

    summary: dict[str, Any] = {
        "storefront": normalized_storefront,
        "page_limit": normalized_page_limit,
        "max_pages": normalized_max_pages,
        "ts_created_from_iso": ts_created_from_iso,
        "include_returns": bool(include_returns),
        "include_order_unit_details": bool(include_order_unit_details),
        "orders_pages": 0,
        "orders_seen": 0,
        "orders_saved": 0,
        "orders_inserted": 0,
        "orders_updated": 0,
        "orders_unchanged": 0,
        "order_details_loaded": 0,
        "order_details_failed": 0,
        "order_units_seen": 0,
        "order_units_saved": 0,
        "order_units_inserted": 0,
        "order_units_updated": 0,
        "order_units_unchanged": 0,
        "order_unit_details_loaded": 0,
        "order_unit_details_failed": 0,
        "returns_pages": 0,
        "returns_seen": 0,
        "returns_saved": 0,
        "returns_inserted": 0,
        "returns_updated": 0,
        "returns_unchanged": 0,
        "return_details_loaded": 0,
        "return_details_failed": 0,
        "return_units_seen": 0,
        "return_units_saved": 0,
        "error_count": 0,
        "started_at_iso": started_at,
        "finished_at_iso": None,
        "duration_seconds": None,
    }
    errors: list[dict[str, Any]] = []

    def add_error(scope: str, message: str, **meta: Any) -> None:
        summary["error_count"] += 1
        row = {"scope": scope, "error": str(message)}
        row.update(meta)
        if len(errors) < 250:
            errors.append(row)

    def apply_change(prefix: str, result: dict[str, Any], *, saved_key: str) -> str:
        change = str(result.get("change") or "")
        if change == "inserted":
            summary[saved_key] += 1
            summary[f"{prefix}_inserted"] += 1
        elif change == "updated":
            summary[saved_key] += 1
            summary[f"{prefix}_updated"] += 1
        elif change == "unchanged":
            summary[f"{prefix}_unchanged"] += 1
        return change

    sync_status = "success"

    try:
        client = KauflandLiveClient(config)

        with _connect() as connection:
            orders_offset = 0
            for _ in range(normalized_max_pages):
                summary["orders_pages"] += 1
                url, uri = _build_orders_url(config.base_url, normalized_storefront, normalized_page_limit, orders_offset, ts_created_from_iso=ts_created_from_iso)

                try:
                    payload = client.get_json(url, uri)
                except KauflandLiveError as exc:
                    add_error("orders_list", str(exc), offset=orders_offset, status_code=exc.status_code)
                    sync_status = "partial"
                    break

                orders = _extract_array_loose(payload) or []
                summary["orders_seen"] += len(orders)

                for order in orders:
                    if not isinstance(order, dict):
                        continue

                    order_id = _get_order_identifier(order)
                    if not order_id:
                        add_error("order_detail", "Could not resolve id_order from order payload")
                        sync_status = "partial"
                        continue

                    merged_order = dict(order)
                    detail_data = None

                    detail_url, detail_uri = _build_order_detail_url(config.base_url, order_id)
                    try:
                        detail_payload = client.get_json(detail_url, detail_uri)
                    except KauflandLiveError as exc:
                        add_error("order_detail", str(exc), order_id=order_id, status_code=exc.status_code)
                        summary["order_details_failed"] += 1
                        sync_status = "partial"
                    else:
                        detail_obj = _extract_data_object(detail_payload)
                        if isinstance(detail_obj, dict):
                            detail_data = detail_obj
                            merged_order.update(detail_obj)
                            summary["order_details_loaded"] += 1
                        else:
                            add_error("order_detail", "Detail response has no object in 'data'", order_id=order_id)
                            summary["order_details_failed"] += 1
                            sync_status = "partial"

                    if merged_order.get("order_units_count") is None and isinstance(merged_order.get("order_units"), list):
                        merged_order["order_units_count"] = len(merged_order.get("order_units"))

                    order_result = upsert_order(connection, merged_order)
                    order_change = apply_change("orders", order_result, saved_key="orders_saved")
                    if order_change not in {"inserted", "updated", "unchanged"}:
                        add_error("order_upsert", "Order could not be saved", order_id=order_id)
                        sync_status = "partial"

                    order_units: list[dict[str, Any]] = []
                    if isinstance(detail_data, dict) and isinstance(detail_data.get("order_units"), list):
                        order_units = [row for row in detail_data.get("order_units") if isinstance(row, dict)]
                    elif isinstance(merged_order.get("order_units"), list):
                        order_units = [row for row in merged_order.get("order_units") if isinstance(row, dict)]

                    summary["order_units_seen"] += len(order_units)

                    for order_unit in order_units:
                        unit_payload = dict(order_unit)
                        raw_unit_id = order_unit.get("id_order_unit")
                        has_unit_id = raw_unit_id is not None and str(raw_unit_id).strip() != ""

                        if include_order_unit_details and has_unit_id:
                            unit_url, unit_uri = _build_order_unit_detail_url(config.base_url, str(raw_unit_id))
                            try:
                                unit_payload_raw = client.get_json(unit_url, unit_uri)
                            except KauflandLiveError as exc:
                                add_error(
                                    "order_unit_detail",
                                    str(exc),
                                    order_id=order_id,
                                    id_order_unit=raw_unit_id,
                                    status_code=exc.status_code,
                                )
                                summary["order_unit_details_failed"] += 1
                                sync_status = "partial"
                            else:
                                unit_detail_obj = _extract_data_object(unit_payload_raw)
                                if isinstance(unit_detail_obj, dict):
                                    unit_payload.update(unit_detail_obj)
                                    summary["order_unit_details_loaded"] += 1
                                else:
                                    add_error(
                                        "order_unit_detail",
                                        "Unit detail response has no object in 'data'",
                                        order_id=order_id,
                                        id_order_unit=raw_unit_id,
                                    )
                                    summary["order_unit_details_failed"] += 1
                                    sync_status = "partial"

                        unit_result = upsert_order_unit(connection, unit_payload, parent_order_id=order_id)
                        unit_change = apply_change("order_units", unit_result, saved_key="order_units_saved")
                        if unit_change not in {"inserted", "updated", "unchanged"}:
                            add_error("order_unit_upsert", "Order unit could not be saved", order_id=order_id, id_order_unit=raw_unit_id)
                            sync_status = "partial"

                connection.commit()

                if len(orders) < normalized_page_limit:
                    break
                orders_offset += normalized_page_limit

            if include_returns:
                returns_offset = 0
                for _ in range(normalized_max_pages):
                    summary["returns_pages"] += 1
                    url, uri = _build_returns_url(config.base_url, normalized_storefront, normalized_page_limit, returns_offset, ts_created_from_iso=ts_created_from_iso)

                    try:
                        payload = client.get_json(url, uri)
                    except KauflandLiveError as exc:
                        add_error("returns_list", str(exc), offset=returns_offset, status_code=exc.status_code)
                        sync_status = "partial"
                        break

                    returns_list = _extract_array_loose(payload) or []
                    summary["returns_seen"] += len(returns_list)

                    for return_entry in returns_list:
                        if not isinstance(return_entry, dict):
                            continue

                        return_id = _get_return_identifier(return_entry)
                        if not return_id:
                            add_error("return_detail", "Could not resolve id_return from return payload")
                            sync_status = "partial"
                            continue

                        merged_return = dict(return_entry)

                        detail_url, detail_uri = _build_return_detail_url(config.base_url, return_id)
                        try:
                            detail_payload = client.get_json(detail_url, detail_uri)
                        except KauflandLiveError as exc:
                            add_error("return_detail", str(exc), id_return=return_id, status_code=exc.status_code)
                            summary["return_details_failed"] += 1
                            sync_status = "partial"
                        else:
                            detail_obj = _extract_data_object(detail_payload)
                            if isinstance(detail_obj, dict):
                                detail_obj.pop("ts_updated_iso", None)
                                merged_return.update(detail_obj)
                                summary["return_details_loaded"] += 1
                            else:
                                add_error("return_detail", "Detail response has no object in 'data'", id_return=return_id)
                                summary["return_details_failed"] += 1
                                sync_status = "partial"

                        merged_return.pop("ts_updated_iso", None)
                        return_result = upsert_return(connection, merged_return)
                        return_change = apply_change("returns", return_result, saved_key="returns_saved")
                        if return_change not in {"inserted", "updated", "unchanged"}:
                            add_error("return_upsert", "Return could not be saved", id_return=return_id)
                            sync_status = "partial"

                        units_to_store: list[dict[str, Any]] = []
                        raw_units = merged_return.get("return_units")
                        if isinstance(raw_units, list):
                            for unit in raw_units:
                                if not isinstance(unit, dict):
                                    continue
                                cleaned = dict(unit)
                                cleaned.pop("ts_updated_iso", None)
                                units_to_store.append(cleaned)

                        summary["return_units_seen"] += len(units_to_store)
                        if return_change in {"inserted", "updated"}:
                            saved_return_id = str(return_result.get("id_return") or return_id)
                            summary["return_units_saved"] += replace_return_units(connection, saved_return_id, units_to_store)

                    connection.commit()

                    if len(returns_list) < normalized_page_limit:
                        break
                    returns_offset += normalized_page_limit

            if summary["error_count"] > 0 and sync_status == "success":
                sync_status = "partial"

            summary["finished_at_iso"] = _utc_now_iso()
            summary["duration_seconds"] = round(time.monotonic() - started_monotonic, 3)
            run_summary = dict(summary)
            run_summary["errors"] = errors[:100]

            _record_sync_run(
                connection,
                storefront=normalized_storefront,
                status=sync_status,
                started_at_iso=str(summary["started_at_iso"]),
                finished_at_iso=str(summary["finished_at_iso"]),
                error_count=int(summary["error_count"]),
                summary=run_summary,
            )
            connection.commit()

        return {
            "status": sync_status,
            "provider": "kaufland",
            "database_path": db_path,
            "summary": summary,
            "errors": errors,
            "config": config_summary,
        }
    except KauflandLiveError as exc:
        summary["finished_at_iso"] = _utc_now_iso()
        summary["duration_seconds"] = round(time.monotonic() - started_monotonic, 3)
        add_error("kaufland", str(exc), status_code=exc.status_code)
        return {
            "status": "error",
            "provider": "kaufland",
            "database_path": db_path,
            "summary": summary,
            "errors": errors,
            "error": str(exc),
            "status_code": exc.status_code,
            "config": config_summary,
        }
    except Exception as exc:
        LOGGER.exception("kaufland live sync failed")
        summary["finished_at_iso"] = _utc_now_iso()
        summary["duration_seconds"] = round(time.monotonic() - started_monotonic, 3)
        add_error("kaufland", str(exc))
        return {
            "status": "error",
            "provider": "kaufland",
            "database_path": db_path,
            "summary": summary,
            "errors": errors,
            "error": str(exc),
            "config": config_summary,
        }
