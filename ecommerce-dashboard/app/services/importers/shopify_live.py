from __future__ import annotations

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
from urllib import parse as urlparse
from urllib import request as urlrequest

from app.config import SHOPIFY_DB_PATH, SHOPIFY_BOOTSTRAP_ENV_PATH


LOGGER = logging.getLogger(__name__)


class ShopifyLiveError(RuntimeError):
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
class ShopifyLiveConfig:
    domain: str
    client_id: str
    client_secret: str
    api_version: str
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
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _normalize_domain(value: str) -> str:
    candidate = (value or "").strip()
    if candidate.startswith("https://"):
        candidate = candidate[len("https://") :]
    if candidate.startswith("http://"):
        candidate = candidate[len("http://") :]
    candidate = candidate.strip().strip("/")
    if "/" in candidate:
        candidate = candidate.split("/", 1)[0]
    if not candidate:
        raise ValueError("SHOPIFY_SHOP_DOMAIN is missing.")
    if not candidate.lower().endswith(".myshopify.com"):
        raise ValueError(
            "SHOPIFY_SHOP_DOMAIN must be your '*.myshopify.com' domain "
            f"(current: '{candidate}')."
        )
    return candidate


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
    skip_raw = _get_env_value("SHOPIFY_INSECURE_SKIP_VERIFY", defaults).lower()
    if skip_raw in {"1", "true", "yes", "on"}:
        return False

    ca_bundle = _get_env_value("SHOPIFY_CA_BUNDLE", defaults) or _get_env_value("REQUESTS_CA_BUNDLE", defaults)
    if not ca_bundle:
        return True

    path = Path(ca_bundle).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    if not path.exists() or not path.is_file():
        raise ValueError(f"Configured Shopify CA bundle was not found: {path}")
    return str(path)


def load_shopify_live_config() -> tuple[Optional[ShopifyLiveConfig], list[str], dict[str, Any]]:
    fallback_env = _load_env_file_values(SHOPIFY_BOOTSTRAP_ENV_PATH)

    domain = _get_env_value("SHOPIFY_SHOP_DOMAIN", fallback_env)
    client_id = _get_env_value("SHOPIFY_CLIENT_ID", fallback_env)
    client_secret = _get_env_value("SHOPIFY_CLIENT_SECRET", fallback_env)
    api_version = _get_env_value("SHOPIFY_API_VERSION", fallback_env) or "2025-01"

    missing: list[str] = []
    if not domain:
        missing.append("SHOPIFY_SHOP_DOMAIN")
    if not client_id:
        missing.append("SHOPIFY_CLIENT_ID")
    if not client_secret:
        missing.append("SHOPIFY_CLIENT_SECRET")

    summary = {
        "shop_domain": domain,
        "api_version": api_version,
        "bootstrap_env_path": str(SHOPIFY_BOOTSTRAP_ENV_PATH),
        "bootstrap_env_exists": SHOPIFY_BOOTSTRAP_ENV_PATH.exists(),
    }

    if missing:
        return None, missing, summary

    normalized_domain = _normalize_domain(domain)
    verify_ssl = _resolve_ssl_verify(fallback_env)

    cfg = ShopifyLiveConfig(
        domain=normalized_domain,
        client_id=client_id,
        client_secret=client_secret,
        api_version=api_version,
        verify_ssl=verify_ssl,
    )
    return cfg, [], summary


def _connect() -> sqlite3.Connection:
    SHOPIFY_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(SHOPIFY_DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def _table_has_column(connection: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    for row in rows:
        if str(row["name"]) == column_name:
            return True
    return False


def _ensure_column(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_sql_type: str,
) -> None:
    if _table_has_column(connection, table_name, column_name):
        return
    connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql_type}")


def init_shopify_db() -> str:
    with _connect() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id TEXT PRIMARY KEY,
                order_number INTEGER,
                name TEXT,
                email TEXT,
                created_at TEXT,
                updated_at TEXT,
                payment_method TEXT,
                financial_status TEXT,
                fulfillment_status TEXT,
                total_price TEXT,
                subtotal_price TEXT,
                total_tax TEXT,
                total_discounts TEXT,
                currency TEXT,
                tags TEXT,
                note TEXT,
                customer_id TEXT,
                customer_email TEXT,
                customer_first_name TEXT,
                customer_last_name TEXT,
                shipping_country TEXT,
                shipping_city TEXT,
                line_items_count INTEGER,
                fulfillments_count INTEGER,
                refunds_count INTEGER,
                estimated_paypal_fee TEXT,
                estimated_net_after_fee TEXT,
                fee_estimation_note TEXT,
                raw_json TEXT NOT NULL,
                synced_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS order_line_items (
                id TEXT PRIMARY KEY,
                order_id TEXT,
                title TEXT,
                variant_title TEXT,
                sku TEXT,
                quantity INTEGER,
                price TEXT,
                total_discount TEXT,
                fulfillment_status TEXT,
                product_id TEXT,
                variant_id TEXT,
                vendor TEXT,
                raw_json TEXT NOT NULL,
                synced_at TEXT NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS order_fulfillments (
                id TEXT PRIMARY KEY,
                order_id TEXT,
                created_at TEXT,
                updated_at TEXT,
                status TEXT,
                tracking_company TEXT,
                tracking_number TEXT,
                tracking_url TEXT,
                tracking_numbers_json TEXT,
                shipment_status TEXT,
                raw_json TEXT NOT NULL,
                synced_at TEXT NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS order_refunds (
                id TEXT PRIMARY KEY,
                order_id TEXT,
                created_at TEXT,
                note TEXT,
                restock INTEGER,
                user_id TEXT,
                refund_line_items_json TEXT,
                transactions_json TEXT,
                raw_json TEXT NOT NULL,
                synced_at TEXT NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS order_transactions (
                id TEXT PRIMARY KEY,
                order_id TEXT,
                kind TEXT,
                status TEXT,
                gateway TEXT,
                amount TEXT,
                currency TEXT,
                fee_amount TEXT,
                net_amount TEXT,
                payment_method TEXT,
                processed_at TEXT,
                raw_json TEXT NOT NULL,
                synced_at TEXT NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sync_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT,
                finished_at TEXT,
                status TEXT,
                storefront TEXT,
                orders_inserted INTEGER,
                orders_updated INTEGER,
                orders_unchanged INTEGER,
                line_items_inserted INTEGER,
                line_items_updated INTEGER,
                fulfillments_inserted INTEGER,
                refunds_inserted INTEGER,
                error_count INTEGER,
                errors_json TEXT,
                summary_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
            CREATE INDEX IF NOT EXISTS idx_line_items_order_id ON order_line_items(order_id);
            CREATE INDEX IF NOT EXISTS idx_fulfillments_order_id ON order_fulfillments(order_id);
            CREATE INDEX IF NOT EXISTS idx_refunds_order_id ON order_refunds(order_id);
            CREATE INDEX IF NOT EXISTS idx_transactions_order_id ON order_transactions(order_id);
            CREATE INDEX IF NOT EXISTS idx_sync_runs_started_at ON sync_runs(started_at);

            CREATE TABLE IF NOT EXISTS product_image_cache (
                product_id TEXT PRIMARY KEY,
                image_src TEXT,
                checked_at TEXT NOT NULL
            );
            """
        )

        _ensure_column(connection, "orders", "estimated_paypal_fee", "TEXT")
        _ensure_column(connection, "orders", "estimated_net_after_fee", "TEXT")
        _ensure_column(connection, "orders", "fee_estimation_note", "TEXT")
        _ensure_column(connection, "orders", "payment_method", "TEXT")
        connection.commit()

    return str(SHOPIFY_DB_PATH)


def lookup_product_images(product_ids: list[str]) -> dict[str, Optional[str]]:
    """Return a mapping of product_id → image_src for the given IDs.

    Uses a local cache table; fetches from the Shopify API for any
    product_ids that are not yet cached.
    """
    if not product_ids:
        return {}

    unique_ids = list({str(pid).strip() for pid in product_ids if str(pid).strip()})
    if not unique_ids:
        return {}

    result: dict[str, Optional[str]] = {}
    uncached: list[str] = []

    # --- check cache ---
    if SHOPIFY_DB_PATH.exists():
        try:
            with _connect() as connection:
                # ensure the table exists
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS product_image_cache "
                    "(product_id TEXT PRIMARY KEY, image_src TEXT, checked_at TEXT NOT NULL)"
                )
                placeholders = ", ".join("?" for _ in unique_ids)
                rows = connection.execute(
                    f"SELECT product_id, image_src FROM product_image_cache WHERE product_id IN ({placeholders})",
                    unique_ids,
                ).fetchall()
                for row in rows:
                    result[str(row["product_id"])] = row["image_src"] or None
        except Exception:
            LOGGER.debug("product_image_cache lookup failed", exc_info=True)

    uncached = [pid for pid in unique_ids if pid not in result]
    if not uncached:
        return result

    # --- fetch from Shopify API for uncached product_ids ---
    config, missing, _ = load_shopify_live_config()
    if config is None:
        # no Shopify credentials → return what we have
        for pid in uncached:
            result[pid] = None
        return result

    try:
        client = ShopifyLiveClient(config)
    except Exception:
        LOGGER.debug("ShopifyLiveClient init failed", exc_info=True)
        for pid in uncached:
            result[pid] = None
        return result

    now = _utc_now_iso()
    fetched: dict[str, Optional[str]] = {}
    for pid in uncached:
        image_src = client.get_product_image(pid)
        fetched[pid] = image_src
        result[pid] = image_src

    # --- persist to cache ---
    if fetched:
        try:
            with _connect() as connection:
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS product_image_cache "
                    "(product_id TEXT PRIMARY KEY, image_src TEXT, checked_at TEXT NOT NULL)"
                )
                for pid, src in fetched.items():
                    connection.execute(
                        "INSERT INTO product_image_cache (product_id, image_src, checked_at) "
                        "VALUES (?, ?, ?) ON CONFLICT(product_id) DO UPDATE SET image_src=excluded.image_src, checked_at=excluded.checked_at",
                        (pid, src, now),
                    )
                connection.commit()
        except Exception:
            LOGGER.debug("product_image_cache persist failed", exc_info=True)

    return result


def _upsert(connection: sqlite3.Connection, table: str, key_column: str, values: dict[str, Any]) -> None:
    columns = list(values.keys())
    placeholders = ", ".join("?" for _ in columns)
    update_clause = ", ".join(f"{column}=excluded.{column}" for column in columns if column != key_column)
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT({key_column}) DO UPDATE SET {update_clause}"
    )
    connection.execute(sql, tuple(values[column] for column in columns))


def _fetch_existing_raw_json(
    connection: sqlite3.Connection,
    table_name: str,
    key_column: str,
    key_value: str,
) -> Optional[str]:
    row = connection.execute(
        f"SELECT raw_json FROM {table_name} WHERE {key_column} = ?",
        (key_value,),
    ).fetchone()
    if row is None:
        return None
    raw = row["raw_json"]
    return str(raw) if raw is not None else None


def _detect_change(existing_raw: Optional[str], new_raw: str) -> str:
    if existing_raw is None:
        return "inserted"
    if existing_raw == new_raw:
        return "unchanged"
    return "updated"


def _first_non_empty_text(*values: Any) -> Optional[str]:
    for value in values:
        cleaned = _clean_text(value)
        if cleaned:
            return cleaned
    return None


def _contains_paypal_gateway(order: dict[str, Any]) -> bool:
    gateways = order.get("payment_gateway_names")
    if not isinstance(gateways, list):
        return False
    for gateway in gateways:
        text = _clean_text(gateway)
        if text and "paypal" in text.lower():
            return True
    return False


def _estimate_paypal_fee_for_order(order: dict[str, Any]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    if not _contains_paypal_gateway(order):
        return None, None, None

    amount = _to_float(order.get("total_price"))
    if amount is None:
        return None, None, None

    billing = order.get("billing_address") if isinstance(order.get("billing_address"), dict) else {}
    shipping = order.get("shipping_address") if isinstance(order.get("shipping_address"), dict) else {}
    country_code = _first_non_empty_text(billing.get("country_code"), shipping.get("country_code"))
    normalized_country = (country_code or "").upper()

    non_eu_codes = {"CH", "GB", "UK", "US", "CA", "AU"}
    rate = 0.0299
    fixed_fee = 0.39
    if normalized_country in non_eu_codes:
        rate += 0.0199

    fee = round((amount * rate) + fixed_fee, 2)
    net = round(amount - fee, 2)
    note = f"paypal_estimate rate={rate:.4f} fixed={fixed_fee:.2f} country={normalized_country or 'unknown'}"

    return f"{fee:.2f}", f"{net:.2f}", note


def _format_payment_method(raw_value: Any) -> Optional[str]:
    text = _clean_text(raw_value)
    if not text:
        return None

    normalized = text.lower().replace("-", "_").replace(" ", "_")
    mapping = {
        "paypal": "PayPal",
        "paypal_express_checkout": "PayPal",
        "shopify_payments": "Shopify Payments",
        "klarna": "Klarna",
        "visa": "Visa",
        "mastercard": "Mastercard",
        "maestro": "Maestro",
        "amex": "American Express",
        "american_express": "American Express",
        "apple_pay": "Apple Pay",
        "google_pay": "Google Pay",
        "shop_pay": "Shop Pay",
        "ideal": "iDEAL",
    }
    if normalized in mapping:
        return mapping[normalized]
    return text.replace("_", " ").title()


def _extract_order_payment_method(order: dict[str, Any]) -> str:
    payment_details = order.get("payment_details") if isinstance(order.get("payment_details"), dict) else {}
    method_name = _first_non_empty_text(
        payment_details.get("credit_card_company"),
        payment_details.get("payment_method_name"),
        payment_details.get("payment_method"),
        payment_details.get("wallet"),
    )

    if not method_name:
        gateways = order.get("payment_gateway_names")
        if isinstance(gateways, list):
            method_name = _first_non_empty_text(*gateways)

    if not method_name:
        method_name = "Keine Angabe"
    return _format_payment_method(method_name) or "Keine Angabe"


def _extract_transaction_payment_method(transaction: dict[str, Any]) -> Optional[str]:
    payment_details = transaction.get("payment_details") if isinstance(transaction.get("payment_details"), dict) else {}
    method = _first_non_empty_text(
        payment_details.get("credit_card_company"),
        payment_details.get("payment_method_name"),
        payment_details.get("payment_method"),
        payment_details.get("wallet"),
        transaction.get("gateway"),
        transaction.get("source_name"),
    )
    return _format_payment_method(method) if method else None


def _extract_transaction_fee(transaction: dict[str, Any]) -> Optional[str]:
    direct = _clean_text(transaction.get("fee"))
    if direct:
        return direct
    receipt = transaction.get("receipt") if isinstance(transaction.get("receipt"), dict) else {}
    return _first_non_empty_text(
        receipt.get("fee"),
        receipt.get("processing_fee"),
        receipt.get("shopify_fee"),
        receipt.get("total_fee"),
    )


def _extract_transaction_net(transaction: dict[str, Any]) -> Optional[str]:
    direct = _clean_text(transaction.get("net"))
    if direct:
        return direct
    receipt = transaction.get("receipt") if isinstance(transaction.get("receipt"), dict) else {}
    return _first_non_empty_text(
        receipt.get("net"),
        receipt.get("net_amount"),
        receipt.get("amount_net"),
        receipt.get("payout"),
    )


def _line_item_pk(order_id: str, line_item: dict[str, Any], position: int) -> str:
    item_id = _clean_text(line_item.get("id"))
    suffix = item_id if item_id else f"idx{position}"
    return f"{order_id}_{suffix}"


def _fulfillment_pk(order_id: str, fulfillment: dict[str, Any], position: int) -> str:
    fulfillment_id = _clean_text(fulfillment.get("id"))
    if fulfillment_id:
        return fulfillment_id
    return f"{order_id}_idx{position}"


def _refund_pk(order_id: str, refund: dict[str, Any], position: int) -> str:
    refund_id = _clean_text(refund.get("id"))
    suffix = refund_id if refund_id else f"idx{position}"
    return f"{order_id}_{suffix}"


def _transaction_pk(order_id: str, transaction: dict[str, Any], position: int) -> str:
    transaction_id = _clean_text(transaction.get("id"))
    suffix = transaction_id if transaction_id else f"idx{position}"
    return f"{order_id}_{suffix}"


def upsert_order(connection: sqlite3.Connection, order: dict[str, Any]) -> dict[str, Any]:
    order_id = _clean_text(order.get("id"))
    if not order_id:
        return {"ok": False, "id": None, "change": "skipped"}

    raw_json = _json_dumps(order)
    existing_raw = _fetch_existing_raw_json(connection, "orders", "id", order_id)
    change = _detect_change(existing_raw, raw_json)
    if change == "unchanged":
        return {"ok": True, "id": order_id, "change": "unchanged"}

    customer = order.get("customer") if isinstance(order.get("customer"), dict) else {}
    shipping = order.get("shipping_address") if isinstance(order.get("shipping_address"), dict) else {}
    line_items = order.get("line_items") if isinstance(order.get("line_items"), list) else []
    fulfillments = order.get("fulfillments") if isinstance(order.get("fulfillments"), list) else []
    refunds = order.get("refunds") if isinstance(order.get("refunds"), list) else []

    estimated_fee, estimated_net, estimation_note = _estimate_paypal_fee_for_order(order)

    values = {
        "id": order_id,
        "order_number": _to_int(order.get("order_number")),
        "name": _clean_text(order.get("name")),
        "email": _clean_text(order.get("email")),
        "created_at": _clean_text(order.get("created_at")),
        "updated_at": _clean_text(order.get("updated_at")),
        "payment_method": _extract_order_payment_method(order),
        "financial_status": _clean_text(order.get("financial_status")),
        "fulfillment_status": _clean_text(order.get("fulfillment_status")),
        "total_price": _clean_text(order.get("total_price")),
        "subtotal_price": _clean_text(order.get("subtotal_price")),
        "total_tax": _clean_text(order.get("total_tax")),
        "total_discounts": _clean_text(order.get("total_discounts")),
        "currency": _clean_text(order.get("currency")),
        "tags": _clean_text(order.get("tags")),
        "note": _clean_text(order.get("note")),
        "customer_id": _clean_text(customer.get("id")),
        "customer_email": _clean_text(customer.get("email")),
        "customer_first_name": _clean_text(customer.get("first_name")),
        "customer_last_name": _clean_text(customer.get("last_name")),
        "shipping_country": _clean_text(shipping.get("country")),
        "shipping_city": _clean_text(shipping.get("city")),
        "line_items_count": len(line_items),
        "fulfillments_count": len(fulfillments),
        "refunds_count": len(refunds),
        "estimated_paypal_fee": estimated_fee,
        "estimated_net_after_fee": estimated_net,
        "fee_estimation_note": estimation_note,
        "raw_json": raw_json,
        "synced_at": _utc_now_iso(),
    }
    _upsert(connection, "orders", "id", values)
    return {"ok": True, "id": order_id, "change": change}


def upsert_line_item(connection: sqlite3.Connection, order_id: str, line_item: dict[str, Any], position: int) -> dict[str, Any]:
    record_id = _line_item_pk(order_id, line_item, position)
    raw_json = _json_dumps(line_item)
    existing_raw = _fetch_existing_raw_json(connection, "order_line_items", "id", record_id)
    change = _detect_change(existing_raw, raw_json)
    if change == "unchanged":
        return {"ok": True, "id": record_id, "change": "unchanged"}

    values = {
        "id": record_id,
        "order_id": _clean_text(order_id),
        "title": _clean_text(line_item.get("title")),
        "variant_title": _clean_text(line_item.get("variant_title")),
        "sku": _clean_text(line_item.get("sku")),
        "quantity": _to_int(line_item.get("quantity")),
        "price": _clean_text(line_item.get("price")),
        "total_discount": _clean_text(line_item.get("total_discount")),
        "fulfillment_status": _clean_text(line_item.get("fulfillment_status")),
        "product_id": _clean_text(line_item.get("product_id")),
        "variant_id": _clean_text(line_item.get("variant_id")),
        "vendor": _clean_text(line_item.get("vendor")),
        "raw_json": raw_json,
        "synced_at": _utc_now_iso(),
    }
    _upsert(connection, "order_line_items", "id", values)
    return {"ok": True, "id": record_id, "change": change}


def _normalize_tracking_numbers(fulfillment: dict[str, Any]) -> list[str]:
    tracking_numbers = fulfillment.get("tracking_numbers")
    if isinstance(tracking_numbers, list):
        values: list[str] = []
        for item in tracking_numbers:
            text = _clean_text(item)
            if text:
                values.append(text)
        if values:
            return values

    fallback = _clean_text(fulfillment.get("tracking_number"))
    if fallback:
        return [fallback]
    return []


def _first_tracking_url(fulfillment: dict[str, Any]) -> Optional[str]:
    direct = _clean_text(fulfillment.get("tracking_url"))
    if direct:
        return direct

    tracking_urls = fulfillment.get("tracking_urls")
    if isinstance(tracking_urls, list):
        for item in tracking_urls:
            text = _clean_text(item)
            if text:
                return text
    return None


def upsert_fulfillment(connection: sqlite3.Connection, order_id: str, fulfillment: dict[str, Any], position: int) -> dict[str, Any]:
    record_id = _fulfillment_pk(order_id, fulfillment, position)
    raw_json = _json_dumps(fulfillment)
    existing_raw = _fetch_existing_raw_json(connection, "order_fulfillments", "id", record_id)
    change = _detect_change(existing_raw, raw_json)
    if change == "unchanged":
        return {"ok": True, "id": record_id, "change": "unchanged"}

    tracking_numbers = _normalize_tracking_numbers(fulfillment)
    tracking_number = _clean_text(fulfillment.get("tracking_number"))
    if not tracking_number and tracking_numbers:
        tracking_number = tracking_numbers[0]

    values = {
        "id": record_id,
        "order_id": _clean_text(order_id),
        "created_at": _clean_text(fulfillment.get("created_at")),
        "updated_at": _clean_text(fulfillment.get("updated_at")),
        "status": _clean_text(fulfillment.get("status")),
        "tracking_company": _clean_text(fulfillment.get("tracking_company")),
        "tracking_number": tracking_number,
        "tracking_url": _first_tracking_url(fulfillment),
        "tracking_numbers_json": _json_dumps(tracking_numbers) if tracking_numbers else None,
        "shipment_status": _clean_text(fulfillment.get("shipment_status")),
        "raw_json": raw_json,
        "synced_at": _utc_now_iso(),
    }
    _upsert(connection, "order_fulfillments", "id", values)
    return {"ok": True, "id": record_id, "change": change}


def _refund_restock_flag(refund: dict[str, Any]) -> Optional[int]:
    direct = refund.get("restock")
    if isinstance(direct, bool):
        return int(direct)
    if isinstance(direct, (int, float)):
        return 0 if direct == 0 else 1
    if isinstance(direct, str):
        lowered = direct.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return 1
        if lowered in {"0", "false", "no", "off"}:
            return 0

    refund_line_items = refund.get("refund_line_items") if isinstance(refund.get("refund_line_items"), list) else []
    any_restock = False
    for item in refund_line_items:
        if not isinstance(item, dict):
            continue
        restock_type = _clean_text(item.get("restock_type"))
        if restock_type and restock_type.lower() != "no_restock":
            any_restock = True
            break
    return 1 if any_restock else 0


def upsert_refund(connection: sqlite3.Connection, order_id: str, refund: dict[str, Any], position: int) -> dict[str, Any]:
    record_id = _refund_pk(order_id, refund, position)
    raw_json = _json_dumps(refund)
    existing_raw = _fetch_existing_raw_json(connection, "order_refunds", "id", record_id)
    change = _detect_change(existing_raw, raw_json)
    if change == "unchanged":
        return {"ok": True, "id": record_id, "change": "unchanged"}

    refund_line_items = refund.get("refund_line_items") if isinstance(refund.get("refund_line_items"), list) else []
    transactions = refund.get("transactions") if isinstance(refund.get("transactions"), list) else []

    values = {
        "id": record_id,
        "order_id": _clean_text(order_id),
        "created_at": _clean_text(refund.get("created_at")),
        "note": _clean_text(refund.get("note")),
        "restock": _refund_restock_flag(refund),
        "user_id": _clean_text(refund.get("user_id")),
        "refund_line_items_json": _json_dumps(refund_line_items),
        "transactions_json": _json_dumps(transactions),
        "raw_json": raw_json,
        "synced_at": _utc_now_iso(),
    }
    _upsert(connection, "order_refunds", "id", values)
    return {"ok": True, "id": record_id, "change": change}


def upsert_transaction(
    connection: sqlite3.Connection,
    order_id: str,
    transaction: dict[str, Any],
    position: int,
) -> dict[str, Any]:
    record_id = _transaction_pk(order_id, transaction, position)
    raw_json = _json_dumps(transaction)

    values = {
        "id": record_id,
        "order_id": _clean_text(order_id),
        "kind": _clean_text(transaction.get("kind")),
        "status": _clean_text(transaction.get("status")),
        "gateway": _clean_text(transaction.get("gateway")),
        "amount": _clean_text(transaction.get("amount")),
        "currency": _first_non_empty_text(transaction.get("currency"), transaction.get("currency_exchange_currency")),
        "fee_amount": _extract_transaction_fee(transaction),
        "net_amount": _extract_transaction_net(transaction),
        "payment_method": _extract_transaction_payment_method(transaction),
        "processed_at": _clean_text(transaction.get("processed_at")),
        "raw_json": raw_json,
        "synced_at": _utc_now_iso(),
    }

    existing_row = connection.execute(
        """
        SELECT raw_json, kind, status, gateway, amount, currency, fee_amount, net_amount, payment_method, processed_at
        FROM order_transactions
        WHERE id = ?
        LIMIT 1
        """,
        (record_id,),
    ).fetchone()

    if existing_row is None:
        change = "inserted"
    else:
        same_raw = str(existing_row["raw_json"] or "") == raw_json
        compare_columns = [
            "kind",
            "status",
            "gateway",
            "amount",
            "currency",
            "fee_amount",
            "net_amount",
            "payment_method",
            "processed_at",
        ]
        same_derived = True
        for column in compare_columns:
            existing_value = _clean_text(existing_row[column])
            new_value = _clean_text(values[column])
            if existing_value != new_value:
                same_derived = False
                break

        if same_raw and same_derived:
            return {"ok": True, "id": record_id, "change": "unchanged"}
        change = "updated"

    _upsert(connection, "order_transactions", "id", values)
    return {"ok": True, "id": record_id, "change": change}


def delete_stale_order_rows(connection: sqlite3.Connection, table_name: str, order_id: str, keep_ids: list[str]) -> int:
    normalized_order_id = _clean_text(order_id)
    if not normalized_order_id:
        return 0

    quoted_table = '"' + str(table_name).replace('"', '""') + '"'
    if not keep_ids:
        cursor = connection.execute(
            f"DELETE FROM {quoted_table} WHERE order_id = ?",
            (normalized_order_id,),
        )
        return int(cursor.rowcount or 0)

    placeholders = ", ".join("?" for _ in keep_ids)
    params: list[Any] = [normalized_order_id, *keep_ids]
    cursor = connection.execute(
        f"DELETE FROM {quoted_table} WHERE order_id = ? AND id NOT IN ({placeholders})",
        params,
    )
    return int(cursor.rowcount or 0)


def refresh_order_payment_method_from_transactions(
    connection: sqlite3.Connection,
    order_id: str,
    fallback_method: Optional[str] = None,
) -> Optional[str]:
    normalized_order_id = _clean_text(order_id)
    if not normalized_order_id:
        return None

    row = connection.execute(
        """
        SELECT t.payment_method
        FROM order_transactions t
        WHERE
            t.order_id = ?
            AND COALESCE(t.payment_method, '') <> ''
            AND LOWER(TRIM(t.payment_method)) NOT IN ('unknown', 'unbekannt', 'n/a', 'none', 'null', '-', '--', 'keine angabe')
        ORDER BY
            CASE
                WHEN LOWER(COALESCE(t.kind, '')) IN ('sale', 'capture')
                     AND LOWER(COALESCE(t.status, '')) IN ('success', 'succeeded', 'paid', 'completed') THEN 0
                WHEN LOWER(COALESCE(t.kind, '')) IN ('sale', 'capture') THEN 1
                WHEN LOWER(COALESCE(t.status, '')) IN ('success', 'succeeded', 'paid', 'completed') THEN 2
                ELSE 3
            END,
            COALESCE(t.processed_at, '') ASC,
            t.id ASC
        LIMIT 1
        """,
        (normalized_order_id,),
    ).fetchone()

    chosen_method = _clean_text(row["payment_method"]) if row is not None else None
    if not chosen_method:
        chosen_method = _format_payment_method(fallback_method)

    if chosen_method:
        connection.execute(
            "UPDATE orders SET payment_method = ?, synced_at = ? WHERE id = ?",
            (chosen_method, _utc_now_iso(), normalized_order_id),
        )
    return chosen_method


def _record_sync_run(
    connection: sqlite3.Connection,
    *,
    started_at: str,
    finished_at: str,
    status: str,
    storefront: str,
    summary: dict[str, Any],
    errors: list[dict[str, Any]],
) -> None:
    connection.execute(
        """
        INSERT INTO sync_runs (
            started_at,
            finished_at,
            status,
            storefront,
            orders_inserted,
            orders_updated,
            orders_unchanged,
            line_items_inserted,
            line_items_updated,
            fulfillments_inserted,
            refunds_inserted,
            error_count,
            errors_json,
            summary_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            started_at,
            finished_at,
            status,
            storefront,
            int(summary.get("orders_inserted") or 0),
            int(summary.get("orders_updated") or 0),
            int(summary.get("orders_unchanged") or 0),
            int(summary.get("line_items_inserted") or 0),
            int(summary.get("line_items_updated") or 0),
            int(summary.get("fulfillments_inserted") or 0),
            int(summary.get("refunds_inserted") or 0),
            len(errors),
            _json_dumps(errors),
            _json_dumps(summary),
        ),
    )


def _read_last_sync(connection: sqlite3.Connection) -> Optional[dict[str, Any]]:
    row = connection.execute(
        """
        SELECT id, started_at, finished_at, status, storefront, error_count, summary_json
        FROM sync_runs
        ORDER BY id DESC
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
    """Return the ``finished_at`` ISO timestamp of the most recent successful
    (or partial) Shopify sync run, or *None* if no qualifying run exists.
    Used by the background worker to supply ``updated_at_min`` for delta syncs.
    """
    if not SHOPIFY_DB_PATH.exists():
        return None
    try:
        with _connect() as connection:
            row = connection.execute(
                """
                SELECT finished_at
                FROM sync_runs
                WHERE storefront = 'shopify'
                  AND status IN ('success', 'partial')
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            return str(row["finished_at"]) if row else None
    except Exception:
        return None


def build_shopify_live_status() -> dict[str, Any]:
    config, missing, summary = load_shopify_live_config()
    db_exists = SHOPIFY_DB_PATH.exists()

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
            "shop_domain": summary.get("shop_domain"),
            "api_version": summary.get("api_version"),
            "bootstrap_env_path": summary.get("bootstrap_env_path"),
            "bootstrap_env_exists": summary.get("bootstrap_env_exists"),
        },
        "runtime_db": {
            "path": str(SHOPIFY_DB_PATH),
            "exists": db_exists,
        },
        "last_sync": last_sync,
    }


class ShopifyLiveClient:
    def __init__(self, config: ShopifyLiveConfig) -> None:
        self.domain = config.domain
        self.client_id = config.client_id
        self.client_secret = config.client_secret
        self.api_version = config.api_version
        self.verify_ssl = config.verify_ssl
        self.base_url = f"https://{self.domain}/admin/api/{self.api_version}"
        self._token: Optional[str] = None
        self._token_expires_at = 0.0
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

    def _parse_next_link(self, link_header: Optional[str]) -> Optional[str]:
        if not link_header:
            return None
        chunks = [part.strip() for part in str(link_header).split(",") if part.strip()]
        for chunk in chunks:
            if ";" not in chunk:
                continue
            url_part, attrs = chunk.split(";", 1)
            attrs_text = attrs.lower()
            if 'rel="next"' not in attrs_text and "rel=next" not in attrs_text:
                continue
            cleaned = url_part.strip()
            if cleaned.startswith("<") and cleaned.endswith(">"):
                cleaned = cleaned[1:-1]
            cleaned = cleaned.strip()
            if cleaned:
                return cleaned
        return None

    def _request_with_urllib(
        self,
        method: str,
        url: str,
        *,
        params: Optional[dict[str, Any]] = None,
        body_obj: Any = None,
        timeout: int = 25,
        headers: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:
        final_url = url
        if params:
            query = urlparse.urlencode(params, doseq=True)
            separator = "&" if "?" in final_url else "?"
            final_url = f"{final_url}{separator}{query}"

        body_bytes: Optional[bytes] = None
        if body_obj is not None:
            body_bytes = json.dumps(body_obj).encode("utf-8")

        request = urlrequest.Request(
            final_url,
            data=body_bytes,
            headers=dict(headers or self._headers()),
            method=method.upper(),
        )

        context = self._ssl_context()

        try:
            with urlrequest.urlopen(request, timeout=timeout, context=context) as response:
                status_code = int(getattr(response, "status", 200))
                text = response.read().decode("utf-8", errors="replace")
                headers = dict(response.headers.items())
        except urlerror.HTTPError as exc:
            status_code = int(getattr(exc, "code", 500))
            response_body = b""
            try:
                response_body = exc.read()
            except Exception:
                response_body = b""
            text = response_body.decode("utf-8", errors="replace")
            headers = dict(exc.headers.items()) if exc.headers is not None else {}
        except urlerror.URLError as exc:
            raise ShopifyLiveError(f"Request to Shopify failed: {exc}", status_code=502) from exc
        except TimeoutError as exc:
            raise ShopifyLiveError("Request to Shopify timed out.", status_code=504) from exc
        except OSError as exc:
            raise ShopifyLiveError(
                f"Shopify request failed due to local SSL/CA configuration: {exc}",
                status_code=502,
            ) from exc

        payload: Any = {}
        if text:
            try:
                payload = json.loads(text)
            except ValueError:
                payload = {"raw_response": text[:1000]}

        if not (200 <= status_code < 300):
            raise ShopifyLiveError(
                self._extract_error_message(payload, status_code),
                status_code=status_code,
                payload=payload,
                response_text=text[:1000],
            )

        return {
            "status_code": status_code,
            "payload": payload,
            "headers": headers,
            "text": text,
            "next_url": self._parse_next_link(headers.get("Link") or headers.get("link")),
        }

    def _extract_error_message(self, payload: Any, status_code: int) -> str:
        if isinstance(payload, dict):
            if "errors" in payload:
                return str(payload.get("errors"))
            if "error" in payload:
                return str(payload.get("error"))
            if "message" in payload:
                return str(payload.get("message"))
            if "raw_response" in payload:
                return str(payload.get("raw_response"))
        return f"Shopify request failed with HTTP {status_code}."

    def _get_token(self) -> str:
        now = time.time()
        if self._token and (self._token_expires_at - now) >= 60:
            return self._token

        requests = self._requests_module()
        url = f"https://{self.domain}/admin/oauth/access_token"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }
        if requests:
            try:
                response = requests.post(
                    url,
                    headers={"Accept": "application/json", "Content-Type": "application/json"},
                    json=payload,
                    timeout=20,
                    verify=self.verify_ssl,
                )
            except requests.exceptions.Timeout as exc:
                raise ShopifyLiveError("Token request to Shopify timed out.", status_code=504) from exc
            except requests.exceptions.RequestException as exc:
                raise ShopifyLiveError(f"Token request to Shopify failed: {exc}", status_code=502) from exc
            except OSError as exc:
                raise ShopifyLiveError(
                    f"Token request failed due to local SSL/CA configuration: {exc}",
                    status_code=502,
                ) from exc

            token_payload: Any = {}
            if response.text:
                try:
                    token_payload = response.json()
                except ValueError:
                    token_payload = {"raw_response": (response.text or "")[:1000]}

            status_code = int(response.status_code)
            response_text = (response.text or "")[:1000]
        else:
            raw = self._request_with_urllib(
                "POST",
                url,
                body_obj=payload,
                timeout=20,
                headers={"Accept": "application/json", "Content-Type": "application/json"},
            )
            token_payload = raw.get("payload")
            status_code = int(raw.get("status_code") or 200)
            response_text = str(raw.get("text") or "")[:1000]

        if not (200 <= status_code < 300):
            raise ShopifyLiveError(
                self._extract_error_message(token_payload, status_code),
                status_code=status_code,
                payload=token_payload,
                response_text=response_text,
            )

        token = str(token_payload.get("access_token") if isinstance(token_payload, dict) else "").strip()
        if not token:
            raise ShopifyLiveError("Shopify token endpoint response did not include access_token.", status_code=status_code)

        expires_in = 3600
        if isinstance(token_payload, dict):
            try:
                expires = int(token_payload.get("expires_in") or 3600)
                if expires > 0:
                    expires_in = expires
            except (TypeError, ValueError):
                expires_in = 3600

        self._token = token
        self._token_expires_at = now + expires_in
        return token

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self._get_token(),
            "User-Agent": "dashboard-combined/1.0",
        }

    def _request(self, method: str, url: str, params: Optional[dict[str, Any]] = None) -> Any:
        requests = self._requests_module()
        max_attempts = 3

        for attempt in range(1, max_attempts + 1):
            if requests:
                try:
                    response = requests.request(
                        method=method.upper(),
                        url=url,
                        headers=self._headers(),
                        params=params,
                        timeout=25,
                        verify=self.verify_ssl,
                    )
                except requests.exceptions.Timeout as exc:
                    raise ShopifyLiveError("Request to Shopify timed out.", status_code=504) from exc
                except requests.exceptions.RequestException as exc:
                    raise ShopifyLiveError(f"Request to Shopify failed: {exc}", status_code=502) from exc
                except OSError as exc:
                    raise ShopifyLiveError(
                        f"Shopify request failed due to local SSL/CA configuration: {exc}",
                        status_code=502,
                    ) from exc

                if response.status_code == 401 and attempt == 1:
                    self._token = None
                    self._token_expires_at = 0.0
                    continue

                if response.status_code == 429 and attempt < max_attempts:
                    retry_after_header = response.headers.get("Retry-After")
                    try:
                        retry_after = float(retry_after_header) if retry_after_header is not None else 1.0
                    except (TypeError, ValueError):
                        retry_after = 1.0
                    time.sleep(max(0.5, retry_after + 0.25))
                    continue

                text = response.text or ""
                if not text:
                    payload: Any = {}
                else:
                    try:
                        payload = response.json()
                    except ValueError:
                        payload = {"raw_response": text[:1000]}

                if not (200 <= response.status_code < 300):
                    raise ShopifyLiveError(
                        self._extract_error_message(payload, response.status_code),
                        status_code=response.status_code,
                        payload=payload,
                        response_text=text[:1000],
                    )

                return payload, {
                    "next_url": response.links.get("next", {}).get("url"),
                    "headers": dict(response.headers),
                }

            try:
                raw = self._request_with_urllib(
                    method,
                    url,
                    params=params,
                    timeout=25,
                )
            except ShopifyLiveError as exc:
                if exc.status_code == 401 and attempt == 1:
                    self._token = None
                    self._token_expires_at = 0.0
                    continue
                if exc.status_code == 429 and attempt < max_attempts:
                    time.sleep(1.0)
                    continue
                raise

            return raw.get("payload"), raw

        raise ShopifyLiveError("Shopify request failed after retries.", status_code=502)

    def get_orders_page(
        self,
        *,
        status: str,
        limit: int,
        cursor_url: Optional[str] = None,
        updated_at_min: Optional[str] = None,
    ) -> tuple[list[dict[str, Any]], Optional[str]]:
        normalized_limit = min(max(int(limit), 1), 250)
        normalized_status = (status or "any").strip() or "any"

        if cursor_url:
            payload, response = self._request("GET", cursor_url, params=None)
        else:
            params: dict[str, Any] = {"status": normalized_status, "limit": normalized_limit}
            if updated_at_min:
                params["updated_at_min"] = updated_at_min
            payload, response = self._request(
                "GET",
                f"{self.base_url}/orders.json",
                params=params,
            )

        orders = payload.get("orders") if isinstance(payload, dict) else None
        if not isinstance(orders, list):
            orders = []

        next_url = None
        if isinstance(response, dict):
            next_url = response.get("next_url")
        else:
            next_url = response.links.get("next", {}).get("url") if hasattr(response, "links") else None
        if next_url is not None:
            next_url = str(next_url)

        clean_orders = [item for item in orders if isinstance(item, dict)]
        return clean_orders, next_url

    def get_order_transactions(self, order_id: str) -> list[dict[str, Any]]:
        payload, _ = self._request("GET", f"{self.base_url}/orders/{order_id}/transactions.json", params=None)
        rows = payload.get("transactions") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            return []
        return [item for item in rows if isinstance(item, dict)]

    def get_product_image(self, product_id: str) -> Optional[str]:
        """Fetch the primary image URL for a product. Returns None if not found."""
        try:
            payload, _ = self._request(
                "GET",
                f"{self.base_url}/products/{product_id}.json",
                params={"fields": "id,image,images"},
            )
        except ShopifyLiveError:
            return None
        product = payload.get("product") if isinstance(payload, dict) else None
        if not isinstance(product, dict):
            return None
        image = product.get("image")
        if isinstance(image, dict) and image.get("src"):
            return str(image["src"]).strip() or None
        images = product.get("images")
        if isinstance(images, list) and images:
            first = images[0]
            if isinstance(first, dict) and first.get("src"):
                return str(first["src"]).strip() or None
        return None


def sync_shopify_live(
    *,
    status: str = "any",
    page_limit: int = 250,
    max_pages: int = 500,
    include_line_items: bool = True,
    include_fulfillments: bool = True,
    include_refunds: bool = True,
    include_transactions: bool = True,
    updated_at_min: Optional[str] = None,
) -> dict[str, Any]:
    started_at = _utc_now_iso()
    started_monotonic = time.monotonic()

    config, missing, config_summary = load_shopify_live_config()
    if config is None:
        return {
            "status": "error",
            "provider": "shopify",
            "error": "Shopify credentials are not configured.",
            "missing_env": missing,
            "config": config_summary,
        }

    db_path = init_shopify_db()

    summary: dict[str, Any] = {
        "status_filter": status,
        "page_limit": min(max(int(page_limit), 1), 250),
        "max_pages": min(max(int(max_pages), 1), 20000),
        "updated_at_min": updated_at_min,
        "include_line_items": bool(include_line_items),
        "include_fulfillments": bool(include_fulfillments),
        "include_refunds": bool(include_refunds),
        "include_transactions": bool(include_transactions),
        "pages_fetched": 0,
        "orders_seen": 0,
        "orders_inserted": 0,
        "orders_updated": 0,
        "orders_unchanged": 0,
        "line_items_seen": 0,
        "line_items_inserted": 0,
        "line_items_updated": 0,
        "line_items_unchanged": 0,
        "fulfillments_seen": 0,
        "fulfillments_inserted": 0,
        "fulfillments_updated": 0,
        "fulfillments_unchanged": 0,
        "refunds_seen": 0,
        "refunds_inserted": 0,
        "refunds_updated": 0,
        "refunds_unchanged": 0,
        "transactions_seen": 0,
        "transactions_inserted": 0,
        "transactions_updated": 0,
        "transactions_unchanged": 0,
        "error_count": 0,
        "started_at": started_at,
        "finished_at": None,
        "duration_seconds": None,
    }
    errors: list[dict[str, Any]] = []

    def add_error(scope: str, message: str, **meta: Any) -> None:
        summary["error_count"] += 1
        row = {"scope": scope, "error": str(message)}
        row.update(meta)
        if len(errors) < 250:
            errors.append(row)

    def apply_change(prefix: str, change: str) -> None:
        key = f"{prefix}_{change}"
        if key in summary:
            summary[key] += 1

    sync_status = "success"

    try:
        client = ShopifyLiveClient(config)

        with _connect() as connection:
            cursor_url: Optional[str] = None
            page_number = 0

            while page_number < int(summary["max_pages"]):
                page_number += 1
                summary["pages_fetched"] += 1

                try:
                    orders, cursor_url = client.get_orders_page(
                        status=str(summary["status_filter"]),
                        limit=int(summary["page_limit"]),
                        cursor_url=cursor_url,
                        updated_at_min=updated_at_min,
                    )
                except ShopifyLiveError as exc:
                    add_error(
                        "orders_page",
                        str(exc),
                        page_number=page_number,
                        status_code=exc.status_code,
                    )
                    sync_status = "error" if summary["orders_seen"] == 0 else "partial"
                    break

                summary["orders_seen"] += len(orders)

                for order in orders:
                    order_id = _clean_text(order.get("id"))
                    if not order_id:
                        add_error("order", "Order payload has no id.")
                        sync_status = "partial"
                        continue

                    try:
                        order_result = upsert_order(connection, order)
                        apply_change("orders", str(order_result.get("change") or ""))

                        if include_line_items:
                            line_items = order.get("line_items") if isinstance(order.get("line_items"), list) else []
                            keep_ids: list[str] = []
                            summary["line_items_seen"] += len(line_items)
                            for idx, line_item in enumerate(line_items):
                                if not isinstance(line_item, dict):
                                    continue
                                result = upsert_line_item(connection, order_id, line_item, idx)
                                apply_change("line_items", str(result.get("change") or ""))
                                record_id = _clean_text(result.get("id"))
                                if record_id:
                                    keep_ids.append(record_id)
                            delete_stale_order_rows(connection, "order_line_items", order_id, keep_ids)

                        if include_fulfillments:
                            fulfillments = order.get("fulfillments") if isinstance(order.get("fulfillments"), list) else []
                            keep_ids = []
                            summary["fulfillments_seen"] += len(fulfillments)
                            for idx, fulfillment in enumerate(fulfillments):
                                if not isinstance(fulfillment, dict):
                                    continue
                                result = upsert_fulfillment(connection, order_id, fulfillment, idx)
                                apply_change("fulfillments", str(result.get("change") or ""))
                                record_id = _clean_text(result.get("id"))
                                if record_id:
                                    keep_ids.append(record_id)
                            delete_stale_order_rows(connection, "order_fulfillments", order_id, keep_ids)

                        if include_refunds:
                            refunds = order.get("refunds") if isinstance(order.get("refunds"), list) else []
                            keep_ids = []
                            summary["refunds_seen"] += len(refunds)
                            for idx, refund in enumerate(refunds):
                                if not isinstance(refund, dict):
                                    continue
                                result = upsert_refund(connection, order_id, refund, idx)
                                apply_change("refunds", str(result.get("change") or ""))
                                record_id = _clean_text(result.get("id"))
                                if record_id:
                                    keep_ids.append(record_id)
                            delete_stale_order_rows(connection, "order_refunds", order_id, keep_ids)

                        if include_transactions:
                            transactions: list[dict[str, Any]] = []
                            inline = order.get("transactions") if isinstance(order.get("transactions"), list) else []
                            inline_clean = [item for item in inline if isinstance(item, dict)]
                            if inline_clean:
                                transactions = inline_clean
                            else:
                                try:
                                    transactions = client.get_order_transactions(order_id)
                                except ShopifyLiveError as exc:
                                    add_error(
                                        "order_transactions",
                                        str(exc),
                                        order_id=order_id,
                                        status_code=exc.status_code,
                                    )
                                    sync_status = "partial"
                                    transactions = []

                            keep_ids = []
                            summary["transactions_seen"] += len(transactions)
                            for idx, transaction in enumerate(transactions):
                                result = upsert_transaction(connection, order_id, transaction, idx)
                                apply_change("transactions", str(result.get("change") or ""))
                                record_id = _clean_text(result.get("id"))
                                if record_id:
                                    keep_ids.append(record_id)

                            delete_stale_order_rows(connection, "order_transactions", order_id, keep_ids)
                            refresh_order_payment_method_from_transactions(
                                connection,
                                order_id,
                                fallback_method=_extract_order_payment_method(order),
                            )
                    except Exception as exc:
                        add_error("order_process", str(exc), order_id=order_id)
                        sync_status = "partial"

                connection.commit()

                if not cursor_url:
                    break

            if cursor_url:
                add_error("pagination", "Max page limit reached before Shopify cursor pagination ended.", max_pages=summary["max_pages"])
                sync_status = "partial"

            if summary["error_count"] > 0 and sync_status == "success":
                sync_status = "partial"

            summary["finished_at"] = _utc_now_iso()
            summary["duration_seconds"] = round(time.monotonic() - started_monotonic, 3)
            summary["total_inserted"] = (
                summary["orders_inserted"]
                + summary["line_items_inserted"]
                + summary["fulfillments_inserted"]
                + summary["refunds_inserted"]
                + summary["transactions_inserted"]
            )
            summary["total_updated"] = (
                summary["orders_updated"]
                + summary["line_items_updated"]
                + summary["fulfillments_updated"]
                + summary["refunds_updated"]
                + summary["transactions_updated"]
            )
            summary["total_unchanged"] = (
                summary["orders_unchanged"]
                + summary["line_items_unchanged"]
                + summary["fulfillments_unchanged"]
                + summary["refunds_unchanged"]
                + summary["transactions_unchanged"]
            )

            _record_sync_run(
                connection,
                started_at=started_at,
                finished_at=str(summary["finished_at"]),
                status=sync_status,
                storefront="shopify",
                summary=summary,
                errors=errors,
            )
            connection.commit()

        return {
            "status": sync_status,
            "provider": "shopify",
            "database_path": db_path,
            "summary": summary,
            "errors": errors,
            "config": config_summary,
        }
    except ShopifyLiveError as exc:
        summary["finished_at"] = _utc_now_iso()
        summary["duration_seconds"] = round(time.monotonic() - started_monotonic, 3)
        add_error("shopify", str(exc), status_code=exc.status_code)
        return {
            "status": "error",
            "provider": "shopify",
            "database_path": db_path,
            "summary": summary,
            "errors": errors,
            "error": str(exc),
            "status_code": exc.status_code,
            "config": config_summary,
        }
    except Exception as exc:
        LOGGER.exception("shopify live sync failed")
        summary["finished_at"] = _utc_now_iso()
        summary["duration_seconds"] = round(time.monotonic() - started_monotonic, 3)
        add_error("shopify", str(exc))
        return {
            "status": "error",
            "provider": "shopify",
            "database_path": db_path,
            "summary": summary,
            "errors": errors,
            "error": str(exc),
            "config": config_summary,
        }
