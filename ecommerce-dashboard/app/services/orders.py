from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from app.config import ALLOWED_MARKETPLACES, KAUFLAND_DB_PATH, SHOPIFY_DB_PATH
from app.db import fetch_enrichment_map
from app.services.importers.shopify_live import lookup_product_images


NON_EU_PAYPAL_COUNTRY_CODES = {"CH", "GB", "UK", "US", "CA", "AU"}

# EEA (European Economic Area) country codes for Shopify Payments fee tiers
EEA_COUNTRY_CODES = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
    "DE", "GR", "HU", "IS", "IE", "IT", "LV", "LI", "LT", "LU",
    "MT", "NL", "NO", "PL", "PT", "RO", "SK", "SI", "ES", "SE",
}


def _connect_readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    return connection


def _safe_json_load(raw_text: Any) -> dict[str, Any]:
    if not isinstance(raw_text, str) or not raw_text.strip():
        return {}
    try:
        value = json.loads(raw_text)
    except (TypeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _parse_iso(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _to_iso_utc(value: Any) -> str:
    parsed = _parse_iso(value)
    if parsed is None:
        return ""
    return parsed.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_eur_cents(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return int(round(float(value) * 100))
    if isinstance(value, float):
        return int(round(value * 100))

    text = str(value).strip().replace(",", ".")
    if not text:
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    return int(round(parsed * 100))


def _to_kaufland_cents(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return int(round(value))

    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace(",", ".")
    if "." in normalized:
        try:
            parsed = float(normalized)
        except ValueError:
            return None
        return int(round(parsed * 100)) if abs(parsed) < 1000 else int(round(parsed))
    try:
        return int(normalized)
    except ValueError:
        return None


def _normalize_name(*parts: Any) -> str:
    values: list[str] = []
    for part in parts:
        if part is None:
            continue
        text = str(part).strip()
        if text:
            values.append(text)
    return " ".join(values).strip()


def _first_non_empty(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _kaufland_unit_value(unit: dict[str, Any], key: str) -> str:
    value = unit.get(key)
    return str(value).strip() if value is not None else ""


def _kaufland_unit_name(unit: dict[str, Any], prefix: str) -> str:
    return _normalize_name(
        _kaufland_unit_value(unit, f"{prefix}_first_name"),
        _kaufland_unit_value(unit, f"{prefix}_last_name"),
    )


def _kaufland_unit_has_address(unit: dict[str, Any], prefix: str) -> bool:
    keys = [
        f"{prefix}_first_name",
        f"{prefix}_last_name",
        f"{prefix}_street",
        f"{prefix}_house_number",
        f"{prefix}_postcode",
        f"{prefix}_city",
        f"{prefix}_country",
    ]
    for key in keys:
        if _kaufland_unit_value(unit, key):
            return True
    return False


def _build_kaufland_address(unit: dict[str, Any], prefix: str) -> dict[str, Any]:
    raw_payload = unit.get("raw") if isinstance(unit.get("raw"), dict) else {}
    raw_address = raw_payload.get(f"{prefix}_address") if isinstance(raw_payload.get(f"{prefix}_address"), dict) else {}

    def pick(column_suffix: str, raw_key: str) -> Optional[str]:
        value = _kaufland_unit_value(unit, f"{prefix}_{column_suffix}")
        if value:
            return value
        fallback = _first_non_empty(raw_address.get(raw_key))
        return fallback or None

    return {
        "first_name": pick("first_name", "first_name"),
        "last_name": pick("last_name", "last_name"),
        "street": pick("street", "street"),
        "house_number": pick("house_number", "house_number"),
        "postcode": pick("postcode", "postcode"),
        "city": pick("city", "city"),
        "country": pick("country", "country"),
        "phone": pick("phone", "phone"),
    }


def _extract_shopify_shipping_cents(order_payload: dict[str, Any]) -> int:
    shipping_set = order_payload.get("total_shipping_price_set")
    if isinstance(shipping_set, dict):
        shop_money = shipping_set.get("shop_money")
        if isinstance(shop_money, dict):
            cents = _to_eur_cents(shop_money.get("amount"))
            if cents is not None:
                return cents
        presentment_money = shipping_set.get("presentment_money")
        if isinstance(presentment_money, dict):
            cents = _to_eur_cents(presentment_money.get("amount"))
            if cents is not None:
                return cents

    shipping_lines = order_payload.get("shipping_lines")
    if isinstance(shipping_lines, list):
        total = 0
        has_value = False
        for item in shipping_lines:
            if not isinstance(item, dict):
                continue
            cents = _to_eur_cents(item.get("discounted_price"))
            if cents is None:
                cents = _to_eur_cents(item.get("price"))
            if cents is None:
                continue
            total += cents
            has_value = True
        if has_value:
            return total

    return 0


def _estimate_shopify_paypal_fee_cents(order_payload: dict[str, Any], total_cents: int) -> Optional[int]:
    gateways_raw = order_payload.get("payment_gateway_names")
    gateways = [str(item).strip().lower() for item in gateways_raw if item is not None] if isinstance(gateways_raw, list) else []
    if not any("paypal" in item for item in gateways):
        return None

    billing = order_payload.get("billing_address")
    shipping = order_payload.get("shipping_address")
    billing_dict = billing if isinstance(billing, dict) else {}
    shipping_dict = shipping if isinstance(shipping, dict) else {}
    country_code = (
        _first_non_empty(
            billing_dict.get("country_code"),
            shipping_dict.get("country_code"),
            billing_dict.get("country"),
            shipping_dict.get("country"),
        )
        .upper()
        .strip()
    )

    rate = 0.0299
    fixed_fee = 0.39
    if country_code in NON_EU_PAYPAL_COUNTRY_CODES:
        rate += 0.0199

    total_eur = total_cents / 100.0
    fee_eur = (total_eur * rate) + fixed_fee
    return int(round(fee_eur * 100))


def _estimate_shopify_payments_fee_cents(
    order_payload: dict[str, Any],
    total_cents: int,
    payment_method: str = "",
) -> Optional[int]:
    """Estimate Shopify Payments fees (cards, Klarna, Bancontact/EPS/iDEAL).

    Rates (user-configured, Shopify Payments EU):
      - EEA cards (Visa/Mastercard/etc.): 2.1 % + 0.30 EUR
      - International cards:              3.2 % + 0.30 EUR
      - Klarna:                           2.99% + 0.35 EUR
      - Bancontact / EPS / iDEAL:         2.4 % + 0.25 EUR
      - Currency conversion surcharge:    2.0 % (non-EUR orders)
    """
    if total_cents <= 0:
        return 0

    gateways_raw = order_payload.get("payment_gateway_names")
    gateways = [str(item).strip().lower() for item in gateways_raw if item is not None] if isinstance(gateways_raw, list) else []
    # If PayPal is the gateway, this isn't a Shopify Payments order
    if any("paypal" in item for item in gateways):
        return None

    total_eur = total_cents / 100.0
    pm_lower = str(payment_method or "").strip().lower()

    # ── Determine processing fee rate + fixed fee based on payment method ──
    if pm_lower in ("klarna", "shopify payments") or "klarna" in pm_lower:
        # Klarna (shows as "Shopify Payments" in the payment_method column)
        rate = 0.0299
        fixed_fee = 0.35
    elif pm_lower in ("bancontact", "eps", "ideal", "sofort"):
        # Bancontact / EPS / iDEAL
        rate = 0.024
        fixed_fee = 0.25
    else:
        # Card payments (Visa, Mastercard, Amex, etc.) — EEA vs International
        billing = order_payload.get("billing_address")
        shipping = order_payload.get("shipping_address")
        billing_dict = billing if isinstance(billing, dict) else {}
        shipping_dict = shipping if isinstance(shipping, dict) else {}
        country_code = (
            _first_non_empty(
                billing_dict.get("country_code"),
                shipping_dict.get("country_code"),
                billing_dict.get("country"),
                shipping_dict.get("country"),
            )
            .upper()
            .strip()
        )
        if country_code in EEA_COUNTRY_CODES:
            rate = 0.021
        else:
            rate = 0.032
        fixed_fee = 0.30

    fee_eur = (total_eur * rate) + fixed_fee

    # ── Currency conversion surcharge (2%) for non-EUR orders ──
    tps = order_payload.get("total_price_set")
    if isinstance(tps, dict):
        presentment = tps.get("presentment_money")
        if isinstance(presentment, dict):
            presentment_currency = str(presentment.get("currency_code", "EUR")).upper()
            if presentment_currency != "EUR":
                fee_eur += total_eur * 0.02

    return int(round(fee_eur * 100))


def _shopify_summary_from_row(row: sqlite3.Row) -> dict[str, Any]:
    order_payload = _safe_json_load(row["raw_json"])

    total_cents = _to_eur_cents(row["total_price"]) or 0

    # ── Fee resolution chain with source tracking ──
    fee_cents = _to_eur_cents(row["fee_total"])
    fee_source: str = "api"  # real transaction data from Shopify API

    if fee_cents is None:
        fee_cents = _to_eur_cents(row["estimated_paypal_fee"])
        fee_source = "stored_estimate"

    if fee_cents is None:
        fee_cents = _estimate_shopify_paypal_fee_cents(order_payload, total_cents)
        fee_source = "estimated"

    if fee_cents is None:
        fee_cents = _estimate_shopify_payments_fee_cents(order_payload, total_cents, _first_non_empty(row["payment_method"]))
        fee_source = "estimated"

    if fee_cents is None:
        fee_cents = 0
        fee_source = "none"

    # Detect currency conversion surcharge for estimated fees
    if fee_source == "estimated":
        tps = order_payload.get("total_price_set")
        if isinstance(tps, dict):
            presentment = tps.get("presentment_money")
            if isinstance(presentment, dict):
                presentment_currency = str(presentment.get("currency_code", "EUR")).upper()
                if presentment_currency != "EUR":
                    fee_source = "estimated_fx"

    after_fees_cents = _to_eur_cents(row["net_total"])
    if after_fees_cents is None:
        after_fees_cents = _to_eur_cents(row["estimated_net_after_fee"])
    if after_fees_cents is None:
        after_fees_cents = max(total_cents - fee_cents, 0)

    customer = _normalize_name(row["customer_first_name"], row["customer_last_name"])
    if not customer:
        customer = _first_non_empty(row["customer_email"], row["email"], order_payload.get("email"), "Unbekannt")

    article = _first_non_empty(row["first_article"])
    if not article:
        line_items = order_payload.get("line_items")
        if isinstance(line_items, list) and line_items:
            first = line_items[0]
            if isinstance(first, dict):
                article = _first_non_empty(first.get("title"))
    if not article:
        article = "-"

    created_iso = _to_iso_utc(row["created_at"])
    shipping_cents = _extract_shopify_shipping_cents(order_payload)

    # Line items count (from subquery, fallback to 1)
    try:
        line_items_count = int(row["line_items_count"])
    except (ValueError, TypeError, IndexError, KeyError):
        line_items_count = 1

    summary = {
        "marketplace": "shopify",
        "order_id": str(row["id"]),
        "external_order_id": _first_non_empty(row["name"], row["id"]),
        "order_date": created_iso,
        "customer": customer,
        "article": article,
        "line_items_count": line_items_count,
        "total_cents": total_cents,
        "fees_cents": max(fee_cents, 0),
        "after_fees_cents": max(after_fees_cents, 0),
        "shipping_cents": max(shipping_cents, 0),
        "currency": _first_non_empty(row["currency"], "EUR").upper(),
        "fulfillment_status": _first_non_empty(row["fulfillment_status"], row["financial_status"], "unknown"),
        "payment_method": _first_non_empty(row["payment_method"], "Shopify"),
        "fee_source": fee_source,
    }
    summary["raw_status"] = summary.get("fulfillment_status")
    summary["financial_status"] = _first_non_empty(row["financial_status"], "")
    return summary


def _kaufland_summary_from_row(row: sqlite3.Row) -> dict[str, Any]:
    raw_payload = _safe_json_load(row["raw_json"])
    total_cents = _to_kaufland_cents(row["units_price_sum"]) or 0
    after_fees_cents = _to_kaufland_cents(row["revenue_gross_sum"]) or total_cents
    fees_cents = total_cents - after_fees_cents
    if fees_cents < 0:
        fees_cents = 0

    shipping_cents = _to_kaufland_cents(row["shipping_sum"]) or 0

    customer = _first_non_empty(row["customer_name"])
    if not customer:
        buyer = raw_payload.get("buyer") if isinstance(raw_payload.get("buyer"), dict) else {}
        customer = _first_non_empty(buyer.get("email"), "Unbekannt")

    article = _first_non_empty(row["first_article"])
    if not article:
        order_units = raw_payload.get("order_units")
        if isinstance(order_units, list) and order_units:
            first_unit = order_units[0]
            if isinstance(first_unit, dict):
                product = first_unit.get("product") if isinstance(first_unit.get("product"), dict) else {}
                article = _first_non_empty(product.get("title"))
    if not article:
        article = "-"

    order_date_iso = _to_iso_utc(row["ts_created_iso"])

    currency = _first_non_empty(raw_payload.get("currency"))
    if not currency:
        order_units = raw_payload.get("order_units")
        if isinstance(order_units, list) and order_units:
            first_unit = order_units[0]
            if isinstance(first_unit, dict):
                currency = _first_non_empty(first_unit.get("currency"))

    # Line items count (from subquery, fallback to 1)
    try:
        line_items_count = int(row["line_items_count"])
    except (ValueError, TypeError, IndexError, KeyError):
        line_items_count = 1

    summary = {
        "marketplace": "kaufland",
        "order_id": str(row["id_order"]),
        "external_order_id": str(row["id_order"]),
        "order_date": order_date_iso,
        "customer": customer,
        "article": article,
        "line_items_count": line_items_count,
        "total_cents": total_cents,
        "fees_cents": fees_cents,
        "after_fees_cents": max(after_fees_cents, 0),
        "shipping_cents": max(shipping_cents, 0),
        "currency": _first_non_empty(currency, "EUR").upper(),
        "fulfillment_status": _first_non_empty(row["unit_status"], "unknown"),
        "payment_method": "Kaufland Settlement",
        "fee_source": "api",
    }
    summary["raw_status"] = summary.get("fulfillment_status")
    summary["financial_status"] = ""
    return summary


def _load_shopify_orders() -> list[dict[str, Any]]:
    if not SHOPIFY_DB_PATH.exists():
        return []

    with _connect_readonly(SHOPIFY_DB_PATH) as connection:
        rows = connection.execute(
            """
            SELECT
                o.id,
                o.name,
                o.created_at,
                o.updated_at,
                o.payment_method,
                o.financial_status,
                o.fulfillment_status,
                o.total_price,
                o.currency,
                o.customer_first_name,
                o.customer_last_name,
                o.customer_email,
                o.email,
                o.estimated_paypal_fee,
                o.estimated_net_after_fee,
                o.raw_json,
                (
                    SELECT li.title
                    FROM order_line_items li
                    WHERE li.order_id = o.id
                    ORDER BY li.id ASC
                    LIMIT 1
                ) AS first_article,
                (
                    SELECT COUNT(*)
                    FROM order_line_items li2
                    WHERE li2.order_id = o.id
                ) AS line_items_count,
                (
                    SELECT ROUND(SUM(CAST(NULLIF(t.fee_amount, '') AS REAL)), 2)
                    FROM order_transactions t
                    WHERE t.order_id = o.id AND COALESCE(t.fee_amount, '') <> ''
                ) AS fee_total,
                (
                    SELECT ROUND(SUM(CAST(NULLIF(t.net_amount, '') AS REAL)), 2)
                    FROM order_transactions t
                    WHERE t.order_id = o.id AND COALESCE(t.net_amount, '') <> ''
                ) AS net_total
            FROM orders o
            ORDER BY COALESCE(o.created_at, '') DESC, o.id DESC
            """
        ).fetchall()

    return [_shopify_summary_from_row(row) for row in rows]


def _load_kaufland_orders() -> list[dict[str, Any]]:
    if not KAUFLAND_DB_PATH.exists():
        return []

    with _connect_readonly(KAUFLAND_DB_PATH) as connection:
        rows = connection.execute(
            """
            SELECT
                o.id_order,
                o.ts_created_iso,
                o.raw_json,
                (
                    SELECT COALESCE(SUM(CAST(COALESCE(NULLIF(ou.price, ''), 0) AS REAL)), 0)
                    FROM order_units ou
                    WHERE ou.id_order = o.id_order
                ) AS units_price_sum,
                (
                    SELECT COALESCE(SUM(CAST(COALESCE(NULLIF(ou.revenue_gross, ''), 0) AS REAL)), 0)
                    FROM order_units ou
                    WHERE ou.id_order = o.id_order
                ) AS revenue_gross_sum,
                (
                    SELECT COALESCE(SUM(CAST(COALESCE(NULLIF(ou.shipping_rate, ''), 0) AS REAL)), 0)
                    FROM order_units ou
                    WHERE ou.id_order = o.id_order
                ) AS shipping_sum,
                (
                    SELECT ou.product_title
                    FROM order_units ou
                    WHERE ou.id_order = o.id_order
                    ORDER BY COALESCE(ou.ts_created_iso, '') ASC, ou.id_order_unit ASC
                    LIMIT 1
                ) AS first_article,
                (
                    SELECT COUNT(*)
                    FROM order_units ou2
                    WHERE ou2.id_order = o.id_order
                ) AS line_items_count,
                (
                    SELECT TRIM(COALESCE(ou.shipping_first_name, '') || ' ' || COALESCE(ou.shipping_last_name, ''))
                    FROM order_units ou
                    WHERE ou.id_order = o.id_order
                    ORDER BY
                        CASE
                            WHEN TRIM(COALESCE(ou.shipping_first_name, '') || ' ' || COALESCE(ou.shipping_last_name, '')) <> '' THEN 0
                            ELSE 1
                        END,
                        COALESCE(ou.ts_created_iso, '') ASC,
                        ou.id_order_unit ASC
                    LIMIT 1
                ) AS customer_name,
                (
                    SELECT COALESCE(ou.status, '')
                    FROM order_units ou
                    WHERE ou.id_order = o.id_order
                    ORDER BY COALESCE(ou.ts_created_iso, '') ASC, ou.id_order_unit ASC
                    LIMIT 1
                ) AS unit_status
            FROM orders o
            ORDER BY COALESCE(o.ts_created_iso, '') DESC, o.id_order DESC
            """
        ).fetchall()

    return [_kaufland_summary_from_row(row) for row in rows]


def _merge_enrichment(base_order: dict[str, Any], enrichment_map: dict[tuple[str, str], dict[str, Any]]) -> dict[str, Any]:
    key = (base_order["marketplace"], base_order["order_id"])
    enrichment = enrichment_map.get(key, {})

    purchase_cost = enrichment.get("purchase_cost_cents")
    purchase_cost_cents = int(purchase_cost) if isinstance(purchase_cost, int) else None
    if purchase_cost_cents is None:
        purchase_cost_cents = 0

    profit_cents = int(base_order["after_fees_cents"]) - purchase_cost_cents

    invoice_payload: Optional[dict[str, Any]] = None
    if enrichment.get("invoice_document_id"):
        invoice_payload = {
            "document_id": enrichment.get("invoice_document_id"),
            "original_filename": enrichment.get("original_filename"),
            "stored_filename": enrichment.get("stored_filename"),
            "mime_type": enrichment.get("mime_type"),
            "uploaded_at": enrichment.get("uploaded_at"),
        }

    merged = {
        **base_order,
        "purchase_cost_cents": purchase_cost_cents,
        "purchase_currency": enrichment.get("purchase_currency") or "EUR",
        "purchase_supplier": enrichment.get("supplier_name"),
        "purchase_notes": enrichment.get("purchase_notes"),
        "invoice": invoice_payload,
        "profit_cents": profit_cents,
    }
    return merged


def _apply_market_filter(
    rows: list[dict[str, Any]],
    marketplace_filter: Optional[str],
) -> list[dict[str, Any]]:
    if not marketplace_filter:
        return rows

    tokens = [part.strip().lower() for part in marketplace_filter.split(",") if part.strip()]
    allowed = {token for token in tokens if token in ALLOWED_MARKETPLACES}
    if not allowed:
        return rows
    return [row for row in rows if row["marketplace"] in allowed]


def _apply_date_filter(
    rows: list[dict[str, Any]],
    from_date: Optional[str],
    to_date: Optional[str],
) -> list[dict[str, Any]]:
    from_dt = _parse_iso(from_date) if from_date else None
    to_dt = _parse_iso(to_date) if to_date else None
    if to_dt is not None:
        to_dt = to_dt.replace(hour=23, minute=59, second=59)

    if from_dt is None and to_dt is None:
        return rows

    filtered: list[dict[str, Any]] = []
    for row in rows:
        order_dt = _parse_iso(row.get("order_date"))
        if order_dt is None:
            continue
        if from_dt is not None and order_dt < from_dt:
            continue
        if to_dt is not None and order_dt > to_dt:
            continue
        filtered.append(row)
    return filtered


def _apply_search_filter(rows: list[dict[str, Any]], query: Optional[str]) -> list[dict[str, Any]]:
    needle = (query or "").strip().lower()
    if not needle:
        return rows

    filtered: list[dict[str, Any]] = []
    for row in rows:
        haystack = " ".join(
            [
                str(row.get("external_order_id") or ""),
                str(row.get("order_id") or ""),
                str(row.get("customer") or ""),
                str(row.get("article") or ""),
                str(row.get("payment_method") or ""),
                str(row.get("fulfillment_status") or ""),
            ]
        ).lower()
        if needle in haystack:
            filtered.append(row)
    return filtered


def _normalize_status_token(value: Any) -> str:
    return str(value or "").strip().lower()


def _is_return_like_status(value: Any) -> bool:
    token = _normalize_status_token(value)
    if not token:
        return False
    keywords = [
        "cancel",
        "cancelled",
        "canceled",
        "void",
        "return",
        "returned",
        "refund",
        "refunded",
        "partially_refunded",
        "rma",
        "revoked",
        "returning",
    ]
    return any(keyword in token for keyword in keywords)


def _apply_status_filter(rows: list[dict[str, Any]], status_filter: Optional[str]) -> list[dict[str, Any]]:
    token = str(status_filter or "").strip().lower()
    if not token:
        return rows
    if token != "returns":
        return rows

    filtered: list[dict[str, Any]] = []
    for row in rows:
        if _is_return_like_status(row.get("fulfillment_status")):
            filtered.append(row)
            continue
        if _is_return_like_status(row.get("financial_status")):
            filtered.append(row)
            continue
        if _is_return_like_status(row.get("raw_status")):
            filtered.append(row)
            continue
    return filtered


def list_orders(
    *,
    from_date: Optional[str],
    to_date: Optional[str],
    marketplace: Optional[str],
    query: Optional[str],
    status_filter: Optional[str] = None,
    limit: int,
    offset: int,
) -> dict[str, Any]:
    source_rows = [*_load_shopify_orders(), *_load_kaufland_orders()]
    enrichment_map = fetch_enrichment_map()
    merged_rows = [_merge_enrichment(row, enrichment_map) for row in source_rows]

    filtered = _apply_market_filter(merged_rows, marketplace)
    filtered = _apply_date_filter(filtered, from_date, to_date)
    filtered = _apply_search_filter(filtered, query)
    filtered = _apply_status_filter(filtered, status_filter)

    filtered.sort(key=lambda item: item.get("order_date") or "", reverse=True)

    total = len(filtered)
    start = max(offset, 0)
    end = start + max(limit, 0)
    items = filtered[start:end]
    return {
        "total": total,
        "items": items,
    }


def _load_shopify_order_detail(order_id: str) -> Optional[dict[str, Any]]:
    if not SHOPIFY_DB_PATH.exists():
        return None

    with _connect_readonly(SHOPIFY_DB_PATH) as connection:
        row = connection.execute(
            "SELECT * FROM orders WHERE id = ? LIMIT 1",
            (order_id,),
        ).fetchone()
        if row is None:
            return None

        line_items = connection.execute(
            "SELECT * FROM order_line_items WHERE order_id = ? ORDER BY id ASC",
            (order_id,),
        ).fetchall()
        fulfillments = connection.execute(
            "SELECT * FROM order_fulfillments WHERE order_id = ? ORDER BY COALESCE(created_at, '') ASC, id ASC",
            (order_id,),
        ).fetchall()
        refunds = connection.execute(
            "SELECT * FROM order_refunds WHERE order_id = ? ORDER BY COALESCE(created_at, '') ASC, id ASC",
            (order_id,),
        ).fetchall()
        transactions = connection.execute(
            "SELECT * FROM order_transactions WHERE order_id = ? ORDER BY COALESCE(processed_at, '') ASC, id ASC",
            (order_id,),
        ).fetchall()

        summary_query = connection.execute(
            """
            SELECT
                o.*,
                (
                    SELECT li.title
                    FROM order_line_items li
                    WHERE li.order_id = o.id
                    ORDER BY li.id ASC
                    LIMIT 1
                ) AS first_article,
                (
                    SELECT COUNT(*)
                    FROM order_line_items li2
                    WHERE li2.order_id = o.id
                ) AS line_items_count,
                (
                    SELECT ROUND(SUM(CAST(NULLIF(t.fee_amount, '') AS REAL)), 2)
                    FROM order_transactions t
                    WHERE t.order_id = o.id AND COALESCE(t.fee_amount, '') <> ''
                ) AS fee_total,
                (
                    SELECT ROUND(SUM(CAST(NULLIF(t.net_amount, '') AS REAL)), 2)
                    FROM order_transactions t
                    WHERE t.order_id = o.id AND COALESCE(t.net_amount, '') <> ''
                ) AS net_total
            FROM orders o
            WHERE o.id = ?
            LIMIT 1
            """,
            (order_id,),
        ).fetchone()

    if summary_query is None:
        return None

    summary = _shopify_summary_from_row(summary_query)
    raw_order = _safe_json_load(row["raw_json"])

    enriched_line_items = [dict(item) for item in line_items]

    # Enrich line items with product images from cache / Shopify API
    product_ids = list({
        str(li["product_id"]).strip()
        for li in enriched_line_items
        if li.get("product_id") and str(li["product_id"]).strip()
    })
    if product_ids:
        try:
            image_map = lookup_product_images(product_ids)
            for li in enriched_line_items:
                pid = str(li.get("product_id") or "").strip()
                if pid and pid in image_map and image_map[pid]:
                    li["image_src"] = image_map[pid]
        except Exception:
            pass  # image enrichment is best-effort

    return {
        "summary": summary,
        "order": dict(row),
        "order_raw": raw_order,
        "line_items": enriched_line_items,
        "fulfillments": [dict(item) for item in fulfillments],
        "refunds": [dict(item) for item in refunds],
        "transactions": [dict(item) for item in transactions],
        "shipping_address": raw_order.get("shipping_address") if isinstance(raw_order.get("shipping_address"), dict) else {},
        "billing_address": raw_order.get("billing_address") if isinstance(raw_order.get("billing_address"), dict) else {},
        "customer": raw_order.get("customer") if isinstance(raw_order.get("customer"), dict) else {},
    }


def _load_kaufland_order_detail(order_id: str) -> Optional[dict[str, Any]]:
    if not KAUFLAND_DB_PATH.exists():
        return None

    with _connect_readonly(KAUFLAND_DB_PATH) as connection:
        order_row = connection.execute(
            "SELECT * FROM orders WHERE id_order = ? LIMIT 1",
            (order_id,),
        ).fetchone()
        if order_row is None:
            return None

        unit_rows = connection.execute(
            "SELECT * FROM order_units WHERE id_order = ? ORDER BY COALESCE(ts_created_iso, '') ASC, id_order_unit ASC",
            (order_id,),
        ).fetchall()

        summary_row = connection.execute(
            """
            SELECT
                o.id_order,
                o.ts_created_iso,
                o.raw_json,
                (
                    SELECT COALESCE(SUM(CAST(COALESCE(NULLIF(ou.price, ''), 0) AS REAL)), 0)
                    FROM order_units ou
                    WHERE ou.id_order = o.id_order
                ) AS units_price_sum,
                (
                    SELECT COALESCE(SUM(CAST(COALESCE(NULLIF(ou.revenue_gross, ''), 0) AS REAL)), 0)
                    FROM order_units ou
                    WHERE ou.id_order = o.id_order
                ) AS revenue_gross_sum,
                (
                    SELECT COALESCE(SUM(CAST(COALESCE(NULLIF(ou.shipping_rate, ''), 0) AS REAL)), 0)
                    FROM order_units ou
                    WHERE ou.id_order = o.id_order
                ) AS shipping_sum,
                (
                    SELECT ou.product_title
                    FROM order_units ou
                    WHERE ou.id_order = o.id_order
                    ORDER BY COALESCE(ou.ts_created_iso, '') ASC, ou.id_order_unit ASC
                    LIMIT 1
                ) AS first_article,
                (
                    SELECT COUNT(*)
                    FROM order_units ou2
                    WHERE ou2.id_order = o.id_order
                ) AS line_items_count,
                (
                    SELECT TRIM(COALESCE(ou.shipping_first_name, '') || ' ' || COALESCE(ou.shipping_last_name, ''))
                    FROM order_units ou
                    WHERE ou.id_order = o.id_order
                    ORDER BY
                        CASE
                            WHEN TRIM(COALESCE(ou.shipping_first_name, '') || ' ' || COALESCE(ou.shipping_last_name, '')) <> '' THEN 0
                            ELSE 1
                        END,
                        COALESCE(ou.ts_created_iso, '') ASC,
                        ou.id_order_unit ASC
                    LIMIT 1
                ) AS customer_name,
                (
                    SELECT COALESCE(ou.status, '')
                    FROM order_units ou
                    WHERE ou.id_order = o.id_order
                    ORDER BY COALESCE(ou.ts_created_iso, '') ASC, ou.id_order_unit ASC
                    LIMIT 1
                ) AS unit_status
            FROM orders o
            WHERE o.id_order = ?
            LIMIT 1
            """,
            (order_id,),
        ).fetchone()

    if summary_row is None:
        return None

    summary = _kaufland_summary_from_row(summary_row)

    order_raw = _safe_json_load(order_row["raw_json"])
    units_payload = [dict(row) for row in unit_rows]
    parsed_units: list[dict[str, Any]] = []
    for unit in units_payload:
        unit_raw = _safe_json_load(unit.get("raw_json"))
        payload = dict(unit)
        payload["raw"] = unit_raw
        parsed_units.append(payload)

    first_unit = parsed_units[0] if parsed_units else {}

    shipping_unit = next((unit for unit in parsed_units if _kaufland_unit_has_address(unit, "shipping")), first_unit)
    billing_unit = next((unit for unit in parsed_units if _kaufland_unit_has_address(unit, "billing")), shipping_unit or first_unit)

    customer_name = _first_non_empty(
        _kaufland_unit_name(shipping_unit, "shipping"),
        _kaufland_unit_name(billing_unit, "billing"),
        summary.get("customer"),
    )

    buyer_id = _first_non_empty(
        shipping_unit.get("buyer_id_buyer"),
        billing_unit.get("buyer_id_buyer"),
        first_unit.get("buyer_id_buyer"),
    )

    buyer_email_from_order = _first_non_empty(
        (order_raw.get("buyer") if isinstance(order_raw.get("buyer"), dict) else {}).get("email")
    )
    buyer_email_from_units = ""
    for unit in parsed_units:
        raw = unit.get("raw") if isinstance(unit.get("raw"), dict) else {}
        buyer = raw.get("buyer") if isinstance(raw.get("buyer"), dict) else {}
        buyer_email_from_units = _first_non_empty(buyer.get("email"))
        if buyer_email_from_units:
            break

    return {
        "summary": summary,
        "order": dict(order_row),
        "order_raw": order_raw,
        "units": parsed_units,
        "shipping_address": _build_kaufland_address(shipping_unit, "shipping") if shipping_unit else {},
        "billing_address": _build_kaufland_address(billing_unit, "billing") if billing_unit else {},
        "customer": {
            "name": customer_name,
            "buyer_id": buyer_id or None,
            "email": _first_non_empty(buyer_email_from_order, buyer_email_from_units),
        },
    }


def get_order_detail(marketplace: str, order_id: str) -> Optional[dict[str, Any]]:
    market = marketplace.strip().lower()
    if market not in ALLOWED_MARKETPLACES:
        return None

    if market == "shopify":
        detail = _load_shopify_order_detail(order_id)
    else:
        detail = _load_kaufland_order_detail(order_id)

    if detail is None:
        return None

    enrichment_map = fetch_enrichment_map()
    summary = detail.get("summary") if isinstance(detail.get("summary"), dict) else {}
    merged_summary = _merge_enrichment(summary, enrichment_map)
    detail["summary"] = merged_summary
    return detail


def list_all_orders_without_pagination(
    *,
    from_date: Optional[str],
    to_date: Optional[str],
    marketplace: Optional[str],
    query: Optional[str],
    status_filter: Optional[str] = None,
) -> list[dict[str, Any]]:
    payload = list_orders(
        from_date=from_date,
        to_date=to_date,
        marketplace=marketplace,
        query=query,
        status_filter=status_filter,
        limit=1_000_000,
        offset=0,
    )
    items = payload.get("items")
    if not isinstance(items, list):
        return []
    return items
