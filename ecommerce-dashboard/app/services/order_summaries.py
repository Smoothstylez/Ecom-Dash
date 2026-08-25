from __future__ import annotations

import json
import math
import sqlite3
from datetime import datetime, timezone
from typing import Any, Optional

NON_EU_PAYPAL_COUNTRY_CODES = {"CH", "GB", "UK", "US", "CA", "AU"}

# EEA (European Economic Area) country codes for Shopify Payments fee tiers
EEA_COUNTRY_CODES = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
    "DE", "GR", "HU", "IS", "IE", "IT", "LV", "LI", "LT", "LU",
    "MT", "NL", "NO", "PL", "PT", "RO", "SK", "SI", "ES", "SE",
}


def safe_json_load(raw_text: Any) -> dict[str, Any]:
    if not isinstance(raw_text, str) or not raw_text.strip():
        return {}
    try:
        value = json.loads(raw_text)
    except (TypeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def parse_iso(value: Any) -> Optional[datetime]:
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


def to_iso_utc(value: Any) -> str:
    parsed = parse_iso(value)
    if parsed is None:
        return ""
    return parsed.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def to_eur_cents(value: Any) -> Optional[int]:
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


def to_kaufland_cents(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        rounded = round(value)
        if math.isclose(value, rounded, abs_tol=1e-9):
            return int(rounded)
        return int(round(value * 100))

    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace(",", ".")
    if "." in normalized:
        try:
            parsed = float(normalized)
        except ValueError:
            return None
        if not math.isfinite(parsed):
            return None
        rounded = round(parsed)
        if math.isclose(parsed, rounded, abs_tol=1e-9):
            return int(rounded)
        return int(round(parsed * 100))
    try:
        return int(normalized)
    except ValueError:
        return None


def _cents_from_real_aggregate(value: Any) -> int:
    """Convert a SQL REAL money aggregate that is already denominated in cents.

    Kaufland summary rows are built from SQL aggregates like
    ``SUM(price * vat / (100 + vat))`` whose results are cent amounts stored as
    REAL. They must be converted deterministically (round half away from zero,
    matching SQLite's ROUND) instead of being routed through the ambiguous
    euro/cents heuristic in :func:`to_kaufland_cents`, which multiplies every
    non-integer value by 100.
    """
    if value is None:
        return 0
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0
    if not math.isfinite(parsed):
        return 0
    rounded = math.floor(parsed + 0.5) if parsed >= 0 else math.ceil(parsed - 0.5)
    return int(rounded)


def normalize_name(*parts: Any) -> str:
    values: list[str] = []
    for part in parts:
        if part is None:
            continue
        text = str(part).strip()
        if text:
            values.append(text)
    return " ".join(values).strip()


def first_non_empty(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def extract_shopify_shipping_cents(order_payload: dict[str, Any]) -> int:
    shipping_set = order_payload.get("total_shipping_price_set")
    if isinstance(shipping_set, dict):
        shop_money = shipping_set.get("shop_money")
        if isinstance(shop_money, dict):
            cents = to_eur_cents(shop_money.get("amount"))
            if cents is not None:
                return cents
        presentment_money = shipping_set.get("presentment_money")
        if isinstance(presentment_money, dict):
            cents = to_eur_cents(presentment_money.get("amount"))
            if cents is not None:
                return cents

    shipping_lines = order_payload.get("shipping_lines")
    if isinstance(shipping_lines, list):
        total = 0
        has_value = False
        for item in shipping_lines:
            if not isinstance(item, dict):
                continue
            cents = to_eur_cents(item.get("discounted_price"))
            if cents is None:
                cents = to_eur_cents(item.get("price"))
            if cents is None:
                continue
            total += cents
            has_value = True
        if has_value:
            return total

    return 0


def estimate_shopify_paypal_fee_cents(order_payload: dict[str, Any], total_cents: int) -> Optional[int]:
    gateways_raw = order_payload.get("payment_gateway_names")
    gateways = [str(item).strip().lower() for item in gateways_raw if item is not None] if isinstance(gateways_raw, list) else []
    if not any("paypal" in item for item in gateways):
        return None

    billing = order_payload.get("billing_address")
    shipping = order_payload.get("shipping_address")
    billing_dict = billing if isinstance(billing, dict) else {}
    shipping_dict = shipping if isinstance(shipping, dict) else {}
    country_code = (
        first_non_empty(
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


def estimate_shopify_payments_fee_cents(
    order_payload: dict[str, Any],
    total_cents: int,
    payment_method: str = "",
) -> Optional[int]:
    if total_cents <= 0:
        return 0

    gateways_raw = order_payload.get("payment_gateway_names")
    gateways = [str(item).strip().lower() for item in gateways_raw if item is not None] if isinstance(gateways_raw, list) else []
    if any("paypal" in item for item in gateways):
        return None

    total_eur = total_cents / 100.0
    pm_lower = str(payment_method or "").strip().lower()

    if pm_lower in ("klarna", "shopify payments") or "klarna" in pm_lower:
        rate = 0.0299
        fixed_fee = 0.35
    elif pm_lower in ("bancontact", "eps", "ideal", "sofort"):
        rate = 0.024
        fixed_fee = 0.25
    else:
        billing = order_payload.get("billing_address")
        shipping = order_payload.get("shipping_address")
        billing_dict = billing if isinstance(billing, dict) else {}
        shipping_dict = shipping if isinstance(shipping, dict) else {}
        country_code = (
            first_non_empty(
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

    tps = order_payload.get("total_price_set")
    if isinstance(tps, dict):
        presentment = tps.get("presentment_money")
        if isinstance(presentment, dict):
            presentment_currency = str(presentment.get("currency_code", "EUR")).upper()
            if presentment_currency != "EUR":
                fee_eur += total_eur * 0.02

    return int(round(fee_eur * 100))


def cents_to_eur(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        cents = int(value)
    except (TypeError, ValueError):
        return None
    return round(cents / 100.0, 2)


def _scale_cents(value_cents: int, *, numerator: int, denominator: int) -> int:
    if denominator <= 0:
        return max(int(value_cents), 0)
    return max(int(round(int(value_cents) * (numerator / denominator))), 0)


FULLY_RETURN_LIKE_STATUS_KEYWORDS = (
    "cancel",
    "cancelled",
    "canceled",
    "void",
    "return",
    "returned",
    "refund",
    "refunded",
    "rma",
    "revoked",
    "returning",
)


def _is_fully_return_like_status(token: Any) -> bool:
    """True for statuses that void the whole order economically.

    Mirrors the return-like semantics used by bookings/analytics sync, but
    explicitly excludes ``partially_refunded`` which keeps its reduced amount.
    """
    normalized = str(token or "").strip().lower()
    if not normalized or "partial" in normalized:
        return False
    return any(keyword in normalized for keyword in FULLY_RETURN_LIKE_STATUS_KEYWORDS)


def shopify_summary_from_row(row: sqlite3.Row, *, include_raw_fallbacks: bool = True) -> dict[str, Any]:
    order_payload = safe_json_load(row["raw_json"]) if include_raw_fallbacks else {}

    financial_status_raw = str(row["financial_status"] or "").strip().lower()
    gross_total_cents = to_eur_cents(row["total_price"]) or 0
    gross_tax_cents = to_eur_cents(row["total_tax"]) or 0
    refund_cents = to_eur_cents(row["refund_amount_sum"]) or 0
    total_cents = gross_total_cents
    if financial_status_raw == "partially_refunded":
        total_cents = max(gross_total_cents - refund_cents, 0)

    sales_gross_cents = total_cents
    sales_vat_cents = gross_tax_cents
    if financial_status_raw == "partially_refunded" and gross_total_cents > 0 and refund_cents > 0:
        sales_vat_cents = _scale_cents(gross_tax_cents, numerator=sales_gross_cents, denominator=gross_total_cents)
    sales_net_cents = max(sales_gross_cents - sales_vat_cents, 0)

    fee_cents = to_eur_cents(row["fee_total"])
    fee_source: str = "api"

    if fee_cents is None:
        fee_cents = to_eur_cents(row["estimated_paypal_fee"])
        fee_source = "stored_estimate"

    if fee_cents is not None and financial_status_raw == "partially_refunded" and gross_total_cents > 0 and refund_cents > 0:
        fee_cents = max(int(round(fee_cents * (total_cents / gross_total_cents))), 0)

    if fee_cents is None:
        fee_cents = estimate_shopify_paypal_fee_cents(order_payload, total_cents)
        fee_source = "estimated"

    if fee_cents is None:
        fee_cents = estimate_shopify_payments_fee_cents(order_payload, total_cents, first_non_empty(row["payment_method"]))
        fee_source = "estimated"

    if fee_cents is None:
        fee_cents = 0
        fee_source = "none"

    if fee_source == "estimated":
        tps = order_payload.get("total_price_set")
        if isinstance(tps, dict):
            presentment = tps.get("presentment_money")
            if isinstance(presentment, dict):
                presentment_currency = str(presentment.get("currency_code", "EUR")).upper()
                if presentment_currency != "EUR":
                    fee_source = "estimated_fx"

    after_fees_cents = to_eur_cents(row["net_total"])
    if after_fees_cents is None:
        after_fees_cents = to_eur_cents(row["estimated_net_after_fee"])
    if after_fees_cents is not None and financial_status_raw == "partially_refunded" and gross_total_cents > 0 and refund_cents > 0:
        after_fees_cents = max(int(round(after_fees_cents * (total_cents / gross_total_cents))), 0)
    if after_fees_cents is None:
        after_fees_cents = max(total_cents - fee_cents, 0)

    customer = normalize_name(row["customer_first_name"], row["customer_last_name"])
    if not customer:
        customer = first_non_empty(row["customer_email"], row["email"], order_payload.get("email"), "Unbekannt")

    article = first_non_empty(row["first_article"])
    if not article and include_raw_fallbacks:
        line_items = order_payload.get("line_items")
        if isinstance(line_items, list) and line_items:
            first = line_items[0]
            if isinstance(first, dict):
                article = first_non_empty(first.get("title"))
    if not article:
        article = "-"

    created_iso = to_iso_utc(row["created_at"])
    shipping_cents = extract_shopify_shipping_cents(order_payload) if include_raw_fallbacks else 0

    if _is_fully_return_like_status(financial_status_raw):
        # Fully refunded/voided/cancelled orders carry no economic value and
        # must not feed revenue, VAT, threshold, or bookkeeping mirrors.
        total_cents = 0
        sales_gross_cents = 0
        sales_vat_cents = 0
        sales_net_cents = 0
        fee_cents = 0
        after_fees_cents = 0
        shipping_cents = 0

    try:
        line_items_count = int(row["line_items_count"])
    except (ValueError, TypeError, IndexError, KeyError):
        line_items_count = 1

    summary = {
        "marketplace": "shopify",
        "order_id": str(row["id"]),
        "external_order_id": first_non_empty(row["name"], row["id"]),
        "order_date": created_iso,
        "customer": customer,
        "article": article,
        "line_items_count": line_items_count,
        "total_cents": total_cents,
        "sales_gross_cents": max(sales_gross_cents, 0),
        "sales_net_cents": max(sales_net_cents, 0),
        "sales_vat_cents": max(sales_vat_cents, 0),
        "fees_cents": max(fee_cents, 0),
        "after_fees_cents": max(after_fees_cents, 0),
        "shipping_cents": max(shipping_cents, 0),
        "currency": first_non_empty(row["currency"], "EUR").upper(),
        "fulfillment_status": first_non_empty(row["fulfillment_status"], row["financial_status"], "unknown"),
        "payment_method": first_non_empty(row["payment_method"], "Shopify"),
        "fee_source": fee_source,
        "is_test": bool(order_payload.get("test")),
    }
    summary["raw_status"] = summary.get("fulfillment_status")
    summary["financial_status"] = first_non_empty(row["financial_status"], "")
    return summary


def kaufland_summary_from_row(row: sqlite3.Row, *, include_raw_fallbacks: bool = True) -> dict[str, Any]:
    raw_payload = safe_json_load(row["raw_json"]) if include_raw_fallbacks else {}
    total_cents = _cents_from_real_aggregate(row["units_price_sum"])
    after_fees_cents = _cents_from_real_aggregate(row["revenue_gross_sum"]) or total_cents
    fees_cents = total_cents - after_fees_cents
    if fees_cents < 0:
        fees_cents = 0

    shipping_cents = _cents_from_real_aggregate(row["shipping_sum"])
    gross_sales_cents = max(total_cents + shipping_cents, 0)
    source_vat_cents = max(
        _cents_from_real_aggregate(row["units_vat_sum"]) + _cents_from_real_aggregate(row["shipping_vat_sum"]),
        0,
    )
    refund_cents = _cents_from_real_aggregate(row["refund_amount_sum"])
    sales_gross_cents = max(gross_sales_cents - refund_cents, 0)
    sales_vat_cents = source_vat_cents
    if gross_sales_cents > 0 and refund_cents > 0:
        sales_vat_cents = _scale_cents(source_vat_cents, numerator=sales_gross_cents, denominator=gross_sales_cents)
    sales_net_cents = max(sales_gross_cents - sales_vat_cents, 0)

    customer = first_non_empty(row["customer_name"])
    if not customer and include_raw_fallbacks:
        buyer = raw_payload.get("buyer") if isinstance(raw_payload.get("buyer"), dict) else {}
        customer = first_non_empty(buyer.get("email"), "Unbekannt")
    if not customer:
        customer = "Unbekannt"

    article = first_non_empty(row["first_article"])
    if not article and include_raw_fallbacks:
        order_units = raw_payload.get("order_units")
        if isinstance(order_units, list) and order_units:
            first_unit = order_units[0]
            if isinstance(first_unit, dict):
                product = first_unit.get("product") if isinstance(first_unit.get("product"), dict) else {}
                article = first_non_empty(product.get("title"))
    if not article:
        article = "-"

    order_date_iso = to_iso_utc(row["ts_created_iso"])

    currency = first_non_empty(raw_payload.get("currency")) if include_raw_fallbacks else ""
    if not currency and include_raw_fallbacks:
        order_units = raw_payload.get("order_units")
        if isinstance(order_units, list) and order_units:
            first_unit = order_units[0]
            if isinstance(first_unit, dict):
                currency = first_non_empty(first_unit.get("currency"))

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
        "sales_gross_cents": sales_gross_cents,
        "sales_net_cents": sales_net_cents,
        "sales_vat_cents": sales_vat_cents,
        "fees_cents": fees_cents,
        "after_fees_cents": max(after_fees_cents, 0),
        "shipping_cents": max(shipping_cents, 0),
        "currency": first_non_empty(currency, "EUR").upper(),
        "fulfillment_status": first_non_empty(row["unit_status"], "unknown"),
        "payment_method": "Kaufland Settlement",
        "fee_source": "api",
    }
    summary["raw_status"] = summary.get("fulfillment_status")
    summary["financial_status"] = ""
    return summary
