from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Optional

from app.config import ALLOWED_MARKETPLACES, KAUFLAND_DB_PATH, SHOPIFY_DB_PATH
from app.db import connect_combined_db, fetch_aliexpress_order_mappings, fetch_enrichment_map
from app.services.importers.shopify_live import lookup_product_images
from app.services.order_summaries import (
    cents_to_eur,
    first_non_empty,
    kaufland_summary_from_row,
    normalize_name,
    parse_iso,
    safe_json_load,
    shopify_summary_from_row,
    to_eur_cents,
    to_iso_utc,
    to_kaufland_cents,
)


def _connect_readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    return connection


def _kaufland_unit_value(unit: dict[str, Any], key: str) -> str:
    value = unit.get(key)
    return str(value).strip() if value is not None else ""


def _kaufland_unit_name(unit: dict[str, Any], prefix: str) -> str:
    return normalize_name(
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
        fallback = first_non_empty(raw_address.get(raw_key))
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


def _shopify_summary_from_row(row: sqlite3.Row, *, include_raw_fallbacks: bool = True) -> dict[str, Any]:
    return shopify_summary_from_row(row, include_raw_fallbacks=include_raw_fallbacks)


def _kaufland_summary_from_row(row: sqlite3.Row, *, include_raw_fallbacks: bool = True) -> dict[str, Any]:
    return kaufland_summary_from_row(row, include_raw_fallbacks=include_raw_fallbacks)


def _to_kaufland_cents(value: Any) -> Optional[int]:
    return to_kaufland_cents(value)


def _load_shopify_orders(*, include_raw_fallbacks: bool = True) -> list[dict[str, Any]]:
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
                ) AS net_total,
                (
                    SELECT COALESCE(SUM(
                        CAST(NULLIF(json_extract(t2.value, '$.amount'), '') AS REAL)
                    ), 0)
                    FROM order_refunds r, json_each(r.transactions_json) t2
                    WHERE r.order_id = o.id
                      AND json_extract(t2.value, '$.kind') = 'refund'
                      AND json_extract(t2.value, '$.status') = 'success'
                ) AS refund_amount_sum
            FROM orders o
            ORDER BY COALESCE(o.created_at, '') DESC, o.id DESC
            """
        ).fetchall()

    return [_shopify_summary_from_row(row, include_raw_fallbacks=include_raw_fallbacks) for row in rows]


def _load_kaufland_orders(*, include_raw_fallbacks: bool = True) -> list[dict[str, Any]]:
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
                      AND COALESCE(ou.status, '') NOT IN ('cancelled', 'canceled')
                ) AS units_price_sum,
                (
                    SELECT COALESCE(SUM(CAST(COALESCE(NULLIF(ou.revenue_gross, ''), 0) AS REAL)), 0)
                          - COALESCE((
                               SELECT SUM(CAST(COALESCE(NULLIF(ref.amount, ''), '0') AS REAL))
                               FROM order_unit_refunds ref
                               JOIN order_units ou2 ON ref.id_order_unit = ou2.id_order_unit
                               WHERE ou2.id_order = o.id_order
                                 AND COALESCE(ou2.status, '') NOT IN ('cancelled', 'canceled')
                           ), 0)
                    FROM order_units ou
                    WHERE ou.id_order = o.id_order
                      AND COALESCE(ou.status, '') NOT IN ('cancelled', 'canceled')
                ) AS revenue_gross_sum,
                (
                    SELECT COALESCE(SUM(CAST(COALESCE(NULLIF(ou.shipping_rate, ''), 0) AS REAL)), 0)
                    FROM order_units ou
                    WHERE ou.id_order = o.id_order
                      AND COALESCE(ou.status, '') NOT IN ('cancelled', 'canceled')
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

    return [_kaufland_summary_from_row(row, include_raw_fallbacks=include_raw_fallbacks) for row in rows]


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
        "purchase_cost_eur": cents_to_eur(purchase_cost_cents),
        "purchase_currency": enrichment.get("purchase_currency") or "EUR",
        "purchase_supplier": enrichment.get("supplier_name"),
        "purchase_notes": enrichment.get("purchase_notes"),
        "invoice": invoice_payload,
        "profit_cents": profit_cents,
    }
    return merged


def _build_marketplace_aliexpress_mapping_index() -> dict[tuple[str, str], list[dict[str, Any]]]:
    payload: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in fetch_aliexpress_order_mappings():
        key = (str(row.get("marketplace") or ""), str(row.get("order_id") or ""))
        if not all(key):
            continue
        payload.setdefault(key, []).append(row)
    return payload


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
    from_dt = parse_iso(from_date) if from_date else None
    to_dt = parse_iso(to_date) if to_date else None
    if to_dt is not None:
        to_dt = to_dt.replace(hour=23, minute=59, second=59)

    if from_dt is None and to_dt is None:
        return rows

    filtered: list[dict[str, Any]] = []
    for row in rows:
        order_dt = parse_iso(row.get("order_date"))
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
                str(row.get("purchase_supplier") or ""),
                str(row.get("purchase_notes") or ""),
            ]
        ).lower()
        if needle in haystack:
            filtered.append(row)
    return filtered


def _normalize_status_token(value: Any) -> str:
    return str(value or "").strip().lower()


def _status_tokens_for_row(row: dict[str, Any]) -> set[str]:
    return {
        token
        for token in (
            _normalize_status_token(row.get("fulfillment_status")),
            _normalize_status_token(row.get("financial_status")),
            _normalize_status_token(row.get("raw_status")),
        )
        if token
    }


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


def _status_filter_aliases(token: str) -> set[str]:
    if token in {"cancelled", "canceled"}:
        return {"cancelled", "canceled"}
    if token == "refunded":
        return {"refunded", "partially_refunded"}
    if token == "sent":
        return {"sent", "sent_and_autopaid"}
    return {token}


def _apply_status_filter(rows: list[dict[str, Any]], status_filter: Optional[str]) -> list[dict[str, Any]]:
    token = _normalize_status_token(status_filter)
    if not token:
        return rows

    filtered: list[dict[str, Any]] = []
    aliases = _status_filter_aliases(token)
    for row in rows:
        row_tokens = _status_tokens_for_row(row)
        if token == "returns":
            if any(_is_return_like_status(value) for value in row_tokens):
                filtered.append(row)
            continue
        if row_tokens & aliases:
            filtered.append(row)
    return filtered


def _apply_payment_filter(rows: list[dict[str, Any]], payment_filters: Optional[list[str]]) -> list[dict[str, Any]]:
    selected = {
        str(value or "").strip()
        for value in (payment_filters or [])
        if str(value or "").strip()
    }
    if not selected:
        return rows
    return [row for row in rows if str(row.get("payment_method") or "").strip() in selected]


def _apply_enrichment_filters(
    rows: list[dict[str, Any]],
    *,
    hide_canceled: bool,
    has_purchase_cost: bool,
    no_purchase_cost: bool,
    has_invoice: bool,
    no_invoice: bool,
    status_filter: Optional[str],
) -> list[dict[str, Any]]:
    explicit_cancel_status = str(status_filter or "").strip().lower() in {"cancelled", "canceled", "refunded", "returns"}
    filtered: list[dict[str, Any]] = []

    for row in rows:
        is_canceled = (
            _is_return_like_status(row.get("fulfillment_status"))
            or _is_return_like_status(row.get("financial_status"))
            or _is_return_like_status(row.get("raw_status"))
        )
        purchase_cost_cents = int(row.get("purchase_cost_cents") or 0)
        has_invoice_value = isinstance(row.get("invoice"), dict) and bool(row.get("invoice"))

        if hide_canceled and not explicit_cancel_status and is_canceled:
            continue
        if has_purchase_cost and purchase_cost_cents <= 0:
            continue
        if no_purchase_cost and purchase_cost_cents > 0:
            continue
        if has_invoice and not has_invoice_value:
            continue
        if no_invoice and has_invoice_value:
            continue
        filtered.append(row)

    return filtered


def list_orders(
    *,
    from_date: Optional[str],
    to_date: Optional[str],
    marketplace: Optional[str],
    query: Optional[str],
    status_filter: Optional[str] = None,
    payment_filters: Optional[list[str]] = None,
    hide_canceled: bool = False,
    has_purchase_cost: bool = False,
    no_purchase_cost: bool = False,
    has_invoice: bool = False,
    no_invoice: bool = False,
    limit: int,
    offset: int,
    include_raw_fallbacks: bool = True,
) -> dict[str, Any]:
    try:
        if _combined_orders_ready(marketplace=marketplace):
            return _list_orders_from_combined(
                from_date=from_date,
                to_date=to_date,
                marketplace=marketplace,
                query=query,
                status_filter=status_filter,
                payment_filters=payment_filters,
                hide_canceled=hide_canceled,
                has_purchase_cost=has_purchase_cost,
                no_purchase_cost=no_purchase_cost,
                has_invoice=has_invoice,
                no_invoice=no_invoice,
                limit=limit,
                offset=offset,
                include_raw_fallbacks=include_raw_fallbacks,
            )
    except Exception:
        pass

    return _list_orders_from_sources(
        from_date=from_date,
        to_date=to_date,
        marketplace=marketplace,
        query=query,
        status_filter=status_filter,
        payment_filters=payment_filters,
        hide_canceled=hide_canceled,
        has_purchase_cost=has_purchase_cost,
        no_purchase_cost=no_purchase_cost,
        has_invoice=has_invoice,
        no_invoice=no_invoice,
        limit=limit,
        offset=offset,
        include_raw_fallbacks=include_raw_fallbacks,
    )


def _combined_orders_ready(*, marketplace: Optional[str]) -> bool:
    requested_markets = _requested_marketplaces(marketplace)
    if not requested_markets:
        return False

    if not SHOPIFY_DB_PATH.exists() and "shopify" in requested_markets:
        return False
    if not KAUFLAND_DB_PATH.exists() and "kaufland" in requested_markets:
        return False

    with connect_combined_db() as conn:
        table_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='combined_orders'"
        ).fetchone()
        if not table_exists:
            return False

        combined_counts = {
            str(row[0]).strip().lower(): int(row[1] or 0)
            for row in conn.execute(
                "SELECT marketplace, COUNT(*) FROM combined_orders GROUP BY marketplace"
            ).fetchall()
        }

    source_counts = _source_order_counts()
    for market in requested_markets:
        if int(source_counts.get(market, 0)) != int(combined_counts.get(market, 0)):
            return False
    return True


def _requested_marketplaces(marketplace: Optional[str]) -> set[str]:
    if not marketplace:
        return set(ALLOWED_MARKETPLACES)
    tokens = {part.strip().lower() for part in str(marketplace).split(",") if part.strip()}
    allowed = {token for token in tokens if token in ALLOWED_MARKETPLACES}
    return allowed or set(ALLOWED_MARKETPLACES)


def _source_order_counts() -> dict[str, int]:
    counts = {"shopify": 0, "kaufland": 0}
    if SHOPIFY_DB_PATH.exists():
        with _connect_readonly(SHOPIFY_DB_PATH) as connection:
            row = connection.execute("SELECT COUNT(*) FROM orders").fetchone()
            counts["shopify"] = int(row[0] if row else 0)
    if KAUFLAND_DB_PATH.exists():
        with _connect_readonly(KAUFLAND_DB_PATH) as connection:
            row = connection.execute("SELECT COUNT(*) FROM orders").fetchone()
            counts["kaufland"] = int(row[0] if row else 0)
    return counts


def _list_orders_from_combined(
    *,
    from_date: Optional[str],
    to_date: Optional[str],
    marketplace: Optional[str],
    query: Optional[str],
    status_filter: Optional[str] = None,
    payment_filters: Optional[list[str]] = None,
    hide_canceled: bool = False,
    has_purchase_cost: bool = False,
    no_purchase_cost: bool = False,
    has_invoice: bool = False,
    no_invoice: bool = False,
    limit: int,
    offset: int,
    include_raw_fallbacks: bool,
) -> dict[str, Any]:
    where_clauses: list[str] = []
    params: list[Any] = []

    if marketplace:
        tokens = [p.strip().lower() for p in marketplace.split(",") if p.strip()]
        allowed = {t for t in tokens if t in ALLOWED_MARKETPLACES}
        if allowed:
            placeholders = ",".join("?" for _ in allowed)
            where_clauses.append(f"marketplace IN ({placeholders})")
            params.extend(list(allowed))

    if from_date:
        where_clauses.append("order_date >= ?")
        params.append(from_date)

    if to_date:
        to_dt = parse_iso(to_date)
        if to_dt is not None:
            to_dt = to_dt.replace(hour=23, minute=59, second=59)
            where_clauses.append("order_date <= ?")
            params.append(to_dt.strftime("%Y-%m-%dT%H:%M:%S"))
        else:
            where_clauses.append("order_date <= ?")
            params.append(to_date)

    if query:
        needle = query.strip().lower()
        if needle:
            where_clauses.append(
                "(LOWER(external_order_id) LIKE ? OR LOWER(order_id) LIKE ? "
                "OR LOWER(customer) LIKE ? OR LOWER(article) LIKE ? "
                "OR LOWER(payment_method) LIKE ? OR LOWER(fulfillment_status) LIKE ? "
                "OR LOWER(COALESCE(purchase_supplier, '')) LIKE ? "
                "OR LOWER(COALESCE(purchase_notes, '')) LIKE ?)"
            )
            like_val = f"%{needle}%"
            params.extend([like_val] * 8)

    if status_filter:
        token = _normalize_status_token(status_filter)
        if token:
            if token == "returns":
                cancel_keywords = [
                    "%cancel%", "%return%", "%refund%", "%void%", "%rma%", "%revoked%",
                ]
                or_clauses = []
                for kw in cancel_keywords:
                    or_clauses.append(
                        "(LOWER(fulfillment_status) LIKE ? OR LOWER(financial_status) LIKE ?)"
                    )
                    params.extend([kw, kw])
                where_clauses.append(f"({' OR '.join(or_clauses)})")
            else:
                aliases = _status_filter_aliases(token)
                or_clauses = []
                for alias in aliases:
                    or_clauses.append(
                        "(LOWER(fulfillment_status) LIKE ? OR LOWER(financial_status) LIKE ?)"
                    )
                    params.extend([f"%{alias}%", f"%{alias}%"])
                where_clauses.append(f"({' OR '.join(or_clauses)})")

    if payment_filters:
        selected = [str(v or "").strip() for v in payment_filters if str(v or "").strip()]
        if selected:
            placeholders = ",".join("?" for _ in selected)
            where_clauses.append(f"payment_method IN ({placeholders})")
            params.extend(selected)

    explicit_cancel_status = str(status_filter or "").strip().lower() in {
        "cancelled", "canceled", "refunded", "returns",
    }

    if hide_canceled and not explicit_cancel_status:
        cancel_keywords = [
            "%cancel%", "%return%", "%refund%", "%void%", "%rma%", "%revoked%",
        ]
        for kw in cancel_keywords:
            where_clauses.append(
                "(LOWER(fulfillment_status) NOT LIKE ? AND LOWER(financial_status) NOT LIKE ?)"
            )
            params.extend([kw, kw])

    if has_purchase_cost:
        where_clauses.append("purchase_cost_cents > 0")
    if no_purchase_cost:
        where_clauses.append("purchase_cost_cents <= 0")
    if has_invoice:
        where_clauses.append("has_invoice = 1")
    if no_invoice:
        where_clauses.append("has_invoice = 0")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    with connect_combined_db() as connection:
        count_row = connection.execute(
            f"SELECT COUNT(*) FROM combined_orders {where_sql}", params
        ).fetchone()
        total = count_row[0] if count_row else 0

        rows = connection.execute(
            f"SELECT * FROM combined_orders {where_sql} "
            "ORDER BY order_date DESC, id DESC LIMIT ? OFFSET ?",
            params + [max(limit, 0), max(offset, 0)],
        ).fetchall()

    aliexpress_mapping_index = _build_marketplace_aliexpress_mapping_index()
    items: list[dict[str, Any]] = []
    for row in rows:
        order = dict(row)
        order_id_key = (str(order.get("marketplace") or ""), str(order.get("order_id") or ""))
        mappings = [dict(item) for item in aliexpress_mapping_index.get(order_id_key, [])]
        order["aliexpress_mappings"] = mappings
        order["aliexpress_mapping_count"] = len(mappings)

        invoice_payload = None
        if order.get("invoice_document_id"):
            invoice_payload = {
                "document_id": order.get("invoice_document_id"),
            }

        order["invoice"] = invoice_payload
        order["raw_json"] = order.get("raw_json") if include_raw_fallbacks else {}

        for col in ("has_invoice", "invoice_document_id"):
            order.pop(col, None)

        items.append(order)

    return {"total": total, "items": items}


def _list_orders_from_sources(
    *,
    from_date: Optional[str],
    to_date: Optional[str],
    marketplace: Optional[str],
    query: Optional[str],
    status_filter: Optional[str] = None,
    payment_filters: Optional[list[str]] = None,
    hide_canceled: bool = False,
    has_purchase_cost: bool = False,
    no_purchase_cost: bool = False,
    has_invoice: bool = False,
    no_invoice: bool = False,
    limit: int,
    offset: int,
    include_raw_fallbacks: bool,
) -> dict[str, Any]:
    source_rows = [
        *_load_shopify_orders(include_raw_fallbacks=include_raw_fallbacks),
        *_load_kaufland_orders(include_raw_fallbacks=include_raw_fallbacks),
    ]
    enrichment_map = fetch_enrichment_map()
    aliexpress_mapping_index = _build_marketplace_aliexpress_mapping_index()
    merged_rows = [_merge_enrichment(row, enrichment_map) for row in source_rows]
    for row in merged_rows:
        key = (str(row.get("marketplace") or ""), str(row.get("order_id") or ""))
        mappings = [dict(item) for item in aliexpress_mapping_index.get(key, [])]
        row["aliexpress_mappings"] = mappings
        row["aliexpress_mapping_count"] = len(mappings)

    filtered = _apply_market_filter(merged_rows, marketplace)
    filtered = _apply_date_filter(filtered, from_date, to_date)
    filtered = _apply_search_filter(filtered, query)
    filtered = _apply_status_filter(filtered, status_filter)
    filtered = _apply_payment_filter(filtered, payment_filters)
    filtered = _apply_enrichment_filters(
        filtered,
        hide_canceled=hide_canceled,
        has_purchase_cost=has_purchase_cost,
        no_purchase_cost=no_purchase_cost,
        has_invoice=has_invoice,
        no_invoice=no_invoice,
        status_filter=status_filter,
    )

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
                ) AS net_total,
                (
                    SELECT COALESCE(SUM(
                        CAST(NULLIF(json_extract(t2.value, '$.amount'), '') AS REAL)
                    ), 0)
                    FROM order_refunds r, json_each(r.transactions_json) t2
                    WHERE r.order_id = o.id
                      AND json_extract(t2.value, '$.kind') = 'refund'
                      AND json_extract(t2.value, '$.status') = 'success'
                ) AS refund_amount_sum
            FROM orders o
            WHERE o.id = ?
            LIMIT 1
            """,
            (order_id,),
        ).fetchone()

    if summary_query is None:
        return None

    summary = _shopify_summary_from_row(summary_query)
    raw_order = safe_json_load(row["raw_json"])

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
                      AND COALESCE(ou.status, '') NOT IN ('cancelled', 'canceled')
                ) AS units_price_sum,
                (
                    SELECT COALESCE(SUM(CAST(COALESCE(NULLIF(ou.revenue_gross, ''), 0) AS REAL)), 0)
                          - COALESCE((
                               SELECT SUM(CAST(COALESCE(NULLIF(ref.amount, ''), '0') AS REAL))
                               FROM order_unit_refunds ref
                               JOIN order_units ou2 ON ref.id_order_unit = ou2.id_order_unit
                               WHERE ou2.id_order = o.id_order
                                 AND COALESCE(ou2.status, '') NOT IN ('cancelled', 'canceled')
                           ), 0)
                    FROM order_units ou
                    WHERE ou.id_order = o.id_order
                      AND COALESCE(ou.status, '') NOT IN ('cancelled', 'canceled')
                ) AS revenue_gross_sum,
                (
                    SELECT COALESCE(SUM(CAST(COALESCE(NULLIF(ou.shipping_rate, ''), 0) AS REAL)), 0)
                    FROM order_units ou
                    WHERE ou.id_order = o.id_order
                      AND COALESCE(ou.status, '') NOT IN ('cancelled', 'canceled')
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

    order_raw = safe_json_load(order_row["raw_json"])
    units_payload = [dict(row) for row in unit_rows]
    parsed_units: list[dict[str, Any]] = []
    for unit in units_payload:
        unit_raw = safe_json_load(unit.get("raw_json"))
        payload = dict(unit)
        payload["raw"] = unit_raw
        parsed_units.append(payload)

    first_unit = parsed_units[0] if parsed_units else {}

    shipping_unit = next((unit for unit in parsed_units if _kaufland_unit_has_address(unit, "shipping")), first_unit)
    billing_unit = next((unit for unit in parsed_units if _kaufland_unit_has_address(unit, "billing")), shipping_unit or first_unit)

    customer_name = first_non_empty(
        _kaufland_unit_name(shipping_unit, "shipping"),
        _kaufland_unit_name(billing_unit, "billing"),
        summary.get("customer"),
    )

    buyer_id = first_non_empty(
        shipping_unit.get("buyer_id_buyer"),
        billing_unit.get("buyer_id_buyer"),
        first_unit.get("buyer_id_buyer"),
    )

    buyer_email_from_order = first_non_empty(
        (order_raw.get("buyer") if isinstance(order_raw.get("buyer"), dict) else {}).get("email")
    )
    buyer_email_from_units = ""
    for unit in parsed_units:
        raw = unit.get("raw") if isinstance(unit.get("raw"), dict) else {}
        buyer = raw.get("buyer") if isinstance(raw.get("buyer"), dict) else {}
        buyer_email_from_units = first_non_empty(buyer.get("email"))
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
            "email": first_non_empty(buyer_email_from_order, buyer_email_from_units),
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
    mapping_key = (market, str(merged_summary.get("order_id") or order_id))
    aliexpress_mapping_index = _build_marketplace_aliexpress_mapping_index()
    merged_summary["aliexpress_mappings"] = [dict(item) for item in aliexpress_mapping_index.get(mapping_key, [])]
    merged_summary["aliexpress_mapping_count"] = len(merged_summary["aliexpress_mappings"])
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
        payment_filters=None,
        hide_canceled=False,
        has_purchase_cost=False,
        no_purchase_cost=False,
        has_invoice=False,
        no_invoice=False,
        limit=1_000_000,
        offset=0,
        include_raw_fallbacks=True,
    )
    items = payload.get("items")
    if not isinstance(items, list):
        return []
    return items
