from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from app.services.importers.amazon_sp_api import (
    _connect,
    _json_dumps,
    _stable_id,
    _utc_now,
    extract_modern_financial_breakdown,
    init_amazon_fba_db,
    normalize_amazon_address,
    normalize_fba_status,
)
from app.services.order_summaries import cents_to_eur


def _canonical_financial_event_predicate(alias: str = "e") -> str:
    lifecycle_key = f"COALESCE({alias}.lifecycle_id, {alias}.transaction_id, {alias}.id)"
    return f"""
    (
        ({alias}.amazon_order_id IS NULL AND {alias}.event_type = 'SettlementReportLine')
        OR
        (
            EXISTS (
                SELECT 1 FROM amazon_financial_events modern
                WHERE modern.amazon_order_id = {alias}.amazon_order_id
                  AND modern.event_type LIKE 'ModernTransaction:%'
            )
            AND {alias}.event_type LIKE 'ModernTransaction:%'
            AND NOT EXISTS (
                SELECT 1 FROM amazon_financial_events newer
                WHERE newer.event_type LIKE 'ModernTransaction:%'
                  AND COALESCE(newer.lifecycle_id, newer.transaction_id, newer.id) = {lifecycle_key}
                  AND (
                    CASE json_extract(newer.raw_json, '$.transactionStatus')
                        WHEN 'RELEASED' THEN 3
                        WHEN 'DEFERRED_RELEASED' THEN 2
                        WHEN 'DEFERRED' THEN 1
                        ELSE 0
                    END > CASE json_extract({alias}.raw_json, '$.transactionStatus')
                        WHEN 'RELEASED' THEN 3
                        WHEN 'DEFERRED_RELEASED' THEN 2
                        WHEN 'DEFERRED' THEN 1
                        ELSE 0
                    END
                    OR (
                        CASE json_extract(newer.raw_json, '$.transactionStatus')
                            WHEN 'RELEASED' THEN 3
                            WHEN 'DEFERRED_RELEASED' THEN 2
                            WHEN 'DEFERRED' THEN 1
                            ELSE 0
                        END = CASE json_extract({alias}.raw_json, '$.transactionStatus')
                            WHEN 'RELEASED' THEN 3
                            WHEN 'DEFERRED_RELEASED' THEN 2
                            WHEN 'DEFERRED' THEN 1
                            ELSE 0
                        END
                        AND COALESCE(newer.posted_date, '') > COALESCE({alias}.posted_date, '')
                    )
                  )
            )
        )
        OR (
            NOT EXISTS (
                SELECT 1 FROM amazon_financial_events modern
                WHERE modern.amazon_order_id = {alias}.amazon_order_id
                  AND modern.event_type LIKE 'ModernTransaction:%'
            )
            AND EXISTS (
                SELECT 1 FROM amazon_financial_events report
                WHERE report.amazon_order_id = {alias}.amazon_order_id
                  AND report.event_type = 'SettlementReportLine'
            )
            AND {alias}.event_type = 'SettlementReportLine'
        )
        OR (
            NOT EXISTS (
                SELECT 1 FROM amazon_financial_events modern
                WHERE modern.amazon_order_id = {alias}.amazon_order_id
                  AND modern.event_type LIKE 'ModernTransaction:%'
            )
            AND NOT EXISTS (
                SELECT 1 FROM amazon_financial_events report
                WHERE report.amazon_order_id = {alias}.amazon_order_id
                  AND report.event_type = 'SettlementReportLine'
            )
            AND {alias}.event_type <> 'SettlementReportLine'
        )
    )
    """


def _row_dict(row: Optional[sqlite3.Row]) -> Optional[dict[str, Any]]:
    return dict(row) if row is not None else None


def _raw_json(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _order_summary(row: sqlite3.Row) -> dict[str, Any]:
    sales_cents = int(row["financial_sales_cents"] or row["order_total_cents"] or row["item_sales_cents"] or 0)
    sales_vat_cents = min(max(int(row["item_tax_cents"] or 0), 0), max(sales_cents, 0))
    fees_cents = int(row["financial_fees_cents"] or 0)
    after_fees_cents = sales_cents - fees_cents
    cogs_cents = int(row["fifo_cogs_cents"] or 0)
    financial_event_count = int(row["financial_event_count"] or 0)
    return {
        "marketplace": "amazon",
        "order_id": str(row["amazon_order_id"]),
        "external_order_id": str(row["seller_order_id"] or row["amazon_order_id"]),
        "order_date": str(row["purchase_date"] or ""),
        "customer": str(row["buyer_name"] or "Amazon-Kunde"),
        "article": str(row["first_article"] or "-"),
        "line_items_count": int(row["line_items_count"] or 0),
        "total_cents": sales_cents,
        "sales_gross_cents": sales_cents,
        "sales_net_cents": max(sales_cents - sales_vat_cents, 0),
        "sales_vat_cents": sales_vat_cents,
        "fees_cents": fees_cents,
        "after_fees_cents": after_fees_cents,
        "shipping_cents": 0,
        "currency": str(row["currency"] or "EUR"),
        "fulfillment_status": str(row["order_status"] or "unknown"),
        "fulfillment_channel": str(row["fulfillment_channel"] or "FBA"),
        "payment_method": "Amazon",
        "fee_source": "amazon_finance" if financial_event_count else "pending_amazon_finance",
        "financial_status": str(row["financial_finality"] or "pending"),
        "financial_finality": str(row["financial_finality"] or "pending"),
        "raw_status": str(row["order_status"] or ""),
        "source_marketplace_id": str(row["marketplace_id"] or ""),
        "raw_json": str(row["raw_json"] or "{}"),
        "purchase_cost_cents": cogs_cents,
        "purchase_cost_eur": cents_to_eur(cogs_cents),
        "purchase_vat_cents": 0,
        "purchase_is_vat_deductible": False,
        "purchase_currency": str(row["currency"] or "EUR"),
        "purchase_supplier": None,
        "purchase_notes": "FIFO aus Amazon-FBA-Beschaffung" if cogs_cents else None,
        "invoice": None,
        "profit_cents": after_fees_cents - cogs_cents,
        "is_fba": str(row["fulfillment_channel"] or "").upper() == "AFN",
    }


def load_amazon_order_summaries() -> list[dict[str, Any]]:
    init_amazon_fba_db()
    canonical_event = _canonical_financial_event_predicate("e")
    with _connect() as connection:
        rows = connection.execute(
            f"""
            SELECT
                o.*,
                (SELECT COUNT(*) FROM amazon_order_items oi WHERE oi.amazon_order_id = o.amazon_order_id) AS line_items_count,
                (SELECT oi.title FROM amazon_order_items oi WHERE oi.amazon_order_id = o.amazon_order_id ORDER BY oi.id LIMIT 1) AS first_article,
                COALESCE((SELECT SUM(oi.item_price_cents) FROM amazon_order_items oi WHERE oi.amazon_order_id = o.amazon_order_id), 0) AS item_sales_cents,
                COALESCE((SELECT SUM(oi.item_tax_cents) FROM amazon_order_items oi WHERE oi.amazon_order_id = o.amazon_order_id), 0) AS item_tax_cents,
                COALESCE((SELECT SUM(e.sales_cents) FROM amazon_financial_events e WHERE e.amazon_order_id = o.amazon_order_id AND {canonical_event}), 0) AS financial_sales_cents,
                COALESCE((SELECT SUM(e.fees_cents) FROM amazon_financial_events e WHERE e.amazon_order_id = o.amazon_order_id AND {canonical_event}), 0) AS financial_fees_cents,
                 COALESCE((SELECT SUM(COALESCE(a.allocated_cost_cents, a.quantity * a.unit_cost_cents)) FROM fifo_allocations a WHERE a.amazon_order_id = o.amazon_order_id), 0) AS fifo_cogs_cents,
                (SELECT COUNT(*) FROM amazon_financial_events e WHERE e.amazon_order_id = o.amazon_order_id AND {canonical_event}) AS financial_event_count,
                COALESCE((SELECT CASE WHEN COUNT(*) = 0 THEN 'pending' WHEN MIN(e.financial_finality) = 'released' THEN 'released' ELSE 'deferred' END FROM amazon_financial_events e WHERE e.amazon_order_id = o.amazon_order_id AND {canonical_event}), 'pending') AS financial_finality
            FROM amazon_orders o
            ORDER BY COALESCE(o.purchase_date, '') DESC, o.amazon_order_id DESC
            """
        ).fetchall()
    return [_order_summary(row) for row in rows]


def get_amazon_order_detail(order_id: str) -> Optional[dict[str, Any]]:
    init_amazon_fba_db()
    canonical_event = _canonical_financial_event_predicate("e")
    with _connect() as connection:
        row = connection.execute("SELECT * FROM amazon_orders WHERE amazon_order_id = ? LIMIT 1", (order_id,)).fetchone()
        if row is None:
            return None
        summary_row = connection.execute(
            f"""
            SELECT
                o.*,
                (SELECT COUNT(*) FROM amazon_order_items oi WHERE oi.amazon_order_id = o.amazon_order_id) AS line_items_count,
                (SELECT oi.title FROM amazon_order_items oi WHERE oi.amazon_order_id = o.amazon_order_id ORDER BY oi.id LIMIT 1) AS first_article,
                COALESCE((SELECT SUM(oi.item_price_cents) FROM amazon_order_items oi WHERE oi.amazon_order_id = o.amazon_order_id), 0) AS item_sales_cents,
                COALESCE((SELECT SUM(oi.item_tax_cents) FROM amazon_order_items oi WHERE oi.amazon_order_id = o.amazon_order_id), 0) AS item_tax_cents,
                COALESCE((SELECT SUM(e.sales_cents) FROM amazon_financial_events e WHERE e.amazon_order_id = o.amazon_order_id AND {canonical_event}), 0) AS financial_sales_cents,
                COALESCE((SELECT SUM(e.fees_cents) FROM amazon_financial_events e WHERE e.amazon_order_id = o.amazon_order_id AND {canonical_event}), 0) AS financial_fees_cents,
                COALESCE((SELECT SUM(COALESCE(a.allocated_cost_cents, a.quantity * a.unit_cost_cents)) FROM fifo_allocations a WHERE a.amazon_order_id = o.amazon_order_id), 0) AS fifo_cogs_cents,
                (SELECT COUNT(*) FROM amazon_financial_events e WHERE e.amazon_order_id = o.amazon_order_id AND {canonical_event}) AS financial_event_count,
                COALESCE((SELECT CASE WHEN COUNT(*) = 0 THEN 'pending' WHEN MIN(e.financial_finality) = 'released' THEN 'released' ELSE 'deferred' END FROM amazon_financial_events e WHERE e.amazon_order_id = o.amazon_order_id AND {canonical_event}), 'pending') AS financial_finality
            FROM amazon_orders o WHERE o.amazon_order_id = ?
            """,
            (order_id,),
        ).fetchone()
        item_rows = connection.execute("SELECT * FROM amazon_order_items WHERE amazon_order_id = ? ORDER BY id", (order_id,)).fetchall()
        event_rows = connection.execute(
            f"SELECT * FROM amazon_financial_events e WHERE e.amazon_order_id = ? AND {_canonical_financial_event_predicate('e')} ORDER BY e.posted_date, e.id",
            (order_id,),
        ).fetchall()
        allocation_rows = connection.execute(
            """
            SELECT a.*, l.seller_sku, l.inbound_shipment_id,
                   COALESCE(b.reference, l.inbound_shipment_id) AS batch_reference
            FROM fifo_allocations a
            JOIN inventory_lots l ON l.id = a.inventory_lot_id
            LEFT JOIN procurement_batch_lines bl ON bl.id = l.batch_line_id
            LEFT JOIN procurement_batches b ON b.id = bl.batch_id
            WHERE a.amazon_order_id = ? ORDER BY a.allocated_at, a.id
            """,
            (order_id,),
        ).fetchall()

    raw_order = _raw_json(row["raw_json"])
    shipping_address = normalize_amazon_address(raw_order.get("ShippingAddress"))
    billing_address = normalize_amazon_address(raw_order.get("BillingAddress"))
    events: list[dict[str, Any]] = []
    for event_row in event_rows:
        event = dict(event_row)
        if str(event.get("event_type") or "").startswith("ModernTransaction:"):
            event["financial_breakdown"] = extract_modern_financial_breakdown(_raw_json(event.get("raw_json")))
        events.append(event)

    line_items: list[dict[str, Any]] = []
    for item_row in item_rows:
        item = dict(item_row)
        try:
            item["image_urls"] = json.loads(str(item.get("image_urls_json") or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            item["image_urls"] = []
        if not isinstance(item["image_urls"], list):
            item["image_urls"] = []
        line_items.append(item)

    return {
        "summary": _order_summary(summary_row),
        "order": dict(row),
        "order_raw": raw_order,
        "line_items": line_items,
        "financial_events": events,
        "fifo_allocations": [dict(allocation) for allocation in allocation_rows],
        "shipping_address": shipping_address,
        "billing_address": billing_address,
        "customer": {"name": str(row["buyer_name"] or "Amazon-Kunde"), "email": str(row["buyer_email"] or "")},
    }


def get_amazon_inventory_summary() -> dict[str, Any]:
    init_amazon_fba_db()
    with _connect() as connection:
        latest = connection.execute("SELECT MAX(captured_at) FROM amazon_inventory_snapshots").fetchone()[0]
        if not latest:
            return {"captured_at": None, "items": [], "totals": {"unique_skus": 0, "fulfillable": 0, "inbound_working": 0, "inbound_shipped": 0, "reserved": 0}}
        rows = connection.execute(
            """
            SELECT
                COALESCE(NULLIF(fnsku, ''), seller_sku) AS inventory_key,
                MAX(seller_sku) AS seller_sku, MAX(fnsku) AS fnsku, MAX(asin) AS asin, MAX(product_name) AS product_name,
                MAX(fulfillable_quantity) AS fulfillable_quantity,
                MAX(inbound_working_quantity) AS inbound_working_quantity,
                MAX(inbound_shipped_quantity) AS inbound_shipped_quantity,
                MAX(reserved_quantity) AS reserved_quantity,
                MAX(unfulfillable_quantity) AS unfulfillable_quantity,
                GROUP_CONCAT(DISTINCT marketplace_id) AS marketplace_ids
            FROM amazon_inventory_snapshots
            WHERE captured_at = ?
            GROUP BY COALESCE(NULLIF(fnsku, ''), seller_sku)
            ORDER BY product_name, seller_sku
            """,
            (latest,),
        ).fetchall()
    items = [dict(row) for row in rows]
    return {
        "captured_at": latest,
        "items": items,
        "totals": {
            "unique_skus": len(items),
            "fulfillable": sum(int(item["fulfillable_quantity"] or 0) for item in items),
            "inbound_working": sum(int(item["inbound_working_quantity"] or 0) for item in items),
            "inbound_shipped": sum(int(item["inbound_shipped_quantity"] or 0) for item in items),
            "reserved": sum(int(item["reserved_quantity"] or 0) for item in items),
            "unfulfillable": sum(int(item["unfulfillable_quantity"] or 0) for item in items),
        },
    }


def set_amazon_sku_hidden(sku_key: str, *, hidden: bool) -> None:
    """Persist an explicit user choice to hide (or unhide) a SKU from the
    default 'Bestand' inventory listing, independent of the automatic
    dormant-SKU filter."""
    init_amazon_fba_db()
    with _connect() as connection:
        connection.execute(
            """
            INSERT INTO amazon_sku_visibility(sku_key, hidden, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(sku_key) DO UPDATE SET hidden=excluded.hidden, updated_at=excluded.updated_at
            """,
            (sku_key, 1 if hidden else 0, _utc_now()),
        )
        connection.commit()


def list_amazon_sku_inventory(*, include_hidden: bool = False, include_dormant: bool = False) -> list[dict[str, Any]]:
    """Aggregate sales, FIFO cost, and current stock per SKU for the
    'Bestand' inventory view. The canonical key is seller_sku (falling
    back to asin/fnsku when seller_sku is blank), matching how order
    items, FIFO lots, and inbound shipment items already reference SKUs.
    Includes SKUs that only have current stock and no sales yet.

    'margin_cents' is real profit, not just revenue minus purchase cost:
    it starts from sales_net_cents (item price minus the VAT it already
    includes, since VAT collected is not the seller's money), then
    subtracts both cogs_cents and fees_cents (Amazon's cut). Amazon
    reports fees per order, not per line item, so each order's total fees
    are split across its items proportionally by item revenue share --
    exact for single-SKU orders, an approximation for multi-SKU orders.

    By default, excludes SKUs the operator explicitly hid (persisted via
    set_amazon_sku_hidden) and 'dormant' SKUs with zero current stock AND
    zero sales (e.g. a stale order-item row for a discontinued product) --
    those carry no decision-relevant information. Pass include_hidden=True
    and/or include_dormant=True to see everything."""
    init_amazon_fba_db()
    canonical_event = _canonical_financial_event_predicate("e")
    with _connect() as connection:
        latest_snapshot = connection.execute(
            "SELECT MAX(captured_at) FROM amazon_inventory_snapshots"
        ).fetchone()[0]

        sales_rows = connection.execute(
            """
            SELECT
                COALESCE(NULLIF(seller_sku, ''), asin) AS sku_key,
                MAX(seller_sku) AS seller_sku, MAX(asin) AS asin, MAX(title) AS title,
                SUM(quantity_shipped) AS quantity_sold, SUM(item_price_cents) AS sales_cents,
                SUM(item_tax_cents) AS tax_cents,
                MAX(NULLIF(image_url, '')) AS image_url
            FROM amazon_order_items
            WHERE COALESCE(NULLIF(seller_sku, ''), asin) <> ''
            GROUP BY sku_key
            """
        ).fetchall()
        sales_by_sku = {str(row["sku_key"]): dict(row) for row in sales_rows}

        cogs_rows = connection.execute(
            """
            SELECT l.seller_sku AS sku_key,
                   SUM(COALESCE(a.allocated_cost_cents, a.quantity * a.unit_cost_cents)) AS cogs_cents
            FROM fifo_allocations a
            JOIN inventory_lots l ON l.id = a.inventory_lot_id
            WHERE l.seller_sku <> ''
            GROUP BY l.seller_sku
            """
        ).fetchall()
        cogs_by_sku = {str(row["sku_key"]): int(row["cogs_cents"] or 0) for row in cogs_rows}

        fee_rows = connection.execute(
            f"""
            SELECT
                COALESCE(NULLIF(oi.seller_sku, ''), oi.asin) AS sku_key,
                oi.item_price_cents AS item_revenue_cents,
                order_totals.order_item_revenue_cents,
                COALESCE(order_fees.order_fees_cents, 0) AS order_fees_cents
            FROM amazon_order_items oi
            JOIN (
                SELECT amazon_order_id, SUM(item_price_cents) AS order_item_revenue_cents
                FROM amazon_order_items
                GROUP BY amazon_order_id
            ) order_totals ON order_totals.amazon_order_id = oi.amazon_order_id
            LEFT JOIN (
                SELECT amazon_order_id, SUM(e.fees_cents) AS order_fees_cents
                FROM amazon_financial_events e
                WHERE {canonical_event}
                GROUP BY amazon_order_id
            ) order_fees ON order_fees.amazon_order_id = oi.amazon_order_id
            WHERE COALESCE(NULLIF(oi.seller_sku, ''), oi.asin) <> ''
            """
        ).fetchall()
        fees_by_sku: dict[str, float] = {}
        for row in fee_rows:
            sku_key = str(row["sku_key"])
            order_revenue = int(row["order_item_revenue_cents"] or 0)
            item_revenue = int(row["item_revenue_cents"] or 0)
            order_fees = int(row["order_fees_cents"] or 0)
            share = (item_revenue / order_revenue) if order_revenue else 0.0
            fees_by_sku[sku_key] = fees_by_sku.get(sku_key, 0.0) + order_fees * share

        stock_rows: list[sqlite3.Row] = []
        if latest_snapshot:
            stock_rows = connection.execute(
                """
                SELECT
                    COALESCE(NULLIF(seller_sku, ''), fnsku) AS sku_key,
                    MAX(seller_sku) AS seller_sku, MAX(asin) AS asin, MAX(product_name) AS product_name,
                    MAX(fulfillable_quantity) AS fulfillable_quantity,
                    MAX(inbound_working_quantity) AS inbound_working_quantity,
                    MAX(inbound_shipped_quantity) AS inbound_shipped_quantity,
                    MAX(reserved_quantity) AS reserved_quantity
                FROM amazon_inventory_snapshots
                WHERE captured_at = ? AND COALESCE(NULLIF(seller_sku, ''), fnsku) <> ''
                GROUP BY sku_key
                """,
                (latest_snapshot,),
            ).fetchall()
        stock_by_sku = {str(row["sku_key"]): dict(row) for row in stock_rows}

        hidden_rows = connection.execute(
            "SELECT sku_key FROM amazon_sku_visibility WHERE hidden = 1"
        ).fetchall()
        hidden_keys = {str(row["sku_key"]) for row in hidden_rows}

    items: list[dict[str, Any]] = []
    for sku_key in set(sales_by_sku) | set(stock_by_sku):
        sales = sales_by_sku.get(sku_key, {})
        stock = stock_by_sku.get(sku_key, {})
        sales_cents = int(sales.get("sales_cents") or 0)
        tax_cents = min(max(int(sales.get("tax_cents") or 0), 0), sales_cents)
        sales_net_cents = sales_cents - tax_cents
        cogs_cents = cogs_by_sku.get(sku_key, 0)
        fees_cents = round(fees_by_sku.get(sku_key, 0.0))
        fulfillable_quantity = int(stock.get("fulfillable_quantity") or 0)
        inbound_working_quantity = int(stock.get("inbound_working_quantity") or 0)
        inbound_shipped_quantity = int(stock.get("inbound_shipped_quantity") or 0)
        quantity_sold = int(sales.get("quantity_sold") or 0)
        is_hidden = sku_key in hidden_keys
        is_dormant = (
            quantity_sold == 0 and sales_cents == 0
            and fulfillable_quantity == 0 and inbound_working_quantity == 0 and inbound_shipped_quantity == 0
        )
        if is_hidden and not include_hidden:
            continue
        if is_dormant and not include_dormant:
            continue
        margin_cents = sales_net_cents - cogs_cents - fees_cents
        items.append({
            "sku_key": sku_key,
            "seller_sku": str(sales.get("seller_sku") or stock.get("seller_sku") or ""),
            "asin": str(sales.get("asin") or stock.get("asin") or ""),
            "title": str(sales.get("title") or stock.get("product_name") or ""),
            "image_url": str(sales.get("image_url") or ""),
            "quantity_sold": quantity_sold,
            "sales_cents": sales_cents,
            "tax_cents": tax_cents,
            "sales_net_cents": sales_net_cents,
            "fees_cents": fees_cents,
            "cogs_cents": cogs_cents,
            "margin_cents": margin_cents,
            "margin_percent": round(margin_cents / sales_net_cents * 100, 1) if sales_net_cents else None,
            "fulfillable_quantity": fulfillable_quantity,
            "inbound_working_quantity": inbound_working_quantity,
            "inbound_shipped_quantity": inbound_shipped_quantity,
            "reserved_quantity": int(stock.get("reserved_quantity") or 0),
            "hidden": is_hidden,
        })
    items.sort(key=lambda item: (item["title"] or item["sku_key"]).lower())
    return items


def get_amazon_sku_detail(sku_key: str) -> Optional[dict[str, Any]]:
    """Per-SKU drill-down: everything from list_amazon_sku_inventory()
    (including its tax/fee-aware margin_cents), plus the average Amazon fee
    per unit, estimated days of stock remaining, and associated inbound
    shipments."""
    base = next((item for item in list_amazon_sku_inventory(include_hidden=True, include_dormant=True) if item["sku_key"] == sku_key), None)
    if base is None:
        return None

    init_amazon_fba_db()
    thirty_days_ago = (datetime.now(timezone.utc) - timedelta(days=30)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    with _connect() as connection:
        recent_quantity = connection.execute(
            """
            SELECT COALESCE(SUM(oi.quantity_shipped), 0)
            FROM amazon_order_items oi
            JOIN amazon_orders o ON o.amazon_order_id = oi.amazon_order_id
            WHERE COALESCE(NULLIF(oi.seller_sku, ''), oi.asin) = ? AND o.purchase_date >= ?
            """,
            (sku_key, thirty_days_ago),
        ).fetchone()[0]
        shipment_rows = connection.execute(
            """
            SELECT s.shipment_id, s.shipment_name, s.status, i.quantity_shipped, i.quantity_received
            FROM amazon_inbound_shipment_items i
            JOIN amazon_inbound_shipments s ON s.shipment_id = i.shipment_id
            WHERE COALESCE(NULLIF(i.seller_sku, ''), i.asin) = ?
            ORDER BY s.updated_at DESC
            """,
            (sku_key,),
        ).fetchall()

    fee_per_unit_cents = round(base["fees_cents"] / base["quantity_sold"]) if base["quantity_sold"] else None

    current_stock = base["fulfillable_quantity"] + base["inbound_working_quantity"] + base["inbound_shipped_quantity"]
    daily_velocity = int(recent_quantity or 0) / 30.0
    days_of_stock = round(current_stock / daily_velocity, 1) if daily_velocity > 0 else None

    shipments = [
        {
            "shipment_id": str(row["shipment_id"]),
            "shipment_name": str(row["shipment_name"] or ""),
            **normalize_fba_status(str(row["status"] or "")),
            "quantity_shipped": int(row["quantity_shipped"] or 0),
            "quantity_received": int(row["quantity_received"] or 0),
        }
        for row in shipment_rows
    ]

    return {
        **base,
        "fee_per_unit_cents": fee_per_unit_cents,
        "quantity_sold_last_30_days": int(recent_quantity or 0),
        "days_of_stock": days_of_stock,
        "shipments": shipments,
    }


def list_inbound_shipments(status: Optional[str] = None) -> list[dict[str, Any]]:
    init_amazon_fba_db()
    with _connect() as connection:
        params: list[Any] = []
        where = "WHERE s.status <> 'CANCELLED'"
        if status:
            where = "WHERE s.status = ?"
            params.append(status.upper())
        rows = connection.execute(
            f"""
            SELECT
                s.shipment_id, s.plan_id, s.shipment_name, s.status,
                s.destination_fulfillment_center_id, s.updated_at,
                COALESCE((SELECT SUM(i.quantity_shipped) FROM amazon_inbound_shipment_items i WHERE i.shipment_id = s.shipment_id), 0) AS quantity_shipped,
                COALESCE((SELECT SUM(i.quantity_received) FROM amazon_inbound_shipment_items i WHERE i.shipment_id = s.shipment_id), 0) AS quantity_received,
                COALESCE((SELECT COUNT(*) FROM amazon_inbound_shipment_items i WHERE i.shipment_id = s.shipment_id), 0) AS sku_count,
                (SELECT COUNT(*) FROM amazon_inbound_invoices inv WHERE inv.shipment_id = s.shipment_id) AS invoice_count,
                (SELECT COUNT(*) FROM amazon_inbound_cost_allocations allocation WHERE allocation.shipment_id = s.shipment_id) AS allocation_count,
                COALESCE((SELECT SUM(c.amount_cents) FROM amazon_inbound_costs c WHERE c.shipment_id = s.shipment_id), 0) AS assigned_cost_cents,
                (SELECT currency FROM amazon_inbound_transport_options t WHERE t.shipment_id = s.shipment_id AND t.selected = 1 LIMIT 1) AS transport_currency,
                (SELECT quote_cents FROM amazon_inbound_transport_options t WHERE t.shipment_id = s.shipment_id AND t.selected = 1 LIMIT 1) AS transport_quote_cents
            FROM amazon_inbound_shipments s
            {where}
            ORDER BY s.updated_at DESC, s.shipment_id DESC
            """,
            params,
        ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        status_info = normalize_fba_status(str(item.get("status") or ""))
        item.update(status_info)
        item["status_label"] = status_info["label"]
        if item["allocation_count"]:
            item["cost_status"] = "confirmed"
        elif item["invoice_count"]:
            item["cost_status"] = "entered"
        else:
            item["cost_status"] = "missing"
        result.append(item)
    return result


def get_inbound_shipment(shipment_id: str) -> Optional[dict[str, Any]]:
    init_amazon_fba_db()
    with _connect() as connection:
        row = connection.execute(
            "SELECT * FROM amazon_inbound_shipments WHERE shipment_id = ? LIMIT 1",
            (shipment_id,),
        ).fetchone()
        if row is None:
            return None
        items = connection.execute(
            "SELECT * FROM amazon_inbound_shipment_items WHERE shipment_id = ? ORDER BY seller_sku, fnsku",
            (shipment_id,),
        ).fetchall()
        boxes = connection.execute(
            "SELECT * FROM amazon_inbound_shipment_boxes WHERE shipment_id = ? ORDER BY box_id",
            (shipment_id,),
        ).fetchall()
        transport_options = connection.execute(
            "SELECT * FROM amazon_inbound_transport_options WHERE shipment_id = ? ORDER BY selected DESC, option_id",
            (shipment_id,),
        ).fetchall()
        costs = connection.execute(
            "SELECT * FROM amazon_inbound_costs WHERE shipment_id = ? AND status <> 'superseded' ORDER BY id",
            (shipment_id,),
        ).fetchall()
        invoices = connection.execute(
            "SELECT * FROM amazon_inbound_invoices WHERE shipment_id = ? ORDER BY created_at DESC",
            (shipment_id,),
        ).fetchall()
        invoice_lines = connection.execute(
            """
            SELECT l.*, i.shipment_id
            FROM amazon_inbound_invoice_lines l
            JOIN amazon_inbound_invoices i ON i.id = l.invoice_id
            WHERE i.shipment_id = ? ORDER BY l.seller_sku, l.fnsku
            """,
            (shipment_id,),
        ).fetchall()
        cost_allocations = connection.execute(
            "SELECT * FROM amazon_inbound_cost_allocations WHERE shipment_id = ? ORDER BY seller_sku, id",
            (shipment_id,),
        ).fetchall()
    shipment = dict(row)
    status_info = normalize_fba_status(str(shipment.get("status") or ""))
    shipment.update(status_info)
    shipment["status_label"] = status_info["label"]
    return {
        "shipment": shipment,
        "items": [dict(item) for item in items],
        "boxes": [dict(box) for box in boxes],
        "transport_options": [dict(option) for option in transport_options],
        "costs": [dict(cost) for cost in costs],
        "invoices": [dict(invoice) for invoice in invoices],
        "invoice_lines": [dict(line) for line in invoice_lines],
        "cost_allocations": [dict(allocation) for allocation in cost_allocations],
    }


def list_inbound_costs(shipment_id: Optional[str] = None) -> list[dict[str, Any]]:
    init_amazon_fba_db()
    with _connect() as connection:
        if shipment_id:
            rows = connection.execute(
                "SELECT * FROM amazon_inbound_costs WHERE shipment_id = ? AND status <> 'superseded' ORDER BY id",
                (shipment_id,),
            ).fetchall()
        else:
            rows = connection.execute("SELECT * FROM amazon_inbound_costs WHERE status <> 'superseded' ORDER BY status, id").fetchall()
    return [dict(row) for row in rows]


def add_inbound_cost(
    *,
    shipment_id: str,
    cost_type: str,
    amount_cents: int,
    currency: str = "EUR",
    allocation_method: str = "value",
    status: str = "manual",
    source_event_id: Optional[str] = None,
    raw_json: str = "{}",
    notes: str = "",
) -> dict[str, Any]:
    if amount_cents < 0:
        raise ValueError("amount_cents must be non-negative")
    init_amazon_fba_db()
    cost_id = str(uuid.uuid4())
    with _connect() as connection:
        if connection.execute("SELECT 1 FROM amazon_inbound_shipments WHERE shipment_id = ?", (shipment_id,)).fetchone() is None:
            raise ValueError("Amazon FBA shipment not found")
        connection.execute(
            """
            INSERT INTO amazon_inbound_costs(
                id, shipment_id, source_event_id, cost_type, amount_cents,
                currency, status, allocation_method, raw_json, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (cost_id, shipment_id, source_event_id, cost_type.strip(), amount_cents, currency.upper(), status, allocation_method, raw_json, notes.strip()),
        )
        connection.commit()
    return {"id": cost_id, "shipment_id": shipment_id, "amount_cents": amount_cents, "currency": currency.upper(), "status": status}


def assign_inbound_cost(*, cost_id: str, shipment_id: str) -> dict[str, Any]:
    init_amazon_fba_db()
    with _connect() as connection:
        if connection.execute("SELECT 1 FROM amazon_inbound_shipments WHERE shipment_id = ?", (shipment_id,)).fetchone() is None:
            raise ValueError("Amazon FBA shipment not found")
        updated = connection.execute(
            "UPDATE amazon_inbound_costs SET shipment_id = ?, status = 'confirmed' WHERE id = ?",
            (shipment_id, cost_id),
        ).rowcount
        if not updated:
            raise ValueError("Amazon inbound cost not found")
        connection.commit()
        row = connection.execute("SELECT * FROM amazon_inbound_costs WHERE id = ?", (cost_id,)).fetchone()
    return dict(row)


def add_inbound_invoice(
    *,
    shipment_id: str,
    supplier_name: str,
    invoice_number: str,
    invoice_date: Optional[str],
    currency: str,
    gross_cents: int,
    net_cents: int,
    vat_cents: int,
    document_path: str,
    notes: str = "",
) -> dict[str, Any]:
    if not supplier_name.strip():
        raise ValueError("supplier_name is required")
    if gross_cents < 0 or net_cents < 0 or vat_cents < 0:
        raise ValueError("invoice amounts must be non-negative")
    if gross_cents != net_cents + vat_cents:
        raise ValueError("gross_cents must equal net_cents plus vat_cents")
    init_amazon_fba_db()
    invoice_id = str(uuid.uuid4())
    with _connect() as connection:
        if connection.execute("SELECT 1 FROM amazon_inbound_shipments WHERE shipment_id = ?", (shipment_id,)).fetchone() is None:
            raise ValueError("Amazon FBA shipment not found")
        connection.execute(
            """
            INSERT INTO amazon_inbound_invoices(
                id, shipment_id, supplier_name, invoice_number, invoice_date,
                currency, gross_cents, net_cents, vat_cents, document_path, notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (invoice_id, shipment_id, supplier_name.strip(), invoice_number.strip(), invoice_date, currency.upper(), gross_cents, net_cents, vat_cents, document_path, notes.strip(), _utc_now()),
        )
        if net_cents > 0:
            connection.execute(
                """
                INSERT INTO amazon_inbound_costs(
                    id, shipment_id, source_event_id, cost_type, amount_cents,
                    currency, status, allocation_method, raw_json
                ) VALUES (?, ?, ?, 'supplier_product', ?, ?, 'manual', 'value', ?)
                """,
                (_stable_id("amazon-inbound-invoice-cost", invoice_id), shipment_id, invoice_id, net_cents, currency.upper(), _json_dumps({"invoice_id": invoice_id})),
            )
        connection.commit()
    return {"id": invoice_id, "shipment_id": shipment_id, "document_path": document_path}


def add_inbound_invoice_line(
    *,
    invoice_id: str,
    seller_sku: str,
    fnsku: str,
    asin: str,
    title: str,
    quantity: int,
    gross_cents: int,
    net_cents: int,
    vat_cents: int,
) -> dict[str, Any]:
    if quantity <= 0:
        raise ValueError("invoice line quantity must be positive")
    if net_cents < 0 or vat_cents < 0:
        raise ValueError("invoice line amounts must be non-negative")
    if gross_cents != net_cents + vat_cents:
        raise ValueError("gross_cents must equal net_cents plus vat_cents")
    if not seller_sku.strip() and not fnsku.strip():
        raise ValueError("invoice line requires seller SKU or FNSKU")
    init_amazon_fba_db()
    line_id = _stable_id("amazon-inbound-invoice-line", f"{invoice_id}:{seller_sku.strip()}:{fnsku.strip()}")
    with _connect() as connection:
        invoice = connection.execute(
            "SELECT id, shipment_id FROM amazon_inbound_invoices WHERE id = ?", (invoice_id,)
        ).fetchone()
        if invoice is None:
            raise ValueError("Amazon inbound invoice not found")
        if connection.execute(
            "SELECT 1 FROM amazon_inbound_cost_allocations WHERE invoice_id = ? LIMIT 1", (invoice_id,)
        ).fetchone() is not None:
            raise ValueError("invoice costs are already confirmed")
        connection.execute(
            """
            INSERT INTO amazon_inbound_invoice_lines(
                id, invoice_id, seller_sku, fnsku, asin, title, quantity,
                gross_cents, net_cents, vat_cents, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}')
            ON CONFLICT(invoice_id, seller_sku, fnsku) DO UPDATE SET
                asin=excluded.asin, title=excluded.title, quantity=excluded.quantity,
                gross_cents=excluded.gross_cents, net_cents=excluded.net_cents,
                vat_cents=excluded.vat_cents
            """,
            (
                line_id, invoice_id, seller_sku.strip(), fnsku.strip(), asin.strip(),
                title.strip(), quantity, gross_cents, net_cents, vat_cents,
            ),
        )
        connection.commit()
        row = connection.execute("SELECT * FROM amazon_inbound_invoice_lines WHERE id = ?", (line_id,)).fetchone()
    return dict(row)


def list_inbound_invoice_lines(invoice_id: str) -> list[dict[str, Any]]:
    init_amazon_fba_db()
    with _connect() as connection:
        rows = connection.execute(
            "SELECT * FROM amazon_inbound_invoice_lines WHERE invoice_id = ? ORDER BY seller_sku, fnsku",
            (invoice_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def confirm_inbound_product_costs(shipment_id: str) -> dict[str, Any]:
    init_amazon_fba_db()
    with _connect() as connection:
        shipment = connection.execute(
            "SELECT * FROM amazon_inbound_shipments WHERE shipment_id = ?", (shipment_id,)
        ).fetchone()
        if shipment is None:
            raise ValueError("Amazon FBA shipment not found")
        if str(shipment["status"] or "").upper() not in {"RECEIVING", "CLOSED"}:
            raise ValueError("shipment has not been received")
        items = connection.execute(
            "SELECT * FROM amazon_inbound_shipment_items WHERE shipment_id = ? ORDER BY seller_sku, fnsku",
            (shipment_id,),
        ).fetchall()
        invoices = connection.execute(
            "SELECT * FROM amazon_inbound_invoices WHERE shipment_id = ? ORDER BY created_at DESC",
            (shipment_id,),
        ).fetchall()
        if not invoices:
            raise ValueError("supplier invoice is required")
        lines = connection.execute(
            """
            SELECT l.*, i.shipment_id, i.currency, i.gross_cents AS invoice_gross_cents,
                   i.net_cents AS invoice_net_cents, i.vat_cents AS invoice_vat_cents
            FROM amazon_inbound_invoice_lines l
            JOIN amazon_inbound_invoices i ON i.id = l.invoice_id
            WHERE i.shipment_id = ?
            ORDER BY l.invoice_id, l.seller_sku, l.fnsku
            """,
            (shipment_id,),
        ).fetchall()
        if not lines:
            raise ValueError("invoice lines are required")

        lines_by_invoice: dict[str, list[sqlite3.Row]] = {}
        for line in lines:
            lines_by_invoice.setdefault(str(line["invoice_id"]), []).append(line)
        for invoice in invoices:
            invoice_lines = lines_by_invoice.get(str(invoice["id"]), [])
            if sum(int(line["gross_cents"]) for line in invoice_lines) != int(invoice["gross_cents"]):
                raise ValueError("invoice line gross total must match invoice gross total")
            if sum(int(line["net_cents"]) for line in invoice_lines) != int(invoice["net_cents"]):
                raise ValueError("invoice line net total must match invoice net total")
            if sum(int(line["vat_cents"]) for line in invoice_lines) != int(invoice["vat_cents"]):
                raise ValueError("invoice line VAT total must match invoice VAT total")

        received_items = [item for item in items if int(item["quantity_received"] or 0) > 0]
        items_by_key = {
            (str(item["seller_sku"] or ""), str(item["fnsku"] or "")): item
            for item in received_items
        }
        lines_by_sku: dict[tuple[str, str], list[sqlite3.Row]] = {}
        for line in lines:
            key = (str(line["seller_sku"] or ""), str(line["fnsku"] or ""))
            lines_by_sku.setdefault(key, []).append(line)
        if set(lines_by_sku) != set(items_by_key):
            raise ValueError("invoice lines must cover every shipment SKU")
        if any(len(sku_lines) != 1 for sku_lines in lines_by_sku.values()):
            raise ValueError("each shipment SKU requires exactly one invoice line")
        for sku_lines in lines_by_sku.values():
            line = sku_lines[0]
            key = (str(line["seller_sku"] or ""), str(line["fnsku"] or ""))
            item = items_by_key[key]
            if int(line["quantity"] or 0) != int(item["quantity_received"] or 0):
                raise ValueError("invoice line quantities must match received quantities")

        existing_lots = connection.execute(
            "SELECT * FROM inventory_lots WHERE inbound_shipment_id = ? ORDER BY seller_sku, id",
            (shipment_id,),
        ).fetchall()
        if existing_lots:
            return {
                "shipment_id": shipment_id,
                "allocations": [dict(row) for row in connection.execute(
                    "SELECT * FROM amazon_inbound_cost_allocations WHERE shipment_id = ? ORDER BY seller_sku, id",
                    (shipment_id,),
                ).fetchall()],
                "lots": [dict(row) for row in existing_lots],
            }

        allocations: list[dict[str, Any]] = []
        lots: list[dict[str, Any]] = []
        for line in lines:
            key = (str(line["seller_sku"] or ""), str(line["fnsku"] or ""))
            item = items_by_key[key]
            quantity = int(line["quantity"] or 0)
            net_cents = int(line["net_cents"] or 0)
            unit_cost_cents = net_cents // quantity
            cost_remainder_cents = net_cents - unit_cost_cents * quantity
            allocation_id = _stable_id("amazon-inbound-cost-allocation", str(line["id"]))
            lot_id = _stable_id("amazon-inbound-inventory-lot", f"{shipment_id}:{key[0]}:{key[1]}")
            allocation = {
                "id": allocation_id,
                "invoice_id": str(line["invoice_id"]),
                "invoice_line_id": str(line["id"]),
                "shipment_id": shipment_id,
                "seller_sku": key[0],
                "fnsku": key[1],
                "quantity": quantity,
                "net_cents": net_cents,
                "currency": str(line["currency"] or "EUR"),
                "allocation_method": "invoice_line",
                "created_at": _utc_now(),
            }
            connection.execute(
                """
                INSERT INTO amazon_inbound_cost_allocations(
                    id, invoice_id, invoice_line_id, shipment_id, seller_sku,
                    fnsku, quantity, net_cents, currency, allocation_method, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(allocation.values()),
            )
            lot = {
                "id": lot_id,
                "batch_line_id": None,
                "inbound_shipment_id": shipment_id,
                "inbound_shipment_item_id": str(item["id"]),
                "seller_sku": key[0],
                "available_quantity": quantity,
                "unit_cost_cents": unit_cost_cents,
                "cost_remainder_cents": cost_remainder_cents,
                "received_at": str(shipment["inventory_eligible_at"] or shipment["updated_at"] or _utc_now()),
                "created_at": _utc_now(),
            }
            connection.execute(
                """
                INSERT INTO inventory_lots(
                    id, batch_line_id, inbound_shipment_id, inbound_shipment_item_id,
                    seller_sku, available_quantity, unit_cost_cents, cost_remainder_cents, received_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lot["id"], lot["batch_line_id"], lot["inbound_shipment_id"], lot["inbound_shipment_item_id"],
                    lot["seller_sku"], lot["available_quantity"], lot["unit_cost_cents"], lot["cost_remainder_cents"],
                    lot["received_at"], lot["created_at"],
                ),
            )
            allocations.append(allocation)
            lots.append(lot)
        connection.commit()
    return {"shipment_id": shipment_id, "allocations": allocations, "lots": lots}


def list_procurement_batches() -> list[dict[str, Any]]:
    init_amazon_fba_db()
    with _connect() as connection:
        rows = connection.execute(
            """
            SELECT
                b.*,
                (SELECT COUNT(*) FROM procurement_batch_lines bl WHERE bl.batch_id = b.id) AS line_count,
                (SELECT COUNT(*) FROM supplier_invoices si WHERE si.batch_id = b.id) AS invoice_count,
                COALESCE((SELECT SUM(si.gross_cents) FROM supplier_invoices si WHERE si.batch_id = b.id), 0) AS invoice_gross_cents
            FROM procurement_batches b
            ORDER BY COALESCE(b.received_at, b.created_at) DESC, b.id DESC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def list_settlement_suggestions() -> list[dict[str, Any]]:
    """Return immutable finance events as review-only bookkeeping proposals."""
    init_amazon_fba_db()
    with _connect() as connection:
        rows = connection.execute(
            """
            SELECT e.id, e.event_type, e.amazon_order_id, e.settlement_id,
                   COALESCE(e.posted_date, s.fund_transfer_date) AS posted_date,
                   e.financial_finality, e.currency, e.sales_cents, e.fees_cents, e.net_cents, e.raw_json
            FROM amazon_financial_events e
            LEFT JOIN amazon_settlements s ON s.settlement_id = e.settlement_id
            ORDER BY COALESCE(e.posted_date, s.fund_transfer_date, '') DESC, e.id DESC
            """
        ).fetchall()
    suggestions: list[dict[str, Any]] = []
    for row in rows:
        event = dict(row)
        proposals: list[dict[str, Any]] = []
        if int(event["sales_cents"] or 0) > 0:
            proposals.append({"type": "SALE", "direction": "IN", "amount_cents": int(event["sales_cents"]), "category": "revenue"})
        if int(event["fees_cents"] or 0) > 0:
            proposals.append({"type": "FEE", "direction": "OUT", "amount_cents": int(event["fees_cents"]), "category": "fees"})
        if not proposals and int(event["net_cents"] or 0):
            proposals.append({
                "type": "ADJUSTMENT",
                "direction": "IN" if int(event["net_cents"]) > 0 else "OUT",
                "amount_cents": abs(int(event["net_cents"])),
                "category": "other",
            })
        suggestions.append({
            "event": event,
            "proposals": proposals,
            "review_status": "pending_review",
            "note": "Nicht automatisch gebucht. Freigabe und Steuerbehandlung muessen geprueft werden.",
        })
    return suggestions


def get_amazon_finance_overview() -> dict[str, Any]:
    init_amazon_fba_db()
    canonical_event = _canonical_financial_event_predicate("e")
    with _connect() as connection:
        event_rows = connection.execute(
            f"""
            SELECT e.id, e.event_type, e.amazon_order_id, e.settlement_id,
                   COALESCE(e.posted_date, s.fund_transfer_date) AS posted_date,
                   e.financial_finality, e.currency, e.sales_cents, e.fees_cents, e.net_cents, e.raw_json
            FROM amazon_financial_events e
            LEFT JOIN amazon_settlements s ON s.settlement_id = e.settlement_id
            WHERE {canonical_event}
            ORDER BY COALESCE(e.posted_date, s.fund_transfer_date, '') DESC, e.id DESC
            """
        ).fetchall()
        component_rows = connection.execute(
            "SELECT event_id, component_type, amount_cents, currency, raw_json FROM amazon_financial_components ORDER BY id"
        ).fetchall()

    report_rows = [row for row in event_rows if str(row["event_type"]) == "SettlementReportLine"]
    report_currencies = {str(row["currency"] or "EUR").upper() for row in report_rows}
    report_recovery_currencies = {
        str(row["currency"] or "EUR").upper()
        for row in report_rows
        if int(row["fees_cents"] or 0) == 0 and int(row["net_cents"] or 0) > 0
    }
    if report_rows:
        event_rows = [
            row
            for row in event_rows
            if not (
                str(row["event_type"]) == "ServiceFeeEventList"
                and str(row["currency"] or "EUR").upper() in report_currencies
            )
            and not (
                str(row["event_type"]) == "DebtRecoveryEventList"
                and str(row["currency"] or "EUR").upper() in report_recovery_currencies
            )
        ]

    components_by_event: dict[str, list[dict[str, Any]]] = {}
    fba_inbound_transport_cents = 0
    for component_row in component_rows:
        component = dict(component_row)
        raw = _raw_json(component.get("raw_json"))
        component["name"] = str(raw.get("FeeType") or raw.get("ChargeType") or component["component_type"])
        components_by_event.setdefault(str(component["event_id"]), []).append(component)
        if "fbainboundtransport" in str(component["name"]).lower():
            fba_inbound_transport_cents += abs(int(component["amount_cents"] or 0))

    events: list[dict[str, Any]] = []
    totals_by_currency: dict[str, dict[str, int]] = {}
    operational_totals_by_currency: dict[str, dict[str, int]] = {}
    released_totals_by_currency: dict[str, dict[str, int]] = {}
    for event_row in event_rows:
        event = dict(event_row)
        event["components"] = components_by_event.get(str(event["id"]), [])
        if not event["components"] and str(event.get("event_type") or "").startswith("ModernTransaction:"):
            breakdown = extract_modern_financial_breakdown(_raw_json(event.get("raw_json")))
            event["components"] = [
                {"name": str(fee["type"]), "amount_cents": int(fee["amount_cents"]), "currency": event["currency"]}
                for fee in breakdown["fees"]
            ]
        currency = str(event["currency"] or "EUR").upper()
        breakdown = extract_modern_financial_breakdown(_raw_json(event.get("raw_json"))) if str(event.get("event_type") or "").startswith("ModernTransaction:") else None
        sales_net_cents = (int(breakdown["sales_cents"]) - int(breakdown["tax_cents"])) if breakdown else int(event["sales_cents"] or 0)
        fees_net_cents = sum(int(fee.get("net_cents") or 0) for fee in (breakdown or {}).get("fees", []))
        fees_vat_cents = sum(int(fee.get("vat_cents") or 0) for fee in (breakdown or {}).get("fees", []))
        event["sales_net_cents"] = sales_net_cents
        event["fees_net_cents"] = fees_net_cents
        event["fees_vat_cents"] = fees_vat_cents
        for aggregate in (operational_totals_by_currency, released_totals_by_currency if event["financial_finality"] == "released" else None):
            if aggregate is None:
                continue
            finance_totals = aggregate.setdefault(currency, {"sales_net_cents": 0, "fees_net_cents": 0, "fees_vat_cents": 0})
            finance_totals["sales_net_cents"] += sales_net_cents
            finance_totals["fees_net_cents"] += fees_net_cents
            finance_totals["fees_vat_cents"] += fees_vat_cents
        totals = totals_by_currency.setdefault(currency, {"sales_cents": 0, "fees_cents": 0, "adjustments_cents": 0})
        totals["sales_cents"] += int(event["sales_cents"] or 0)
        totals["fees_cents"] += int(event["fees_cents"] or 0)
        if not int(event["sales_cents"] or 0) and not int(event["fees_cents"] or 0):
            totals["adjustments_cents"] += int(event["net_cents"] or 0)
        events.append(event)
    for totals in totals_by_currency.values():
        totals["net_cents"] = totals["sales_cents"] - totals["fees_cents"] + totals["adjustments_cents"]
    return {
        "totals_by_currency": totals_by_currency,
        "operational_totals_by_currency": operational_totals_by_currency,
        "released_totals_by_currency": released_totals_by_currency,
        "fba_inbound_transport_cents": fba_inbound_transport_cents,
        "events": events,
    }


def create_procurement_batch(*, reference: str, name: str, lines: list[dict[str, Any]], received_at: Optional[str] = None, notes: str = "") -> dict[str, Any]:
    if not reference.strip() or not name.strip() or not lines:
        raise ValueError("reference, name, and at least one line are required")
    init_amazon_fba_db()
    batch_id = _stable_id("procurement-batch", reference.strip())
    timestamp = _utc_now()
    with _connect() as connection:
        connection.execute(
            "INSERT INTO procurement_batches(id, reference, name, status, received_at, notes, created_at, updated_at) VALUES (?, ?, ?, 'draft', ?, ?, ?, ?)",
            (batch_id, reference.strip(), name.strip(), received_at, notes.strip(), timestamp, timestamp),
        )
        normalized_lines: list[dict[str, Any]] = []
        for index, line in enumerate(lines):
            quantity = int(line.get("quantity") or 0)
            if quantity <= 0:
                raise ValueError("procurement line quantity must be positive")
            line_id = _stable_id("procurement-line", f"{batch_id}:{index}")
            payload = {
                "id": line_id,
                "seller_sku": str(line.get("seller_sku") or "").strip(),
                "fnsku": str(line.get("fnsku") or "").strip(),
                "asin": str(line.get("asin") or "").strip(),
                "title": str(line.get("title") or "").strip(),
                "quantity": quantity,
                "allocation_basis": str(line.get("allocation_basis") or "value").strip() or "value",
            }
            connection.execute(
                "INSERT INTO procurement_batch_lines(id, batch_id, seller_sku, fnsku, asin, title, quantity, allocation_basis) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (payload["id"], batch_id, payload["seller_sku"], payload["fnsku"], payload["asin"], payload["title"], payload["quantity"], payload["allocation_basis"]),
            )
            normalized_lines.append(payload)
        connection.commit()
    return {"id": batch_id, "reference": reference.strip(), "name": name.strip(), "lines": normalized_lines}


def add_supplier_invoice(*, batch_id: str, supplier_name: str, invoice_number: str, invoice_date: Optional[str], currency: str, gross_cents: int, net_cents: int, vat_cents: int, document_path: str = "", notes: str = "", input_vat_status: str = "review_required") -> dict[str, Any]:
    if not batch_id or not supplier_name.strip():
        raise ValueError("batch_id and supplier_name are required")
    init_amazon_fba_db()
    invoice_id = str(uuid.uuid4())
    with _connect() as connection:
        batch = connection.execute("SELECT id FROM procurement_batches WHERE id = ?", (batch_id,)).fetchone()
        if batch is None:
            raise ValueError("procurement batch not found")
        connection.execute(
            """
            INSERT INTO supplier_invoices(id, batch_id, supplier_name, invoice_number, invoice_date, currency, gross_cents, net_cents, vat_cents, input_vat_status, document_path, notes, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (invoice_id, batch_id, supplier_name.strip(), invoice_number.strip(), invoice_date, currency.upper(), int(gross_cents), int(net_cents), int(vat_cents), input_vat_status, document_path.strip(), notes.strip(), _utc_now()),
        )
        connection.commit()
    return {"id": invoice_id, "batch_id": batch_id}


def create_inventory_lot(*, batch_line_id: str, unit_cost_cents: int, received_at: str, quantity: Optional[int] = None) -> dict[str, Any]:
    if unit_cost_cents < 0:
        raise ValueError("unit_cost_cents must be non-negative")
    init_amazon_fba_db()
    with _connect() as connection:
        line = connection.execute("SELECT seller_sku, quantity FROM procurement_batch_lines WHERE id = ?", (batch_line_id,)).fetchone()
        if line is None:
            raise ValueError("procurement batch line not found")
        safe_quantity = int(quantity if quantity is not None else line["quantity"])
        if safe_quantity <= 0:
            raise ValueError("lot quantity must be positive")
        lot_id = str(uuid.uuid4())
        connection.execute(
            "INSERT INTO inventory_lots(id, batch_line_id, seller_sku, available_quantity, unit_cost_cents, received_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (lot_id, batch_line_id, str(line["seller_sku"] or ""), safe_quantity, int(unit_cost_cents), received_at, _utc_now()),
        )
        connection.commit()
    return {"id": lot_id, "batch_line_id": batch_line_id, "available_quantity": safe_quantity, "unit_cost_cents": int(unit_cost_cents)}


def allocate_order_fifo(order_id: str) -> dict[str, Any]:
    """Allocate an Amazon order's shipped SKUs to the oldest available lots once."""
    init_amazon_fba_db()
    allocations: list[dict[str, Any]] = []
    with _connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        order = connection.execute("SELECT amazon_order_id FROM amazon_orders WHERE amazon_order_id = ?", (order_id,)).fetchone()
        if order is None:
            raise ValueError("Amazon order not found")
        items = connection.execute("SELECT * FROM amazon_order_items WHERE amazon_order_id = ? ORDER BY id", (order_id,)).fetchall()
        for item in items:
            existing = connection.execute("SELECT COUNT(*) FROM fifo_allocations WHERE amazon_order_item_id = ?", (item["id"],)).fetchone()[0]
            if existing:
                continue
            remaining = int(item["quantity_shipped"] or item["quantity_ordered"] or 0)
            if remaining <= 0:
                continue
            lots = connection.execute(
                "SELECT * FROM inventory_lots WHERE seller_sku = ? AND available_quantity > 0 ORDER BY received_at, created_at, id",
                (str(item["seller_sku"] or ""),),
            ).fetchall()
            for lot in lots:
                if remaining <= 0:
                    break
                allocated = min(remaining, int(lot["available_quantity"]))
                remainder = min(allocated, int(lot["cost_remainder_cents"] or 0))
                allocated_cost_cents = allocated * int(lot["unit_cost_cents"]) + remainder
                allocation_id = str(uuid.uuid4())
                connection.execute(
                    "INSERT INTO fifo_allocations(id, amazon_order_id, amazon_order_item_id, inventory_lot_id, quantity, unit_cost_cents, allocated_cost_cents, allocated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (allocation_id, order_id, item["id"], lot["id"], allocated, int(lot["unit_cost_cents"]), allocated_cost_cents, _utc_now()),
                )
                connection.execute(
                    "UPDATE inventory_lots SET available_quantity = available_quantity - ?, cost_remainder_cents = cost_remainder_cents - ? WHERE id = ?",
                    (allocated, remainder, lot["id"]),
                )
                allocations.append({"id": allocation_id, "order_item_id": item["id"], "inventory_lot_id": lot["id"], "quantity": allocated, "unit_cost_cents": int(lot["unit_cost_cents"]), "allocated_cost_cents": allocated_cost_cents})
                remaining -= allocated
            if remaining:
                raise ValueError(f"insufficient FIFO inventory for SKU {item['seller_sku'] or '(missing SKU)'}")
        connection.commit()
    return {"order_id": order_id, "allocations": allocations, "allocated_cogs_cents": sum(item["allocated_cost_cents"] for item in allocations)}
