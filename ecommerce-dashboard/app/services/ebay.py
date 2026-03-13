"""eBay legacy data service — read-only access to imported eBay order data."""

from __future__ import annotations

import sqlite3
from typing import Any, Optional

from app.config import EBAY_DB_PATH


def _connect() -> Optional[sqlite3.Connection]:
    if not EBAY_DB_PATH.exists():
        return None
    conn = sqlite3.connect(EBAY_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_ebay_orders(
    *,
    shop: Optional[str] = None,
    category: Optional[str] = None,
    include_returns: bool = True,
) -> list[dict[str, Any]]:
    """Return all eBay orders, optionally filtered by shop/category."""
    conn = _connect()
    if conn is None:
        return []

    clauses: list[str] = []
    params: list[Any] = []

    if shop:
        clauses.append("shop = ?")
        params.append(shop)
    if category:
        clauses.append("category = ?")
        params.append(category)
    if not include_returns:
        clauses.append("is_return = 0")

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    rows = conn.execute(
        f"""
        SELECT id, shop, category, artikel, kunde_name, order_number,
               datum, preis, gebuehren, nach_gebuehren, ali_preis,
               provision_rate, gewinn, is_return
        FROM ebay_orders
        {where}
        ORDER BY datum ASC, id ASC
        """,
        params,
    ).fetchall()

    conn.close()
    return [dict(r) for r in rows]


def get_ebay_summary() -> dict[str, Any]:
    """Build KPI summary for the eBay tab."""
    conn = _connect()
    if conn is None:
        return {"available": False}

    # Overall totals (exclude returns from revenue/profit KPIs)
    totals = conn.execute(
        """
        SELECT
            COUNT(*)                          AS total_count,
            COALESCE(SUM(preis), 0)           AS total_revenue,
            COALESCE(SUM(gebuehren), 0)       AS total_fees,
            COALESCE(SUM(ali_preis), 0)       AS total_purchase,
            COALESCE(SUM(gewinn), 0)          AS total_profit,
            COALESCE(SUM(nach_gebuehren), 0)  AS total_after_fees,
            MIN(datum)                        AS first_date,
            MAX(datum)                        AS last_date
        FROM ebay_orders
        WHERE is_return = 0
        """
    ).fetchone()

    returns_row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM ebay_orders WHERE is_return = 1"
    ).fetchone()

    # Per-shop breakdown
    shop_rows = conn.execute(
        """
        SELECT
            shop,
            COUNT(*)                          AS count,
            COALESCE(SUM(preis), 0)           AS revenue,
            COALESCE(SUM(gebuehren), 0)       AS fees,
            COALESCE(SUM(ali_preis), 0)       AS purchase,
            COALESCE(SUM(gewinn), 0)          AS profit,
            MIN(datum)                        AS first_date,
            MAX(datum)                        AS last_date
        FROM ebay_orders
        WHERE is_return = 0
        GROUP BY shop
        ORDER BY shop
        """
    ).fetchall()

    # Top articles by revenue
    top_articles = conn.execute(
        """
        SELECT
            artikel,
            COUNT(*)                    AS count,
            COALESCE(SUM(preis), 0)     AS revenue,
            COALESCE(SUM(gewinn), 0)    AS profit
        FROM ebay_orders
        WHERE is_return = 0
        GROUP BY artikel
        ORDER BY revenue DESC
        LIMIT 15
        """
    ).fetchall()

    # Import metadata
    meta_row = conn.execute(
        "SELECT * FROM ebay_import_meta WHERE id = 1"
    ).fetchone()

    conn.close()

    total_revenue = totals["total_revenue"] if totals else 0
    total_profit = totals["total_profit"] if totals else 0
    margin_pct = (total_profit / total_revenue * 100) if total_revenue else 0

    return {
        "available": True,
        "kpis": {
            "total_orders": totals["total_count"] if totals else 0,
            "total_returns": returns_row["cnt"] if returns_row else 0,
            "total_revenue": round(total_revenue, 2),
            "total_fees": round(totals["total_fees"], 2) if totals else 0,
            "total_after_fees": round(totals["total_after_fees"], 2) if totals else 0,
            "total_purchase": round(totals["total_purchase"], 2) if totals else 0,
            "total_profit": round(total_profit, 2),
            "margin_pct": round(margin_pct, 1),
            "first_date": totals["first_date"] if totals else None,
            "last_date": totals["last_date"] if totals else None,
        },
        "shops": [
            {
                "shop": r["shop"],
                "count": r["count"],
                "revenue": round(r["revenue"], 2),
                "fees": round(r["fees"], 2),
                "purchase": round(r["purchase"], 2),
                "profit": round(r["profit"], 2),
                "first_date": r["first_date"],
                "last_date": r["last_date"],
            }
            for r in shop_rows
        ],
        "top_articles": [
            {
                "artikel": r["artikel"],
                "count": r["count"],
                "revenue": round(r["revenue"], 2),
                "profit": round(r["profit"], 2),
            }
            for r in top_articles
        ],
        "import_meta": dict(meta_row) if meta_row else None,
    }
