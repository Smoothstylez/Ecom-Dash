from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from app.config import (
    COMBINED_DB_PATH,
    INVOICES_DIR,
    KAUFLAND_DB_PATH,
    PROJECT_ROOT,
    SALES_INVOICES_DIR,
    SHOPIFY_DB_PATH,
    ensure_runtime_dirs,
)
from app.services.order_summaries import (
    first_non_empty,
    kaufland_summary_from_row,
    shopify_summary_from_row,
)


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def connect_combined_db() -> sqlite3.Connection:
    ensure_runtime_dirs()
    COMBINED_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(COMBINED_DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def init_combined_db() -> None:
    with connect_combined_db() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS combined_orders (
                id TEXT PRIMARY KEY,
                marketplace TEXT NOT NULL,
                order_id TEXT NOT NULL,
                external_order_id TEXT NOT NULL,
                order_date TEXT,
                customer TEXT,
                article TEXT,
                line_items_count INTEGER NOT NULL DEFAULT 1,
                total_cents INTEGER NOT NULL DEFAULT 0,
                fees_cents INTEGER NOT NULL DEFAULT 0,
                after_fees_cents INTEGER NOT NULL DEFAULT 0,
                shipping_cents INTEGER NOT NULL DEFAULT 0,
                currency TEXT NOT NULL DEFAULT 'EUR',
                fulfillment_status TEXT,
                payment_method TEXT,
                fee_source TEXT,
                financial_status TEXT,
                raw_status TEXT,
                raw_json TEXT,
                sales_gross_cents INTEGER NOT NULL DEFAULT 0,
                sales_net_cents INTEGER NOT NULL DEFAULT 0,
                sales_vat_cents INTEGER NOT NULL DEFAULT 0,
                purchase_cost_cents INTEGER NOT NULL DEFAULT 0,
                purchase_vat_cents INTEGER NOT NULL DEFAULT 0,
                purchase_is_vat_deductible INTEGER NOT NULL DEFAULT 0,
                purchase_currency TEXT NOT NULL DEFAULT 'EUR',
                purchase_supplier TEXT,
                purchase_notes TEXT,
                profit_cents INTEGER NOT NULL DEFAULT 0,
                has_invoice INTEGER NOT NULL DEFAULT 0,
                invoice_document_id TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_combined_orders_date
                ON combined_orders(order_date DESC);
            CREATE INDEX IF NOT EXISTS idx_combined_orders_marketplace
                ON combined_orders(marketplace);
            CREATE INDEX IF NOT EXISTS idx_combined_orders_status
                ON combined_orders(fulfillment_status);
            CREATE INDEX IF NOT EXISTS idx_combined_orders_customer
                ON combined_orders(customer);
            CREATE INDEX IF NOT EXISTS idx_combined_orders_article
                ON combined_orders(article);
            CREATE INDEX IF NOT EXISTS idx_combined_orders_financial
                ON combined_orders(financial_status);
            CREATE INDEX IF NOT EXISTS idx_combined_orders_payment
                ON combined_orders(payment_method);

            CREATE TABLE IF NOT EXISTS order_purchase_documents (
                id TEXT PRIMARY KEY,
                marketplace TEXT NOT NULL,
                order_id TEXT NOT NULL,
                original_filename TEXT NOT NULL,
                stored_filename TEXT NOT NULL,
                file_path TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                uploaded_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS order_enrichments (
                marketplace TEXT NOT NULL,
                order_id TEXT NOT NULL,
                purchase_cost_cents INTEGER,
                purchase_vat_cents INTEGER NOT NULL DEFAULT 0,
                purchase_is_vat_deductible INTEGER NOT NULL DEFAULT 0,
                purchase_currency TEXT NOT NULL DEFAULT 'EUR',
                supplier_name TEXT,
                purchase_notes TEXT,
                invoice_document_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (marketplace, order_id),
                FOREIGN KEY (invoice_document_id) REFERENCES order_purchase_documents(id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_order_enrichments_invoice_doc
                ON order_enrichments(invoice_document_id);
            CREATE INDEX IF NOT EXISTS idx_order_purchase_documents_market_order
                ON order_purchase_documents(marketplace, order_id);

            CREATE TABLE IF NOT EXISTS aliexpress_order_mappings (
                id TEXT PRIMARY KEY,
                marketplace TEXT NOT NULL,
                order_id TEXT NOT NULL,
                aliexpress_order_id TEXT NOT NULL,
                aliexpress_account_id TEXT NOT NULL DEFAULT '',
                aliexpress_account_order_id TEXT NOT NULL DEFAULT '',
                match_status TEXT NOT NULL DEFAULT 'matched',
                match_confidence REAL,
                match_method TEXT,
                source TEXT NOT NULL DEFAULT 'manual',
                note TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(marketplace, order_id, aliexpress_order_id)
            );

            CREATE INDEX IF NOT EXISTS idx_aliexpress_order_mappings_market_order
                ON aliexpress_order_mappings(marketplace, order_id);
            CREATE INDEX IF NOT EXISTS idx_aliexpress_order_mappings_ae_order
                ON aliexpress_order_mappings(aliexpress_order_id);
            CREATE INDEX IF NOT EXISTS idx_aliexpress_order_mappings_ae_account_order
                ON aliexpress_order_mappings(aliexpress_account_id, aliexpress_account_order_id);

            CREATE TABLE IF NOT EXISTS google_ads_import_batches (
                id TEXT PRIMARY KEY,
                import_kind TEXT NOT NULL,
                source_filename TEXT,
                imported_at TEXT NOT NULL,
                meta_json TEXT
            );

            CREATE TABLE IF NOT EXISTS google_ads_daily_costs (
                article_id TEXT NOT NULL,
                day TEXT NOT NULL,
                cost_cents INTEGER NOT NULL,
                currency TEXT NOT NULL DEFAULT 'EUR',
                updated_at TEXT NOT NULL,
                PRIMARY KEY (article_id, day)
            );

            CREATE TABLE IF NOT EXISTS google_ads_product_assignments (
                article_id TEXT PRIMARY KEY,
                product_title TEXT NOT NULL,
                product_key TEXT NOT NULL,
                product_label TEXT NOT NULL,
                product_detail TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS seller_profiles (
                id TEXT PRIMARY KEY,
                legal_name TEXT NOT NULL DEFAULT '',
                street TEXT NOT NULL DEFAULT '',
                address_line2 TEXT NOT NULL DEFAULT '',
                postcode TEXT NOT NULL DEFAULT '',
                city TEXT NOT NULL DEFAULT '',
                country TEXT NOT NULL DEFAULT 'DE',
                email TEXT NOT NULL DEFAULT '',
                phone TEXT NOT NULL DEFAULT '',
                vat_id TEXT NOT NULL DEFAULT '',
                tax_number TEXT NOT NULL DEFAULT '',
                tax_mode TEXT NOT NULL DEFAULT 'small_business',
                vat_effective_from TEXT NOT NULL DEFAULT '',
                invoice_prefix TEXT NOT NULL DEFAULT 'RE',
                default_template TEXT NOT NULL DEFAULT 'clean',
                footer_note TEXT NOT NULL DEFAULT '',
                payment_note TEXT NOT NULL DEFAULT '',
                eu_invoicing_enabled INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sales_invoices (
                id TEXT PRIMARY KEY,
                marketplace TEXT NOT NULL,
                source_order_id TEXT NOT NULL,
                source_external_order_id TEXT NOT NULL,
                invoice_number TEXT NOT NULL UNIQUE,
                invoice_date TEXT NOT NULL,
                delivery_date TEXT NOT NULL,
                currency TEXT NOT NULL DEFAULT 'EUR',
                customer_name TEXT NOT NULL,
                customer_country TEXT NOT NULL DEFAULT '',
                tax_country TEXT NOT NULL DEFAULT '',
                tax_treatment TEXT NOT NULL DEFAULT 'small_business',
                template_key TEXT NOT NULL DEFAULT 'clean',
                total_gross_cents INTEGER NOT NULL DEFAULT 0,
                seller_snapshot_json TEXT NOT NULL,
                customer_snapshot_json TEXT NOT NULL,
                totals_snapshot_json TEXT NOT NULL,
                validation_snapshot_json TEXT NOT NULL,
                notes TEXT NOT NULL DEFAULT '',
                pdf_path TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(marketplace, source_order_id)
            );

            CREATE TABLE IF NOT EXISTS sales_invoice_items (
                id TEXT PRIMARY KEY,
                invoice_id TEXT NOT NULL,
                position INTEGER NOT NULL,
                sku TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 1,
                unit_price_gross_cents INTEGER NOT NULL DEFAULT 0,
                line_total_gross_cents INTEGER NOT NULL DEFAULT 0,
                vat_rate REAL,
                meta_json TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY (invoice_id) REFERENCES sales_invoices(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_google_ads_import_batches_kind_time
                ON google_ads_import_batches(import_kind, imported_at DESC);
            CREATE INDEX IF NOT EXISTS idx_google_ads_daily_costs_day
                ON google_ads_daily_costs(day);
            CREATE INDEX IF NOT EXISTS idx_google_ads_daily_costs_article
                ON google_ads_daily_costs(article_id);
            CREATE INDEX IF NOT EXISTS idx_google_ads_assignments_product_key
                ON google_ads_product_assignments(product_key);
            CREATE INDEX IF NOT EXISTS idx_sales_invoices_created_at
                ON sales_invoices(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_sales_invoices_market_order
                ON sales_invoices(marketplace, source_order_id);
            CREATE INDEX IF NOT EXISTS idx_sales_invoice_items_invoice_id
                ON sales_invoice_items(invoice_id, position);
            """
        )
        _ensure_combined_orders_columns(connection)
        connection.commit()

    _migrate_invoice_paths_to_relative()


def _table_columns(connection: sqlite3.Connection, table_name: str) -> set[str]:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1] if not isinstance(row, sqlite3.Row) else row["name"]) for row in rows}


def _ensure_column(connection: sqlite3.Connection, table_name: str, column_name: str, column_sql: str) -> None:
    if column_name in _table_columns(connection, table_name):
        return
    connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")


def _ensure_combined_orders_columns(connection: sqlite3.Connection) -> None:
    if "combined_orders" not in {
        str(row[0] if not isinstance(row, sqlite3.Row) else row["name"])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }:
        return
    _ensure_column(connection, "combined_orders", "purchase_cost_cents", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "combined_orders", "sales_gross_cents", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "combined_orders", "sales_net_cents", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "combined_orders", "sales_vat_cents", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "combined_orders", "purchase_vat_cents", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "combined_orders", "purchase_is_vat_deductible", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "combined_orders", "purchase_currency", "TEXT NOT NULL DEFAULT 'EUR'")
    _ensure_column(connection, "combined_orders", "purchase_supplier", "TEXT")
    _ensure_column(connection, "combined_orders", "purchase_notes", "TEXT")
    _ensure_column(connection, "combined_orders", "profit_cents", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "combined_orders", "has_invoice", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "combined_orders", "invoice_document_id", "TEXT")
    _ensure_column(connection, "order_enrichments", "purchase_vat_cents", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "order_enrichments", "purchase_is_vat_deductible", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "seller_profiles", "vat_effective_from", "TEXT NOT NULL DEFAULT ''")


def refresh_combined_order_row(*, marketplace: str, order_id: str) -> bool:
    rows_written = populate_combined_orders(marketplace=marketplace, order_id=order_id)
    return rows_written > 0


def _migrate_invoice_paths_to_relative() -> None:
    """One-time migration: convert absolute file_path entries to relative."""
    project_root = PROJECT_ROOT.resolve()
    with connect_combined_db() as connection:
        rows = connection.execute(
            "SELECT id, file_path FROM order_purchase_documents"
        ).fetchall()
        updated = 0
        for row in rows:
            fp = str(row["file_path"] or "")
            if not fp or not Path(fp).is_absolute():
                continue
            try:
                relative = Path(fp).resolve().relative_to(project_root).as_posix()
            except (ValueError, OSError):
                continue
            connection.execute(
                "UPDATE order_purchase_documents SET file_path = ? WHERE id = ?",
                (relative, row["id"]),
            )
            updated += 1
        if updated:
            connection.commit()


def _to_cents(value: Any) -> int:
    if value is None:
        return 0
    try:
        cents = round(float(value) * 100)
        return max(cents, 0)
    except (ValueError, TypeError):
        return 0


def _to_kaufland_cents(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return max(int(value), 0)
    if isinstance(value, float):
        if not (abs(value) < 1e308 and value == value):
            return 0
        rounded = round(value)
        if abs(value - rounded) < 1e-9:
            return max(int(rounded), 0)
        return max(int(round(value * 100)), 0)

    text = str(value).strip()
    if not text:
        return 0
    normalized = text.replace(",", ".")
    if "." in normalized:
        try:
            parsed = float(normalized)
        except ValueError:
            return 0
        rounded = round(parsed)
        if abs(parsed - rounded) < 1e-9:
            return max(int(rounded), 0)
        return max(int(round(parsed * 100)), 0)
    try:
        return max(int(normalized), 0)
    except ValueError:
        return 0


def _to_iso_utc(value: Any) -> str:
    text = str(value or "").strip().strip("Z")
    if not text:
        return ""
    text = text.replace(" ", "T")
    if "T" not in text:
        return text
    return text


def populate_combined_orders(*, marketplace: str | None = None, order_id: str | None = None) -> int:
    enrichment_map = fetch_enrichment_map()
    orders: list[dict[str, Any]] = []
    market_filter = str(marketplace or "").strip().lower() or None
    order_filter = str(order_id or "").strip() or None

    if SHOPIFY_DB_PATH.exists():
        with sqlite3.connect(str(SHOPIFY_DB_PATH)) as src:
            src.row_factory = sqlite3.Row
            rows = src.execute(
                """
                SELECT
                    o.id, o.name, o.created_at, o.payment_method,
                    o.financial_status, o.fulfillment_status,
                    o.total_price, o.total_tax, o.currency,
                    o.customer_first_name, o.customer_last_name,
                    o.customer_email, o.email,
                    o.estimated_paypal_fee, o.estimated_net_after_fee,
                    o.raw_json,
                    (
                        SELECT li.title FROM order_line_items li
                        WHERE li.order_id = o.id ORDER BY li.id ASC LIMIT 1
                    ) AS first_article,
                    (
                        SELECT COUNT(*) FROM order_line_items li2
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

        for row in rows:
            order = shopify_summary_from_row(row)
            order["raw_json"] = str(row["raw_json"] or "")
            if market_filter and order["marketplace"] != market_filter:
                continue
            if order_filter and order["order_id"] != order_filter:
                continue
            orders.append(order)

    if KAUFLAND_DB_PATH.exists():
        with sqlite3.connect(str(KAUFLAND_DB_PATH)) as src:
            src.row_factory = sqlite3.Row
            rows = src.execute(
                """
                SELECT
                    o.id_order, o.ts_created_iso, o.raw_json,
                    (
                        SELECT COALESCE(SUM(CAST(COALESCE(NULLIF(ou.price, ''), 0) AS REAL)), 0)
                        FROM order_units ou
                        WHERE ou.id_order = o.id_order
                          AND COALESCE(ou.status, '') NOT IN ('cancelled', 'canceled')
                    ) AS units_price_sum,
                    (
                        SELECT COALESCE(SUM(
                            CAST(COALESCE(NULLIF(ou.price, ''), 0) AS REAL)
                            * COALESCE(ou.vat, 0)
                            / (100 + COALESCE(ou.vat, 0))
                        ), 0)
                        FROM order_units ou
                        WHERE ou.id_order = o.id_order
                          AND COALESCE(ou.status, '') NOT IN ('cancelled', 'canceled')
                          AND COALESCE(ou.vat, 0) > 0
                    ) AS units_vat_sum,
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
                        SELECT COALESCE(SUM(
                            CAST(COALESCE(NULLIF(ou.shipping_rate, ''), 0) AS REAL)
                            * COALESCE(ou.vat, 0)
                            / (100 + COALESCE(ou.vat, 0))
                        ), 0)
                        FROM order_units ou
                        WHERE ou.id_order = o.id_order
                          AND COALESCE(ou.status, '') NOT IN ('cancelled', 'canceled')
                          AND COALESCE(ou.vat, 0) > 0
                    ) AS shipping_vat_sum,
                    (
                        SELECT COALESCE(SUM(CAST(COALESCE(NULLIF(ref.amount, ''), '0') AS REAL)), 0)
                        FROM order_unit_refunds ref
                        JOIN order_units ou2 ON ref.id_order_unit = ou2.id_order_unit
                        WHERE ou2.id_order = o.id_order
                          AND COALESCE(ou2.status, '') NOT IN ('cancelled', 'canceled')
                    ) AS refund_amount_sum,
                    (
                        SELECT ou.product_title FROM order_units ou
                        WHERE ou.id_order = o.id_order
                        ORDER BY COALESCE(ou.ts_created_iso, '') ASC, ou.id_order_unit ASC
                        LIMIT 1
                    ) AS first_article,
                    (
                        SELECT COUNT(*) FROM order_units ou2
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

        for row in rows:
            order = kaufland_summary_from_row(row)
            order["raw_json"] = str(row["raw_json"] or "")
            if market_filter and order["marketplace"] != market_filter:
                continue
            if order_filter and order["order_id"] != order_filter:
                continue
            orders.append(order)

    if market_filter and order_filter:
        with connect_combined_db() as connection:
            connection.execute(
                "DELETE FROM combined_orders WHERE marketplace = ? AND order_id = ?",
                (market_filter, order_filter),
            )
            connection.commit()

    if not orders:
        return 0

    rows_written = 0
    with connect_combined_db() as connection:
        for order in orders:
            key = (order["marketplace"], order["order_id"])
            enrichment = enrichment_map.get(key, {})

            purchase_cost = enrichment.get("purchase_cost_cents")
            purchase_cost_cents = int(purchase_cost) if isinstance(purchase_cost, int) else 0
            purchase_vat = enrichment.get("purchase_vat_cents")
            purchase_vat_cents = int(purchase_vat) if isinstance(purchase_vat, int) else 0
            purchase_is_vat_deductible = 1 if bool(enrichment.get("purchase_is_vat_deductible")) else 0
            profit_cents = int(order["after_fees_cents"]) - purchase_cost_cents

            order_id = f"{order['marketplace']}:{order['order_id']}"
            connection.execute(
                """
                INSERT OR REPLACE INTO combined_orders (
                    id, marketplace, order_id, external_order_id, order_date,
                    customer, article, line_items_count,
                    total_cents, fees_cents, after_fees_cents, shipping_cents,
                    currency, fulfillment_status, payment_method, fee_source,
                    financial_status, raw_status, raw_json,
                    sales_gross_cents, sales_net_cents, sales_vat_cents,
                    purchase_cost_cents, purchase_vat_cents, purchase_is_vat_deductible,
                    purchase_currency, purchase_supplier,
                    purchase_notes, profit_cents, has_invoice, invoice_document_id
                ) VALUES (
                    ?, ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?
                )
                """,
                (
                    order_id,
                    order["marketplace"],
                    order["order_id"],
                    order["external_order_id"],
                    order["order_date"],
                    order["customer"],
                    order["article"],
                    order["line_items_count"],
                    order["total_cents"],
                    order["fees_cents"],
                    order["after_fees_cents"],
                    order["shipping_cents"],
                    order["currency"],
                    order["fulfillment_status"],
                    order["payment_method"],
                    order["fee_source"],
                    order["financial_status"],
                    order["raw_status"],
                    order["raw_json"],
                    int(order.get("sales_gross_cents") or 0),
                    int(order.get("sales_net_cents") or 0),
                    int(order.get("sales_vat_cents") or 0),
                    purchase_cost_cents,
                    purchase_vat_cents,
                    purchase_is_vat_deductible,
                    enrichment.get("purchase_currency") or "EUR",
                    enrichment.get("supplier_name"),
                    enrichment.get("purchase_notes"),
                    profit_cents,
                    1 if enrichment.get("invoice_document_id") else 0,
                    enrichment.get("invoice_document_id"),
                ),
            )
            rows_written += 1
        connection.commit()

    return rows_written


def _normalize_currency(value: Optional[str]) -> str:
    if not value:
        return "EUR"
    currency = str(value).strip().upper()
    if len(currency) != 3:
        return "EUR"
    return currency


def _split_aliexpress_order_id(value: Optional[str]) -> tuple[str, str]:
    token = str(value or "").strip()
    if not token:
        return ("", "")
    if ":" not in token:
        return ("", token)
    account_id, _, account_order_id = token.partition(":")
    return (account_id.strip(), account_order_id.strip())


def _normalize_mapping_status(value: Optional[str]) -> str:
    token = str(value or "").strip().lower()
    return token or "matched"


def _normalize_mapping_source(value: Optional[str]) -> str:
    token = str(value or "").strip().lower()
    return token or "manual"


def _normalize_mapping_confidence(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("match_confidence must be a number between 0 and 1") from exc
    if parsed < 0 or parsed > 1:
        raise ValueError("match_confidence must be between 0 and 1")
    return round(parsed, 4)


def _resolve_enrichment_order_id(marketplace: str, external_order_id: str) -> str:
    """Resolve an external_order_id back to the raw order_id used in enrichments.

    For Kaufland, these are identical. For Shopify, the external_order_id is
    ``#1148`` while the enrichment key is the numeric Shopify order id
    (e.g. ``7469627212115``).
    """
    if marketplace != "shopify" or not external_order_id.startswith("#"):
        return external_order_id

    conn = None
    try:
        conn = sqlite3.connect(SHOPIFY_DB_PATH)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT id FROM orders WHERE name = ?", (external_order_id,)
        ).fetchone()
        if row:
            return str(row["id"])
    except Exception:
        pass
    finally:
        if conn is not None:
            conn.close()
    return external_order_id


def clear_purchase_enrichment(
    *,
    marketplace: str,
    order_id: str,
) -> bool:
    """Reset purchase_cost_cents to 0 and unlink invoice document for a given order.

    *order_id* may be either the raw source id or an external_order_id
    (e.g. ``#1148`` for Shopify). The function resolves it automatically.

    Returns True if a row was updated, False otherwise.
    """
    resolved_id = _resolve_enrichment_order_id(marketplace, order_id)
    timestamp = now_iso()
    with connect_combined_db() as connection:
        cursor = connection.execute(
            """
            UPDATE order_enrichments
            SET purchase_cost_cents = 0,
                purchase_vat_cents = 0,
                purchase_is_vat_deductible = 0,
                invoice_document_id = NULL,
                updated_at = ?
            WHERE marketplace = ? AND order_id = ?
            """,
            (timestamp, marketplace, resolved_id),
        )
        connection.commit()
        return cursor.rowcount > 0


def upsert_purchase_enrichment(
    *,
    marketplace: str,
    order_id: str,
    purchase_cost_cents: Optional[int],
    purchase_vat_cents: Optional[int] = None,
    purchase_is_vat_deductible: Optional[bool] = None,
    purchase_currency: Optional[str] = None,
    supplier_name: Optional[str] = None,
    purchase_notes: Optional[str] = None,
) -> dict[str, Any]:
    timestamp = now_iso()
    with connect_combined_db() as connection:
        existing = connection.execute(
            """
            SELECT marketplace, order_id, invoice_document_id, created_at
            FROM order_enrichments
            WHERE marketplace = ? AND order_id = ?
            """,
            (marketplace, order_id),
        ).fetchone()

        invoice_document_id = existing["invoice_document_id"] if existing is not None else None
        created_at = existing["created_at"] if existing is not None else timestamp

        connection.execute(
            """
            INSERT INTO order_enrichments (
                marketplace, order_id, purchase_cost_cents, purchase_vat_cents,
                purchase_is_vat_deductible, purchase_currency,
                supplier_name, purchase_notes, invoice_document_id, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(marketplace, order_id) DO UPDATE SET
                purchase_cost_cents = excluded.purchase_cost_cents,
                purchase_vat_cents = excluded.purchase_vat_cents,
                purchase_is_vat_deductible = excluded.purchase_is_vat_deductible,
                purchase_currency = excluded.purchase_currency,
                supplier_name = excluded.supplier_name,
                purchase_notes = excluded.purchase_notes,
                updated_at = excluded.updated_at
            """,
            (
                marketplace,
                order_id,
                purchase_cost_cents,
                max(int(purchase_vat_cents or 0), 0),
                1 if bool(purchase_is_vat_deductible) else 0,
                _normalize_currency(purchase_currency),
                supplier_name,
                purchase_notes,
                invoice_document_id,
                created_at,
                timestamp,
            ),
        )
        connection.commit()

        row = connection.execute(
            """
            SELECT e.*, d.original_filename, d.stored_filename, d.file_path, d.mime_type, d.uploaded_at
            FROM order_enrichments e
            LEFT JOIN order_purchase_documents d ON d.id = e.invoice_document_id
            WHERE e.marketplace = ? AND e.order_id = ?
            """,
            (marketplace, order_id),
        ).fetchone()
    return dict(row) if row is not None else {}


def create_invoice_document(
    *,
    marketplace: str,
    order_id: str,
    original_filename: str,
    stored_filename: str,
    mime_type: str,
    file_path: Path,
) -> dict[str, Any]:
    timestamp = now_iso()
    document_id = str(uuid.uuid4())

    with connect_combined_db() as connection:
        connection.execute(
            """
            INSERT INTO order_purchase_documents (
                id, marketplace, order_id, original_filename, stored_filename,
                file_path, mime_type, uploaded_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                document_id,
                marketplace,
                order_id,
                original_filename,
                stored_filename,
                _to_relative_path(file_path),
                mime_type,
                timestamp,
            ),
        )

        existing = connection.execute(
            """
            SELECT created_at
            FROM order_enrichments
            WHERE marketplace = ? AND order_id = ?
            """,
            (marketplace, order_id),
        ).fetchone()
        created_at = existing["created_at"] if existing is not None else timestamp

        connection.execute(
            """
            INSERT INTO order_enrichments (
                marketplace, order_id, purchase_cost_cents, purchase_vat_cents,
                purchase_is_vat_deductible, purchase_currency,
                supplier_name, purchase_notes, invoice_document_id, created_at, updated_at
            )
            VALUES (?, ?, NULL, 0, 0, 'EUR', NULL, NULL, ?, ?, ?)
            ON CONFLICT(marketplace, order_id) DO UPDATE SET
                invoice_document_id = excluded.invoice_document_id,
                updated_at = excluded.updated_at
            """,
            (marketplace, order_id, document_id, created_at, timestamp),
        )
        connection.commit()

        row = connection.execute(
            """
            SELECT e.*, d.original_filename, d.stored_filename, d.file_path, d.mime_type, d.uploaded_at
            FROM order_enrichments e
            LEFT JOIN order_purchase_documents d ON d.id = e.invoice_document_id
            WHERE e.marketplace = ? AND e.order_id = ?
            """,
            (marketplace, order_id),
        ).fetchone()
    return dict(row) if row is not None else {}


def fetch_enrichment_map() -> dict[tuple[str, str], dict[str, Any]]:
    with connect_combined_db() as connection:
        rows = connection.execute(
            """
            SELECT
                e.marketplace,
                e.order_id,
                e.purchase_cost_cents,
                e.purchase_vat_cents,
                e.purchase_is_vat_deductible,
                e.purchase_currency,
                e.supplier_name,
                e.purchase_notes,
                e.invoice_document_id,
                e.created_at,
                e.updated_at,
                d.original_filename,
                d.stored_filename,
                d.file_path,
                d.mime_type,
                d.uploaded_at
            FROM order_enrichments e
            LEFT JOIN order_purchase_documents d
                ON d.id = e.invoice_document_id
            """
        ).fetchall()
    payload: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        record = dict(row)
        payload[(record["marketplace"], record["order_id"])] = record
    return payload


def fetch_aliexpress_order_mappings() -> list[dict[str, Any]]:
    with connect_combined_db() as connection:
        rows = connection.execute(
            """
            SELECT
                id,
                marketplace,
                order_id,
                aliexpress_order_id,
                aliexpress_account_id,
                aliexpress_account_order_id,
                match_status,
                match_confidence,
                match_method,
                source,
                note,
                created_at,
                updated_at
            FROM aliexpress_order_mappings
            ORDER BY marketplace ASC, order_id ASC, created_at ASC, aliexpress_order_id ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def fetch_aliexpress_order_mappings_for_marketplace_order(
    *,
    marketplace: str,
    order_id: str,
) -> list[dict[str, Any]]:
    with connect_combined_db() as connection:
        rows = connection.execute(
            """
            SELECT
                id,
                marketplace,
                order_id,
                aliexpress_order_id,
                aliexpress_account_id,
                aliexpress_account_order_id,
                match_status,
                match_confidence,
                match_method,
                source,
                note,
                created_at,
                updated_at
            FROM aliexpress_order_mappings
            WHERE marketplace = ? AND order_id = ?
            ORDER BY created_at ASC, aliexpress_order_id ASC
            """,
            (marketplace, order_id),
        ).fetchall()
    return [dict(row) for row in rows]


def replace_aliexpress_order_mappings(
    *,
    marketplace: str,
    order_id: str,
    mappings: list[dict[str, Any]],
    default_source: Optional[str] = None,
) -> list[dict[str, Any]]:
    timestamp = now_iso()
    normalized_default_source = _normalize_mapping_source(default_source)
    normalized_items: list[dict[str, Any]] = []
    seen_order_ids: set[str] = set()

    for item in mappings:
        aliexpress_order_id = str(item.get("aliexpress_order_id") or "").strip()
        if not aliexpress_order_id:
            raise ValueError("aliexpress_order_id is required")
        if aliexpress_order_id in seen_order_ids:
            raise ValueError(f"duplicate aliexpress_order_id: {aliexpress_order_id}")
        seen_order_ids.add(aliexpress_order_id)

        account_id, account_order_id = _split_aliexpress_order_id(aliexpress_order_id)
        normalized_items.append(
            {
                "aliexpress_order_id": aliexpress_order_id,
                "aliexpress_account_id": account_id,
                "aliexpress_account_order_id": account_order_id,
                "match_status": _normalize_mapping_status(item.get("match_status")),
                "match_confidence": _normalize_mapping_confidence(item.get("match_confidence")),
                "match_method": str(item.get("match_method") or "").strip() or None,
                "source": _normalize_mapping_source(item.get("source") or normalized_default_source),
                "note": str(item.get("note") or "").strip() or None,
            }
        )

    with connect_combined_db() as connection:
        existing_rows = connection.execute(
            """
            SELECT id, aliexpress_order_id, created_at
            FROM aliexpress_order_mappings
            WHERE marketplace = ? AND order_id = ?
            """,
            (marketplace, order_id),
        ).fetchall()
        existing_by_ae_id = {
            str(row["aliexpress_order_id"]): {
                "id": str(row["id"]),
                "created_at": str(row["created_at"]),
            }
            for row in existing_rows
        }

        connection.execute(
            "DELETE FROM aliexpress_order_mappings WHERE marketplace = ? AND order_id = ?",
            (marketplace, order_id),
        )

        for item in normalized_items:
            existing = existing_by_ae_id.get(item["aliexpress_order_id"], {})
            connection.execute(
                """
                INSERT INTO aliexpress_order_mappings (
                    id,
                    marketplace,
                    order_id,
                    aliexpress_order_id,
                    aliexpress_account_id,
                    aliexpress_account_order_id,
                    match_status,
                    match_confidence,
                    match_method,
                    source,
                    note,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    existing.get("id") or str(uuid.uuid4()),
                    marketplace,
                    order_id,
                    item["aliexpress_order_id"],
                    item["aliexpress_account_id"],
                    item["aliexpress_account_order_id"],
                    item["match_status"],
                    item["match_confidence"],
                    item["match_method"],
                    item["source"],
                    item["note"],
                    existing.get("created_at") or timestamp,
                    timestamp,
                ),
            )
        connection.commit()

    return fetch_aliexpress_order_mappings_for_marketplace_order(
        marketplace=marketplace,
        order_id=order_id,
    )


def fetch_invoice_document(document_id: str) -> Optional[dict[str, Any]]:
    with connect_combined_db() as connection:
        row = connection.execute(
            """
            SELECT *
            FROM order_purchase_documents
            WHERE id = ?
            LIMIT 1
            """,
            (document_id,),
        ).fetchone()
    return dict(row) if row is not None else None


def sanitize_filename(value: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "-" for ch in value.strip())
    while "--" in safe:
        safe = safe.replace("--", "-")
    safe = safe.strip("-._")
    return safe or "invoice"


def build_invoice_storage_path(marketplace: str, order_id: str, original_filename: str) -> Path:
    ext = Path(original_filename).suffix.lower() or ".bin"
    token_market = sanitize_filename(marketplace.lower())
    token_order = sanitize_filename(order_id.lower())[:48]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    unique = uuid.uuid4().hex[:8]
    stored_name = f"{timestamp}_{token_market}_{token_order}_{unique}{ext}"
    return INVOICES_DIR / stored_name


def build_sales_invoice_storage_path(invoice_number: str) -> Path:
    token_invoice = sanitize_filename(invoice_number.lower())[:64]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    unique = uuid.uuid4().hex[:8]
    stored_name = f"{timestamp}_{token_invoice}_{unique}.pdf"
    return SALES_INVOICES_DIR / stored_name


def _to_relative_path(absolute_path: Path) -> str:
    """Convert an absolute path to a PROJECT_ROOT-relative POSIX path.

    If the path is already relative or cannot be made relative, return it
    as-is with forward slashes.
    """
    try:
        return absolute_path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except (ValueError, OSError):
        return str(absolute_path)


def resolve_invoice_path(raw_path: str) -> Optional[Path]:
    """Resolve a stored invoice path to an absolute filesystem path.

    Handles both relative (``storage/invoices/...``) and legacy absolute
    paths (Windows, Mac, Linux).  Returns *None* when no matching file
    can be located.
    """
    text = str(raw_path or "").strip()
    if not text:
        return None

    candidate = Path(text)

    # Already absolute — check directly
    if candidate.is_absolute():
        if candidate.exists() and candidate.is_file():
            return candidate
        # Attempt to recover by using just the filename under INVOICES_DIR
        fallback = INVOICES_DIR / candidate.name
        if fallback.exists() and fallback.is_file():
            return fallback
        return None

    # Relative — resolve against PROJECT_ROOT
    resolved = (PROJECT_ROOT / candidate).resolve()
    if resolved.exists() and resolved.is_file():
        return resolved

    # Last resort: bare filename in INVOICES_DIR
    fallback = INVOICES_DIR / Path(text).name
    if fallback.exists() and fallback.is_file():
        return fallback

    return None
