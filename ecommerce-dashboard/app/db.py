from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from app.config import COMBINED_DB_PATH, INVOICES_DIR, ensure_runtime_dirs


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

            CREATE INDEX IF NOT EXISTS idx_google_ads_import_batches_kind_time
                ON google_ads_import_batches(import_kind, imported_at DESC);
            CREATE INDEX IF NOT EXISTS idx_google_ads_daily_costs_day
                ON google_ads_daily_costs(day);
            CREATE INDEX IF NOT EXISTS idx_google_ads_daily_costs_article
                ON google_ads_daily_costs(article_id);
            CREATE INDEX IF NOT EXISTS idx_google_ads_assignments_product_key
                ON google_ads_product_assignments(product_key);
            """
        )
        connection.commit()


def _normalize_currency(value: Optional[str]) -> str:
    if not value:
        return "EUR"
    currency = str(value).strip().upper()
    if len(currency) != 3:
        return "EUR"
    return currency


def upsert_purchase_enrichment(
    *,
    marketplace: str,
    order_id: str,
    purchase_cost_cents: Optional[int],
    purchase_currency: Optional[str],
    supplier_name: Optional[str],
    purchase_notes: Optional[str],
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
                marketplace, order_id, purchase_cost_cents, purchase_currency,
                supplier_name, purchase_notes, invoice_document_id, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(marketplace, order_id) DO UPDATE SET
                purchase_cost_cents = excluded.purchase_cost_cents,
                purchase_currency = excluded.purchase_currency,
                supplier_name = excluded.supplier_name,
                purchase_notes = excluded.purchase_notes,
                updated_at = excluded.updated_at
            """,
            (
                marketplace,
                order_id,
                purchase_cost_cents,
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
                str(file_path),
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
                marketplace, order_id, purchase_cost_cents, purchase_currency,
                supplier_name, purchase_notes, invoice_document_id, created_at, updated_at
            )
            VALUES (?, ?, NULL, 'EUR', NULL, NULL, ?, ?, ?)
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
