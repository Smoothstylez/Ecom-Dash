from __future__ import annotations

import mimetypes
import sqlite3
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from app.config import BOOKKEEPING_BOOTSTRAP_DOCUMENTS_DIR, BOOKKEEPING_DB_PATH, BOOKKEEPING_PROJECT_ROOT


def _connect_bookkeeping_db() -> sqlite3.Connection:
    from app.services.bookkeeping_full import _ensure_schema

    connection = sqlite3.connect(BOOKKEEPING_DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    if _ensure_schema(connection):
        connection.commit()
    return connection


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _build_filters(from_date: Optional[str], to_date: Optional[str], query: Optional[str]) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    from_dt = _parse_iso(from_date)
    to_dt = _parse_iso(to_date)
    if to_dt is not None:
        to_dt = to_dt.replace(hour=23, minute=59, second=59)

    if from_dt is not None:
        clauses.append("t.date >= ?")
        params.append(from_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z"))
    if to_dt is not None:
        clauses.append("t.date <= ?")
        params.append(to_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z"))

    if query and query.strip():
        needle = f"%{query.strip().lower()}%"
        clauses.append(
            "(" \
            "LOWER(COALESCE(t.provider, '')) LIKE ? "
            "OR LOWER(COALESCE(t.counterparty_name, '')) LIKE ? "
            "OR LOWER(COALESCE(t.reference, '')) LIKE ? "
            "OR LOWER(COALESCE(t.notes, '')) LIKE ? "
            "OR LOWER(COALESCE(t.type, '')) LIKE ?"
            ")"
        )
        params.extend([needle, needle, needle, needle, needle])

    if not clauses:
        return "", params
    return f"WHERE {' AND '.join(clauses)}", params


COMBINED_SYNC_NAMESPACE = uuid.UUID("eb7a72f6-1685-4225-9ee4-ebc4d54f9e7d")


def _bookkeeping_project_roots() -> list[Path]:
    roots: list[Path] = []
    bootstrap_project_root = BOOKKEEPING_BOOTSTRAP_DOCUMENTS_DIR.parent.parent
    for root in (BOOKKEEPING_PROJECT_ROOT, bootstrap_project_root):
        resolved = root.resolve()
        if resolved not in roots:
            roots.append(resolved)
    return roots


def _resolve_document_path(raw_path: str) -> Optional[Path]:
    text = str(raw_path or "").strip()
    if not text:
        return None

    candidate = Path(text)
    if candidate.is_absolute():
        try:
            return candidate.resolve()
        except OSError:
            return candidate

    fallback: Optional[Path] = None
    for project_root in _bookkeeping_project_roots():
        resolved = (project_root / candidate).resolve()
        try:
            resolved.relative_to(project_root)
        except ValueError:
            continue
        if resolved.exists():
            return resolved
        if fallback is None:
            fallback = resolved

    return fallback


def migrate_bookkeeping_document_paths() -> int:
    """Convert absolute file_path entries in the bookkeeping documents table
    to portable relative paths.  Returns the number of rows updated."""
    if not BOOKKEEPING_DB_PATH.exists():
        return 0
    roots = _bookkeeping_project_roots()
    with _connect_bookkeeping_db() as connection:
        rows = connection.execute("SELECT id, file_path FROM documents").fetchall()
        updated = 0
        for row in rows:
            fp = str(row["file_path"] or "")
            if not fp or not Path(fp).is_absolute():
                continue
            converted = False
            for project_root in roots:
                try:
                    relative = Path(fp).resolve().relative_to(project_root).as_posix()
                    connection.execute(
                        "UPDATE documents SET file_path = ? WHERE id = ?",
                        (relative, row["id"]),
                    )
                    updated += 1
                    converted = True
                    break
                except (ValueError, OSError):
                    continue
        if updated:
            connection.commit()
    return updated


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_iso(value: Optional[str], fallback: Optional[str] = None) -> str:
    parsed = _parse_iso(value)
    if parsed is None and fallback:
        parsed = _parse_iso(fallback)
    if parsed is None:
        return _now_iso()
    return parsed.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _shift_iso(value: str, *, minutes: int = 0) -> str:
    parsed = _parse_iso(value)
    if parsed is None:
        parsed = datetime.now(timezone.utc)
    shifted = parsed + timedelta(minutes=minutes)
    return shifted.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_currency(value: Any) -> str:
    text = str(value or "").strip().upper()
    if len(text) != 3:
        return "EUR"
    return text


def _stable_uuid(token: str) -> str:
    return str(uuid.uuid5(COMBINED_SYNC_NAMESPACE, token))


def _upsert_synced_order(
    connection: sqlite3.Connection,
    *,
    marketplace: str,
    external_order_id: str,
    order_date: str,
    currency: str,
    revenue_gross: int,
    revenue_net: Optional[int],
    status: str,
) -> tuple[str, str]:
    existing = connection.execute(
        """
        SELECT id, created_at
        FROM orders
        WHERE LOWER(COALESCE(provider, '')) = ? AND external_order_id = ?
        LIMIT 1
        """,
        (marketplace, external_order_id),
    ).fetchone()

    timestamp = _now_iso()
    safe_status = status.strip() or "unknown"
    safe_revenue_gross = max(int(revenue_gross), 1)
    safe_revenue_net = int(revenue_net) if isinstance(revenue_net, int) and revenue_net > 0 else None

    if existing is None:
        order_db_id = _stable_uuid(f"combined-order:{marketplace}:{external_order_id}")
        connection.execute(
            """
            INSERT INTO orders (
                id, provider, external_order_id, order_date, currency,
                revenue_gross, revenue_net, vat_amount, status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
            """,
            (
                order_db_id,
                marketplace,
                external_order_id,
                order_date,
                currency,
                safe_revenue_gross,
                safe_revenue_net,
                safe_status,
                timestamp,
                timestamp,
            ),
        )
        return order_db_id, "inserted"

    order_db_id = str(existing["id"])
    connection.execute(
        """
        UPDATE orders
        SET
            provider = ?,
            external_order_id = ?,
            order_date = ?,
            currency = ?,
            revenue_gross = ?,
            revenue_net = ?,
            status = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            marketplace,
            external_order_id,
            order_date,
            currency,
            safe_revenue_gross,
            safe_revenue_net,
            safe_status,
            timestamp,
            order_db_id,
        ),
    )
    return order_db_id, "updated"


def _upsert_synced_document(
    connection: sqlite3.Connection,
    *,
    external_document_id: str,
    original_filename: str,
    stored_filename: str,
    file_path: str,
    mime_type: str,
    uploaded_at: str,
    notes: Optional[str],
) -> tuple[Optional[str], str]:
    resolved = _resolve_document_path(file_path)
    if resolved is None:
        return None, "skipped"

    # Store portable relative path when possible, not the resolved absolute.
    raw = str(file_path or "").strip()
    if Path(raw).is_absolute():
        # Try to convert legacy absolute path to relative
        for project_root in _bookkeeping_project_roots():
            try:
                portable = resolved.relative_to(project_root).as_posix()
                raw = portable
                break
            except ValueError:
                continue
    portable_path = raw

    document_id = _stable_uuid(f"combined-document:{external_document_id}")
    existing = connection.execute(
        "SELECT id FROM documents WHERE id = ? LIMIT 1",
        (document_id,),
    ).fetchone()

    guessed_mime, _ = mimetypes.guess_type(original_filename)
    safe_mime = str(mime_type or guessed_mime or "application/octet-stream")
    safe_original = str(original_filename or Path(resolved).name)
    safe_stored = str(stored_filename or Path(resolved).name)
    safe_uploaded_at = _safe_iso(uploaded_at)

    if existing is None:
        connection.execute(
            """
            INSERT INTO documents (
                id, original_filename, stored_filename, file_path, mime_type, uploaded_at, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                document_id,
                safe_original,
                safe_stored,
                portable_path,
                safe_mime,
                safe_uploaded_at,
                notes,
            ),
        )
        return document_id, "inserted"

    connection.execute(
        """
        UPDATE documents
        SET
            original_filename = ?,
            stored_filename = ?,
            file_path = ?,
            mime_type = ?,
            uploaded_at = ?,
            notes = ?
        WHERE id = ?
        """,
        (
            safe_original,
            safe_stored,
            portable_path,
            safe_mime,
            safe_uploaded_at,
            notes,
            document_id,
        ),
    )
    return document_id, "updated"


def _delete_synced_transaction(connection: sqlite3.Connection, *, source_key: str) -> bool:
    row = connection.execute(
        "SELECT id FROM transactions WHERE source_key = ? LIMIT 1",
        (source_key,),
    ).fetchone()
    if row is None:
        return False
    connection.execute("DELETE FROM transactions WHERE id = ?", (str(row["id"]),))
    return True


def _upsert_synced_transaction(
    connection: sqlite3.Connection,
    *,
    source_key: str,
    order_id: Optional[str],
    date: str,
    tx_type: str,
    direction: str,
    amount_gross: int,
    currency: str,
    provider: str,
    counterparty_name: Optional[str],
    category: Optional[str],
    reference: Optional[str],
    notes: Optional[str],
    document_id: Optional[str],
    booking_class: str = "automatic",
) -> str:
    safe_amount = int(amount_gross)
    if safe_amount <= 0:
        deleted = _delete_synced_transaction(connection, source_key=source_key)
        return "deleted" if deleted else "skipped"

    timestamp = _now_iso()
    existing = connection.execute(
        """
        SELECT id, created_at
        FROM transactions
        WHERE source_key = ?
        LIMIT 1
        """,
        (source_key,),
    ).fetchone()

    if existing is None:
        transaction_id = _stable_uuid(f"combined-transaction:{source_key}")
        connection.execute(
            """
            INSERT INTO transactions (
                id, date, type, direction, amount_gross, currency,
                vat_rate, vat_amount, amount_net, provider, counterparty_name, category,
                reference, notes, order_id, document_id, template_id, payment_account_id, period_key,
                source, source_key, status, booking_class, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, 'api', ?, 'confirmed', ?, ?, ?)
            """,
            (
                transaction_id,
                date,
                tx_type,
                direction,
                safe_amount,
                currency,
                provider,
                counterparty_name,
                category,
                reference,
                notes,
                order_id,
                document_id,
                source_key,
                booking_class,
                timestamp,
                timestamp,
            ),
        )
        return "inserted"

    transaction_id = str(existing["id"])
    connection.execute(
        """
        UPDATE transactions
        SET
            date = ?,
            type = ?,
            direction = ?,
            amount_gross = ?,
            currency = ?,
            vat_rate = NULL,
            vat_amount = NULL,
            amount_net = NULL,
            provider = ?,
            counterparty_name = ?,
            category = ?,
            reference = ?,
            notes = ?,
            order_id = ?,
            document_id = ?,
            template_id = NULL,
            payment_account_id = NULL,
            period_key = NULL,
            source = 'api',
            status = 'confirmed',
            booking_class = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            date,
            tx_type,
            direction,
            safe_amount,
            currency,
            provider,
            counterparty_name,
            category,
            reference,
            notes,
            order_id,
            document_id,
            booking_class,
            timestamp,
            transaction_id,
        ),
    )
    return "updated"


def sync_combined_orders_into_bookkeeping(
    *,
    marketplace: Optional[str] = None,
    order_id: Optional[str] = None,
) -> dict[str, Any]:
    if not BOOKKEEPING_DB_PATH.exists():
        return {
            "db_available": False,
            "selected_orders": 0,
            "orders_inserted": 0,
            "orders_updated": 0,
            "transactions_inserted": 0,
            "transactions_updated": 0,
            "transactions_deleted": 0,
            "documents_inserted": 0,
            "documents_updated": 0,
            "documents_skipped": 0,
        }

    from app.db import fetch_enrichment_map
    from app.services.orders import list_all_orders_without_pagination

    market_filter = str(marketplace or "").strip().lower() or None
    order_filter = str(order_id or "").strip() or None

    source_orders = list_all_orders_without_pagination(
        from_date=None,
        to_date=None,
        marketplace=market_filter,
        query=None,
    )
    filtered_orders = []
    for row in source_orders:
        row_market = str(row.get("marketplace") or "").strip().lower()
        row_order_id = str(row.get("order_id") or "").strip()
        if market_filter and row_market != market_filter:
            continue
        if order_filter and row_order_id != order_filter:
            continue
        filtered_orders.append(row)

    enrichment_map = fetch_enrichment_map()
    summary = {
        "db_available": True,
        "selected_orders": len(filtered_orders),
        "orders_inserted": 0,
        "orders_updated": 0,
        "transactions_inserted": 0,
        "transactions_updated": 0,
        "transactions_deleted": 0,
        "documents_inserted": 0,
        "documents_updated": 0,
        "documents_skipped": 0,
    }

    with _connect_bookkeeping_db() as connection:
        for source_order in filtered_orders:
            market = str(source_order.get("marketplace") or "").strip().lower()
            source_order_id = str(source_order.get("order_id") or "").strip()
            external_order_id = str(source_order.get("external_order_id") or source_order_id).strip()
            if not market or not external_order_id:
                continue

            revenue_cents = int(source_order.get("total_cents") or 0)
            if revenue_cents <= 0:
                continue

            fees_cents = max(int(source_order.get("fees_cents") or 0), 0)
            purchase_cents = max(int(source_order.get("purchase_cost_cents") or 0), 0)
            revenue_after_fees = int(source_order.get("after_fees_cents") or 0)
            revenue_net = revenue_after_fees if revenue_after_fees > 0 else None
            safe_order_date = _safe_iso(str(source_order.get("order_date") or ""))
            safe_currency = _safe_currency(source_order.get("currency"))
            safe_status = str(source_order.get("fulfillment_status") or "unknown").strip() or "unknown"

            bookkeeping_order_id, order_action = _upsert_synced_order(
                connection,
                marketplace=market,
                external_order_id=external_order_id,
                order_date=safe_order_date,
                currency=safe_currency,
                revenue_gross=revenue_cents,
                revenue_net=revenue_net,
                status=safe_status,
            )
            if order_action == "inserted":
                summary["orders_inserted"] += 1
            elif order_action == "updated":
                summary["orders_updated"] += 1

            enrichment = enrichment_map.get((market, source_order_id), {})
            supplier_name = str(enrichment.get("supplier_name") or source_order.get("purchase_supplier") or "").strip()

            # --- Determine fee provider based on marketplace + payment method ---
            payment_method = str(source_order.get("payment_method") or "").strip().lower()
            if market == "kaufland":
                fee_provider = "kaufland"
                fee_counterparty = "Kaufland Fees"
            elif market == "shopify":
                if payment_method in ("paypal", "paypal express checkout"):
                    fee_provider = "paypal"
                    fee_counterparty = "PayPal Fees"
                else:
                    fee_provider = "shopify_payments"
                    fee_counterparty = "Shopify Payments Fees"
            elif market == "ebay":
                fee_provider = "ebay"
                fee_counterparty = "eBay Fees"
            else:
                fee_provider = market
                fee_counterparty = f"{market.title()} Fees"

            cogs_document_id: Optional[str] = None
            invoice_document_id = str(enrichment.get("invoice_document_id") or "").strip()
            invoice_file_path = str(enrichment.get("file_path") or "").strip()
            if invoice_document_id and invoice_file_path:
                cogs_document_id, doc_action = _upsert_synced_document(
                    connection,
                    external_document_id=invoice_document_id,
                    original_filename=str(enrichment.get("original_filename") or Path(invoice_file_path).name),
                    stored_filename=str(enrichment.get("stored_filename") or Path(invoice_file_path).name),
                    file_path=invoice_file_path,
                    mime_type=str(enrichment.get("mime_type") or ""),
                    uploaded_at=str(enrichment.get("uploaded_at") or ""),
                    notes=f"Sync invoice for {market}:{external_order_id}",
                )
                if doc_action == "inserted":
                    summary["documents_inserted"] += 1
                elif doc_action == "updated":
                    summary["documents_updated"] += 1
                else:
                    summary["documents_skipped"] += 1

            sale_key = f"combined:{market}:{external_order_id}:sale"
            fee_key = f"combined:{market}:{external_order_id}:fee"
            cogs_key = f"combined:{market}:{external_order_id}:cogs"

            sale_action = _upsert_synced_transaction(
                connection,
                source_key=sale_key,
                order_id=bookkeeping_order_id,
                date=_shift_iso(safe_order_date, minutes=0),
                tx_type="SALE",
                direction="IN",
                amount_gross=revenue_cents,
                currency=safe_currency,
                provider=market,
                counterparty_name=str(source_order.get("customer") or "Endkunde").strip() or "Endkunde",
                category="revenue",
                reference=external_order_id,
                notes="Auto-sync aus Combined Orders",
                document_id=None,
                booking_class="automatic",
            )

            fee_action = _upsert_synced_transaction(
                connection,
                source_key=fee_key,
                order_id=bookkeeping_order_id,
                date=_shift_iso(safe_order_date, minutes=1),
                tx_type="FEE",
                direction="OUT",
                amount_gross=fees_cents,
                currency=safe_currency,
                provider=fee_provider,
                counterparty_name=fee_counterparty,
                category="fees",
                reference=f"{external_order_id}-fees",
                notes="Auto-sync Gebuehren aus Combined Orders",
                document_id=None,
                booking_class="automatic",
            )

            cogs_action = _upsert_synced_transaction(
                connection,
                source_key=cogs_key,
                order_id=bookkeeping_order_id,
                date=_shift_iso(safe_order_date, minutes=2),
                tx_type="COGS",
                direction="OUT",
                amount_gross=purchase_cents,
                currency=safe_currency,
                provider=supplier_name or "supplier",
                counterparty_name=supplier_name or "Supplier",
                category="cogs",
                reference=f"{external_order_id}-cogs",
                notes="Auto-sync Einkauf aus Combined Orders",
                document_id=cogs_document_id,
                booking_class="automatic",
            )

            for action in (sale_action, fee_action, cogs_action):
                if action == "inserted":
                    summary["transactions_inserted"] += 1
                elif action == "updated":
                    summary["transactions_updated"] += 1
                elif action == "deleted":
                    summary["transactions_deleted"] += 1

        connection.commit()

    return summary


def sync_google_ads_into_bookkeeping() -> dict[str, Any]:
    """Sync daily Google Ads costs into bookkeeping as automatic FEE transactions.

    Reads from `google_ads_daily_costs` (combined DB), aggregates per day,
    and upserts one FEE/OUT transaction per day into the bookkeeping DB.
    These transactions are then picked up by the Sammelrechnung system.
    """
    result: dict[str, Any] = {
        "db_available": False,
        "days_total": 0,
        "transactions_inserted": 0,
        "transactions_updated": 0,
        "transactions_deleted": 0,
        "transactions_skipped": 0,
    }

    if not BOOKKEEPING_DB_PATH.exists():
        return result

    from app.db import connect_combined_db

    # ── Step 1: Read daily costs from combined DB, aggregate by day ──
    daily_totals: dict[str, int] = defaultdict(int)
    with connect_combined_db() as combined_conn:
        rows = combined_conn.execute(
            "SELECT day, cost_cents FROM google_ads_daily_costs"
        ).fetchall()
        for row in rows:
            day = str(row["day"]).strip()
            cents = int(row["cost_cents"] or 0)
            if day:
                daily_totals[day] += cents

    result["db_available"] = True
    result["days_total"] = len(daily_totals)

    # ── Step 2: Find existing google_ads source_keys to detect stale days ──
    with _connect_bookkeeping_db() as bk_conn:
        existing_keys = set()
        for row in bk_conn.execute(
            "SELECT source_key FROM transactions WHERE source_key LIKE 'google_ads:%:fee'"
        ).fetchall():
            existing_keys.add(str(row["source_key"]))

        # ── Step 3: Upsert one FEE transaction per day ──
        for day, total_cents in sorted(daily_totals.items()):
            source_key = f"google_ads:{day}:fee"
            existing_keys.discard(source_key)

            # Build a noon-UTC timestamp for the day so it sorts nicely
            date_iso = f"{day}T12:00:00Z"

            action = _upsert_synced_transaction(
                bk_conn,
                source_key=source_key,
                order_id=None,
                date=date_iso,
                tx_type="FEE",
                direction="OUT",
                amount_gross=total_cents,
                currency="EUR",
                provider="google_ads",
                counterparty_name="Google Ads",
                category="fees",
                reference=f"google-ads-{day}",
                notes="Auto-sync Google Ads Tageskosten",
                document_id=None,
                booking_class="automatic",
            )
            if action == "inserted":
                result["transactions_inserted"] += 1
            elif action == "updated":
                result["transactions_updated"] += 1
            elif action == "deleted":
                result["transactions_deleted"] += 1
            else:
                result["transactions_skipped"] += 1

        # ── Step 4: Delete stale transactions for days no longer in the data ──
        for stale_key in existing_keys:
            if _delete_synced_transaction(bk_conn, source_key=stale_key):
                result["transactions_deleted"] += 1

        bk_conn.commit()

    return result


def list_bookings(
    *,
    limit: int,
    offset: int,
    from_date: Optional[str],
    to_date: Optional[str],
    query: Optional[str],
) -> dict[str, Any]:
    if not BOOKKEEPING_DB_PATH.exists():
        return {"total": 0, "items": [], "db_available": False}

    where_sql, params = _build_filters(from_date, to_date, query)
    with _connect_bookkeeping_db() as connection:
        rows = connection.execute(
            f"""
            SELECT
                t.id,
                t.date,
                t.type,
                t.direction,
                t.amount_gross,
                t.currency,
                t.vat_rate,
                t.vat_amount,
                t.amount_net,
                t.provider,
                t.counterparty_name,
                t.category,
                t.reference,
                t.notes,
                t.status,
                t.source,
                t.order_id,
                t.document_id,
                d.original_filename AS document_original_filename,
                d.stored_filename AS document_stored_filename,
                d.mime_type AS document_mime_type
            FROM transactions t
            LEFT JOIN documents d ON d.id = t.document_id
            {where_sql}
            ORDER BY COALESCE(t.date, '') DESC, t.id DESC
            LIMIT ? OFFSET ?
            """,
            [*params, int(limit), int(offset)],
        ).fetchall()

        total_row = connection.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM transactions t
            {where_sql}
            """,
            params,
        ).fetchone()

    total = int(total_row["c"]) if total_row is not None else 0
    return {
        "total": total,
        "items": [dict(row) for row in rows],
        "db_available": True,
    }


def update_booking(
    *,
    booking_id: str,
    status: Optional[str],
    reference: Optional[str],
    notes: Optional[str],
) -> Optional[dict[str, Any]]:
    if not BOOKKEEPING_DB_PATH.exists():
        return None

    allowed_status = {"pending", "confirmed", "reconciled", ""}
    if status is not None and status.strip().lower() not in allowed_status:
        raise ValueError("status must be pending, confirmed, reconciled or empty")

    updates: list[str] = []
    params: list[Any] = []

    if status is not None:
        normalized_status = status.strip().lower() or None
        updates.append("status = ?")
        params.append(normalized_status)

    if reference is not None:
        updates.append("reference = ?")
        params.append(reference.strip() or None)

    if notes is not None:
        updates.append("notes = ?")
        params.append(notes.strip() or None)

    if not updates:
        return None

    updates.append("updated_at = ?")
    params.append(datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"))

    params.append(booking_id)

    with _connect_bookkeeping_db() as connection:
        row = connection.execute(
            "SELECT id FROM transactions WHERE id = ? LIMIT 1",
            (booking_id,),
        ).fetchone()
        if row is None:
            return None

        connection.execute(
            f"UPDATE transactions SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        connection.commit()

        updated = connection.execute(
            """
            SELECT
                t.id,
                t.date,
                t.type,
                t.direction,
                t.amount_gross,
                t.currency,
                t.provider,
                t.reference,
                t.notes,
                t.status,
                t.document_id,
                d.original_filename AS document_original_filename
            FROM transactions t
            LEFT JOIN documents d ON d.id = t.document_id
            WHERE t.id = ?
            LIMIT 1
            """,
            (booking_id,),
        ).fetchone()

    return dict(updated) if updated is not None else None


def fetch_bookkeeping_document(document_id: str) -> Optional[dict[str, Any]]:
    if not BOOKKEEPING_DB_PATH.exists():
        return None

    with _connect_bookkeeping_db() as connection:
        row = connection.execute(
            """
            SELECT
                id,
                original_filename,
                stored_filename,
                file_path,
                mime_type,
                uploaded_at
            FROM documents
            WHERE id = ?
            LIMIT 1
            """,
            (document_id,),
        ).fetchone()

    if row is None:
        return None

    payload = dict(row)
    resolved = _resolve_document_path(str(payload.get("file_path") or ""))
    payload["resolved_path"] = str(resolved) if resolved is not None else None
    payload["exists"] = bool(resolved and resolved.exists() and resolved.is_file())
    payload["previewable"] = str(payload.get("mime_type") or "").startswith("image/") or "pdf" in str(payload.get("mime_type") or "").lower()
    return payload


def _empty_breakdown() -> dict[str, Any]:
    return {
        "db_available": BOOKKEEPING_DB_PATH.exists(),
        "matched_via": "none",
        "bookkeeping_order": None,
        "income_total_cents": 0,
        "expense_total_cents": 0,
        "additional_expense_total_cents": 0,
        "mirrored_fee_total_cents": 0,
        "mirrored_cogs_total_cents": 0,
        "fee_total_cents": 0,
        "cogs_total_cents": 0,
        "other_expenses_cents": 0,
        "additional_fee_cents": 0,
        "additional_cogs_cents": 0,
        "additional_other_cents": 0,
        "type_breakdown": [],
        "transactions": [],
        "documents": [],
    }


def get_order_bookkeeping_breakdown(
    *,
    marketplace: str,
    external_order_id: str,
) -> dict[str, Any]:
    if not BOOKKEEPING_DB_PATH.exists():
        return _empty_breakdown()

    market = str(marketplace or "").strip().lower()
    external = str(external_order_id or "").strip()
    if not market or not external:
        return _empty_breakdown()

    with _connect_bookkeeping_db() as connection:
        order_row = connection.execute(
            """
            SELECT *
            FROM orders
            WHERE LOWER(COALESCE(provider, '')) = ? AND external_order_id = ?
            LIMIT 1
            """,
            (market, external),
        ).fetchone()

        tx_rows: list[sqlite3.Row] = []
        matched_via = "none"

        if order_row is not None:
            tx_rows = connection.execute(
                """
                SELECT
                    t.*,
                    d.original_filename AS document_original_filename,
                    d.stored_filename AS document_stored_filename,
                    d.mime_type AS document_mime_type
                FROM transactions t
                LEFT JOIN documents d ON d.id = t.document_id
                WHERE t.order_id = ?
                ORDER BY COALESCE(t.date, '') ASC, t.id ASC
                """,
                (order_row["id"],),
            ).fetchall()
            matched_via = "order_id"

        if not tx_rows:
            like_value = f"%{external.lower()}%"
            tx_rows = connection.execute(
                """
                SELECT
                    t.*,
                    d.original_filename AS document_original_filename,
                    d.stored_filename AS document_stored_filename,
                    d.mime_type AS document_mime_type
                FROM transactions t
                LEFT JOIN documents d ON d.id = t.document_id
                WHERE
                    LOWER(COALESCE(t.provider, '')) = ?
                    AND (
                        LOWER(COALESCE(t.reference, '')) LIKE ?
                        OR LOWER(COALESCE(t.notes, '')) LIKE ?
                    )
                ORDER BY COALESCE(t.date, '') ASC, t.id ASC
                """,
                (market, like_value, like_value),
            ).fetchall()
            if tx_rows:
                matched_via = "reference_fallback"

    income_total = 0
    expense_total = 0
    additional_expense_total = 0
    mirrored_fee_total = 0
    mirrored_cogs_total = 0
    fee_total = 0
    cogs_total = 0
    other_total = 0
    additional_fee_total = 0
    additional_cogs_total = 0
    additional_other_total = 0

    type_bucket: dict[str, dict[str, Any]] = defaultdict(lambda: {"count": 0, "total_cents": 0, "type": "", "direction": ""})
    documents: dict[str, dict[str, Any]] = {}
    transactions: list[dict[str, Any]] = []

    for row in tx_rows:
        payload = dict(row)
        amount_cents = int(payload.get("amount_gross") or 0)
        direction = str(payload.get("direction") or "").upper()
        tx_type = str(payload.get("type") or "").upper()
        source = str(payload.get("source") or "").strip().lower()
        source_key = str(payload.get("source_key") or "").strip().lower()
        is_combined_auto = source == "api" and source_key.startswith("combined:")

        if direction == "IN":
            income_total += amount_cents
        elif direction == "OUT":
            expense_total += amount_cents

        if direction == "OUT" and tx_type == "FEE":
            fee_total += amount_cents
        elif direction == "OUT" and tx_type == "COGS":
            cogs_total += amount_cents
        elif direction == "OUT":
            other_total += amount_cents

        if direction == "OUT":
            if is_combined_auto and tx_type == "FEE":
                mirrored_fee_total += amount_cents
            elif is_combined_auto and tx_type == "COGS":
                mirrored_cogs_total += amount_cents
            else:
                additional_expense_total += amount_cents
                if tx_type == "FEE":
                    additional_fee_total += amount_cents
                elif tx_type == "COGS":
                    additional_cogs_total += amount_cents
                else:
                    additional_other_total += amount_cents

        bucket_key = f"{tx_type}:{direction}"
        bucket = type_bucket[bucket_key]
        bucket["count"] += 1
        bucket["total_cents"] += amount_cents
        bucket["type"] = tx_type
        bucket["direction"] = direction

        document_id = payload.get("document_id")
        document_url = None
        if document_id:
            document_url = f"/api/bookings/documents/{document_id}/download"
            if document_id not in documents:
                mime_type = str(payload.get("document_mime_type") or "")
                documents[document_id] = {
                    "document_id": document_id,
                    "original_filename": payload.get("document_original_filename"),
                    "stored_filename": payload.get("document_stored_filename"),
                    "mime_type": mime_type,
                    "download_url": document_url,
                    "previewable": mime_type.startswith("image/") or "pdf" in mime_type.lower(),
                }

        transactions.append(
            {
                "id": payload.get("id"),
                "date": payload.get("date"),
                "type": payload.get("type"),
                "direction": payload.get("direction"),
                "amount_gross": amount_cents,
                "currency": payload.get("currency"),
                "provider": payload.get("provider"),
                "reference": payload.get("reference"),
                "notes": payload.get("notes"),
                "status": payload.get("status"),
                "document_id": document_id,
                "document_original_filename": payload.get("document_original_filename"),
                "document_mime_type": payload.get("document_mime_type"),
                "document_download_url": document_url,
            }
        )

    breakdown = {
        "db_available": True,
        "matched_via": matched_via,
        "bookkeeping_order": dict(order_row) if order_row is not None else None,
        "income_total_cents": income_total,
        "expense_total_cents": expense_total,
        "additional_expense_total_cents": additional_expense_total,
        "mirrored_fee_total_cents": mirrored_fee_total,
        "mirrored_cogs_total_cents": mirrored_cogs_total,
        "fee_total_cents": fee_total,
        "cogs_total_cents": cogs_total,
        "other_expenses_cents": other_total,
        "additional_fee_cents": additional_fee_total,
        "additional_cogs_cents": additional_cogs_total,
        "additional_other_cents": additional_other_total,
        "type_breakdown": sorted(type_bucket.values(), key=lambda item: item["total_cents"], reverse=True),
        "transactions": transactions,
        "documents": list(documents.values()),
    }
    return breakdown


def list_booking_orders(
    *,
    from_date: Optional[str],
    to_date: Optional[str],
    marketplace: Optional[str],
    query: Optional[str],
) -> dict[str, Any]:
    from app.services.orders import list_all_orders_without_pagination

    orders = list_all_orders_without_pagination(
        from_date=from_date,
        to_date=to_date,
        marketplace=marketplace,
        query=query,
    )

    items: list[dict[str, Any]] = []
    for order in orders:
        market = str(order.get("marketplace") or "").strip().lower()
        external = str(order.get("external_order_id") or order.get("order_id") or "").strip()
        if not market or not external:
            continue

        breakdown = get_order_bookkeeping_breakdown(
            marketplace=market,
            external_order_id=external,
        )

        revenue_cents = int(order.get("total_cents") or 0)
        fees_cents = int(order.get("fees_cents") or 0)
        purchase_cents = int(order.get("purchase_cost_cents") or 0)
        bookkeeping_expense_total_cents = int(breakdown.get("expense_total_cents") or 0)
        bookkeeping_expense_cents = int(breakdown.get("additional_expense_total_cents") or 0)
        bookkeeping_fee_cents = int(breakdown.get("fee_total_cents") or 0)
        bookkeeping_cogs_cents = int(breakdown.get("cogs_total_cents") or 0)
        bookkeeping_other_cents = int(breakdown.get("other_expenses_cents") or 0)
        bookkeeping_additional_fee_cents = int(breakdown.get("additional_fee_cents") or 0)
        bookkeeping_additional_cogs_cents = int(breakdown.get("additional_cogs_cents") or 0)
        bookkeeping_additional_other_cents = int(breakdown.get("additional_other_cents") or 0)
        bookkeeping_mirrored_fee_cents = int(breakdown.get("mirrored_fee_total_cents") or 0)
        bookkeeping_mirrored_cogs_cents = int(breakdown.get("mirrored_cogs_total_cents") or 0)
        total_costs_cents = fees_cents + purchase_cents + bookkeeping_expense_cents
        profit_cents = revenue_cents - total_costs_cents

        items.append(
            {
                "marketplace": market,
                "order_id": order.get("order_id"),
                "external_order_id": external,
                "order_date": order.get("order_date"),
                "customer": order.get("customer"),
                "article": order.get("article"),
                "currency": order.get("currency") or "EUR",
                "revenue_cents": revenue_cents,
                "fees_cents": fees_cents,
                "purchase_cents": purchase_cents,
                "bookkeeping_fee_cents": bookkeeping_fee_cents,
                "bookkeeping_cogs_cents": bookkeeping_cogs_cents,
                "bookkeeping_other_cents": bookkeeping_other_cents,
                "bookkeeping_additional_fee_cents": bookkeeping_additional_fee_cents,
                "bookkeeping_additional_cogs_cents": bookkeeping_additional_cogs_cents,
                "bookkeeping_additional_other_cents": bookkeeping_additional_other_cents,
                "bookkeeping_expense_total_cents": bookkeeping_expense_total_cents,
                "bookkeeping_mirrored_fee_cents": bookkeeping_mirrored_fee_cents,
                "bookkeeping_mirrored_cogs_cents": bookkeeping_mirrored_cogs_cents,
                "bookkeeping_income_cents": int(breakdown.get("income_total_cents") or 0),
                "bookkeeping_expense_cents": bookkeeping_expense_cents,
                "total_costs_cents": total_costs_cents,
                "profit_cents": profit_cents,
                "documents_count": len(breakdown.get("documents") or []),
                "bookkeeping_matched_via": breakdown.get("matched_via"),
            }
        )

    items.sort(key=lambda row: str(row.get("order_date") or ""), reverse=True)
    return {
        "total": len(items),
        "items": items,
    }
