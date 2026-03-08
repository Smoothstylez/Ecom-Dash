from __future__ import annotations

import calendar
import mimetypes
import re
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from app.config import BOOKKEEPING_DB_PATH, BOOKKEEPING_DOCUMENTS_DIR, BOOKKEEPING_PROJECT_ROOT, MAX_UPLOAD_BYTES


TRANSACTION_TYPES = {
    "SALE",
    "COGS",
    "FEE",
    "SUBSCRIPTION",
    "EXPENSE",
    "REFUND",
    "PAYOUT",
    "ADJUSTMENT",
}
TRANSACTION_DIRECTIONS = {"IN", "OUT"}
TRANSACTION_SOURCES = {"api", "manual"}
TRANSACTION_STATUSES = {"pending", "confirmed", "reconciled"}
RECURRING_SCHEDULES = {"monthly", "quarterly", "yearly"}
BOOKING_CLASSES = {"automatic", "monthly", "single"}
MONTHLY_INVOICE_STATUSES = {"draft", "matched", "mismatch"}


class BookkeepingServiceError(Exception):
    def __init__(self, status_code: int, detail: str, details: Any | None = None):
        super().__init__(detail)
        self.status_code = int(status_code)
        self.detail = detail
        self.details = details


def _connect_bookkeeping_db() -> sqlite3.Connection:
    connection = sqlite3.connect(BOOKKEEPING_DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    if _ensure_schema(connection):
        connection.commit()
    return connection


def _column_exists(connection: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(row["name"]) == column_name for row in rows)


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _ensure_column(connection: sqlite3.Connection, table_name: str, column_name: str, column_definition: str) -> bool:
    if not _table_exists(connection, table_name):
        return False
    if _column_exists(connection, table_name, column_name):
        return False
    connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}")
    return True


def _ensure_schema(connection: sqlite3.Connection) -> bool:
    changed = False
    changed = _ensure_column(connection, "recurring_templates", "start_date", "TEXT") or changed
    changed = _ensure_column(connection, "transactions", "booking_class", "TEXT DEFAULT 'single'") or changed

    # --- monthly_invoices ---
    if not _table_exists(connection, "monthly_invoices"):
        connection.execute("""
            CREATE TABLE monthly_invoices (
                id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                period_from TEXT NOT NULL,
                period_to TEXT NOT NULL,
                invoice_amount_cents INTEGER NOT NULL,
                currency TEXT NOT NULL DEFAULT 'EUR',
                calculated_sum_cents INTEGER,
                difference_cents INTEGER,
                document_id TEXT,
                notes TEXT,
                status TEXT NOT NULL DEFAULT 'draft',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE SET NULL
            )
        """)
        changed = True

    if not _table_exists(connection, "monthly_invoice_transactions"):
        connection.execute("""
            CREATE TABLE monthly_invoice_transactions (
                invoice_id TEXT NOT NULL,
                transaction_id TEXT NOT NULL,
                PRIMARY KEY (invoice_id, transaction_id),
                FOREIGN KEY (invoice_id) REFERENCES monthly_invoices(id) ON DELETE CASCADE,
                FOREIGN KEY (transaction_id) REFERENCES transactions(id) ON DELETE CASCADE
            )
        """)
        changed = True

    # --- backfill booking_class for existing rows ---
    if changed:
        connection.execute("""
            UPDATE transactions SET booking_class = 'automatic'
            WHERE booking_class IS NULL AND source = 'api'
        """)
        connection.execute("""
            UPDATE transactions SET booking_class = 'monthly'
            WHERE booking_class IS NULL AND source = 'manual' AND type = 'SUBSCRIPTION'
        """)
        connection.execute("""
            UPDATE transactions SET booking_class = 'single'
            WHERE booking_class IS NULL
        """)

    # SUBSCRIPTION transactions should always be classified as 'monthly'
    # regardless of how they were originally created.
    connection.execute("""
        UPDATE transactions SET booking_class = 'monthly'
        WHERE type = 'SUBSCRIPTION' AND booking_class != 'monthly'
    """)

    return changed


def _ensure_db_available() -> None:
    if not BOOKKEEPING_DB_PATH.exists():
        raise BookkeepingServiceError(503, "bookkeeping database not available")


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_relative_project_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(BOOKKEEPING_PROJECT_ROOT.resolve()).as_posix()
    except ValueError:
        return str(resolved)


def _resolve_project_path(relative_or_absolute: str) -> Path:
    path = Path(str(relative_or_absolute or "").strip())
    if path.is_absolute():
        return path
    return (BOOKKEEPING_PROJECT_ROOT / path).resolve()


def _parse_iso_datetime(value: str, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise BookkeepingServiceError(400, f"{field_name} is required")

    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            parsed = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
            return _to_iso_utc(parsed)

        normalized = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return _to_iso_utc(parsed)
    except ValueError as exc:
        raise BookkeepingServiceError(400, f"invalid datetime for {field_name}") from exc


def _parse_filter_date(value: str, field_name: str, *, is_end: bool = False) -> str:
    text = str(value or "").strip()
    if not text:
        raise BookkeepingServiceError(400, f"{field_name} cannot be empty")

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        parsed = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
        if is_end:
            parsed = parsed + timedelta(days=1) - timedelta(seconds=1)
        return _to_iso_utc(parsed)

    return _parse_iso_datetime(text, field_name)


def _parse_template_start_date(value: Any, field_name: str = "start_date") -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise BookkeepingServiceError(400, f"{field_name} must be a string")

    text = value.strip()
    if not text:
        return None

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BookkeepingServiceError(400, f"invalid date for {field_name}") from exc
    return parsed.date().isoformat()


def _day_of_month_from_start_date(start_date: str | None) -> int | None:
    if not start_date:
        return None
    try:
        parsed = datetime.fromisoformat(str(start_date).replace("Z", "+00:00"))
    except ValueError:
        return None
    return int(parsed.day)


def _normalize_currency(value: Any, field_name: str = "currency") -> str:
    if not isinstance(value, str):
        raise BookkeepingServiceError(400, f"{field_name} must be a string")

    normalized = value.strip().upper()
    if len(normalized) != 3:
        raise BookkeepingServiceError(400, f"{field_name} must be a 3-letter code")
    return normalized


def _ensure_uuid(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise BookkeepingServiceError(400, f"{field_name} must be a uuid string")
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise BookkeepingServiceError(400, f"{field_name} must be a valid uuid") from exc
    return str(parsed)


def _ensure_required_string(payload: dict[str, Any], key: str, *, max_length: int | None = None) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise BookkeepingServiceError(400, f"{key} is required")
    normalized = value.strip()
    if not normalized:
        raise BookkeepingServiceError(400, f"{key} is required")
    if max_length is not None and len(normalized) > max_length:
        raise BookkeepingServiceError(400, f"{key} exceeds max length {max_length}")
    return normalized


def _ensure_optional_string(
    payload: dict[str, Any],
    key: str,
    *,
    max_length: int | None = None,
    nullable: bool = False,
) -> str | None:
    if key not in payload:
        return None

    value = payload.get(key)
    if value is None and nullable:
        return None
    if not isinstance(value, str):
        raise BookkeepingServiceError(400, f"{key} must be a string")

    normalized = value.strip()
    if max_length is not None and len(normalized) > max_length:
        raise BookkeepingServiceError(400, f"{key} exceeds max length {max_length}")
    return normalized


def _parse_positive_int(value: Any, field_name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise BookkeepingServiceError(400, f"{field_name} must be an integer") from exc
    if parsed <= 0:
        raise BookkeepingServiceError(400, f"{field_name} must be > 0")
    return parsed


def _parse_nonnegative_int(value: Any, field_name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise BookkeepingServiceError(400, f"{field_name} must be an integer") from exc
    if parsed < 0:
        raise BookkeepingServiceError(400, f"{field_name} must be >= 0")
    return parsed


def _parse_optional_vat_rate(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise BookkeepingServiceError(400, "vat_rate must be numeric") from exc
    if parsed < 0 or parsed > 100:
        raise BookkeepingServiceError(400, "vat_rate must be between 0 and 100")
    return round(parsed, 2)


def _parse_optional_bool(value: Any, field_name: str) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    raise BookkeepingServiceError(400, f"{field_name} must be true/false")


def _parse_period_key(value: Any, field_name: str = "period_key") -> str:
    if not isinstance(value, str):
        raise BookkeepingServiceError(400, f"{field_name} must be a string")
    normalized = value.strip()
    if not re.fullmatch(r"\d{4}-\d{2}", normalized):
        raise BookkeepingServiceError(400, f"{field_name} must be YYYY-MM")
    return normalized


def _period_key_from_date(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc).strftime("%Y-%m")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%Y-%m")


def _date_from_period_key(period_key: str, day_of_month: int | None = None) -> str:
    year, month = period_key.split("-")
    year_int = int(year)
    month_int = int(month)
    _, days_in_month = calendar.monthrange(year_int, month_int)
    day_value = 1 if day_of_month is None else max(1, min(day_of_month, days_in_month))
    dt = datetime(year_int, month_int, day_value, 9, 0, 0, tzinfo=timezone.utc)
    return _to_iso_utc(dt)


def _calculate_vat_amount_cents(amount_gross: int, vat_rate: float | None) -> int | None:
    if vat_rate is None:
        return None
    if vat_rate <= 0:
        return 0
    return round(amount_gross * vat_rate / (100 + vat_rate))


def _calculate_amount_net_cents(amount_gross: int, vat_amount: int | None) -> int | None:
    if vat_amount is None:
        return None
    return max(amount_gross - vat_amount, 0)


def _sanitize_filename(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]", "-", str(value or "").strip())
    cleaned = re.sub(r"-+", "-", cleaned).strip("-._")
    return cleaned or "beleg"


def _slugify_token(value: str | None, fallback: str, max_length: int = 32) -> str:
    if not value:
        return fallback
    slug = re.sub(r"[^a-z0-9]+", "-", value.strip().lower())
    slug = re.sub(r"-+", "-", slug).strip("-")
    if not slug:
        slug = fallback
    return slug[:max_length]


def _doc_type_token(value: str | None) -> str:
    token = _slugify_token(value, "document", 24).upper().replace("-", "_")
    return token or "DOCUMENT"


def _extract_upload_date_token(value: str | None) -> str:
    if not value:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    stripped = value.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", stripped):
        return stripped
    try:
        parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _build_document_storage_name(
    *,
    document_id: str,
    original_filename: str,
    provider_or_counterparty: str | None,
    transaction_type: str | None,
    amount_gross: int | None,
    currency: str | None,
    booking_date: str | None,
    transaction_id: str | None,
) -> str:
    extension = Path(original_filename).suffix.lower() or ".bin"
    date_token = _extract_upload_date_token(booking_date)
    provider_token = _slugify_token(provider_or_counterparty, "unknown", 32)
    type_token = _doc_type_token(transaction_type)
    amount_token = str(amount_gross) if isinstance(amount_gross, int) and amount_gross > 0 else "na"
    currency_token = _normalize_currency(currency or "EUR", "currency").upper()
    transaction_short = transaction_id.replace("-", "")[:8] if transaction_id else "none0000"
    document_short = document_id.replace("-", "")[:8]
    return (
        f"{date_token}_{type_token}_{provider_token}_{amount_token}_{currency_token}"
        f"_TX-{transaction_short}_DOC-{document_short}{extension}"
    )


def _build_document_display_name(
    *,
    original_filename: str,
    provider_or_counterparty: str | None,
    transaction_type: str | None,
) -> str:
    """Build a concise, human-readable display name for a bookkeeping document.

    Pattern: ``{type}_{provider}_beleg.{ext}``
    e.g. ``subscription_openai_beleg.png``
    """
    extension = Path(original_filename).suffix.lower() or ".bin"
    type_token = _slugify_token(transaction_type, "beleg", 24).lower()
    provider_token = _slugify_token(provider_or_counterparty, "", 24)
    if provider_token:
        return _sanitize_filename(f"{type_token}_{provider_token}_beleg{extension}")
    return _sanitize_filename(f"{type_token}_beleg{extension}")


def _order_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": row["id"],
        "provider": row["provider"],
        "external_order_id": row["external_order_id"],
        "order_date": row["order_date"],
        "currency": row["currency"],
        "revenue_gross": row["revenue_gross"],
        "revenue_net": row["revenue_net"],
        "vat_amount": row["vat_amount"],
        "status": row["status"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }
    if "tx_count" in row.keys():
        payload["_count"] = {"transactions": row["tx_count"]}
    return payload


def _document_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": row["id"],
        "original_filename": row["original_filename"],
        "stored_filename": row["stored_filename"],
        "file_path": row["file_path"],
        "mime_type": row["mime_type"],
        "uploaded_at": row["uploaded_at"],
        "notes": row["notes"],
    }
    if "tx_count" in row.keys():
        payload["_count"] = {"transactions": row["tx_count"]}
    return payload


def _payment_account_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "name": row["name"],
        "provider": row["provider"],
        "is_active": bool(row["is_active"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _recurring_template_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": row["id"],
        "name": row["name"],
        "type": row["type"],
        "direction": row["direction"],
        "default_amount_gross": row["default_amount_gross"],
        "currency": row["currency"],
        "provider": row["provider"],
        "counterparty_name": row["counterparty_name"],
        "category": row["category"],
        "vat_rate": None if row["vat_rate"] is None else float(row["vat_rate"]),
        "payment_account_id": row["payment_account_id"],
        "schedule": row["schedule"],
        "day_of_month": row["day_of_month"],
        "start_date": row["start_date"],
        "active": bool(row["active"]),
        "notes_default": row["notes_default"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "payment_account": None,
    }
    if "payment_account_join_id" in row.keys() and row["payment_account_join_id"] is not None:
        payload["payment_account"] = {
            "id": row["payment_account_join_id"],
            "name": row["payment_account_name"],
            "provider": row["payment_account_provider"],
            "is_active": bool(row["payment_account_is_active"]),
        }
    return payload


def _transaction_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": row["id"],
        "date": row["date"],
        "type": row["type"],
        "direction": row["direction"],
        "amount_gross": row["amount_gross"],
        "currency": row["currency"],
        "vat_rate": None if row["vat_rate"] is None else float(row["vat_rate"]),
        "vat_amount": row["vat_amount"],
        "amount_net": row["amount_net"],
        "provider": row["provider"],
        "counterparty_name": row["counterparty_name"],
        "category": row["category"],
        "reference": row["reference"],
        "order_id": row["order_id"],
        "document_id": row["document_id"],
        "template_id": row["template_id"],
        "payment_account_id": row["payment_account_id"],
        "period_key": row["period_key"],
        "notes": row["notes"],
        "source": row["source"],
        "source_key": row["source_key"],
        "status": row["status"],
        "booking_class": row["booking_class"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "order": None,
        "document": None,
        "template": None,
        "payment_account": None,
    }

    if row["order_join_id"] is not None:
        payload["order"] = {
            "id": row["order_join_id"],
            "provider": row["order_provider"],
            "external_order_id": row["order_external_order_id"],
        }

    if row["document_join_id"] is not None:
        payload["document"] = {
            "id": row["document_join_id"],
            "original_filename": row["document_original_filename"],
            "stored_filename": row["document_stored_filename"],
            "file_path": row["document_file_path"],
            "mime_type": row["document_mime_type"],
            "uploaded_at": row["document_uploaded_at"],
            "notes": row["document_notes"],
        }

    if row["template_join_id"] is not None:
        payload["template"] = {
            "id": row["template_join_id"],
            "name": row["template_name"],
            "schedule": row["template_schedule"],
        }

    if row["payment_account_join_id"] is not None:
        payload["payment_account"] = {
            "id": row["payment_account_join_id"],
            "name": row["payment_account_name"],
            "provider": row["payment_account_provider"],
            "is_active": bool(row["payment_account_is_active"]),
        }

    return payload


def _transactions_select_sql() -> str:
    return """
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
            t.order_id,
            t.document_id,
            t.template_id,
            t.payment_account_id,
            t.period_key,
            t.notes,
            t.source,
            t.source_key,
            t.status,
            t.booking_class,
            t.created_at,
            t.updated_at,
            o.id AS order_join_id,
            o.provider AS order_provider,
            o.external_order_id AS order_external_order_id,
            d.id AS document_join_id,
            d.original_filename AS document_original_filename,
            d.stored_filename AS document_stored_filename,
            d.file_path AS document_file_path,
            d.mime_type AS document_mime_type,
            d.uploaded_at AS document_uploaded_at,
            d.notes AS document_notes,
            rt.id AS template_join_id,
            rt.name AS template_name,
            rt.schedule AS template_schedule,
            pa.id AS payment_account_join_id,
            pa.name AS payment_account_name,
            pa.provider AS payment_account_provider,
            pa.is_active AS payment_account_is_active
        FROM transactions t
        LEFT JOIN orders o ON o.id = t.order_id
        LEFT JOIN documents d ON d.id = t.document_id
        LEFT JOIN recurring_templates rt ON rt.id = t.template_id
        LEFT JOIN payment_accounts pa ON pa.id = t.payment_account_id
    """


def _parse_transaction_filters(raw_filters: dict[str, str | None]) -> dict[str, Any]:
    parsed: dict[str, Any] = {
        "dateFrom": None,
        "dateTo": None,
        "type": None,
        "provider": None,
        "direction": None,
        "hasDocument": None,
        "orderId": None,
        "templateId": None,
        "paymentAccountId": None,
        "bookingClass": None,
    }

    if raw_filters.get("dateFrom"):
        parsed["dateFrom"] = _parse_filter_date(str(raw_filters["dateFrom"]), "dateFrom", is_end=False)
    if raw_filters.get("dateTo"):
        parsed["dateTo"] = _parse_filter_date(str(raw_filters["dateTo"]), "dateTo", is_end=True)

    if parsed["dateFrom"] and parsed["dateTo"] and parsed["dateFrom"] > parsed["dateTo"]:
        raise BookkeepingServiceError(400, "dateFrom must be before dateTo")

    tx_type = str(raw_filters.get("type") or "").strip().upper()
    if tx_type:
        if tx_type not in TRANSACTION_TYPES:
            raise BookkeepingServiceError(400, "invalid transaction type filter")
        parsed["type"] = tx_type

    provider = str(raw_filters.get("provider") or "").strip()
    if provider:
        parsed["provider"] = provider

    direction = str(raw_filters.get("direction") or "").strip().upper()
    if direction:
        if direction not in TRANSACTION_DIRECTIONS:
            raise BookkeepingServiceError(400, "invalid direction filter")
        parsed["direction"] = direction

    has_document_raw = str(raw_filters.get("hasDocument") or "").strip().lower()
    if has_document_raw:
        if has_document_raw in {"true", "1", "yes"}:
            parsed["hasDocument"] = True
        elif has_document_raw in {"false", "0", "no"}:
            parsed["hasDocument"] = False
        else:
            raise BookkeepingServiceError(400, "invalid hasDocument filter")

    order_id = str(raw_filters.get("orderId") or "").strip()
    if order_id:
        parsed["orderId"] = _ensure_uuid(order_id, "orderId")

    template_id = str(raw_filters.get("templateId") or "").strip()
    if template_id:
        parsed["templateId"] = _ensure_uuid(template_id, "templateId")

    payment_account_id = str(raw_filters.get("paymentAccountId") or "").strip()
    if payment_account_id:
        parsed["paymentAccountId"] = _ensure_uuid(payment_account_id, "paymentAccountId")

    booking_class = str(raw_filters.get("bookingClass") or "").strip().lower()
    if booking_class:
        if booking_class not in BOOKING_CLASSES:
            raise BookkeepingServiceError(400, "invalid bookingClass filter")
        parsed["bookingClass"] = booking_class

    return parsed


def _fetch_order(connection: sqlite3.Connection, order_id: str) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT o.*, COUNT(t.id) AS tx_count
        FROM orders o
        LEFT JOIN transactions t ON t.order_id = o.id
        WHERE o.id = ?
        GROUP BY o.id
        """,
        (order_id,),
    ).fetchone()
    if row is None:
        return None
    return _order_row_to_dict(row)


def _fetch_payment_account(connection: sqlite3.Connection, payment_account_id: str) -> dict[str, Any] | None:
    row = connection.execute("SELECT * FROM payment_accounts WHERE id = ?", (payment_account_id,)).fetchone()
    if row is None:
        return None
    return _payment_account_row_to_dict(row)


def _fetch_template(connection: sqlite3.Connection, template_id: str) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT
            rt.*,
            pa.id AS payment_account_join_id,
            pa.name AS payment_account_name,
            pa.provider AS payment_account_provider,
            pa.is_active AS payment_account_is_active
        FROM recurring_templates rt
        LEFT JOIN payment_accounts pa ON pa.id = rt.payment_account_id
        WHERE rt.id = ?
        """,
        (template_id,),
    ).fetchone()
    if row is None:
        return None
    return _recurring_template_row_to_dict(row)


def _fetch_transaction_by_id(connection: sqlite3.Connection, transaction_id: str) -> dict[str, Any] | None:
    sql = _transactions_select_sql() + " WHERE t.id = ?"
    row = connection.execute(sql, (transaction_id,)).fetchone()
    if row is None:
        return None
    return _transaction_row_to_dict(row)


def _fetch_document(connection: sqlite3.Connection, document_id: str) -> dict[str, Any] | None:
    row = connection.execute("SELECT * FROM documents WHERE id = ?", (document_id,)).fetchone()
    if row is None:
        return None
    return _document_row_to_dict(row)


def _ensure_order_exists(connection: sqlite3.Connection, order_id: str) -> None:
    row = connection.execute("SELECT id FROM orders WHERE id = ?", (order_id,)).fetchone()
    if row is None:
        raise BookkeepingServiceError(400, "order_id not found")


def _ensure_document_exists(connection: sqlite3.Connection, document_id: str) -> None:
    row = connection.execute("SELECT id FROM documents WHERE id = ?", (document_id,)).fetchone()
    if row is None:
        raise BookkeepingServiceError(400, "document_id not found")


def _ensure_payment_account_exists(connection: sqlite3.Connection, payment_account_id: str) -> None:
    row = connection.execute("SELECT id FROM payment_accounts WHERE id = ?", (payment_account_id,)).fetchone()
    if row is None:
        raise BookkeepingServiceError(400, "payment_account_id not found")


def _ensure_template_exists(connection: sqlite3.Connection, template_id: str) -> None:
    row = connection.execute("SELECT id FROM recurring_templates WHERE id = ?", (template_id,)).fetchone()
    if row is None:
        raise BookkeepingServiceError(400, "template_id not found")


def _parse_transaction_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise BookkeepingServiceError(400, "invalid JSON payload")

    date_raw = _ensure_required_string(payload, "date")
    tx_date = _parse_iso_datetime(date_raw, "date")

    tx_type = _ensure_required_string(payload, "type").upper()
    if tx_type not in TRANSACTION_TYPES:
        raise BookkeepingServiceError(400, "invalid transaction type")

    direction = _ensure_required_string(payload, "direction").upper()
    if direction not in TRANSACTION_DIRECTIONS:
        raise BookkeepingServiceError(400, "invalid direction")

    amount_gross = _parse_positive_int(payload.get("amount_gross"), "amount_gross")
    currency = _normalize_currency(payload.get("currency"), "currency")

    vat_rate = _parse_optional_vat_rate(payload.get("vat_rate"))
    vat_amount_input = payload.get("vat_amount")
    vat_amount = _calculate_vat_amount_cents(amount_gross, vat_rate) if vat_amount_input is None else _parse_nonnegative_int(vat_amount_input, "vat_amount")
    if vat_amount is not None and vat_amount > amount_gross:
        raise BookkeepingServiceError(400, "vat_amount must be <= amount_gross")

    amount_net_input = payload.get("amount_net")
    amount_net = _calculate_amount_net_cents(amount_gross, vat_amount) if amount_net_input is None else _parse_nonnegative_int(amount_net_input, "amount_net")
    if amount_net is not None and amount_net > amount_gross:
        raise BookkeepingServiceError(400, "amount_net must be <= amount_gross")

    provider = _ensure_required_string(payload, "provider", max_length=120)
    counterparty_name = _ensure_optional_string(payload, "counterparty_name", max_length=120)
    if counterparty_name == "":
        counterparty_name = None

    category = _ensure_optional_string(payload, "category", max_length=120)
    if category == "":
        category = None

    reference = _ensure_optional_string(payload, "reference", max_length=120)
    if reference == "":
        reference = None

    order_id = _ensure_uuid(payload.get("order_id"), "order_id") if payload.get("order_id") is not None else None
    document_id = _ensure_uuid(payload.get("document_id"), "document_id") if payload.get("document_id") is not None else None
    template_id = _ensure_uuid(payload.get("template_id"), "template_id") if payload.get("template_id") is not None else None
    payment_account_id = _ensure_uuid(payload.get("payment_account_id"), "payment_account_id") if payload.get("payment_account_id") is not None else None

    if payload.get("period_key") is not None:
        period_key = _parse_period_key(payload.get("period_key"))
    elif template_id is not None:
        period_key = _period_key_from_date(tx_date)
    else:
        period_key = None

    notes = _ensure_optional_string(payload, "notes", max_length=5000)
    if notes == "":
        notes = None

    source = payload.get("source", "manual")
    if not isinstance(source, str):
        raise BookkeepingServiceError(400, "source must be a string")
    source = source.strip().lower()
    if source not in TRANSACTION_SOURCES:
        raise BookkeepingServiceError(400, "invalid source")

    source_key = _ensure_optional_string(payload, "source_key", max_length=190)
    if source_key == "":
        source_key = None

    booking_class_input = payload.get("booking_class", "single")
    if not isinstance(booking_class_input, str):
        raise BookkeepingServiceError(400, "booking_class must be a string")
    booking_class = booking_class_input.strip().lower()
    if booking_class not in BOOKING_CLASSES:
        raise BookkeepingServiceError(400, "invalid booking_class")

    # SUBSCRIPTION transactions are always classified as monthly
    if tx_type == "SUBSCRIPTION":
        booking_class = "monthly"

    status_input = payload.get("status", "pending")
    if status_input is None:
        status: str | None = None
    elif isinstance(status_input, str):
        status = status_input.strip().lower()
        if status not in TRANSACTION_STATUSES:
            raise BookkeepingServiceError(400, "invalid status")
    else:
        raise BookkeepingServiceError(400, "status must be a string")

    return {
        "date": tx_date,
        "type": tx_type,
        "direction": direction,
        "amount_gross": amount_gross,
        "currency": currency,
        "vat_rate": vat_rate,
        "vat_amount": vat_amount,
        "amount_net": amount_net,
        "provider": provider,
        "counterparty_name": counterparty_name,
        "category": category,
        "reference": reference,
        "order_id": order_id,
        "document_id": document_id,
        "template_id": template_id,
        "payment_account_id": payment_account_id,
        "period_key": period_key,
        "notes": notes,
        "source": source,
        "source_key": source_key,
        "booking_class": booking_class,
        "status": status,
    }


def _parse_transaction_patch_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise BookkeepingServiceError(400, "invalid JSON payload")

    allowed = {
        "date",
        "type",
        "direction",
        "amount_gross",
        "currency",
        "vat_rate",
        "vat_amount",
        "amount_net",
        "provider",
        "counterparty_name",
        "category",
        "reference",
        "order_id",
        "document_id",
        "template_id",
        "payment_account_id",
        "period_key",
        "notes",
        "status",
        "booking_class",
    }
    payload_keys = set(payload.keys())
    unknown = payload_keys - allowed
    if unknown:
        raise BookkeepingServiceError(400, f"unsupported fields: {', '.join(sorted(unknown))}")
    if not payload_keys:
        raise BookkeepingServiceError(400, "at least one field is required")

    updates: dict[str, Any] = {}

    if "date" in payload:
        value = payload.get("date")
        if not isinstance(value, str):
            raise BookkeepingServiceError(400, "date must be a string")
        updates["date"] = _parse_iso_datetime(value, "date")

    if "type" in payload:
        value = payload.get("type")
        if not isinstance(value, str):
            raise BookkeepingServiceError(400, "type must be a string")
        normalized = value.strip().upper()
        if normalized not in TRANSACTION_TYPES:
            raise BookkeepingServiceError(400, "invalid transaction type")
        updates["type"] = normalized

    if "direction" in payload:
        value = payload.get("direction")
        if not isinstance(value, str):
            raise BookkeepingServiceError(400, "direction must be a string")
        normalized = value.strip().upper()
        if normalized not in TRANSACTION_DIRECTIONS:
            raise BookkeepingServiceError(400, "invalid direction")
        updates["direction"] = normalized

    if "amount_gross" in payload:
        updates["amount_gross"] = _parse_positive_int(payload.get("amount_gross"), "amount_gross")

    if "currency" in payload:
        updates["currency"] = _normalize_currency(payload.get("currency"), "currency")

    if "vat_rate" in payload:
        updates["vat_rate"] = _parse_optional_vat_rate(payload.get("vat_rate"))

    if "vat_amount" in payload:
        value = payload.get("vat_amount")
        updates["vat_amount"] = None if value is None else _parse_nonnegative_int(value, "vat_amount")

    if "amount_net" in payload:
        value = payload.get("amount_net")
        updates["amount_net"] = None if value is None else _parse_nonnegative_int(value, "amount_net")

    if "provider" in payload:
        value = payload.get("provider")
        if not isinstance(value, str) or not value.strip():
            raise BookkeepingServiceError(400, "provider is required")
        updates["provider"] = value.strip()

    if "counterparty_name" in payload:
        value = payload.get("counterparty_name")
        if value is None:
            updates["counterparty_name"] = None
        elif isinstance(value, str):
            if len(value) > 120:
                raise BookkeepingServiceError(400, "counterparty_name exceeds max length 120")
            updates["counterparty_name"] = value.strip() or None
        else:
            raise BookkeepingServiceError(400, "counterparty_name must be string or null")

    if "category" in payload:
        value = payload.get("category")
        if value is None:
            updates["category"] = None
        elif isinstance(value, str):
            if len(value) > 120:
                raise BookkeepingServiceError(400, "category exceeds max length 120")
            updates["category"] = value.strip() or None
        else:
            raise BookkeepingServiceError(400, "category must be string or null")

    if "notes" in payload:
        value = payload.get("notes")
        if value is None:
            updates["notes"] = None
        elif isinstance(value, str):
            if len(value) > 5000:
                raise BookkeepingServiceError(400, "notes exceeds max length 5000")
            updates["notes"] = value
        else:
            raise BookkeepingServiceError(400, "notes must be string or null")

    if "status" in payload:
        value = payload.get("status")
        if value is None:
            updates["status"] = None
        elif isinstance(value, str):
            normalized = value.strip().lower()
            if normalized not in TRANSACTION_STATUSES:
                raise BookkeepingServiceError(400, "invalid status")
            updates["status"] = normalized
        else:
            raise BookkeepingServiceError(400, "status must be string or null")

    if "reference" in payload:
        value = payload.get("reference")
        if value is None:
            updates["reference"] = None
        elif isinstance(value, str):
            if len(value) > 120:
                raise BookkeepingServiceError(400, "reference exceeds max length 120")
            updates["reference"] = value
        else:
            raise BookkeepingServiceError(400, "reference must be string or null")

    if "order_id" in payload:
        value = payload.get("order_id")
        updates["order_id"] = None if value is None else _ensure_uuid(value, "order_id")

    if "document_id" in payload:
        value = payload.get("document_id")
        updates["document_id"] = None if value is None else _ensure_uuid(value, "document_id")

    if "template_id" in payload:
        value = payload.get("template_id")
        updates["template_id"] = None if value is None else _ensure_uuid(value, "template_id")

    if "payment_account_id" in payload:
        value = payload.get("payment_account_id")
        updates["payment_account_id"] = None if value is None else _ensure_uuid(value, "payment_account_id")

    if "period_key" in payload:
        value = payload.get("period_key")
        updates["period_key"] = None if value is None else _parse_period_key(value)

    if "booking_class" in payload:
        value = payload.get("booking_class")
        if not isinstance(value, str):
            raise BookkeepingServiceError(400, "booking_class must be a string")
        normalized = value.strip().lower()
        if normalized not in BOOKING_CLASSES:
            raise BookkeepingServiceError(400, "invalid booking_class")
        updates["booking_class"] = normalized

    return updates


def _parse_payment_account_payload(payload: Any, *, allow_partial: bool = False) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise BookkeepingServiceError(400, "invalid JSON payload")

    allowed = {"name", "provider", "is_active"}
    unknown = set(payload.keys()) - allowed
    if unknown:
        raise BookkeepingServiceError(400, f"unsupported fields: {', '.join(sorted(unknown))}")
    if allow_partial and not payload:
        raise BookkeepingServiceError(400, "at least one field is required")

    parsed: dict[str, Any] = {}
    if "name" in payload or not allow_partial:
        parsed["name"] = _ensure_required_string(payload, "name", max_length=120)
    if "provider" in payload or not allow_partial:
        provider = _ensure_optional_string(payload, "provider", max_length=80, nullable=True)
        parsed["provider"] = None if provider in {None, ""} else provider
    if "is_active" in payload:
        is_active = _parse_optional_bool(payload.get("is_active"), "is_active")
        if is_active is None:
            raise BookkeepingServiceError(400, "is_active cannot be null")
        parsed["is_active"] = is_active
    elif not allow_partial:
        parsed["is_active"] = True
    return parsed


def _parse_template_payload(payload: Any, *, allow_partial: bool = False) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise BookkeepingServiceError(400, "invalid JSON payload")

    allowed = {
        "name",
        "type",
        "direction",
        "default_amount_gross",
        "currency",
        "provider",
        "counterparty_name",
        "category",
        "vat_rate",
        "payment_account_id",
        "schedule",
        "day_of_month",
        "start_date",
        "active",
        "notes_default",
    }
    unknown = set(payload.keys()) - allowed
    if unknown:
        raise BookkeepingServiceError(400, f"unsupported fields: {', '.join(sorted(unknown))}")
    if allow_partial and not payload:
        raise BookkeepingServiceError(400, "at least one field is required")

    parsed: dict[str, Any] = {}
    if "name" in payload or not allow_partial:
        parsed["name"] = _ensure_required_string(payload, "name", max_length=140)

    if "type" in payload or not allow_partial:
        tx_type = _ensure_required_string(payload, "type").upper()
        if tx_type not in TRANSACTION_TYPES:
            raise BookkeepingServiceError(400, "invalid transaction type")
        parsed["type"] = tx_type

    if "direction" in payload or not allow_partial:
        direction = _ensure_required_string(payload, "direction").upper()
        if direction not in TRANSACTION_DIRECTIONS:
            raise BookkeepingServiceError(400, "invalid direction")
        parsed["direction"] = direction

    if "default_amount_gross" in payload:
        value = payload.get("default_amount_gross")
        parsed["default_amount_gross"] = None if value is None else _parse_positive_int(value, "default_amount_gross")
    elif not allow_partial:
        parsed["default_amount_gross"] = None

    if "currency" in payload or not allow_partial:
        parsed["currency"] = _normalize_currency(payload.get("currency"), "currency")

    if "provider" in payload or not allow_partial:
        parsed["provider"] = _ensure_required_string(payload, "provider", max_length=120)

    if "counterparty_name" in payload or not allow_partial:
        counterparty_name = _ensure_optional_string(payload, "counterparty_name", max_length=120, nullable=True)
        parsed["counterparty_name"] = None if counterparty_name in {None, ""} else counterparty_name

    if "category" in payload or not allow_partial:
        category = _ensure_optional_string(payload, "category", max_length=120, nullable=True)
        parsed["category"] = None if category in {None, ""} else category

    if "vat_rate" in payload or not allow_partial:
        parsed["vat_rate"] = _parse_optional_vat_rate(payload.get("vat_rate"))

    if "payment_account_id" in payload:
        value = payload.get("payment_account_id")
        parsed["payment_account_id"] = None if value is None else _ensure_uuid(value, "payment_account_id")
    elif not allow_partial:
        parsed["payment_account_id"] = None

    if "schedule" in payload or not allow_partial:
        schedule = _ensure_required_string(payload, "schedule").lower()
        if schedule not in RECURRING_SCHEDULES:
            raise BookkeepingServiceError(400, "invalid schedule")
        parsed["schedule"] = schedule

    if "day_of_month" in payload:
        value = payload.get("day_of_month")
        if value is None:
            parsed["day_of_month"] = None
        else:
            day_value = _parse_positive_int(value, "day_of_month")
            if day_value > 31:
                raise BookkeepingServiceError(400, "day_of_month must be between 1 and 31")
            parsed["day_of_month"] = day_value
    elif not allow_partial:
        parsed["day_of_month"] = None

    if "start_date" in payload or not allow_partial:
        parsed["start_date"] = _parse_template_start_date(payload.get("start_date"))

    if not allow_partial and parsed.get("day_of_month") is None:
        parsed["day_of_month"] = _day_of_month_from_start_date(parsed.get("start_date"))

    if "active" in payload:
        active = _parse_optional_bool(payload.get("active"), "active")
        if active is None:
            raise BookkeepingServiceError(400, "active cannot be null")
        parsed["active"] = active
    elif not allow_partial:
        parsed["active"] = True

    if "notes_default" in payload or not allow_partial:
        notes_default = _ensure_optional_string(payload, "notes_default", max_length=5000, nullable=True)
        parsed["notes_default"] = None if notes_default in {None, ""} else notes_default

    return parsed


def _parse_template_generate_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise BookkeepingServiceError(400, "invalid JSON payload")

    period_key_input = payload.get("period_key")
    date_input = payload.get("date")

    parsed_date: str | None = None
    if date_input is not None:
        if not isinstance(date_input, str):
            raise BookkeepingServiceError(400, "date must be a string")
        parsed_date = _parse_iso_datetime(date_input, "date")

    if period_key_input is not None:
        period_key = _parse_period_key(period_key_input)
    elif parsed_date is not None:
        period_key = _period_key_from_date(parsed_date)
    else:
        period_key = datetime.now(timezone.utc).strftime("%Y-%m")

    status_input = payload.get("status", "pending")
    if not isinstance(status_input, str):
        raise BookkeepingServiceError(400, "status must be a string")
    status = status_input.strip().lower()
    if status not in TRANSACTION_STATUSES:
        raise BookkeepingServiceError(400, "invalid status")

    return {"period_key": period_key, "date": parsed_date, "status": status}


def _create_transaction_from_template(
    connection: sqlite3.Connection,
    *,
    template_id: str,
    period_key: str,
    date_override: str | None,
    status: str,
) -> dict[str, Any]:
    template = _fetch_template(connection, template_id)
    if template is None:
        raise BookkeepingServiceError(404, "template not found")

    amount_gross = template["default_amount_gross"]
    if amount_gross is None:
        raise BookkeepingServiceError(400, "template has no default_amount_gross")

    template_start_date = template.get("start_date")
    if template_start_date:
        template_start_period = _period_key_from_date(str(template_start_date))
        if period_key < template_start_period:
            raise BookkeepingServiceError(
                400,
                f"period_key is before template start_date ({template_start_date})",
            )

    vat_rate = float(template["vat_rate"]) if template["vat_rate"] is not None else None
    vat_amount = _calculate_vat_amount_cents(int(amount_gross), vat_rate)
    amount_net = _calculate_amount_net_cents(int(amount_gross), vat_amount)
    template_day_of_month = template["day_of_month"]
    if template_day_of_month is None:
        template_day_of_month = _day_of_month_from_start_date(
            str(template_start_date) if template_start_date else None
        )
    tx_date = date_override or _date_from_period_key(period_key, template_day_of_month)

    transaction_id = str(uuid.uuid4())
    timestamp = _now_iso()
    source_key = f"template-{template_id[:8]}-{period_key}"

    try:
        connection.execute(
            """
            INSERT INTO transactions (
                id, date, type, direction, amount_gross, currency,
                vat_rate, vat_amount, amount_net, provider, counterparty_name, category,
                reference, notes, order_id, document_id, template_id, payment_account_id, period_key,
                source, source_key, status, booking_class, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'monthly', ?, ?)
            """,
            (
                transaction_id,
                tx_date,
                template["type"],
                template["direction"],
                int(amount_gross),
                template["currency"],
                vat_rate,
                vat_amount,
                amount_net,
                template["provider"],
                template["counterparty_name"],
                template["category"],
                f"{template['name']} {period_key}",
                template["notes_default"],
                None,
                None,
                template_id,
                template["payment_account_id"],
                period_key,
                "manual",
                source_key,
                status,
                timestamp,
                timestamp,
            ),
        )
        connection.commit()
    except sqlite3.IntegrityError as error:
        if "uq_transactions_template_period" in str(error) or "UNIQUE constraint failed: transactions.template_id, transactions.period_key" in str(error):
            raise BookkeepingServiceError(409, "transaction already exists for this template and period") from error
        if "UNIQUE" in str(error).upper():
            raise BookkeepingServiceError(409, "unique constraint violated", str(error)) from error
        raise

    transaction = _fetch_transaction_by_id(connection, transaction_id)
    if transaction is None:
        raise BookkeepingServiceError(500, "transaction could not be loaded after creation")
    return transaction


def list_bookkeeping_orders() -> dict[str, Any]:
    if not BOOKKEEPING_DB_PATH.exists():
        return {"total": 0, "items": [], "db_available": False}

    with _connect_bookkeeping_db() as connection:
        rows = connection.execute(
            """
            SELECT o.*, COUNT(t.id) AS tx_count
            FROM orders o
            LEFT JOIN transactions t ON t.order_id = o.id
            GROUP BY o.id
            ORDER BY o.order_date DESC
            """
        ).fetchall()

    items = [_order_row_to_dict(row) for row in rows]
    return {"total": len(items), "items": items, "db_available": True}


def list_bookkeeping_transactions(filters: dict[str, str | None]) -> dict[str, Any]:
    if not BOOKKEEPING_DB_PATH.exists():
        return {"total": 0, "items": [], "db_available": False}

    parsed_filters = _parse_transaction_filters(filters)
    clauses: list[str] = []
    args: list[Any] = []

    if parsed_filters.get("dateFrom"):
        clauses.append("t.date >= ?")
        args.append(parsed_filters["dateFrom"])
    if parsed_filters.get("dateTo"):
        clauses.append("t.date <= ?")
        args.append(parsed_filters["dateTo"])
    if parsed_filters.get("type"):
        clauses.append("t.type = ?")
        args.append(parsed_filters["type"])
    if parsed_filters.get("provider"):
        clauses.append("t.provider = ?")
        args.append(parsed_filters["provider"])
    if parsed_filters.get("direction"):
        clauses.append("t.direction = ?")
        args.append(parsed_filters["direction"])
    if parsed_filters.get("orderId"):
        clauses.append("t.order_id = ?")
        args.append(parsed_filters["orderId"])
    if parsed_filters.get("templateId"):
        clauses.append("t.template_id = ?")
        args.append(parsed_filters["templateId"])
    if parsed_filters.get("paymentAccountId"):
        clauses.append("t.payment_account_id = ?")
        args.append(parsed_filters["paymentAccountId"])
    if parsed_filters.get("hasDocument") is True:
        clauses.append("t.document_id IS NOT NULL")
    if parsed_filters.get("hasDocument") is False:
        clauses.append("t.document_id IS NULL")
    if parsed_filters.get("bookingClass"):
        clauses.append("t.booking_class = ?")
        args.append(parsed_filters["bookingClass"])

    sql = _transactions_select_sql()
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY t.date DESC, t.id DESC"

    with _connect_bookkeeping_db() as connection:
        rows = connection.execute(sql, tuple(args)).fetchall()

    items = [_transaction_row_to_dict(row) for row in rows]
    return {"total": len(items), "items": items, "db_available": True}


def create_bookkeeping_transaction(payload: Any) -> dict[str, Any]:
    _ensure_db_available()
    parsed = _parse_transaction_payload(payload)

    with _connect_bookkeeping_db() as connection:
        if parsed["order_id"] is not None:
            _ensure_order_exists(connection, str(parsed["order_id"]))
        if parsed["document_id"] is not None:
            _ensure_document_exists(connection, str(parsed["document_id"]))
        if parsed["template_id"] is not None:
            _ensure_template_exists(connection, str(parsed["template_id"]))
        if parsed["payment_account_id"] is not None:
            _ensure_payment_account_exists(connection, str(parsed["payment_account_id"]))

        transaction_id = str(uuid.uuid4())
        timestamp = _now_iso()
        try:
            connection.execute(
                """
                INSERT INTO transactions (
                    id, date, type, direction, amount_gross, currency,
                    vat_rate, vat_amount, amount_net, provider, counterparty_name, category,
                    reference, notes, order_id, document_id, template_id, payment_account_id, period_key,
                    source, source_key, status, booking_class, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    transaction_id,
                    parsed["date"],
                    parsed["type"],
                    parsed["direction"],
                    parsed["amount_gross"],
                    parsed["currency"],
                    parsed["vat_rate"],
                    parsed["vat_amount"],
                    parsed["amount_net"],
                    parsed["provider"],
                    parsed["counterparty_name"],
                    parsed["category"],
                    parsed["reference"],
                    parsed["notes"],
                    parsed["order_id"],
                    parsed["document_id"],
                    parsed["template_id"],
                    parsed["payment_account_id"],
                    parsed["period_key"],
                    parsed["source"],
                    parsed["source_key"],
                    parsed["status"],
                    parsed["booking_class"],
                    timestamp,
                    timestamp,
                ),
            )
            connection.commit()
        except sqlite3.IntegrityError as error:
            if "uq_transactions_template_period" in str(error) or "UNIQUE constraint failed: transactions.template_id, transactions.period_key" in str(error):
                raise BookkeepingServiceError(409, "transaction already exists for this template and period") from error
            if "UNIQUE" in str(error).upper():
                raise BookkeepingServiceError(409, "unique constraint violated", str(error)) from error
            raise

        created = _fetch_transaction_by_id(connection, transaction_id)
    if created is None:
        raise BookkeepingServiceError(500, "transaction could not be loaded after creation")
    return created


def update_bookkeeping_transaction(transaction_id: str, payload: Any) -> dict[str, Any]:
    _ensure_db_available()
    tx_id = _ensure_uuid(transaction_id, "transaction_id")
    updates = _parse_transaction_patch_payload(payload)

    with _connect_bookkeeping_db() as connection:
        existing = connection.execute(
            """
            SELECT id, date, amount_gross, vat_rate, vat_amount, amount_net, template_id, type
            FROM transactions
            WHERE id = ?
            """,
            (tx_id,),
        ).fetchone()
        if existing is None:
            raise BookkeepingServiceError(404, "transaction not found")

        if "document_id" in updates and updates["document_id"] is not None:
            _ensure_document_exists(connection, str(updates["document_id"]))
        if "order_id" in updates and updates["order_id"] is not None:
            _ensure_order_exists(connection, str(updates["order_id"]))
        if "template_id" in updates and updates["template_id"] is not None:
            _ensure_template_exists(connection, str(updates["template_id"]))
        if "payment_account_id" in updates and updates["payment_account_id"] is not None:
            _ensure_payment_account_exists(connection, str(updates["payment_account_id"]))

        effective_amount_gross = int(updates.get("amount_gross", existing["amount_gross"]))

        if "vat_rate" in updates:
            effective_vat_rate: float | None = updates["vat_rate"]
        elif existing["vat_rate"] is None:
            effective_vat_rate = None
        else:
            effective_vat_rate = float(existing["vat_rate"])

        if "vat_amount" not in updates and "vat_rate" in updates:
            updates["vat_amount"] = _calculate_vat_amount_cents(effective_amount_gross, effective_vat_rate)

        effective_vat_amount = updates.get("vat_amount", existing["vat_amount"])
        if effective_vat_amount is not None and int(effective_vat_amount) > effective_amount_gross:
            raise BookkeepingServiceError(400, "vat_amount must be <= amount_gross")

        if "amount_net" not in updates and (
            "amount_gross" in updates or "vat_amount" in updates or "vat_rate" in updates
        ):
            updates["amount_net"] = _calculate_amount_net_cents(
                effective_amount_gross,
                None if effective_vat_amount is None else int(effective_vat_amount),
            )

        effective_amount_net = updates.get("amount_net", existing["amount_net"])
        if effective_amount_net is not None and int(effective_amount_net) > effective_amount_gross:
            raise BookkeepingServiceError(400, "amount_net must be <= amount_gross")

        if updates.get("template_id", existing["template_id"]) is not None and "period_key" not in updates:
            effective_date = str(updates.get("date", existing["date"]))
            updates["period_key"] = _period_key_from_date(effective_date)

        if "template_id" in updates and updates["template_id"] is None and "period_key" not in updates:
            updates["period_key"] = None

        # SUBSCRIPTION transactions are always classified as monthly
        effective_type = updates.get("type", existing["type"])
        if effective_type == "SUBSCRIPTION":
            updates["booking_class"] = "monthly"

        set_clauses: list[str] = []
        args: list[Any] = []
        for key in [
            "date",
            "type",
            "direction",
            "amount_gross",
            "currency",
            "vat_rate",
            "vat_amount",
            "amount_net",
            "provider",
            "counterparty_name",
            "category",
            "reference",
            "order_id",
            "document_id",
            "template_id",
            "payment_account_id",
            "period_key",
            "notes",
            "status",
            "booking_class",
        ]:
            if key in updates:
                set_clauses.append(f"{key} = ?")
                args.append(updates[key])

        set_clauses.append("updated_at = ?")
        args.append(_now_iso())
        args.append(tx_id)

        try:
            connection.execute(
                f"UPDATE transactions SET {', '.join(set_clauses)} WHERE id = ?",
                tuple(args),
            )
            connection.commit()
        except sqlite3.IntegrityError as error:
            if "uq_transactions_template_period" in str(error) or "UNIQUE constraint failed: transactions.template_id, transactions.period_key" in str(error):
                raise BookkeepingServiceError(409, "transaction already exists for this template and period") from error
            if "UNIQUE" in str(error).upper():
                raise BookkeepingServiceError(409, "unique constraint violated", str(error)) from error
            raise

        updated = _fetch_transaction_by_id(connection, tx_id)

    if updated is None:
        raise BookkeepingServiceError(500, "transaction could not be loaded after update")
    return updated


def get_bookkeeping_transaction(transaction_id: str) -> dict[str, Any]:
    _ensure_db_available()
    tx_id = _ensure_uuid(transaction_id, "transaction_id")

    with _connect_bookkeeping_db() as connection:
        transaction = _fetch_transaction_by_id(connection, tx_id)

    if transaction is None:
        raise BookkeepingServiceError(404, "transaction not found")
    return transaction


def delete_bookkeeping_transaction(transaction_id: str) -> dict[str, Any]:
    _ensure_db_available()
    tx_id = _ensure_uuid(transaction_id, "transaction_id")

    with _connect_bookkeeping_db() as connection:
        existing = connection.execute(
            "SELECT id, document_id, source, source_key FROM transactions WHERE id = ?",
            (tx_id,),
        ).fetchone()
        if existing is None:
            raise BookkeepingServiceError(404, "transaction not found")

        connection.execute("DELETE FROM transactions WHERE id = ?", (tx_id,))
        connection.commit()

    return {
        "id": tx_id,
        "document_id": existing["document_id"],
        "source": existing["source"],
        "source_key": existing["source_key"],
    }


def list_payment_accounts() -> dict[str, Any]:
    if not BOOKKEEPING_DB_PATH.exists():
        return {"total": 0, "items": [], "db_available": False}

    with _connect_bookkeeping_db() as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM payment_accounts
            ORDER BY is_active DESC, name ASC
            """
        ).fetchall()

    items = [_payment_account_row_to_dict(row) for row in rows]
    return {"total": len(items), "items": items, "db_available": True}


def create_payment_account(payload: Any) -> dict[str, Any]:
    _ensure_db_available()
    parsed = _parse_payment_account_payload(payload, allow_partial=False)

    with _connect_bookkeeping_db() as connection:
        payment_account_id = str(uuid.uuid4())
        timestamp = _now_iso()
        try:
            connection.execute(
                """
                INSERT INTO payment_accounts (id, name, provider, is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    payment_account_id,
                    parsed["name"],
                    parsed["provider"],
                    1 if parsed["is_active"] else 0,
                    timestamp,
                    timestamp,
                ),
            )
            connection.commit()
        except sqlite3.IntegrityError as error:
            if "UNIQUE" in str(error).upper():
                raise BookkeepingServiceError(409, "unique constraint violated", str(error)) from error
            raise

        created = _fetch_payment_account(connection, payment_account_id)

    if created is None:
        raise BookkeepingServiceError(500, "payment account could not be loaded after creation")
    return created


def update_payment_account(payment_account_id: str, payload: Any) -> dict[str, Any]:
    _ensure_db_available()
    account_id = _ensure_uuid(payment_account_id, "payment_account_id")
    updates = _parse_payment_account_payload(payload, allow_partial=True)

    with _connect_bookkeeping_db() as connection:
        existing = connection.execute("SELECT id FROM payment_accounts WHERE id = ?", (account_id,)).fetchone()
        if existing is None:
            raise BookkeepingServiceError(404, "payment account not found")

        set_clauses: list[str] = []
        args: list[Any] = []
        for key in ["name", "provider"]:
            if key in updates:
                set_clauses.append(f"{key} = ?")
                args.append(updates[key])
        if "is_active" in updates:
            set_clauses.append("is_active = ?")
            args.append(1 if updates["is_active"] else 0)

        set_clauses.append("updated_at = ?")
        args.append(_now_iso())
        args.append(account_id)

        try:
            connection.execute(
                f"UPDATE payment_accounts SET {', '.join(set_clauses)} WHERE id = ?",
                tuple(args),
            )
            connection.commit()
        except sqlite3.IntegrityError as error:
            if "UNIQUE" in str(error).upper():
                raise BookkeepingServiceError(409, "unique constraint violated", str(error)) from error
            raise

        updated = _fetch_payment_account(connection, account_id)

    if updated is None:
        raise BookkeepingServiceError(500, "payment account could not be loaded after update")
    return updated


def list_templates() -> dict[str, Any]:
    if not BOOKKEEPING_DB_PATH.exists():
        return {"total": 0, "items": [], "db_available": False}

    with _connect_bookkeeping_db() as connection:
        rows = connection.execute(
            """
            SELECT
                rt.*,
                pa.id AS payment_account_join_id,
                pa.name AS payment_account_name,
                pa.provider AS payment_account_provider,
                pa.is_active AS payment_account_is_active
            FROM recurring_templates rt
            LEFT JOIN payment_accounts pa ON pa.id = rt.payment_account_id
            ORDER BY rt.active DESC, rt.name ASC
            """
        ).fetchall()

    items = [_recurring_template_row_to_dict(row) for row in rows]
    return {"total": len(items), "items": items, "db_available": True}


def create_template(payload: Any) -> dict[str, Any]:
    _ensure_db_available()
    parsed = _parse_template_payload(payload, allow_partial=False)

    with _connect_bookkeeping_db() as connection:
        if parsed["payment_account_id"] is not None:
            _ensure_payment_account_exists(connection, str(parsed["payment_account_id"]))

        template_id = str(uuid.uuid4())
        timestamp = _now_iso()
        try:
            connection.execute(
                """
                INSERT INTO recurring_templates (
                    id, name, type, direction, default_amount_gross, currency, provider,
                    counterparty_name, category, vat_rate, payment_account_id, schedule,
                    day_of_month, start_date, active, notes_default, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    template_id,
                    parsed["name"],
                    parsed["type"],
                    parsed["direction"],
                    parsed["default_amount_gross"],
                    parsed["currency"],
                    parsed["provider"],
                    parsed["counterparty_name"],
                    parsed["category"],
                    parsed["vat_rate"],
                    parsed["payment_account_id"],
                    parsed["schedule"],
                    parsed["day_of_month"],
                    parsed["start_date"],
                    1 if parsed["active"] else 0,
                    parsed["notes_default"],
                    timestamp,
                    timestamp,
                ),
            )
            connection.commit()
        except sqlite3.IntegrityError as error:
            if "UNIQUE" in str(error).upper():
                raise BookkeepingServiceError(409, "unique constraint violated", str(error)) from error
            raise

        created = _fetch_template(connection, template_id)

    if created is None:
        raise BookkeepingServiceError(500, "template could not be loaded after creation")
    return created


def update_template(template_id: str, payload: Any) -> dict[str, Any]:
    _ensure_db_available()
    tpl_id = _ensure_uuid(template_id, "template_id")
    updates = _parse_template_payload(payload, allow_partial=True)

    with _connect_bookkeeping_db() as connection:
        if "payment_account_id" in updates and updates["payment_account_id"] is not None:
            _ensure_payment_account_exists(connection, str(updates["payment_account_id"]))

        existing = connection.execute("SELECT id FROM recurring_templates WHERE id = ?", (tpl_id,)).fetchone()
        if existing is None:
            raise BookkeepingServiceError(404, "template not found")

        set_clauses: list[str] = []
        args: list[Any] = []
        for key in [
            "name",
            "type",
            "direction",
            "default_amount_gross",
            "currency",
            "provider",
            "counterparty_name",
            "category",
            "vat_rate",
            "payment_account_id",
            "schedule",
            "day_of_month",
            "start_date",
            "notes_default",
        ]:
            if key in updates:
                set_clauses.append(f"{key} = ?")
                args.append(updates[key])

        if "active" in updates:
            set_clauses.append("active = ?")
            args.append(1 if updates["active"] else 0)

        set_clauses.append("updated_at = ?")
        args.append(_now_iso())
        args.append(tpl_id)

        try:
            connection.execute(
                f"UPDATE recurring_templates SET {', '.join(set_clauses)} WHERE id = ?",
                tuple(args),
            )
            connection.commit()
        except sqlite3.IntegrityError as error:
            if "UNIQUE" in str(error).upper():
                raise BookkeepingServiceError(409, "unique constraint violated", str(error)) from error
            raise

        updated = _fetch_template(connection, tpl_id)

    if updated is None:
        raise BookkeepingServiceError(500, "template could not be loaded after update")
    return updated


def generate_template_transaction(template_id: str, payload: Any) -> dict[str, Any]:
    _ensure_db_available()
    tpl_id = _ensure_uuid(template_id, "template_id")
    parsed = _parse_template_generate_payload(payload)
    with _connect_bookkeeping_db() as connection:
        return _create_transaction_from_template(
            connection,
            template_id=tpl_id,
            period_key=parsed["period_key"],
            date_override=parsed["date"],
            status=parsed["status"],
        )


def list_documents() -> dict[str, Any]:
    if not BOOKKEEPING_DB_PATH.exists():
        return {"total": 0, "items": [], "db_available": False}

    with _connect_bookkeeping_db() as connection:
        rows = connection.execute(
            """
            SELECT d.*, COUNT(t.id) AS tx_count
            FROM documents d
            LEFT JOIN transactions t ON t.document_id = d.id
            GROUP BY d.id
            ORDER BY d.uploaded_at DESC
            """
        ).fetchall()

    items = [_document_row_to_dict(row) for row in rows]
    return {"total": len(items), "items": items, "db_available": True}


def upload_document(
    *,
    filename: str,
    content: bytes,
    mime_type: Optional[str],
    notes: Optional[str] = None,
    transaction_id: Optional[str] = None,
    provider: Optional[str] = None,
    transaction_type: Optional[str] = None,
    booking_date: Optional[str] = None,
    amount_cents: Optional[int] = None,
    currency: Optional[str] = None,
) -> dict[str, Any]:
    _ensure_db_available()

    original_filename = Path(str(filename or "")).name
    if not original_filename:
        raise BookkeepingServiceError(400, "missing file name")
    if not content:
        raise BookkeepingServiceError(400, "uploaded file is empty")
    if len(content) > MAX_UPLOAD_BYTES:
        raise BookkeepingServiceError(413, "file too large")

    clean_notes = str(notes or "").strip() or None
    transaction_id_hint = _ensure_uuid(transaction_id, "transaction_id") if transaction_id else None
    amount_hint = int(amount_cents) if isinstance(amount_cents, int) and amount_cents > 0 else None
    currency_hint = str(currency or "").strip().upper() or None
    provider_hint = str(provider or "").strip() or None
    tx_type_hint = str(transaction_type or "").strip() or None
    booking_date_hint = str(booking_date or "").strip() or None

    with _connect_bookkeeping_db() as connection:
        linked_transaction: dict[str, Any] | None = None
        if transaction_id_hint is not None:
            linked_transaction = _fetch_transaction_by_id(connection, transaction_id_hint)
            if linked_transaction is None:
                raise BookkeepingServiceError(400, "transaction_id does not exist")

        document_id = str(uuid.uuid4())
        provider_or_counterparty = provider_hint
        document_type = tx_type_hint or "DOCUMENT"
        amount_gross = amount_hint
        booking_date_value = booking_date_hint
        currency_value = currency_hint or "EUR"

        if linked_transaction is not None:
            provider_or_counterparty = (
                linked_transaction.get("counterparty_name")
                or linked_transaction.get("provider")
                or provider_hint
            )
            document_type = str(linked_transaction.get("type") or "DOCUMENT")
            amount_gross = int(linked_transaction.get("amount_gross") or 0) or amount_hint
            booking_date_value = str(linked_transaction.get("date") or booking_date_hint or "")
            currency_value = str(linked_transaction.get("currency") or currency_value)

        stored_name = _build_document_storage_name(
            document_id=document_id,
            original_filename=original_filename,
            provider_or_counterparty=provider_or_counterparty,
            transaction_type=document_type,
            amount_gross=amount_gross,
            currency=currency_value,
            booking_date=booking_date_value,
            transaction_id=transaction_id_hint,
        )

        BOOKKEEPING_DOCUMENTS_DIR.mkdir(parents=True, exist_ok=True)
        target_path = BOOKKEEPING_DOCUMENTS_DIR / stored_name
        if target_path.exists():
            collision_suffix = uuid.uuid4().hex[:6]
            target_path = BOOKKEEPING_DOCUMENTS_DIR / f"{target_path.stem}-{collision_suffix}{target_path.suffix}"
            stored_name = target_path.name

        target_path.write_bytes(content)

        uploaded_at = _now_iso()
        safe_mime = mime_type or mimetypes.guess_type(original_filename)[0] or "application/octet-stream"

        connection.execute(
            """
            INSERT INTO documents (id, original_filename, stored_filename, file_path, mime_type, uploaded_at, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                document_id,
                _build_document_display_name(
                    original_filename=original_filename,
                    provider_or_counterparty=provider_or_counterparty,
                    transaction_type=document_type,
                ),
                stored_name,
                _to_relative_project_path(target_path),
                safe_mime,
                uploaded_at,
                clean_notes,
            ),
        )

        if linked_transaction is not None:
            connection.execute(
                """
                UPDATE transactions
                SET document_id = ?, updated_at = ?
                WHERE id = ?
                """,
                (document_id, _now_iso(), linked_transaction["id"]),
            )

        connection.commit()
        created = _fetch_document(connection, document_id)

    if created is None:
        raise BookkeepingServiceError(500, "document could not be loaded after upload")
    return created


def get_document_resolved_path(file_path: str) -> Path:
    return _resolve_project_path(file_path)


# ---------------------------------------------------------------------------
# Monthly Invoice (Sammelrechnung) CRUD
# ---------------------------------------------------------------------------

SAMMELRECHNUNG_PROVIDERS = {"paypal", "shopify_payments", "kaufland", "google_ads", "ebay"}


def _monthly_invoice_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": row["id"],
        "provider": row["provider"],
        "period_from": row["period_from"],
        "period_to": row["period_to"],
        "invoice_amount_cents": row["invoice_amount_cents"],
        "currency": row["currency"],
        "calculated_sum_cents": row["calculated_sum_cents"],
        "difference_cents": row["difference_cents"],
        "document_id": row["document_id"],
        "notes": row["notes"],
        "status": row["status"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }
    # Include document info if joined
    if "document_original_filename" in row.keys():
        if row["document_original_filename"] is not None:
            payload["document"] = {
                "id": row["document_id"],
                "original_filename": row["document_original_filename"],
                "mime_type": row["document_mime_type"],
            }
        else:
            payload["document"] = None
    return payload


def sum_automatic_transactions(
    *,
    provider: str,
    period_from: str,
    period_to: str,
) -> dict[str, Any]:
    """Sum all automatic transactions for a given provider within a date range."""
    _ensure_db_available()

    provider_clean = str(provider or "").strip().lower()
    if not provider_clean:
        raise BookkeepingServiceError(400, "provider is required")

    date_from = _parse_filter_date(period_from, "period_from", is_end=False)
    date_to = _parse_filter_date(period_to, "period_to", is_end=True)

    with _connect_bookkeeping_db() as connection:
        row = connection.execute(
            """
            SELECT
                COALESCE(SUM(t.amount_gross), 0) AS total_cents,
                COUNT(*) AS tx_count
            FROM transactions t
            WHERE t.booking_class = 'automatic'
              AND t.provider = ?
              AND t.direction = 'OUT'
              AND t.date >= ?
              AND t.date <= ?
            """,
            (provider_clean, date_from, date_to),
        ).fetchone()

        tx_rows = connection.execute(
            """
            SELECT
                t.id,
                t.date,
                t.type,
                t.amount_gross,
                t.counterparty_name,
                t.reference,
                t.notes
            FROM transactions t
            WHERE t.booking_class = 'automatic'
              AND t.provider = ?
              AND t.direction = 'OUT'
              AND t.date >= ?
              AND t.date <= ?
            ORDER BY t.date ASC
            """,
            (provider_clean, date_from, date_to),
        ).fetchall()

    return {
        "provider": provider_clean,
        "period_from": period_from,
        "period_to": period_to,
        "total_cents": int(row["total_cents"]) if row else 0,
        "transaction_count": int(row["tx_count"]) if row else 0,
        "transactions": [
            {
                "id": tx["id"],
                "date": tx["date"],
                "type": tx["type"],
                "amount_gross": tx["amount_gross"],
                "counterparty_name": tx["counterparty_name"],
                "reference": tx["reference"],
                "notes": tx["notes"],
            }
            for tx in tx_rows
        ],
    }


def list_monthly_invoices() -> dict[str, Any]:
    """List all monthly invoices (Sammelrechnungen)."""
    if not BOOKKEEPING_DB_PATH.exists():
        return {"total": 0, "items": [], "db_available": False}

    with _connect_bookkeeping_db() as connection:
        rows = connection.execute(
            """
            SELECT
                mi.*,
                d.original_filename AS document_original_filename,
                d.mime_type AS document_mime_type
            FROM monthly_invoices mi
            LEFT JOIN documents d ON d.id = mi.document_id
            ORDER BY mi.period_to DESC, mi.created_at DESC
            """
        ).fetchall()

    items = [_monthly_invoice_row_to_dict(row) for row in rows]
    return {"total": len(items), "items": items, "db_available": True}


def get_monthly_invoice(invoice_id: str) -> dict[str, Any]:
    """Get a single monthly invoice by ID, including linked transactions."""
    _ensure_db_available()
    inv_id = _ensure_uuid(invoice_id, "invoice_id")

    with _connect_bookkeeping_db() as connection:
        row = connection.execute(
            """
            SELECT
                mi.*,
                d.original_filename AS document_original_filename,
                d.mime_type AS document_mime_type
            FROM monthly_invoices mi
            LEFT JOIN documents d ON d.id = mi.document_id
            WHERE mi.id = ?
            """,
            (inv_id,),
        ).fetchone()

        if row is None:
            raise BookkeepingServiceError(404, "monthly invoice not found")

        # Fetch linked transactions via junction table
        tx_rows = connection.execute(
            """
            SELECT
                t.id,
                t.date,
                t.type,
                t.direction,
                t.amount_gross,
                t.currency,
                t.provider,
                t.counterparty_name,
                t.category,
                t.reference,
                t.notes,
                t.order_id,
                t.source,
                t.status,
                t.booking_class
            FROM transactions t
            JOIN monthly_invoice_transactions mit ON mit.transaction_id = t.id
            WHERE mit.invoice_id = ?
            ORDER BY t.date ASC, t.created_at ASC
            """,
            (inv_id,),
        ).fetchall()

    result = _monthly_invoice_row_to_dict(row)
    result["transactions"] = [
        {
            "id": tx["id"],
            "date": tx["date"],
            "type": tx["type"],
            "direction": tx["direction"],
            "amount_gross": tx["amount_gross"],
            "currency": tx["currency"],
            "provider": tx["provider"],
            "counterparty_name": tx["counterparty_name"],
            "category": tx["category"],
            "reference": tx["reference"],
            "notes": tx["notes"],
            "order_id": tx["order_id"],
            "source": tx["source"],
            "status": tx["status"],
            "booking_class": tx["booking_class"],
        }
        for tx in tx_rows
    ]
    return result


def create_monthly_invoice(payload: Any) -> dict[str, Any]:
    """Create a new monthly invoice and auto-reconcile against automatic transactions."""
    _ensure_db_available()
    if not isinstance(payload, dict):
        raise BookkeepingServiceError(400, "invalid JSON payload")

    provider = _ensure_required_string(payload, "provider", max_length=80).lower()
    if provider not in SAMMELRECHNUNG_PROVIDERS:
        raise BookkeepingServiceError(400, f"provider must be one of: {', '.join(sorted(SAMMELRECHNUNG_PROVIDERS))}")

    period_from = _parse_filter_date(payload.get("period_from", ""), "period_from", is_end=False)
    period_to = _parse_filter_date(payload.get("period_to", ""), "period_to", is_end=True)
    if period_from >= period_to:
        raise BookkeepingServiceError(400, "period_from must be before period_to")
    invoice_amount_cents = _parse_positive_int(payload.get("invoice_amount_cents"), "invoice_amount_cents")
    currency = _normalize_currency(payload.get("currency", "EUR"), "currency")

    document_id = None
    if payload.get("document_id") is not None:
        document_id = _ensure_uuid(payload["document_id"], "document_id")

    notes = _ensure_optional_string(payload, "notes", max_length=5000)
    if notes == "":
        notes = None

    # Reconcile: sum automatic transactions for this provider + period
    with _connect_bookkeeping_db() as connection:
        if document_id is not None:
            _ensure_document_exists(connection, document_id)

        sum_row = connection.execute(
            """
            SELECT COALESCE(SUM(t.amount_gross), 0) AS total_cents
            FROM transactions t
            WHERE t.booking_class = 'automatic'
              AND t.provider = ?
              AND t.direction = 'OUT'
              AND t.date >= ?
              AND t.date <= ?
            """,
            (provider, period_from, period_to),
        ).fetchone()

        calculated_sum_cents = int(sum_row["total_cents"]) if sum_row else 0
        difference_cents = invoice_amount_cents - calculated_sum_cents

        if difference_cents == 0:
            status = "matched"
        else:
            status = "mismatch"

        invoice_id = str(uuid.uuid4())
        timestamp = _now_iso()

        connection.execute(
            """
            INSERT INTO monthly_invoices (
                id, provider, period_from, period_to, invoice_amount_cents, currency,
                calculated_sum_cents, difference_cents, document_id, notes, status,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                invoice_id,
                provider,
                period_from,
                period_to,
                invoice_amount_cents,
                currency,
                calculated_sum_cents,
                difference_cents,
                document_id,
                notes,
                status,
                timestamp,
                timestamp,
            ),
        )

        # Link matching automatic transactions via junction table
        matching_txs = connection.execute(
            """
            SELECT t.id
            FROM transactions t
            WHERE t.booking_class = 'automatic'
              AND t.provider = ?
              AND t.direction = 'OUT'
              AND t.date >= ?
              AND t.date <= ?
            """,
            (provider, period_from, period_to),
        ).fetchall()

        for tx_row in matching_txs:
            connection.execute(
                "INSERT OR IGNORE INTO monthly_invoice_transactions (invoice_id, transaction_id) VALUES (?, ?)",
                (invoice_id, tx_row["id"]),
            )

        connection.commit()

        created_row = connection.execute(
            """
            SELECT
                mi.*,
                d.original_filename AS document_original_filename,
                d.mime_type AS document_mime_type
            FROM monthly_invoices mi
            LEFT JOIN documents d ON d.id = mi.document_id
            WHERE mi.id = ?
            """,
            (invoice_id,),
        ).fetchone()

    if created_row is None:
        raise BookkeepingServiceError(500, "monthly invoice could not be loaded after creation")
    return _monthly_invoice_row_to_dict(created_row)


def update_monthly_invoice(invoice_id: str, payload: Any) -> dict[str, Any]:
    """Update a monthly invoice and re-reconcile."""
    _ensure_db_available()
    inv_id = _ensure_uuid(invoice_id, "invoice_id")

    if not isinstance(payload, dict):
        raise BookkeepingServiceError(400, "invalid JSON payload")

    allowed = {"provider", "period_from", "period_to", "invoice_amount_cents", "currency", "document_id", "notes"}
    unknown = set(payload.keys()) - allowed
    if unknown:
        raise BookkeepingServiceError(400, f"unsupported fields: {', '.join(sorted(unknown))}")
    if not payload:
        raise BookkeepingServiceError(400, "at least one field is required")

    with _connect_bookkeeping_db() as connection:
        existing = connection.execute("SELECT * FROM monthly_invoices WHERE id = ?", (inv_id,)).fetchone()
        if existing is None:
            raise BookkeepingServiceError(404, "monthly invoice not found")

        updates: dict[str, Any] = {}
        if "provider" in payload:
            p = str(payload["provider"]).strip().lower()
            if p not in SAMMELRECHNUNG_PROVIDERS:
                raise BookkeepingServiceError(400, f"provider must be one of: {', '.join(sorted(SAMMELRECHNUNG_PROVIDERS))}")
            updates["provider"] = p
        if "period_from" in payload:
            updates["period_from"] = _parse_filter_date(payload["period_from"], "period_from", is_end=False)
        if "period_to" in payload:
            updates["period_to"] = _parse_filter_date(payload["period_to"], "period_to", is_end=True)
        if "invoice_amount_cents" in payload:
            updates["invoice_amount_cents"] = _parse_positive_int(payload["invoice_amount_cents"], "invoice_amount_cents")
        if "currency" in payload:
            updates["currency"] = _normalize_currency(payload["currency"], "currency")
        if "document_id" in payload:
            if payload["document_id"] is None:
                updates["document_id"] = None
            else:
                doc_id = _ensure_uuid(payload["document_id"], "document_id")
                _ensure_document_exists(connection, doc_id)
                updates["document_id"] = doc_id
        if "notes" in payload:
            v = payload["notes"]
            updates["notes"] = None if v is None else str(v).strip() or None

        # Apply updates — normalise period boundaries so re-reconciliation
        # always uses start-of-day for period_from and end-of-day for period_to,
        # even when the values come from records stored before the boundary fix.
        effective_provider = updates.get("provider", existing["provider"])
        effective_from = updates.get("period_from", existing["period_from"])
        effective_to = updates.get("period_to", existing["period_to"])
        # If effective_to was stored with T00:00:00Z (old bug), re-normalise it
        if effective_to and effective_to.endswith("T00:00:00Z") and "period_to" not in updates:
            try:
                effective_to = _parse_filter_date(effective_to[:10], "period_to", is_end=True)
                updates["period_to"] = effective_to
            except BookkeepingServiceError:
                pass
        # Validate period_from < period_to
        if effective_from >= effective_to:
            raise BookkeepingServiceError(400, "period_from must be before period_to")
        effective_amount = updates.get("invoice_amount_cents", existing["invoice_amount_cents"])

        # Re-reconcile
        sum_row = connection.execute(
            """
            SELECT COALESCE(SUM(t.amount_gross), 0) AS total_cents
            FROM transactions t
            WHERE t.booking_class = 'automatic'
              AND t.provider = ?
              AND t.direction = 'OUT'
              AND t.date >= ?
              AND t.date <= ?
            """,
            (effective_provider, effective_from, effective_to),
        ).fetchone()

        calculated_sum_cents = int(sum_row["total_cents"]) if sum_row else 0
        difference_cents = int(effective_amount) - calculated_sum_cents
        status = "matched" if difference_cents == 0 else "mismatch"

        updates["calculated_sum_cents"] = calculated_sum_cents
        updates["difference_cents"] = difference_cents
        updates["status"] = status
        updates["updated_at"] = _now_iso()

        set_clauses = [f"{k} = ?" for k in updates]
        args = list(updates.values()) + [inv_id]
        connection.execute(f"UPDATE monthly_invoices SET {', '.join(set_clauses)} WHERE id = ?", args)

        # Re-link transactions
        connection.execute("DELETE FROM monthly_invoice_transactions WHERE invoice_id = ?", (inv_id,))
        matching_txs = connection.execute(
            """
            SELECT t.id
            FROM transactions t
            WHERE t.booking_class = 'automatic'
              AND t.provider = ?
              AND t.direction = 'OUT'
              AND t.date >= ?
              AND t.date <= ?
            """,
            (effective_provider, effective_from, effective_to),
        ).fetchall()
        for tx_row in matching_txs:
            connection.execute(
                "INSERT OR IGNORE INTO monthly_invoice_transactions (invoice_id, transaction_id) VALUES (?, ?)",
                (inv_id, tx_row["id"]),
            )

        connection.commit()

        updated_row = connection.execute(
            """
            SELECT
                mi.*,
                d.original_filename AS document_original_filename,
                d.mime_type AS document_mime_type
            FROM monthly_invoices mi
            LEFT JOIN documents d ON d.id = mi.document_id
            WHERE mi.id = ?
            """,
            (inv_id,),
        ).fetchone()

    if updated_row is None:
        raise BookkeepingServiceError(500, "monthly invoice could not be loaded after update")
    return _monthly_invoice_row_to_dict(updated_row)


def delete_monthly_invoice(invoice_id: str) -> dict[str, Any]:
    """Delete a monthly invoice."""
    _ensure_db_available()
    inv_id = _ensure_uuid(invoice_id, "invoice_id")

    with _connect_bookkeeping_db() as connection:
        existing = connection.execute("SELECT id FROM monthly_invoices WHERE id = ?", (inv_id,)).fetchone()
        if existing is None:
            raise BookkeepingServiceError(404, "monthly invoice not found")

        connection.execute("DELETE FROM monthly_invoice_transactions WHERE invoice_id = ?", (inv_id,))
        connection.execute("DELETE FROM monthly_invoices WHERE id = ?", (inv_id,))
        connection.commit()

    return {"id": inv_id, "deleted": True}
