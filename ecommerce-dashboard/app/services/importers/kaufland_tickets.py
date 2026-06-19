from __future__ import annotations

import json
import logging
import sqlite3
import time
from urllib import error as urlerror
from urllib import request as urlrequest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from app.config import KAUFLAND_DB_PATH, SUPPORT_DB_PATH
from app.services.importers.kaufland_live import (
    KauflandLiveClient,
    KauflandLiveError,
    load_kaufland_live_config,
)


LOGGER = logging.getLogger(__name__)

DEFAULT_STOREFRONT = "de"
OPEN_STATUSES = ["opened"]
CLOSED_STATUSES = [
    "buyer_closed",
    "seller_closed",
    "both_closed",
    "customer_service_closed_final",
]
ALL_TICKET_STATUSES = [*OPEN_STATUSES, *CLOSED_STATUSES]


@dataclass
class SupportSyncOptions:
    storefront: str = DEFAULT_STOREFRONT
    page_limit: int = 30
    max_pages: int = 1000
    include_closed: bool = True
    updated_from_iso: Optional[str] = None
    lookback_minutes: int = 60


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


def _parse_iso(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _safe_json_load(raw_value: Any) -> Any:
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None
    try:
        return json.loads(raw_value)
    except (TypeError, ValueError):
        return None


def _connect_support_db() -> sqlite3.Connection:
    SUPPORT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(SUPPORT_DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


def init_support_db() -> str:
    with _connect_support_db() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS tickets (
                id_ticket TEXT PRIMARY KEY,
                storefront TEXT,
                id_buyer TEXT,
                status TEXT,
                open_reason TEXT,
                topic TEXT,
                fulfillment_type TEXT,
                is_seller_responsible INTEGER,
                ts_created_iso TEXT,
                ts_updated_iso TEXT,
                first_response_due_at TEXT,
                first_response_sent_at TEXT,
                order_units_count INTEGER NOT NULL DEFAULT 0,
                last_message_at_iso TEXT,
                raw_json TEXT NOT NULL,
                synced_at_iso TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ticket_order_units (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_ticket TEXT NOT NULL,
                id_order_unit TEXT NOT NULL,
                position INTEGER NOT NULL,
                synced_at_iso TEXT NOT NULL,
                UNIQUE(id_ticket, position),
                FOREIGN KEY (id_ticket) REFERENCES tickets(id_ticket) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ticket_messages (
                id_ticket_message TEXT PRIMARY KEY,
                id_ticket TEXT NOT NULL,
                author_role TEXT,
                author_name TEXT,
                text TEXT,
                ts_created_iso TEXT,
                direction TEXT,
                raw_json TEXT NOT NULL,
                synced_at_iso TEXT NOT NULL,
                FOREIGN KEY (id_ticket) REFERENCES tickets(id_ticket) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ticket_attachments_meta (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_ticket TEXT NOT NULL,
                filename TEXT NOT NULL,
                uri TEXT NOT NULL,
                ts_created_iso TEXT,
                synced_at_iso TEXT NOT NULL,
                UNIQUE(id_ticket, filename, uri),
                FOREIGN KEY (id_ticket) REFERENCES tickets(id_ticket) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ticket_internal_notes (
                id TEXT PRIMARY KEY,
                id_ticket TEXT NOT NULL,
                note_text TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (id_ticket) REFERENCES tickets(id_ticket) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ticket_sync_runs (
                id_sync_run INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_started_iso TEXT NOT NULL,
                ts_finished_iso TEXT NOT NULL,
                storefront TEXT NOT NULL,
                status TEXT NOT NULL,
                mode TEXT NOT NULL,
                error_count INTEGER NOT NULL,
                summary_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_tickets_status ON tickets(status);
            CREATE INDEX IF NOT EXISTS idx_tickets_responsible ON tickets(is_seller_responsible);
            CREATE INDEX IF NOT EXISTS idx_tickets_updated ON tickets(ts_updated_iso DESC);
            CREATE INDEX IF NOT EXISTS idx_ticket_messages_ticket_created ON ticket_messages(id_ticket, ts_created_iso ASC);
            CREATE INDEX IF NOT EXISTS idx_ticket_order_units_ticket ON ticket_order_units(id_ticket);
            CREATE INDEX IF NOT EXISTS idx_ticket_notes_ticket ON ticket_internal_notes(id_ticket, updated_at DESC);
            """
        )
        connection.commit()
    return str(SUPPORT_DB_PATH)


def _upsert(connection: sqlite3.Connection, table: str, key_columns: list[str], values: dict[str, Any]) -> None:
    columns = list(values.keys())
    placeholders = ", ".join("?" for _ in columns)
    conflict_target = ", ".join(key_columns)
    update_clause = ", ".join(
        f"{column}=excluded.{column}"
        for column in columns
        if column not in key_columns
    )
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT({conflict_target}) DO UPDATE SET {update_clause}"
    )
    connection.execute(sql, tuple(values[column] for column in columns))


def _fetch_existing_raw_json(connection: sqlite3.Connection, table_name: str, key_column: str, key_value: str) -> Optional[str]:
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


def _author_direction(author_role: Any) -> str:
    role = str(author_role or "").strip().lower()
    if role == "seller":
        return "outbound"
    return "inbound"


def _compute_first_response_due_at(ts_created_iso: Any) -> Optional[str]:
    created = _parse_iso(ts_created_iso)
    if created is None:
        return None
    due = created + timedelta(hours=48)
    return due.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _replace_ticket_order_units(connection: sqlite3.Connection, id_ticket: str, order_unit_ids: list[Any], synced_at_iso: str) -> int:
    connection.execute("DELETE FROM ticket_order_units WHERE id_ticket = ?", (id_ticket,))
    inserted = 0
    for position, order_unit_id in enumerate(order_unit_ids):
        text = str(order_unit_id or "").strip()
        if not text:
            continue
        connection.execute(
            """
            INSERT INTO ticket_order_units (id_ticket, id_order_unit, position, synced_at_iso)
            VALUES (?, ?, ?, ?)
            """,
            (id_ticket, text, position, synced_at_iso),
        )
        inserted += 1
    return inserted


def _extract_ticket_order_unit_ids(ticket: dict[str, Any]) -> list[str]:
    candidates: list[Any] = []

    ids_order_units = ticket.get("ids_order_units")
    if isinstance(ids_order_units, list):
        candidates.extend(ids_order_units)

    id_order_unit = ticket.get("id_order_unit")
    if isinstance(id_order_unit, list):
        candidates.extend(id_order_unit)
    elif id_order_unit is not None:
        candidates.append(id_order_unit)

    order_units = ticket.get("order_units")
    if isinstance(order_units, list):
        for item in order_units:
            if isinstance(item, dict):
                candidates.append(item.get("id_order_unit"))
            else:
                candidates.append(item)

    normalized: list[str] = []
    seen: set[str] = set()
    for value in candidates:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _normalize_attachment_entry(raw_file: Any, *, ts_created_iso: Any = None) -> Optional[dict[str, Any]]:
    if not isinstance(raw_file, dict):
        return None

    filename = _clean_text(
        raw_file.get("filename")
        or raw_file.get("name")
        or raw_file.get("file_name")
        or raw_file.get("original_filename")
    )
    uri = _clean_text(
        raw_file.get("uri")
        or raw_file.get("url")
        or raw_file.get("href")
        or raw_file.get("download_url")
        or raw_file.get("file_url")
    )
    if not filename or not uri:
        return None

    return {
        "filename": filename,
        "uri": uri,
        "ts_created_iso": _clean_text(
            raw_file.get("ts_created_iso")
            or raw_file.get("created_at")
            or raw_file.get("ts_uploaded_iso")
            or ts_created_iso
        ),
    }


def _extract_ticket_attachments(ticket: dict[str, Any]) -> list[dict[str, Any]]:
    extracted: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def append_items(items: Any, *, ts_created_iso: Any = None) -> None:
        if not isinstance(items, list):
            return
        for item in items:
            normalized = _normalize_attachment_entry(item, ts_created_iso=ts_created_iso)
            if normalized is None:
                continue
            key = (str(normalized["filename"]), str(normalized["uri"]))
            if key in seen:
                continue
            seen.add(key)
            extracted.append(normalized)

    append_items(ticket.get("files"))
    append_items(ticket.get("ticket_message_files"))

    messages = ticket.get("messages") if isinstance(ticket.get("messages"), list) else []
    for message in messages:
        if not isinstance(message, dict):
            continue
        created_at = message.get("ts_created_iso")
        append_items(message.get("files"), ts_created_iso=created_at)
        append_items(message.get("ticket_message_files"), ts_created_iso=created_at)
        append_items(message.get("attachments"), ts_created_iso=created_at)

    return extracted


def _replace_ticket_attachments(connection: sqlite3.Connection, id_ticket: str, files: list[dict[str, Any]], synced_at_iso: str) -> int:
    connection.execute("DELETE FROM ticket_attachments_meta WHERE id_ticket = ?", (id_ticket,))
    inserted = 0
    seen: set[tuple[str, str]] = set()
    for file in files:
        filename = str(file.get("filename") or "").strip()
        uri = str(file.get("uri") or "").strip()
        if not filename or not uri:
            continue
        key = (filename, uri)
        if key in seen:
            continue
        seen.add(key)
        connection.execute(
            """
            INSERT INTO ticket_attachments_meta (id_ticket, filename, uri, ts_created_iso, synced_at_iso)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                id_ticket,
                filename,
                uri,
                _clean_text(file.get("ts_created_iso")),
                synced_at_iso,
            ),
        )
        inserted += 1
    return inserted


def _upsert_ticket(connection: sqlite3.Connection, ticket: dict[str, Any], *, synced_at_iso: str) -> str:
    ticket_id = str(ticket.get("id_ticket") or "").strip()
    raw_json = _json_dumps(ticket)
    existing_raw = _fetch_existing_raw_json(connection, "tickets", "id_ticket", ticket_id)
    change = _detect_change(existing_raw, raw_json)
    first_response_sent_at = None
    messages = ticket.get("messages") if isinstance(ticket.get("messages"), list) else []
    for message in messages:
        if not isinstance(message, dict):
            continue
        author = message.get("author") if isinstance(message.get("author"), dict) else {}
        if str(author.get("role") or "").strip().lower() == "seller":
            first_response_sent_at = _clean_text(message.get("ts_created_iso"))
            break

    values = {
        "id_ticket": ticket_id,
        "storefront": _clean_text(ticket.get("storefront")) or DEFAULT_STOREFRONT,
        "id_buyer": _clean_text(ticket.get("id_buyer")),
        "status": _clean_text(ticket.get("status")),
        "open_reason": _clean_text(ticket.get("open_reason")),
        "topic": _clean_text(ticket.get("topic")),
        "fulfillment_type": _clean_text(ticket.get("fulfillment_type")),
        "is_seller_responsible": 1 if bool(ticket.get("is_seller_responsible")) else 0,
        "ts_created_iso": _clean_text(ticket.get("ts_created_iso")),
        "ts_updated_iso": _clean_text(ticket.get("ts_updated_iso")),
        "first_response_due_at": _compute_first_response_due_at(ticket.get("ts_created_iso")),
        "first_response_sent_at": first_response_sent_at,
        "order_units_count": len(_extract_ticket_order_unit_ids(ticket)),
        "last_message_at_iso": _clean_text(messages[-1].get("ts_created_iso")) if messages else None,
        "raw_json": raw_json,
        "synced_at_iso": synced_at_iso,
    }
    _upsert(connection, "tickets", ["id_ticket"], values)
    return change


def _upsert_ticket_message(
    connection: sqlite3.Connection,
    message: dict[str, Any],
    *,
    id_ticket: str,
    synced_at_iso: str,
) -> str:
    message_id = str(message.get("id_ticket_message") or "").strip()
    raw_json = _json_dumps(message)
    existing_raw = _fetch_existing_raw_json(connection, "ticket_messages", "id_ticket_message", message_id)
    change = _detect_change(existing_raw, raw_json)
    author = message.get("author") if isinstance(message.get("author"), dict) else {}
    values = {
        "id_ticket_message": message_id,
        # Kaufland message payloads may reference the same ticket without leading zeros.
        "id_ticket": str(id_ticket).strip(),
        "author_role": _clean_text(author.get("role")),
        "author_name": _clean_text(author.get("name")),
        "text": _clean_text(message.get("text")) or "",
        "ts_created_iso": _clean_text(message.get("ts_created_iso")),
        "direction": _author_direction(author.get("role")),
        "raw_json": raw_json,
        "synced_at_iso": synced_at_iso,
    }
    _upsert(connection, "ticket_messages", ["id_ticket_message"], values)
    return change


def _extract_data_object(payload: Any) -> Optional[dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    return payload


def _extract_array(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if isinstance(data, list):
        return data
    return []


def _build_status_filter_payload(include_closed: bool) -> list[str]:
    return ALL_TICKET_STATUSES if include_closed else OPEN_STATUSES


def _fetch_ticket_page(
    client: KauflandLiveClient,
    *,
    storefront: str,
    statuses: list[str],
    page_limit: int,
    offset: int,
    updated_from_iso: Optional[str],
) -> dict[str, Any]:
    query: dict[str, Any] = {
        "storefront": [storefront],
        "status": statuses,
        "limit": page_limit,
        "offset": offset,
        "sort": "ts_updated_iso:desc",
    }
    if updated_from_iso:
        query["ts_updated_from_iso"] = updated_from_iso
    from app.services.importers.kaufland_live import _build_endpoint_url

    url, _ = _build_endpoint_url(client.base_url, "tickets", query)
    payload = client.get_json(url, "")
    return payload if isinstance(payload, dict) else {}


def _fetch_ticket_detail(client: KauflandLiveClient, ticket_id: str) -> dict[str, Any]:
    from app.services.importers.kaufland_live import _build_endpoint_url

    candidate_queries: list[Optional[dict[str, Any]]] = [
        {"embedded": ["buyer", "product", "messages", "order_units", "files"]},
        {"embedded": ["messages", "order_units", "files"]},
        None,
    ]
    last_error: Optional[KauflandLiveError] = None

    for query in candidate_queries:
        url, _ = _build_endpoint_url(client.base_url, f"tickets/{ticket_id}", query)
        try:
            payload = client.get_json(url, "")
        except KauflandLiveError as exc:
            last_error = exc
            if exc.status_code not in {400, 422} or query is None:
                raise
            continue
        data = _extract_data_object(payload)
        return data if isinstance(data, dict) else {}

    if last_error is not None:
        raise last_error
    return {}


def _record_sync_run(
    connection: sqlite3.Connection,
    *,
    started_at_iso: str,
    finished_at_iso: str,
    storefront: str,
    status: str,
    mode: str,
    error_count: int,
    summary: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO ticket_sync_runs (
            ts_started_iso, ts_finished_iso, storefront, status, mode, error_count, summary_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (started_at_iso, finished_at_iso, storefront, status, mode, int(error_count), _json_dumps(summary)),
    )


def get_last_successful_ticket_sync_time() -> Optional[str]:
    if not SUPPORT_DB_PATH.exists():
        return None
    try:
        with _connect_support_db() as connection:
            row = connection.execute(
                """
                SELECT ts_finished_iso
                FROM ticket_sync_runs
                WHERE status IN ('success', 'partial')
                ORDER BY id_sync_run DESC
                LIMIT 1
                """
            ).fetchone()
            return str(row["ts_finished_iso"]) if row else None
    except Exception:
        return None


def build_kaufland_tickets_status() -> dict[str, Any]:
    config, missing, config_summary = load_kaufland_live_config()
    db_exists = SUPPORT_DB_PATH.exists()
    counts = {
        "tickets_total": 0,
        "tickets_open": 0,
        "tickets_waiting": 0,
        "tickets_closed": 0,
        "notes_total": 0,
    }
    last_sync = None

    if db_exists:
        try:
            with _connect_support_db() as connection:
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM tickets"
                ).fetchone()
                counts["tickets_total"] = int(row["count"] or 0) if row else 0
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM tickets WHERE status = 'opened' AND is_seller_responsible = 1"
                ).fetchone()
                counts["tickets_open"] = int(row["count"] or 0) if row else 0
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM tickets WHERE status = 'opened' AND is_seller_responsible = 0"
                ).fetchone()
                counts["tickets_waiting"] = int(row["count"] or 0) if row else 0
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM tickets WHERE status <> 'opened'"
                ).fetchone()
                counts["tickets_closed"] = int(row["count"] or 0) if row else 0
                row = connection.execute("SELECT COUNT(*) AS count FROM ticket_internal_notes").fetchone()
                counts["notes_total"] = int(row["count"] or 0) if row else 0
                last_sync_row = connection.execute(
                    """
                    SELECT id_sync_run, ts_started_iso, ts_finished_iso, storefront, status, mode, error_count, summary_json
                    FROM ticket_sync_runs
                    ORDER BY id_sync_run DESC
                    LIMIT 1
                    """
                ).fetchone()
                if last_sync_row is not None:
                    last_sync = dict(last_sync_row)
        except sqlite3.Error:
            last_sync = None

    return {
        "configured": config is not None,
        "missing_env": missing,
        "config": {
            "base_url": config_summary.get("base_url"),
            "bootstrap_env_path": config_summary.get("bootstrap_env_path"),
            "bootstrap_env_exists": config_summary.get("bootstrap_env_exists"),
            "storefront": DEFAULT_STOREFRONT,
            "polling_interval_seconds": 300,
        },
        "runtime_db": {"path": str(SUPPORT_DB_PATH), "exists": db_exists},
        "counts": counts,
        "last_sync": last_sync,
    }


def sync_kaufland_tickets(*, mode: str = "poll", options: Optional[SupportSyncOptions] = None) -> dict[str, Any]:
    started_at = _utc_now_iso()
    started_monotonic = time.monotonic()
    init_support_db()

    config, missing, config_summary = load_kaufland_live_config()
    if config is None:
        return {
            "status": "error",
            "provider": "kaufland_tickets",
            "error": "Kaufland credentials are not configured.",
            "missing_env": missing,
            "config": config_summary,
        }

    opts = options or SupportSyncOptions()
    storefront = str(opts.storefront or DEFAULT_STOREFRONT).strip().lower() or DEFAULT_STOREFRONT
    page_limit = min(max(int(opts.page_limit), 1), 30)
    max_pages = min(max(int(opts.max_pages), 1), 5000)
    include_closed = bool(opts.include_closed)
    updated_from_iso = _clean_text(opts.updated_from_iso)
    if mode == "poll" and not updated_from_iso:
        updated_from_iso = get_last_successful_ticket_sync_time()
        parsed = _parse_iso(updated_from_iso)
        if parsed is not None:
            lookback_minutes = max(int(opts.lookback_minutes or 60), 5)
            updated_from_iso = (
                parsed - timedelta(minutes=lookback_minutes)
            ).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    summary: dict[str, Any] = {
        "storefront": storefront,
        "mode": mode,
        "page_limit": page_limit,
        "max_pages": max_pages,
        "include_closed": include_closed,
        "ts_updated_from_iso": updated_from_iso,
        "tickets_seen": 0,
        "tickets_saved": 0,
        "tickets_inserted": 0,
        "tickets_updated": 0,
        "tickets_unchanged": 0,
        "messages_saved": 0,
        "messages_inserted": 0,
        "messages_updated": 0,
        "messages_unchanged": 0,
        "attachments_saved": 0,
        "order_unit_links_saved": 0,
        "pages": 0,
        "error_count": 0,
        "started_at_iso": started_at,
        "finished_at_iso": None,
        "duration_seconds": None,
    }
    errors: list[dict[str, Any]] = []
    status = "success"

    def add_error(scope: str, message: str, **meta: Any) -> None:
        summary["error_count"] += 1
        row = {"scope": scope, "error": str(message)}
        row.update(meta)
        if len(errors) < 250:
            errors.append(row)

    try:
        client = KauflandLiveClient(config)
        with _connect_support_db() as connection:
            offset = 0
            statuses = _build_status_filter_payload(include_closed)
            for _ in range(max_pages):
                summary["pages"] += 1
                payload = _fetch_ticket_page(
                    client,
                    storefront=storefront,
                    statuses=statuses,
                    page_limit=page_limit,
                    offset=offset,
                    updated_from_iso=updated_from_iso,
                )
                page_items = _extract_array(payload)
                summary["tickets_seen"] += len(page_items)

                for item in page_items:
                    if not isinstance(item, dict):
                        continue
                    ticket_id = str(item.get("id_ticket") or "").strip()
                    if not ticket_id:
                        add_error("ticket_list", "Missing id_ticket in payload")
                        status = "partial"
                        continue
                    try:
                        detail = _fetch_ticket_detail(client, ticket_id)
                    except KauflandLiveError as exc:
                        add_error("ticket_detail", str(exc), id_ticket=ticket_id, status_code=exc.status_code)
                        status = "partial"
                        continue
                    if not detail:
                        add_error("ticket_detail", "Ticket detail response is empty", id_ticket=ticket_id)
                        status = "partial"
                        continue

                    synced_at_iso = _utc_now_iso()
                    ticket_change = _upsert_ticket(connection, detail, synced_at_iso=synced_at_iso)
                    if ticket_change == "inserted":
                        summary["tickets_saved"] += 1
                        summary["tickets_inserted"] += 1
                    elif ticket_change == "updated":
                        summary["tickets_saved"] += 1
                        summary["tickets_updated"] += 1
                    else:
                        summary["tickets_unchanged"] += 1

                    ids_order_units = _extract_ticket_order_unit_ids(detail)
                    summary["order_unit_links_saved"] += _replace_ticket_order_units(connection, ticket_id, ids_order_units, synced_at_iso)

                    files = _extract_ticket_attachments(detail)
                    summary["attachments_saved"] += _replace_ticket_attachments(
                        connection,
                        ticket_id,
                        files,
                        synced_at_iso,
                    )

                    messages = detail.get("messages") if isinstance(detail.get("messages"), list) else []
                    for message in messages:
                        if not isinstance(message, dict):
                            continue
                        message_change = _upsert_ticket_message(
                            connection,
                            message,
                            id_ticket=ticket_id,
                            synced_at_iso=synced_at_iso,
                        )
                        if message_change == "inserted":
                            summary["messages_saved"] += 1
                            summary["messages_inserted"] += 1
                        elif message_change == "updated":
                            summary["messages_saved"] += 1
                            summary["messages_updated"] += 1
                        else:
                            summary["messages_unchanged"] += 1

                connection.commit()
                if len(page_items) < page_limit:
                    break
                offset += page_limit

            summary["finished_at_iso"] = _utc_now_iso()
            summary["duration_seconds"] = round(time.monotonic() - started_monotonic, 3)
            if summary["error_count"] > 0 and status == "success":
                status = "partial"
            _record_sync_run(
                connection,
                started_at_iso=started_at,
                finished_at_iso=str(summary["finished_at_iso"]),
                storefront=storefront,
                status=status,
                mode=mode,
                error_count=int(summary["error_count"]),
                summary={**summary, "errors": errors[:100]},
            )
            connection.commit()

        return {
            "status": status,
            "provider": "kaufland_tickets",
            "database_path": str(SUPPORT_DB_PATH),
            "summary": summary,
            "errors": errors,
            "config": config_summary,
        }
    except KauflandLiveError as exc:
        summary["finished_at_iso"] = _utc_now_iso()
        summary["duration_seconds"] = round(time.monotonic() - started_monotonic, 3)
        add_error("kaufland_tickets", str(exc), status_code=exc.status_code)
        return {
            "status": "error",
            "provider": "kaufland_tickets",
            "database_path": str(SUPPORT_DB_PATH),
            "summary": summary,
            "errors": errors,
            "error": str(exc),
            "status_code": exc.status_code,
            "config": config_summary,
        }
    except Exception as exc:
        LOGGER.exception("kaufland ticket sync failed")
        summary["finished_at_iso"] = _utc_now_iso()
        summary["duration_seconds"] = round(time.monotonic() - started_monotonic, 3)
        add_error("kaufland_tickets", str(exc))
        return {
            "status": "error",
            "provider": "kaufland_tickets",
            "database_path": str(SUPPORT_DB_PATH),
            "summary": summary,
            "errors": errors,
            "error": str(exc),
            "config": config_summary,
        }


def send_ticket_message(
    id_ticket: str,
    *,
    text: str,
    interim_notice: bool = False,
    ticket_message_files: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    config, missing, config_summary = load_kaufland_live_config()
    if config is None:
        raise RuntimeError(f"Kaufland credentials are not configured: {', '.join(missing)}")
    client = KauflandLiveClient(config)
    payload: dict[str, Any] = {
        "text": str(text or "").strip(),
        "interim_notice": bool(interim_notice),
    }
    if ticket_message_files:
        payload["ticket_message_files"] = ticket_message_files
    request_ticket_write(client, "POST", f"tickets/{id_ticket}/messages", payload)
    sync_result = sync_kaufland_tickets(
        mode="poll",
        options=SupportSyncOptions(
            storefront=DEFAULT_STOREFRONT,
            page_limit=30,
            max_pages=5,
            include_closed=True,
            updated_from_iso=(_parse_iso(_utc_now_iso()) - timedelta(minutes=10)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        ),
    )
    return {"ok": True, "sync": sync_result, "config": config_summary}


def close_ticket(id_ticket: str) -> dict[str, Any]:
    config, missing, config_summary = load_kaufland_live_config()
    if config is None:
        raise RuntimeError(f"Kaufland credentials are not configured: {', '.join(missing)}")
    client = KauflandLiveClient(config)
    request_ticket_write(client, "PATCH", f"tickets/{id_ticket}/close")
    sync_result = sync_kaufland_tickets(mode="poll")
    return {"ok": True, "sync": sync_result, "config": config_summary}


def open_ticket(id_order_unit: list[int], reason: str, message: str) -> dict[str, Any]:
    config, missing, config_summary = load_kaufland_live_config()
    if config is None:
        raise RuntimeError(f"Kaufland credentials are not configured: {', '.join(missing)}")
    client = KauflandLiveClient(config)
    payload = {
        "id_order_unit": id_order_unit,
        "reason": reason,
        "message": message,
    }
    request_ticket_write(client, "POST", "tickets", payload)
    sync_result = sync_kaufland_tickets(mode="poll")
    return {"ok": True, "sync": sync_result, "config": config_summary}


def fetch_remote_attachment_bytes(uri: str) -> tuple[bytes, str]:
    import urllib.request

    request = urllib.request.Request(str(uri or "").strip(), headers={"User-Agent": "dashboard-combined/1.0"})
    with urllib.request.urlopen(request, timeout=25) as response:
        content_type = str(response.headers.get("Content-Type") or "application/octet-stream")
        return response.read(), content_type


def request_ticket_write(client: KauflandLiveClient, method: str, endpoint_path: str, body: Optional[dict[str, Any]] = None) -> None:
    from app.services.importers.kaufland_live import _build_endpoint_url

    url, _ = _build_endpoint_url(client.base_url, endpoint_path)
    requests = client._requests_module()
    normalized_method = method.upper().strip() or "POST"
    body_text = _json_dumps(body) if body is not None else ""
    headers = client._sign_headers(normalized_method, url, body_text)
    if body is not None:
        headers["Content-Type"] = "application/json"

    if requests:
        try:
            response = requests.request(
                method=normalized_method,
                url=url,
                headers=headers,
                data=body_text if body is not None else None,
                timeout=25,
                verify=client.verify_ssl,
            )
        except requests.exceptions.Timeout as exc:
            raise KauflandLiveError("Request to Kaufland API timed out", status_code=504) from exc
        except requests.exceptions.RequestException as exc:
            raise KauflandLiveError(f"Request to Kaufland API failed: {exc}", status_code=502) from exc
        if not (200 <= int(response.status_code) < 300):
            payload = None
            try:
                payload = response.json()
            except ValueError:
                payload = None
            message = "HTTP %s" % response.status_code
            if isinstance(payload, dict):
                message = str(payload.get("message") or payload.get("error") or message)
            raise KauflandLiveError(message, status_code=response.status_code, payload=payload, response_text=(response.text or "")[:1000])
        return

    request = urlrequest.Request(
        url,
        headers=headers,
        data=body_text.encode("utf-8") if body is not None else None,
        method=normalized_method,
    )
    try:
        with urlrequest.urlopen(request, timeout=25, context=client._ssl_context()) as response:
            status_code = int(getattr(response, "status", 200))
            if not (200 <= status_code < 300):
                raise KauflandLiveError(f"HTTP {status_code}", status_code=status_code)
    except urlerror.HTTPError as exc:
        body_bytes = b""
        try:
            body_bytes = exc.read()
        except Exception:
            body_bytes = b""
        text = body_bytes.decode("utf-8", errors="replace")
        payload = None
        try:
            payload = json.loads(text) if text else None
        except ValueError:
            payload = None
        message = str(payload.get("message") or payload.get("error") or f"HTTP {exc.code}") if isinstance(payload, dict) else f"HTTP {exc.code}"
        raise KauflandLiveError(message, status_code=int(exc.code), payload=payload, response_text=text[:1000]) from exc
    except urlerror.URLError as exc:
        raise KauflandLiveError(f"Request to Kaufland API failed: {exc}", status_code=502) from exc


def sqlite_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}
