from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Optional
from urllib import error as urlerror

from app.config import SUPPORT_DB_PATH
from app.services.importers.kaufland_tickets import _connect_support_db, _safe_json_load, fetch_remote_attachment_bytes
from app.services.orders import get_order_detail


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ticket_counts(connection: sqlite3.Connection, id_ticket: str) -> dict[str, int]:
    note_row = connection.execute(
        "SELECT COUNT(*) AS count FROM ticket_internal_notes WHERE id_ticket = ?",
        (id_ticket,),
    ).fetchone()
    message_row = connection.execute(
        "SELECT COUNT(*) AS count FROM ticket_messages WHERE id_ticket = ?",
        (id_ticket,),
    ).fetchone()
    return {
        "notes": int(note_row["count"] or 0) if note_row else 0,
        "messages": int(message_row["count"] or 0) if message_row else 0,
    }


def _normalize_filter_mode(filter_mode: str | None) -> str:
    token = str(filter_mode or "todo").strip().lower()
    if token in {"todo", "waiting", "closed", "all"}:
        return token
    return "todo"


def list_support_tickets(
    *,
    filter_mode: str = "todo",
    q: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    if not SUPPORT_DB_PATH.exists():
        return {"total": 0, "items": []}

    mode = _normalize_filter_mode(filter_mode)
    where: list[str] = []
    params: list[Any] = []

    if mode == "todo":
        where.append("status = 'opened' AND is_seller_responsible = 1")
    elif mode == "waiting":
        where.append("status = 'opened' AND is_seller_responsible = 0")
    elif mode == "closed":
        where.append("status <> 'opened'")

    if q:
        needle = f"%{str(q).strip().lower()}%"
        if needle != "%%":
            where.append(
                "(" 
                "LOWER(id_ticket) LIKE ? OR LOWER(COALESCE(topic, '')) LIKE ? OR LOWER(COALESCE(open_reason, '')) LIKE ? OR "
                "EXISTS (SELECT 1 FROM ticket_order_units tou WHERE tou.id_ticket = tickets.id_ticket AND LOWER(tou.id_order_unit) LIKE ?) OR "
                "EXISTS (SELECT 1 FROM ticket_messages tm WHERE tm.id_ticket = tickets.id_ticket AND LOWER(COALESCE(tm.text, '')) LIKE ?)"
                ")"
            )
            params.extend([needle, needle, needle, needle, needle])

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    with _connect_support_db() as connection:
        count_row = connection.execute(
            f"SELECT COUNT(*) AS count FROM tickets {where_sql}",
            params,
        ).fetchone()
        rows = connection.execute(
            f"SELECT * FROM tickets {where_sql} ORDER BY COALESCE(ts_updated_iso, ts_created_iso, '') DESC, id_ticket DESC LIMIT ? OFFSET ?",
            [*params, max(limit, 0), max(offset, 0)],
        ).fetchall()

        items: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["is_seller_responsible"] = bool(payload.get("is_seller_responsible"))
            payload["order_unit_ids"] = [
                str(link_row["id_order_unit"])
                for link_row in connection.execute(
                    "SELECT id_order_unit FROM ticket_order_units WHERE id_ticket = ? ORDER BY position ASC",
                    (payload["id_ticket"],),
                ).fetchall()
            ]
            payload["counts"] = _ticket_counts(connection, str(payload["id_ticket"]))
            items.append(payload)

    return {"total": int(count_row["count"] or 0) if count_row else len(items), "items": items}


def get_support_ticket_detail(id_ticket: str) -> Optional[dict[str, Any]]:
    if not SUPPORT_DB_PATH.exists():
        return None

    with _connect_support_db() as connection:
        ticket_row = connection.execute(
            "SELECT * FROM tickets WHERE id_ticket = ? LIMIT 1",
            (id_ticket,),
        ).fetchone()
        if ticket_row is None:
            return None

        payload = dict(ticket_row)
        payload["is_seller_responsible"] = bool(payload.get("is_seller_responsible"))
        raw_ticket = _safe_json_load(payload.get("raw_json"))
        order_unit_rows = connection.execute(
            "SELECT * FROM ticket_order_units WHERE id_ticket = ? ORDER BY position ASC",
            (id_ticket,),
        ).fetchall()
        message_rows = connection.execute(
            "SELECT * FROM ticket_messages WHERE id_ticket = ? ORDER BY COALESCE(ts_created_iso, '') ASC, id_ticket_message ASC",
            (id_ticket,),
        ).fetchall()
        attachment_rows = connection.execute(
            "SELECT * FROM ticket_attachments_meta WHERE id_ticket = ? ORDER BY COALESCE(ts_created_iso, '') ASC, filename ASC",
            (id_ticket,),
        ).fetchall()
        note_rows = connection.execute(
            "SELECT * FROM ticket_internal_notes WHERE id_ticket = ? ORDER BY updated_at DESC, created_at DESC",
            (id_ticket,),
        ).fetchall()

    order_unit_ids = [str(row["id_order_unit"]) for row in order_unit_rows]
    order_context = None
    if raw_ticket and isinstance(raw_ticket, dict):
        order_units = raw_ticket.get("order_units") if isinstance(raw_ticket.get("order_units"), list) else []
        first_order_id = ""
        for item in order_units:
            if not isinstance(item, dict):
                continue
            first_order_id = str(item.get("id_order") or "").strip()
            if first_order_id:
                break
        if first_order_id:
            order_context = get_order_detail("kaufland", first_order_id)

    return {
        "ticket": payload,
        "ticket_raw": raw_ticket if isinstance(raw_ticket, dict) else {},
        "order_unit_ids": order_unit_ids,
        "messages": [dict(row) | {"raw": _safe_json_load(row["raw_json"])} for row in message_rows],
        "attachments": [dict(row) for row in attachment_rows],
        "notes": [dict(row) for row in note_rows],
        "order_context": order_context,
    }


def list_ticket_notes(id_ticket: str) -> list[dict[str, Any]]:
    if not SUPPORT_DB_PATH.exists():
        return []
    with _connect_support_db() as connection:
        rows = connection.execute(
            "SELECT * FROM ticket_internal_notes WHERE id_ticket = ? ORDER BY updated_at DESC, created_at DESC",
            (id_ticket,),
        ).fetchall()
        return [dict(row) for row in rows]


def create_ticket_note(id_ticket: str, note_text: str) -> dict[str, Any]:
    now = _now_iso()
    note_id = str(uuid.uuid4())
    with _connect_support_db() as connection:
        ticket_row = connection.execute("SELECT id_ticket FROM tickets WHERE id_ticket = ?", (id_ticket,)).fetchone()
        if ticket_row is None:
            raise ValueError("ticket not found")
        connection.execute(
            """
            INSERT INTO ticket_internal_notes (id, id_ticket, note_text, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (note_id, id_ticket, str(note_text or "").strip(), now, now),
        )
        connection.commit()
    return {"id": note_id, "id_ticket": id_ticket, "note_text": str(note_text or "").strip(), "created_at": now, "updated_at": now}


def update_ticket_note(id_ticket: str, note_id: str, note_text: str) -> dict[str, Any]:
    now = _now_iso()
    with _connect_support_db() as connection:
        row = connection.execute(
            "SELECT * FROM ticket_internal_notes WHERE id = ? AND id_ticket = ?",
            (note_id, id_ticket),
        ).fetchone()
        if row is None:
            raise ValueError("note not found")
        connection.execute(
            "UPDATE ticket_internal_notes SET note_text = ?, updated_at = ? WHERE id = ? AND id_ticket = ?",
            (str(note_text or "").strip(), now, note_id, id_ticket),
        )
        connection.commit()
        updated = connection.execute(
            "SELECT * FROM ticket_internal_notes WHERE id = ? AND id_ticket = ?",
            (note_id, id_ticket),
        ).fetchone()
    return dict(updated) if updated is not None else {}


def delete_ticket_note(id_ticket: str, note_id: str) -> None:
    with _connect_support_db() as connection:
        connection.execute(
            "DELETE FROM ticket_internal_notes WHERE id = ? AND id_ticket = ?",
            (note_id, id_ticket),
        )
        connection.commit()


def resolve_attachment_preview(id_ticket: str, filename: str) -> tuple[bytes, str, str]:
    if not SUPPORT_DB_PATH.exists():
        raise ValueError("support db not found")
    with _connect_support_db() as connection:
        row = connection.execute(
            "SELECT * FROM ticket_attachments_meta WHERE id_ticket = ? AND filename = ? ORDER BY COALESCE(ts_created_iso, '') DESC LIMIT 1",
            (id_ticket, filename),
        ).fetchone()
        if row is None:
            raise ValueError("attachment not found")
        try:
            content, content_type = fetch_remote_attachment_bytes(str(row["uri"] or ""))
        except (RuntimeError, urlerror.URLError, TimeoutError, OSError) as exc:
            raise ValueError(f"attachment preview unavailable: {exc}") from exc
        return content, content_type, str(row["filename"] or filename)
