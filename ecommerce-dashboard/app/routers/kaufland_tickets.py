from __future__ import annotations

import base64
from typing import Any, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from fastapi.responses import Response
from pydantic import BaseModel, Field

from app.auth import require_admin_access
from app.services.importers.kaufland_tickets import (
    SupportSyncOptions,
    build_kaufland_tickets_status,
    close_ticket,
    init_support_db,
    open_ticket,
    send_ticket_message,
    sync_kaufland_tickets,
)
from app.services.kaufland_tickets import (
    create_ticket_note,
    delete_ticket_note,
    get_support_ticket_detail,
    list_support_tickets,
    list_ticket_notes,
    resolve_attachment_preview,
    update_ticket_note,
)


router = APIRouter(prefix="/api/kaufland-tickets", tags=["kaufland-tickets"])
ADMIN_ONLY = [Depends(require_admin_access)]


class TicketSyncRequest(BaseModel):
    storefront: str = Field(default="de")
    include_closed: bool = Field(default=True)
    page_limit: int = Field(default=30, ge=1, le=30)
    max_pages: int = Field(default=1000, ge=1, le=5000)
    updated_from_iso: Optional[str] = Field(default=None)
    lookback_minutes: int = Field(default=60, ge=5, le=1440)


class TicketMessageRequest(BaseModel):
    text: str = Field(min_length=1, max_length=65535)
    interim_notice: bool = Field(default=False)


class OpenTicketRequest(BaseModel):
    id_order_unit: list[int] = Field(min_length=1)
    reason: str = Field(min_length=1)
    message: str = Field(min_length=1)


class TicketNoteRequest(BaseModel):
    note_text: str = Field(min_length=1, max_length=10000)


def _to_data_uri(upload: UploadFile, data: bytes) -> dict[str, str]:
    mime_type = str(upload.content_type or "application/octet-stream").strip() or "application/octet-stream"
    encoded = base64.b64encode(data).decode("ascii")
    return {
        "filename": str(upload.filename or "attachment"),
        "mime_type": mime_type,
        "data": f"data:{mime_type};base64,{encoded}",
    }


@router.get("/status")
def api_kaufland_tickets_status() -> dict[str, Any]:
    init_support_db()
    return build_kaufland_tickets_status()


@router.get("")
def api_list_kaufland_tickets(
    filter_mode: str = Query(default="todo", alias="filter"),
    q: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    init_support_db()
    payload = list_support_tickets(filter_mode=filter_mode, q=q, limit=limit, offset=offset)
    return {"filter": filter_mode, **payload, "limit": limit, "offset": offset}


@router.get("/{id_ticket}")
def api_get_kaufland_ticket(id_ticket: str) -> dict[str, Any]:
    init_support_db()
    payload = get_support_ticket_detail(id_ticket)
    if payload is None:
        raise HTTPException(status_code=404, detail="ticket not found")
    return payload


@router.post("/sync/backfill", dependencies=ADMIN_ONLY)
def api_kaufland_tickets_sync_backfill(payload: Optional[TicketSyncRequest] = None) -> dict[str, Any]:
    request = payload or TicketSyncRequest()
    init_support_db()
    return sync_kaufland_tickets(
        mode="backfill",
        options=SupportSyncOptions(
            storefront=request.storefront,
            page_limit=request.page_limit,
            max_pages=request.max_pages,
            include_closed=request.include_closed,
            updated_from_iso=request.updated_from_iso,
            lookback_minutes=request.lookback_minutes,
        ),
    )


@router.post("/sync/poll", dependencies=ADMIN_ONLY)
def api_kaufland_tickets_sync_poll(payload: Optional[TicketSyncRequest] = None) -> dict[str, Any]:
    request = payload or TicketSyncRequest(include_closed=True, page_limit=30, max_pages=50)
    init_support_db()
    return sync_kaufland_tickets(
        mode="poll",
        options=SupportSyncOptions(
            storefront=request.storefront,
            page_limit=request.page_limit,
            max_pages=request.max_pages,
            include_closed=request.include_closed,
            updated_from_iso=request.updated_from_iso,
            lookback_minutes=request.lookback_minutes,
        ),
    )


@router.post("/{id_ticket}/messages", dependencies=ADMIN_ONLY)
async def api_send_kaufland_ticket_message(
    id_ticket: str,
    text: str = Form(...),
    interim_notice: bool = Form(default=False),
    files: list[UploadFile] = File(default_factory=list),
) -> dict[str, Any]:
    init_support_db()
    attachments: list[dict[str, str]] = []
    for upload in files:
        content = await upload.read()
        if not content:
            continue
        attachments.append(_to_data_uri(upload, content))
    try:
        return send_ticket_message(
            id_ticket,
            text=text,
            interim_notice=interim_notice,
            ticket_message_files=attachments or None,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.patch("/{id_ticket}/close", dependencies=ADMIN_ONLY)
def api_close_kaufland_ticket(id_ticket: str) -> dict[str, Any]:
    init_support_db()
    try:
        return close_ticket(id_ticket)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("", dependencies=ADMIN_ONLY)
def api_open_kaufland_ticket(payload: OpenTicketRequest) -> dict[str, Any]:
    init_support_db()
    try:
        return open_ticket(payload.id_order_unit, payload.reason, payload.message)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{id_ticket}/attachments/{filename}/preview", dependencies=ADMIN_ONLY)
def api_preview_kaufland_ticket_attachment(id_ticket: str, filename: str) -> Response:
    init_support_db()
    try:
        content, media_type, download_name = resolve_attachment_preview(id_ticket, filename)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    headers = {
        "Content-Disposition": f'inline; filename="{download_name}"',
        "Cache-Control": "no-store",
    }
    return Response(content=content, media_type=media_type, headers=headers)


@router.get("/{id_ticket}/notes")
def api_list_kaufland_ticket_notes(id_ticket: str) -> dict[str, Any]:
    init_support_db()
    return {"items": list_ticket_notes(id_ticket)}


@router.post("/{id_ticket}/notes", dependencies=ADMIN_ONLY)
def api_create_kaufland_ticket_note(id_ticket: str, payload: TicketNoteRequest) -> dict[str, Any]:
    init_support_db()
    try:
        note = create_ticket_note(id_ticket, payload.note_text)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"ok": True, "note": note}


@router.patch("/{id_ticket}/notes/{note_id}", dependencies=ADMIN_ONLY)
def api_update_kaufland_ticket_note(id_ticket: str, note_id: str, payload: TicketNoteRequest) -> dict[str, Any]:
    init_support_db()
    try:
        note = update_ticket_note(id_ticket, note_id, payload.note_text)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"ok": True, "note": note}


@router.delete("/{id_ticket}/notes/{note_id}", dependencies=ADMIN_ONLY)
def api_delete_kaufland_ticket_note(id_ticket: str, note_id: str) -> dict[str, Any]:
    init_support_db()
    delete_ticket_note(id_ticket, note_id)
    return {"ok": True}
