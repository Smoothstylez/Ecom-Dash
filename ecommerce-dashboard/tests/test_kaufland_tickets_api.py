from __future__ import annotations

import importlib
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient


def _seed_support_db(db_path: Path) -> None:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE tickets (
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

        CREATE TABLE ticket_order_units (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            id_ticket TEXT NOT NULL,
            id_order_unit TEXT NOT NULL,
            position INTEGER NOT NULL,
            synced_at_iso TEXT NOT NULL
        );

        CREATE TABLE ticket_messages (
            id_ticket_message TEXT PRIMARY KEY,
            id_ticket TEXT NOT NULL,
            author_role TEXT,
            author_name TEXT,
            text TEXT,
            ts_created_iso TEXT,
            direction TEXT,
            raw_json TEXT NOT NULL,
            synced_at_iso TEXT NOT NULL
        );

        CREATE TABLE ticket_attachments_meta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            id_ticket TEXT NOT NULL,
            filename TEXT NOT NULL,
            uri TEXT NOT NULL,
            ts_created_iso TEXT,
            synced_at_iso TEXT NOT NULL
        );

        CREATE TABLE ticket_internal_notes (
            id TEXT PRIMARY KEY,
            id_ticket TEXT NOT NULL,
            note_text TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE ticket_sync_runs (
            id_sync_run INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_started_iso TEXT NOT NULL,
            ts_finished_iso TEXT NOT NULL,
            storefront TEXT NOT NULL,
            status TEXT NOT NULL,
            mode TEXT NOT NULL,
            error_count INTEGER NOT NULL,
            summary_json TEXT NOT NULL
        );
        """
    )

    connection.execute(
        """
        INSERT INTO tickets (
            id_ticket, storefront, id_buyer, status, open_reason, topic,
            fulfillment_type, is_seller_responsible, ts_created_iso, ts_updated_iso,
            first_response_due_at, first_response_sent_at, order_units_count,
            last_message_at_iso, raw_json, synced_at_iso
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "T-100",
            "de",
            "buyer-1",
            "opened",
            "product_not_delivered",
            "delivery_not_received",
            "fulfilled_by_merchant",
            1,
            "2026-06-15T09:00:00Z",
            "2026-06-15T10:00:00Z",
            "2026-06-17T09:00:00Z",
            None,
            1,
            "2026-06-15T10:00:00Z",
            '{"id_ticket":"T-100","order_units":[{"id_order":"K-ORDER-1"}],"messages":[{"id_ticket_message":"M-1"}],"files":[{"filename":"proof.pdf","uri":"https://files.example/proof.pdf"}]}',
            "2026-06-15T10:00:00Z",
        ),
    )
    connection.execute(
        "INSERT INTO ticket_order_units (id_ticket, id_order_unit, position, synced_at_iso) VALUES (?, ?, ?, ?)",
        ("T-100", "OU-1", 0, "2026-06-15T10:00:00Z"),
    )
    connection.execute(
        """
        INSERT INTO ticket_messages (
            id_ticket_message, id_ticket, author_role, author_name, text,
            ts_created_iso, direction, raw_json, synced_at_iso
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "M-1",
            "T-100",
            "buyer",
            "Alice",
            "Where is my order?",
            "2026-06-15T10:00:00Z",
            "inbound",
            '{"id_ticket_message":"M-1","author":{"role":"buyer","name":"Alice"},"text":"Where is my order?"}',
            "2026-06-15T10:00:00Z",
        ),
    )
    connection.execute(
        "INSERT INTO ticket_attachments_meta (id_ticket, filename, uri, ts_created_iso, synced_at_iso) VALUES (?, ?, ?, ?, ?)",
        ("T-100", "proof.pdf", "https://files.example/proof.pdf", "2026-06-15T10:01:00Z", "2026-06-15T10:01:00Z"),
    )
    connection.execute(
        "INSERT INTO ticket_internal_notes (id, id_ticket, note_text, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        ("note-1", "T-100", "Internal follow-up", "2026-06-15T10:02:00Z", "2026-06-15T10:02:00Z"),
    )
    connection.execute(
        "INSERT INTO ticket_sync_runs (ts_started_iso, ts_finished_iso, storefront, status, mode, error_count, summary_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            "2026-06-15T09:55:00Z",
            "2026-06-15T10:05:00Z",
            "de",
            "success",
            "poll",
            0,
            '{"tickets_seen":1}',
        ),
    )
    connection.commit()
    connection.close()


def _build_client(monkeypatch, tmp_path: Path) -> TestClient:
    support_db = tmp_path / "support.sqlite3"
    monkeypatch.setenv("AUTO_SYNC_ON_STARTUP", "0")
    monkeypatch.setenv("APP_ADMIN_TOKEN", "test-token")
    monkeypatch.setenv("SUPPORT_DB_PATH", str(support_db))

    import app.config as config_module
    import app.services.importers.kaufland_tickets as importer_module
    import app.services.kaufland_tickets as service_module
    import app.routers.kaufland_tickets as router_module
    import app.main as main_module

    importlib.reload(config_module)
    importlib.reload(importer_module)
    importlib.reload(service_module)
    importlib.reload(router_module)
    importlib.reload(main_module)

    _seed_support_db(support_db)
    return TestClient(main_module.app)


def test_support_status_and_list(monkeypatch, tmp_path: Path) -> None:
    client = _build_client(monkeypatch, tmp_path)

    response = client.get("/api/kaufland-tickets/status")
    assert response.status_code == 200
    payload = response.json()
    assert payload["counts"]["tickets_total"] == 1
    assert payload["counts"]["tickets_open"] == 1
    assert payload["counts"]["notes_total"] == 1

    response = client.get("/api/kaufland-tickets?filter=todo&q=order")
    assert response.status_code == 200
    list_payload = response.json()
    assert list_payload["total"] == 1
    assert list_payload["items"][0]["id_ticket"] == "T-100"
    assert list_payload["items"][0]["counts"]["messages"] == 1


def test_support_detail_and_notes_crud(monkeypatch, tmp_path: Path) -> None:
    client = _build_client(monkeypatch, tmp_path)
    admin_headers = {"x-admin-token": "test-token"}

    response = client.get("/api/kaufland-tickets/T-100")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ticket"]["id_ticket"] == "T-100"
    assert payload["order_unit_ids"] == ["OU-1"]
    assert payload["messages"][0]["id_ticket_message"] == "M-1"
    assert payload["attachments"][0]["filename"] == "proof.pdf"
    assert payload["notes"][0]["note_text"] == "Internal follow-up"

    response = client.post(
        "/api/kaufland-tickets/T-100/notes",
        headers=admin_headers,
        json={"note_text": "Need courier confirmation"},
    )
    assert response.status_code == 200
    created = response.json()["note"]
    assert created["id_ticket"] == "T-100"
    assert created["note_text"] == "Need courier confirmation"

    note_id = created["id"]
    response = client.patch(
        f"/api/kaufland-tickets/T-100/notes/{note_id}",
        headers=admin_headers,
        json={"note_text": "Need courier confirmation ASAP"},
    )
    assert response.status_code == 200
    assert response.json()["note"]["note_text"] == "Need courier confirmation ASAP"

    response = client.delete(
        f"/api/kaufland-tickets/T-100/notes/{note_id}",
        headers=admin_headers,
    )
    assert response.status_code == 200
    assert response.json()["ok"] is True


def test_support_preview_and_auth(monkeypatch, tmp_path: Path) -> None:
    client = _build_client(monkeypatch, tmp_path)

    response = client.post("/api/kaufland-tickets/sync/poll", json={})
    assert response.status_code == 401

    response = client.get("/api/kaufland-tickets/T-100/attachments/proof.pdf/preview")
    assert response.status_code == 401

    import app.routers.kaufland_tickets as router_module

    def fake_preview(id_ticket: str, filename: str):
        assert id_ticket == "T-100"
        assert filename == "proof.pdf"
        return b"%PDF-test", "application/pdf", "proof.pdf"

    monkeypatch.setattr(router_module, "resolve_attachment_preview", fake_preview)

    response = client.get(
        "/api/kaufland-tickets/T-100/attachments/proof.pdf/preview",
        headers={"x-admin-token": "test-token"},
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/pdf")
    assert response.headers["content-disposition"] == 'inline; filename="proof.pdf"'
    assert response.content == b"%PDF-test"


def test_support_preview_missing_remote_is_404(monkeypatch, tmp_path: Path) -> None:
    client = _build_client(monkeypatch, tmp_path)

    response = client.get(
        "/api/kaufland-tickets/T-100/attachments/proof.pdf/preview",
        headers={"x-admin-token": "test-token"},
    )
    assert response.status_code == 404
    assert "attachment preview unavailable" in response.json()["detail"]


def test_support_poll_surfaces_provider_failures(monkeypatch, tmp_path: Path) -> None:
    client = _build_client(monkeypatch, tmp_path)
    import app.routers.kaufland_tickets as router_module

    monkeypatch.setattr(
        router_module,
        "sync_kaufland_tickets",
        lambda **_: {"status": "error", "error": "Validation Failed"},
    )

    response = client.post(
        "/api/kaufland-tickets/sync/poll",
        headers={"x-admin-token": "test-token"},
        json={},
    )

    assert response.status_code == 502
    assert response.json()["detail"] == "Validation Failed"


def test_open_ticket_rejects_unknown_kaufland_reason(monkeypatch, tmp_path: Path) -> None:
    client = _build_client(monkeypatch, tmp_path)

    response = client.post(
        "/api/kaufland-tickets",
        headers={"x-admin-token": "test-token"},
        json={
            "id_order_unit": [314568008668014],
            "reason": "refund_now",
            "message": "Please issue a refund.",
        },
    )

    assert response.status_code == 422
    assert "reason" in response.text


def test_send_ticket_message_rejects_unsupported_attachment_mime_type(monkeypatch, tmp_path: Path) -> None:
    client = _build_client(monkeypatch, tmp_path)

    response = client.post(
        "/api/kaufland-tickets/T-100/messages",
        headers={"x-admin-token": "test-token"},
        data={"text": "See attachment", "interim_notice": "false"},
        files={"files": ("script.exe", b"MZ", "application/x-msdownload")},
    )

    assert response.status_code == 422
    assert response.json()["detail"] == "unsupported ticket attachment MIME type: application/x-msdownload"


def test_send_ticket_message_rejects_attachment_larger_than_twelve_mib(monkeypatch, tmp_path: Path) -> None:
    client = _build_client(monkeypatch, tmp_path)
    oversized_content = b"x" * (12 * 1024 * 1024 + 1)

    response = client.post(
        "/api/kaufland-tickets/T-100/messages",
        headers={"x-admin-token": "test-token"},
        data={"text": "See attachment", "interim_notice": "false"},
        files={"files": ("large.pdf", oversized_content, "application/pdf")},
    )

    assert response.status_code == 422
    assert response.json()["detail"] == "ticket attachment exceeds 12 MiB limit: large.pdf"


def test_agent_can_poll_read_note_reply_and_close_ticket(monkeypatch, tmp_path: Path) -> None:
    client = _build_client(monkeypatch, tmp_path)
    admin_headers = {"x-admin-token": "test-token"}
    import app.routers.kaufland_tickets as router_module

    monkeypatch.setattr(router_module, "sync_kaufland_tickets", lambda **_: {"status": "success", "summary": {"tickets_seen": 1}})
    monkeypatch.setattr(
        router_module,
        "send_ticket_message",
        lambda ticket_id, **payload: {"ok": True, "id_ticket": ticket_id, "sent_text": payload["text"]},
    )
    monkeypatch.setattr(router_module, "close_ticket", lambda ticket_id: {"ok": True, "id_ticket": ticket_id})

    assert client.post("/api/kaufland-tickets/sync/poll", headers=admin_headers, json={}).status_code == 200
    listed = client.get("/api/kaufland-tickets?filter=todo", headers=admin_headers).json()
    assert listed["items"][0]["id_ticket"] == "T-100"
    assert client.get("/api/kaufland-tickets/T-100", headers=admin_headers).json()["messages"][0]["text"] == "Where is my order?"

    note_response = client.post(
        "/api/kaufland-tickets/T-100/notes",
        headers=admin_headers,
        json={"note_text": "Agent checked shipment status before replying."},
    )
    assert note_response.status_code == 200

    reply_response = client.post(
        "/api/kaufland-tickets/T-100/messages",
        headers=admin_headers,
        data={"text": "Your shipment is being checked.", "interim_notice": "true"},
    )
    assert reply_response.status_code == 200
    assert reply_response.json()["sent_text"] == "Your shipment is being checked."

    close_response = client.patch("/api/kaufland-tickets/T-100/close", headers=admin_headers)
    assert close_response.status_code == 200
    assert close_response.json()["id_ticket"] == "T-100"


def test_open_ticket_accepts_documented_kaufland_reason(monkeypatch, tmp_path: Path) -> None:
    client = _build_client(monkeypatch, tmp_path)
    import app.routers.kaufland_tickets as router_module

    monkeypatch.setattr(
        router_module,
        "open_ticket",
        lambda unit_ids, reason, message: {
            "ok": True,
            "id_order_unit": unit_ids,
            "reason": reason,
            "message": message,
        },
    )

    response = client.post(
        "/api/kaufland-tickets",
        headers={"x-admin-token": "test-token"},
        json={
            "id_order_unit": [314568008668014],
            "reason": "product_return",
            "message": "Please provide a return option.",
        },
    )

    assert response.status_code == 200
    assert response.json()["reason"] == "product_return"
