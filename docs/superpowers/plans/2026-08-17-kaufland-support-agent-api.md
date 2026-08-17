# Kaufland Support Agent API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the existing Kaufland support API safe and fully documented for an autonomous agent that can read, synchronize, annotate, reply to, open, and close tickets.

**Architecture:** Keep `/api/kaufland-tickets` as the only support API. Harden its existing mutation boundary with request validation and provider-error semantics, then document the exact read-before-write and sync-after-write operating contract in the main dashboard API reference. Reference that contract from the repository README, API skill, and production operator agent so all agent entry points converge on one source of truth.

**Tech Stack:** Python 3.12, FastAPI, Pydantic, SQLite, pytest, React/TypeScript, Vite, Markdown, curl.

## Global Constraints

- Do not add an agent-specific API router, draft queue, or human approval gate.
- The agent may directly send Kaufland messages, create tickets, close tickets, and manage local notes with the existing dashboard admin token.
- Do not expose or document real dashboard tokens, Kaufland client keys, Kaufland secret keys, customer data, or attachment URLs.
- Keep `/api/kaufland-tickets` as the only support API prefix.
- Treat `last_sync` from `GET /api/kaufland-tickets/status` as the local-data freshness indicator.
- A normal agent workflow is poll, list, detail, mutate, poll, then detail verification.
- Do not describe locally calculated `first_response_due_at` as an authoritative Kaufland SLA.
- Allowed message attachment MIME types are `text/plain`, `image/png`, `image/jpeg`, `image/gif`, `image/tiff`, `application/pdf`, `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`, `application/vnd.openxmlformats-officedocument.wordprocessingml.document`, and `application/msword`.
- Enforce the existing `MAX_UPLOAD_BYTES` limit of 12 MiB for each uploaded support attachment before base64 encoding.

---

### Task 1: Validate Agent-Initiated Ticket Writes at the Router Boundary

**Files:**
- Modify: `ecommerce-dashboard/app/routers/kaufland_tickets.py:1-227`
- Modify: `ecommerce-dashboard/tests/test_kaufland_tickets_api.py`

**Interfaces:**
- Produces `OPEN_TICKET_REASONS: frozenset[str]` containing `product_not_as_described`, `product_defect`, `product_not_delivered`, `product_return`, and `contact_other`.
- Produces `ALLOWED_TICKET_ATTACHMENT_MIME_TYPES: frozenset[str]` with the nine values in Global Constraints.
- `POST /api/kaufland-tickets` rejects unsupported `reason` values with HTTP `422`.
- `POST /api/kaufland-tickets/{id_ticket}/messages` rejects unsupported file MIME types and files larger than 12 MiB with HTTP `422`, before calling `send_ticket_message`.

- [ ] **Step 1: Write failing validation tests.**

Add these tests to `tests/test_kaufland_tickets_api.py`. They name the breaks: accepting a Kaufland-invalid ticket reason, accepting a file Kaufland will reject, and base64-encoding a file beyond the dashboard upload limit.

```python
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
```

- [ ] **Step 2: Run the focused tests and verify they fail.**

Run: `python3 -m pytest tests/test_kaufland_tickets_api.py -k 'unknown_kaufland_reason or unsupported_attachment or larger_than_twelve' -v`

Expected: FAIL because `OpenTicketRequest` accepts any non-empty reason and the message route accepts every uploaded MIME type and size.

- [ ] **Step 3: Add Pydantic reason validation and upload validation.**

In `app/routers/kaufland_tickets.py`, import `MAX_UPLOAD_BYTES` from `app.config`. Define the two `frozenset` constants immediately below `ADMIN_ONLY`. Replace the current unconstrained `reason: str` with a Pydantic field that accepts only `OPEN_TICKET_REASONS`. Use a `field_validator` if the installed Pydantic version requires explicit validation; trim/lowercase the input and raise `ValueError("unsupported Kaufland ticket reason")` for invalid values.

Before `_to_data_uri(upload, content)`, reject empty files as today, then apply:

```python
mime_type = str(upload.content_type or "application/octet-stream").strip().lower()
if mime_type not in ALLOWED_TICKET_ATTACHMENT_MIME_TYPES:
    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        detail=f"unsupported ticket attachment MIME type: {mime_type}",
    )
if len(content) > MAX_UPLOAD_BYTES:
    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        detail=f"ticket attachment exceeds 12 MiB limit: {upload.filename or 'attachment'}",
    )
```

Do not call `send_ticket_message` when validation fails. Preserve valid multipart behavior and existing admin protection.

- [ ] **Step 4: Run the focused validation tests and the full support API suite.**

Run: `python3 -m pytest tests/test_kaufland_tickets_api.py -v`

Expected: PASS. Existing note, preview, authentication, and provider-error tests remain green.

- [ ] **Step 5: Commit the focused validation change.**

```bash
git add ecommerce-dashboard/app/routers/kaufland_tickets.py ecommerce-dashboard/tests/test_kaufland_tickets_api.py
git commit -m "fix: validate Kaufland support ticket writes"
```

### Task 2: Add an Agent-Style Support Lifecycle Regression Test

**Files:**
- Modify: `ecommerce-dashboard/tests/test_kaufland_tickets_api.py`

**Interfaces:**
- The existing routes form the agent workflow: `POST /sync/poll`, `GET /`, `GET /{id_ticket}`, `POST /{id_ticket}/notes`, `POST /{id_ticket}/messages`, and `PATCH /{id_ticket}/close`.
- Provider calls are mocked only at the imported router functions; route validation, authentication, and local ticket persistence remain real.

- [ ] **Step 1: Write the failing lifecycle test.**

Add this test. The production change it catches is a route that stops accepting the documented agent sequence or returns success without using the current ticket ID and message fields.

```python
def test_agent_can_poll_read_note_reply_and_close_ticket(monkeypatch, tmp_path: Path) -> None:
    client = _build_client(monkeypatch, tmp_path)
    admin_headers = {"x-admin-token": "test-token"}
    import app.routers.kaufland_tickets as router_module

    monkeypatch.setattr(router_module, "sync_kaufland_tickets", lambda **_: {"status": "success", "summary": {"tickets_seen": 1}})
    monkeypatch.setattr(router_module, "send_ticket_message", lambda ticket_id, **payload: {"ok": True, "id_ticket": ticket_id, "sent_text": payload["text"]})
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
```

- [ ] **Step 2: Run the lifecycle test as a characterization check.**

Run: `python3 -m pytest tests/test_kaufland_tickets_api.py -k 'agent_can_poll_read_note_reply_and_close' -v`

Expected: PASS if the documented route sequence, authentication rule, multipart field, and mutation response contracts already match the agent workflow. If it fails, correct the test fixture seam or the API contract before documenting the workflow.

- [ ] **Step 3: Adjust only test fixture seams needed for real route execution.**

Keep the current `_build_client` and SQLite fixture. If the test fails because the support router imported a provider function under a different name, patch that exact router module symbol. Do not add test-only production methods or bypass FastAPI request validation.

- [ ] **Step 4: Run the lifecycle test and full support API suite.**

Run: `python3 -m pytest tests/test_kaufland_tickets_api.py -v`

Expected: PASS with a lifecycle test that exercises real HTTP route parsing, admin authentication, note persistence, multipart parsing, and response shapes.

- [ ] **Step 5: Commit the lifecycle regression coverage.**

```bash
git add ecommerce-dashboard/tests/test_kaufland_tickets_api.py
git commit -m "test: cover Kaufland support agent lifecycle"
```

### Task 3: Expand the Agent-Facing Support API Reference

**Files:**
- Modify: `docs/dashboard-backend-api.md:1059-1085`
- Modify: `scripts/dashboard-api/README.md:12-26`

**Interfaces:**
- `docs/dashboard-backend-api.md` becomes the canonical complete operational reference for `Support Tickets API`.
- `scripts/dashboard-api/README.md` points support-ticket operators to that canonical section and does not invent an incomplete parallel contract.

- [ ] **Step 1: Write the Support Tickets API reference section.**

Replace the terse endpoint list at `docs/dashboard-backend-api.md:1059-1085` with a full section containing these exact subsections:

```markdown
## Kaufland Support Agent API

### Required operating sequence
1. Read `GET /api/kaufland-tickets/status`.
2. Run `POST /api/kaufland-tickets/sync/poll` when `last_sync` is missing, stale, or before a working queue is selected.
3. List with `GET /api/kaufland-tickets?filter=todo`.
4. Read `GET /api/kaufland-tickets/{id_ticket}` immediately before each remote mutation.
5. After message, open, or close: poll again and re-read detail.
```

Document all 13 support routes with method, auth requirement, body type, payload fields, response fields, and error statuses. Include sanitized `curl --fail-with-body -sS` examples for status, poll, list, detail, all note operations, multipart message send, open, close, and attachment preview.

Use these precise facts:

- Filters: `todo` = `opened` plus `is_seller_responsible=true`; `waiting` = `opened` plus `is_seller_responsible=false`; `closed` = any other status.
- Detail fields: `ticket`, `ticket_raw`, `order_unit_ids`, `messages`, `attachments`, `notes`, `order_context`.
- Open reasons: `product_not_as_described`, `product_defect`, `product_not_delivered`, `product_return`, `contact_other`.
- Sync uses `page_limit` from 1 to 30; `poll` is incremental and `backfill` is history repair.
- Message upload uses repeated `files` multipart fields, each capped at 12 MiB and restricted to the nine MIME types in Global Constraints.
- `interim_notice=true` intentionally preserves seller responsibility for a later follow-up.
- HTTP errors: `401`, `404`, `422`, `502`; successful sync response may still have `status: partial`.
- Local notes never reach Kaufland.
- Do not state `first_response_due_at` as an authoritative SLA.

Use generated placeholder values such as `T-100`, `314568008668014`, and `example.pdf`; do not use production ticket, order-unit, customer, message, or URL data.

- [ ] **Step 2: Add README discovery text.**

Under `Helpers:` in `scripts/dashboard-api/README.md`, add:

```markdown
Kaufland support:

- The full agent workflow and all `/api/kaufland-tickets` request examples live in `docs/dashboard-backend-api.md` under `Kaufland Support Agent API`.
- Support is operated through direct API calls; no shell helper is provided because the workflow requires a fresh detail read and a verify-after-write sequence per ticket.
```

- [ ] **Step 3: Verify the documented request shapes through the lifecycle test.**

Keep the sanitized request examples in the documentation structurally identical to the route requests in `test_agent_can_poll_read_note_reply_and_close_ticket`: admin header, JSON note payload, multipart `text`/`interim_notice` message payload, poll request, and close request. Add the Open Ticket JSON request shape to `test_open_ticket_rejects_unknown_kaufland_reason` as a valid companion request after the invalid-reason assertion:

```python
monkeypatch.setattr(router_module, "open_ticket", lambda unit_ids, reason, message: {
    "ok": True,
    "id_order_unit": unit_ids,
    "reason": reason,
    "message": message,
})
valid_response = client.post(
    "/api/kaufland-tickets",
    headers={"x-admin-token": "test-token"},
    json={
        "id_order_unit": [314568008668014],
        "reason": "product_return",
        "message": "Please provide a return option.",
    },
)
assert valid_response.status_code == 200
assert valid_response.json()["reason"] == "product_return"
```

Run: `python3 -m pytest tests/test_kaufland_tickets_api.py -v`

Expected: PASS. The documented payload examples match actual routes.

- [ ] **Step 4: Commit the canonical reference and README discovery update.**

```bash
git add docs/dashboard-backend-api.md scripts/dashboard-api/README.md
git commit -m "docs: document Kaufland support agent API"
```

### Task 4: Route Agents to the Canonical Support Contract

**Files:**
- Modify: `.opencode/skills/dashboard-backend-api/SKILL.md:11-40`
- Modify: `.opencode/agents/dashboard-production-api-operator.md:9-136`

**Interfaces:**
- The `dashboard-backend-api` skill explicitly directs all Kaufland inbox operations to `docs/dashboard-backend-api.md#kaufland-support-agent-api`.
- The production API operator includes the agent operating sequence and does not treat a successful HTTP status with `status: partial` as a complete sync.

- [ ] **Step 1: Update the backend API skill.**

Replace the current single-line support guidance with:

```markdown
For any Kaufland support task, read `docs/dashboard-backend-api.md` section
`Kaufland Support Agent API` before making a call. It is the canonical contract
for ticket freshness, full history reads, local notes, attachment previews,
direct message sends, ticket opening, ticket closing, allowed attachment types,
and verification after every remote mutation.
```

Add these core rules beneath the existing read/write rules:

```markdown
- For support work: poll, list, detail, mutate, poll, then detail verification.
- Do not send or close based only on a cached list row.
- Treat a sync result with `status: partial` as incomplete, even if the HTTP response is successful.
- Do not interpret `first_response_due_at` as a confirmed Kaufland SLA.
```

- [ ] **Step 2: Update the production API operator.**

Replace the existing four Support Tickets bullets with these exact operational requirements:

```markdown
- Before selecting work, read `/api/kaufland-tickets/status` and run `/sync/poll` when `last_sync` is missing or stale.
- Read `GET /api/kaufland-tickets/{id_ticket}` immediately before every message, open, close, or note mutation.
- The agent is allowed to directly send messages, open tickets, close tickets, and manage local notes; follow the canonical sequence in `docs/dashboard-backend-api.md#kaufland-support-agent-api`.
- After a message, open, or close response, poll and re-read ticket detail before reporting completion.
- Local notes are never customer-visible. `interim_notice=true` keeps seller responsibility open intentionally.
- Do not report a `partial` support sync as complete, and never use local overdue calculations as a Kaufland SLA.
- Send admin authentication headers for every support route, including status, list, detail, and attachment preview.
```

- [ ] **Step 3: Verify references resolve and no stale generic guidance remains.**

Run:

```bash
rg -n "Kaufland Support Agent API|poll, list, detail|status: partial|first_response_due_at" \
  docs/dashboard-backend-api.md \
  .opencode/skills/dashboard-backend-api/SKILL.md \
  .opencode/agents/dashboard-production-api-operator.md \
  scripts/dashboard-api/README.md
```

Expected: all four files contain a direct or explicit reference to the canonical support contract; no support instruction tells the agent to act only from a list row.

- [ ] **Step 4: Commit the skill and operator routing changes.**

```bash
git add .opencode/skills/dashboard-backend-api/SKILL.md .opencode/agents/dashboard-production-api-operator.md
git commit -m "docs: route agents through support API contract"
```

### Task 5: Final Contract Verification

**Files:**
- No planned source edits. Resolve any failure in the task where its contract was introduced, then re-run this verification task.

- [ ] **Step 1: Run backend contract tests.**

Run: `python3 -m pytest tests/test_kaufland_tickets_api.py -v`

Expected: PASS, including the provider error, validation, attachment, note, and lifecycle tests.

- [ ] **Step 2: Generate OpenAPI and verify documented support paths.**

Run:

```bash
python3 - <<'PY'
from app.main import app

paths = app.openapi()["paths"]
required = {
    "/api/kaufland-tickets/status",
    "/api/kaufland-tickets",
    "/api/kaufland-tickets/{id_ticket}",
    "/api/kaufland-tickets/sync/poll",
    "/api/kaufland-tickets/sync/backfill",
    "/api/kaufland-tickets/{id_ticket}/messages",
    "/api/kaufland-tickets/{id_ticket}/close",
    "/api/kaufland-tickets/{id_ticket}/attachments/{filename}/preview",
    "/api/kaufland-tickets/{id_ticket}/notes",
    "/api/kaufland-tickets/{id_ticket}/notes/{note_id}",
}
assert not required.difference(paths), required.difference(paths)
print("support OpenAPI contract verified")
PY
```

Expected: `support OpenAPI contract verified`.

- [ ] **Step 3: Run frontend checks because support documentation and validation changes must not break the shipped dashboard.**

Run from `frontend/`: `npm run typecheck && npm run build`

Expected: TypeScript succeeds, Vite production build succeeds, and `ecommerce-dashboard/frontend_dist` receives the updated build.

- [ ] **Step 4: Run the support browser smoke test.**

Start a local server in `ecommerce-dashboard/` with `AUTO_SYNC_ON_STARTUP=0 python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8012`, wait until `curl --fail-with-body -sS http://127.0.0.1:8012/api/kaufland-tickets/status` succeeds, then run from `frontend/`:

```bash
npx playwright test e2e/dashboard-smoke.spec.ts --grep "boots /support"
```

Expected: PASS with `#supportPanel[data-react-support-mounted="true"]` and no page errors.

- [ ] **Step 5: Inspect the final worktree and commit only intended agent-contract files.**

Run:

```bash
git status --short
git diff --check
git diff -- ecommerce-dashboard/app/routers/kaufland_tickets.py ecommerce-dashboard/tests/test_kaufland_tickets_api.py docs/dashboard-backend-api.md .opencode/skills/dashboard-backend-api/SKILL.md .opencode/agents/dashboard-production-api-operator.md scripts/dashboard-api/README.md
```

Expected: no whitespace errors and no unrelated data, build, or concurrent-work files staged. Commit only the files changed by Tasks 1 through 4:

```bash
git add ecommerce-dashboard/app/routers/kaufland_tickets.py ecommerce-dashboard/tests/test_kaufland_tickets_api.py docs/dashboard-backend-api.md .opencode/skills/dashboard-backend-api/SKILL.md .opencode/agents/dashboard-production-api-operator.md scripts/dashboard-api/README.md
git commit -m "feat: document Kaufland support agent operations"
```
