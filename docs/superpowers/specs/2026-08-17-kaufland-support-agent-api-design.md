# Kaufland Support Agent API Design

## Goal

Allow an autonomous agent to operate Kaufland DE support through the existing
`/api/kaufland-tickets` API. The agent can read the entire locally synchronized
ticket state, manage local notes, send customer messages, open and close
tickets, retrieve attachments, and trigger synchronization. The API does not
enforce a human approval step; operational instructions are the safeguard.

## Scope

- Keep `/api/kaufland-tickets` as the single API surface. Do not add an
  agent-specific router or duplicate data model.
- Publish an agent-facing operational reference that documents every support
  endpoint, authentication, request/response contracts, state semantics,
  error handling, and verified request examples.
- Update the existing backend API reference, dashboard API skill, and
  production API operator instructions together so the agent's source of truth
  cannot drift.
- Document the exact freshness workflow: sync, list, detail, write, verify.
- Preserve all existing human UI behavior.

## Non-Goals

- No approval queue, draft-only mode, or technical restriction on agent sends.
- No cancellation, refund, return-label, or order-fulfillment actions beyond
  normal Kaufland ticket messaging and ticket lifecycle operations.
- No new agent framework, LLM integration, or separate credential type.
- No attempt to infer or manufacture a Kaufland SLA deadline from ticket
  creation time.

## Authentication and Authorization

- The agent uses the dashboard admin token with either `X-Admin-Token` or
  `Authorization: Bearer <token>`.
- The agent sends authentication on every request, including reads, even where
  the current route remains readable without a configured token for local UI
  compatibility.
- Mutating endpoints remain admin-protected.
- Documentation must never include a real token, credential file contents, or
  Kaufland client/secret key.

## Read Contract

### Status and Freshness

- `GET /api/kaufland-tickets/status` returns configuration status, local ticket
  counts, the local support database path, and the latest completed sync run.
- The agent treats `last_sync` as the freshness source of truth. Missing
  `last_sync`, a status of `error`, or a timestamp older than the task's
  required freshness requires a sync before making decisions.
- `POST /api/kaufland-tickets/sync/poll` performs an incremental read from
  Kaufland. The agent calls it before reading a working queue and after every
  successful remote mutation.
- `POST /api/kaufland-tickets/sync/backfill` is reserved for initial/history
  repair. It must not be used as normal per-ticket refresh behavior.

### Inbox and Ticket Detail

- `GET /api/kaufland-tickets?filter=todo|waiting|closed|all&q=...&limit=...&offset=...`
  lists locally synchronized tickets.
- `todo` means `status=opened` and `is_seller_responsible=true`; the seller is
  expected to act.
- `waiting` means `status=opened` and `is_seller_responsible=false`; Kaufland,
  the buyer, or another party is expected to act.
- `closed` is any non-`opened` ticket status.
- `GET /api/kaufland-tickets/{id_ticket}` is mandatory before a mutation. It
  returns the canonical ticket record, complete message history, attachment
  metadata, local notes, raw Kaufland ticket payload, related order-unit IDs,
  and available dashboard order context.
- The agent reads `ticket_raw` when it needs fields not promoted into the
  normalized response. It treats values in the normalized `ticket`,
  `messages`, `attachments`, and `order_context` sections as the preferred
  contract.

### Attachments

- `GET /api/kaufland-tickets/{id_ticket}/attachments/{filename}/preview`
  fetches the attachment on demand. It may fail when Kaufland's remote URL has
  expired or is unavailable.
- The agent uses the metadata from ticket detail first and requests the preview
  only when file content is needed.

## Write Contract

### Local Notes

- `POST /api/kaufland-tickets/{id_ticket}/notes` creates a local-only note.
- `PATCH /api/kaufland-tickets/{id_ticket}/notes/{note_id}` updates a note.
- `DELETE /api/kaufland-tickets/{id_ticket}/notes/{note_id}` removes a note.
- Notes never reach Kaufland and must not be represented as customer-visible
  actions in an agent report.

### Messages

- `POST /api/kaufland-tickets/{id_ticket}/messages` uses multipart form data:
  `text`, optional `interim_notice`, and optional repeated `files` fields.
- A normal message changes Kaufland's responsibility state to waiting where
  Kaufland applies that standard behavior.
- `interim_notice=true` is only for an acknowledgement where the agent must
  deliberately keep seller responsibility open for a later follow-up.
- Supported attachment MIME types and maximum payload limitations are taken
  from Kaufland documentation and must be listed in the operational reference.

### Ticket Lifecycle

- `POST /api/kaufland-tickets` opens a ticket for one or more numeric
  `id_order_unit` values that belong to the same order. Its `reason` must be a
  Kaufland-supported value and `message` is customer-visible.
- `PATCH /api/kaufland-tickets/{id_ticket}/close` closes an existing ticket.
- The agent closes a ticket only after the concrete request is resolved or the
  seller is no longer expected to act. It must record the rationale in a local
  note before closing.

## Agent Operating Rules

1. Poll before selecting a working queue.
2. Read ticket detail immediately before every remote mutation. Never send
   based on a stale list row or previous agent context alone.
3. Check current `status`, `is_seller_responsible`, recent messages, local
   notes, and order context before composing an answer.
4. Avoid duplicate sends: compare the proposed response with the latest seller
   messages and read detail again after any uncertain/timeout outcome before
   retrying.
5. After a successful message/open/close operation, run poll sync and re-read
   the ticket detail to verify the resulting state.
6. Use local notes for reasoning, handoff state, and close rationale. Do not
   include sensitive internal reasoning in a customer message.
7. Do not use ticket creation or messages to perform refunds, cancellations,
   return-label issuance, or other order actions not exposed by this API.
8. Do not treat a locally calculated "overdue" value as a Kaufland SLA. Make
   operational decisions from live status, responsibility, recent history,
   and explicit Kaufland message content.

## Error Semantics

- `401`: missing or invalid dashboard admin token.
- `404`: requested ticket, note, or attachment metadata is absent locally.
- `422`: invalid path/query/body shape; correct the request without retrying
  unchanged.
- `502`: Kaufland provider or synchronization failure. Do not report success;
  preserve the attempted action in a local note when appropriate, poll only
  after the provider recovers, and re-read before retrying.
- A successful HTTP status with sync result `partial` means some ticket details
  did not synchronize. The agent may operate only on records whose required
  detail is present and should report the partial result.

## Documentation Deliverables

- Add a dedicated `Kaufland Support Agent API` section to
  `docs/dashboard-backend-api.md`.
- Update `.opencode/skills/dashboard-backend-api/SKILL.md` and
  `.opencode/agents/dashboard-production-api-operator.md` to route support
  tasks to the reference and state the operating rules.
- Include copyable `curl` examples for status, poll, list, detail, note CRUD,
  multipart message send, open, close, and attachment preview.
- Include complete response examples stripped of real customer data.
- Include a troubleshooting table for stale data, `partial` syncs, `502`,
  expired attachment URLs, duplicate-send uncertainty, and responsibility
  changes after messages.
- Treat these documents as part of the API contract: endpoint/schema changes
  require matching documentation updates in the same change.

## Verification

- Backend tests cover status/list/detail, note CRUD, attachment preview,
  authentication, and provider-sync error propagation.
- Add lifecycle tests for an agent-style sequence: poll, list, detail, note,
  send message, verify refresh, and close. Remote Kaufland calls are replaced
  at the transport boundary only; route validation and local persistence stay
  real.
- Add tests for the Open Ticket allowed-reason validation and multipart
  attachment validation.
- Add a documentation example test or fixture-driven smoke script that runs
  all documented request shapes against `TestClient` without live Kaufland
  access.
- Run the support backend suite, frontend typecheck, production frontend build,
  and the `/support` browser smoke test.
