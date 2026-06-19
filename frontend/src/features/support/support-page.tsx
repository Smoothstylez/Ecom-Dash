import { useDashboardRuntime } from "@/app/dashboard-runtime";
import { useDashboardShellState } from "@/app/dashboard-shell-state";
import { formatMoneyFromCents } from "@/features/analytics/format";
import { buildDashboardApiUrl } from "@/shared/runtime/base-path";
import { useEffect, useMemo, useRef, useState } from "react";

import {
  fetchSupportAttachmentPreview,
  closeSupportTicket,
  createSupportNote,
  deleteSupportNote,
  fetchSupportStatus,
  fetchSupportTicketDetail,
  fetchSupportTickets,
  runSupportBackfill,
  runSupportPoll,
  sendSupportMessage,
  type SupportFilterMode,
  type SupportStatusResponse,
  type SupportTicketAttachment,
  type SupportTicketDetailResponse,
  type SupportTicketNote,
  type SupportTicketSummary,
} from "./api";

type StatusMessage = {
  text: string;
  level: "info" | "ok" | "error";
};

type SupportPageProps = {
  isActive: boolean;
};

const POLL_INTERVAL_MS = 300_000;

function formatDateTime(value: string | undefined) {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return `${new Intl.DateTimeFormat("de-DE").format(date)} ${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
}

function humanizeStatus(value: string | undefined) {
  const token = String(value || "").trim().toLowerCase();
  if (!token) {
    return "-";
  }
  if (token === "opened") {
    return "Offen";
  }
  if (token === "buyer_closed") {
    return "Kunde geschlossen";
  }
  if (token === "seller_closed") {
    return "Von mir geschlossen";
  }
  if (token === "both_closed") {
    return "Beidseitig geschlossen";
  }
  if (token === "customer_service_closed_final") {
    return "Final geschlossen";
  }
  return token;
}

function humanizeTopic(value: string | undefined) {
  return String(value || "-").trim().replace(/_/g, " ");
}

function dueState(value: string | undefined) {
  const token = String(value || "").trim();
  if (!token) {
    return { label: "-", level: "normal" as const };
  }
  const due = new Date(token);
  if (Number.isNaN(due.getTime())) {
    return { label: token, level: "normal" as const };
  }
  const diffMs = due.getTime() - Date.now();
  const diffHours = Math.round(diffMs / (1000 * 60 * 60));
  if (diffMs < 0) {
    return { label: `${Math.abs(diffHours)}h ueberfaellig`, level: "danger" as const };
  }
  if (diffHours <= 6) {
    return { label: `${diffHours}h uebrig`, level: "danger" as const };
  }
  if (diffHours <= 24) {
    return { label: `${diffHours}h uebrig`, level: "warn" as const };
  }
  return { label: `${diffHours}h uebrig`, level: "normal" as const };
}

function statusClass(level: StatusMessage["level"]) {
  return level === "error" ? "status-error" : level === "ok" ? "status-ok" : "status-info";
}

function textValue(value: unknown) {
  return String(value || "").trim();
}

function orderSummary(detail: SupportTicketDetailResponse | null) {
  const summary = detail?.order_context && typeof detail.order_context === "object"
    ? (detail.order_context as Record<string, unknown>).summary
    : null;
  return summary && typeof summary === "object" ? summary as Record<string, unknown> : null;
}

function orderUnitsFromContext(detail: SupportTicketDetailResponse | null) {
  const context = detail?.order_context && typeof detail.order_context === "object"
    ? detail.order_context as Record<string, unknown>
    : null;
  return Array.isArray(context?.units) ? context?.units as Array<Record<string, unknown>> : [];
}

function revokePreviewObjectUrl(ref: { current: string }) {
  if (!ref.current) {
    return;
  }
  URL.revokeObjectURL(ref.current);
  ref.current = "";
}

export function SupportPage({ isActive }: SupportPageProps) {
  const { filters: shellFilters, refreshRequestToken } = useDashboardShellState();
  const { previewModalApi } = useDashboardRuntime();
  const [filterMode, setFilterMode] = useState<SupportFilterMode>("todo");
  const [tickets, setTickets] = useState<SupportTicketSummary[]>([]);
  const [ticketsLoading, setTicketsLoading] = useState(true);
  const [ticketsError, setTicketsError] = useState("");
  const [selectedTicketId, setSelectedTicketId] = useState("");
  const [detail, setDetail] = useState<SupportTicketDetailResponse | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");
  const [status, setStatus] = useState<SupportStatusResponse | null>(null);
  const [statusMessage, setStatusMessage] = useState<StatusMessage | null>(null);
  const [messageText, setMessageText] = useState("");
  const [interimNotice, setInterimNotice] = useState(false);
  const [messageFiles, setMessageFiles] = useState<File[]>([]);
  const [sending, setSending] = useState(false);
  const [closing, setClosing] = useState(false);
  const [noteDraft, setNoteDraft] = useState("");
  const [noteBusy, setNoteBusy] = useState(false);
  const pollTimerRef = useRef<number | null>(null);
  const ticketsAbortRef = useRef<AbortController | null>(null);
  const detailAbortRef = useRef<AbortController | null>(null);
  const previewObjectUrlRef = useRef("");
  const selectedTicket = useMemo(() => tickets.find((ticket) => ticket.id_ticket === selectedTicketId) || null, [tickets, selectedTicketId]);
  const searchQuery = shellFilters.q ?? "";

  async function reloadStatus() {
    try {
      const nextStatus = await fetchSupportStatus();
      setStatus(nextStatus);
    } catch {
      // keep last known status
    }
  }

  async function reloadTickets(signal?: AbortSignal, preserveSelection = true) {
    setTicketsLoading(true);
    try {
      const payload = await fetchSupportTickets(filterMode, searchQuery, signal);
      const items = Array.isArray(payload.items) ? payload.items : [];
      setTickets(items);
      setTicketsError("");
      setSelectedTicketId((current) => {
        if (preserveSelection && current && items.some((item) => item.id_ticket === current)) {
          return current;
        }
        return String(items[0]?.id_ticket || "");
      });
    } catch (error) {
      if (signal?.aborted) {
        return;
      }
      setTickets([]);
      setTicketsError(error instanceof Error ? error.message : "Tickets konnten nicht geladen werden.");
    } finally {
      if (!signal?.aborted) {
        setTicketsLoading(false);
      }
    }
  }

  async function reloadDetail(ticketId: string, signal?: AbortSignal) {
    if (!ticketId) {
      setDetail(null);
      return;
    }
    setDetailLoading(true);
    try {
      const payload = await fetchSupportTicketDetail(ticketId, signal);
      setDetail(payload);
      setDetailError("");
    } catch (error) {
      if (signal?.aborted) {
        return;
      }
      setDetail(null);
      setDetailError(error instanceof Error ? error.message : "Ticketdetails konnten nicht geladen werden.");
    } finally {
      if (!signal?.aborted) {
        setDetailLoading(false);
      }
    }
  }

  async function refreshAll(preserveSelection = true) {
    ticketsAbortRef.current?.abort();
    const ticketsController = new AbortController();
    ticketsAbortRef.current = ticketsController;
    await Promise.all([
      reloadStatus(),
      reloadTickets(ticketsController.signal, preserveSelection),
    ]);
  }

  useEffect(() => {
    if (!isActive) {
      return;
    }
    void refreshAll(false);
  }, [filterMode, isActive, searchQuery, refreshRequestToken]);

  useEffect(() => {
    if (!isActive) {
      return;
    }
    if (!selectedTicketId) {
      setDetail(null);
      setDetailError("");
      return;
    }
    detailAbortRef.current?.abort();
    const controller = new AbortController();
    detailAbortRef.current = controller;
    void reloadDetail(selectedTicketId, controller.signal);
    return () => {
      controller.abort();
    };
  }, [isActive, selectedTicketId]);

  useEffect(() => {
    if (!isActive) {
      return;
    }
    const runCycle = async () => {
      try {
        await runSupportPoll();
      } catch {
        // ignore polling errors, next refresh shows them
      }
      await refreshAll(true);
      if (selectedTicketId) {
        await reloadDetail(selectedTicketId);
      }
    };

    pollTimerRef.current = window.setInterval(() => {
      void runCycle();
    }, POLL_INTERVAL_MS);

    return () => {
      if (pollTimerRef.current !== null) {
        window.clearInterval(pollTimerRef.current);
        pollTimerRef.current = null;
      }
    };
  }, [isActive, selectedTicketId, filterMode, searchQuery]);

  useEffect(() => {
    return () => {
      ticketsAbortRef.current?.abort();
      detailAbortRef.current?.abort();
      revokePreviewObjectUrl(previewObjectUrlRef);
    };
  }, []);

  async function handleManualSync() {
    setStatusMessage({ text: "Support-Sync laeuft...", level: "info" });
    try {
      await runSupportPoll();
      await refreshAll(true);
      if (selectedTicketId) {
        await reloadDetail(selectedTicketId);
      }
      setStatusMessage({ text: "Support-Sync abgeschlossen.", level: "ok" });
    } catch (error) {
      setStatusMessage({ text: error instanceof Error ? error.message : "Support-Sync fehlgeschlagen.", level: "error" });
    }
  }

  async function handleBackfill() {
    setStatusMessage({ text: "Initialer Ticket-Backfill laeuft...", level: "info" });
    try {
      await runSupportBackfill();
      await refreshAll(false);
      setStatusMessage({ text: "Backfill abgeschlossen.", level: "ok" });
    } catch (error) {
      setStatusMessage({ text: error instanceof Error ? error.message : "Backfill fehlgeschlagen.", level: "error" });
    }
  }

  async function handleSendMessage() {
    if (!selectedTicketId || !String(messageText).trim()) {
      return;
    }
    setSending(true);
    try {
      await sendSupportMessage(selectedTicketId, messageText, interimNotice, messageFiles);
      setMessageText("");
      setInterimNotice(false);
      setMessageFiles([]);
      await refreshAll(true);
      await reloadDetail(selectedTicketId);
      setStatusMessage({ text: "Nachricht wurde gesendet.", level: "ok" });
    } catch (error) {
      setStatusMessage({ text: error instanceof Error ? error.message : "Nachricht konnte nicht gesendet werden.", level: "error" });
    } finally {
      setSending(false);
    }
  }

  async function handleCloseTicket() {
    if (!selectedTicketId) {
      return;
    }
    setClosing(true);
    try {
      await closeSupportTicket(selectedTicketId);
      await refreshAll(true);
      await reloadDetail(selectedTicketId);
      setStatusMessage({ text: "Ticket wurde geschlossen.", level: "ok" });
    } catch (error) {
      setStatusMessage({ text: error instanceof Error ? error.message : "Ticket konnte nicht geschlossen werden.", level: "error" });
    } finally {
      setClosing(false);
    }
  }

  async function handleCreateNote() {
    if (!selectedTicketId || !String(noteDraft).trim()) {
      return;
    }
    setNoteBusy(true);
    try {
      await createSupportNote(selectedTicketId, noteDraft);
      setNoteDraft("");
      await reloadDetail(selectedTicketId);
      await refreshAll(true);
      setStatusMessage({ text: "Interne Notiz gespeichert.", level: "ok" });
    } catch (error) {
      setStatusMessage({ text: error instanceof Error ? error.message : "Notiz konnte nicht gespeichert werden.", level: "error" });
    } finally {
      setNoteBusy(false);
    }
  }

  async function handleDeleteNote(note: SupportTicketNote) {
    if (!selectedTicketId || !note.id) {
      return;
    }
    setNoteBusy(true);
    try {
      await deleteSupportNote(selectedTicketId, String(note.id));
      await reloadDetail(selectedTicketId);
      await refreshAll(true);
      setStatusMessage({ text: "Interne Notiz geloescht.", level: "ok" });
    } catch (error) {
      setStatusMessage({ text: error instanceof Error ? error.message : "Notiz konnte nicht geloescht werden.", level: "error" });
    } finally {
      setNoteBusy(false);
    }
  }

  async function openAttachmentPreview(attachment: SupportTicketAttachment) {
    const ticketId = String(selectedTicketId || "").trim();
    const filename = String(attachment.filename || "").trim();
    if (!ticketId || !filename || !previewModalApi) {
      return;
    }
    try {
      const payload = await fetchSupportAttachmentPreview(ticketId, filename);
      revokePreviewObjectUrl(previewObjectUrlRef);
      const objectUrl = URL.createObjectURL(payload.blob);
      previewObjectUrlRef.current = objectUrl;
      previewModalApi.open(
        objectUrl,
        filename,
        payload.mimeType || (filename.toLowerCase().endsWith(".pdf") ? "application/pdf" : payload.blob.type || "image/jpeg"),
      );
    } catch (error) {
      setStatusMessage({ text: error instanceof Error ? error.message : "Preview konnte nicht geladen werden.", level: "error" });
    }
  }

  const summary = orderSummary(detail);
  const units = orderUnitsFromContext(detail);

  return (
    <div id="supportPanel" className="tab-panel active" data-react-support-mounted="true">
      <div className="page" style={{ paddingBottom: 24 }}>
        <div className="card" style={{ marginBottom: 12 }}>
          <div className="table-head" style={{ alignItems: "center", gap: 12, flexWrap: "wrap" }}>
            <div>
              <div className="table-title">Support Inbox</div>
              <div className="table-meta">Kaufland Tickets DE, Polling alle 300s</div>
            </div>
            <div style={{ marginLeft: "auto", display: "flex", gap: 8, flexWrap: "wrap" }}>
              <button className="btn-inline" type="button" onClick={() => { void handleManualSync(); }}>Sync</button>
              <button className="btn-inline" type="button" onClick={() => { void handleBackfill(); }}>Backfill</button>
            </div>
          </div>
          {statusMessage ? <div className={`status ${statusClass(statusMessage.level)}`} style={{ marginTop: 10 }}>{statusMessage.text}</div> : null}
        </div>

        <div className="card" style={{ marginBottom: 12 }}>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
            <button id="supportFilterTodo" className={`chip ${filterMode === "todo" ? "active" : ""}`} type="button" onClick={() => { setFilterMode("todo"); }}>Zu bearbeiten ({status?.counts?.tickets_open || 0})</button>
            <button id="supportFilterWaiting" className={`chip ${filterMode === "waiting" ? "active" : ""}`} type="button" onClick={() => { setFilterMode("waiting"); }}>Wartet ({status?.counts?.tickets_waiting || 0})</button>
            <button id="supportFilterClosed" className={`chip ${filterMode === "closed" ? "active" : ""}`} type="button" onClick={() => { setFilterMode("closed"); }}>Geschlossen ({status?.counts?.tickets_closed || 0})</button>
            <button id="supportFilterAll" className={`chip ${filterMode === "all" ? "active" : ""}`} type="button" onClick={() => { setFilterMode("all"); }}>Alle ({status?.counts?.tickets_total || 0})</button>
            <div className="table-meta" style={{ marginLeft: "auto" }}>Suche: {searchQuery ? searchQuery : "-"}</div>
          </div>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "minmax(320px, 420px) minmax(0, 1fr)", gap: 12 }}>
          <section className="card" style={{ minHeight: 640 }}>
            <div className="table-head">
              <div className="table-title">Tickets</div>
              <div className="table-meta" id="supportMeta">{ticketsLoading ? "..." : `${tickets.length} sichtbar`}</div>
            </div>
            {ticketsError ? <div className="status status-error" style={{ marginBottom: 10 }}>{ticketsError}</div> : null}
            <div id="supportTicketsList" style={{ display: "grid", gap: 8 }}>
              {ticketsLoading ? <div className="table-meta">Lade Tickets...</div> : null}
              {!ticketsLoading && tickets.length === 0 ? <div data-react-support-empty="true" className="table-meta">Keine Tickets fuer den aktuellen Filter.</div> : null}
              {tickets.map((ticket) => {
                const isActiveRow = ticket.id_ticket === selectedTicketId;
                const due = dueState(ticket.first_response_due_at);
                return (
                  <button
                    key={String(ticket.id_ticket || "")}
                    type="button"
                    data-react-support-row="true"
                    className={`order-card ${isActiveRow ? "active" : ""}`}
                    style={{ textAlign: "left", padding: 12, border: isActiveRow ? "1px solid var(--accent)" : undefined }}
                    onClick={() => {
                      setSelectedTicketId(String(ticket.id_ticket || ""));
                    }}
                  >
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                      <strong>{ticket.id_ticket}</strong>
                      <span className={`status ${ticket.is_seller_responsible ? "status-error" : "status-info"}`}>{ticket.is_seller_responsible ? "Aktion" : "Wartet"}</span>
                    </div>
                    <div className="table-meta" style={{ marginTop: 6 }}>{humanizeTopic(ticket.topic || ticket.open_reason)}</div>
                    <div className="table-meta" style={{ marginTop: 4 }}>Status: {humanizeStatus(ticket.status)}</div>
                    <div className="table-meta" style={{ marginTop: 4 }}>Order Units: {(ticket.order_unit_ids || []).join(", ") || "-"}</div>
                    <div className={`table-meta ${due.level === "danger" ? "text-danger" : due.level === "warn" ? "text-warn" : ""}`} style={{ marginTop: 4 }}>Frist: {due.label}</div>
                  </button>
                );
              })}
            </div>
          </section>

          <section className="card" style={{ minHeight: 640 }}>
            {!selectedTicketId ? <div className="table-meta">Kein Ticket ausgewaehlt.</div> : null}
            {detailError ? <div className="status status-error">{detailError}</div> : null}
            {detailLoading ? <div className="table-meta">Ticketdetails werden geladen...</div> : null}
            {detail && selectedTicket ? (
              <div id="supportDetailContent" data-ticket-id={selectedTicketId}>
                <div className="table-head" style={{ alignItems: "flex-start", gap: 12 }}>
                  <div>
                    <div className="table-title">Ticket {selectedTicket.id_ticket}</div>
                    <div className="table-meta">{humanizeTopic(selectedTicket.topic || selectedTicket.open_reason)}</div>
                  </div>
                  <div style={{ marginLeft: "auto", display: "flex", gap: 8, flexWrap: "wrap" }}>
                    <span className={`status ${selectedTicket.is_seller_responsible ? "status-error" : "status-info"}`}>{selectedTicket.is_seller_responsible ? "Zu bearbeiten" : "Wartet"}</span>
                    <button
                      id="supportCloseTicketBtn"
                      className="btn-inline"
                      type="button"
                      disabled={closing || String(selectedTicket.status || "").toLowerCase() !== "opened"}
                      onClick={() => { void handleCloseTicket(); }}
                    >
                      {closing ? "Schliesse..." : "Schliessen"}
                    </button>
                  </div>
                </div>

                <div style={{ display: "grid", gridTemplateColumns: "1.2fr 0.8fr", gap: 12, marginTop: 10 }}>
                  <div className="card" style={{ padding: 12 }}>
                    <div className="table-title" style={{ fontSize: 15 }}>Verlauf</div>
                    <div id="supportMessages" style={{ display: "grid", gap: 8, marginTop: 10, maxHeight: 320, overflowY: "auto" }}>
                      {(detail.messages || []).map((message) => (
                        <div key={String(message.id_ticket_message || "")} className="order-card" style={{ padding: 10 }}>
                          <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                            <strong>{textValue(message.author_name) || textValue(message.author_role) || "Unbekannt"}</strong>
                            <span className="table-meta">{formatDateTime(message.ts_created_iso)}</span>
                          </div>
                          <div className="table-meta" style={{ marginTop: 4 }}>{textValue(message.direction) === "outbound" ? "Ausgehend" : "Eingehend"}</div>
                          <div style={{ whiteSpace: "pre-wrap", marginTop: 8 }}>{textValue(message.text) || "-"}</div>
                        </div>
                      ))}
                    </div>

                    <div style={{ marginTop: 14 }}>
                      <div className="table-title" style={{ fontSize: 15 }}>Antwort senden</div>
                      <textarea
                        id="supportMessageInput"
                        value={messageText}
                        onChange={(event) => { setMessageText(event.target.value); }}
                        rows={7}
                        placeholder="Nachricht an den Kunden..."
                        style={{ width: "100%", marginTop: 8 }}
                        disabled={String(selectedTicket.status || "").toLowerCase() !== "opened" || sending}
                      />
                      <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap", marginTop: 8 }}>
                        <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
                          <input type="checkbox" checked={interimNotice} onChange={(event) => { setInterimNotice(event.target.checked); }} />
                          Interim Notice
                        </label>
                        <label className="btn-inline" style={{ cursor: "pointer" }}>
                          Dateien
                          <input
                            id="supportFileInput"
                            type="file"
                            multiple
                            style={{ display: "none" }}
                            onChange={(event) => {
                              const nextFiles = Array.from(event.target.files || []);
                              setMessageFiles(nextFiles);
                            }}
                          />
                        </label>
                        <button
                          id="supportSendBtn"
                          className="btn-inline"
                          type="button"
                          disabled={sending || String(selectedTicket.status || "").toLowerCase() !== "opened" || !String(messageText).trim()}
                          onClick={() => { void handleSendMessage(); }}
                        >
                          {sending ? "Sende..." : "Senden"}
                        </button>
                      </div>
                      {messageFiles.length ? <div className="table-meta" style={{ marginTop: 6 }}>Anhaenge: {messageFiles.map((file) => file.name).join(", ")}</div> : null}
                    </div>
                  </div>

                  <div style={{ display: "grid", gap: 12 }}>
                    <div className="card" style={{ padding: 12 }}>
                      <div className="table-title" style={{ fontSize: 15 }}>Kontext</div>
                      <div className="table-meta" style={{ marginTop: 8 }}>Status: {humanizeStatus(selectedTicket.status)}</div>
                      <div className="table-meta" style={{ marginTop: 4 }}>Erstellt: {formatDateTime(selectedTicket.ts_created_iso)}</div>
                      <div className="table-meta" style={{ marginTop: 4 }}>Aktualisiert: {formatDateTime(selectedTicket.ts_updated_iso)}</div>
                      <div className="table-meta" style={{ marginTop: 4 }}>Frist: {dueState(selectedTicket.first_response_due_at).label}</div>
                      <div className="table-meta" style={{ marginTop: 4 }}>Order Units: {(detail.order_unit_ids || []).join(", ") || "-"}</div>
                      {summary ? (
                        <>
                          <div className="table-meta" style={{ marginTop: 10 }}>Order: {textValue(summary.external_order_id) || textValue(summary.order_id) || "-"}</div>
                          <div className="table-meta" style={{ marginTop: 4 }}>Kunde: {textValue(summary.customer) || "-"}</div>
                          <div className="table-meta" style={{ marginTop: 4 }}>Artikel: {textValue(summary.article) || "-"}</div>
                          <div className="table-meta" style={{ marginTop: 4 }}>Gesamt: {typeof summary.total_cents === "number" ? formatMoneyFromCents(summary.total_cents) : "-"}</div>
                          <div className="table-meta" style={{ marginTop: 4 }}>Fulfillment: {textValue(summary.fulfillment_status) || "-"}</div>
                        </>
                      ) : null}
                      {units.length ? (
                        <div style={{ marginTop: 10 }}>
                          <div className="table-meta">Units</div>
                          <div style={{ display: "grid", gap: 6, marginTop: 6 }}>
                            {units.slice(0, 5).map((unit, index) => (
                              <div key={`${textValue(unit.id_order_unit) || index}`} className="table-meta">
                                {textValue(unit.id_order_unit)} | {textValue(unit.product_title) || "-"} | {textValue(unit.status) || "-"}
                              </div>
                            ))}
                          </div>
                        </div>
                      ) : null}
                    </div>

                    <div className="card" style={{ padding: 12 }}>
                      <div className="table-title" style={{ fontSize: 15 }}>Anhaenge</div>
                      <div id="supportAttachments" style={{ display: "grid", gap: 8, marginTop: 8 }}>
                        {(detail.attachments || []).length === 0 ? <div className="table-meta">Keine Anhaenge.</div> : null}
                        {(detail.attachments || []).map((attachment) => (
                          <div key={`${attachment.filename}:${attachment.uri}`} style={{ display: "flex", gap: 8, alignItems: "center", justifyContent: "space-between" }}>
                            <div>
                              <div>{textValue(attachment.filename) || "Datei"}</div>
                              <div className="table-meta">{formatDateTime(attachment.ts_created_iso)}</div>
                            </div>
                            <button
                              type="button"
                              className="btn-inline"
                              data-support-preview="true"
                              onClick={() => {
                                void openAttachmentPreview(attachment);
                              }}
                            >
                              Preview
                            </button>
                          </div>
                        ))}
                      </div>
                    </div>

                    <div className="card" style={{ padding: 12 }}>
                      <div className="table-title" style={{ fontSize: 15 }}>Interne Notizen</div>
                      <textarea
                        id="supportNoteInput"
                        rows={4}
                        value={noteDraft}
                        onChange={(event) => { setNoteDraft(event.target.value); }}
                        placeholder="Nur lokal sichtbar..."
                        style={{ width: "100%", marginTop: 8 }}
                      />
                      <div style={{ marginTop: 8 }}>
                        <button className="btn-inline" type="button" disabled={noteBusy || !String(noteDraft).trim()} onClick={() => { void handleCreateNote(); }}>
                          {noteBusy ? "Speichere..." : "Notiz speichern"}
                        </button>
                      </div>
                      <div id="supportNotes" style={{ display: "grid", gap: 8, marginTop: 10 }}>
                        {(detail.notes || []).length === 0 ? <div className="table-meta">Keine internen Notizen.</div> : null}
                        {(detail.notes || []).map((note) => (
                          <div key={String(note.id || "")} className="order-card" style={{ padding: 10 }}>
                            <div style={{ whiteSpace: "pre-wrap" }}>{textValue(note.note_text)}</div>
                            <div style={{ display: "flex", justifyContent: "space-between", gap: 8, marginTop: 8 }}>
                              <div className="table-meta">{formatDateTime(note.updated_at || note.created_at)}</div>
                              <button className="btn-inline" type="button" onClick={() => { void handleDeleteNote(note); }}>Loeschen</button>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            ) : null}
          </section>
        </div>
      </div>
    </div>
  );
}
