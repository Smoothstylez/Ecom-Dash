import { withAdminHeaders } from "@/shared/api/admin-auth";
import { fetchJson } from "@/shared/api/client";
import { buildDashboardApiUrl } from "@/shared/runtime/base-path";

export type SupportTicketSummary = {
  id_ticket?: string;
  storefront?: string;
  id_buyer?: string;
  status?: string;
  open_reason?: string;
  topic?: string;
  fulfillment_type?: string;
  is_seller_responsible?: boolean;
  ts_created_iso?: string;
  ts_updated_iso?: string;
  first_response_due_at?: string;
  first_response_sent_at?: string;
  order_units_count?: number;
  last_message_at_iso?: string;
  order_unit_ids?: string[];
  counts?: {
    notes?: number;
    messages?: number;
  };
};

export type SupportTicketMessage = {
  id_ticket_message?: string;
  id_ticket?: string;
  author_role?: string;
  author_name?: string;
  text?: string;
  ts_created_iso?: string;
  direction?: string;
  raw?: Record<string, unknown> | null;
};

export type SupportTicketAttachment = {
  filename?: string;
  uri?: string;
  ts_created_iso?: string;
};

export type SupportTicketNote = {
  id?: string;
  id_ticket?: string;
  note_text?: string;
  created_at?: string;
  updated_at?: string;
};

export type SupportTicketDetailResponse = {
  ticket?: SupportTicketSummary;
  ticket_raw?: Record<string, unknown>;
  order_unit_ids?: string[];
  messages?: SupportTicketMessage[];
  attachments?: SupportTicketAttachment[];
  notes?: SupportTicketNote[];
  order_context?: Record<string, unknown> | null;
};

export type SupportTicketListResponse = {
  filter?: string;
  total: number;
  items: SupportTicketSummary[];
  limit?: number;
  offset?: number;
};

export type SupportStatusResponse = {
  counts?: {
    tickets_total?: number;
    tickets_open?: number;
    tickets_waiting?: number;
    tickets_closed?: number;
    notes_total?: number;
  };
  last_sync?: Record<string, unknown> | null;
  config?: Record<string, unknown>;
};

export type SupportFilterMode = "todo" | "waiting" | "closed" | "all";

export function fetchSupportStatus(signal?: AbortSignal) {
  return fetchJson<SupportStatusResponse>(buildDashboardApiUrl("/api/kaufland-tickets/status"), { signal });
}

export function fetchSupportTickets(filterMode: SupportFilterMode, q: string, signal?: AbortSignal) {
  const params = new URLSearchParams();
  params.set("filter", filterMode);
  if (String(q || "").trim()) {
    params.set("q", String(q).trim());
  }
  params.set("limit", "200");
  params.set("offset", "0");
  return fetchJson<SupportTicketListResponse>(buildDashboardApiUrl(`/api/kaufland-tickets?${params.toString()}`), { signal });
}

export function fetchSupportTicketDetail(idTicket: string, signal?: AbortSignal) {
  return fetchJson<SupportTicketDetailResponse>(
    buildDashboardApiUrl(`/api/kaufland-tickets/${encodeURIComponent(idTicket)}`),
    { signal },
  );
}

export function runSupportPoll() {
  return fetchJson<Record<string, unknown>>(buildDashboardApiUrl("/api/kaufland-tickets/sync/poll"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ storefront: "de", include_closed: true, page_limit: 30, max_pages: 50, lookback_minutes: 60 }),
  });
}

export function runSupportBackfill() {
  return fetchJson<Record<string, unknown>>(buildDashboardApiUrl("/api/kaufland-tickets/sync/backfill"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ storefront: "de", include_closed: true, page_limit: 30, max_pages: 1000 }),
  });
}

export async function sendSupportMessage(idTicket: string, text: string, interimNotice: boolean, files: File[]) {
  const formData = new FormData();
  formData.set("text", text);
  formData.set("interim_notice", interimNotice ? "true" : "false");
  for (const file of files) {
    formData.append("files", file, file.name);
  }
  return fetchJson<Record<string, unknown>>(
    buildDashboardApiUrl(`/api/kaufland-tickets/${encodeURIComponent(idTicket)}/messages`),
    { method: "POST", body: formData },
  );
}

export function closeSupportTicket(idTicket: string) {
  return fetchJson<Record<string, unknown>>(
    buildDashboardApiUrl(`/api/kaufland-tickets/${encodeURIComponent(idTicket)}/close`),
    { method: "PATCH" },
  );
}

export function fetchSupportNotes(idTicket: string, signal?: AbortSignal) {
  return fetchJson<{ items?: SupportTicketNote[] }>(
    buildDashboardApiUrl(`/api/kaufland-tickets/${encodeURIComponent(idTicket)}/notes`),
    { signal },
  );
}

export function createSupportNote(idTicket: string, noteText: string) {
  return fetchJson<{ ok?: boolean; note?: SupportTicketNote }>(
    buildDashboardApiUrl(`/api/kaufland-tickets/${encodeURIComponent(idTicket)}/notes`),
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ note_text: noteText }),
    },
  );
}

export function updateSupportNote(idTicket: string, noteId: string, noteText: string) {
  return fetchJson<{ ok?: boolean; note?: SupportTicketNote }>(
    buildDashboardApiUrl(`/api/kaufland-tickets/${encodeURIComponent(idTicket)}/notes/${encodeURIComponent(noteId)}`),
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ note_text: noteText }),
    },
  );
}

export function deleteSupportNote(idTicket: string, noteId: string) {
  return fetchJson<{ ok?: boolean }>(
    buildDashboardApiUrl(`/api/kaufland-tickets/${encodeURIComponent(idTicket)}/notes/${encodeURIComponent(noteId)}`),
    { method: "DELETE" },
  );
}

export async function fetchSupportAttachmentPreview(idTicket: string, filename: string) {
  const response = await fetch(
    buildDashboardApiUrl(
      `/api/kaufland-tickets/${encodeURIComponent(idTicket)}/attachments/${encodeURIComponent(filename)}/preview`,
    ),
    withAdminHeaders({
      headers: {
        Accept: "application/pdf,image/*,application/octet-stream",
      },
    }),
  );

  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    const detail = payload && typeof payload === "object" ? payload.detail : "";
    const message = typeof detail === "string" && detail.trim()
      ? detail
      : `Request failed: ${response.status}`;
    throw new Error(message);
  }

  return {
    blob: await response.blob(),
    mimeType: String(response.headers.get("Content-Type") || "").trim(),
  };
}
