import { withAdminHeaders } from "@/shared/api/admin-auth";
import { buildDashboardApiUrl } from "@/shared/runtime/base-path";

export type BookingRow = {
  id?: string;
  date?: string;
  type?: string;
  provider?: string;
  direction?: string;
  booking_class?: string;
  amount_gross?: number;
  reference?: string;
  notes?: string;
  counterparty_name?: string;
  payment_account_id?: string;
  payment_account_name?: string;
  document_id?: string;
  document?: {
    original_filename?: string;
    stored_filename?: string;
    mime_type?: string;
  } | null;
  status?: string;
  currency?: string;
  order?: {
    provider?: string;
    external_order_id?: string;
  } | null;
  template?: {
    name?: string;
  } | null;
  payment_account?: {
    name?: string;
  } | null;
  source?: string;
  source_key?: string;
  category?: string;
};

export type BookingOrderRow = {
  marketplace?: string;
  order_id?: string;
  external_order_id?: string;
  order_date?: string;
  customer?: string;
  revenue_cents?: number;
  bookkeeping_income_cents?: number;
  total_costs_cents?: number;
  fees_cents?: number;
  purchase_cents?: number;
  bookkeeping_expense_cents?: number;
  profit_cents?: number;
  bookkeeping_matched_via?: string;
  documents_count?: number;
};

export type BookingTemplateRow = {
  id?: string;
  name?: string;
  type?: string;
  direction?: string;
  counterparty_name?: string;
  start_date?: string;
  default_amount_gross?: number;
  schedule?: string;
  payment_account_id?: string;
  active?: boolean;
};

export type BookingAccountRow = {
  id?: string;
  name?: string;
  provider?: string;
  is_active?: boolean;
};

export type BookingDocumentRow = {
  id?: string;
  uploaded_at?: string;
  original_filename?: string;
  stored_filename?: string;
  _count?: {
    transactions?: number;
  };
  mime_type?: string;
};

export type MonthlyInvoiceRow = {
  id?: string;
  provider?: string;
  period_from?: string;
  period_to?: string;
  invoice_amount_cents?: number;
  calculated_sum_cents?: number;
  difference_cents?: number;
  status?: string;
  notes?: string;
  document_id?: string;
  document?: {
    original_filename?: string;
    mime_type?: string;
  } | null;
  currency?: string;
  created_at?: string;
  updated_at?: string;
  transactions?: BookingRow[];
};

export type BookingDetailResponse = {
  transaction?: BookingRow | null;
};

export type MonthlyInvoiceDetailResponse = {
  invoice?: MonthlyInvoiceRow | null;
};

export type BookingTransactionsSumResponse = {
  total_cents?: number;
  transaction_count?: number;
  transactions?: Array<{
    id?: string;
    date?: string;
    amount_gross?: number;
    reference?: string;
  }>;
};

export type BookingTransactionCategoryCounts = {
  sale?: number;
  fee?: number;
  cogs?: number;
  invoice?: number;
  subscription?: number;
  refund?: number;
  other?: number;
};

export type OptionItem = {
  id?: string;
  name?: string;
  provider?: string;
  external_order_id?: string;
  start_date?: string;
  schedule?: string;
  default_amount_gross?: number;
  reference?: string;
  type?: string;
  date?: string;
};

type ListResponse<T> = {
  items: T[];
  total?: number;
  limit?: number;
  offset?: number;
  category_counts?: BookingTransactionCategoryCounts;
};

type CreateUpdateResponse<T, K extends string> = {
  ok?: boolean;
} & Partial<Record<K, T>>;

type BookingsQuery = {
  from?: string;
  to?: string;
  q?: string;
  marketplace?: string;
  bookingClass?: string;
  category?: string;
  type?: string;
  limit?: number;
  offset?: number;
};

function buildBookingsTransactionQuery(query: BookingsQuery) {
  const params = new URLSearchParams();
  if (query.from) {
    params.set("dateFrom", query.from);
  }
  if (query.to) {
    params.set("dateTo", query.to);
  }
  if (query.marketplace) {
    params.set("marketplace", query.marketplace);
  }
  if (query.q) {
    params.set("q", query.q);
  }
  if (query.category) {
    params.set("category", query.category);
  }
  if (query.type) {
    params.set("type", query.type);
  }
  if (query.bookingClass && query.bookingClass !== "all") {
    params.set("bookingClass", query.bookingClass);
  }
  if (query.limit) {
    params.set("limit", String(query.limit));
  }
  if (query.offset) {
    params.set("offset", String(query.offset));
  }
  return params.toString();
}

function buildSharedQuery(query: BookingsQuery) {
  const params = new URLSearchParams();
  if (query.from) {
    params.set("from", query.from);
  }
  if (query.to) {
    params.set("to", query.to);
  }
  if (query.marketplace) {
    params.set("marketplace", query.marketplace);
  }
  if (query.q) {
    params.set("q", query.q);
  }
  return params.toString();
}

async function readErrorMessage(response: Response) {
  const payload = await response.json().catch(() => ({}));
  const detail = payload && typeof payload === "object" ? payload.detail : "";
  if (typeof detail === "string" && detail.trim()) {
    return detail;
  }
  return `Bookings request failed: ${response.status}`;
}

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, withAdminHeaders({
    ...init,
    headers: {
      Accept: "application/json",
      ...(init?.headers || {}),
    },
  }));

  if (!response.ok) {
    throw new Error(await readErrorMessage(response));
  }

  return (await response.json()) as T;
}

async function fetchWithoutJson(url: string, init?: RequestInit) {
  const response = await fetch(url, withAdminHeaders(init));
  if (!response.ok) {
    throw new Error(await readErrorMessage(response));
  }
  return response;
}

export async function fetchBookingsTransactions(query: BookingsQuery) {
  const search = buildBookingsTransactionQuery(query);
  const payload = await fetchJson<ListResponse<BookingRow>>(buildDashboardApiUrl(search ? `/api/bookings/transactions?${search}` : "/api/bookings/transactions"));
  return {
    items: Array.isArray(payload.items) ? payload.items : [],
    total: Number(payload.total || 0),
    limit: Number(payload.limit || 0),
    offset: Number(payload.offset || 0),
    categoryCounts: payload.category_counts && typeof payload.category_counts === "object" ? payload.category_counts : {},
  };
}

export function fetchBookingOrders(query: BookingsQuery) {
  const params = new URLSearchParams(buildSharedQuery(query));
  if (query.limit) {
    params.set("limit", String(query.limit));
  }
  if (query.offset) {
    params.set("offset", String(query.offset));
  }
  const search = params.toString();
  return fetchJson<ListResponse<BookingOrderRow>>(buildDashboardApiUrl(search ? `/api/bookings/orders?${search}` : "/api/bookings/orders"));
}

export function fetchBookingLedgerOrders() {
  return fetchJson<ListResponse<OptionItem>>(buildDashboardApiUrl("/api/bookings/ledger/orders"));
}

export function fetchBookingAccounts() {
  return fetchJson<ListResponse<BookingAccountRow>>(buildDashboardApiUrl("/api/bookings/payment-accounts"));
}

export function fetchBookingTemplates() {
  return fetchJson<ListResponse<BookingTemplateRow>>(buildDashboardApiUrl("/api/bookings/templates"));
}

export function fetchBookingDocuments() {
  return fetchJson<ListResponse<BookingDocumentRow>>(buildDashboardApiUrl("/api/bookings/documents"));
}

export function fetchMonthlyInvoices() {
  return fetchJson<ListResponse<MonthlyInvoiceRow>>(buildDashboardApiUrl("/api/bookings/monthly-invoices"));
}

export function fetchBookingTransactionDetail(transactionId: string) {
  return fetchJson<BookingDetailResponse>(buildDashboardApiUrl(`/api/bookings/transactions/${encodeURIComponent(transactionId)}`));
}

export function fetchMonthlyInvoiceDetail(invoiceId: string) {
  return fetchJson<MonthlyInvoiceDetailResponse>(buildDashboardApiUrl(`/api/bookings/monthly-invoices/${encodeURIComponent(invoiceId)}`));
}

export function createMonthlyInvoice(payload: Record<string, unknown>) {
  return fetchJson<CreateUpdateResponse<MonthlyInvoiceRow, "invoice">>(buildDashboardApiUrl("/api/bookings/monthly-invoices"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function fetchBookingTransactionsSum(provider: string, periodFrom: string, periodTo: string) {
  const params = new URLSearchParams({ provider, periodFrom, periodTo });
  return fetchJson<BookingTransactionsSumResponse>(buildDashboardApiUrl(`/api/bookings/transactions/sum?${params.toString()}`));
}

export function updateMonthlyInvoice(invoiceId: string, payload: Record<string, unknown>) {
  return fetchJson<CreateUpdateResponse<MonthlyInvoiceRow, "invoice">>(buildDashboardApiUrl(`/api/bookings/monthly-invoices/${encodeURIComponent(invoiceId)}`), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function deleteMonthlyInvoice(invoiceId: string) {
  await fetchWithoutJson(buildDashboardApiUrl(`/api/bookings/monthly-invoices/${encodeURIComponent(invoiceId)}`), {
    method: "DELETE",
  });
}

export function createBookingTransaction(payload: Record<string, unknown>) {
  return fetchJson<CreateUpdateResponse<BookingRow, "transaction">>(buildDashboardApiUrl("/api/bookings/transactions"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function updateBookingTransaction(transactionId: string, payload: Record<string, unknown>) {
  return fetchJson<CreateUpdateResponse<BookingRow, "transaction">>(buildDashboardApiUrl(`/api/bookings/transactions/${encodeURIComponent(transactionId)}`), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function deleteBookingTransaction(transactionId: string) {
  await fetchWithoutJson(buildDashboardApiUrl(`/api/bookings/transactions/${encodeURIComponent(transactionId)}`), {
    method: "DELETE",
  });
}

export function createBookingTemplate(payload: Record<string, unknown>) {
  return fetchJson<CreateUpdateResponse<BookingTemplateRow, "template">>(buildDashboardApiUrl("/api/bookings/templates"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function updateBookingTemplate(templateId: string, payload: Record<string, unknown>) {
  return fetchJson<CreateUpdateResponse<BookingTemplateRow, "template">>(buildDashboardApiUrl(`/api/bookings/templates/${encodeURIComponent(templateId)}`), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function runBookingTemplate(templateId: string, payload: Record<string, unknown>) {
  const response = await fetch(buildDashboardApiUrl(`/api/bookings/templates/${encodeURIComponent(templateId)}/generate-transaction`), withAdminHeaders({
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  }));
  const body = await response.json().catch(() => ({}));
  if (response.ok) {
    return { status: "created" as const, body };
  }

  let message = body && typeof body === "object" ? (body as { detail?: unknown; error?: unknown }).detail || (body as { detail?: unknown; error?: unknown }).error : "";
  if (message && typeof message === "object") {
    message = (message as { message?: string }).message || JSON.stringify(message);
  }
  if (response.status === 409) {
    return { status: "duplicate" as const, body, message: String(message || `HTTP ${response.status}`) };
  }

  throw new Error(String(message || `HTTP ${response.status}`));
}

export function createBookingAccount(payload: Record<string, unknown>) {
  return fetchJson<CreateUpdateResponse<BookingAccountRow, "payment_account">>(buildDashboardApiUrl("/api/bookings/payment-accounts"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function updateBookingAccount(accountId: string, payload: Record<string, unknown>) {
  return fetchJson<CreateUpdateResponse<BookingAccountRow, "payment_account">>(buildDashboardApiUrl(`/api/bookings/payment-accounts/${encodeURIComponent(accountId)}`), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function uploadBookingDocument(payload: FormData) {
  return fetchJson<CreateUpdateResponse<BookingDocumentRow, "document">>(buildDashboardApiUrl("/api/bookings/documents/upload"), {
    method: "POST",
    body: payload,
  });
}
