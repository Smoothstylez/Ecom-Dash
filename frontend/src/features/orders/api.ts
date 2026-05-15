import { withAdminHeaders } from "@/shared/api/admin-auth";
import { buildDashboardApiUrl } from "@/shared/runtime/base-path";

export type InvoiceDocument = {
  document_id?: string;
  original_filename?: string;
  stored_filename?: string;
  mime_type?: string;
  uploaded_at?: string;
};

export type OrderSummary = {
  marketplace?: string;
  order_id?: string;
  order_date?: string;
  external_order_id?: string;
  customer?: string;
  article?: string;
  total_cents?: number;
  after_fees_cents?: number;
  fees_cents?: number;
  fee_source?: string;
  purchase_cost_cents?: number;
  purchase_currency?: string;
  purchase_supplier?: string;
  purchase_notes?: string;
  profit_cents?: number;
  fulfillment_status?: string;
  financial_status?: string;
  raw_status?: string;
  payment_method?: string;
  invoice?: InvoiceDocument | null;
  line_items_count?: number;
  currency?: string;
};

export type OrderDetail = {
  summary?: OrderSummary;
  order?: Record<string, unknown>;
  order_raw?: Record<string, unknown>;
  line_items?: Array<Record<string, unknown>>;
  fulfillments?: Array<Record<string, unknown>>;
  refunds?: Array<Record<string, unknown>>;
  transactions?: Array<Record<string, unknown>>;
  units?: Array<Record<string, unknown>>;
  shipping_address?: Record<string, unknown>;
  billing_address?: Record<string, unknown>;
  customer?: Record<string, unknown>;
  bookkeeping_breakdown?: Record<string, unknown>;
};

export type OrdersResponse = {
  total: number;
  items: OrderSummary[];
  limit?: number;
  offset?: number;
};

export type OrdersQuery = {
  from?: string;
  to?: string;
  marketplace?: string;
  q?: string;
  status?: string;
  payment?: string[];
  hideCanceled?: boolean;
  hasPurchaseCost?: boolean;
  noPurchaseCost?: boolean;
  hasInvoice?: boolean;
  noInvoice?: boolean;
  limit?: number;
  offset?: number;
};

function buildOrdersUrl(query: OrdersQuery) {
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
  if (query.status) {
    params.set("status", query.status);
  }
  if (Array.isArray(query.payment)) {
    for (const payment of query.payment) {
      const token = String(payment || "").trim();
      if (token) {
        params.append("payment", token);
      }
    }
  }
  if (query.hideCanceled) {
    params.set("hide_canceled", "true");
  }
  if (query.hasPurchaseCost) {
    params.set("has_purchase_cost", "true");
  }
  if (query.noPurchaseCost) {
    params.set("no_purchase_cost", "true");
  }
  if (query.hasInvoice) {
    params.set("has_invoice", "true");
  }
  if (query.noInvoice) {
    params.set("no_invoice", "true");
  }
  if (query.limit) {
    params.set("limit", String(query.limit));
  }
  if (query.offset) {
    params.set("offset", String(query.offset));
  }

  const search = params.toString();
  return buildDashboardApiUrl(search ? `/api/orders?${search}` : "/api/orders");
}

async function readErrorMessage(response: Response) {
  const payload = await response.json().catch(() => ({}));
  const detail = payload && typeof payload === "object" ? payload.detail : "";
  if (typeof detail === "string" && detail.trim()) {
    return detail;
  }
  return `Orders request failed: ${response.status}`;
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

export function fetchOrders(query: OrdersQuery): Promise<OrdersResponse> {
  return fetchJson<OrdersResponse>(buildOrdersUrl(query));
}

export function fetchOrderDetail(marketplace: string, orderId: string): Promise<OrderDetail> {
  return fetchJson<OrderDetail>(
    buildDashboardApiUrl(`/api/orders/${encodeURIComponent(marketplace)}/${encodeURIComponent(orderId)}`),
  );
}

export function updateOrderPurchase(marketplace: string, orderId: string, purchaseCostEur: number | null) {
  return fetchJson<Record<string, unknown>>(
    buildDashboardApiUrl(`/api/orders/${encodeURIComponent(marketplace)}/${encodeURIComponent(orderId)}/purchase`),
    {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ purchase_cost_eur: purchaseCostEur }),
    },
  );
}

export function uploadOrderInvoice(marketplace: string, orderId: string, formData: FormData) {
  return fetchJson<Record<string, unknown>>(
    buildDashboardApiUrl(`/api/orders/${encodeURIComponent(marketplace)}/${encodeURIComponent(orderId)}/invoice`),
    {
      method: "POST",
      body: formData,
    },
  );
}
