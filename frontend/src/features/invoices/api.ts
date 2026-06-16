import { withAdminHeaders } from "@/shared/api/admin-auth";
import { buildDashboardApiUrl } from "@/shared/runtime/base-path";

export type InvoiceSellerProfile = {
  id?: string;
  legal_name?: string;
  street?: string;
  address_line2?: string;
  postcode?: string;
  city?: string;
  country?: string;
  email?: string;
  phone?: string;
  vat_id?: string;
  tax_number?: string;
  tax_mode?: string;
  invoice_prefix?: string;
  default_template?: string;
  footer_note?: string;
  payment_note?: string;
  eu_invoicing_enabled?: boolean;
  created_at?: string;
  updated_at?: string;
};

export type InvoiceDraftItem = {
  position?: number;
  sku?: string;
  title?: string;
  quantity?: number;
  unit_price_gross_cents?: number;
  line_total_gross_cents?: number;
  vat_rate?: number | null;
};

export type InvoiceDraft = {
  invoice?: {
    invoice_number_preview?: string;
    invoice_date?: string;
    delivery_date?: string;
    currency?: string;
    marketplace?: string;
    order_id?: string;
    external_order_id?: string;
    tax_treatment?: string;
  };
  template?: {
    key?: string;
    label?: string;
  };
  seller?: InvoiceSellerProfile;
  customer?: {
    name?: string;
    email?: string;
    country?: string;
    billing_address?: Record<string, unknown>;
    shipping_address?: Record<string, unknown>;
  };
  order?: {
    marketplace?: string;
    order_id?: string;
    external_order_id?: string;
    order_date?: string;
    status?: string;
    first_article?: string;
  };
  items?: InvoiceDraftItem[];
  totals?: {
    gross_cents?: number;
    shipping_cents?: number;
    source_tax_cents?: number;
  };
  validation?: {
    blockers?: string[];
    warnings?: string[];
    billing_source?: string;
    ready?: boolean;
  };
  existing_invoice?: {
    id?: string;
    invoice_number?: string;
    created_at?: string;
  } | null;
};

export type SalesInvoiceItem = {
  position?: number;
  sku?: string;
  title?: string;
  quantity?: number;
  unit_price_gross_cents?: number;
  line_total_gross_cents?: number;
};

export type SalesInvoice = {
  id?: string;
  marketplace?: string;
  source_order_id?: string;
  source_external_order_id?: string;
  invoice_number?: string;
  invoice_date?: string;
  delivery_date?: string;
  currency?: string;
  customer_name?: string;
  customer_country?: string;
  tax_country?: string;
  tax_treatment?: string;
  template_key?: string;
  total_gross_cents?: number;
  created_at?: string;
  updated_at?: string;
  items?: SalesInvoiceItem[];
};

export type InvoicesQuery = {
  from?: string;
  to?: string;
  marketplace?: string;
  q?: string;
  limit?: number;
  offset?: number;
};

function buildInvoicesQuery(query: InvoicesQuery) {
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
  if (query.limit) {
    params.set("limit", String(query.limit));
  }
  if (query.offset) {
    params.set("offset", String(query.offset));
  }
  return params.toString();
}

async function readErrorMessage(response: Response) {
  const payload = await response.json().catch(() => ({}));
  const detail = payload && typeof payload === "object" ? payload.detail : "";
  if (typeof detail === "string" && detail.trim()) {
    return detail;
  }
  if (detail && typeof detail === "object" && typeof (detail as { message?: unknown }).message === "string") {
    return String((detail as { message?: unknown }).message || "");
  }
  return `Invoices request failed: ${response.status}`;
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

export function fetchInvoiceProfile() {
  return fetchJson<{ profile?: InvoiceSellerProfile }>(buildDashboardApiUrl("/api/invoices/profile"));
}

export function updateInvoiceProfile(profile: InvoiceSellerProfile) {
  return fetchJson<{ ok?: boolean; profile?: InvoiceSellerProfile }>(buildDashboardApiUrl("/api/invoices/profile"), {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(profile),
  });
}

export function fetchInvoiceDraft(marketplace: string, orderId: string, templateKey?: string) {
  const params = new URLSearchParams({
    marketplace,
    order_id: orderId,
  });
  if (templateKey) {
    params.set("template_key", templateKey);
  }
  return fetchJson<InvoiceDraft>(buildDashboardApiUrl(`/api/invoices/draft?${params.toString()}`));
}

export function fetchInvoices(query: InvoicesQuery) {
  const search = buildInvoicesQuery(query);
  return fetchJson<{ total?: number; items?: SalesInvoice[]; limit?: number; offset?: number }>(
    buildDashboardApiUrl(search ? `/api/invoices?${search}` : "/api/invoices"),
  );
}

export function createInvoice(marketplace: string, orderId: string, templateKey?: string) {
  return fetchJson<{ ok?: boolean; invoice?: SalesInvoice }>(buildDashboardApiUrl("/api/invoices"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      marketplace,
      order_id: orderId,
      template_key: templateKey || undefined,
    }),
  });
}

export function buildInvoicePreviewUrl(marketplace: string, orderId: string, templateKey?: string, nonce?: number) {
  const params = new URLSearchParams({
    marketplace,
    order_id: orderId,
  });
  if (templateKey) {
    params.set("template_key", templateKey);
  }
  if (nonce) {
    params.set("nonce", String(nonce));
  }
  return buildDashboardApiUrl(`/api/invoices/preview.pdf?${params.toString()}`);
}

export function buildSalesInvoicePdfUrl(invoiceId: string, disposition: "inline" | "attachment" = "attachment") {
  return buildDashboardApiUrl(`/api/invoices/${encodeURIComponent(invoiceId)}/pdf?disposition=${encodeURIComponent(disposition)}`);
}
