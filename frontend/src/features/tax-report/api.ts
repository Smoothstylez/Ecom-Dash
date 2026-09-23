import { withAdminHeaders } from "@/shared/api/admin-auth";
import { buildDashboardApiUrl } from "@/shared/runtime/base-path";

export type BlockingItem = { code: string; count?: number; hint?: string };
export type WarningItem = { code: string; count?: number; hint?: string };

export type AmazonBucket = {
  count: number;
  gross: number;
  net: number;
  output_vat: number;
  by_original_class?: Record<string, number>;
};

export type UstReport = {
  month: string;
  revision: number | null;
  kind: string | null;
  supersedes_id: string | null;
  status: "draft" | "ready" | "filed";
  id?: string;
  settings: Record<string, unknown>;
  business_rules: Record<string, boolean>;
  sections: {
    kaufland: {
      revenue_after_returns_cents: number;
      net_cents: number;
      output_vat_cents: number;
      rate_overrides_pending: number;
      pre_vat_units_cents: number;
      returns: { count: number; order_unit_ids?: string[] };
      returns_synced: boolean;
      rows: Array<Record<string, unknown>>;
    };
    amazon: Record<string, AmazonBucket>;
    input_vat: {
      purchases_cents: number;
      amazon_fees_cents: number;
      kaufland_fees_cents: number;
      other_cents: number;
      pending_review_cents: number;
      pending_review_count: number;
      nontaxable_cents: number;
      input_vat_incomplete: boolean;
    };
  };
  totals: {
    output_vat_cents: number;
    input_vat_cents: number;
    vat_payable_cents: number;
  };
  blockers: BlockingItem[];
  warnings: WarningItem[];
};

export type UstDocument = {
  id: string;
  provider: string;
  doc_type: string;
  invoice_number: string;
  invoice_date: string;
  received_date: string;
  service_date: string;
  period_from: string;
  period_to: string;
  currency: string;
  gross_cents: number;
  net_cents: number;
  vat_cents: number;
  deductible_vat_cents: number;
  input_vat_status: string;
  deduction_month: string;
  source: string;
  sha256: string;
  notes: string;
};

export type ReportMonth = {
  month: string;
  revision: number;
  kind: string;
  status: string;
  supersedes_id: string | null;
  filed_at: string | null;
};

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(buildDashboardApiUrl(path), withAdminHeaders(init));
  if (!response.ok) {
    let detail = `HTTP ${response.status}`;
    try {
      const body = (await response.json()) as { detail?: string };
      if (body?.detail) {
        detail = String(body.detail);
      }
    } catch (_error) {
      // Antwortkoerper ist kein JSON.
    }
    throw new Error(detail);
  }
  return (await response.json()) as T;
}

export function fetchUstReport(month: string) {
  const params = new URLSearchParams({ month });
  return request<UstReport>(`/api/ust-report?${params.toString()}`);
}

export function fetchReportMonths() {
  return request<{ items: ReportMonth[]; total: number }>("/api/ust-report/months");
}

export function refreshReport(month: string) {
  return request<{ ok: boolean; report: UstReport }>(`/api/ust-report/${month}/refresh`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });
}

export function fileReport(month: string) {
  return request<{ ok: boolean; report: Record<string, unknown> }>(`/api/ust-report/${month}/file`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });
}

export function amendReport(month: string) {
  return request<{ ok: boolean; report: Record<string, unknown> }>(`/api/ust-report/${month}/amend`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });
}

export function fetchDocuments(month?: string) {
  const params = new URLSearchParams();
  if (month) {
    params.set("month", month);
  }
  const query = params.toString();
  return request<{ items: UstDocument[]; total: number }>(
    `/api/ust-report/documents${query ? `?${query}` : ""}`,
  );
}

export function uploadDocument(form: FormData) {
  return request<{ ok: boolean; invoice: UstDocument }>("/api/ust-report/documents", {
    method: "POST",
    body: form,
  });
}

export function patchDocument(documentId: string, fields: Record<string, string>) {
  return request<{ ok: boolean; invoice: UstDocument }>(`/api/ust-report/documents/${documentId}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(fields),
  });
}

export function saveKauflandOverride(payload: {
  id_order_unit: string;
  to_rate: number;
  reason: string;
}) {
  return request<{ ok: boolean; override: Record<string, unknown> }>(
    "/api/ust-report/kaufland-overrides",
    { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) },
  );
}

export function bulkOverrideZeroRates(payload: { month: string; reason: string; to_rate?: number }) {
  return request<{ ok: boolean; overridden: number }>(
    "/api/ust-report/kaufland-overrides/bulk",
    { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) },
  );
}

export function saveSettings(payload: Record<string, unknown>) {
  return request<{ ok: boolean; settings: Record<string, unknown> }>("/api/ust-report/settings", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function euro(cents: number): string {
  return `${(cents / 100).toFixed(2)} €`;
}
