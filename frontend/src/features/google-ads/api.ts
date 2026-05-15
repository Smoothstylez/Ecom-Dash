import { withAdminHeaders } from "@/shared/api/admin-auth";
import { buildDashboardApiUrl } from "@/shared/runtime/base-path";

export type GoogleAdsImportMeta = {
  filename?: string;
  imported_at?: string;
  meta?: {
    report_from_day?: string;
    report_to_day?: string;
    last_non_zero_day?: string;
    rows?: number;
    non_zero_rows?: number;
  };
};

export type GoogleAdsAssignmentImportMeta = {
  filename?: string;
  imported_at?: string;
  meta?: {
    rows?: number;
  };
};

export type GoogleAdsProduct = {
  product_key?: string;
  product_label?: string;
  product_detail?: string;
  mapped?: boolean;
  ads_cost_cents?: number;
  order_count?: number;
  revenue_total_cents?: number;
  profit_before_ads_cents?: number;
  profit_after_ads_cents?: number;
};

export type GoogleAdsMissingAssignment = {
  article_id?: string;
  ads_cost_cents?: number;
  day_count?: number;
};

export type GoogleAdsTrendPoint = {
  day?: string;
  ads_cost_cents?: number;
  mapped_ads_cost_cents?: number;
  revenue_cents?: number;
  profit_cents?: number;
  order_count?: number;
};

export type GoogleAdsAnalytics = {
  kpis: Record<string, number>;
  imports: {
    report?: GoogleAdsImportMeta;
    assignment?: GoogleAdsAssignmentImportMeta;
  };
  products: GoogleAdsProduct[];
  missing_assignments: GoogleAdsMissingAssignment[];
  trend: GoogleAdsTrendPoint[];
};

export type GoogleAdsProductDetail = {
  product_key?: string;
  kpis: Record<string, number>;
  trend: GoogleAdsTrendPoint[];
};

type GoogleAdsQuery = {
  from?: string;
  to?: string;
  q?: string;
};

function buildQueryString(query: GoogleAdsQuery) {
  const params = new URLSearchParams();

  if (query.from) {
    params.set("from", query.from);
  }
  if (query.to) {
    params.set("to", query.to);
  }
  if (query.q) {
    params.set("q", query.q);
  }

  return params.toString();
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
    throw new Error(`Google Ads request failed: ${response.status}`);
  }

  return (await response.json()) as T;
}

export function fetchGoogleAdsAnalytics(query: GoogleAdsQuery): Promise<GoogleAdsAnalytics> {
  const search = buildQueryString(query);
  return fetchJson<GoogleAdsAnalytics>(buildDashboardApiUrl(search ? `/api/google-ads/analytics?${search}` : "/api/google-ads/analytics"));
}

export function fetchGoogleAdsProductDetail(productKey: string, query: GoogleAdsQuery): Promise<GoogleAdsProductDetail> {
  const params = new URLSearchParams(buildQueryString(query));
  params.set("product_key", productKey);
  return fetchJson<GoogleAdsProductDetail>(buildDashboardApiUrl(`/api/google-ads/product-detail?${params.toString()}`));
}

export function uploadGoogleAdsFiles(formData: FormData): Promise<Record<string, unknown>> {
  return fetchJson<Record<string, unknown>>(buildDashboardApiUrl("/api/google-ads/upload"), {
    method: "POST",
    body: formData,
  });
}

export function resetGoogleAds(): Promise<Record<string, unknown>> {
  return fetchJson<Record<string, unknown>>(buildDashboardApiUrl("/api/google-ads/reset"), {
    method: "DELETE",
  });
}
