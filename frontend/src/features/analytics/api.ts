import type { AnalyticsPayload, AnalyticsQuery } from "@/features/analytics/types";
import { buildDashboardApiUrl } from "@/shared/runtime/base-path";
import { fetchJson } from "@/shared/api/client";

function buildAnalyticsUrl(query: AnalyticsQuery) {
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
  if (query.trendGranularity) {
    params.set("trendGranularity", query.trendGranularity);
  }

  const search = params.toString();
  return buildDashboardApiUrl(search ? `/api/analytics/kpis?${search}` : "/api/analytics/kpis");
}

export function fetchAnalytics(query: AnalyticsQuery, signal?: AbortSignal): Promise<AnalyticsPayload> {
  return fetchJson<AnalyticsPayload>(buildAnalyticsUrl(query), { signal });
}
