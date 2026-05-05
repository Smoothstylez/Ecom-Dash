import type { AnalyticsPayload, AnalyticsQuery } from "@/features/analytics/types";

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
  return search ? `/api/analytics/kpis?${search}` : "/api/analytics/kpis";
}

export async function fetchAnalytics(query: AnalyticsQuery): Promise<AnalyticsPayload> {
  const response = await fetch(buildAnalyticsUrl(query), {
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(`Analytics request failed: ${response.status}`);
  }

  return (await response.json()) as AnalyticsPayload;
}
