import { buildDashboardApiUrl } from "@/shared/runtime/base-path";
import { fetchJson } from "@/shared/api/client";

export type CustomerAddress = {
  street?: string;
  postcode?: string;
  city?: string;
  country?: string;
};

export type CustomerKpis = {
  customers_count?: number;
  repeat_customers_count?: number;
  repeat_customers_rate_pct?: number;
  avg_orders_per_customer?: number;
  orders_total_count?: number;
  avg_revenue_per_customer_cents?: number;
  revenue_total_cents?: number;
  with_email_count?: number;
  with_phone_count?: number;
  with_address_count?: number;
  cross_market_customers_count?: number;
  shopify_customers_count?: number;
  kaufland_customers_count?: number;
};

export type CustomerItem = {
  customer_id?: string;
  customer_name?: string;
  emails?: string[];
  phones?: string[];
  primary_address?: CustomerAddress;
  marketplaces?: string[];
  order_count?: number;
  repeat_customer?: boolean;
  revenue_total_cents?: number;
  profit_total_cents?: number;
  last_order_date?: string;
  top_articles?: string[];
};

export type CustomersOverviewResponse = {
  total: number;
  kpis: CustomerKpis;
  items: CustomerItem[];
  limit?: number;
  offset?: number;
};

export type CustomerGeoMarketplace = {
  marketplace?: string;
  order_count?: number;
};

export type CustomerGeoSummary = {
  orders_total?: number;
  points_total?: number;
  resolved_source_coordinates_count?: number;
  resolved_geocoded_count?: number;
  resolved_country_centroid_count?: number;
  unresolved_orders_count?: number;
  geocode_attempts?: number;
  geocode_successes?: number;
  cache_location_hits?: number;
  cache_hit?: boolean;
  generated_in_ms?: number;
};

export type CustomerGeoPoint = {
  lat?: number;
  lng?: number;
  city?: string;
  country_code?: string;
  country?: string;
  order_count?: number;
  revenue_total_cents?: number;
  profit_total_cents?: number;
  marketplaces?: CustomerGeoMarketplace[];
  dominant_marketplace?: string;
  provider?: string;
  weight?: number;
};

export type CustomerLocationsResponse = {
  summary?: CustomerGeoSummary;
  points: CustomerGeoPoint[];
};

type CustomersQuery = {
  from?: string;
  to?: string;
  marketplace?: string;
  q?: string;
  limit?: number;
  offset?: number;
  refresh?: boolean;
};

function buildCustomersQuery(query: CustomersQuery) {
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
  if (query.refresh) {
    params.set("refresh", "true");
  }

  return params.toString();
}

export function fetchCustomersOverview(query: CustomersQuery, signal?: AbortSignal): Promise<CustomersOverviewResponse> {
  const search = buildCustomersQuery(query);
  return fetchJson<CustomersOverviewResponse>(buildDashboardApiUrl(search ? `/api/customers?${search}` : "/api/customers"), { signal });
}

export function fetchCustomerLocations(query: CustomersQuery, signal?: AbortSignal): Promise<CustomerLocationsResponse> {
  const search = buildCustomersQuery(query);
  return fetchJson<CustomerLocationsResponse>(buildDashboardApiUrl(search ? `/api/customers/locations?${search}` : "/api/customers/locations"), { signal });
}
