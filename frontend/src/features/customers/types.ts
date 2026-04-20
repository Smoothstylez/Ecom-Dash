export interface CustomerAddress {
  full_name?: string;
  street?: string;
  postcode?: string;
  city?: string;
  country?: string;
  phone?: string;
}

export interface CustomerOverviewItem {
  customer_id?: string;
  customer_name?: string;
  marketplaces?: string[];
  order_count?: number;
  repeat_customer?: boolean;
  first_order_date?: string;
  last_order_date?: string;
  revenue_total_cents?: number;
  after_fees_total_cents?: number;
  purchase_total_cents?: number;
  profit_total_cents?: number;
  emails?: string[];
  phones?: string[];
  primary_address?: CustomerAddress;
  top_articles?: string[];
}

export interface CustomersKpis {
  customers_count?: number;
  repeat_customers_count?: number;
  repeat_customers_rate_pct?: number;
  orders_total_count?: number;
  avg_orders_per_customer?: number;
  revenue_total_cents?: number;
  avg_revenue_per_customer_cents?: number;
  with_email_count?: number;
  with_phone_count?: number;
  with_address_count?: number;
  cross_market_customers_count?: number;
  shopify_customers_count?: number;
  kaufland_customers_count?: number;
}

export interface CustomersPayload {
  total?: number;
  items?: CustomerOverviewItem[];
  limit?: number;
  offset?: number;
  kpis?: CustomersKpis;
}

export interface CustomerLocationPoint {
  lat?: number;
  lng?: number;
  city?: string;
  country_code?: string;
  country?: string;
  order_count?: number;
  revenue_total_cents?: number;
  profit_total_cents?: number;
  dominant_marketplace?: string;
  provider?: string;
  marketplaces?: Array<{ marketplace?: string; order_count?: number }>;
  weight?: number;
}

export interface CustomerLocationSummary {
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
}

export interface CustomerLocationsPayload {
  summary?: CustomerLocationSummary;
  points?: CustomerLocationPoint[];
}
