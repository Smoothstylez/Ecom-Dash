export interface GoogleAdsImportMeta {
  rows?: number;
  non_zero_rows?: number;
  report_from_day?: string;
  report_to_day?: string;
  last_non_zero_day?: string;
}

export interface GoogleAdsImportStatus {
  filename?: string;
  imported_at?: string;
  meta?: GoogleAdsImportMeta;
}

export interface GoogleAdsKpis {
  products_count?: number;
  orders_count?: number;
  ads_cost_total_cents?: number;
  ads_cost_mapped_cents?: number;
  ads_cost_unmapped_cents?: number;
  shopify_revenue_total_cents?: number;
  profit_before_ads_total_cents?: number;
  profit_after_ads_total_cents?: number;
  roas?: number;
  missing_assignments_count?: number;
}

export interface GoogleAdsProductRow {
  product_key?: string;
  product_label?: string;
  product_detail?: string;
  mapped?: boolean;
  article_count?: number;
  ads_cost_cents?: number;
  order_count?: number;
  revenue_total_cents?: number;
  after_fees_total_cents?: number;
  purchase_total_cents?: number;
  profit_before_ads_cents?: number;
  profit_after_ads_cents?: number;
}

export interface GoogleAdsMissingAssignment {
  article_id?: string;
  ads_cost_cents?: number;
  day_count?: number;
}

export interface GoogleAdsTrendRow {
  day?: string;
  ads_cost_cents?: number;
  mapped_ads_cost_cents?: number;
  revenue_cents?: number;
  profit_cents?: number;
  order_count?: number;
}

export interface GoogleAdsAnalyticsPayload {
  kpis?: GoogleAdsKpis;
  products?: GoogleAdsProductRow[];
  missing_assignments?: GoogleAdsMissingAssignment[];
  trend?: GoogleAdsTrendRow[];
  imports?: {
    report?: GoogleAdsImportStatus;
    assignment?: GoogleAdsImportStatus;
  };
}

export interface GoogleAdsProductDetailPayload {
  product_key?: string;
  trend?: GoogleAdsTrendRow[];
  kpis?: {
    ads_cost_total_cents?: number;
    orders_count?: number;
    revenue_total_cents?: number;
    profit_before_ads_cents?: number;
    profit_after_ads_cents?: number;
    roas?: number;
  };
}
