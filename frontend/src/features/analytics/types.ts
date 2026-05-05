export type AnalyticsMetricBucket = {
  order_count: number;
  revenue_total_cents: number;
  fees_total_cents?: number;
  after_fees_total_cents?: number;
  purchase_total_cents: number;
  profit_total_cents: number;
};

export type AnalyticsTrendPoint = AnalyticsMetricBucket & {
  bucket_start: string;
  bucket_end: string;
};

export type AnalyticsMarketplaceRow = AnalyticsMetricBucket & {
  marketplace: string;
  shipping_total_cents: number;
  returns_order_count: number;
  orders_with_purchase_count: number;
  margin_pct: number;
  aov_cents: number;
  avg_profit_per_order_cents: number;
  return_rate_pct: number;
  purchase_coverage_pct: number;
  revenue_share_pct: number;
};

export type AnalyticsPaymentMethodRow = {
  payment_method: string;
  order_count: number;
  share_pct: number;
};

export type AnalyticsTopArticleRow = {
  article: string;
  order_count: number;
  revenue_total_cents: number;
  profit_total_cents: number;
};

export type AnalyticsPreviousPeriod = {
  from: string;
  to: string;
  span_days: number;
  order_count: number;
  revenue_total_cents: number;
  fees_total_cents: number;
  after_fees_total_cents: number;
  purchase_total_cents: number;
  profit_total_cents: number;
  margin_pct: number;
};

export type AnalyticsPayload = {
  order_count: number;
  revenue_total_cents: number;
  fees_total_cents: number;
  after_fees_total_cents: number;
  purchase_total_cents: number;
  profit_total_cents: number;
  margin_pct: number;
  aov_cents: number;
  avg_profit_per_order_cents: number;
  fees_ratio_pct: number;
  shipping_total_cents: number;
  orders_with_purchase_count: number;
  purchase_missing_count: number;
  purchase_coverage_pct: number;
  returns_order_count: number;
  return_rate_pct: number;
  unique_customers: number;
  repeat_customers: number;
  repeat_customer_rate_pct: number;
  status_summary: {
    completed_like_count: number;
    pending_like_count: number;
    return_like_count: number;
    other_count: number;
  };
  marketplaces: AnalyticsMarketplaceRow[];
  top_payment_methods: AnalyticsPaymentMethodRow[];
  shopify_revenue_total_cents: number;
  kaufland_revenue_total_cents: number;
  monthly: Array<{
    month: string;
    order_count: number;
    revenue_total_cents: number;
    fees_total_cents: number;
    after_fees_total_cents: number;
    purchase_total_cents: number;
    profit_total_cents: number;
  }>;
  trend: {
    granularity: string;
    title: string;
    from: string;
    to: string;
    point_count: number;
    order_count: number;
    revenue_total_cents: number;
    profit_total_cents: number;
    points: AnalyticsTrendPoint[];
  };
  top_articles: AnalyticsTopArticleRow[];
  purchase_heatmap: number[][];
  previous_period: AnalyticsPreviousPeriod | null;
};

export type AnalyticsQuery = {
  from: string;
  to: string;
  marketplace: string;
  q: string;
  trendGranularity: string;
};
