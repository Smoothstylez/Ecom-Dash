export interface EbaySummaryKpis {
  total_orders?: number;
  total_returns?: number;
  total_revenue?: number;
  total_fees?: number;
  total_after_fees?: number;
  total_purchase?: number;
  total_profit?: number;
  margin_pct?: number;
  first_date?: string | null;
  last_date?: string | null;
}

export interface EbayShopSummary {
  shop?: string;
  count?: number;
  revenue?: number;
  fees?: number;
  purchase?: number;
  profit?: number;
  first_date?: string | null;
  last_date?: string | null;
}

export interface EbayTopArticle {
  artikel?: string;
  count?: number;
  revenue?: number;
  profit?: number;
}

export interface EbayImportMeta {
  imported_at?: string;
  source_file?: string;
  shops?: string;
  total_orders?: number;
  total_returns?: number;
}

export interface EbaySummaryPayload {
  available?: boolean;
  kpis?: EbaySummaryKpis;
  shops?: EbayShopSummary[];
  top_articles?: EbayTopArticle[];
  import_meta?: EbayImportMeta | null;
}

export interface EbayOrder {
  id?: number;
  shop?: string;
  category?: string;
  artikel?: string;
  kunde_name?: string;
  order_number?: string;
  datum?: string;
  preis?: number;
  gebuehren?: number;
  nach_gebuehren?: number;
  ali_preis?: number;
  provision_rate?: number;
  gewinn?: number;
  is_return?: number;
}

export interface EbayOrdersPayload {
  orders?: EbayOrder[];
  total?: number;
}
