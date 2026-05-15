import { buildDashboardApiUrl } from "@/shared/runtime/base-path";

type EbayOrdersQuery = {
  shop?: string;
  category?: string;
  limit?: number;
  offset?: number;
};

export type EbayOrder = {
  datum?: string;
  shop?: string;
  category?: string;
  artikel?: string;
  kunde_name?: string;
  order_number?: string;
  preis?: number;
  gebuehren?: number;
  ali_preis?: number;
  gewinn?: number;
  is_return?: number;
};

export type EbayShop = {
  shop?: string;
  count?: number;
  first_date?: string;
  last_date?: string;
  revenue?: number;
  fees?: number;
  purchase?: number;
  profit?: number;
};

export type EbayTopArticle = {
  artikel?: string;
  count?: number;
  revenue?: number;
  profit?: number;
};

export type EbaySummary = {
  available?: boolean;
  kpis?: Record<string, string | number>;
  shops?: EbayShop[];
  top_articles?: EbayTopArticle[];
  import_meta?: Record<string, string | number> | null;
};

export type EbayOrdersResponse = {
  orders: EbayOrder[];
  total: number;
  limit?: number;
  offset?: number;
};

function buildOrdersUrl(query: EbayOrdersQuery) {
  const params = new URLSearchParams();

  if (query.shop) {
    params.set("shop", query.shop);
  }
  if (query.category) {
    params.set("category", query.category);
  }
  if (query.limit) {
    params.set("limit", String(query.limit));
  }
  if (query.offset) {
    params.set("offset", String(query.offset));
  }

  const search = params.toString();
  return buildDashboardApiUrl(search ? `/api/ebay/orders?${search}` : "/api/ebay/orders");
}

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url, {
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(`eBay request failed: ${response.status}`);
  }

  return (await response.json()) as T;
}

export function fetchEbaySummary(): Promise<EbaySummary> {
  return fetchJson<EbaySummary>(buildDashboardApiUrl("/api/ebay/summary"));
}

export function fetchEbayOrders(query: EbayOrdersQuery): Promise<EbayOrdersResponse> {
  return fetchJson<EbayOrdersResponse>(buildOrdersUrl(query));
}
