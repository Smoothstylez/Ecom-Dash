type EbayOrdersQuery = {
  shop?: string;
  category?: string;
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
};

function buildOrdersUrl(query: EbayOrdersQuery) {
  const params = new URLSearchParams();

  if (query.shop) {
    params.set("shop", query.shop);
  }
  if (query.category) {
    params.set("category", query.category);
  }

  const search = params.toString();
  return search ? `/api/ebay/orders?${search}` : "/api/ebay/orders";
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
  return fetchJson<EbaySummary>("/api/ebay/summary");
}

export function fetchEbayOrders(query: EbayOrdersQuery): Promise<EbayOrdersResponse> {
  return fetchJson<EbayOrdersResponse>(buildOrdersUrl(query));
}
