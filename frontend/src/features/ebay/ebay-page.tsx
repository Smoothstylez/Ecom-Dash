import { useEffect, useMemo, useRef, useState } from "react";

import { useDashboardShellState } from "@/app/dashboard-shell-state";
import { formatDateToken, MONEY_FORMATTER, NUMBER_FORMATTER } from "@/features/analytics/format";

import {
  fetchEbayOrders,
  fetchEbaySummary,
  type EbayOrder,
  type EbayShop,
  type EbaySummary,
  type EbayTopArticle,
} from "./api";

type EbaySnapshot = {
  kpis: Record<string, string | number>;
  shops: EbayShop[];
  top_articles: EbayTopArticle[];
  import_meta: Record<string, string | number>;
  orders: EbayOrder[];
  totalOrders: number;
  filters: {
    shop: string;
    category: string;
  };
  availableShops: string[];
};

const CATEGORY_LABELS: Record<string, string> = {
  order: "Bestellung",
  computer: "Computer",
  return: "Ruecksendung",
};

function normalizeSnapshot(input?: Partial<EbaySnapshot>): EbaySnapshot {
  const filters = input?.filters && typeof input.filters === "object" ? input.filters : { shop: "", category: "" };
  return {
    kpis: input?.kpis && typeof input.kpis === "object" ? input.kpis : {},
    shops: Array.isArray(input?.shops) ? input.shops : [],
    top_articles: Array.isArray(input?.top_articles) ? input.top_articles : [],
    import_meta: input?.import_meta && typeof input.import_meta === "object" ? input.import_meta : {},
    orders: Array.isArray(input?.orders) ? input.orders : [],
    totalOrders: Number(input?.totalOrders || 0),
    filters: {
      shop: String(filters.shop || ""),
      category: String(filters.category || ""),
    },
    availableShops: Array.isArray(input?.availableShops) ? input.availableShops.map((value) => String(value || "")) : [],
  };
}

function normalizeSummary(summary?: EbaySummary, orders?: EbayOrder[], totalOrders?: number, filters?: { shop: string; category: string }): EbaySnapshot {
  const kpis = summary?.kpis && typeof summary.kpis === "object" ? summary.kpis : {};
  const shops = Array.isArray(summary?.shops) ? summary.shops : [];
  const topArticles = Array.isArray(summary?.top_articles) ? summary.top_articles : [];
  const importMeta = summary?.import_meta && typeof summary.import_meta === "object" ? summary.import_meta : {};
  const allOrders = Array.isArray(orders) ? orders : [];
  const availableShops = [...new Set([
    ...shops.map((shop) => String(shop.shop || "")).filter(Boolean),
    ...allOrders.map((order) => String(order.shop || "")).filter(Boolean),
  ])].sort();

  return normalizeSnapshot({
    kpis,
    shops,
    top_articles: topArticles,
    import_meta: importMeta,
    orders: allOrders,
    totalOrders: Number(totalOrders || allOrders.length || 0),
    filters,
    availableShops,
  });
}

function formatDateTime(value: string | undefined) {
  if (!value) {
    return "-";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }

  return `${new Intl.DateTimeFormat("de-DE").format(date)} ${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
}

type EbayPageProps = {
  isActive: boolean;
};

const EBAY_PAGE_SIZE = 150;

export function EbayPage({ isActive }: EbayPageProps) {
  const { refreshRequestToken } = useDashboardShellState();
  const [summary, setSummary] = useState<EbaySummary | null>(null);
  const [orders, setOrders] = useState<EbayOrder[]>([]);
  const [ordersTotal, setOrdersTotal] = useState(0);
  const [ordersView, setOrdersView] = useState({ shop: "", category: "", pageIndex: 0 });
  const [error, setError] = useState("");
  const [isLoadingSummary, setLoadingSummary] = useState(true);
  const [isLoadingOrders, setLoadingOrders] = useState(true);
  const lastRefreshRequestTokenRef = useRef(refreshRequestToken);

  const ordersQuery = useMemo(() => ({
    shop: ordersView.shop,
    category: ordersView.category,
    limit: EBAY_PAGE_SIZE,
    offset: ordersView.pageIndex * EBAY_PAGE_SIZE,
  }), [ordersView.category, ordersView.pageIndex, ordersView.shop]);

  const currentPage = ordersView.pageIndex + 1;
  const totalPages = Math.max(1, Math.ceil(ordersTotal / EBAY_PAGE_SIZE));
  const pageStart = ordersTotal > 0 ? (ordersView.pageIndex * EBAY_PAGE_SIZE) + 1 : 0;
  const pageEnd = ordersTotal > 0 ? Math.min(ordersTotal, pageStart + Math.max(orders.length - 1, 0)) : 0;

  useEffect(() => {
    if (!isActive) {
      return;
    }
    let cancelled = false;

    const loadSummary = async () => {
      setLoadingSummary(true);
      try {
        const payload = await fetchEbaySummary();
        if (!cancelled) {
          setSummary(payload);
          setError("");
        }
      } catch (nextError) {
        if (!cancelled) {
          setError(nextError instanceof Error ? nextError.message : "eBay Daten konnten nicht geladen werden.");
        }
      } finally {
        if (!cancelled) {
          setLoadingSummary(false);
        }
      }
    };

    void loadSummary();

    return () => {
      cancelled = true;
    };
  }, [isActive]);

  useEffect(() => {
    if (!isActive) {
      return;
    }
    let cancelled = false;

    const loadOrders = async () => {
      setLoadingOrders(true);
      try {
        const payload = await fetchEbayOrders(ordersQuery);
        if (!cancelled) {
          setOrders(Array.isArray(payload.orders) ? payload.orders : []);
          setOrdersTotal(Number(payload.total || 0));
          setError("");
        }
      } catch (nextError) {
        if (!cancelled) {
          setError(nextError instanceof Error ? nextError.message : "eBay Bestellungen konnten nicht geladen werden.");
          setOrders([]);
          setOrdersTotal(0);
        }
      } finally {
        if (!cancelled) {
          setLoadingOrders(false);
        }
      }
    };

    void loadOrders();

    return () => {
      cancelled = true;
    };
  }, [isActive, ordersQuery]);

  useEffect(() => {
    if (refreshRequestToken === 0 || lastRefreshRequestTokenRef.current === refreshRequestToken) {
      return;
    }
    if (!isActive) {
      return;
    }
    lastRefreshRequestTokenRef.current = refreshRequestToken;

    setLoadingSummary(true);
    setLoadingOrders(true);
    void fetchEbaySummary()
      .then((payload) => {
        setSummary(payload);
        setError("");
      })
      .catch((nextError: Error) => {
        setError(nextError.message);
      })
      .finally(() => {
        setLoadingSummary(false);
      });
    void fetchEbayOrders(ordersQuery)
      .then((payload) => {
        setOrders(Array.isArray(payload.orders) ? payload.orders : []);
        setOrdersTotal(Number(payload.total || 0));
        setError("");
      })
      .catch((nextError: Error) => {
        setError(nextError.message);
        setOrders([]);
        setOrdersTotal(0);
      })
      .finally(() => {
        setLoadingOrders(false);
      });
  }, [isActive, ordersQuery, refreshRequestToken]);

  const snapshot = useMemo(
    () => normalizeSummary(summary || undefined, orders, ordersTotal, { shop: ordersView.shop, category: ordersView.category }),
    [orders, ordersTotal, ordersView.category, ordersView.shop, summary],
  );
  const kpis = snapshot.kpis;
  const isLoading = isLoadingSummary || isLoadingOrders;

  return (
    <div id="ebayPanel" className="tab-panel active" data-react-ebay-mounted="true">
      <div id="ebayReactRoot">
        {error ? (
          <section className="card" style={{ marginBottom: 12, padding: 16 }}>
            <div className="table-meta" style={{ color: "var(--danger, #c44)" }}>
              eBay Daten konnten nicht geladen werden: {error}
            </div>
          </section>
        ) : null}

        <section className="kpi-grid">
          <article className="card kpi">
            <div className="kpi-name">Bestellungen</div>
            <div className="kpi-value">{isLoading ? "..." : NUMBER_FORMATTER.format(Number(kpis.total_orders || 0))}</div>
            <div className="kpi-sub">
              {Number(kpis.total_returns || 0) > 0
                ? `${Number(kpis.total_returns || 0)} Ruecksendungen | ${kpis.first_date && kpis.last_date ? `${formatDateToken(String(kpis.first_date || ""))} - ${formatDateToken(String(kpis.last_date || ""))}` : ""}`
                : (kpis.first_date && kpis.last_date ? `${formatDateToken(String(kpis.first_date || ""))} - ${formatDateToken(String(kpis.last_date || ""))}` : "Legacy eBay Daten")}
            </div>
          </article>
          <article className="card kpi">
            <div className="kpi-name">Umsatz</div>
            <div className="kpi-value">{isLoading ? "..." : MONEY_FORMATTER.format(Number(kpis.total_revenue || 0))}</div>
            <div className="kpi-sub">Brutto-Verkaufspreis</div>
          </article>
          <article className="card kpi">
            <div className="kpi-name">Einkauf + Gebuehren</div>
            <div className="kpi-value">{isLoading ? "..." : MONEY_FORMATTER.format(Number(kpis.total_purchase || 0) + Number(kpis.total_fees || 0))}</div>
            <div className="kpi-sub">{`Einkauf ${MONEY_FORMATTER.format(Number(kpis.total_purchase || 0))} + Gebuehren ${MONEY_FORMATTER.format(Number(kpis.total_fees || 0))}`}</div>
          </article>
          <article className="card kpi">
            <div className="kpi-name">Gewinn</div>
            <div className="kpi-value">{isLoading ? "..." : MONEY_FORMATTER.format(Number(kpis.total_profit || 0))}</div>
            <div className="kpi-sub">{`Marge: ${NUMBER_FORMATTER.format(Number(kpis.margin_pct || 0))}%`}</div>
          </article>
        </section>

        <section className="card table-card" style={{ marginTop: 12 }}>
          <div className="table-head">
            <h2 className="table-title">eBay Shops</h2>
            <div className="table-meta">{`${snapshot.shops.length} Shop${snapshot.shops.length !== 1 ? "s" : ""}`}</div>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Shop</th>
                  <th>Orders</th>
                  <th>Zeitraum</th>
                  <th>Umsatz</th>
                  <th>Gebuehren</th>
                  <th>Einkauf</th>
                  <th>Gewinn</th>
                </tr>
              </thead>
              <tbody>
                {snapshot.shops.length ? snapshot.shops.map((shop, index) => (
                  <tr key={`${String(shop.shop || "shop")}:${index}`}>
                    <td><strong>{String(shop.shop || "-")}</strong></td>
                    <td>{NUMBER_FORMATTER.format(Number(shop.count || 0))}</td>
                    <td>{shop.first_date && shop.last_date ? `${formatDateToken(String(shop.first_date || ""))} - ${formatDateToken(String(shop.last_date || ""))}` : "-"}</td>
                    <td>{MONEY_FORMATTER.format(Number(shop.revenue || 0))}</td>
                    <td>{MONEY_FORMATTER.format(Number(shop.fees || 0))}</td>
                    <td>{MONEY_FORMATTER.format(Number(shop.purchase || 0))}</td>
                    <td className={Number(shop.profit || 0) >= 0 ? "profit-positive" : "profit-negative"}>{MONEY_FORMATTER.format(Number(shop.profit || 0))}</td>
                  </tr>
                )) : <tr><td colSpan={7}>Keine eBay Daten importiert.</td></tr>}
              </tbody>
            </table>
          </div>
        </section>

        <section className="card table-card" style={{ marginTop: 12 }}>
          <div className="table-head">
            <h2 className="table-title">Alle eBay Bestellungen</h2>
            <div className="bookings-filter-card" style={{ marginBottom: 0, padding: 0, background: "none", border: "none", boxShadow: "none" }}>
              <div className="bookings-filter-grid" style={{ gridTemplateColumns: "auto auto auto" }}>
                <select
                  id="ebayShopSelect"
                  className="booking-select"
                  style={{ minWidth: 120 }}
                  value={snapshot.filters.shop}
                  onChange={(event) => {
                    setOrdersView((current) => ({ ...current, shop: event.target.value, pageIndex: 0 }));
                  }}
                >
                  <option value="">Alle Shops</option>
                  {snapshot.availableShops.map((shop) => <option key={shop} value={shop}>{shop}</option>)}
                </select>
                <select
                  id="ebayCategorySelect"
                  className="booking-select"
                  style={{ minWidth: 120 }}
                  value={snapshot.filters.category}
                  onChange={(event) => {
                    setOrdersView((current) => ({ ...current, category: event.target.value, pageIndex: 0 }));
                  }}
                >
                  <option value="">Alle Kategorien</option>
                  <option value="order">Bestellungen</option>
                  <option value="computer">Computer</option>
                  <option value="return">Ruecksendungen</option>
                </select>
                <div id="ebayOrdersMeta" className="table-meta" style={{ minWidth: 100, textAlign: "right" }}>
                  {isLoadingOrders ? "..." : ordersTotal > 0
                    ? `${NUMBER_FORMATTER.format(pageStart)}-${NUMBER_FORMATTER.format(pageEnd)} / ${NUMBER_FORMATTER.format(ordersTotal)} Zeilen`
                    : "0 Zeilen"}
                </div>
              </div>
            </div>
          </div>
          <div className="table-wrap">
            <table className="orders-table">
              <thead>
                <tr>
                  <th>Datum</th>
                  <th>Shop</th>
                  <th>Kategorie</th>
                  <th>Artikel</th>
                  <th>Kunde</th>
                  <th>Order Nr.</th>
                  <th>Preis</th>
                  <th>Gebuehren</th>
                  <th>Einkauf</th>
                  <th>Gewinn</th>
                </tr>
              </thead>
              <tbody>
                {snapshot.orders.length ? snapshot.orders.map((order, index) => {
                  const isReturn = Number(order.is_return || 0) === 1;
                  const categoryKey = String(order.category || "").trim().toLowerCase();
                  return (
                    <tr key={`${String(order.order_number || order.artikel || "order")}:${index}`} className={isReturn ? "return-row" : undefined}>
                      <td data-label="Datum">{order.datum ? formatDateToken(String(order.datum)) : "-"}</td>
                      <td data-label="Shop">{String(order.shop || "-")}</td>
                      <td data-label="Kategorie"><span className={`badge badge-${isReturn ? "cancel" : categoryKey === "computer" ? "partial" : "ok"}`}>{CATEGORY_LABELS[categoryKey] || categoryKey || "-"}</span></td>
                      <td data-label="Artikel">{String(order.artikel || "-")}</td>
                      <td data-label="Kunde">{String(order.kunde_name || "-")}</td>
                      <td data-label="Order Nr.">{String(order.order_number || "-")}</td>
                      <td data-label="Preis">{order.preis != null ? MONEY_FORMATTER.format(Number(order.preis || 0)) : "-"}</td>
                      <td data-label="Gebuehren">{MONEY_FORMATTER.format(Number(order.gebuehren || 0))}</td>
                      <td data-label="Einkauf">{order.ali_preis != null ? MONEY_FORMATTER.format(Number(order.ali_preis || 0)) : "-"}</td>
                      <td data-label="Gewinn" className={Number(order.gewinn || 0) >= 0 ? "profit-positive" : "profit-negative"}>{MONEY_FORMATTER.format(Number(order.gewinn || 0))}</td>
                    </tr>
                  );
                }) : <tr><td colSpan={10}>{isLoadingOrders ? "Lade eBay Bestellungen..." : "Keine eBay Bestellungen fuer aktuellen Filter."}</td></tr>}
              </tbody>
            </table>
          </div>
          {!isLoadingOrders && totalPages > 1 ? (
            <div className="orders-pagination-row">
              <div className="table-meta">
                {`Seite ${NUMBER_FORMATTER.format(currentPage)} von ${NUMBER_FORMATTER.format(totalPages)}`}
              </div>
              <div className="orders-pagination-actions">
                <button
                  id="ebayPrevPageBtn"
                  className="btn-inline ghost"
                  type="button"
                  disabled={ordersView.pageIndex <= 0}
                  onClick={() => {
                    setOrdersView((current) => ({ ...current, pageIndex: Math.max(0, current.pageIndex - 1) }));
                  }}
                >
                  Vorherige
                </button>
                <button
                  id="ebayNextPageBtn"
                  className="btn-inline ghost"
                  type="button"
                  disabled={ordersView.pageIndex >= totalPages - 1}
                  onClick={() => {
                    setOrdersView((current) => ({ ...current, pageIndex: Math.min(totalPages - 1, current.pageIndex + 1) }));
                  }}
                >
                  Naechste
                </button>
              </div>
            </div>
          ) : null}
        </section>

        <section className="card table-card" style={{ marginTop: 12 }}>
          <div className="table-head">
            <h2 className="table-title">Top Artikel</h2>
            <div className="table-meta">Nach Umsatz</div>
          </div>
          <div className="table-wrap" style={{ maxHeight: 340 }}>
            <table>
              <thead>
                <tr>
                  <th>Artikel</th>
                  <th>Anzahl</th>
                  <th>Umsatz</th>
                  <th>Gewinn</th>
                </tr>
              </thead>
              <tbody>
                {snapshot.top_articles.length ? snapshot.top_articles.map((item, index) => (
                  <tr key={`${String(item.artikel || "artikel")}:${index}`}>
                    <td>{String(item.artikel || "-")}</td>
                    <td>{NUMBER_FORMATTER.format(Number(item.count || 0))}</td>
                    <td>{MONEY_FORMATTER.format(Number(item.revenue || 0))}</td>
                    <td className={Number(item.profit || 0) >= 0 ? "profit-positive" : "profit-negative"}>{MONEY_FORMATTER.format(Number(item.profit || 0))}</td>
                  </tr>
                )) : <tr><td colSpan={4}>Keine Daten.</td></tr>}
              </tbody>
            </table>
          </div>
        </section>

        <section className="card" style={{ marginTop: 12, padding: 16 }}>
          <div className="table-meta" style={{ fontSize: "0.82rem", color: "var(--th-ink-4)", lineHeight: 1.5, whiteSpace: "pre-line" }}>
            {snapshot.import_meta && snapshot.import_meta.imported_at
              ? [
                  `Quelle: ${String(snapshot.import_meta.source_file || "-")}`,
                  `Importiert: ${formatDateTime(String(snapshot.import_meta.imported_at || ""))}`,
                  `Shops: ${String(snapshot.import_meta.shops || "-")}`,
                  `Bestellungen: ${String(snapshot.import_meta.total_orders || 0)} | Ruecksendungen: ${String(snapshot.import_meta.total_returns || 0)}`,
                  "Hinweis: eBay Legacy-Daten - kein aktiver Verkauf, nur Dokumentation.",
                ].join("\n")
              : "Keine eBay Daten importiert. Bitte import_ebay.py ausfuehren."}
          </div>
        </section>
      </div>
    </div>
  );
}
