import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ArrowUpRight, Boxes, ChartColumnIncreasing, RotateCcw, Wallet } from "lucide-react";
import { DataTableShell } from "@/components/ui/data-table-shell";
import { SurfaceCard } from "@/components/ui/surface-card";
import { Button } from "@/components/ui/button";
import type { EbayOrdersPayload, EbaySummaryPayload } from "@/features/ebay/types";
import { buildDashboardQuery, fetchJson } from "@/lib/api";
import { formatDate, formatNumber } from "@/lib/format";

const moneyFormatter = new Intl.NumberFormat("de-DE", {
  style: "currency",
  currency: "EUR",
});

function formatMoney(value?: number) {
  return moneyFormatter.format(Number(value ?? 0));
}

async function fetchEbaySummary() {
  return fetchJson<EbaySummaryPayload>("/api/ebay/summary");
}

async function fetchEbayOrders(filters: { shop: string; category: string; includeReturns: boolean }) {
  const query = buildDashboardQuery({
    shop: filters.shop || undefined,
    category: filters.category || undefined,
    includeReturns: String(filters.includeReturns),
  });

  return fetchJson<EbayOrdersPayload>(`/api/ebay/orders${query ? `?${query}` : ""}`);
}

export function EbayPage() {
  const [filters, setFilters] = useState({
    shop: "",
    category: "",
    includeReturns: true,
  });

  const summaryQuery = useQuery({
    queryKey: ["ebay-summary-preview"],
    queryFn: fetchEbaySummary,
  });

  const ordersQuery = useQuery({
    queryKey: ["ebay-orders-preview", filters.shop, filters.category, filters.includeReturns],
    queryFn: () => fetchEbayOrders(filters),
  });

  const summary = summaryQuery.data;
  const kpis = summary?.kpis ?? {};
  const shops = summary?.shops ?? [];
  const topArticles = summary?.top_articles ?? [];
  const importMeta = summary?.import_meta;
  const orders = ordersQuery.data?.orders ?? [];

  const shopOptions = useMemo(() => {
    return [...new Set((ordersQuery.data?.orders ?? []).map((order) => String(order.shop ?? "").trim()).filter(Boolean))].sort();
  }, [ordersQuery.data?.orders]);

  const cards = [
    { label: "eBay Orders", value: formatNumber(Number(kpis.total_orders ?? 0)), icon: Boxes },
    { label: "Umsatz", value: formatMoney(kpis.total_revenue), icon: ChartColumnIncreasing },
    { label: "Kosten", value: formatMoney(Number(kpis.total_purchase ?? 0) + Number(kpis.total_fees ?? 0)), icon: Wallet },
    { label: "Ruecksendungen", value: formatNumber(Number(kpis.total_returns ?? 0)), icon: RotateCcw },
  ];

  return (
    <div className="space-y-5">
      <section className="grid gap-3 xl:grid-cols-[1.55fr_1fr]">
        <SurfaceCard
          description="Die eBay-Route ist die naechste risikoarme React-Migration: read-only, keine Mutationen und dieselben Legacy-APIs."
          title="eBay Migration Preview"
        />

        <SurfaceCard
          action={
            <a href="/legacy?tab=ebay" rel="noreferrer">
              <Button variant="outline">
                Legacy eBay
                <ArrowUpRight className="ml-2 h-4 w-4" />
              </Button>
            </a>
          }
          description="Die Daten bleiben rein dokumentarisch und read-only. Dadurch eignet sich die Route gut als naechster Migrationsschritt nach Orders und Customers."
          title="Risikoarm"
        />
      </section>

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        {cards.map((card) => {
          const Icon = card.icon;

          return (
            <SurfaceCard key={card.label}>
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">{card.label}</p>
                  <p className="mt-3 font-[var(--font-display)] text-2xl font-semibold text-[var(--ink)]">
                    {summaryQuery.isLoading ? "..." : card.value}
                  </p>
                </div>
                <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface-2)] p-2.5 text-[var(--ink-3)]">
                  <Icon className="h-4 w-4" />
                </div>
              </div>
            </SurfaceCard>
          );
        })}
      </section>

      <section className="grid gap-3 xl:grid-cols-[1.1fr_0.9fr]">
        <SurfaceCard description="Direkte Filter fuer den read-only Orders-Endpunkt." title="Filter">
          <div className="grid gap-3 md:grid-cols-3">
            <label className="block">
              <span className="mb-2 block text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">Shop</span>
              <select
                className="w-full rounded-2xl border border-[var(--border-strong)] bg-[var(--surface)] px-3 py-2.5 text-sm text-[var(--ink)] outline-none"
                onChange={(event) => setFilters((current) => ({ ...current, shop: event.target.value }))}
                value={filters.shop}
              >
                <option value="">Alle Shops</option>
                {shopOptions.map((shop) => (
                  <option key={shop} value={shop}>
                    {shop}
                  </option>
                ))}
              </select>
            </label>

            <label className="block">
              <span className="mb-2 block text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">Kategorie</span>
              <select
                className="w-full rounded-2xl border border-[var(--border-strong)] bg-[var(--surface)] px-3 py-2.5 text-sm text-[var(--ink)] outline-none"
                onChange={(event) => setFilters((current) => ({ ...current, category: event.target.value }))}
                value={filters.category}
              >
                <option value="">Alle</option>
                <option value="order">Bestellung</option>
                <option value="computer">Computer</option>
                <option value="return">Ruecksendung</option>
              </select>
            </label>

            <label className="flex items-center gap-3 rounded-2xl border border-[var(--border-strong)] bg-[var(--surface)] px-3 py-2.5 text-sm text-[var(--ink)]">
              <input
                checked={filters.includeReturns}
                onChange={(event) => setFilters((current) => ({ ...current, includeReturns: event.target.checked }))}
                type="checkbox"
              />
              Ruecksendungen einbeziehen
            </label>
          </div>
        </SurfaceCard>

        <SurfaceCard title="Import Status">
          {importMeta?.imported_at ? (
            <div className="grid gap-3 sm:grid-cols-2">
              <InfoTile label="Quelle" value={String(importMeta.source_file ?? "-")} />
              <InfoTile label="Importiert" value={formatDate(importMeta.imported_at)} />
              <InfoTile label="Shops" value={String(importMeta.shops ?? "-")} />
              <InfoTile
                label="Datensatz"
                value={`${formatNumber(Number(importMeta.total_orders ?? 0))} Orders · ${formatNumber(Number(importMeta.total_returns ?? 0))} Returns`}
              />
            </div>
          ) : (
            <p className="text-sm text-[var(--ink-4)]">Keine eBay-Daten importiert. Bitte `import_ebay.py` ausfuehren.</p>
          )}
        </SurfaceCard>
      </section>

      <section className="grid gap-3 xl:grid-cols-[1.05fr_0.95fr]">
        <DataTableShell description="Read-only Breakdown der importierten eBay-Shops." meta={`${formatNumber(shops.length)} Shops`} title="Shops">
          <table className="min-w-full border-collapse text-sm">
            <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
              <tr>
                <th className="px-4 py-3 font-medium">Shop</th>
                <th className="px-4 py-3 font-medium">Orders</th>
                <th className="px-4 py-3 font-medium">Zeitraum</th>
                <th className="px-4 py-3 font-medium">Umsatz</th>
                <th className="px-4 py-3 font-medium">Gewinn</th>
              </tr>
            </thead>
            <tbody>
              {summaryQuery.isLoading ? (
                <tr>
                  <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={5}>
                    eBay-Shops werden geladen...
                  </td>
                </tr>
              ) : shops.length ? (
                shops.map((shop) => {
                  const period = shop.first_date && shop.last_date ? `${shop.first_date} - ${shop.last_date}` : "-";
                  const profit = Number(shop.profit ?? 0);
                  return (
                    <tr className="border-t border-[var(--border)] text-[var(--ink-2)]" key={shop.shop}>
                      <td className="px-4 py-3 text-[var(--ink)]">{shop.shop ?? "-"}</td>
                      <td className="px-4 py-3">{formatNumber(Number(shop.count ?? 0))}</td>
                      <td className="px-4 py-3">{period}</td>
                      <td className="px-4 py-3">{formatMoney(shop.revenue)}</td>
                      <td className={`px-4 py-3 ${profit < 0 ? "text-[var(--danger)]" : "text-[var(--success)]"}`}>{formatMoney(shop.profit)}</td>
                    </tr>
                  );
                })
              ) : (
                <tr>
                  <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={5}>
                    Keine eBay Daten importiert.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </DataTableShell>

        <DataTableShell description="Top-Artikel nach Umsatz aus dem Legacy-eBay-Import." meta={`${formatNumber(topArticles.length)} Zeilen`} title="Top Artikel">
          <table className="min-w-full border-collapse text-sm">
            <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
              <tr>
                <th className="px-4 py-3 font-medium">Artikel</th>
                <th className="px-4 py-3 font-medium">Anzahl</th>
                <th className="px-4 py-3 font-medium">Umsatz</th>
                <th className="px-4 py-3 font-medium">Gewinn</th>
              </tr>
            </thead>
            <tbody>
              {summaryQuery.isLoading ? (
                <tr>
                  <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={4}>
                    Top-Artikel werden geladen...
                  </td>
                </tr>
              ) : topArticles.length ? (
                topArticles.slice(0, 12).map((article, index) => {
                  const profit = Number(article.profit ?? 0);
                  return (
                    <tr className="border-t border-[var(--border)] text-[var(--ink-2)]" key={`${article.artikel}-${index}`}>
                      <td className="px-4 py-3 text-[var(--ink)]">{article.artikel ?? "-"}</td>
                      <td className="px-4 py-3">{formatNumber(Number(article.count ?? 0))}</td>
                      <td className="px-4 py-3">{formatMoney(article.revenue)}</td>
                      <td className={`px-4 py-3 ${profit < 0 ? "text-[var(--danger)]" : "text-[var(--success)]"}`}>{formatMoney(article.profit)}</td>
                    </tr>
                  );
                })
              ) : (
                <tr>
                  <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={4}>
                    Keine Daten.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </DataTableShell>
      </section>

      <DataTableShell
        description="Read-only eBay Orders-Tabelle aus `/api/ebay/orders`."
        meta={ordersQuery.isLoading ? "Lade..." : `${formatNumber(Number(ordersQuery.data?.total ?? orders.length))} Zeilen`}
        title="eBay Orders"
      >
        <table className="min-w-full border-collapse text-sm">
          <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
            <tr>
              <th className="px-4 py-3 font-medium">Datum</th>
              <th className="px-4 py-3 font-medium">Shop</th>
              <th className="px-4 py-3 font-medium">Kategorie</th>
              <th className="px-4 py-3 font-medium">Artikel</th>
              <th className="px-4 py-3 font-medium">Kunde</th>
              <th className="px-4 py-3 font-medium">Order Nr.</th>
              <th className="px-4 py-3 font-medium">Preis</th>
              <th className="px-4 py-3 font-medium">Gebuehren</th>
              <th className="px-4 py-3 font-medium">Einkauf</th>
              <th className="px-4 py-3 font-medium">Gewinn</th>
            </tr>
          </thead>
          <tbody>
            {ordersQuery.isLoading ? (
              <tr>
                <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={10}>
                  eBay Orders werden geladen...
                </td>
              </tr>
            ) : ordersQuery.isError ? (
              <tr>
                <td className="px-4 py-4 text-[var(--danger)]" colSpan={10}>
                  eBay Orders konnten nicht geladen werden: {ordersQuery.error.message}
                </td>
              </tr>
            ) : orders.length ? (
              orders.map((order) => {
                const isReturn = Number(order.is_return ?? 0) === 1;
                const profit = Number(order.gewinn ?? 0);

                return (
                  <tr className="border-t border-[var(--border)] text-[var(--ink-2)]" key={`${order.id}-${order.order_number}`}> 
                    <td className="px-4 py-3">{order.datum ? String(order.datum) : "-"}</td>
                    <td className="px-4 py-3 text-[var(--ink)]">{order.shop ?? "-"}</td>
                    <td className="px-4 py-3">
                      <span className={`inline-flex rounded-full px-2.5 py-1 text-xs font-medium ${isReturn ? "bg-[color:rgba(255,241,238,0.92)] text-[var(--danger)]" : "bg-[var(--surface-2)] text-[var(--ink-3)]"}`}>
                        {order.category ?? "-"}
                      </span>
                    </td>
                    <td className="px-4 py-3">{order.artikel ?? "-"}</td>
                    <td className="px-4 py-3">{order.kunde_name ?? "-"}</td>
                    <td className="px-4 py-3">{order.order_number ?? "-"}</td>
                    <td className="px-4 py-3">{formatMoney(order.preis)}</td>
                    <td className="px-4 py-3">{formatMoney(order.gebuehren)}</td>
                    <td className="px-4 py-3">{order.ali_preis != null ? formatMoney(order.ali_preis) : "-"}</td>
                    <td className={`px-4 py-3 ${profit < 0 ? "text-[var(--danger)]" : "text-[var(--success)]"}`}>{formatMoney(order.gewinn)}</td>
                  </tr>
                );
              })
            ) : (
              <tr>
                <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={10}>
                  Keine eBay Bestellungen fuer aktuellen Filter.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </DataTableShell>
    </div>
  );
}

function InfoTile({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface-2)] px-4 py-3">
      <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">{label}</p>
      <p className="mt-2 break-words text-sm font-medium text-[var(--ink)]">{value}</p>
    </div>
  );
}
