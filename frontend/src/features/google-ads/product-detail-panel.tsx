import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { ArrowUpRight, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { DataTableShell } from "@/components/ui/data-table-shell";
import { SurfaceCard } from "@/components/ui/surface-card";
import type { GoogleAdsProductDetailPayload } from "@/features/google-ads/types";
import { buildDashboardQuery, fetchJson } from "@/lib/api";
import { formatCurrencyFromCents, formatDate, formatNumber } from "@/lib/format";

interface ProductSelection {
  productKey: string;
  from: string;
  to: string;
}

interface GoogleAdsProductDetailPanelProps {
  selection: ProductSelection | null;
  onClose: () => void;
}

async function fetchGoogleAdsProductDetail(selection: ProductSelection) {
  const query = buildDashboardQuery({
    product_key: selection.productKey,
    from: selection.from || undefined,
    to: selection.to || undefined,
  });

  return fetchJson<GoogleAdsProductDetailPayload>(`/api/google-ads/product-detail?${query}`);
}

export function GoogleAdsProductDetailPanel({ selection, onClose }: GoogleAdsProductDetailPanelProps) {
  const detailQuery = useQuery({
    queryKey: ["google-ads-product-detail", selection?.productKey, selection?.from, selection?.to],
    queryFn: () => fetchGoogleAdsProductDetail(selection as ProductSelection),
    enabled: Boolean(selection),
  });

  const trend = detailQuery.data?.trend ?? [];
  const kpis = detailQuery.data?.kpis ?? {};
  const trendRows = useMemo(() => {
    return trend.map((row) => ({
      ...row,
      profit_after_ads_cents: Number(row.profit_cents ?? 0) - Number(row.ads_cost_cents ?? 0),
    }));
  }, [trend]);

  if (!selection) {
    return null;
  }

  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-[color:rgba(19,25,37,0.28)] backdrop-blur-[1px]">
      <div className="flex h-full w-full max-w-[920px] flex-col overflow-y-auto border-l border-[var(--border)] bg-[var(--panel)] shadow-[0_18px_50px_rgba(18,25,37,0.18)]">
        <div className="sticky top-0 z-10 border-b border-[var(--border)] bg-[var(--panel)] px-5 py-4">
          <div className="flex items-start justify-between gap-4">
            <div>
              <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">Google Ads Produkt</p>
              <h2 className="mt-1 font-[var(--font-display)] text-2xl font-semibold text-[var(--ink)]">{selection.productKey}</h2>
            </div>
            <div className="flex items-center gap-2">
              <a href="/legacy?tab=googleads" rel="noreferrer">
                <Button size="sm" variant="outline">
                  Legacy
                  <ArrowUpRight className="ml-2 h-4 w-4" />
                </Button>
              </a>
              <Button onClick={onClose} size="sm" variant="ghost">
                <X className="h-4 w-4" />
              </Button>
            </div>
          </div>
        </div>

        <div className="space-y-4 p-5">
          {detailQuery.isLoading ? (
            <SurfaceCard>
              <p className="text-sm text-[var(--ink-4)]">Produktdaten werden geladen...</p>
            </SurfaceCard>
          ) : detailQuery.isError ? (
            <SurfaceCard>
              <p className="text-sm text-[var(--danger)]">Produktdaten konnten nicht geladen werden: {detailQuery.error.message}</p>
            </SurfaceCard>
          ) : (
            <>
              <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
                <MetricTile label="Ads Kosten" value={formatCurrencyFromCents(kpis.ads_cost_total_cents)} />
                <MetricTile label="Orders" value={formatNumber(Number(kpis.orders_count ?? 0))} />
                <MetricTile label="Umsatz" value={formatCurrencyFromCents(kpis.revenue_total_cents)} />
                <MetricTile label="Gewinn vor Ads" value={formatCurrencyFromCents(kpis.profit_before_ads_cents)} />
                <MetricTile label="Gewinn nach Ads" value={formatCurrencyFromCents(kpis.profit_after_ads_cents)} />
                <MetricTile label="ROAS" value={`${Number(kpis.roas ?? 0).toFixed(2)}x`} />
              </section>

              <DataTableShell
                description="Tagesverlauf fuer Ads Kosten, Umsatz und Gewinn."
                meta={`${formatNumber(trendRows.length)} Tage`}
                title="Produktverlauf"
              >
                <table className="min-w-full border-collapse text-sm">
                  <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
                    <tr>
                      <th className="px-4 py-3 font-medium">Tag</th>
                      <th className="px-4 py-3 font-medium">Ads</th>
                      <th className="px-4 py-3 font-medium">Orders</th>
                      <th className="px-4 py-3 font-medium">Umsatz</th>
                      <th className="px-4 py-3 font-medium">Gewinn vor Ads</th>
                      <th className="px-4 py-3 font-medium">Gewinn nach Ads</th>
                    </tr>
                  </thead>
                  <tbody>
                    {trendRows.length ? (
                      trendRows.map((row, index) => {
                        const profitAfterAds = Number(row.profit_after_ads_cents ?? 0);

                        return (
                          <tr className="border-t border-[var(--border)] text-[var(--ink-2)]" key={`${row.day}-${index}`}>
                            <td className="px-4 py-3 text-[var(--ink)]">{formatDate(row.day)}</td>
                            <td className="px-4 py-3">{formatCurrencyFromCents(row.ads_cost_cents)}</td>
                            <td className="px-4 py-3">{formatNumber(Number(row.order_count ?? 0))}</td>
                            <td className="px-4 py-3">{formatCurrencyFromCents(row.revenue_cents)}</td>
                            <td className="px-4 py-3">{formatCurrencyFromCents(row.profit_cents)}</td>
                            <td className={`px-4 py-3 ${profitAfterAds < 0 ? "text-[var(--danger)]" : "text-[var(--success)]"}`}>
                              {formatCurrencyFromCents(profitAfterAds)}
                            </td>
                          </tr>
                        );
                      })
                    ) : (
                      <tr>
                        <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={6}>
                          Keine Produktdaten fuer den aktuellen Zeitraum.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </DataTableShell>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

function MetricTile({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface-2)] px-4 py-3">
      <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">{label}</p>
      <p className="mt-2 text-sm font-medium text-[var(--ink)]">{value}</p>
    </div>
  );
}
