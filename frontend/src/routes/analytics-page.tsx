import { useQuery } from "@tanstack/react-query";
import { Activity, ArrowRightLeft, Euro, PackageSearch, Percent, UsersRound } from "lucide-react";
import { DataTableShell } from "@/components/ui/data-table-shell";
import { SurfaceCard } from "@/components/ui/surface-card";
import { fetchJson } from "@/lib/api";
import { formatCurrencyFromCents, formatNumber, formatPercent } from "@/lib/format";

interface MarketplaceRow {
  marketplace?: string;
  order_count?: number;
  revenue_total_cents?: number;
  profit_total_cents?: number;
  margin_pct?: number;
  aov_cents?: number;
  return_rate_pct?: number;
}

interface AnalyticsPayload {
  order_count?: number;
  revenue_total_cents?: number;
  profit_total_cents?: number;
  margin_pct?: number;
  avg_profit_per_order_cents?: number;
  aov_cents?: number;
  unique_customers?: number;
  return_rate_pct?: number;
  repeat_customer_rate_pct?: number;
  purchase_coverage_pct?: number;
  marketplaces?: MarketplaceRow[];
}

interface HealthPayload {
  status?: string;
  combined_db?: {
    exists?: boolean;
  };
  live_sync_status?: {
    enabled?: boolean;
    thread_alive?: boolean;
  };
}

async function fetchAnalytics() {
  return fetchJson<AnalyticsPayload>("/api/analytics/kpis?trendGranularity=auto");
}

async function fetchHealth() {
  return fetchJson<HealthPayload>("/api/health");
}

export function AnalyticsPage() {
  const analyticsQuery = useQuery({
    queryKey: ["analytics-preview"],
    queryFn: fetchAnalytics,
  });

  const healthQuery = useQuery({
    queryKey: ["health-preview"],
    queryFn: fetchHealth,
  });

  const analytics = analyticsQuery.data;
  const marketplaces = analytics?.marketplaces ?? [];

  const cards = [
    {
      label: "Orders",
      value: formatNumber(Number(analytics?.order_count ?? 0)),
      icon: PackageSearch,
    },
    {
      label: "Umsatz",
      value: formatCurrencyFromCents(analytics?.revenue_total_cents),
      icon: Euro,
    },
    {
      label: "Gewinn",
      value: formatCurrencyFromCents(analytics?.profit_total_cents),
      icon: Activity,
    },
    {
      label: "Marge",
      value: formatPercent(analytics?.margin_pct),
      icon: Percent,
    },
    {
      label: "AOV",
      value: formatCurrencyFromCents(analytics?.aov_cents),
      icon: ArrowRightLeft,
    },
    {
      label: "Unique Kunden",
      value: formatNumber(Number(analytics?.unique_customers ?? 0)),
      icon: UsersRound,
    },
  ];

  return (
    <div className="space-y-5">
      <section className="grid gap-3 xl:grid-cols-[1.8fr_1fr]">
        <SurfaceCard>
          <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
            <div>
              <p className="text-xs font-medium uppercase tracking-[0.18em] text-[var(--ink-4)]">Analytics Blueprint</p>
              <h2 className="mt-2 font-[var(--font-display)] text-2xl font-semibold text-[var(--ink)]">
                Erste migrierte Route mit echten API-Daten
              </h2>
              <p className="mt-3 max-w-3xl text-sm leading-6 text-[var(--ink-4)]">
                Diese Preview nutzt bereits die bestehenden FastAPI-Endpunkte. Layout, Karten und Tabellen folgen der aktuellen Analytics-Seite
                und werden zum gemeinsamen Referenzsystem fuer die restlichen Bereiche ausgebaut.
              </p>
            </div>
            <div className="grid gap-2 text-sm text-[var(--ink-4)] sm:min-w-[280px]">
              <StatusPill
                label="Backend"
                value={healthQuery.data?.status === "ok" ? "verbunden" : analyticsQuery.isError ? "fehler" : "pruefen"}
              />
              <StatusPill
                label="Combined DB"
                value={healthQuery.data?.combined_db?.exists ? "vorhanden" : "fehlt"}
              />
              <StatusPill
                label="Live Sync"
                value={healthQuery.data?.live_sync_status?.thread_alive ? "aktiv" : healthQuery.data?.live_sync_status?.enabled ? "bereit" : "aus"}
              />
            </div>
          </div>
        </SurfaceCard>

        <SurfaceCard title="Migration Guardrails">
          <ul className="mt-4 space-y-3 text-sm leading-6 text-[var(--ink-4)]">
            <li>Legacy-Dashboard bleibt bis zum finalen Umschalten produktiv.</li>
            <li>Neue Komponenten werden an Analytics-Spacing und -Typografie ausgerichtet.</li>
            <li>API- und UI-Smokes wachsen routeweise mit jeder Migration.</li>
          </ul>
        </SurfaceCard>
      </section>

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-6">
        {cards.map((card) => {
          const Icon = card.icon;

          return (
            <SurfaceCard key={card.label}>
              <div className="flex items-start justify-between">
                <div>
                  <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">{card.label}</p>
                  <p className="mt-3 font-[var(--font-display)] text-2xl font-semibold text-[var(--ink)]">
                    {analyticsQuery.isLoading ? "..." : card.value}
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

      <section className="grid gap-3 xl:grid-cols-[1.3fr_1fr]">
        <DataTableShell
          description="Echte Daten aus `/api/analytics/kpis` als erste Tabellen-Regression."
          meta={`${marketplaces.length} Zeilen`}
          title="Channel Vergleich"
        >
          <table className="min-w-full border-collapse text-sm">
            <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
              <tr>
                <th className="px-4 py-3 font-medium">Channel</th>
                <th className="px-4 py-3 font-medium">Orders</th>
                <th className="px-4 py-3 font-medium">Umsatz</th>
                <th className="px-4 py-3 font-medium">Gewinn</th>
                <th className="px-4 py-3 font-medium">Marge</th>
                <th className="px-4 py-3 font-medium">AOV</th>
                <th className="px-4 py-3 font-medium">Retouren</th>
              </tr>
            </thead>
            <tbody>
              {analyticsQuery.isLoading ? (
                <tr>
                  <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={7}>
                    Analytics-Daten werden geladen...
                  </td>
                </tr>
              ) : marketplaces.length ? (
                marketplaces.map((item) => {
                  const profit = Number(item.profit_total_cents ?? 0);

                  return (
                    <tr key={item.marketplace} className="border-t border-[var(--border)] text-[var(--ink-2)]">
                      <td className="px-4 py-3 font-medium capitalize text-[var(--ink)]">{item.marketplace ?? "-"}</td>
                      <td className="px-4 py-3">{formatNumber(Number(item.order_count ?? 0))}</td>
                      <td className="px-4 py-3">{formatCurrencyFromCents(item.revenue_total_cents)}</td>
                      <td className={`px-4 py-3 ${profit < 0 ? "text-[var(--danger)]" : "text-[var(--success)]"}`}>
                        {formatCurrencyFromCents(item.profit_total_cents)}
                      </td>
                      <td className="px-4 py-3">{formatPercent(item.margin_pct)}</td>
                      <td className="px-4 py-3">{formatCurrencyFromCents(item.aov_cents)}</td>
                      <td className="px-4 py-3">{formatPercent(item.return_rate_pct)}</td>
                    </tr>
                  );
                })
              ) : (
                <tr>
                  <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={7}>
                    Keine Channel-Daten verfuegbar.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </DataTableShell>

        <SurfaceCard title="Kernmetriken">
          <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-1">
            <MetricTile label="Gewinn / Order" value={formatCurrencyFromCents(analytics?.avg_profit_per_order_cents)} loading={analyticsQuery.isLoading} />
            <MetricTile label="Repeat Customer" value={formatPercent(analytics?.repeat_customer_rate_pct)} loading={analyticsQuery.isLoading} />
            <MetricTile label="Retourenrate" value={formatPercent(analytics?.return_rate_pct)} loading={analyticsQuery.isLoading} />
            <MetricTile label="Einkauf gepflegt" value={formatPercent(analytics?.purchase_coverage_pct)} loading={analyticsQuery.isLoading} />
          </div>
        </SurfaceCard>
      </section>

      {analyticsQuery.isError ? (
        <div className="rounded-[20px] border border-[color:rgba(183,72,55,0.25)] bg-[color:rgba(255,241,238,0.9)] px-4 py-3 text-sm text-[color:#8d3728]">
          Analytics-Preview konnte nicht geladen werden: {analyticsQuery.error.message}
        </div>
      ) : null}
    </div>
  );
}

function StatusPill({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between rounded-2xl border border-[var(--border)] bg-[var(--surface-2)] px-3 py-2">
      <span className="text-xs uppercase tracking-[0.14em] text-[var(--ink-4)]">{label}</span>
      <strong className="text-sm font-medium capitalize text-[var(--ink)]">{value}</strong>
    </div>
  );
}

function MetricTile({ label, value, loading }: { label: string; value: string; loading: boolean }) {
  return (
    <div className="rounded-[20px] border border-[var(--border)] bg-[var(--surface-2)] px-4 py-4">
      <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">{label}</p>
      <p className="mt-3 font-[var(--font-display)] text-2xl font-semibold text-[var(--ink)]">{loading ? "..." : value}</p>
    </div>
  );
}
