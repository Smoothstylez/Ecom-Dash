import { useDeferredValue, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ArrowUpRight, Globe2, Mail, MapPinned, Repeat2, UsersRound } from "lucide-react";
import { PageFilterCard } from "@/components/page-filter-card";
import { DataTableShell } from "@/components/ui/data-table-shell";
import { SurfaceCard } from "@/components/ui/surface-card";
import { Button } from "@/components/ui/button";
import { CustomerGeoMap } from "@/features/customers/customer-geo-map";
import type { CustomerLocationsPayload, CustomersPayload } from "@/features/customers/types";
import { buildDashboardQuery, fetchJson } from "@/lib/api";
import { formatCurrencyFromCents, formatDate, formatNumber, formatPercent } from "@/lib/format";

interface CustomerFilters {
  q: string;
  marketplace: string;
  from: string;
  to: string;
}

function createDefaultCustomerFilters(): CustomerFilters {
  return {
    q: "",
    marketplace: "",
    from: "",
    to: "",
  };
}

async function fetchCustomers(filters: CustomerFilters) {
  const query = buildDashboardQuery({
    limit: "2000",
    q: filters.q || undefined,
    marketplace: filters.marketplace || undefined,
    from: filters.from || undefined,
    to: filters.to || undefined,
  });

  return fetchJson<CustomersPayload>(`/api/customers${query ? `?${query}` : ""}`);
}

async function fetchCustomerLocations(filters: CustomerFilters) {
  const query = buildDashboardQuery({
    q: filters.q || undefined,
    marketplace: filters.marketplace || undefined,
    from: filters.from || undefined,
    to: filters.to || undefined,
  });

  return fetchJson<CustomerLocationsPayload>(`/api/customers/locations${query ? `?${query}` : ""}`);
}

export function CustomersPage() {
  const [filters, setFilters] = useState(createDefaultCustomerFilters);
  const deferredSearch = useDeferredValue(filters.q);

  const customersQuery = useQuery({
    queryKey: ["customers-preview", deferredSearch, filters.marketplace, filters.from, filters.to],
    queryFn: () =>
      fetchCustomers({
        ...filters,
        q: deferredSearch,
      }),
  });

  const locationsQuery = useQuery({
    queryKey: ["customer-locations-preview", deferredSearch, filters.marketplace, filters.from, filters.to],
    queryFn: () =>
      fetchCustomerLocations({
        ...filters,
        q: deferredSearch,
      }),
  });

  const payload = customersQuery.data;
  const kpis = payload?.kpis ?? {};
  const items = payload?.items ?? [];
  const locationSummary = locationsQuery.data?.summary ?? {};
  const points = locationsQuery.data?.points ?? [];

  const customerCount = Math.max(Number(kpis.customers_count ?? payload?.total ?? 0), 0);
  const coverageCards = [
    {
      label: "Mit E-Mail",
      value: `${formatNumber(Number(kpis.with_email_count ?? 0))} (${formatPercent((Number(kpis.with_email_count ?? 0) / Math.max(customerCount, 1)) * 100)})`,
      icon: Mail,
    },
    {
      label: "Mit Adresse",
      value: `${formatNumber(Number(kpis.with_address_count ?? 0))} (${formatPercent((Number(kpis.with_address_count ?? 0) / Math.max(customerCount, 1)) * 100)})`,
      icon: MapPinned,
    },
    {
      label: "Cross-Channel",
      value: `${formatNumber(Number(kpis.cross_market_customers_count ?? 0))} (${formatPercent((Number(kpis.cross_market_customers_count ?? 0) / Math.max(customerCount, 1)) * 100)})`,
      icon: Repeat2,
    },
    {
      label: "Geo-Punkte",
      value: formatNumber(Number(locationSummary.points_total ?? 0)),
      icon: Globe2,
    },
  ];

  const marketplaceRows = [
    { label: "Shopify", count: Number(kpis.shopify_customers_count ?? 0) },
    { label: "Kaufland", count: Number(kpis.kaufland_customers_count ?? 0) },
  ];

  return (
    <div className="space-y-5">
      <section className="grid gap-3 xl:grid-cols-[1.55fr_1fr]">
        <SurfaceCard
          description="Die React-Customers-Route nutzt jetzt die bestehenden Overview- und Geo-Endpunkte inklusive interaktiver Karte."
          title="Customers Migration Preview"
        />

        <SurfaceCard
          action={
            <a href="/legacy?tab=customers" rel="noreferrer">
              <Button variant="outline">
                Legacy Geo View
                <ArrowUpRight className="ml-2 h-4 w-4" />
              </Button>
            </a>
          }
          description="Die Legacy-Route bleibt als Fallback erhalten. Karte, KPIs, Coverage und Kundenliste laufen jetzt aber bereits in React."
          title="Migrationsstrategie"
        />
      </section>

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <CustomerMetricCard label="Kunden" loading={customersQuery.isLoading} value={formatNumber(customerCount)} />
        <CustomerMetricCard
          label="Wiederkehrend"
          loading={customersQuery.isLoading}
          value={`${formatNumber(Number(kpis.repeat_customers_count ?? 0))} · ${formatPercent(Number(kpis.repeat_customers_rate_pct ?? 0))}`}
        />
        <CustomerMetricCard
          label="Orders / Kunde"
          loading={customersQuery.isLoading}
          value={Number(kpis.avg_orders_per_customer ?? 0).toLocaleString("de-DE", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
        />
        <CustomerMetricCard
          label="Umsatz / Kunde"
          loading={customersQuery.isLoading}
          value={formatCurrencyFromCents(Number(kpis.avg_revenue_per_customer_cents ?? 0))}
        />
      </section>

      <section className="grid gap-3 xl:grid-cols-[1.15fr_0.85fr]">
        <PageFilterCard
          description="Diese Filter laufen ueber die bestehenden Customers- und Customer-Location-Endpunkte."
          from={filters.from}
          marketplace={filters.marketplace}
          onFromChange={(value) => setFilters((current) => ({ ...current, from: value }))}
          onMarketplaceChange={(value) => setFilters((current) => ({ ...current, marketplace: value }))}
          onQueryChange={(value) => setFilters((current) => ({ ...current, q: value }))}
          onToChange={(value) => setFilters((current) => ({ ...current, to: value }))}
          query={filters.q}
          queryPlaceholder="Kunde, E-Mail, Telefon, Artikel"
          title="Basisfilter"
          to={filters.to}
        />

        <SurfaceCard description="Live-Status der Geo-Aufbereitung fuer denselben Filter wie in der Legacy-Kartenansicht." title="Geo-Status">
          <div className="grid gap-3 sm:grid-cols-2">
            <InfoTile label="Orders gesamt" value={formatNumber(Number(locationSummary.orders_total ?? 0))} />
            <InfoTile label="Punkte" value={formatNumber(Number(locationSummary.points_total ?? 0))} />
            <InfoTile label="Geocoded" value={formatNumber(Number(locationSummary.resolved_geocoded_count ?? 0))} />
            <InfoTile label="Country Fallback" value={formatNumber(Number(locationSummary.resolved_country_centroid_count ?? 0))} />
            <InfoTile label="Cache" value={locationSummary.cache_hit ? "Hit" : "Frisch"} />
            <InfoTile label="Serverzeit" value={`${formatNumber(Number(locationSummary.generated_in_ms ?? 0))} ms`} />
          </div>
        </SurfaceCard>
      </section>

      <section className="grid gap-3 xl:grid-cols-[1.05fr_0.95fr]">
        <SurfaceCard title="Datenabdeckung">
          <div className="grid gap-3 sm:grid-cols-2">
            {coverageCards.map((card) => {
              const Icon = card.icon;

              return (
                <div key={card.label} className="rounded-2xl border border-[var(--border)] bg-[var(--surface-2)] p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">{card.label}</p>
                      <p className="mt-3 text-sm font-medium text-[var(--ink)]">{customersQuery.isLoading ? "..." : card.value}</p>
                    </div>
                    <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-2.5 text-[var(--ink-3)]">
                      <Icon className="h-4 w-4" />
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </SurfaceCard>

        <SurfaceCard title="Marketplace Verteilung">
          <div className="space-y-3">
            {marketplaceRows.map((item) => {
              const share = customerCount > 0 ? (item.count / customerCount) * 100 : 0;

              return (
                <div key={item.label} className="rounded-2xl border border-[var(--border)] bg-[var(--surface-2)] px-4 py-3">
                  <div className="flex items-center justify-between gap-3 text-sm text-[var(--ink)]">
                    <span className="font-medium">{item.label}</span>
                    <span>{formatNumber(item.count)}</span>
                  </div>
                  <div className="mt-2 text-xs text-[var(--ink-4)]">{formatPercent(share)}</div>
                </div>
              );
            })}
          </div>
        </SurfaceCard>
      </section>

      <SurfaceCard
        action={
          <span className="rounded-full border border-[var(--border)] bg-[var(--surface-2)] px-3 py-1 text-xs text-[var(--ink-4)]">
            {locationsQuery.isLoading ? "Lade..." : `${formatNumber(points.length)} Punkte`}
          </span>
        }
        description="Interaktive Karte auf Basis derselben Geo-Punkte wie in der Legacy-Ansicht."
        title="Bestell-Herkunft"
      >
        {locationsQuery.isLoading ? (
          <div className="rounded-[20px] border border-[var(--border)] bg-[var(--surface-2)] px-4 py-4 text-sm text-[var(--ink-4)]">
            Geo-Daten werden geladen...
          </div>
        ) : locationsQuery.isError ? (
          <div className="rounded-[20px] border border-[color:rgba(183,72,55,0.25)] bg-[color:rgba(255,241,238,0.9)] px-4 py-3 text-sm text-[var(--danger)]">
            Geo-Daten konnten nicht geladen werden: {locationsQuery.error.message}
          </div>
        ) : points.length ? (
          <CustomerGeoMap points={points} />
        ) : (
          <div className="rounded-[20px] border border-[var(--border)] bg-[var(--surface-2)] px-4 py-4 text-sm text-[var(--ink-4)]">
            Keine Ortsdaten fuer den aktuellen Filter.
          </div>
        )}
      </SurfaceCard>

      <DataTableShell
        description="Top-Geo-Buckets aus `/api/customers/locations`."
        meta={locationsQuery.isLoading ? "Lade..." : `${formatNumber(points.length)} Punkte`}
        title="Geo Buckets"
      >
        <table className="min-w-full border-collapse text-sm">
          <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
            <tr>
              <th className="px-4 py-3 font-medium">Ort</th>
              <th className="px-4 py-3 font-medium">Channel</th>
              <th className="px-4 py-3 font-medium">Orders</th>
              <th className="px-4 py-3 font-medium">Umsatz</th>
              <th className="px-4 py-3 font-medium">Gewinn</th>
              <th className="px-4 py-3 font-medium">Quelle</th>
            </tr>
          </thead>
          <tbody>
            {locationsQuery.isLoading ? (
              <tr>
                <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={6}>
                  Geo-Daten werden geladen...
                </td>
              </tr>
            ) : locationsQuery.isError ? (
              <tr>
                <td className="px-4 py-4 text-[var(--danger)]" colSpan={6}>
                  Geo-Daten konnten nicht geladen werden: {locationsQuery.error.message}
                </td>
              </tr>
            ) : points.length ? (
              points.slice(0, 12).map((point, index) => {
                const profit = Number(point.profit_total_cents ?? 0);

                return (
                  <tr className="border-t border-[var(--border)] text-[var(--ink-2)]" key={`${point.city}-${point.country}-${index}`}>
                    <td className="px-4 py-3 text-[var(--ink)]">{[point.city, point.country].filter(Boolean).join(", ") || "Unbekannt"}</td>
                    <td className="px-4 py-3 capitalize">{point.dominant_marketplace ?? "-"}</td>
                    <td className="px-4 py-3">{formatNumber(Number(point.order_count ?? 0))}</td>
                    <td className="px-4 py-3">{formatCurrencyFromCents(point.revenue_total_cents)}</td>
                    <td className={`px-4 py-3 ${profit < 0 ? "text-[var(--danger)]" : "text-[var(--success)]"}`}>
                      {formatCurrencyFromCents(point.profit_total_cents)}
                    </td>
                    <td className="px-4 py-3">{point.provider ?? "-"}</td>
                  </tr>
                );
              })
            ) : (
              <tr>
                <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={6}>
                  Keine Ortsdaten fuer den aktuellen Filter.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </DataTableShell>

      <DataTableShell
        description="Die Kundenliste entspricht funktional dem Legacy-Overview und nutzt dieselbe Merging-Logik im Backend."
        meta={customersQuery.isLoading ? "Lade..." : `${formatNumber(Number(payload?.total ?? items.length))} Zeilen`}
        title="Kundenliste"
      >
        <table className="min-w-full border-collapse text-sm">
          <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
            <tr>
              <th className="px-4 py-3 font-medium">Kunde</th>
              <th className="px-4 py-3 font-medium">Kontakt</th>
              <th className="px-4 py-3 font-medium">Adresse</th>
              <th className="px-4 py-3 font-medium">Channel</th>
              <th className="px-4 py-3 font-medium">Orders</th>
              <th className="px-4 py-3 font-medium">Repeat</th>
              <th className="px-4 py-3 font-medium">Umsatz</th>
              <th className="px-4 py-3 font-medium">Gewinn</th>
              <th className="px-4 py-3 font-medium">Letzte Bestellung</th>
              <th className="px-4 py-3 font-medium">Top Artikel</th>
            </tr>
          </thead>
          <tbody>
            {customersQuery.isLoading ? (
              <tr>
                <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={10}>
                  Kunden werden geladen...
                </td>
              </tr>
            ) : customersQuery.isError ? (
              <tr>
                <td className="px-4 py-4 text-[var(--danger)]" colSpan={10}>
                  Kunden konnten nicht geladen werden: {customersQuery.error.message}
                </td>
              </tr>
            ) : items.length ? (
              items.map((item) => {
                const profit = Number(item.profit_total_cents ?? 0);

                return (
                  <tr className="border-t border-[var(--border)] align-top text-[var(--ink-2)]" key={item.customer_id || item.customer_name}>
                    <td className="px-4 py-3">
                      <div className="font-medium text-[var(--ink)]">{item.customer_name ?? "Unbekannt"}</div>
                      <div className="mt-1 text-xs text-[var(--ink-4)]">{item.customer_id ?? "-"}</div>
                    </td>
                    <td className="px-4 py-3">
                      <StackedLines values={[...(item.emails ?? []).slice(0, 1), ...(item.phones ?? []).slice(0, 1)]} />
                    </td>
                    <td className="px-4 py-3">
                      <StackedLines
                        values={[
                          String(item.primary_address?.street ?? "").trim(),
                          [String(item.primary_address?.postcode ?? "").trim(), String(item.primary_address?.city ?? "").trim()].filter(Boolean).join(" "),
                          String(item.primary_address?.country ?? "").trim(),
                        ]}
                      />
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex max-w-[180px] flex-wrap gap-1.5">
                        {(item.marketplaces ?? []).length
                          ? item.marketplaces?.map((market) => (
                              <span
                                className={`inline-flex rounded-full px-2.5 py-1 text-xs font-medium ${
                                  market === "shopify"
                                    ? "bg-[color:rgba(237,245,255,0.95)] text-[color:#2d568f]"
                                    : "bg-[color:rgba(233,249,240,0.95)] text-[color:#136b45]"
                                }`}
                                key={`${item.customer_id}-${market}`}
                              >
                                {market}
                              </span>
                            ))
                          : <span className="text-[var(--ink-4)]">-</span>}
                      </div>
                    </td>
                    <td className="px-4 py-3">{formatNumber(Number(item.order_count ?? 0))}</td>
                    <td className="px-4 py-3">
                      <span
                        className={`inline-flex rounded-full px-2.5 py-1 text-xs font-medium ${
                          item.repeat_customer
                            ? "bg-[color:rgba(233,249,240,0.95)] text-[color:#136b45]"
                            : "bg-[var(--surface-2)] text-[var(--ink-3)]"
                        }`}
                      >
                        {item.repeat_customer ? "Ja" : "Nein"}
                      </span>
                    </td>
                    <td className="px-4 py-3">{formatCurrencyFromCents(item.revenue_total_cents)}</td>
                    <td className={`px-4 py-3 ${profit < 0 ? "text-[var(--danger)]" : "text-[var(--success)]"}`}>
                      {formatCurrencyFromCents(item.profit_total_cents)}
                    </td>
                    <td className="px-4 py-3">{formatDate(item.last_order_date)}</td>
                    <td className="px-4 py-3">
                      <StackedLines values={(item.top_articles ?? []).slice(0, 3)} />
                    </td>
                  </tr>
                );
              })
            ) : (
              <tr>
                <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={10}>
                  Keine Kunden fuer den aktuellen Filter.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </DataTableShell>
    </div>
  );
}

function CustomerMetricCard({ label, value, loading }: { label: string; value: string; loading: boolean }) {
  return (
    <SurfaceCard>
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">{label}</p>
          <p className="mt-3 font-[var(--font-display)] text-2xl font-semibold text-[var(--ink)]">{loading ? "..." : value}</p>
        </div>
        <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface-2)] p-2.5 text-[var(--ink-3)]">
          <UsersRound className="h-4 w-4" />
        </div>
      </div>
    </SurfaceCard>
  );
}

function InfoTile({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface-2)] px-4 py-3">
      <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">{label}</p>
      <p className="mt-2 text-sm font-medium text-[var(--ink)]">{value}</p>
    </div>
  );
}

function StackedLines({ values }: { values: string[] }) {
  const lines = values.map((value) => String(value || "").trim()).filter(Boolean);

  if (!lines.length) {
    return <span className="text-[var(--ink-4)]">-</span>;
  }

  return (
    <div className="space-y-1">
      {lines.map((line) => (
        <div key={line}>{line}</div>
      ))}
    </div>
  );
}
