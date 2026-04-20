import { useDeferredValue, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowUpRight, FileUp, Megaphone, RefreshCcw, SearchCheck, TriangleAlert, WalletCards } from "lucide-react";
import { PageFilterCard } from "@/components/page-filter-card";
import { Button } from "@/components/ui/button";
import { DataTableShell } from "@/components/ui/data-table-shell";
import { SurfaceCard } from "@/components/ui/surface-card";
import { GoogleAdsProductDetailPanel } from "@/features/google-ads/product-detail-panel";
import type { GoogleAdsAnalyticsPayload, GoogleAdsProductRow } from "@/features/google-ads/types";
import { buildDashboardQuery, fetchJson } from "@/lib/api";
import { formatCurrencyFromCents, formatDate, formatNumber } from "@/lib/format";

interface GoogleAdsFilters {
  q: string;
  from: string;
  to: string;
}

function createDefaultGoogleAdsFilters(): GoogleAdsFilters {
  return {
    q: "",
    from: "",
    to: "",
  };
}

async function fetchGoogleAdsAnalytics(filters: GoogleAdsFilters) {
  const query = buildDashboardQuery({
    q: filters.q || undefined,
    from: filters.from || undefined,
    to: filters.to || undefined,
  });

  return fetchJson<GoogleAdsAnalyticsPayload>(`/api/google-ads/analytics${query ? `?${query}` : ""}`);
}

async function uploadGoogleAdsData(input: { reportFile: File | null; assignmentFile: File | null }) {
  const formData = new FormData();
  if (input.reportFile) {
    formData.append("report_file", input.reportFile);
  }
  if (input.assignmentFile) {
    formData.append("assignment_file", input.assignmentFile);
  }

  return fetchJson("/api/google-ads/upload", {
    method: "POST",
    body: formData,
  });
}

async function resetGoogleAdsData() {
  return fetchJson("/api/google-ads/reset", {
    method: "DELETE",
  });
}

export function GoogleAdsPage() {
  const queryClient = useQueryClient();
  const reportInputRef = useRef<HTMLInputElement | null>(null);
  const assignmentInputRef = useRef<HTMLInputElement | null>(null);
  const [filters, setFilters] = useState(createDefaultGoogleAdsFilters);
  const [reportFile, setReportFile] = useState<File | null>(null);
  const [assignmentFile, setAssignmentFile] = useState<File | null>(null);
  const [selectedProduct, setSelectedProduct] = useState<{ productKey: string; from: string; to: string } | null>(null);
  const [formMessage, setFormMessage] = useState<{ tone: "ok" | "error"; text: string } | null>(null);
  const deferredSearch = useDeferredValue(filters.q);

  const analyticsQuery = useQuery({
    queryKey: ["google-ads-preview", deferredSearch, filters.from, filters.to],
    queryFn: () =>
      fetchGoogleAdsAnalytics({
        ...filters,
        q: deferredSearch,
      }),
  });

  const uploadMutation = useMutation({
    mutationFn: uploadGoogleAdsData,
    onSuccess: async () => {
      setFormMessage({ tone: "ok", text: "Google Ads Dateien wurden importiert." });
      setReportFile(null);
      setAssignmentFile(null);
      if (reportInputRef.current) {
        reportInputRef.current.value = "";
      }
      if (assignmentInputRef.current) {
        assignmentInputRef.current.value = "";
      }
      await queryClient.invalidateQueries({ queryKey: ["google-ads-preview"] });
    },
    onError: (error: Error) => {
      setFormMessage({ tone: "error", text: error.message });
    },
  });

  const resetMutation = useMutation({
    mutationFn: resetGoogleAdsData,
    onSuccess: async () => {
      setFormMessage({ tone: "ok", text: "Google Ads Daten wurden zurueckgesetzt." });
      await queryClient.invalidateQueries({ queryKey: ["google-ads-preview"] });
    },
    onError: (error: Error) => {
      setFormMessage({ tone: "error", text: error.message });
    },
  });

  const payload = analyticsQuery.data;
  const kpis = payload?.kpis ?? {};
  const products = payload?.products ?? [];
  const missingAssignments = payload?.missing_assignments ?? [];
  const trend = payload?.trend ?? [];
  const imports = payload?.imports ?? {};
  const reportStatus = imports.report ?? {};
  const assignmentStatus = imports.assignment ?? {};

  const totalAdsCost = Number(kpis.ads_cost_total_cents ?? 0);
  const totalRevenue = Number(kpis.shopify_revenue_total_cents ?? 0);
  const totalOrders = Number(kpis.orders_count ?? 0);
  const totalProfitAfterAds = Number(kpis.profit_after_ads_total_cents ?? 0);
  const totalProfitBeforeAds = Number(kpis.profit_before_ads_total_cents ?? 0);
  const cards = [
    { label: "Ads Kosten", value: formatCurrencyFromCents(totalAdsCost), icon: WalletCards },
    { label: "Shopify Umsatz", value: formatCurrencyFromCents(totalRevenue), icon: Megaphone },
    { label: "Gewinn nach Ads", value: formatCurrencyFromCents(totalProfitAfterAds), icon: SearchCheck },
    { label: "Fehlende Zuordnung", value: formatNumber(Number(kpis.missing_assignments_count ?? 0)), icon: TriangleAlert },
  ];

  return (
    <div className="space-y-5">
      <section className="grid gap-3 xl:grid-cols-[1.55fr_1fr]">
        <SurfaceCard
          description="Die React-Google-Ads-Route uebernimmt jetzt Importstatus, Upload, Reset, KPI-Analyse, Produkttabelle und Detail-Drilldown ueber die bestehenden FastAPI-Endpunkte."
          title="Google Ads Migration Preview"
        />

        <SurfaceCard
          action={
            <a href="/legacy?tab=googleads" rel="noreferrer">
              <Button variant="outline">
                Legacy Google Ads
                <ArrowUpRight className="ml-2 h-4 w-4" />
              </Button>
            </a>
          }
          description="Der Upload bleibt voll funktionsfaehig, weil die React-Route direkt dieselben Import- und Reset-Endpunkte nutzt wie das Legacy-Dashboard."
          title="Funktionssicherheit"
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

      <section className="grid gap-3 xl:grid-cols-[1.1fr_0.9fr]">
        <PageFilterCard
          description="Zeitraum und Produktsuche werden 1:1 an `/api/google-ads/analytics` weitergegeben."
          from={filters.from}
          marketplace=""
          marketplaceLabel="Dataset"
          marketplaceOptions={[{ value: "", label: "Google Ads" }]}
          onFromChange={(value) => setFilters((current) => ({ ...current, from: value }))}
          onMarketplaceChange={() => undefined}
          onQueryChange={(value) => setFilters((current) => ({ ...current, q: value }))}
          onToChange={(value) => setFilters((current) => ({ ...current, to: value }))}
          query={filters.q}
          queryPlaceholder="Produkt, Key oder Missing Assignment"
          title="Basisfilter"
          to={filters.to}
        />

        <SurfaceCard description="CSV-Import und Reset laufen direkt ueber die bestehenden Google-Ads-Endpoints." title="Import & Reset">
          <div className="grid gap-3">
            <label className="block">
              <span className="mb-2 block text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">Report CSV</span>
              <input
                accept=".csv,text/csv"
                className="block w-full rounded-2xl border border-[var(--border-strong)] bg-[var(--surface)] px-3 py-2.5 text-sm text-[var(--ink)] file:mr-3 file:rounded-xl file:border-0 file:bg-[var(--surface-2)] file:px-3 file:py-2 file:text-xs file:font-medium"
                onChange={(event) => setReportFile(event.target.files?.[0] ?? null)}
                ref={reportInputRef}
                type="file"
              />
              <p className="mt-2 text-xs text-[var(--ink-4)]">{reportFile ? reportFile.name : reportStatus.filename || "Keine Datei"}</p>
            </label>

            <label className="block">
              <span className="mb-2 block text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">Assignment CSV</span>
              <input
                accept=".csv,text/csv"
                className="block w-full rounded-2xl border border-[var(--border-strong)] bg-[var(--surface)] px-3 py-2.5 text-sm text-[var(--ink)] file:mr-3 file:rounded-xl file:border-0 file:bg-[var(--surface-2)] file:px-3 file:py-2 file:text-xs file:font-medium"
                onChange={(event) => setAssignmentFile(event.target.files?.[0] ?? null)}
                ref={assignmentInputRef}
                type="file"
              />
              <p className="mt-2 text-xs text-[var(--ink-4)]">{assignmentFile ? assignmentFile.name : assignmentStatus.filename || "Keine Datei"}</p>
            </label>

            {formMessage ? (
              <div
                className={`rounded-2xl border px-4 py-3 text-sm ${
                  formMessage.tone === "ok"
                    ? "border-[color:rgba(39,134,86,0.22)] bg-[color:rgba(239,250,244,0.96)] text-[color:#13613f]"
                    : "border-[color:rgba(183,72,55,0.24)] bg-[color:rgba(255,241,238,0.92)] text-[var(--danger)]"
                }`}
              >
                {formMessage.text}
              </div>
            ) : null}

            <div className="flex flex-wrap gap-2">
              <Button
                disabled={uploadMutation.isPending || (!reportFile && !assignmentFile)}
                onClick={() => {
                  setFormMessage(null);
                  uploadMutation.mutate({ reportFile, assignmentFile });
                }}
              >
                <FileUp className="mr-2 h-4 w-4" />
                Importieren
              </Button>
              <Button
                disabled={resetMutation.isPending}
                onClick={() => {
                  setFormMessage(null);
                  resetMutation.mutate();
                }}
                variant="outline"
              >
                <RefreshCcw className="mr-2 h-4 w-4" />
                Zuruecksetzen
              </Button>
            </div>
          </div>
        </SurfaceCard>
      </section>

      <section className="grid gap-3 xl:grid-cols-[1.05fr_0.95fr]">
        <SurfaceCard title="Import Status">
          <div className="grid gap-3 sm:grid-cols-2">
            <InfoTile label="Report Datei" value={String(reportStatus.filename || "-")} />
            <InfoTile label="Report Import" value={reportStatus.imported_at ? formatDate(reportStatus.imported_at) : "-"} />
            <InfoTile
              label="Report Zeitraum"
              value={
                reportStatus.meta?.report_from_day && reportStatus.meta?.report_to_day
                  ? `${reportStatus.meta.report_from_day} - ${reportStatus.meta.report_to_day}`
                  : "-"
              }
            />
            <InfoTile label="Report Zeilen" value={formatNumber(Number(reportStatus.meta?.rows ?? 0))} />
          </div>
        </SurfaceCard>

        <SurfaceCard title="Performance Snapshot">
          <div className="grid gap-3 sm:grid-cols-2">
            <InfoTile label="ROAS" value={`${Number(kpis.roas ?? 0).toFixed(2)}x`} />
            <InfoTile label="Orders" value={formatNumber(totalOrders)} />
            <InfoTile
              label="Kosten / Order"
              value={totalOrders > 0 ? formatCurrencyFromCents(Math.round(totalAdsCost / totalOrders)) : "-"}
            />
            <InfoTile
              label="Ads Anteil Umsatz"
              value={totalRevenue > 0 ? `${((totalAdsCost / totalRevenue) * 100).toFixed(1)} %` : "-"}
            />
            <InfoTile label="Vor Ads" value={formatCurrencyFromCents(totalProfitBeforeAds)} />
            <InfoTile label="Nach Ads" value={formatCurrencyFromCents(totalProfitAfterAds)} />
          </div>
        </SurfaceCard>
      </section>

      <DataTableShell
        description="Tagesverlauf fuer Ads Kosten, gemappte Ads, Umsatz und Profit."
        meta={analyticsQuery.isLoading ? "Lade..." : `${formatNumber(trend.length)} Tage`}
        title="Trend"
      >
        <table className="min-w-full border-collapse text-sm">
          <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
            <tr>
              <th className="px-4 py-3 font-medium">Tag</th>
              <th className="px-4 py-3 font-medium">Ads gesamt</th>
              <th className="px-4 py-3 font-medium">Ads gemappt</th>
              <th className="px-4 py-3 font-medium">Umsatz</th>
              <th className="px-4 py-3 font-medium">Gewinn</th>
              <th className="px-4 py-3 font-medium">Orders</th>
            </tr>
          </thead>
          <tbody>
            {analyticsQuery.isLoading ? (
              <tr>
                <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={6}>
                  Trenddaten werden geladen...
                </td>
              </tr>
            ) : trend.length ? (
              trend.map((row, index) => (
                <tr className="border-t border-[var(--border)] text-[var(--ink-2)]" key={`${row.day}-${index}`}>
                  <td className="px-4 py-3 text-[var(--ink)]">{formatDate(row.day)}</td>
                  <td className="px-4 py-3">{formatCurrencyFromCents(row.ads_cost_cents)}</td>
                  <td className="px-4 py-3">{formatCurrencyFromCents(row.mapped_ads_cost_cents)}</td>
                  <td className="px-4 py-3">{formatCurrencyFromCents(row.revenue_cents)}</td>
                  <td className="px-4 py-3">{formatCurrencyFromCents(row.profit_cents)}</td>
                  <td className="px-4 py-3">{formatNumber(Number(row.order_count ?? 0))}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={6}>
                  Keine Trenddaten fuer den aktuellen Filter.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </DataTableShell>

      <section className="grid gap-3 xl:grid-cols-[1.15fr_0.85fr]">
        <DataTableShell
          description="Produktsicht mit Profit vor und nach Ads. Klick auf eine Zeile oeffnet den Detailverlauf."
          meta={analyticsQuery.isLoading ? "Lade..." : `${formatNumber(products.length)} Zeilen`}
          title="Produkte"
        >
          <table className="min-w-full border-collapse text-sm">
            <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
              <tr>
                <th className="px-4 py-3 font-medium">Produkt</th>
                <th className="px-4 py-3 font-medium">Mapping</th>
                <th className="px-4 py-3 font-medium">Ads</th>
                <th className="px-4 py-3 font-medium">Orders</th>
                <th className="px-4 py-3 font-medium">Umsatz</th>
                <th className="px-4 py-3 font-medium">ROAS</th>
                <th className="px-4 py-3 font-medium">Gewinn vor Ads</th>
                <th className="px-4 py-3 font-medium">Gewinn nach Ads</th>
              </tr>
            </thead>
            <tbody>
              {analyticsQuery.isLoading ? (
                <tr>
                  <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={8}>
                    Produkte werden geladen...
                  </td>
                </tr>
              ) : products.length ? (
                products.map((product) => <ProductRow filters={filters} key={product.product_key} onOpen={setSelectedProduct} product={product} />)
              ) : (
                <tr>
                  <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={8}>
                    Keine Produktdaten fuer den aktuellen Filter.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </DataTableShell>

        <DataTableShell
          description="Article IDs ohne Assignment-Zuordnung."
          meta={analyticsQuery.isLoading ? "Lade..." : `${formatNumber(missingAssignments.length)} Zeilen`}
          title="Fehlende Zuweisungen"
        >
          <table className="min-w-full border-collapse text-sm">
            <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
              <tr>
                <th className="px-4 py-3 font-medium">Artikel-ID</th>
                <th className="px-4 py-3 font-medium">Ads Kosten</th>
                <th className="px-4 py-3 font-medium">Tage</th>
              </tr>
            </thead>
            <tbody>
              {analyticsQuery.isLoading ? (
                <tr>
                  <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={3}>
                    Missing Assignments werden geladen...
                  </td>
                </tr>
              ) : missingAssignments.length ? (
                missingAssignments.map((item) => (
                  <tr className="border-t border-[var(--border)] text-[var(--ink-2)]" key={item.article_id}>
                    <td className="px-4 py-3 text-[var(--ink)]">{item.article_id ?? "-"}</td>
                    <td className="px-4 py-3">{formatCurrencyFromCents(item.ads_cost_cents)}</td>
                    <td className="px-4 py-3">{formatNumber(Number(item.day_count ?? 0))}</td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={3}>
                    Keine fehlenden Zuweisungen.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </DataTableShell>
      </section>

      <GoogleAdsProductDetailPanel onClose={() => setSelectedProduct(null)} selection={selectedProduct} />
    </div>
  );
}

function ProductRow({
  product,
  onOpen,
  filters,
}: {
  product: GoogleAdsProductRow;
  onOpen: (selection: { productKey: string; from: string; to: string }) => void;
  filters: GoogleAdsFilters;
}) {
  const adsCost = Number(product.ads_cost_cents ?? 0);
  const revenue = Number(product.revenue_total_cents ?? 0);
  const roas = adsCost > 0 ? revenue / adsCost : 0;
  const profitAfterAds = Number(product.profit_after_ads_cents ?? 0);

  return (
    <tr className="border-t border-[var(--border)] text-[var(--ink-2)]">
      <td className="px-4 py-3 text-[var(--ink)]">
        <button
          className="max-w-[280px] text-left font-medium underline-offset-4 hover:underline"
          onClick={() =>
            onOpen({
              productKey: String(product.product_key ?? ""),
              from: filters.from,
              to: filters.to,
            })
          }
          type="button"
        >
          {product.product_label ?? "-"}
        </button>
        <div className="mt-1 text-xs text-[var(--ink-4)]">{product.product_detail ?? "-"}</div>
      </td>
      <td className="px-4 py-3">
        <span
          className={`inline-flex rounded-full px-2.5 py-1 text-xs font-medium ${
            product.mapped
              ? "bg-[color:rgba(237,245,255,0.95)] text-[color:#2d568f]"
              : "bg-[color:rgba(255,241,238,0.92)] text-[var(--danger)]"
          }`}
        >
          {product.mapped ? "Gemappt" : "Unmapped"}
        </span>
      </td>
      <td className="px-4 py-3">{formatCurrencyFromCents(product.ads_cost_cents)}</td>
      <td className="px-4 py-3">{formatNumber(Number(product.order_count ?? 0))}</td>
      <td className="px-4 py-3">{formatCurrencyFromCents(product.revenue_total_cents)}</td>
      <td className="px-4 py-3">{adsCost > 0 ? `${roas.toFixed(2)}x` : "-"}</td>
      <td className="px-4 py-3">{formatCurrencyFromCents(product.profit_before_ads_cents)}</td>
      <td className={`px-4 py-3 ${profitAfterAds < 0 ? "text-[var(--danger)]" : "text-[var(--success)]"}`}>
        {formatCurrencyFromCents(product.profit_after_ads_cents)}
      </td>
    </tr>
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
