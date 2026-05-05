import { useDashboardShellState } from "@/app/dashboard-shell-state";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  BarController,
  BarElement,
  CategoryScale,
  Chart,
  Filler,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
  type ChartConfiguration,
} from "chart.js";

import { formatDateToken, formatMoneyFromCents, MONEY_FORMATTER, NUMBER_FORMATTER } from "@/features/analytics/format";
import { useTheme } from "@/shared/theme/theme-provider";

import {
  fetchGoogleAdsAnalytics,
  fetchGoogleAdsProductDetail,
  resetGoogleAds,
  uploadGoogleAdsFiles,
  type GoogleAdsAnalytics,
  type GoogleAdsProduct,
  type GoogleAdsProductDetail,
  type GoogleAdsTrendPoint,
} from "./api";

Chart.register(
  BarController,
  BarElement,
  CategoryScale,
  Filler,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
);

function readCssVariable(name: string, fallback: string) {
  const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return value || fallback;
}

function useChart(
  canvasId: string,
  buildConfiguration: () => ChartConfiguration | null,
  deps: ReadonlyArray<unknown>,
) {
  useEffect(() => {
    const canvas = document.getElementById(canvasId);
    if (!(canvas instanceof HTMLCanvasElement)) {
      return;
    }

    const configuration = buildConfiguration();
    if (!configuration) {
      return;
    }

    const chart = new Chart(canvas, configuration);
    return () => {
      chart.destroy();
    };
  }, deps);
}

function useThemeRenderToken() {
  const { theme, themeVersion } = useTheme();
  return `${theme}:${themeVersion}`;
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

function truncateLabel(text: string, maxLen: number) {
  if (!text || text.length <= maxLen) {
    return text || "";
  }
  return `${text.slice(0, maxLen - 1)}...`;
}

function buildQuery(from: string, to: string, q: string) {
  return {
    from: String(from || "").trim(),
    to: String(to || "").trim(),
    q: String(q || "").trim(),
  };
}

function ProductDetailRow({
  detail,
  product,
  colSpan,
  themeToken,
}: {
  detail: GoogleAdsProductDetail | null;
  product: GoogleAdsProduct;
  colSpan: number;
  themeToken: string;
}) {
  const trend = Array.isArray(detail?.trend) ? detail?.trend : [];
  const kpis = detail?.kpis || {};

  useChart(
    "gaProductDetailCanvas",
    () => {
      if (!trend.length) {
        return null;
      }

      const labels = trend.map((row) => formatDateToken(String(row.day || "")));
      let cumulativeAds = 0;
      let cumulativeRevenue = 0;
      let cumulativeProfit = 0;
      const cumulativeAdsSeries: number[] = [];
      const cumulativeRevenueSeries: number[] = [];
      const cumulativeProfitAfterSeries: number[] = [];

      for (const row of trend) {
        cumulativeAds += Number(row.ads_cost_cents || 0) / 100;
        cumulativeRevenue += Number(row.revenue_cents || 0) / 100;
        cumulativeProfit += Number(row.profit_cents || 0) / 100;
        cumulativeAdsSeries.push(cumulativeAds);
        cumulativeRevenueSeries.push(cumulativeRevenue);
        cumulativeProfitAfterSeries.push(cumulativeProfit - cumulativeAds);
      }

      const pointRadius = trend.length > 120 ? 0 : trend.length > 64 ? 1 : 2;
      const okColor = readCssVariable("--th-ok", "#2b9b68");
      const warnColor = readCssVariable("--th-warn", "#d88b25");
      const chartTwo = readCssVariable("--th-chart-2", "#0f8a7a");
      const gridColor = readCssVariable("--th-chart-grid", "rgba(73, 90, 118, 0.15)");
      const labelColor = readCssVariable("--th-chart-label", "#74839c");

      return {
        type: "line",
        data: {
          labels,
          datasets: [
            {
              label: "Kum. Umsatz",
              data: cumulativeRevenueSeries,
              borderColor: chartTwo,
              pointRadius,
              pointHoverRadius: 4,
              pointHitRadius: 10,
              borderWidth: 1.5,
              tension: 0.3,
              fill: false,
              borderDash: [4, 3],
            },
            {
              label: "Kum. Ads Kosten",
              data: cumulativeAdsSeries,
              borderColor: warnColor,
              backgroundColor: `${warnColor}22`,
              pointRadius,
              pointHoverRadius: 4,
              pointHitRadius: 10,
              borderWidth: 2.1,
              tension: 0.3,
              fill: false,
            },
            {
              label: "Kum. Gewinn nach Ads",
              data: cumulativeProfitAfterSeries,
              borderColor: okColor,
              backgroundColor: `${okColor}18`,
              pointRadius,
              pointHoverRadius: 4,
              pointHitRadius: 10,
              borderWidth: 2.5,
              tension: 0.3,
              fill: true,
            },
          ],
        },
        options: {
          maintainAspectRatio: false,
          interaction: { mode: "index", intersect: false },
          plugins: {
            legend: {
              display: true,
              position: "bottom",
              align: "start",
              labels: {
                usePointStyle: true,
                pointStyle: "line",
                boxWidth: 20,
                boxHeight: 6,
                color: labelColor,
                font: { size: 11, weight: 600 },
              },
            },
            tooltip: {
              callbacks: {
                label(context: any) {
                  const label = String(context.dataset?.label || "Wert");
                  return `${label}: ${MONEY_FORMATTER.format(Number(context.parsed?.y || 0))}`;
                },
              },
            },
          },
          scales: {
            x: {
              grid: { display: false },
              ticks: { color: labelColor, maxRotation: 0, autoSkip: true, maxTicksLimit: 12 },
            },
            y: {
              grid: { color: gridColor },
              ticks: {
                color: labelColor,
                callback(value: string | number) {
                  return Number(value).toLocaleString("de-DE", { style: "currency", currency: "EUR" });
                },
              },
            },
          },
        },
      } as unknown as ChartConfiguration;
    },
    [detail, themeToken],
  );

  const adsCost = Number(kpis.ads_cost_total_cents || 0);
  const revenue = Number(kpis.revenue_total_cents || 0);
  const profitBefore = Number(kpis.profit_before_ads_cents || 0);
  const profitAfter = Number(kpis.profit_after_ads_cents || 0);
  const roas = Number(kpis.roas || 0);
  const orders = Number(kpis.orders_count || 0);
  const costPerOrder = orders > 0 ? Math.round(adsCost / orders) : 0;

  return (
    <tr id="gaProductDetailRow" className="ga-product-detail-row">
      <td colSpan={colSpan}>
        <div className="ga-product-detail">
          {!detail ? <div className="ga-product-detail-loading">Lade Produktdaten...</div> : null}
          {detail ? (
            <>
              <div className="ga-product-detail-kpis">
                <div className="ga-detail-kpi"><span className="ga-detail-kpi-label">Produkt</span><span className="ga-detail-kpi-value">{String(product.product_label || "-")}</span></div>
                <div className="ga-detail-kpi"><span className="ga-detail-kpi-label">Ads Kosten</span><span className="ga-detail-kpi-value">{formatMoneyFromCents(adsCost)}</span></div>
                <div className="ga-detail-kpi"><span className="ga-detail-kpi-label">Umsatz</span><span className="ga-detail-kpi-value">{formatMoneyFromCents(revenue)}</span></div>
                <div className="ga-detail-kpi"><span className="ga-detail-kpi-label">Gewinn vor Ads</span><span className="ga-detail-kpi-value">{formatMoneyFromCents(profitBefore)}</span></div>
                <div className="ga-detail-kpi"><span className="ga-detail-kpi-label">Gewinn nach Ads</span><span className={`ga-detail-kpi-value ${profitAfter < 0 ? "value-neg" : "value-pos"}`}>{formatMoneyFromCents(profitAfter)}</span></div>
                <div className="ga-detail-kpi"><span className="ga-detail-kpi-label">ROAS</span><span className="ga-detail-kpi-value">{`${roas.toFixed(2)}x`}</span></div>
                <div className="ga-detail-kpi"><span className="ga-detail-kpi-label">Orders</span><span className="ga-detail-kpi-value">{NUMBER_FORMATTER.format(orders)}</span></div>
                <div className="ga-detail-kpi"><span className="ga-detail-kpi-label">Kosten/Order</span><span className="ga-detail-kpi-value">{orders > 0 ? formatMoneyFromCents(costPerOrder) : "-"}</span></div>
              </div>
              {trend.length ? (
                <div className="ga-product-detail-chart-wrap">
                  <canvas id="gaProductDetailCanvas" />
                </div>
              ) : null}
            </>
          ) : null}
        </div>
      </td>
    </tr>
  );
}

export function GoogleAdsPage() {
  const { filters, refreshRequestToken } = useDashboardShellState();
  const themeToken = useThemeRenderToken();
  const [payload, setPayload] = useState<GoogleAdsAnalytics | null>(null);
  const [expandedProductKey, setExpandedProductKey] = useState("");
  const [productDetail, setProductDetail] = useState<GoogleAdsProductDetail | null>(null);
  const [reportFileName, setReportFileName] = useState("Keine Datei");
  const [assignmentFileName, setAssignmentFileName] = useState("Keine Datei");
  const [error, setError] = useState("");
  const [isLoading, setLoading] = useState(true);
  const [isUploading, setUploading] = useState(false);
  const [isResetting, setResetting] = useState(false);
  const detailRequestIdRef = useRef(0);
  const lastRefreshRequestTokenRef = useRef(refreshRequestToken);

  const query = useMemo(() => buildQuery(filters.from, filters.to, filters.q), [filters.from, filters.q, filters.to]);

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      setLoading(true);
      try {
        const nextPayload = await fetchGoogleAdsAnalytics(query);
        if (!cancelled) {
          setPayload(nextPayload);
          setError("");
        }
      } catch (nextError) {
        if (!cancelled) {
          setError(nextError instanceof Error ? nextError.message : "Google Ads Daten konnten nicht geladen werden.");
          setPayload(null);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    };

    void load();

    return () => {
      cancelled = true;
    };
  }, [query]);

  useEffect(() => {
    setExpandedProductKey("");
    setProductDetail(null);
  }, [query]);

  useEffect(() => {
    if (refreshRequestToken === 0 || lastRefreshRequestTokenRef.current === refreshRequestToken) {
      return;
    }
    lastRefreshRequestTokenRef.current = refreshRequestToken;

    setLoading(true);
    void fetchGoogleAdsAnalytics(query)
      .then((nextPayload) => {
        setPayload(nextPayload);
        setError("");
      })
      .catch((nextError: Error) => {
        setError(nextError.message);
        setPayload(null);
      })
      .finally(() => {
        setLoading(false);
      });
  }, [query, refreshRequestToken]);

  useEffect(() => {
    if (!expandedProductKey) {
      return;
    }

    const requestId = detailRequestIdRef.current + 1;
    detailRequestIdRef.current = requestId;
    setProductDetail(null);

    void fetchGoogleAdsProductDetail(expandedProductKey, query)
      .then((detail) => {
        if (detailRequestIdRef.current === requestId) {
          setProductDetail(detail);
          setError("");
        }
      })
      .catch((nextError: Error) => {
        if (detailRequestIdRef.current === requestId) {
          setError(nextError.message);
          setProductDetail(null);
        }
      });
  }, [expandedProductKey, query]);

  const kpis = payload?.kpis || {};
  const imports = payload?.imports || {};
  const reportImport = imports.report && typeof imports.report === "object" ? imports.report : {};
  const assignmentImport = imports.assignment && typeof imports.assignment === "object" ? imports.assignment : {};
  const reportMeta = reportImport.meta && typeof reportImport.meta === "object" ? reportImport.meta : {};
  const assignmentMeta = assignmentImport.meta && typeof assignmentImport.meta === "object" ? assignmentImport.meta : {};
  const products = Array.isArray(payload?.products) ? payload.products : [];
  const missingAssignments = Array.isArray(payload?.missing_assignments) ? payload.missing_assignments : [];
  const trendRows = Array.isArray(payload?.trend) ? payload.trend : [];

  const totalAdsCost = Number(kpis.ads_cost_total_cents || 0);
  const totalRevenue = Number(kpis.shopify_revenue_total_cents || 0);
  const totalOrders = Number(kpis.orders_count || 0);
  const profitAfter = Number(kpis.profit_after_ads_total_cents || 0);
  const profitBefore = Number(kpis.profit_before_ads_total_cents || 0);
  const roas = Number(kpis.roas || 0);
  const missingCount = Number(kpis.missing_assignments_count || 0);
  const hasData = totalAdsCost > 0 || Number(kpis.products_count || 0) > 0;

  const reportRangeLabel = reportMeta.report_from_day && reportMeta.report_to_day
    ? `${formatDateToken(String(reportMeta.report_from_day || ""))} - ${formatDateToken(String(reportMeta.report_to_day || ""))}`
    : "-";
  const reportLastLabel = reportMeta.report_to_day || reportMeta.last_non_zero_day
    ? formatDateToken(String(reportMeta.report_to_day || reportMeta.last_non_zero_day || ""))
    : "-";

  useChart(
    "googleAdsTrendChart",
    () => {
      if (!trendRows.length) {
        return null;
      }

      const pointRadius = trendRows.length > 120 ? 0 : trendRows.length > 64 ? 1 : 2;
      return {
        type: "line",
        data: {
          labels: trendRows.map((row) => formatDateToken(String(row.day || ""))),
          datasets: [
            {
              label: "Ads gesamt",
              data: trendRows.map((row) => Number(row.ads_cost_cents || 0) / 100),
              borderColor: readCssVariable("--th-chart-1", "#1f73e0"),
              backgroundColor: readCssVariable("--th-chart-1-fill", "rgba(31, 115, 224, 0.12)"),
              pointRadius,
              pointHoverRadius: 4,
              pointHitRadius: 10,
              borderWidth: 2.1,
              tension: 0.3,
              fill: false,
            },
            {
              label: "Ads gemappt",
              data: trendRows.map((row) => Number(row.mapped_ads_cost_cents || 0) / 100),
              borderColor: readCssVariable("--th-chart-2", "#0f8a7a"),
              backgroundColor: readCssVariable("--th-chart-2-fill", "rgba(15, 138, 122, 0.14)"),
              pointRadius,
              pointHoverRadius: 4,
              pointHitRadius: 10,
              borderWidth: 2,
              tension: 0.3,
              fill: false,
            },
          ],
        },
        options: {
          maintainAspectRatio: false,
          interaction: { mode: "index", intersect: false },
          plugins: {
            legend: {
              display: true,
              position: "bottom",
              align: "start",
              labels: {
                usePointStyle: true,
                pointStyle: "line",
                boxWidth: 20,
                boxHeight: 6,
                color: readCssVariable("--th-chart-label", "#74839c"),
                font: { size: 11, weight: 600 },
              },
            },
            tooltip: {
              callbacks: {
                label(context: any) {
                  return `${String(context.dataset?.label || "Wert")}: ${MONEY_FORMATTER.format(Number(context.parsed?.y || 0))}`;
                },
              },
            },
          },
          scales: {
            x: {
              grid: { display: false },
              ticks: { color: readCssVariable("--th-chart-label", "#74839c"), maxRotation: 0, autoSkip: true, maxTicksLimit: 14 },
            },
            y: {
              grid: { color: readCssVariable("--th-chart-grid", "rgba(73, 90, 118, 0.15)") },
              ticks: {
                color: readCssVariable("--th-chart-label", "#74839c"),
                callback(value: string | number) {
                  return Number(value).toLocaleString("de-DE", { style: "currency", currency: "EUR" });
                },
              },
            },
          },
        },
      } as unknown as ChartConfiguration;
    },
    [trendRows, themeToken],
  );

  useChart(
    "googleAdsCumulChart",
    () => {
      if (!trendRows.length) {
        return null;
      }

      const labels = trendRows.map((row) => formatDateToken(String(row.day || "")));
      let cumulativeAds = 0;
      let cumulativeRevenue = 0;
      let cumulativeProfit = 0;
      const cumulativeAdsSeries: number[] = [];
      const cumulativeRevenueSeries: number[] = [];
      const cumulativeProfitSeries: number[] = [];
      const cumulativeProfitAfterSeries: number[] = [];

      for (const row of trendRows) {
        cumulativeAds += Number(row.ads_cost_cents || 0) / 100;
        cumulativeRevenue += Number(row.revenue_cents || 0) / 100;
        cumulativeProfit += Number(row.profit_cents || 0) / 100;
        cumulativeAdsSeries.push(cumulativeAds);
        cumulativeRevenueSeries.push(cumulativeRevenue);
        cumulativeProfitSeries.push(cumulativeProfit);
        cumulativeProfitAfterSeries.push(cumulativeProfit - cumulativeAds);
      }

      const pointRadius = trendRows.length > 120 ? 0 : trendRows.length > 64 ? 1 : 2;
      const chartTwo = readCssVariable("--th-chart-2", "#0f8a7a");
      const warnColor = readCssVariable("--th-warn", "#d88b25");
      const okColor = readCssVariable("--th-ok", "#2b9b68");
      const gridColor = readCssVariable("--th-chart-grid", "rgba(73, 90, 118, 0.15)");
      const labelColor = readCssVariable("--th-chart-label", "#74839c");

      return {
        type: "line",
        data: {
          labels,
          datasets: [
            {
              label: "Kum. Umsatz",
              data: cumulativeRevenueSeries,
              borderColor: chartTwo,
              pointRadius,
              pointHoverRadius: 4,
              pointHitRadius: 10,
              borderWidth: 1.5,
              tension: 0.3,
              fill: false,
              borderDash: [4, 3],
            },
            {
              label: "Kum. Gewinn (vor Ads)",
              data: cumulativeProfitSeries,
              borderColor: chartTwo,
              backgroundColor: `${chartTwo}1f`,
              pointRadius,
              pointHoverRadius: 4,
              pointHitRadius: 10,
              borderWidth: 2.1,
              tension: 0.3,
              fill: false,
            },
            {
              label: "Kum. Ads Kosten",
              data: cumulativeAdsSeries,
              borderColor: warnColor,
              backgroundColor: `${warnColor}22`,
              pointRadius,
              pointHoverRadius: 4,
              pointHitRadius: 10,
              borderWidth: 2.1,
              tension: 0.3,
              fill: false,
            },
            {
              label: "Kum. Gewinn nach Ads",
              data: cumulativeProfitAfterSeries,
              borderColor: okColor,
              backgroundColor: `${okColor}18`,
              pointRadius,
              pointHoverRadius: 4,
              pointHitRadius: 10,
              borderWidth: 2.5,
              tension: 0.3,
              fill: true,
            },
          ],
        },
        options: {
          maintainAspectRatio: false,
          interaction: { mode: "index", intersect: false },
          plugins: {
            legend: {
              display: true,
              position: "bottom",
              align: "start",
              labels: {
                usePointStyle: true,
                pointStyle: "line",
                boxWidth: 20,
                boxHeight: 6,
                color: labelColor,
                font: { size: 11, weight: 600 },
              },
            },
            tooltip: {
              callbacks: {
                label(context: any) {
                  return `${String(context.dataset?.label || "Wert")}: ${MONEY_FORMATTER.format(Number(context.parsed?.y || 0))}`;
                },
              },
            },
          },
          scales: {
            x: {
              grid: { display: false },
              ticks: { color: labelColor, maxRotation: 0, autoSkip: true, maxTicksLimit: 14 },
            },
            y: {
              grid: { color: gridColor },
              ticks: {
                color: labelColor,
                callback(value: string | number) {
                  return Number(value).toLocaleString("de-DE", { style: "currency", currency: "EUR" });
                },
              },
            },
          },
        },
      } as unknown as ChartConfiguration;
    },
    [trendRows, themeToken],
  );

  const productsWithAds = products.filter((product) => Number(product.ads_cost_cents || 0) > 0);
  const topProfitProducts = productsWithAds
    .slice()
    .sort((left, right) => Number(left.profit_after_ads_cents || 0) - Number(right.profit_after_ads_cents || 0))
    .slice(-12)
    .reverse();

  useChart(
    "googleAdsProfitChart",
    () => {
      if (!topProfitProducts.length) {
        return null;
      }

      const profitData = topProfitProducts.map((product) => Number(product.profit_after_ads_cents || 0) / 100);
      const okColor = readCssVariable("--th-ok", "#2b9b68");
      const warnColor = readCssVariable("--th-warn", "#d88b25");
      return {
        type: "bar",
        data: {
          labels: topProfitProducts.map((product) => truncateLabel(String(product.product_label || "-"), 28)),
          datasets: [{
            label: "Gewinn nach Ads",
            data: profitData,
            backgroundColor: profitData.map((value) => value >= 0 ? `${okColor}33` : `${warnColor}33`),
            borderColor: profitData.map((value) => value >= 0 ? okColor : warnColor),
            borderWidth: 1.5,
            borderRadius: 4,
            borderSkipped: false,
          }],
        },
        options: {
          indexAxis: "y",
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              callbacks: {
                label(context: any) {
                  return `Gewinn: ${MONEY_FORMATTER.format(Number(context.parsed?.x || 0))}`;
                },
              },
            },
          },
          scales: {
            x: {
              grid: { color: readCssVariable("--th-chart-grid", "rgba(73, 90, 118, 0.15)") },
              ticks: {
                color: readCssVariable("--th-chart-label", "#74839c"),
                callback(value: string | number) {
                  return Number(value).toLocaleString("de-DE", { style: "currency", currency: "EUR" });
                },
              },
            },
            y: {
              grid: { display: false },
              ticks: { color: readCssVariable("--th-chart-label", "#74839c"), font: { size: 11 } },
            },
          },
        },
      } as unknown as ChartConfiguration;
    },
    [themeToken, topProfitProducts],
  );

  const topRoasProducts = productsWithAds
    .filter((product) => Number(product.revenue_total_cents || 0) > 0)
    .map((product) => ({
      label: String(product.product_label || "-"),
      roas: Number(product.revenue_total_cents || 0) / Number(product.ads_cost_cents || 1),
    }))
    .sort((left, right) => right.roas - left.roas)
    .slice(0, 12);

  useChart(
    "googleAdsRoasChart",
    () => {
      if (!topRoasProducts.length) {
        return null;
      }

      const roasData = topRoasProducts.map((product) => Math.round(product.roas * 100) / 100);
      const okColor = readCssVariable("--th-ok", "#2b9b68");
      const warnColor = readCssVariable("--th-warn", "#d88b25");
      return {
        type: "bar",
        data: {
          labels: topRoasProducts.map((product) => truncateLabel(product.label, 28)),
          datasets: [{
            label: "ROAS",
            data: roasData,
            backgroundColor: roasData.map((value) => value >= 1 ? `${okColor}33` : `${warnColor}33`),
            borderColor: roasData.map((value) => value >= 1 ? okColor : warnColor),
            borderWidth: 1.5,
            borderRadius: 4,
            borderSkipped: false,
          }],
        },
        options: {
          indexAxis: "y",
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              callbacks: {
                label(context: any) {
                  return `ROAS: ${Number(context.parsed?.x || 0).toFixed(2)}x`;
                },
              },
            },
          },
          scales: {
            x: {
              grid: { color: readCssVariable("--th-chart-grid", "rgba(73, 90, 118, 0.15)") },
              ticks: {
                color: readCssVariable("--th-chart-label", "#74839c"),
                callback(value: string | number) {
                  return `${Number(value).toFixed(1)}x`;
                },
              },
            },
            y: {
              grid: { display: false },
              ticks: { color: readCssVariable("--th-chart-label", "#74839c"), font: { size: 11 } },
            },
          },
        },
      } as unknown as ChartConfiguration;
    },
    [themeToken, topRoasProducts],
  );

  async function reloadAnalytics() {
    setLoading(true);
    try {
      const nextPayload = await fetchGoogleAdsAnalytics(query);
      setPayload(nextPayload);
      setError("");
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Google Ads Daten konnten nicht geladen werden.");
      setPayload(null);
    } finally {
      setLoading(false);
    }
  }

  async function handleUpload() {
    const reportInput = document.getElementById("googleAdsReportInput");
    const assignmentInput = document.getElementById("googleAdsAssignmentInput");
    const reportFile = reportInput instanceof HTMLInputElement ? reportInput.files?.[0] : null;
    const assignmentFile = assignmentInput instanceof HTMLInputElement ? assignmentInput.files?.[0] : null;

    if (!reportFile && !assignmentFile) {
      setError("Bitte mindestens Report oder Zuweisungs-CSV auswaehlen.");
      return;
    }

    const formData = new FormData();
    if (reportFile) {
      formData.append("report_file", reportFile);
    }
    if (assignmentFile) {
      formData.append("assignment_file", assignmentFile);
    }

    setUploading(true);
    try {
      await uploadGoogleAdsFiles(formData);
      if (reportInput instanceof HTMLInputElement) {
        reportInput.value = "";
      }
      if (assignmentInput instanceof HTMLInputElement) {
        assignmentInput.value = "";
      }
      setReportFileName("Keine Datei");
      setAssignmentFileName("Keine Datei");
      await reloadAnalytics();
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Google Ads Import fehlgeschlagen.");
    } finally {
      setUploading(false);
    }
  }

  async function handleReset() {
    const confirmed = window.confirm("Alle Google Ads Daten (Kosten + Zuordnungen) unwiderruflich loeschen?");
    if (!confirmed) {
      return;
    }

    setResetting(true);
    try {
      await resetGoogleAds();
      setExpandedProductKey("");
      setProductDetail(null);
      await reloadAnalytics();
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Google Ads Reset fehlgeschlagen.");
    } finally {
      setResetting(false);
    }
  }

  return (
    <div id="googleAdsPanel" className="tab-panel active" data-react-google-ads-mounted="true">
      <div id="googleAdsReactRoot">
        {error ? (
          <section className="card" style={{ marginBottom: 12, padding: 16 }}>
            <div className="table-meta" style={{ color: "var(--danger, #c44)" }}>
              Google Ads Daten konnten nicht geladen werden: {error}
            </div>
          </section>
        ) : null}

        <section className="card table-card">
          <div className="table-head">
            <h2 className="table-title">Google Ads CSV Import</h2>
            <div className="table-meta" style={{ minWidth: 96, textAlign: "right" }}>{hasData ? "Aktiv" : "Keine Daten"}</div>
          </div>
          <div className="google-ads-import-shell">
            <div className="google-ads-import-row">
              <div className="google-ads-file-field">
                <div className="google-ads-field-label">Report CSV</div>
                <label className="google-ads-file-btn" htmlFor="googleAdsReportInput">Datei waehlen</label>
                <input
                  id="googleAdsReportInput"
                  className="google-ads-file-input"
                  type="file"
                  accept=".csv,text/csv"
                  onChange={(event) => {
                    const file = event.target.files?.[0];
                    setReportFileName(file ? file.name : "Keine Datei");
                  }}
                />
                <div id="googleAdsReportFileLabel" className="google-ads-file-name">{reportFileName}</div>
              </div>
              <div className="google-ads-file-field">
                <div className="google-ads-field-label">Zuweisungs CSV <span className="google-ads-optional">(optional)</span></div>
                <label className="google-ads-file-btn" htmlFor="googleAdsAssignmentInput">Datei waehlen</label>
                <input
                  id="googleAdsAssignmentInput"
                  className="google-ads-file-input"
                  type="file"
                  accept=".csv,text/csv"
                  onChange={(event) => {
                    const file = event.target.files?.[0];
                    setAssignmentFileName(file ? file.name : "Keine Datei");
                  }}
                />
                <div id="googleAdsAssignmentFileLabel" className="google-ads-file-name">{assignmentFileName}</div>
              </div>
              <div className="google-ads-action-field">
                <button id="googleAdsUploadBtn" className="btn-inline primary google-ads-upload-btn" type="button" disabled={isUploading} onClick={() => { void handleUpload(); }}>
                  {isUploading ? "Importiere..." : "Importieren"}
                </button>
                <button id="googleAdsResetBtn" className="btn-inline danger google-ads-upload-btn" type="button" disabled={isResetting} onClick={() => { void handleReset(); }}>
                  {isResetting ? "Zuruecksetzen..." : "Zuruecksetzen"}
                </button>
              </div>
            </div>
            <div className="google-ads-status-info">
              {[
                `Report: ${String(reportImport.filename || "-") || "-"}`,
                `Zeitraum: ${reportRangeLabel}`,
                `Letztes Datum: ${reportLastLabel}`,
                `Zeilen: ${NUMBER_FORMATTER.format(Number(reportMeta.rows || 0))} (mit Kosten: ${NUMBER_FORMATTER.format(Number(reportMeta.non_zero_rows || 0))})`,
                `Import: ${formatDateTime(reportImport.imported_at)}`,
                `Zuweisung: ${String(assignmentImport.filename || "-") || "-"} (${NUMBER_FORMATTER.format(Number(assignmentMeta.rows || 0))} Zeilen, ${formatDateTime(assignmentImport.imported_at)})`,
              ].join(" · ")}
            </div>
          </div>
        </section>

        <section className="kpi-grid kpi-grid-6" style={{ marginTop: 12 }}>
          <article className="card kpi">
            <div className="kpi-name">Ads Kosten gesamt</div>
            <div className="kpi-value">{formatMoneyFromCents(totalAdsCost)}</div>
            <div className="kpi-sub">{`Gemappt ${formatMoneyFromCents(Number(kpis.ads_cost_mapped_cents || 0))} | Unmapped ${formatMoneyFromCents(Number(kpis.ads_cost_unmapped_cents || 0))}`}</div>
          </article>
          <article className="card kpi">
            <div className="kpi-name">Shopify Umsatz</div>
            <div className="kpi-value">{formatMoneyFromCents(totalRevenue)}</div>
            <div className="kpi-sub">Zugeordnete Produkte</div>
          </article>
          <article className="card kpi">
            <div className={profitAfter < 0 ? "kpi-name value-neg" : "kpi-name value-pos"}>Gewinn nach Ads</div>
            <div className={profitAfter < 0 ? "kpi-value value-neg" : "kpi-value value-pos"}>{formatMoneyFromCents(profitAfter)}</div>
            <div className="kpi-sub">{`Vor Ads: ${formatMoneyFromCents(profitBefore)}`}</div>
          </article>
          <article className="card kpi">
            <div className="kpi-name">ROAS</div>
            <div className="kpi-value">{`${roas.toFixed(2)}x`}</div>
            <div className="kpi-sub">{`Fehlende Zuweisungen: ${NUMBER_FORMATTER.format(missingCount)}`}</div>
          </article>
          <article className="card kpi">
            <div className="kpi-name">Kosten pro Order</div>
            <div className="kpi-value">{totalOrders > 0 && totalAdsCost > 0 ? formatMoneyFromCents(Math.round(totalAdsCost / totalOrders)) : "-"}</div>
            <div className="kpi-sub">{`${NUMBER_FORMATTER.format(totalOrders)} Orders`}</div>
          </article>
          <article className="card kpi">
            <div className="kpi-name">Ads-Anteil am Umsatz</div>
            <div className={(totalRevenue > 0 && totalAdsCost > 0 && (totalAdsCost / totalRevenue) * 100 > 30) ? "kpi-value value-neg" : "kpi-value value-pos"}>
              {totalRevenue > 0 && totalAdsCost > 0 ? `${((totalAdsCost / totalRevenue) * 100).toFixed(1)} %` : "-"}
            </div>
            <div className="kpi-sub">Ads Kosten / Umsatz</div>
          </article>
        </section>

        <section className="card table-card" style={{ marginTop: 12 }}>
          <div className="table-head">
            <h2 className="table-title">Ads Trend</h2>
            <div className="table-meta">{trendRows.length ? `Gesamt vs. gemappt · ${NUMBER_FORMATTER.format(trendRows.length)} Tage` : "Keine Trenddaten"}</div>
          </div>
          <div className="canvas-wrap">
            <canvas id="googleAdsTrendChart" />
          </div>
        </section>

        <section className="card table-card" style={{ marginTop: 12 }}>
          <div className="table-head">
            <h2 className="table-title">Kumulative Profitabilitaet</h2>
            <div className="table-meta">{trendRows.length ? "Kumulierte Ads Kosten vs. Gewinn" : "Keine Daten"}</div>
          </div>
          <div className="canvas-wrap">
            <canvas id="googleAdsCumulChart" />
          </div>
        </section>

        <section className="google-ads-chart-pair" style={{ marginTop: 12 }}>
          <article className="card table-card">
            <div className="table-head">
              <h2 className="table-title">Top Gewinn nach Ads</h2>
              <div className="table-meta">{topProfitProducts.length ? `Top ${topProfitProducts.length} Produkte` : "Keine Produkte mit Ads-Kosten"}</div>
            </div>
            <div className="canvas-wrap canvas-wrap-bar">
              <canvas id="googleAdsProfitChart" />
            </div>
          </article>
          <article className="card table-card">
            <div className="table-head">
              <h2 className="table-title">Top ROAS</h2>
              <div className="table-meta">{topRoasProducts.length ? `Top ${topRoasProducts.length} Produkte` : "Keine Produkte mit Umsatz und Ads-Kosten"}</div>
            </div>
            <div className="canvas-wrap canvas-wrap-bar">
              <canvas id="googleAdsRoasChart" />
            </div>
          </article>
        </section>

        <section className="card table-card" style={{ marginTop: 12 }}>
          <div className="table-head">
            <h2 className="table-title">Ads Kosten pro Produkt</h2>
            <div className="table-meta">{`${NUMBER_FORMATTER.format(products.length)} Zeilen`}</div>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Produkt</th>
                  <th>Mapping</th>
                  <th>Ads Kosten</th>
                  <th>Orders</th>
                  <th>Umsatz</th>
                  <th>ROAS</th>
                  <th>Gewinn vor Ads</th>
                  <th>Gewinn nach Ads</th>
                </tr>
              </thead>
              <tbody>
                {products.length ? products.map((item, index) => {
                  const productKey = String(item.product_key || "");
                  const adsCost = Number(item.ads_cost_cents || 0);
                  const revenue = Number(item.revenue_total_cents || 0);
                  const productRoas = adsCost > 0 ? revenue / adsCost : 0;
                  const profit = Number(item.profit_after_ads_cents || 0);
                  const isExpanded = expandedProductKey === productKey;
                  return (
                    <>
                      <tr
                        key={`${productKey || "product"}:${index}`}
                        className={`ga-product-row${isExpanded ? " ga-row-active" : ""}`}
                        data-product-key={productKey}
                        style={{ cursor: productKey ? "pointer" : undefined }}
                        onClick={() => {
                          if (!productKey) {
                            return;
                          }
                          if (expandedProductKey === productKey) {
                            setExpandedProductKey("");
                            setProductDetail(null);
                            return;
                          }
                          setExpandedProductKey(productKey);
                        }}
                      >
                        <td title={String(item.product_detail || "-")}>{String(item.product_label || "-")}</td>
                        <td><span className={item.mapped ? "badge badge-invoice" : "badge badge-refund"}>{item.mapped ? "Gemappt" : "Unmapped"}</span></td>
                        <td>{formatMoneyFromCents(adsCost)}</td>
                        <td>{NUMBER_FORMATTER.format(Number(item.order_count || 0))}</td>
                        <td>{formatMoneyFromCents(revenue)}</td>
                        <td>{adsCost > 0 ? `${productRoas.toFixed(2)}x` : "-"}</td>
                        <td>{formatMoneyFromCents(Number(item.profit_before_ads_cents || 0))}</td>
                        <td className={profit < 0 ? "value-neg" : "value-pos"}>{formatMoneyFromCents(profit)}</td>
                      </tr>
                      {isExpanded ? <ProductDetailRow key={`${productKey}:detail`} detail={productDetail} product={item} colSpan={8} themeToken={themeToken} /> : null}
                    </>
                  );
                }) : <tr><td colSpan={8}>{isLoading ? "Lade Google Ads Daten..." : "Keine Daten fuer den aktuellen Filter."}</td></tr>}
              </tbody>
            </table>
          </div>
        </section>

        <section className="card table-card" style={{ marginTop: 12 }}>
          <div className="table-head">
            <h2 className="table-title">Fehlende Zuweisungen</h2>
            <div className="table-meta">{`${NUMBER_FORMATTER.format(missingAssignments.length)} Zeilen`}</div>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Artikel-ID</th>
                  <th>Ads Kosten</th>
                  <th>Tage</th>
                </tr>
              </thead>
              <tbody>
                {missingAssignments.length ? missingAssignments.map((item, index) => (
                  <tr key={`${String(item.article_id || "missing")}:${index}`}>
                    <td>{String(item.article_id || "-")}</td>
                    <td>{formatMoneyFromCents(Number(item.ads_cost_cents || 0))}</td>
                    <td>{NUMBER_FORMATTER.format(Number(item.day_count || 0))}</td>
                  </tr>
                )) : <tr><td colSpan={3}>Keine fehlenden Zuweisungen.</td></tr>}
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </div>
  );
}
