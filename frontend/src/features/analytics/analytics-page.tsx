import {
  useDashboardShellState,
  type DatePreset,
  type ShellFilters,
} from "@/app/dashboard-shell-state";
import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type DragEvent,
  type ReactNode,
} from "react";
import {
  ArcElement,
  CategoryScale,
  Chart,
  DoughnutController,
  Filler,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
  type ChartConfiguration,
} from "chart.js";

import { fetchAnalytics } from "@/features/analytics/api";
import {
  NUMBER_FORMATTER,
  formatDateToken,
  formatMoneyFromCents,
  formatPercent,
  isoWeekNumber,
  normalizeTrendGranularity,
  parseDateToken,
  trendGranularityLabel,
} from "@/features/analytics/format";
import type { AnalyticsPayload, AnalyticsQuery, AnalyticsTrendPoint } from "@/features/analytics/types";
import { useTheme } from "@/shared/theme/theme-provider";

Chart.register(
  ArcElement,
  CategoryScale,
  DoughnutController,
  Filler,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
);

type AnalyticsFilters = {
  datePreset: DatePreset;
  from: string;
  to: string;
  marketplace: string;
  q: string;
  trendGranularity: string;
};

const SECTION_CONFIG = {
  "sec-kpi": { group: "kpi", className: "kpi-grid" },
  "sec-insights-a": { group: "insights-a", className: "analytics-insights-grid" },
  "sec-insights-b": { group: "insights-b", className: "analytics-insights-grid" },
  "sec-donuts": { group: "donuts", className: "donut-grid" },
  "sec-heatmap": { group: "heatmap", className: "heatmap-section" },
  "sec-charts": { group: "charts", className: "charts-grid" },
} as const;

const DEFAULT_CARD_ORDER = {
  kpi: ["orders", "revenue", "after-fees", "purchase", "profit", "channels"],
  "insights-a": ["snapshot", "channel-compare"],
  "insights-b": ["fulfillment", "payments"],
  donuts: ["donut-profit", "donut-revenue"],
  heatmap: ["heatmap"],
  charts: ["trend", "top-articles"],
} as const;

type SectionId = keyof typeof SECTION_CONFIG;
type DragGroup = keyof typeof DEFAULT_CARD_ORDER;

type LayoutState = {
  sections: SectionId[];
  cards: { [K in DragGroup]: string[] };
};

type DragState = {
  group: DragGroup;
  cardId: string;
} | null;

type CardDragProps = {
  className?: string;
  draggable: boolean;
  onDragStart: (event: DragEvent<HTMLElement>) => void;
  onDragOver: (event: DragEvent<HTMLElement>) => void;
  onDrop: (event: DragEvent<HTMLElement>) => void;
  onDragEnd: () => void;
};

type TrendDelta = {
  className: string;
  arrow: string;
  label: string;
} | null;

const LAYOUT_STORAGE_KEY = "dash-combined.analytics-layout";

function classNames(...parts: Array<string | false | null | undefined>) {
  return parts.filter(Boolean).join(" ");
}

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

function normalizeFilters(input: ShellFilters, trendGranularity: string): AnalyticsFilters {
  const from = String(input.from || "").trim();
  const to = String(input.to || "").trim() || from;
  return {
    datePreset: input.datePreset as DatePreset,
    from,
    to,
    marketplace: String(input.marketplace || "").trim().toLowerCase(),
    q: String(input.q || "").trim(),
    trendGranularity: normalizeTrendGranularity(trendGranularity),
  };
}

function createDefaultLayout(): LayoutState {
  return {
    sections: Object.keys(SECTION_CONFIG) as SectionId[],
    cards: {
      kpi: [...DEFAULT_CARD_ORDER.kpi],
      "insights-a": [...DEFAULT_CARD_ORDER["insights-a"]],
      "insights-b": [...DEFAULT_CARD_ORDER["insights-b"]],
      donuts: [...DEFAULT_CARD_ORDER.donuts],
      heatmap: [...DEFAULT_CARD_ORDER.heatmap],
      charts: [...DEFAULT_CARD_ORDER.charts],
    },
  };
}

function normalizeOrderedIds(defaultIds: readonly string[], rawIds: unknown) {
  if (!Array.isArray(rawIds)) {
    return [...defaultIds];
  }

  const seen = new Set<string>();
  const ordered = rawIds
    .map((value) => String(value || "").trim())
    .filter((value) => defaultIds.includes(value) && !seen.has(value))
    .map((value) => {
      seen.add(value);
      return value;
    });

  for (const defaultId of defaultIds) {
    if (!seen.has(defaultId)) {
      ordered.push(defaultId);
    }
  }

  return ordered;
}

function readStoredLayout(): LayoutState {
  const fallback = createDefaultLayout();

  try {
    const rawValue = localStorage.getItem(LAYOUT_STORAGE_KEY);
    if (!rawValue) {
      return fallback;
    }

    const parsed = JSON.parse(rawValue) as
      | {
          sections?: unknown;
          cards?: Record<string, unknown>;
        }
      | Record<string, unknown>
      | null;

    if (!parsed || typeof parsed !== "object") {
      return fallback;
    }

    const rawSections = "sections" in parsed ? parsed.sections : undefined;
    const rawCards: Record<string, unknown> = "cards" in parsed && parsed.cards && typeof parsed.cards === "object"
      ? parsed.cards as Record<string, unknown>
      : parsed as Record<string, unknown>;

    return {
      sections: normalizeOrderedIds(fallback.sections, rawSections) as SectionId[],
      cards: {
        kpi: normalizeOrderedIds(DEFAULT_CARD_ORDER.kpi, rawCards.kpi),
        "insights-a": normalizeOrderedIds(DEFAULT_CARD_ORDER["insights-a"], rawCards["insights-a"]),
        "insights-b": normalizeOrderedIds(DEFAULT_CARD_ORDER["insights-b"], rawCards["insights-b"]),
        donuts: normalizeOrderedIds(DEFAULT_CARD_ORDER.donuts, rawCards.donuts),
        heatmap: normalizeOrderedIds(DEFAULT_CARD_ORDER.heatmap, rawCards.heatmap),
        charts: normalizeOrderedIds(DEFAULT_CARD_ORDER.charts, rawCards.charts),
      },
    };
  } catch (_error) {
    return fallback;
  }
}

function moveSection(layout: LayoutState, sectionId: SectionId, delta: number): LayoutState {
  const sections = [...layout.sections];
  const currentIndex = sections.indexOf(sectionId);
  const nextIndex = currentIndex + delta;

  if (currentIndex < 0 || nextIndex < 0 || nextIndex >= sections.length) {
    return layout;
  }

  sections.splice(currentIndex, 1);
  sections.splice(nextIndex, 0, sectionId);
  return { ...layout, sections };
}

function moveCardWithinGroup(layout: LayoutState, group: DragGroup, sourceCardId: string, targetCardId: string): LayoutState {
  const cards = [...layout.cards[group]];
  const sourceIndex = cards.indexOf(sourceCardId);
  const targetIndex = cards.indexOf(targetCardId);

  if (sourceIndex < 0 || targetIndex < 0 || sourceIndex === targetIndex) {
    return layout;
  }

  cards.splice(sourceIndex, 1);
  const insertIndex = sourceIndex < targetIndex ? targetIndex : targetIndex;
  cards.splice(insertIndex, 0, sourceCardId);

  return {
    ...layout,
    cards: {
      ...layout.cards,
      [group]: cards,
    },
  };
}

function renderTrendPointLabel(point: AnalyticsTrendPoint, granularity: string) {
  const parsed = parseDateToken(point.bucket_start);
  if (!parsed) {
    return point.bucket_start || "-";
  }
  if (granularity === "day") {
    return parsed.toLocaleDateString("de-DE", { day: "2-digit", month: "2-digit" });
  }
  if (granularity === "week") {
    return `KW ${String(isoWeekNumber(parsed)).padStart(2, "0")}`;
  }
  return parsed.toLocaleDateString("de-DE", { month: "short", year: "2-digit" });
}

function resolveTrendDelta(current: number, previous: number | null | undefined): TrendDelta {
  if (previous == null || previous === 0) {
    return null;
  }

  const change = ((current - previous) / Math.abs(previous)) * 100;
  const rounded = Math.abs(change) >= 10 ? Math.round(change) : Number(change.toFixed(1));

  return {
    className: change > 0 ? "trend-up" : change < 0 ? "trend-down" : "trend-flat",
    arrow: change > 0 ? "▲" : change < 0 ? "▼" : "",
    label: `${change > 0 ? "+" : ""}${rounded}%`,
  };
}

function StateCard({ title, message }: { title: string; message: string }) {
  return (
    <section className="analytics-react-state">
      <article className="card analytics-react-message-card">
        <h2 className="chart-title">{title}</h2>
        <p className="chart-sub analytics-react-message">{message}</p>
      </article>
    </section>
  );
}

function normalizeAnalyticsPayload(payload: AnalyticsPayload): AnalyticsPayload {
  const statusSummary = payload.status_summary && typeof payload.status_summary === "object"
    ? payload.status_summary
    : {
        completed_like_count: 0,
        pending_like_count: 0,
        return_like_count: 0,
        other_count: 0,
      };
  const trend = payload.trend && typeof payload.trend === "object"
    ? payload.trend
    : null;

  return {
    ...payload,
    order_count: Number(payload.order_count || 0),
    revenue_total_cents: Number(payload.revenue_total_cents || 0),
    fees_total_cents: Number(payload.fees_total_cents || 0),
    after_fees_total_cents: Number(payload.after_fees_total_cents || 0),
    purchase_total_cents: Number(payload.purchase_total_cents || 0),
    profit_total_cents: Number(payload.profit_total_cents || 0),
    margin_pct: Number(payload.margin_pct || 0),
    aov_cents: Number(payload.aov_cents || 0),
    avg_profit_per_order_cents: Number(payload.avg_profit_per_order_cents || 0),
    fees_ratio_pct: Number(payload.fees_ratio_pct || 0),
    shipping_total_cents: Number(payload.shipping_total_cents || 0),
    orders_with_purchase_count: Number(payload.orders_with_purchase_count || 0),
    purchase_missing_count: Number(payload.purchase_missing_count || 0),
    purchase_coverage_pct: Number(payload.purchase_coverage_pct || 0),
    returns_order_count: Number(payload.returns_order_count || 0),
    return_rate_pct: Number(payload.return_rate_pct || 0),
    unique_customers: Number(payload.unique_customers || 0),
    repeat_customers: Number(payload.repeat_customers || 0),
    repeat_customer_rate_pct: Number(payload.repeat_customer_rate_pct || 0),
    shopify_revenue_total_cents: Number(payload.shopify_revenue_total_cents || 0),
    kaufland_revenue_total_cents: Number(payload.kaufland_revenue_total_cents || 0),
    marketplaces: Array.isArray(payload.marketplaces) ? payload.marketplaces : [],
    top_payment_methods: Array.isArray(payload.top_payment_methods) ? payload.top_payment_methods : [],
    monthly: Array.isArray(payload.monthly) ? payload.monthly : [],
    top_articles: Array.isArray(payload.top_articles) ? payload.top_articles : [],
    purchase_heatmap: Array.isArray(payload.purchase_heatmap) ? payload.purchase_heatmap : [],
    previous_period: payload.previous_period ?? null,
    status_summary: {
      completed_like_count: Number(statusSummary.completed_like_count || 0),
      pending_like_count: Number(statusSummary.pending_like_count || 0),
      return_like_count: Number(statusSummary.return_like_count || 0),
      other_count: Number(statusSummary.other_count || 0),
    },
    trend: {
      granularity: String(trend?.granularity || ""),
      title: String(trend?.title || ""),
      from: String(trend?.from || ""),
      to: String(trend?.to || ""),
      point_count: Number(trend?.point_count || 0),
      order_count: Number(trend?.order_count || 0),
      revenue_total_cents: Number(trend?.revenue_total_cents || 0),
      profit_total_cents: Number(trend?.profit_total_cents || 0),
      points: Array.isArray(trend?.points) ? trend.points : [],
    },
  };
}

function KpiCard({
  cardId,
  name,
  valueId,
  value,
  tooltip,
  trendId,
  trend,
  subId,
  subText,
  valueClassName,
  dragProps,
}: {
  cardId: string;
  name: string;
  valueId: string;
  value: string;
  tooltip?: string;
  trendId?: string;
  trend?: TrendDelta;
  subId?: string;
  subText?: string;
  valueClassName?: string;
  dragProps: CardDragProps;
}) {
  return (
    <article
      data-card-id={cardId}
      className={classNames("card", "kpi", dragProps.className)}
      draggable={dragProps.draggable}
      onDragStart={dragProps.onDragStart}
      onDragOver={dragProps.onDragOver}
      onDrop={dragProps.onDrop}
      onDragEnd={dragProps.onDragEnd}
    >
      <div className="kpi-name" data-tooltip={tooltip}>{name}</div>
      <div id={valueId} className={classNames("kpi-value", valueClassName)}>{value}</div>
      {trendId ? (
        <div id={trendId} className={classNames("kpi-trend", trend?.className)}>
          {trend ? `${trend.arrow} ${trend.label}` : ""}
        </div>
      ) : null}
      {subId ? <div id={subId} className="kpi-sub">{subText}</div> : null}
    </article>
  );
}

function TrendCard({
  payload,
  trendGranularity,
  themeToken,
  onTrendGranularityChange,
  dragProps,
}: {
  payload: AnalyticsPayload;
  trendGranularity: string;
  themeToken: string;
  onTrendGranularityChange: (next: string) => void;
  dragProps: CardDragProps;
}) {
  const trend = payload.trend ?? null;
  const points = Array.isArray(trend?.points) && trend.points.length
    ? trend.points
    : payload.monthly.map((row) => ({
        bucket_start: `${row.month}-01`,
        bucket_end: `${row.month}-01`,
        order_count: row.order_count,
        revenue_total_cents: row.revenue_total_cents,
        fees_total_cents: row.fees_total_cents,
        after_fees_total_cents: row.after_fees_total_cents,
        purchase_total_cents: row.purchase_total_cents,
        profit_total_cents: row.profit_total_cents,
      }));

  const resolvedGranularity = normalizeTrendGranularity(trend?.granularity || trendGranularity || "auto") === "auto"
    ? (points.length && payload.monthly.length === points.length ? "month" : "day")
    : normalizeTrendGranularity(trend?.granularity || trendGranularity || "auto");

  useChart(
    "trendChart",
    () => {
      const pointRadius = points.length > 120 ? 0 : points.length > 64 ? 1 : 2;
      return {
        type: "line",
        data: {
          labels: points.map((point) => renderTrendPointLabel(point, resolvedGranularity)),
          datasets: [
            {
              label: "Umsatz",
              data: points.map((point) => Number(point.revenue_total_cents || 0) / 100),
              borderColor: readCssVariable("--th-chart-1", "#1f73e0"),
              backgroundColor: readCssVariable("--th-chart-1-fill", "rgba(31, 115, 224, 0.12)"),
              pointRadius,
              pointHoverRadius: 4,
              pointHitRadius: 10,
              borderWidth: 2.1,
              tension: 0.33,
              fill: false,
            },
            {
              label: "Gewinn",
              data: points.map((point) => Number(point.profit_total_cents || 0) / 100),
              borderColor: readCssVariable("--th-chart-2", "#0f8a7a"),
              backgroundColor: readCssVariable("--th-chart-2-fill", "rgba(15, 138, 122, 0.14)"),
              pointRadius,
              pointHoverRadius: 4,
              pointHitRadius: 10,
              borderWidth: 2,
              tension: 0.33,
              fill: false,
            },
          ],
        },
        options: {
          maintainAspectRatio: false,
          interaction: {
            mode: "index",
            intersect: false,
          },
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
                font: {
                  size: 11,
                  weight: 600,
                },
              },
            },
            tooltip: {
              callbacks: {
                label(context: any) {
                  return `${String(context.dataset?.label || "Wert")}: ${formatMoneyFromCents(Number(context.parsed?.y || 0) * 100)}`;
                },
              },
            },
          },
          scales: {
            x: {
              grid: {
                display: false,
              },
              ticks: {
                color: readCssVariable("--th-chart-label", "#74839c"),
                maxRotation: 0,
                autoSkip: true,
                maxTicksLimit: resolvedGranularity === "day" ? 14 : 12,
              },
            },
            y: {
              grid: {
                color: readCssVariable("--th-chart-grid", "rgba(73, 90, 118, 0.15)"),
              },
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
    [payload, resolvedGranularity, themeToken],
  );

  const fromToken = String(trend?.from || "").trim();
  const toToken = String(trend?.to || "").trim();
  const rangeText = fromToken && toToken
    ? `${formatDateToken(fromToken)} - ${formatDateToken(toToken)}`
    : "Aktueller Filter";

  return (
    <article
      data-card-id="trend"
      className={classNames("card", "chart-card", dragProps.className)}
      draggable={dragProps.draggable}
      onDragStart={dragProps.onDragStart}
      onDragOver={dragProps.onDragOver}
      onDrop={dragProps.onDrop}
      onDragEnd={dragProps.onDragEnd}
    >
      <div className="chart-head-row">
        <div>
          <h2 id="trendChartTitle" className="chart-title">{trend?.title || "Verlauf"}</h2>
          <p id="trendChartSub" className="chart-sub">
            {`Umsatz und Gewinn · ${trendGranularity === "auto" ? `Auto (${trendGranularityLabel(resolvedGranularity)})` : trendGranularityLabel(resolvedGranularity)} · ${rangeText} · ${NUMBER_FORMATTER.format(points.length)} Punkte`}
          </p>
        </div>
        <div id="trendGranularityGroup" className="trend-granularity" role="tablist" aria-label="Trend Aufloesung">
          {["auto", "day", "week", "month"].map((token) => (
            <button
              key={token}
              id={`trendGranularity${token.charAt(0).toUpperCase()}${token.slice(1)}Btn`}
              className={classNames("trend-granularity-btn", trendGranularity === token && "active")}
              type="button"
              data-trend-granularity={token}
              onClick={() => onTrendGranularityChange(token)}
            >
              {token === "auto" ? "Auto" : trendGranularityLabel(token)}
            </button>
          ))}
        </div>
      </div>
      <div className="canvas-wrap">
        <canvas id="trendChart" />
      </div>
    </article>
  );
}

function DonutMarketplaceCard({
  payload,
  themeToken,
  dragProps,
}: {
  payload: AnalyticsPayload;
  themeToken: string;
  dragProps: CardDragProps;
}) {
  const marketplaces = payload.marketplaces || [];
  const shopifyProfit = Number(marketplaces.find((item) => item.marketplace === "shopify")?.profit_total_cents || 0) / 100;
  const kauflandProfit = Number(marketplaces.find((item) => item.marketplace === "kaufland")?.profit_total_cents || 0) / 100;
  const total = shopifyProfit + kauflandProfit;
  const visualTotal = Math.max(0, shopifyProfit) + Math.max(0, kauflandProfit);
  const hasData = shopifyProfit > 0 || kauflandProfit > 0;

  useChart(
    "donutMarketplace",
    () => ({
      type: "doughnut",
      data: {
        labels: hasData ? ["Shopify", "Kaufland"] : ["Keine Daten"],
        datasets: [
          {
            data: hasData ? [Math.max(0, shopifyProfit), Math.max(0, kauflandProfit)] : [1],
            backgroundColor: hasData
              ? [readCssVariable("--th-donut-shopify", "#50b468"), readCssVariable("--th-donut-kaufland", "#d85048")]
              : [readCssVariable("--th-line", "#ccc")],
            borderWidth: 0,
            hoverOffset: 6,
          },
        ],
      },
      options: {
        cutout: "64%",
        maintainAspectRatio: false,
        plugins: {
          legend: {
            display: true,
            position: "bottom",
            labels: {
              usePointStyle: true,
              pointStyle: "circle",
              boxWidth: 10,
              color: readCssVariable("--th-chart-label", "#74839c"),
              font: { size: 11, weight: 600 },
              padding: 14,
            },
          },
          tooltip: {
            callbacks: {
              label(context: any) {
                const value = Number(context.parsed || 0);
                const pct = visualTotal > 0 ? ((value / visualTotal) * 100).toFixed(1) : "0.0";
                return `${String(context.label || "")}: ${value.toLocaleString("de-DE", { style: "currency", currency: "EUR" })} (${pct}%)`;
              },
            },
          },
        },
      },
    }) as unknown as ChartConfiguration,
    [payload, themeToken],
  );

  return (
    <article
      data-card-id="donut-profit"
      className={classNames("card", "donut-card", dragProps.className)}
      draggable={dragProps.draggable}
      onDragStart={dragProps.onDragStart}
      onDragOver={dragProps.onDragOver}
      onDrop={dragProps.onDrop}
      onDragEnd={dragProps.onDragEnd}
    >
      <h2 className="chart-title" data-tooltip="Verteilung des Gewinns auf Shopify und Kaufland.">Gewinn nach Marktplatz</h2>
      <div className="donut-canvas-wrap">
        <canvas id="donutMarketplace" />
        <div className="donut-center-label">
          <div id="donutMarketplaceCenterValue" className="donut-center-value">
            {total.toLocaleString("de-DE", { style: "currency", currency: "EUR" })}
          </div>
          <div className="donut-center-sub">Gesamt</div>
        </div>
      </div>
    </article>
  );
}

function DonutRevenueCard({
  payload,
  themeToken,
  dragProps,
}: {
  payload: AnalyticsPayload;
  themeToken: string;
  dragProps: CardDragProps;
}) {
  const revenue = Number(payload.revenue_total_cents || 0) / 100;
  const afterFees = Number(payload.after_fees_total_cents || 0) / 100;
  const purchase = Number(payload.purchase_total_cents || 0) / 100;
  const profit = Number(payload.profit_total_cents || 0) / 100;
  const fees = revenue - afterFees;
  const hasData = revenue > 0;

  useChart(
    "donutRevenue",
    () => ({
      type: "doughnut",
      data: {
        labels: hasData ? ["Fees", "Einkauf", "Gewinn"] : ["Keine Daten"],
        datasets: [
          {
            data: hasData ? [Math.max(0, fees), Math.max(0, purchase), Math.max(0, profit)] : [1],
            backgroundColor: hasData
              ? [
                  readCssVariable("--th-donut-fees", "#e0a030"),
                  readCssVariable("--th-donut-purchase", "#c05070"),
                  readCssVariable("--th-donut-profit", "#38a868"),
                ]
              : [readCssVariable("--th-line", "#ccc")],
            borderWidth: 0,
            hoverOffset: 6,
          },
        ],
      },
      options: {
        cutout: "64%",
        maintainAspectRatio: false,
        plugins: {
          legend: {
            display: true,
            position: "bottom",
            labels: {
              usePointStyle: true,
              pointStyle: "circle",
              boxWidth: 10,
              color: readCssVariable("--th-chart-label", "#74839c"),
              font: { size: 11, weight: 600 },
              padding: 14,
            },
          },
          tooltip: {
            callbacks: {
              label(context: any) {
                const value = Number(context.parsed || 0);
                const pct = revenue > 0 ? ((value / revenue) * 100).toFixed(1) : "0.0";
                return `${String(context.label || "")}: ${value.toLocaleString("de-DE", { style: "currency", currency: "EUR" })} (${pct}%)`;
              },
            },
          },
        },
      },
    }) as unknown as ChartConfiguration,
    [payload, themeToken],
  );

  return (
    <article
      data-card-id="donut-revenue"
      className={classNames("card", "donut-card", dragProps.className)}
      draggable={dragProps.draggable}
      onDragStart={dragProps.onDragStart}
      onDragOver={dragProps.onDragOver}
      onDrop={dragProps.onDrop}
      onDragEnd={dragProps.onDragEnd}
    >
      <h2 className="chart-title" data-tooltip="Aufschluesselung in Fees, Einkauf und Gewinn.">Umsatz-Aufteilung</h2>
      <div className="donut-canvas-wrap">
        <canvas id="donutRevenue" />
        <div className="donut-center-label">
          <div id="donutRevenueCenterValue" className="donut-center-value">
            {revenue.toLocaleString("de-DE", { style: "currency", currency: "EUR" })}
          </div>
          <div className="donut-center-sub">Umsatz</div>
        </div>
      </div>
    </article>
  );
}

function HeatmapCard({ payload, dragProps }: { payload: AnalyticsPayload; dragProps: CardDragProps }) {
  const grid = Array.isArray(payload.purchase_heatmap) ? payload.purchase_heatmap : [];
  const maxCount = grid.flat().reduce((currentMax, value) => Math.max(currentMax, Number(value || 0)), 0);
  const days = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"];

  return (
    <article
      data-card-id="heatmap"
      className={classNames("card", "heatmap-card", dragProps.className)}
      draggable={dragProps.draggable}
      onDragStart={dragProps.onDragStart}
      onDragOver={dragProps.onDragOver}
      onDrop={dragProps.onDrop}
      onDragEnd={dragProps.onDragEnd}
    >
      <h2 className="chart-title" data-tooltip="Bestellverteilung nach Wochentag und Uhrzeit. Dunklere Felder = mehr Bestellungen.">Kaufzeitpunkt-Analyse</h2>
      <div id="purchaseHeatmap" className="heatmap-grid">
        {grid.length === 7 ? (
          <>
            <div className="heatmap-row heatmap-header">
              <div className="heatmap-day-label" />
              {Array.from({ length: 24 }, (_, hour) => (
                <div key={hour} className="heatmap-hour-label">{hour}</div>
              ))}
            </div>
            {grid.map((row, dayIndex) => (
              <div key={days[dayIndex]} className="heatmap-row">
                <div className="heatmap-day-label">{days[dayIndex]}</div>
                {row.map((count, hourIndex) => {
                  const intensity = maxCount > 0 ? Number(count || 0) / maxCount : 0;
                  const opacity = intensity > 0 ? 0.1 + intensity * 0.9 : 0;
                  return (
                    <div
                      key={`${dayIndex}-${hourIndex}`}
                      className="heatmap-cell"
                      style={{ "--cell-opacity": opacity.toFixed(2) } as CSSProperties}
                      title={`${days[dayIndex]} ${String(hourIndex).padStart(2, "0")}:00 - ${count} ${count === 1 ? "Bestellung" : "Bestellungen"}`}
                    />
                  );
                })}
              </div>
            ))}
          </>
        ) : (
          <div className="heatmap-empty">Keine Daten.</div>
        )}
      </div>
    </article>
  );
}

function TopArticlesCard({ payload, dragProps }: { payload: AnalyticsPayload; dragProps: CardDragProps }) {
  return (
    <article
      data-card-id="top-articles"
      className={classNames("card", "chart-card", dragProps.className)}
      draggable={dragProps.draggable}
      onDragStart={dragProps.onDragStart}
      onDragOver={dragProps.onDragOver}
      onDrop={dragProps.onDrop}
      onDragEnd={dragProps.onDragEnd}
    >
      <h2 className="chart-title" data-tooltip="Nach Umsatz im aktuellen Filter">Top Artikel</h2>
      <div className="table-wrap" style={{ maxHeight: 280, marginTop: 8 }}>
        <table>
          <thead>
            <tr>
              <th>Artikel</th>
              <th>Orders</th>
              <th>Umsatz</th>
              <th>Gewinn</th>
            </tr>
          </thead>
          <tbody id="topArticlesBody">
            {payload.top_articles.length ? payload.top_articles.map((item, index) => (
              <tr key={`${item.article}-${index}`}>
                <td title={item.article || "-"}>{item.article || "-"}</td>
                <td>{NUMBER_FORMATTER.format(item.order_count)}</td>
                <td>{formatMoneyFromCents(item.revenue_total_cents)}</td>
                <td>{formatMoneyFromCents(item.profit_total_cents)}</td>
              </tr>
            )) : (
              <tr><td colSpan={4}>Keine Daten.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </article>
  );
}

type AnalyticsPageProps = {
  isActive: boolean;
};

export function AnalyticsPage({ isActive }: AnalyticsPageProps) {
  const { filters: shellFilters, requestCloseSettingsPanel, refreshRequestToken } = useDashboardShellState();
  const [trendGranularity, setTrendGranularity] = useState("auto");
  const [payload, setPayload] = useState<AnalyticsPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [editing, setEditing] = useState(false);
  const [layout, setLayout] = useState<LayoutState>(() => readStoredLayout());
  const [dragState, setDragState] = useState<DragState>(null);
  const [dragOverCard, setDragOverCard] = useState<string>("");
  const lastRefreshRequestTokenRef = useRef(refreshRequestToken);

  const themeToken = useThemeRenderToken();
  const filters = useMemo<AnalyticsFilters>(() => {
    return normalizeFilters(shellFilters, trendGranularity);
  }, [shellFilters, trendGranularity]);

  const query = useMemo<AnalyticsQuery>(() => ({
    from: filters.from,
    to: filters.to,
    marketplace: filters.marketplace,
    q: filters.q,
    trendGranularity: filters.trendGranularity,
  }), [filters]);

  useEffect(() => {
    document.title = "Combined Dropshipping Dashboard";
  }, []);

  useEffect(() => {
    try {
      localStorage.setItem(LAYOUT_STORAGE_KEY, JSON.stringify(layout));
    } catch (_error) {
      // Ignore storage write failures.
    }
  }, [layout]);

  useEffect(() => {
    if (!isActive) {
      return;
    }
    let cancelled = false;
    setLoading(true);
    setError("");

    fetchAnalytics(query)
      .then((nextPayload) => {
        if (!cancelled) {
          setPayload(normalizeAnalyticsPayload(nextPayload));
        }
      })
      .catch((nextError: Error) => {
        if (!cancelled) {
          setError(nextError.message);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [isActive, query]);

  useEffect(() => {
    if (refreshRequestToken === 0 || lastRefreshRequestTokenRef.current === refreshRequestToken) {
      return;
    }
    if (!isActive) {
      return;
    }
    lastRefreshRequestTokenRef.current = refreshRequestToken;

    setLoading(true);
    setError("");
    void fetchAnalytics(query)
      .then((nextPayload) => {
        setPayload(normalizeAnalyticsPayload(nextPayload));
      })
      .catch((nextError: Error) => {
        setError(nextError.message);
      })
      .finally(() => {
        setLoading(false);
      });
  }, [isActive, query, refreshRequestToken]);

  useEffect(() => {
    const layoutEditMenuBtn = document.getElementById("layoutEditMenuBtn");

    const handleOpenLayoutEdit = () => {
      if (!payload || loading || error) {
        return;
      }
      requestCloseSettingsPanel();
      setEditing(true);
    };

    if (layoutEditMenuBtn instanceof HTMLButtonElement) {
      layoutEditMenuBtn.disabled = !payload || loading || Boolean(error);
      layoutEditMenuBtn.addEventListener("click", handleOpenLayoutEdit);
    }

    return () => {
      if (layoutEditMenuBtn instanceof HTMLButtonElement) {
        layoutEditMenuBtn.disabled = false;
        layoutEditMenuBtn.removeEventListener("click", handleOpenLayoutEdit);
      }
    };
  }, [error, loading, payload, requestCloseSettingsPanel]);

  const previous = payload?.previous_period;
  const ordersTrend = resolveTrendDelta(payload?.order_count || 0, previous?.order_count);
  const revenueTrend = resolveTrendDelta(payload?.revenue_total_cents || 0, previous?.revenue_total_cents);
  const afterFeesTrend = resolveTrendDelta(payload?.after_fees_total_cents || 0, previous?.after_fees_total_cents);
  const purchaseTrend = resolveTrendDelta(payload?.purchase_total_cents || 0, previous?.purchase_total_cents);
  const profitTrend = resolveTrendDelta(payload?.profit_total_cents || 0, previous?.profit_total_cents);

  const handleTrendGranularityChange = (next: string) => {
    setTrendGranularity(normalizeTrendGranularity(next));
  };

  const buildCardDragProps = (group: DragGroup, cardId: string): CardDragProps => ({
    className: classNames(
      dragState?.group === group && dragState.cardId === cardId && "dragging",
      dragOverCard === cardId && "drag-over",
    ),
    draggable: editing,
    onDragStart: (event) => {
      if (!editing) {
        event.preventDefault();
        return;
      }

      const target = event.target;
      if (target instanceof Element && target.closest("button, input, select, textarea, a, canvas, .custom-select-trigger, .custom-select-menu")) {
        event.preventDefault();
        return;
      }

      setDragState({ group, cardId });
      setDragOverCard("");
      event.dataTransfer.effectAllowed = "move";
      event.dataTransfer.setData("text/plain", cardId);
    },
    onDragOver: (event) => {
      if (!editing || !dragState || dragState.group !== group || dragState.cardId === cardId) {
        return;
      }

      event.preventDefault();
      event.dataTransfer.dropEffect = "move";
      setDragOverCard(cardId);
    },
    onDrop: (event) => {
      if (!editing || !dragState || dragState.group !== group || dragState.cardId === cardId) {
        return;
      }

      event.preventDefault();
      setLayout((current) => moveCardWithinGroup(current, group, dragState.cardId, cardId));
      setDragState(null);
      setDragOverCard("");
    },
    onDragEnd: () => {
      setDragState(null);
      setDragOverCard("");
    },
  });

  const renderCard = (group: DragGroup, cardId: string): ReactNode => {
    const dragProps = buildCardDragProps(group, cardId);

    if (!payload) {
      return null;
    }

    if (group === "kpi" && cardId === "orders") {
      return (
        <KpiCard
          cardId="orders"
          name="Orders"
          valueId="kpiOrders"
          value={NUMBER_FORMATTER.format(payload.order_count)}
          tooltip="Gefilterter Scope"
          trendId="kpiOrdersTrend"
          trend={ordersTrend}
          dragProps={dragProps}
        />
      );
    }

    if (group === "kpi" && cardId === "revenue") {
      return (
        <KpiCard
          cardId="revenue"
          name="Umsatz"
          valueId="kpiRevenue"
          value={formatMoneyFromCents(payload.revenue_total_cents)}
          tooltip="Kundenbetrag gesamt"
          trendId="kpiRevenueTrend"
          trend={revenueTrend}
          dragProps={dragProps}
        />
      );
    }

    if (group === "kpi" && cardId === "after-fees") {
      return (
        <KpiCard
          cardId="after-fees"
          name="After Fees"
          valueId="kpiAfterFees"
          value={formatMoneyFromCents(payload.after_fees_total_cents)}
          tooltip="Nach Marketplace/Payment Fees"
          trendId="kpiAfterFeesTrend"
          trend={afterFeesTrend}
          dragProps={dragProps}
        />
      );
    }

    if (group === "kpi" && cardId === "purchase") {
      return (
        <KpiCard
          cardId="purchase"
          name="Einkauf"
          valueId="kpiPurchase"
          value={formatMoneyFromCents(payload.purchase_total_cents)}
          tooltip="Manuell gepflegte Einkaufskosten"
          trendId="kpiPurchaseTrend"
          trend={purchaseTrend}
          dragProps={dragProps}
        />
      );
    }

    if (group === "kpi" && cardId === "profit") {
      return (
        <KpiCard
          cardId="profit"
          name="Gewinn"
          valueId="kpiProfit"
          value={formatMoneyFromCents(payload.profit_total_cents)}
          trendId="kpiProfitTrend"
          trend={profitTrend}
          subId="kpiProfitSub"
          subText={`Marge ${formatPercent(payload.margin_pct)}`}
          valueClassName={payload.profit_total_cents < 0 ? "value-neg" : "value-pos"}
          dragProps={dragProps}
        />
      );
    }

    if (group === "kpi" && cardId === "channels") {
      return (
        <KpiCard
          cardId="channels"
          name="Channel Split"
          valueId="kpiChannels"
          value={`S: ${formatMoneyFromCents(payload.shopify_revenue_total_cents)} | K: ${formatMoneyFromCents(payload.kaufland_revenue_total_cents)}`}
          tooltip="Shopify / Kaufland Umsatz"
          dragProps={dragProps}
        />
      );
    }

    if (group === "insights-a" && cardId === "snapshot") {
      return (
        <article
          data-card-id="snapshot"
          className={classNames("card", "insight-card", dragProps.className)}
          draggable={dragProps.draggable}
          onDragStart={dragProps.onDragStart}
          onDragOver={dragProps.onDragOver}
          onDrop={dragProps.onDrop}
          onDragEnd={dragProps.onDragEnd}
        >
          <h2 className="chart-title" data-tooltip="E-Commerce Kernmetriken fuer Shopify und Kaufland im aktiven Filter.">Performance Snapshot</h2>
          <div className="insight-metric-grid">
            <div className="insight-metric"><div className="insight-label">AOV</div><div id="insightAov" className="insight-value">{formatMoneyFromCents(payload.aov_cents)}</div></div>
            <div className="insight-metric"><div className="insight-label">Gewinn / Order</div><div id="insightProfitPerOrder" className="insight-value">{formatMoneyFromCents(payload.avg_profit_per_order_cents)}</div></div>
            <div className="insight-metric"><div className="insight-label">Fee Quote</div><div id="insightFeeRate" className="insight-value">{formatPercent(payload.fees_ratio_pct)}</div></div>
            <div className="insight-metric"><div className="insight-label">Retourenrate</div><div id="insightReturnRate" className="insight-value">{formatPercent(payload.return_rate_pct)}</div></div>
            <div className="insight-metric"><div className="insight-label">Repeat Customer</div><div id="insightRepeatRate" className="insight-value">{formatPercent(payload.repeat_customer_rate_pct)}</div></div>
            <div className="insight-metric"><div className="insight-label">Einkauf gepflegt</div><div id="insightPurchaseCoverage" className="insight-value">{formatPercent(payload.purchase_coverage_pct)}</div></div>
            <div className="insight-metric"><div className="insight-label">Unique Kunden</div><div id="insightUniqueCustomers" className="insight-value">{NUMBER_FORMATTER.format(payload.unique_customers)}</div></div>
            <div className="insight-metric"><div className="insight-label">Fehlende Einkaufswerte</div><div id="insightMissingPurchase" className="insight-value">{NUMBER_FORMATTER.format(payload.purchase_missing_count)}</div></div>
          </div>
        </article>
      );
    }

    if (group === "insights-a" && cardId === "channel-compare") {
      return (
        <article
          data-card-id="channel-compare"
          className={classNames("card", "insight-card", dragProps.className)}
          draggable={dragProps.draggable}
          onDragStart={dragProps.onDragStart}
          onDragOver={dragProps.onDragOver}
          onDrop={dragProps.onDrop}
          onDragEnd={dragProps.onDragEnd}
        >
          <h2 className="chart-title" data-tooltip="Shopify vs. Kaufland: Umsatz, Gewinn und Qualitaetsmetriken.">Channel Vergleich</h2>
          <div className="table-wrap analytics-compact-table-wrap">
            <table className="analytics-compact-table">
              <thead>
                <tr>
                  <th>Channel</th>
                  <th>Orders</th>
                  <th>Umsatz</th>
                  <th>Gewinn</th>
                  <th>Marge</th>
                  <th>AOV</th>
                  <th>Retouren</th>
                </tr>
              </thead>
              <tbody id="marketplaceCompareBody">
                {payload.marketplaces.length ? payload.marketplaces.map((market) => (
                  <tr key={market.marketplace}>
                    <td>{market.marketplace === "shopify" ? "Shopify" : market.marketplace === "kaufland" ? "Kaufland" : market.marketplace.toUpperCase()}</td>
                    <td>{NUMBER_FORMATTER.format(market.order_count)}</td>
                    <td>{formatMoneyFromCents(market.revenue_total_cents)}</td>
                    <td className={market.profit_total_cents < 0 ? "value-neg" : "value-pos"}>{formatMoneyFromCents(market.profit_total_cents)}</td>
                    <td>{formatPercent(market.margin_pct)}</td>
                    <td>{formatMoneyFromCents(market.aov_cents)}</td>
                    <td>{formatPercent(market.return_rate_pct)}</td>
                  </tr>
                )) : (
                  <tr><td colSpan={7}>Keine Daten.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </article>
      );
    }

    if (group === "insights-b" && cardId === "fulfillment") {
      return (
        <article
          data-card-id="fulfillment"
          className={classNames("card", "insight-card", dragProps.className)}
          draggable={dragProps.draggable}
          onDragStart={dragProps.onDragStart}
          onDragOver={dragProps.onDragOver}
          onDrop={dragProps.onDrop}
          onDragEnd={dragProps.onDragEnd}
        >
          <h2 className="chart-title" data-tooltip="Statusverteilung im aktuellen Zeitraum.">Fulfillment &amp; Status</h2>
          <div className="status-pill-grid">
            <div className="status-pill"><div className="status-pill-label">Completed</div><div id="statusCompletedLike" className="status-pill-value">{NUMBER_FORMATTER.format(payload.status_summary.completed_like_count)}</div></div>
            <div className="status-pill"><div className="status-pill-label">Pending</div><div id="statusPendingLike" className="status-pill-value">{NUMBER_FORMATTER.format(payload.status_summary.pending_like_count)}</div></div>
            <div className="status-pill"><div className="status-pill-label">Returns</div><div id="statusReturnLike" className="status-pill-value">{NUMBER_FORMATTER.format(payload.status_summary.return_like_count)}</div></div>
            <div className="status-pill"><div className="status-pill-label">Other</div><div id="statusOther" className="status-pill-value">{NUMBER_FORMATTER.format(payload.status_summary.other_count)}</div></div>
          </div>
        </article>
      );
    }

    if (group === "insights-b" && cardId === "payments") {
      return (
        <article
          data-card-id="payments"
          className={classNames("card", "insight-card", dragProps.className)}
          draggable={dragProps.draggable}
          onDragStart={dragProps.onDragStart}
          onDragOver={dragProps.onDragOver}
          onDrop={dragProps.onDrop}
          onDragEnd={dragProps.onDragEnd}
        >
          <h2 className="chart-title" data-tooltip="Hauefigkeit nach Bestellungen im aktiven Scope.">Top Payment Methoden</h2>
          <div id="paymentMethodsList" className="payment-method-list">
            {payload.top_payment_methods.length ? payload.top_payment_methods.map((item) => (
              <div key={item.payment_method} className="payment-method-row">
                <span className="payment-method-name" title={item.payment_method}>{item.payment_method}</span>
                <span className="payment-method-count">{NUMBER_FORMATTER.format(item.order_count)}</span>
                <span className="payment-method-share">{formatPercent(item.share_pct)}</span>
              </div>
            )) : (
              <div className="payment-method-row"><span className="payment-method-name">Keine Daten</span><span className="payment-method-count">-</span><span className="payment-method-share">-</span></div>
            )}
          </div>
        </article>
      );
    }

    if (group === "donuts" && cardId === "donut-profit") {
      return <DonutMarketplaceCard payload={payload} themeToken={themeToken} dragProps={dragProps} />;
    }

    if (group === "donuts" && cardId === "donut-revenue") {
      return <DonutRevenueCard payload={payload} themeToken={themeToken} dragProps={dragProps} />;
    }

    if (group === "heatmap" && cardId === "heatmap") {
      return <HeatmapCard payload={payload} dragProps={dragProps} />;
    }

    if (group === "charts" && cardId === "trend") {
      return (
        <TrendCard
          payload={payload}
          trendGranularity={filters.trendGranularity}
          themeToken={themeToken}
          onTrendGranularityChange={handleTrendGranularityChange}
          dragProps={dragProps}
        />
      );
    }

    if (group === "charts" && cardId === "top-articles") {
      return <TopArticlesCard payload={payload} dragProps={dragProps} />;
    }

    return null;
  };

  let content: ReactNode;

  if (!payload && loading) {
    content = <StateCard title="Analytics wird geladen" message="KPIs, Trends und Verteilungen werden aus der neuen React-Route geladen." />;
  } else if (error) {
    content = <StateCard title="Analytics konnte nicht geladen werden" message={error} />;
  } else if (!payload) {
    content = <StateCard title="Analytics ist leer" message="Es sind noch keine Analytics-Daten verfuegbar." />;
  } else {
    content = layout.sections.map((sectionId, index) => {
      const section = SECTION_CONFIG[sectionId];
      const sectionCards = layout.cards[section.group]
        .map((cardId) => renderCard(section.group, cardId))
        .filter(Boolean);

      return (
        <section
          key={sectionId}
          className={section.className}
          data-drag-group={section.group}
          data-section-id={sectionId}
        >
          <div className="section-reorder-bar">
            <button
              type="button"
              className="section-move-btn"
              title="Nach oben"
              disabled={index === 0}
              onClick={() => {
                setLayout((current) => moveSection(current, sectionId, -1));
              }}
            >
              &#9650;
            </button>
            <button
              type="button"
              className="section-move-btn"
              title="Nach unten"
              disabled={index === layout.sections.length - 1}
              onClick={() => {
                setLayout((current) => moveSection(current, sectionId, 1));
              }}
            >
              &#9660;
            </button>
          </div>
          {sectionCards}
        </section>
      );
    });
  }

  return (
    <div id="analyticsPanel" className={classNames("tab-panel", "active", editing && "layout-editing")} data-react-analytics-panel="true">
      <div className="layout-toolbar">
        <button type="button" className="layout-edit-btn active" onClick={() => setEditing(false)}>
          Fertig
        </button>
      </div>
      {content}
    </div>
  );
}
