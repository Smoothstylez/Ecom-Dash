"use strict";

/* ── Analytics Tab ── */

/* ── Trend Granularity Helpers ── */

function normalizeTrendGranularity(value) {
  const token = String(value || "").trim().toLowerCase();
  if (token === "day" || token === "daily") {
    return "day";
  }
  if (token === "week" || token === "weekly" || token === "woche") {
    return "week";
  }
  if (token === "year" || token === "yearly" || token === "jahr") {
    return "year";
  }
  if (token === "month" || token === "monthly" || token === "monat") {
    return "month";
  }
  return "auto";
}

function trendGranularityLabel(value) {
  const token = normalizeTrendGranularity(value);
  if (token === "day") {
    return "Tag";
  }
  if (token === "week") {
    return "Woche";
  }
  if (token === "month") {
    return "Monat";
  }
  if (token === "year") {
    return "Jahr";
  }
  return "Auto";
}

function updateTrendGranularitySelection() {
  if (!(els.trendGranularityGroup instanceof HTMLElement)) {
    return;
  }
  const active = normalizeTrendGranularity(state.trendGranularity);
  const buttons = els.trendGranularityGroup.querySelectorAll("[data-trend-granularity]");
  buttons.forEach((button) => {
    if (!(button instanceof HTMLElement)) {
      return;
    }
    const token = normalizeTrendGranularity(button.dataset.trendGranularity || "auto");
    button.classList.toggle("active", token === active);
  });
}

function setTrendGranularity(value) {
  state.trendGranularity = normalizeTrendGranularity(value);
  updateTrendGranularitySelection();
}

function isoWeekNumber(dateValue) {
  const target = new Date(dateValue.getTime());
  target.setHours(0, 0, 0, 0);
  target.setDate(target.getDate() + 3 - ((target.getDay() + 6) % 7));
  const firstThursday = new Date(target.getFullYear(), 0, 4);
  firstThursday.setHours(0, 0, 0, 0);
  firstThursday.setDate(firstThursday.getDate() + 3 - ((firstThursday.getDay() + 6) % 7));
  return 1 + Math.round((target.getTime() - firstThursday.getTime()) / 604800000);
}

function formatTrendPointLabel(point, granularity) {
  const token = String(point?.bucket_start || "").trim();
  const parsed = parseDateToken(token);
  if (!parsed) {
    return token || "-";
  }
  if (granularity === "day") {
    return parsed.toLocaleDateString("de-DE", { day: "2-digit", month: "2-digit" });
  }
  if (granularity === "week") {
    return `KW ${String(isoWeekNumber(parsed)).padStart(2, "0")}`;
  }
  return parsed.toLocaleDateString("de-DE", { month: "short", year: "2-digit" });
}

/* ── Load ── */

async function loadAnalytics() {
  const params = buildQuery();
  params.set("trendGranularity", normalizeTrendGranularity(state.trendGranularity));
  const payload = await fetchJson(`${API_BASE}/analytics/kpis?${params.toString()}`);
  state.analytics = payload;
  const resolved = normalizeTrendGranularity(payload?.trend?.granularity || state.trendGranularity);
  state.trendGranularityResolved = resolved === "auto" ? "day" : resolved;
}

/* ── Render Functions ── */

function renderChannelKpiText() {
  const shopifyValue = Number(state.kpiAnimatedValues.channelShopify || 0);
  const kauflandValue = Number(state.kpiAnimatedValues.channelKaufland || 0);
  els.kpiChannels.textContent = `S: ${centsToMoney(Math.round(shopifyValue))} | K: ${centsToMoney(Math.round(kauflandValue))}`;
}

function renderKpis() {
  const analytics = state.analytics || {};
  const orderCount = Number(analytics.order_count || 0);
  const revenue = Number(analytics.revenue_total_cents || 0);
  const afterFees = Number(analytics.after_fees_total_cents || 0);
  const purchase = Number(analytics.purchase_total_cents || 0);
  const profit = Number(analytics.profit_total_cents || 0);
  const margin = Number(analytics.margin_pct || 0);
  const shopifyRevenue = Number(analytics.shopify_revenue_total_cents || 0);
  const kauflandRevenue = Number(analytics.kaufland_revenue_total_cents || 0);

  animateKpiValue("orderCount", orderCount, (value) => {
    els.kpiOrders.textContent = NUMBER_FMT.format(Math.round(value));
  });
  animateKpiValue("revenueCents", revenue, (value) => {
    els.kpiRevenue.textContent = centsToMoney(Math.round(value));
  });
  animateKpiValue("afterFeesCents", afterFees, (value) => {
    els.kpiAfterFees.textContent = centsToMoney(Math.round(value));
  });
  animateKpiValue("purchaseCents", purchase, (value) => {
    els.kpiPurchase.textContent = centsToMoney(Math.round(value));
  });
  animateKpiValue("profitCents", profit, (value) => {
    els.kpiProfit.textContent = centsToMoney(Math.round(value));
  });
  animateKpiValue("marginPct", margin, (value) => {
    els.kpiProfitSub.textContent = `Marge ${value.toFixed(2)}%`;
  });
  animateKpiValue("channelShopify", shopifyRevenue, () => {
    renderChannelKpiText();
  });
  animateKpiValue("channelKaufland", kauflandRevenue, () => {
    renderChannelKpiText();
  });

  els.kpiProfit.classList.toggle("value-neg", profit < 0);
  els.kpiProfit.classList.toggle("value-pos", profit >= 0);
}

function formatPercent(value, fractionDigits = 2) {
  const numeric = Number(value);
  const safe = Number.isFinite(numeric) ? numeric : 0;
  return `${safe.toLocaleString("de-DE", {
    minimumFractionDigits: fractionDigits,
    maximumFractionDigits: fractionDigits,
  })}%`;
}

function renderAnalyticsInsights() {
  const analytics = state.analytics || {};

  const aovCents = Number(analytics.aov_cents || 0);
  const avgProfitPerOrderCents = Number(analytics.avg_profit_per_order_cents || 0);
  const feeRatePct = Number(analytics.fees_ratio_pct || 0);
  const returnRatePct = Number(analytics.return_rate_pct || 0);
  const repeatRatePct = Number(analytics.repeat_customer_rate_pct || 0);
  const purchaseCoveragePct = Number(analytics.purchase_coverage_pct || 0);
  const uniqueCustomers = Number(analytics.unique_customers || 0);
  const missingPurchaseCount = Number(analytics.purchase_missing_count || 0);

  animateKpiValue("insightAovCents", aovCents, (value) => {
    if (els.insightAov instanceof HTMLElement) {
      els.insightAov.textContent = centsToMoney(Math.round(value));
    }
  });
  animateKpiValue("insightProfitPerOrderCents", avgProfitPerOrderCents, (value) => {
    if (els.insightProfitPerOrder instanceof HTMLElement) {
      els.insightProfitPerOrder.textContent = centsToMoney(Math.round(value));
    }
  });
  animateKpiValue("insightFeeRatePct", feeRatePct, (value) => {
    if (els.insightFeeRate instanceof HTMLElement) {
      els.insightFeeRate.textContent = formatPercent(value);
    }
  });
  animateKpiValue("insightReturnRatePct", returnRatePct, (value) => {
    if (els.insightReturnRate instanceof HTMLElement) {
      els.insightReturnRate.textContent = formatPercent(value);
    }
  });
  animateKpiValue("insightRepeatRatePct", repeatRatePct, (value) => {
    if (els.insightRepeatRate instanceof HTMLElement) {
      els.insightRepeatRate.textContent = formatPercent(value);
    }
  });
  animateKpiValue("insightPurchaseCoveragePct", purchaseCoveragePct, (value) => {
    if (els.insightPurchaseCoverage instanceof HTMLElement) {
      els.insightPurchaseCoverage.textContent = formatPercent(value);
    }
  });
  animateKpiValue("insightUniqueCustomers", uniqueCustomers, (value) => {
    if (els.insightUniqueCustomers instanceof HTMLElement) {
      els.insightUniqueCustomers.textContent = NUMBER_FMT.format(Math.round(value));
    }
  });
  animateKpiValue("insightMissingPurchase", missingPurchaseCount, (value) => {
    if (els.insightMissingPurchase instanceof HTMLElement) {
      els.insightMissingPurchase.textContent = NUMBER_FMT.format(Math.round(value));
    }
  });

  if (els.marketplaceCompareBody instanceof HTMLElement) {
    const markets = Array.isArray(analytics.marketplaces) ? analytics.marketplaces : [];
    const rows = markets.map((item) => {
      const marketToken = String(item?.marketplace || "").trim().toLowerCase();
      const marketLabel = marketToken === "shopify"
        ? "Shopify"
        : (marketToken === "kaufland" ? "Kaufland" : (marketToken || "-").toUpperCase());
      const profitCents = Number(item?.profit_total_cents || 0);
      const profitClass = profitCents < 0 ? "value-neg" : "value-pos";
      return `<tr>
        <td>${escapeHtml(marketLabel)}</td>
        <td>${escapeHtml(NUMBER_FMT.format(Number(item?.order_count || 0)))}</td>
        <td>${escapeHtml(centsToMoney(item?.revenue_total_cents || 0))}</td>
        <td class="${profitClass}">${escapeHtml(centsToMoney(profitCents))}</td>
        <td>${escapeHtml(formatPercent(item?.margin_pct || 0))}</td>
        <td>${escapeHtml(centsToMoney(item?.aov_cents || 0))}</td>
        <td>${escapeHtml(formatPercent(item?.return_rate_pct || 0))}</td>
      </tr>`;
    }).join("");
    els.marketplaceCompareBody.innerHTML = rows || "<tr><td colspan=\"7\">Keine Daten.</td></tr>";
  }

  const statusSummary = analytics.status_summary && typeof analytics.status_summary === "object"
    ? analytics.status_summary
    : {};
  animateKpiValue("statusCompletedLike", Number(statusSummary.completed_like_count || 0), (value) => {
    if (els.statusCompletedLike instanceof HTMLElement) {
      els.statusCompletedLike.textContent = NUMBER_FMT.format(Math.round(value));
    }
  });
  animateKpiValue("statusPendingLike", Number(statusSummary.pending_like_count || 0), (value) => {
    if (els.statusPendingLike instanceof HTMLElement) {
      els.statusPendingLike.textContent = NUMBER_FMT.format(Math.round(value));
    }
  });
  animateKpiValue("statusReturnLike", Number(statusSummary.return_like_count || 0), (value) => {
    if (els.statusReturnLike instanceof HTMLElement) {
      els.statusReturnLike.textContent = NUMBER_FMT.format(Math.round(value));
    }
  });
  animateKpiValue("statusOther", Number(statusSummary.other_count || 0), (value) => {
    if (els.statusOther instanceof HTMLElement) {
      els.statusOther.textContent = NUMBER_FMT.format(Math.round(value));
    }
  });

  if (els.paymentMethodsList instanceof HTMLElement) {
    const paymentMethods = Array.isArray(analytics.top_payment_methods)
      ? analytics.top_payment_methods
      : [];
    const rows = paymentMethods.map((item) => {
      const name = String(item?.payment_method || "-").trim() || "-";
      const count = Number(item?.order_count || 0);
      const sharePct = Number(item?.share_pct || 0);
      return `<div class="payment-method-row">
        <span class="payment-method-name" title="${escapeHtml(name)}">${escapeHtml(name)}</span>
        <span class="payment-method-count">${escapeHtml(NUMBER_FMT.format(count))}</span>
        <span class="payment-method-share">${escapeHtml(formatPercent(sharePct))}</span>
      </div>`;
    }).join("");
    if (rows) {
      els.paymentMethodsList.innerHTML = rows;
    } else {
      els.paymentMethodsList.innerHTML = "<div class=\"payment-method-row\"><span class=\"payment-method-name\">Keine Daten</span><span class=\"payment-method-count\">-</span><span class=\"payment-method-share\">-</span></div>";
    }
  }
}

function renderTrendChart() {
  const analytics = state.analytics || {};
  const trend = analytics.trend && typeof analytics.trend === "object" ? analytics.trend : {};
  updateTrendGranularitySelection();
  let resolvedGranularity = normalizeTrendGranularity(trend.granularity || state.trendGranularityResolved || state.trendGranularity);
  if (resolvedGranularity === "auto") {
    resolvedGranularity = "day";
  }
  state.trendGranularityResolved = resolvedGranularity;

  let points = Array.isArray(trend.points) ? trend.points : [];
  if (!points.length) {
    const monthlyFallback = Array.isArray(analytics.monthly) ? analytics.monthly : [];
    points = monthlyFallback.map((row) => ({
      bucket_start: `${String(row.month || "").trim()}-01`,
      revenue_total_cents: Number(row.revenue_total_cents || 0),
      profit_total_cents: Number(row.profit_total_cents || 0),
      order_count: Number(row.order_count || 0),
    }));
    resolvedGranularity = "month";
    state.trendGranularityResolved = resolvedGranularity;
  }

  const labels = points.map((row) => formatTrendPointLabel(row, resolvedGranularity));
  const revenue = points.map((row) => Number(row.revenue_total_cents || 0) / 100);
  const profit = points.map((row) => Number(row.profit_total_cents || 0) / 100);
  const pointRadius = points.length > 120 ? 0 : (points.length > 64 ? 1 : 2);

  if (els.trendChartTitle instanceof HTMLElement) {
    const title = String(trend.title || "").trim();
    if (title) {
      els.trendChartTitle.textContent = title;
    } else if (resolvedGranularity === "day") {
      els.trendChartTitle.textContent = "Tagesverlauf";
    } else if (resolvedGranularity === "week") {
      els.trendChartTitle.textContent = "Wochenverlauf";
    } else {
      els.trendChartTitle.textContent = "Monatsverlauf";
    }
  }

  if (els.trendChartSub instanceof HTMLElement) {
    const fromToken = String(trend.from || state.filters.from || "").trim();
    const toToken = String(trend.to || state.filters.to || "").trim();
    const rangeText = fromToken && toToken
      ? `${formatDateTokenLabel(fromToken)} - ${formatDateTokenLabel(toToken)}`
      : "Aktueller Filter";
    const selectedMode = normalizeTrendGranularity(state.trendGranularity);
    const modeText = selectedMode === "auto"
      ? `Auto (${trendGranularityLabel(resolvedGranularity)})`
      : trendGranularityLabel(resolvedGranularity);
    els.trendChartSub.textContent = `Umsatz und Gewinn · ${modeText} · ${rangeText} · ${NUMBER_FMT.format(points.length)} Punkte`;
  }

  if (state.trendChart) {
    state.trendChart.destroy();
    state.trendChart = null;
  }

  if (!(els.trendChartCanvas instanceof HTMLCanvasElement)) {
    return;
  }

  const cs = getComputedStyle(document.documentElement);
  const c1 = cs.getPropertyValue("--th-chart-1").trim();
  const c1f = cs.getPropertyValue("--th-chart-1-fill").trim();
  const c2 = cs.getPropertyValue("--th-chart-2").trim();
  const c2f = cs.getPropertyValue("--th-chart-2-fill").trim();
  const grid = cs.getPropertyValue("--th-chart-grid").trim();
  const label = cs.getPropertyValue("--th-chart-label").trim();

  state.trendChart = new Chart(els.trendChartCanvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Umsatz",
          data: revenue,
          borderColor: c1,
          backgroundColor: c1f,
          pointRadius,
          pointHoverRadius: 4,
          pointHitRadius: 10,
          borderWidth: 2.1,
          tension: 0.33,
          fill: false,
        },
        {
          label: "Gewinn",
          data: profit,
          borderColor: c2,
          backgroundColor: c2f,
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
            color: label,
            font: {
              size: 11,
              weight: "600",
            },
          },
        },
        tooltip: {
          callbacks: {
            label(context) {
              const label = String(context.dataset?.label || "Wert");
              return `${label}: ${MONEY_FMT.format(Number(context.parsed?.y || 0))}`;
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
            color: label,
            maxRotation: 0,
            autoSkip: true,
            maxTicksLimit: resolvedGranularity === "day" ? 14 : 12,
          },
        },
        y: {
          grid: {
            color: grid,
          },
          ticks: {
            color: label,
            callback(value) {
              return MONEY_FMT.format(value);
            },
          },
        },
      },
    },
  });
}

function renderDonutMarketplace() {
  const analytics = state.analytics || {};
  const marketplaces = Array.isArray(analytics.marketplaces) ? analytics.marketplaces : [];

  if (state.donutMarketplaceChart) {
    state.donutMarketplaceChart.destroy();
    state.donutMarketplaceChart = null;
  }
  if (!(els.donutMarketplaceCanvas instanceof HTMLCanvasElement)) return;

  const cs = getComputedStyle(document.documentElement);
  const colShopify = cs.getPropertyValue("--th-donut-shopify").trim();
  const colKaufland = cs.getPropertyValue("--th-donut-kaufland").trim();
  const labelColor = cs.getPropertyValue("--th-chart-label").trim();

  let shopifyProfit = 0;
  let kauflandProfit = 0;
  marketplaces.forEach(function (mp) {
    const name = String(mp.marketplace || "").toLowerCase();
    const profit = Number(mp.profit_total_cents || 0) / 100;
    if (name === "shopify") shopifyProfit = profit;
    else if (name === "kaufland") kauflandProfit = profit;
  });

  const total = shopifyProfit + kauflandProfit;
  const visualTotal = Math.max(0, shopifyProfit) + Math.max(0, kauflandProfit);
  if (els.donutMarketplaceCenterValue instanceof HTMLElement) {
    els.donutMarketplaceCenterValue.textContent = MONEY_FMT.format(total);
  }

  /* If both are zero or negative, show a placeholder grey ring */
  const hasData = shopifyProfit > 0 || kauflandProfit > 0;
  const data = hasData ? [Math.max(0, shopifyProfit), Math.max(0, kauflandProfit)] : [1];
  const bgColors = hasData ? [colShopify, colKaufland] : [cs.getPropertyValue("--th-line").trim() || "#ccc"];
  const labels = hasData ? ["Shopify", "Kaufland"] : ["Keine Daten"];

  state.donutMarketplaceChart = new Chart(els.donutMarketplaceCanvas, {
    type: "doughnut",
    data: {
      labels: labels,
      datasets: [{
        data: data,
        backgroundColor: bgColors,
        borderWidth: 0,
        hoverOffset: 6,
      }],
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
            color: labelColor,
            font: { size: 11, weight: "600" },
            padding: 14,
          },
        },
        tooltip: {
          callbacks: {
            label: function (context) {
              var label = String(context.label || "");
              var val = Number(context.parsed || 0);
              var pct = visualTotal > 0 ? ((val / visualTotal) * 100).toFixed(1) : "0.0";
              return label + ": " + MONEY_FMT.format(val) + " (" + pct + "%)";
            },
          },
        },
      },
    },
  });
}

function renderDonutRevenue() {
  const analytics = state.analytics || {};
  const revenue = Number(analytics.revenue_total_cents || 0) / 100;
  const afterFees = Number(analytics.after_fees_total_cents || 0) / 100;
  const purchase = Number(analytics.purchase_total_cents || 0) / 100;
  const profit = Number(analytics.profit_total_cents || 0) / 100;
  const fees = revenue - afterFees;

  if (state.donutRevenueChart) {
    state.donutRevenueChart.destroy();
    state.donutRevenueChart = null;
  }
  if (!(els.donutRevenueCanvas instanceof HTMLCanvasElement)) return;

  const cs = getComputedStyle(document.documentElement);
  const colFees = cs.getPropertyValue("--th-donut-fees").trim();
  const colPurchase = cs.getPropertyValue("--th-donut-purchase").trim();
  const colProfit = cs.getPropertyValue("--th-donut-profit").trim();
  const labelColor = cs.getPropertyValue("--th-chart-label").trim();

  if (els.donutRevenueCenterValue instanceof HTMLElement) {
    els.donutRevenueCenterValue.textContent = MONEY_FMT.format(revenue);
  }

  const hasData = revenue > 0;
  const data = hasData ? [Math.max(0, fees), Math.max(0, purchase), Math.max(0, profit)] : [1];
  const bgColors = hasData ? [colFees, colPurchase, colProfit] : [cs.getPropertyValue("--th-line").trim() || "#ccc"];
  const labels = hasData ? ["Fees", "Einkauf", "Gewinn"] : ["Keine Daten"];

  state.donutRevenueChart = new Chart(els.donutRevenueCanvas, {
    type: "doughnut",
    data: {
      labels: labels,
      datasets: [{
        data: data,
        backgroundColor: bgColors,
        borderWidth: 0,
        hoverOffset: 6,
      }],
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
            color: labelColor,
            font: { size: 11, weight: "600" },
            padding: 14,
          },
        },
        tooltip: {
          callbacks: {
            label: function (context) {
              var label = String(context.label || "");
              var val = Number(context.parsed || 0);
              var pct = revenue > 0 ? ((val / revenue) * 100).toFixed(1) : "0.0";
              return label + ": " + MONEY_FMT.format(val) + " (" + pct + "%)";
            },
          },
        },
      },
    },
  });
}

function renderTopArticles() {
  const analytics = state.analytics || {};
  const items = Array.isArray(analytics.top_articles) ? analytics.top_articles : [];
  const rows = items.map((item) => {
    return `<tr>
      <td title="${escapeHtml(item.article || "-")}">${escapeHtml(item.article || "-")}</td>
      <td>${escapeHtml(NUMBER_FMT.format(Number(item.order_count || 0)))}</td>
      <td>${escapeHtml(centsToMoney(item.revenue_total_cents || 0))}</td>
      <td>${escapeHtml(centsToMoney(item.profit_total_cents || 0))}</td>
    </tr>`;
  }).join("");

  els.topArticlesBody.innerHTML = rows || "<tr><td colspan=\"4\">Keine Daten.</td></tr>";
}

/* ── KPI Trend Indicators (previous period comparison) ── */

function renderKpiTrendBadge(el, current, previous) {
  if (!(el instanceof HTMLElement)) return;
  if (previous == null || previous === 0) {
    el.textContent = "";
    el.className = "kpi-trend";
    return;
  }
  const change = ((current - previous) / Math.abs(previous)) * 100;
  const rounded = Math.abs(change) >= 10 ? Math.round(change) : change.toFixed(1);
  const sign = change > 0 ? "+" : "";
  const arrow = change > 0 ? "\u25B2" : change < 0 ? "\u25BC" : "";
  const cls = change > 0 ? "trend-up" : change < 0 ? "trend-down" : "trend-flat";
  el.textContent = `${arrow} ${sign}${rounded}%`;
  el.className = `kpi-trend ${cls}`;
}

function renderKpiTrends() {
  const analytics = state.analytics || {};
  const prev = analytics.previous_period;
  if (!prev) {
    [els.kpiOrdersTrend, els.kpiRevenueTrend, els.kpiAfterFeesTrend,
     els.kpiPurchaseTrend, els.kpiProfitTrend].forEach((el) => {
      if (el instanceof HTMLElement) {
        el.textContent = "";
        el.className = "kpi-trend";
      }
    });
    return;
  }
  renderKpiTrendBadge(els.kpiOrdersTrend, Number(analytics.order_count || 0), Number(prev.order_count || 0));
  renderKpiTrendBadge(els.kpiRevenueTrend, Number(analytics.revenue_total_cents || 0), Number(prev.revenue_total_cents || 0));
  renderKpiTrendBadge(els.kpiAfterFeesTrend, Number(analytics.after_fees_total_cents || 0), Number(prev.after_fees_total_cents || 0));
  renderKpiTrendBadge(els.kpiPurchaseTrend, Number(analytics.purchase_total_cents || 0), Number(prev.purchase_total_cents || 0));
  renderKpiTrendBadge(els.kpiProfitTrend, Number(analytics.profit_total_cents || 0), Number(prev.profit_total_cents || 0));
}

/* ── Purchase Timing Heatmap ── */

function renderPurchaseHeatmap() {
  const container = els.purchaseHeatmap;
  if (!(container instanceof HTMLElement)) return;

  const analytics = state.analytics || {};
  const grid = analytics.purchase_heatmap;
  if (!Array.isArray(grid) || grid.length !== 7) {
    container.innerHTML = "<div class=\"heatmap-empty\">Keine Daten.</div>";
    return;
  }

  let maxCount = 0;
  for (let d = 0; d < 7; d++) {
    for (let h = 0; h < 24; h++) {
      const v = grid[d][h] || 0;
      if (v > maxCount) maxCount = v;
    }
  }

  const dayLabels = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"];
  const themeColors = getComputedStyle(document.documentElement);
  const accentColor = themeColors.getPropertyValue("--th-btn-primary-from").trim() || "#3b82f6";

  let html = "<div class=\"heatmap-row heatmap-header\"><div class=\"heatmap-day-label\"></div>";
  for (let h = 0; h < 24; h++) {
    html += `<div class="heatmap-hour-label">${h}</div>`;
  }
  html += "</div>";

  for (let d = 0; d < 7; d++) {
    html += `<div class="heatmap-row"><div class="heatmap-day-label">${dayLabels[d]}</div>`;
    for (let h = 0; h < 24; h++) {
      const count = grid[d][h] || 0;
      const intensity = maxCount > 0 ? count / maxCount : 0;
      const opacity = intensity > 0 ? 0.1 + intensity * 0.9 : 0;
      const title = `${dayLabels[d]} ${String(h).padStart(2, "0")}:00 – ${count} ${count === 1 ? "Bestellung" : "Bestellungen"}`;
      html += `<div class="heatmap-cell" style="--cell-opacity: ${opacity.toFixed(2)}" title="${title}"></div>`;
    }
    html += "</div>";
  }

  container.innerHTML = html;
}
