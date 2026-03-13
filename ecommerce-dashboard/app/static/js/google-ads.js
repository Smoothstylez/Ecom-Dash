"use strict";

/* ── Google Ads Tab ── */

/* ── Load ── */
async function loadGoogleAds() {
  const params = buildQuery();
  const payload = await fetchJson(`${API_BASE}/google-ads/analytics?${params.toString()}`);
  state.googleAds = payload;
}

/* ── File Label Sync ── */
function syncGoogleAdsFileLabels() {
  if (els.googleAdsReportFileLabel instanceof HTMLElement) {
    const file = els.googleAdsReportInput?.files && els.googleAdsReportInput.files[0];
    els.googleAdsReportFileLabel.textContent = file ? file.name : "Keine Datei";
  }
  if (els.googleAdsAssignmentFileLabel instanceof HTMLElement) {
    const file = els.googleAdsAssignmentInput?.files && els.googleAdsAssignmentInput.files[0];
    els.googleAdsAssignmentFileLabel.textContent = file ? file.name : "Keine Datei";
  }
}

/* ── Render ── */
function renderGoogleAds() {
  const payload = state.googleAds || {};
  const kpis = payload.kpis && typeof payload.kpis === "object" ? payload.kpis : {};
  const imports = payload.imports && typeof payload.imports === "object" ? payload.imports : {};
  const reportImport = imports.report && typeof imports.report === "object" ? imports.report : {};
  const assignmentImport = imports.assignment && typeof imports.assignment === "object" ? imports.assignment : {};
  const reportMeta = reportImport.meta && typeof reportImport.meta === "object" ? reportImport.meta : {};
  const assignmentMeta = assignmentImport.meta && typeof assignmentImport.meta === "object" ? assignmentImport.meta : {};

  const reportFilename = String(reportImport.filename || "-").trim() || "-";
  const reportImportedAt = reportImport.imported_at ? formatDate(reportImport.imported_at) : "-";
  const reportFromToken = String(reportMeta.report_from_day || "").trim();
  const reportToToken = String(reportMeta.report_to_day || "").trim();
  const reportLastToken = String(reportMeta.report_to_day || reportMeta.last_non_zero_day || "").trim();
  const reportRows = Number(reportMeta.rows || 0);
  const reportNonZeroRows = Number(reportMeta.non_zero_rows || 0);
  const reportRangeLabel = reportFromToken && reportToToken
    ? `${formatDateTokenLabel(reportFromToken)} - ${formatDateTokenLabel(reportToToken)}`
    : "-";
  const reportLastLabel = reportLastToken ? formatDateTokenLabel(reportLastToken) : "-";

  const assignmentFilename = String(assignmentImport.filename || "-").trim() || "-";
  const assignmentImportedAt = assignmentImport.imported_at ? formatDate(assignmentImport.imported_at) : "-";
  const assignmentRows = Number(assignmentMeta.rows || 0);

  /* ── Import status badge ── */
  if (els.googleAdsImportMeta instanceof HTMLElement) {
    const hasData = Number(kpis.ads_cost_total_cents || 0) > 0 || Number(kpis.products_count || 0) > 0;
    els.googleAdsImportMeta.textContent = hasData ? "Aktiv" : "Keine Daten";
  }

  /* ── Import status info ── */
  if (els.googleAdsStatusInfo instanceof HTMLElement) {
    const lines = [
      `Report: <strong>${escapeHtml(reportFilename)}</strong>`,
      `Zeitraum: <strong>${escapeHtml(reportRangeLabel)}</strong>`,
      `Letztes Datum: <strong>${escapeHtml(reportLastLabel)}</strong>`,
      `Zeilen: <strong>${escapeHtml(NUMBER_FMT.format(reportRows))}</strong> (mit Kosten: ${escapeHtml(NUMBER_FMT.format(reportNonZeroRows))})`,
      `Import: <strong>${escapeHtml(reportImportedAt)}</strong>`,
      `Zuweisung: <strong>${escapeHtml(assignmentFilename)}</strong> (${escapeHtml(NUMBER_FMT.format(assignmentRows))} Zeilen, ${escapeHtml(assignmentImportedAt)})`,
    ];
    els.googleAdsStatusInfo.innerHTML = lines.join(" &middot; ");
  }

  /* ── KPIs ── */
  const totalAdsCost = Number(kpis.ads_cost_total_cents || 0);
  const totalRevenue = Number(kpis.shopify_revenue_total_cents || 0);
  const totalOrders = Number(kpis.orders_count || 0);
  const profitAfter = Number(kpis.profit_after_ads_total_cents || 0);
  const profitBefore = Number(kpis.profit_before_ads_total_cents || 0);
  const roas = Number(kpis.roas || 0);
  const missingCount = Number(kpis.missing_assignments_count || 0);

  if (els.googleAdsKpiCostTotal instanceof HTMLElement) {
    els.googleAdsKpiCostTotal.textContent = centsToMoney(totalAdsCost);
  }
  if (els.googleAdsKpiCostSplit instanceof HTMLElement) {
    els.googleAdsKpiCostSplit.textContent = `Gemappt ${centsToMoney(kpis.ads_cost_mapped_cents || 0)} | Unmapped ${centsToMoney(kpis.ads_cost_unmapped_cents || 0)}`;
  }
  if (els.googleAdsKpiRevenue instanceof HTMLElement) {
    els.googleAdsKpiRevenue.textContent = centsToMoney(totalRevenue);
  }
  if (els.googleAdsKpiProfitAfter instanceof HTMLElement) {
    els.googleAdsKpiProfitAfter.textContent = centsToMoney(profitAfter);
    els.googleAdsKpiProfitAfter.classList.toggle("value-neg", profitAfter < 0);
    els.googleAdsKpiProfitAfter.classList.toggle("value-pos", profitAfter >= 0);
  }
  if (els.googleAdsKpiProfitBefore instanceof HTMLElement) {
    els.googleAdsKpiProfitBefore.textContent = `Vor Ads: ${centsToMoney(profitBefore)}`;
  }
  if (els.googleAdsKpiRoas instanceof HTMLElement) {
    els.googleAdsKpiRoas.textContent = `${roas.toFixed(2)}x`;
  }
  if (els.googleAdsKpiMissing instanceof HTMLElement) {
    els.googleAdsKpiMissing.textContent = `Fehlende Zuweisungen: ${NUMBER_FMT.format(missingCount)}`;
  }

  /* New KPIs */
  if (els.googleAdsKpiCostPerOrder instanceof HTMLElement) {
    if (totalOrders > 0 && totalAdsCost > 0) {
      els.googleAdsKpiCostPerOrder.textContent = centsToMoney(Math.round(totalAdsCost / totalOrders));
    } else {
      els.googleAdsKpiCostPerOrder.textContent = "-";
    }
  }
  if (els.googleAdsKpiOrderCount instanceof HTMLElement) {
    els.googleAdsKpiOrderCount.textContent = `${NUMBER_FMT.format(totalOrders)} Orders`;
  }
  if (els.googleAdsKpiAdsShare instanceof HTMLElement) {
    if (totalRevenue > 0 && totalAdsCost > 0) {
      const share = (totalAdsCost / totalRevenue) * 100;
      els.googleAdsKpiAdsShare.textContent = `${share.toFixed(1)} %`;
      els.googleAdsKpiAdsShare.classList.toggle("value-neg", share > 30);
      els.googleAdsKpiAdsShare.classList.toggle("value-pos", share <= 30);
    } else {
      els.googleAdsKpiAdsShare.textContent = "-";
      els.googleAdsKpiAdsShare.classList.remove("value-neg", "value-pos");
    }
  }

  /* ── Products table ── */
  const products = Array.isArray(payload.products) ? payload.products : [];
  if (els.googleAdsProductsMeta instanceof HTMLElement) {
    els.googleAdsProductsMeta.textContent = `${NUMBER_FMT.format(products.length)} Zeilen`;
  }
  if (els.googleAdsProductsBody instanceof HTMLElement) {
    /* Collapse any open detail row */
    _collapseGoogleAdsProductDetail();

    const productRows = products.map((item, idx) => {
      const profitAfter = Number(item?.profit_after_ads_cents || 0);
      const adsCost = Number(item?.ads_cost_cents || 0);
      const revenue = Number(item?.revenue_total_cents || 0);
      const itemRoas = adsCost > 0 ? (revenue / adsCost) : 0;
      const profitClass = profitAfter < 0 ? "value-neg" : "value-pos";
      const mappingLabel = item?.mapped ? "Gemappt" : "Unmapped";
      const mappingClass = item?.mapped ? "badge badge-invoice" : "badge badge-refund";
      const pKey = escapeHtml(item?.product_key || "");
      return `<tr class="ga-product-row" data-product-key="${pKey}" data-product-idx="${idx}" style="cursor:pointer">
        <td title="${escapeHtml(item?.product_detail || "-")}">${escapeHtml(item?.product_label || "-")}</td>
        <td><span class="${mappingClass}">${escapeHtml(mappingLabel)}</span></td>
        <td>${escapeHtml(centsToMoney(adsCost))}</td>
        <td>${escapeHtml(NUMBER_FMT.format(Number(item?.order_count || 0)))}</td>
        <td>${escapeHtml(centsToMoney(revenue))}</td>
        <td>${adsCost > 0 ? escapeHtml(itemRoas.toFixed(2) + "x") : "-"}</td>
        <td>${escapeHtml(centsToMoney(item?.profit_before_ads_cents || 0))}</td>
        <td class="${profitClass}">${escapeHtml(centsToMoney(profitAfter))}</td>
      </tr>`;
    }).join("");
    els.googleAdsProductsBody.innerHTML = productRows || "<tr><td colspan=\"8\">Keine Daten fuer den aktuellen Filter.</td></tr>";

    /* Attach click listeners */
    els.googleAdsProductsBody.querySelectorAll(".ga-product-row").forEach((tr) => {
      tr.addEventListener("click", () => {
        const key = tr.getAttribute("data-product-key") || "";
        if (key) _toggleGoogleAdsProductDetail(key, tr);
      });
    });
  }

  /* ── Missing assignments table ── */
  const missing = Array.isArray(payload.missing_assignments) ? payload.missing_assignments : [];
  if (els.googleAdsMissingMeta instanceof HTMLElement) {
    els.googleAdsMissingMeta.textContent = `${NUMBER_FMT.format(missing.length)} Zeilen`;
  }
  if (els.googleAdsMissingBody instanceof HTMLElement) {
    const missingRows = missing.map((item) => {
      return `<tr>
        <td>${escapeHtml(item?.article_id || "-")}</td>
        <td>${escapeHtml(centsToMoney(item?.ads_cost_cents || 0))}</td>
        <td>${escapeHtml(NUMBER_FMT.format(Number(item?.day_count || 0)))}</td>
      </tr>`;
    }).join("");
    els.googleAdsMissingBody.innerHTML = missingRows || "<tr><td colspan=\"3\">Keine fehlenden Zuweisungen.</td></tr>";
  }

  /* ── Destroy old charts ── */
  if (state.googleAdsTrendChart) {
    state.googleAdsTrendChart.destroy();
    state.googleAdsTrendChart = null;
  }
  if (state.googleAdsCumulChart) {
    state.googleAdsCumulChart.destroy();
    state.googleAdsCumulChart = null;
  }
  if (state.googleAdsProfitChart) {
    state.googleAdsProfitChart.destroy();
    state.googleAdsProfitChart = null;
  }
  if (state.googleAdsRoasChart) {
    state.googleAdsRoasChart.destroy();
    state.googleAdsRoasChart = null;
  }

  /* ── Theme colors ── */
  const cs = getComputedStyle(document.documentElement);
  const c1 = cs.getPropertyValue("--th-chart-1").trim();
  const c1f = cs.getPropertyValue("--th-chart-1-fill").trim();
  const c2 = cs.getPropertyValue("--th-chart-2").trim();
  const c2f = cs.getPropertyValue("--th-chart-2-fill").trim();
  const grid = cs.getPropertyValue("--th-chart-grid").trim();
  const label = cs.getPropertyValue("--th-chart-label").trim();
  const okColor = cs.getPropertyValue("--th-ok").trim();
  const warnColor = cs.getPropertyValue("--th-warn").trim();

  /* ── Trend chart ── */
  const trendRows = Array.isArray(payload.trend) ? payload.trend : [];
  if (els.googleAdsTrendSub instanceof HTMLElement) {
    if (!trendRows.length) {
      els.googleAdsTrendSub.textContent = "Keine Trenddaten fuer den aktuellen Filter.";
    } else {
      const fromToken = String(trendRows[0]?.day || "").trim();
      const toToken = String(trendRows[trendRows.length - 1]?.day || "").trim();
      const fromLabel = fromToken ? formatDateTokenLabel(fromToken) : "-";
      const toLabel = toToken ? formatDateTokenLabel(toToken) : "-";
      els.googleAdsTrendSub.textContent = `Gesamt vs. gemappt \u00b7 ${fromLabel} - ${toLabel} \u00b7 ${NUMBER_FMT.format(trendRows.length)} Tage`;
    }
  }

  if (els.googleAdsTrendChartCanvas instanceof HTMLCanvasElement && trendRows.length) {
    const labels = trendRows.map((row) => formatDateTokenLabel(row?.day || ""));
    const totalSeries = trendRows.map((row) => Number(row?.ads_cost_cents || 0) / 100);
    const mappedSeries = trendRows.map((row) => Number(row?.mapped_ads_cost_cents || 0) / 100);
    const pointRadius = trendRows.length > 120 ? 0 : (trendRows.length > 64 ? 1 : 2);

    state.googleAdsTrendChart = new Chart(els.googleAdsTrendChartCanvas, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "Ads gesamt",
            data: totalSeries,
            borderColor: c1,
            backgroundColor: c1f,
            pointRadius,
            pointHoverRadius: 4,
            pointHitRadius: 10,
            borderWidth: 2.1,
            tension: 0.3,
            fill: false,
          },
          {
            label: "Ads gemappt",
            data: mappedSeries,
            borderColor: c2,
            backgroundColor: c2f,
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
              color: label,
              font: { size: 11, weight: "600" },
            },
          },
          tooltip: {
            callbacks: {
              label(context) {
                const l = String(context.dataset?.label || "Wert");
                return `${l}: ${MONEY_FMT.format(Number(context.parsed?.y || 0))}`;
              },
            },
          },
        },
        scales: {
          x: {
            grid: { display: false },
            ticks: { color: label, maxRotation: 0, autoSkip: true, maxTicksLimit: 14 },
          },
          y: {
            grid: { color: grid },
            ticks: {
              color: label,
              callback(value) { return MONEY_FMT.format(value); },
            },
          },
        },
      },
    });
  }

  /* ── Cumulative profitability chart ── */
  if (els.googleAdsCumulSub instanceof HTMLElement) {
    if (!trendRows.length) {
      els.googleAdsCumulSub.textContent = "Keine Daten fuer den aktuellen Filter.";
    } else {
      els.googleAdsCumulSub.textContent = "Kumulierte Ads Kosten vs. kumulierter Gewinn (vor Ads) \u2013 steigt der Abstand, lohnt es sich.";
    }
  }

  if (els.googleAdsCumulChartCanvas instanceof HTMLCanvasElement && trendRows.length) {
    const cumulLabels = trendRows.map((row) => formatDateTokenLabel(row?.day || ""));
    let cumAds = 0;
    let cumRevenue = 0;
    let cumProfit = 0;
    const cumAdsSeries = [];
    const cumRevenueSeries = [];
    const cumProfitSeries = [];
    const cumProfitAfterAdsSeries = [];

    for (const row of trendRows) {
      cumAds += Number(row?.ads_cost_cents || 0) / 100;
      cumRevenue += Number(row?.revenue_cents || 0) / 100;
      cumProfit += Number(row?.profit_cents || 0) / 100;
      cumAdsSeries.push(cumAds);
      cumRevenueSeries.push(cumRevenue);
      cumProfitSeries.push(cumProfit);
      cumProfitAfterAdsSeries.push(cumProfit - cumAds);
    }

    const cumulPointRadius = trendRows.length > 120 ? 0 : (trendRows.length > 64 ? 1 : 2);

    state.googleAdsCumulChart = new Chart(els.googleAdsCumulChartCanvas, {
      type: "line",
      data: {
        labels: cumulLabels,
        datasets: [
          {
            label: "Kum. Umsatz",
            data: cumRevenueSeries,
            borderColor: c2,
            backgroundColor: c2f,
            pointRadius: cumulPointRadius,
            pointHoverRadius: 4,
            pointHitRadius: 10,
            borderWidth: 1.5,
            tension: 0.3,
            fill: false,
            borderDash: [4, 3],
          },
          {
            label: "Kum. Gewinn (vor Ads)",
            data: cumProfitSeries,
            borderColor: c2,
            backgroundColor: c2f,
            pointRadius: cumulPointRadius,
            pointHoverRadius: 4,
            pointHitRadius: 10,
            borderWidth: 2.1,
            tension: 0.3,
            fill: false,
          },
          {
            label: "Kum. Ads Kosten",
            data: cumAdsSeries,
            borderColor: warnColor,
            backgroundColor: warnColor + "22",
            pointRadius: cumulPointRadius,
            pointHoverRadius: 4,
            pointHitRadius: 10,
            borderWidth: 2.1,
            tension: 0.3,
            fill: false,
          },
          {
            label: "Kum. Gewinn nach Ads",
            data: cumProfitAfterAdsSeries,
            borderColor: okColor,
            backgroundColor: okColor + "18",
            pointRadius: cumulPointRadius,
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
              color: label,
              font: { size: 11, weight: "600" },
            },
          },
          tooltip: {
            callbacks: {
              label(context) {
                const l = String(context.dataset?.label || "Wert");
                return `${l}: ${MONEY_FMT.format(Number(context.parsed?.y || 0))}`;
              },
            },
          },
        },
        scales: {
          x: {
            grid: { display: false },
            ticks: { color: label, maxRotation: 0, autoSkip: true, maxTicksLimit: 14 },
          },
          y: {
            grid: { color: grid },
            ticks: {
              color: label,
              callback(value) { return MONEY_FMT.format(value); },
            },
          },
        },
      },
    });
  }

  /* ── Per-product profitability chart (horizontal bar) ── */
  const productsWithAds = products.filter((p) => Number(p?.ads_cost_cents || 0) > 0);
  const topProfitProducts = productsWithAds
    .slice()
    .sort((a, b) => Number(a?.profit_after_ads_cents || 0) - Number(b?.profit_after_ads_cents || 0))
    .slice(-12)
    .reverse();

  if (els.googleAdsProfitChartSub instanceof HTMLElement) {
    els.googleAdsProfitChartSub.textContent = topProfitProducts.length
      ? `Top ${topProfitProducts.length} Produkte mit Ads-Kosten`
      : "Keine Produkte mit Ads-Kosten.";
  }

  if (els.googleAdsProfitChartCanvas instanceof HTMLCanvasElement && topProfitProducts.length) {
    const profitLabels = topProfitProducts.map((p) => truncateLabel(p?.product_label || "-", 28));
    const profitData = topProfitProducts.map((p) => Number(p?.profit_after_ads_cents || 0) / 100);
    const profitColors = profitData.map((v) => v >= 0 ? okColor : warnColor);
    const profitBgColors = profitData.map((v) =>
      v >= 0 ? okColor + "33" : warnColor + "33"
    );

    state.googleAdsProfitChart = new Chart(els.googleAdsProfitChartCanvas, {
      type: "bar",
      data: {
        labels: profitLabels,
        datasets: [{
          label: "Gewinn nach Ads",
          data: profitData,
          backgroundColor: profitBgColors,
          borderColor: profitColors,
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
              label(context) {
                return `Gewinn: ${MONEY_FMT.format(Number(context.parsed?.x || 0))}`;
              },
            },
          },
        },
        scales: {
          x: {
            grid: { color: grid },
            ticks: {
              color: label,
              callback(value) { return MONEY_FMT.format(value); },
            },
          },
          y: {
            grid: { display: false },
            ticks: { color: label, font: { size: 11 } },
          },
        },
      },
    });
  }

  /* ── ROAS per product chart (horizontal bar) ── */
  const topRoasProducts = productsWithAds
    .filter((p) => Number(p?.revenue_total_cents || 0) > 0)
    .map((p) => ({
      label: p?.product_label || "-",
      roas: Number(p?.revenue_total_cents || 0) / Number(p?.ads_cost_cents || 1),
    }))
    .sort((a, b) => b.roas - a.roas)
    .slice(0, 12);

  if (els.googleAdsRoasChartSub instanceof HTMLElement) {
    els.googleAdsRoasChartSub.textContent = topRoasProducts.length
      ? `Top ${topRoasProducts.length} Produkte mit Umsatz und Ads-Kosten`
      : "Keine Produkte mit Umsatz und Ads-Kosten.";
  }

  if (els.googleAdsRoasChartCanvas instanceof HTMLCanvasElement && topRoasProducts.length) {
    const roasLabels = topRoasProducts.map((p) => truncateLabel(p.label, 28));
    const roasData = topRoasProducts.map((p) => Math.round(p.roas * 100) / 100);
    const roasColors = roasData.map((v) => v >= 1 ? okColor : warnColor);
    const roasBgColors = roasData.map((v) => v >= 1 ? okColor + "33" : warnColor + "33");

    state.googleAdsRoasChart = new Chart(els.googleAdsRoasChartCanvas, {
      type: "bar",
      data: {
        labels: roasLabels,
        datasets: [{
          label: "ROAS",
          data: roasData,
          backgroundColor: roasBgColors,
          borderColor: roasColors,
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
              label(context) {
                return `ROAS: ${Number(context.parsed?.x || 0).toFixed(2)}x`;
              },
            },
          },
        },
        scales: {
          x: {
            grid: { color: grid },
            ticks: {
              color: label,
              callback(value) { return value.toFixed(1) + "x"; },
            },
          },
          y: {
            grid: { display: false },
            ticks: { color: label, font: { size: 11 } },
          },
        },
      },
    });
  }
}

/* ── Product Detail Drill-Down ── */

function _collapseGoogleAdsProductDetail() {
  if (state.googleAdsProductDetailChart) {
    state.googleAdsProductDetailChart.destroy();
    state.googleAdsProductDetailChart = null;
  }
  const existing = document.getElementById("gaProductDetailRow");
  if (existing) existing.remove();
  /* Remove active highlight */
  document.querySelectorAll(".ga-product-row.ga-row-active").forEach((el) => el.classList.remove("ga-row-active"));
  state.googleAdsExpandedProductKey = null;
}

function _toggleGoogleAdsProductDetail(productKey, anchorTr) {
  const wasOpen = state.googleAdsExpandedProductKey === productKey;
  _collapseGoogleAdsProductDetail();
  if (wasOpen) return;

  state.googleAdsExpandedProductKey = productKey;
  anchorTr.classList.add("ga-row-active");

  /* Create detail row */
  const detailTr = document.createElement("tr");
  detailTr.id = "gaProductDetailRow";
  detailTr.className = "ga-product-detail-row";
  const detailTd = document.createElement("td");
  detailTd.colSpan = 8;
  detailTd.innerHTML = `
    <div class="ga-product-detail">
      <div class="ga-product-detail-loading">Lade Produktdaten\u2026</div>
      <div class="ga-product-detail-kpis" style="display:none"></div>
      <div class="ga-product-detail-chart-wrap" style="display:none">
        <canvas id="gaProductDetailCanvas"></canvas>
      </div>
    </div>`;
  detailTr.appendChild(detailTd);
  anchorTr.after(detailTr);

  /* Fetch data */
  const params = buildQuery();
  params.set("product_key", productKey);
  fetchJson(`${API_BASE}/google-ads/product-detail?${params.toString()}`)
    .then((data) => _renderGoogleAdsProductDetail(data, detailTd))
    .catch((err) => {
      const loadingEl = detailTd.querySelector(".ga-product-detail-loading");
      if (loadingEl) loadingEl.textContent = `Fehler: ${err.message}`;
    });
}

function _renderGoogleAdsProductDetail(data, containerTd) {
  const loadingEl = containerTd.querySelector(".ga-product-detail-loading");
  if (loadingEl) loadingEl.style.display = "none";

  const kpis = data?.kpis || {};
  const trend = Array.isArray(data?.trend) ? data.trend : [];

  /* ── KPI bar ── */
  const kpiWrap = containerTd.querySelector(".ga-product-detail-kpis");
  if (kpiWrap) {
    const adsCost = Number(kpis.ads_cost_total_cents || 0);
    const revenue = Number(kpis.revenue_total_cents || 0);
    const profitBefore = Number(kpis.profit_before_ads_cents || 0);
    const profitAfter = Number(kpis.profit_after_ads_cents || 0);
    const roas = Number(kpis.roas || 0);
    const orders = Number(kpis.orders_count || 0);
    const costPerOrder = orders > 0 ? Math.round(adsCost / orders) : 0;
    const profitAfterClass = profitAfter < 0 ? "value-neg" : "value-pos";

    kpiWrap.innerHTML = `
      <div class="ga-detail-kpi"><span class="ga-detail-kpi-label">Ads Kosten</span><span class="ga-detail-kpi-value">${centsToMoney(adsCost)}</span></div>
      <div class="ga-detail-kpi"><span class="ga-detail-kpi-label">Umsatz</span><span class="ga-detail-kpi-value">${centsToMoney(revenue)}</span></div>
      <div class="ga-detail-kpi"><span class="ga-detail-kpi-label">Gewinn vor Ads</span><span class="ga-detail-kpi-value">${centsToMoney(profitBefore)}</span></div>
      <div class="ga-detail-kpi"><span class="ga-detail-kpi-label">Gewinn nach Ads</span><span class="ga-detail-kpi-value ${profitAfterClass}">${centsToMoney(profitAfter)}</span></div>
      <div class="ga-detail-kpi"><span class="ga-detail-kpi-label">ROAS</span><span class="ga-detail-kpi-value">${roas.toFixed(2)}x</span></div>
      <div class="ga-detail-kpi"><span class="ga-detail-kpi-label">Orders</span><span class="ga-detail-kpi-value">${NUMBER_FMT.format(orders)}</span></div>
      <div class="ga-detail-kpi"><span class="ga-detail-kpi-label">Kosten/Order</span><span class="ga-detail-kpi-value">${orders > 0 ? centsToMoney(costPerOrder) : "-"}</span></div>
    `;
    kpiWrap.style.display = "";
  }

  /* ── Cumulative chart ── */
  const chartWrap = containerTd.querySelector(".ga-product-detail-chart-wrap");
  const canvas = containerTd.querySelector("#gaProductDetailCanvas");
  if (!chartWrap || !(canvas instanceof HTMLCanvasElement) || !trend.length) return;
  chartWrap.style.display = "";

  const cs = getComputedStyle(document.documentElement);
  const okColor = cs.getPropertyValue("--th-ok").trim();
  const warnColor = cs.getPropertyValue("--th-warn").trim();
  const c2 = cs.getPropertyValue("--th-chart-2").trim();
  const grid = cs.getPropertyValue("--th-chart-grid").trim();
  const labelColor = cs.getPropertyValue("--th-chart-label").trim();

  const labels = trend.map((r) => formatDateTokenLabel(r?.day || ""));
  let cumAds = 0, cumRevenue = 0, cumProfit = 0;
  const cumAdsSeries = [];
  const cumRevenueSeries = [];
  const cumProfitAfterSeries = [];

  for (const row of trend) {
    cumAds += Number(row?.ads_cost_cents || 0) / 100;
    cumRevenue += Number(row?.revenue_cents || 0) / 100;
    cumProfit += Number(row?.profit_cents || 0) / 100;
    cumAdsSeries.push(cumAds);
    cumRevenueSeries.push(cumRevenue);
    cumProfitAfterSeries.push(cumProfit - cumAds);
  }

  const pointRadius = trend.length > 120 ? 0 : (trend.length > 64 ? 1 : 2);

  if (state.googleAdsProductDetailChart) {
    state.googleAdsProductDetailChart.destroy();
    state.googleAdsProductDetailChart = null;
  }

  state.googleAdsProductDetailChart = new Chart(canvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Kum. Umsatz",
          data: cumRevenueSeries,
          borderColor: c2,
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
          data: cumAdsSeries,
          borderColor: warnColor,
          backgroundColor: warnColor + "22",
          pointRadius,
          pointHoverRadius: 4,
          pointHitRadius: 10,
          borderWidth: 2.1,
          tension: 0.3,
          fill: false,
        },
        {
          label: "Kum. Gewinn nach Ads",
          data: cumProfitAfterSeries,
          borderColor: okColor,
          backgroundColor: okColor + "18",
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
            font: { size: 11, weight: "600" },
          },
        },
        tooltip: {
          callbacks: {
            label(context) {
              const l = String(context.dataset?.label || "Wert");
              return `${l}: ${MONEY_FMT.format(Number(context.parsed?.y || 0))}`;
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
          grid: { color: grid },
          ticks: {
            color: labelColor,
            callback(value) { return MONEY_FMT.format(value); },
          },
        },
      },
    },
  });
}

/* ── Truncate label helper ── */
function truncateLabel(text, maxLen) {
  if (!text || text.length <= maxLen) return text || "";
  return text.slice(0, maxLen - 1) + "\u2026";
}

/* ── Upload ── */
async function uploadGoogleAdsCsv() {
  const reportFile = els.googleAdsReportInput?.files && els.googleAdsReportInput.files[0];
  const assignmentFile = els.googleAdsAssignmentInput?.files && els.googleAdsAssignmentInput.files[0];
  if (!reportFile && !assignmentFile) {
    setStatus("Bitte mindestens Report oder Zuweisungs-CSV auswaehlen.", "error");
    return;
  }

  const form = new FormData();
  if (reportFile) {
    form.append("report_file", reportFile);
  }
  if (assignmentFile) {
    form.append("assignment_file", assignmentFile);
  }

  try {
    const payload = await fetchJson(`${API_BASE}/google-ads/upload`, {
      method: "POST",
      body: form,
    });
    if (els.googleAdsReportInput instanceof HTMLInputElement) {
      els.googleAdsReportInput.value = "";
    }
    if (els.googleAdsAssignmentInput instanceof HTMLInputElement) {
      els.googleAdsAssignmentInput.value = "";
    }
    syncGoogleAdsFileLabels();
    await loadGoogleAds();
    renderGoogleAds();
    const reportRows = Number(payload?.report?.rows || 0);
    const assignmentRows = Number(payload?.assignment?.rows || 0);
    const reportDayToken = String(payload?.report?.report_to_day || payload?.report?.last_non_zero_day || "").trim();
    const reportDayText = reportDayToken ? formatDateTokenLabel(reportDayToken) : "-";
    setStatus(
      `Google Ads Import abgeschlossen (Report: ${NUMBER_FMT.format(reportRows)}, Zuweisung: ${NUMBER_FMT.format(assignmentRows)}, letztes Datum: ${reportDayText}).`,
      "ok",
    );
  } catch (error) {
    setStatus(`Google Ads Import fehlgeschlagen: ${error.message}`, "error");
  }
}

async function resetGoogleAdsData() {
  if (!confirm("Alle Google Ads Daten (Kosten + Zuordnungen) unwiderruflich loeschen?")) return;
  try {
    await fetchJson(`${API_BASE}/google-ads/reset`, { method: "DELETE" });
    await loadGoogleAds();
    renderGoogleAds();
    setStatus("Google Ads Daten wurden zurueckgesetzt.", "ok");
  } catch (error) {
    setStatus(`Google Ads Reset fehlgeschlagen: ${error.message}`, "error");
  }
}
