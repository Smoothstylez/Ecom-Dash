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
    els.googleAdsReportFileLabel.textContent = file ? file.name : "Keine Datei ausgewaehlt";
  }
  if (els.googleAdsAssignmentFileLabel instanceof HTMLElement) {
    const file = els.googleAdsAssignmentInput?.files && els.googleAdsAssignmentInput.files[0];
    els.googleAdsAssignmentFileLabel.textContent = file ? file.name : "Keine Datei ausgewaehlt";
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

  if (els.googleAdsImportMeta instanceof HTMLElement) {
    const hasData = Number(kpis.ads_cost_total_cents || 0) > 0 || Number(kpis.products_count || 0) > 0;
    els.googleAdsImportMeta.textContent = hasData ? "Aktiv" : "Keine Daten";
  }

  if (els.googleAdsStatusInfo instanceof HTMLElement) {
    const lines = [
      `Report Datei: <strong>${escapeHtml(reportFilename)}</strong>`,
      `Report Zeitraum: <strong>${escapeHtml(reportRangeLabel)}</strong>`,
      `Letztes Report-Datum: <strong>${escapeHtml(reportLastLabel)}</strong>`,
      `Report Zeilen: <strong>${escapeHtml(NUMBER_FMT.format(reportRows))}</strong> (mit Kosten: ${escapeHtml(NUMBER_FMT.format(reportNonZeroRows))})`,
      `Report Import: <strong>${escapeHtml(reportImportedAt)}</strong>`,
      `Assignment Datei: <strong>${escapeHtml(assignmentFilename)}</strong>`,
      `Assignment Zeilen: <strong>${escapeHtml(NUMBER_FMT.format(assignmentRows))}</strong>`,
      `Assignment Import: <strong>${escapeHtml(assignmentImportedAt)}</strong>`,
    ];
    els.googleAdsStatusInfo.innerHTML = lines.join("<br>");
  }

  if (els.googleAdsKpiCostTotal instanceof HTMLElement) {
    els.googleAdsKpiCostTotal.textContent = centsToMoney(kpis.ads_cost_total_cents || 0);
  }
  if (els.googleAdsKpiCostSplit instanceof HTMLElement) {
    els.googleAdsKpiCostSplit.textContent = `Gemappt ${centsToMoney(kpis.ads_cost_mapped_cents || 0)} | Unmapped ${centsToMoney(kpis.ads_cost_unmapped_cents || 0)}`;
  }
  if (els.googleAdsKpiRevenue instanceof HTMLElement) {
    els.googleAdsKpiRevenue.textContent = centsToMoney(kpis.shopify_revenue_total_cents || 0);
  }
  if (els.googleAdsKpiProfitAfter instanceof HTMLElement) {
    const profitAfter = Number(kpis.profit_after_ads_total_cents || 0);
    els.googleAdsKpiProfitAfter.textContent = centsToMoney(profitAfter);
    els.googleAdsKpiProfitAfter.classList.toggle("value-neg", profitAfter < 0);
    els.googleAdsKpiProfitAfter.classList.toggle("value-pos", profitAfter >= 0);
  }
  if (els.googleAdsKpiProfitBefore instanceof HTMLElement) {
    els.googleAdsKpiProfitBefore.textContent = `Vor Ads: ${centsToMoney(kpis.profit_before_ads_total_cents || 0)}`;
  }
  if (els.googleAdsKpiRoas instanceof HTMLElement) {
    const roas = Number(kpis.roas || 0);
    els.googleAdsKpiRoas.textContent = `${roas.toFixed(2)}x`;
  }
  if (els.googleAdsKpiMissing instanceof HTMLElement) {
    els.googleAdsKpiMissing.textContent = `Fehlende Assignments: ${NUMBER_FMT.format(Number(kpis.missing_assignments_count || 0))}`;
  }

  const products = Array.isArray(payload.products) ? payload.products : [];
  if (els.googleAdsProductsMeta instanceof HTMLElement) {
    els.googleAdsProductsMeta.textContent = `${NUMBER_FMT.format(products.length)} Zeilen`;
  }
  if (els.googleAdsProductsBody instanceof HTMLElement) {
    const productRows = products.map((item) => {
      const profitAfter = Number(item?.profit_after_ads_cents || 0);
      const profitClass = profitAfter < 0 ? "value-neg" : "value-pos";
      const mappingLabel = item?.mapped ? "Gemappt" : "Unmapped";
      const mappingClass = item?.mapped ? "badge badge-invoice" : "badge badge-refund";
      return `<tr>
        <td title="${escapeHtml(item?.product_detail || "-")}">${escapeHtml(item?.product_label || "-")}</td>
        <td><span class="${mappingClass}">${escapeHtml(mappingLabel)}</span></td>
        <td>${escapeHtml(centsToMoney(item?.ads_cost_cents || 0))}</td>
        <td>${escapeHtml(NUMBER_FMT.format(Number(item?.order_count || 0)))}</td>
        <td>${escapeHtml(centsToMoney(item?.revenue_total_cents || 0))}</td>
        <td>${escapeHtml(centsToMoney(item?.profit_before_ads_cents || 0))}</td>
        <td class="${profitClass}">${escapeHtml(centsToMoney(item?.profit_after_ads_cents || 0))}</td>
      </tr>`;
    }).join("");
    els.googleAdsProductsBody.innerHTML = productRows || "<tr><td colspan=\"7\">Keine Daten fuer den aktuellen Filter.</td></tr>";
  }

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
    els.googleAdsMissingBody.innerHTML = missingRows || "<tr><td colspan=\"3\">Keine fehlenden Assignments.</td></tr>";
  }

  if (state.googleAdsTrendChart) {
    state.googleAdsTrendChart.destroy();
    state.googleAdsTrendChart = null;
  }

  const trendRows = Array.isArray(payload.trend) ? payload.trend : [];
  if (els.googleAdsTrendSub instanceof HTMLElement) {
    if (!trendRows.length) {
      els.googleAdsTrendSub.textContent = "Keine Trenddaten fuer den aktuellen Filter.";
    } else {
      const fromToken = String(trendRows[0]?.day || "").trim();
      const toToken = String(trendRows[trendRows.length - 1]?.day || "").trim();
      const fromLabel = fromToken ? formatDateTokenLabel(fromToken) : "-";
      const toLabel = toToken ? formatDateTokenLabel(toToken) : "-";
      els.googleAdsTrendSub.textContent = `Gesamt vs. gemappt · ${fromLabel} - ${toLabel} · ${NUMBER_FMT.format(trendRows.length)} Tage`;
    }
  }

  if (!(els.googleAdsTrendChartCanvas instanceof HTMLCanvasElement) || !trendRows.length) {
    return;
  }

  const labels = trendRows.map((row) => formatDateTokenLabel(row?.day || ""));
  const totalSeries = trendRows.map((row) => Number(row?.ads_cost_cents || 0) / 100);
  const mappedSeries = trendRows.map((row) => Number(row?.mapped_ads_cost_cents || 0) / 100);
  const pointRadius = trendRows.length > 120 ? 0 : (trendRows.length > 64 ? 1 : 2);

  const cs = getComputedStyle(document.documentElement);
  const c1 = cs.getPropertyValue("--th-chart-1").trim();
  const c1f = cs.getPropertyValue("--th-chart-1-fill").trim();
  const c2 = cs.getPropertyValue("--th-chart-2").trim();
  const c2f = cs.getPropertyValue("--th-chart-2-fill").trim();
  const grid = cs.getPropertyValue("--th-chart-grid").trim();
  const label = cs.getPropertyValue("--th-chart-label").trim();

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
            maxTicksLimit: 14,
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

/* ── Upload ── */
async function uploadGoogleAdsCsv() {
  const reportFile = els.googleAdsReportInput?.files && els.googleAdsReportInput.files[0];
  const assignmentFile = els.googleAdsAssignmentInput?.files && els.googleAdsAssignmentInput.files[0];
  if (!reportFile && !assignmentFile) {
    setStatus("Bitte mindestens Report oder Assignment CSV auswaehlen.", "error");
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
      `Google Ads Import abgeschlossen (Report: ${NUMBER_FMT.format(reportRows)}, Assignment: ${NUMBER_FMT.format(assignmentRows)}, letztes Report-Datum: ${reportDayText}).`,
      "ok",
    );
  } catch (error) {
    setStatus(`Google Ads Import fehlgeschlagen: ${error.message}`, "error");
  }
}
