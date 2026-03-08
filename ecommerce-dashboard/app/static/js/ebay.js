"use strict";

/* ── eBay Legacy Tab ── */

/* ── Load ── */
async function loadEbay() {
  const [summaryRes, ordersRes] = await Promise.all([
    fetchJson(`${API_BASE}/ebay/summary`),
    fetchJson(`${API_BASE}/ebay/orders`),
  ]);
  state.ebaySummary = summaryRes;
  state.ebayOrders = ordersRes.orders || [];
  state.ebayOrdersTotal = ordersRes.total || 0;
}

/* ── Category labels ── */
const EBAY_CATEGORY_LABELS = {
  order: "Bestellung",
  computer: "Computer",
  return: "Ruecksendung",
};

/* ── Render ── */
function renderEbay() {
  const summary = state.ebaySummary || {};
  const kpis = summary.kpis || {};
  const shops = summary.shops || [];
  const topArticles = summary.top_articles || [];
  const importMeta = summary.import_meta || {};

  /* KPIs */
  if (els.ebayKpiOrders) {
    els.ebayKpiOrders.textContent = NUMBER_FMT.format(kpis.total_orders || 0);
  }
  if (els.ebayKpiOrdersSub) {
    const returns = kpis.total_returns || 0;
    const dateRange = kpis.first_date && kpis.last_date
      ? `${formatDateTokenLabel(kpis.first_date)} - ${formatDateTokenLabel(kpis.last_date)}`
      : "";
    els.ebayKpiOrdersSub.textContent = returns > 0
      ? `${returns} Ruecksendungen | ${dateRange}`
      : dateRange || "Legacy eBay Daten";
  }
  if (els.ebayKpiRevenue) {
    els.ebayKpiRevenue.textContent = MONEY_FMT.format(kpis.total_revenue || 0);
  }
  if (els.ebayKpiCosts) {
    const costs = (kpis.total_purchase || 0) + (kpis.total_fees || 0);
    els.ebayKpiCosts.textContent = MONEY_FMT.format(costs);
  }
  if (els.ebayKpiCostsSub) {
    els.ebayKpiCostsSub.textContent = `Einkauf ${MONEY_FMT.format(kpis.total_purchase || 0)} + Gebuehren ${MONEY_FMT.format(kpis.total_fees || 0)}`;
  }
  if (els.ebayKpiProfit) {
    els.ebayKpiProfit.textContent = MONEY_FMT.format(kpis.total_profit || 0);
  }
  if (els.ebayKpiProfitSub) {
    els.ebayKpiProfitSub.textContent = `Marge: ${NUMBER_FMT.format(kpis.margin_pct || 0)}%`;
  }

  /* Shops table */
  if (els.ebayShopsBody) {
    els.ebayShopsBody.innerHTML = shops.length
      ? shops.map(s => {
          const dateRange = s.first_date && s.last_date
            ? `${formatDateTokenLabel(s.first_date)} - ${formatDateTokenLabel(s.last_date)}`
            : "-";
          return `<tr>
            <td><strong>${escapeHtml(s.shop)}</strong></td>
            <td>${s.count}</td>
            <td>${escapeHtml(dateRange)}</td>
            <td>${MONEY_FMT.format(s.revenue)}</td>
            <td>${MONEY_FMT.format(s.fees)}</td>
            <td>${MONEY_FMT.format(s.purchase)}</td>
            <td class="${s.profit >= 0 ? 'profit-positive' : 'profit-negative'}">${MONEY_FMT.format(s.profit)}</td>
          </tr>`;
        }).join("")
      : '<tr><td colspan="7">Keine eBay Daten importiert.</td></tr>';
  }
  if (els.ebayShopsMeta) {
    els.ebayShopsMeta.textContent = `${shops.length} Shop${shops.length !== 1 ? "s" : ""}`;
  }

  /* Populate shop filter dropdown */
  if (els.ebayShopFilter) {
    const currentVal = els.ebayShopFilter.value;
    const shopNames = [...new Set((state.ebayOrders || []).map(o => o.shop))].sort();
    let optionsHtml = '<option value="">Alle Shops</option>';
    for (const name of shopNames) {
      const sel = name === currentVal ? " selected" : "";
      optionsHtml += `<option value="${escapeHtml(name)}"${sel}>${escapeHtml(name)}</option>`;
    }
    els.ebayShopFilter.innerHTML = optionsHtml;
  }

  /* Orders table (filtered) */
  renderEbayOrders();

  /* Top articles */
  if (els.ebayTopArticlesBody) {
    els.ebayTopArticlesBody.innerHTML = topArticles.length
      ? topArticles.map(a => `<tr>
          <td>${escapeHtml(a.artikel || "-")}</td>
          <td>${a.count}</td>
          <td>${MONEY_FMT.format(a.revenue)}</td>
          <td class="${a.profit >= 0 ? 'profit-positive' : 'profit-negative'}">${MONEY_FMT.format(a.profit)}</td>
        </tr>`).join("")
      : '<tr><td colspan="4">Keine Daten.</td></tr>';
  }

  /* Import info */
  if (els.ebayImportInfo) {
    if (importMeta && importMeta.imported_at) {
      els.ebayImportInfo.innerHTML = [
        `Quelle: <strong>${escapeHtml(importMeta.source_file || "-")}</strong>`,
        `Importiert: <strong>${formatDate(importMeta.imported_at)}</strong>`,
        `Shops: <strong>${escapeHtml(importMeta.shops || "-")}</strong>`,
        `Bestellungen: <strong>${importMeta.total_orders || 0}</strong> | Ruecksendungen: <strong>${importMeta.total_returns || 0}</strong>`,
        '<span style="opacity:.6">Hinweis: eBay Legacy-Daten — kein aktiver Verkauf, nur Dokumentation.</span>',
      ].join("<br>");
    } else {
      els.ebayImportInfo.textContent = "Keine eBay Daten importiert. Bitte import_ebay.py ausfuehren.";
    }
  }
}

function renderEbayOrders() {
  const allOrders = state.ebayOrders || [];
  const shopFilter = els.ebayShopFilter ? els.ebayShopFilter.value : "";
  const categoryFilter = els.ebayCategoryFilter ? els.ebayCategoryFilter.value : "";

  const filtered = allOrders.filter(o => {
    if (shopFilter && o.shop !== shopFilter) return false;
    if (categoryFilter && o.category !== categoryFilter) return false;
    return true;
  });

  if (els.ebayOrdersMeta) {
    els.ebayOrdersMeta.textContent = `${filtered.length} Zeilen`;
  }

  if (els.ebayOrdersBody) {
    els.ebayOrdersBody.innerHTML = filtered.length
      ? filtered.map(o => {
          const isReturn = o.is_return === 1;
          const rowClass = isReturn ? ' class="return-row"' : "";
          const catLabel = EBAY_CATEGORY_LABELS[o.category] || o.category || "-";
          const gewinn = o.gewinn != null ? o.gewinn : 0;
          return `<tr${rowClass}>
            <td data-label="Datum">${o.datum ? formatDateTokenLabel(o.datum) : "-"}</td>
            <td data-label="Shop">${escapeHtml(o.shop || "-")}</td>
            <td data-label="Kategorie"><span class="badge badge-${isReturn ? 'cancel' : o.category === 'computer' ? 'partial' : 'ok'}">${escapeHtml(catLabel)}</span></td>
            <td data-label="Artikel">${escapeHtml(o.artikel || "-")}</td>
            <td data-label="Kunde">${escapeHtml(o.kunde_name || "-")}</td>
            <td data-label="Order Nr.">${escapeHtml(o.order_number || "-")}</td>
            <td data-label="Preis">${o.preis != null ? MONEY_FMT.format(o.preis) : "-"}</td>
            <td data-label="Gebuehren">${MONEY_FMT.format(o.gebuehren || 0)}</td>
            <td data-label="Einkauf">${o.ali_preis != null ? MONEY_FMT.format(o.ali_preis) : "-"}</td>
            <td data-label="Gewinn" class="${gewinn >= 0 ? 'profit-positive' : 'profit-negative'}">${MONEY_FMT.format(gewinn)}</td>
          </tr>`;
        }).join("")
      : '<tr><td colspan="10">Keine eBay Bestellungen fuer aktuellen Filter.</td></tr>';
  }
}

/* formatDateTokenLabel is provided by core.js */
