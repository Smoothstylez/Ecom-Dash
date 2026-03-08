"use strict";

/* ── Customers Tab ── */

/* ── Globe Theme Colors ── */

function applyGlobeThemeColors() {
  if (!state.customerGlobe) return;

  const cs = getComputedStyle(document.documentElement);
  const globeBg = cs.getPropertyValue("--th-globe-bg").trim();
  const globeAtmo = cs.getPropertyValue("--th-globe-atmosphere").trim();
  const globeSurface = cs.getPropertyValue("--th-globe-surface").trim();
  const globeEmissive = cs.getPropertyValue("--th-globe-emissive").trim();
  const hexLandColor = cs.getPropertyValue("--th-globe-hex-land").trim();
  const hexTopR = parseInt(cs.getPropertyValue("--th-globe-hex-top-r").trim(), 10) || 42;
  const hexTopG = parseInt(cs.getPropertyValue("--th-globe-hex-top-g").trim(), 10) || 108;
  const hexTopB = parseInt(cs.getPropertyValue("--th-globe-hex-top-b").trim(), 10) || 202;
  const hexSideR = parseInt(cs.getPropertyValue("--th-globe-hex-side-r").trim(), 10) || 32;
  const hexSideG = parseInt(cs.getPropertyValue("--th-globe-hex-side-g").trim(), 10) || 88;
  const hexSideB = parseInt(cs.getPropertyValue("--th-globe-hex-side-b").trim(), 10) || 176;

  if (globeBg && typeof state.customerGlobe.backgroundColor === "function") {
    state.customerGlobe.backgroundColor(globeBg);
  }
  if (globeAtmo && typeof state.customerGlobe.atmosphereColor === "function") {
    state.customerGlobe.atmosphereColor(globeAtmo);
  }
  if (typeof state.customerGlobe.hexTopColor === "function") {
    state.customerGlobe.hexTopColor((hex) => {
      const weight = Number(hex?.sumWeight || 0);
      const alpha = Math.min(0.95, 0.38 + (weight / 20));
      return `rgba(${hexTopR}, ${hexTopG}, ${hexTopB}, ${alpha.toFixed(3)})`;
    });
  }
  if (typeof state.customerGlobe.hexSideColor === "function") {
    state.customerGlobe.hexSideColor((hex) => {
      const weight = Number(hex?.sumWeight || 0);
      const alpha = Math.min(0.96, 0.4 + (weight / 18));
      return `rgba(${hexSideR}, ${hexSideG}, ${hexSideB}, ${alpha.toFixed(3)})`;
    });
  }
  if (hexLandColor && typeof state.customerGlobe.hexPolygonColor === "function") {
    state.customerGlobe.hexPolygonColor(() => hexLandColor);
  }
  if (typeof window.THREE !== "undefined" && typeof state.customerGlobe.globeMaterial === "function") {
    const material = state.customerGlobe.globeMaterial();
    if (material) {
      if (globeSurface) material.color = new window.THREE.Color(globeSurface);
      if (globeEmissive) material.emissive = new window.THREE.Color(globeEmissive);
    }
  }
}

/* ── Load & Geo Helpers ── */

async function loadCustomers() {
  const params = buildQuery();
  params.set("limit", "2000");
  const payload = await fetchJson(`${API_BASE}/customers?${params.toString()}`);
  state.customersPayload = payload;
  state.customersNeedsReload = false;
}

async function loadCustomerGeoLocations() {
  const startedAt = performance.now();
  appendCustomerGeoLog("Start: /api/customers/locations");
  const params = buildQuery();
  const payload = await fetchJson(`${API_BASE}/customers/locations?${params.toString()}`);
  state.customerGeoPayload = payload;
  state.customerGeoNeedsReload = false;
  state.customerGeoLastSummary = payload?.summary && typeof payload.summary === "object" ? payload.summary : null;
  state.customerGeoLastLoadMs = Math.max(0, Math.round(performance.now() - startedAt));
  const summary = state.customerGeoLastSummary || {};
  appendCustomerGeoLog(
    `Fertig: ${NUMBER_FMT.format(Number(summary.points_total || 0))} Punkte / ${NUMBER_FMT.format(Number(summary.orders_total || 0))} Orders (${NUMBER_FMT.format(state.customerGeoLastLoadMs)} ms)`
  );
  renderCustomerGeoStatusInfo();
}

function appendCustomerGeoLog(message) {
  const timestamp = new Date();
  const hh = String(timestamp.getHours()).padStart(2, "0");
  const mm = String(timestamp.getMinutes()).padStart(2, "0");
  const ss = String(timestamp.getSeconds()).padStart(2, "0");
  state.customerGeoLog.unshift(`${hh}:${mm}:${ss} ${String(message || "").trim()}`);
  state.customerGeoLog = state.customerGeoLog.slice(0, 6);
}

function renderCustomerGeoStatusInfo() {
  if (!(els.customerGeoStatusInfo instanceof HTMLElement)) {
    return;
  }
  const summary = state.customerGeoLastSummary && typeof state.customerGeoLastSummary === "object"
    ? state.customerGeoLastSummary
    : (state.customerGeoPayload && typeof state.customerGeoPayload.summary === "object" ? state.customerGeoPayload.summary : {});

  const lines = [
    `Orders: <strong>${escapeHtml(NUMBER_FMT.format(Number(summary?.orders_total || 0)))}</strong> · Punkte: <strong>${escapeHtml(NUMBER_FMT.format(Number(summary?.points_total || 0)))}</strong>`,
    `Quelle Koordinaten: <strong>${escapeHtml(NUMBER_FMT.format(Number(summary?.resolved_source_coordinates_count || 0)))}</strong> · Geocoded: <strong>${escapeHtml(NUMBER_FMT.format(Number(summary?.resolved_geocoded_count || 0)))}</strong> · Country-Fallback: <strong>${escapeHtml(NUMBER_FMT.format(Number(summary?.resolved_country_centroid_count || 0)))}</strong>`,
    `Unaufgeloest: <strong>${escapeHtml(NUMBER_FMT.format(Number(summary?.unresolved_orders_count || 0)))}</strong> · Geocode Lauf: <strong>${escapeHtml(NUMBER_FMT.format(Number(summary?.geocode_successes || 0)))}</strong>/${escapeHtml(NUMBER_FMT.format(Number(summary?.geocode_attempts || 0)))} · Geo-Cache Orte: <strong>${escapeHtml(NUMBER_FMT.format(Number(summary?.cache_location_hits || 0)))}</strong>`,
    `API: <strong>${summary?.cache_hit ? "Cache" : "Frisch"}</strong> · Server: <strong>${escapeHtml(NUMBER_FMT.format(Number(summary?.generated_in_ms || 0)))} ms</strong> · Browser: <strong>${escapeHtml(NUMBER_FMT.format(Number(state.customerGeoLastLoadMs || 0)))} ms</strong>`,
  ];
  if (Array.isArray(state.customerGeoLog) && state.customerGeoLog.length) {
    const recent = state.customerGeoLog.slice(0, 3).map((entry) => escapeHtml(entry)).join(" | ");
    lines.push(`Log: ${recent}`);
  }

  els.customerGeoStatusInfo.innerHTML = lines.join("<br>");
}

function setCustomerGeoLoading(loading, text = "Lade Kundendaten...") {
  if (els.customerGeoLoadingText instanceof HTMLElement) {
    els.customerGeoLoadingText.textContent = text;
  }
  if (els.customerGeoLoadingOverlay instanceof HTMLElement) {
    els.customerGeoLoadingOverlay.classList.toggle("active", Boolean(loading));
    els.customerGeoLoadingOverlay.setAttribute("aria-hidden", loading ? "false" : "true");
  }
  if (loading && text) {
    const latest = Array.isArray(state.customerGeoLog) && state.customerGeoLog.length ? String(state.customerGeoLog[0]) : "";
    if (!latest.includes(text)) {
      appendCustomerGeoLog(text);
    }
  }
  renderCustomerGeoStatusInfo();
}

async function ensureCustomersDataLoaded(withStatus = false) {
  const needsCustomers = state.customersPayload === null || state.customersNeedsReload;
  const needsGeo = state.customerGeoPayload === null || state.customerGeoNeedsReload;
  if (!needsCustomers && !needsGeo) {
    return;
  }

  if (state.customersLoadingPromise) {
    await state.customersLoadingPromise;
    return;
  }

  const task = (async () => {
    try {
      setCustomerGeoLoading(true, "Kundendaten werden geladen...");
      await new Promise((resolve) => window.requestAnimationFrame(() => resolve()));
      if (needsCustomers) {
        await loadCustomers();
      }
      setCustomerGeoLoading(true, "Ortsdaten werden vorbereitet...");
      await new Promise((resolve) => window.requestAnimationFrame(() => resolve()));
      if (needsGeo) {
        await loadCustomerGeoLocations();
      }
    } finally {
      setCustomerGeoLoading(false);
    }
  })();

  state.customersLoadingPromise = task;
  try {
    await task;
    rerender();
    if (withStatus) {
      setStatus("Kundenansicht aktualisiert.", "ok");
    }
  } catch (error) {
    setStatus(`Kundenansicht konnte nicht geladen werden: ${error.message}`, "error");
  } finally {
    state.customersLoadingPromise = null;
  }
}

/* ── Render Functions ── */

function renderCustomers() {
  const payload = state.customersPayload || {};
  const kpis = payload.kpis && typeof payload.kpis === "object" ? payload.kpis : {};
  const total = Number(payload.total || 0);
  const items = Array.isArray(payload.items) ? payload.items : [];

  if (els.customersKpiCount instanceof HTMLElement) {
    els.customersKpiCount.textContent = NUMBER_FMT.format(Number(kpis.customers_count || total));
  }
  if (els.customersKpiRepeat instanceof HTMLElement) {
    els.customersKpiRepeat.textContent = NUMBER_FMT.format(Number(kpis.repeat_customers_count || 0));
  }
  if (els.customersKpiRepeatSub instanceof HTMLElement) {
    els.customersKpiRepeatSub.textContent = formatPercent(Number(kpis.repeat_customers_rate_pct || 0));
  }
  if (els.customersKpiOrdersPer instanceof HTMLElement) {
    const value = Number(kpis.avg_orders_per_customer || 0);
    els.customersKpiOrdersPer.textContent = value.toLocaleString("de-DE", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }
  if (els.customersKpiOrdersTotal instanceof HTMLElement) {
    els.customersKpiOrdersTotal.textContent = `Orders gesamt: ${NUMBER_FMT.format(Number(kpis.orders_total_count || 0))}`;
  }
  if (els.customersKpiRevenuePer instanceof HTMLElement) {
    els.customersKpiRevenuePer.textContent = centsToMoney(Number(kpis.avg_revenue_per_customer_cents || 0));
  }
  if (els.customersKpiRevenueTotal instanceof HTMLElement) {
    els.customersKpiRevenueTotal.textContent = `Umsatz gesamt: ${centsToMoney(Number(kpis.revenue_total_cents || 0))}`;
  }

  const customersCount = Math.max(Number(kpis.customers_count || total), 0);
  const coverageEmail = Number(kpis.with_email_count || 0);
  const coveragePhone = Number(kpis.with_phone_count || 0);
  const coverageAddress = Number(kpis.with_address_count || 0);
  const coverageCross = Number(kpis.cross_market_customers_count || 0);

  if (els.customersCoverageEmail instanceof HTMLElement) {
    els.customersCoverageEmail.textContent = `${NUMBER_FMT.format(coverageEmail)} (${formatPercent((coverageEmail / Math.max(customersCount, 1)) * 100)})`;
  }
  if (els.customersCoveragePhone instanceof HTMLElement) {
    els.customersCoveragePhone.textContent = `${NUMBER_FMT.format(coveragePhone)} (${formatPercent((coveragePhone / Math.max(customersCount, 1)) * 100)})`;
  }
  if (els.customersCoverageAddress instanceof HTMLElement) {
    els.customersCoverageAddress.textContent = `${NUMBER_FMT.format(coverageAddress)} (${formatPercent((coverageAddress / Math.max(customersCount, 1)) * 100)})`;
  }
  if (els.customersCoverageCross instanceof HTMLElement) {
    els.customersCoverageCross.textContent = `${NUMBER_FMT.format(coverageCross)} (${formatPercent((coverageCross / Math.max(customersCount, 1)) * 100)})`;
  }

  if (els.customersMarketplaceList instanceof HTMLElement) {
    const shopifyCount = Number(kpis.shopify_customers_count || 0);
    const kauflandCount = Number(kpis.kaufland_customers_count || 0);
    const rows = [
      { label: "Shopify", count: shopifyCount },
      { label: "Kaufland", count: kauflandCount },
    ].map((item) => {
      const share = customersCount > 0 ? (item.count / customersCount) * 100 : 0;
      return `<div class="payment-method-row">
        <span class="payment-method-name">${escapeHtml(item.label)}</span>
        <span class="payment-method-count">${escapeHtml(NUMBER_FMT.format(item.count))}</span>
        <span class="payment-method-share">${escapeHtml(formatPercent(share))}</span>
      </div>`;
    }).join("");
    els.customersMarketplaceList.innerHTML = rows;
  }

  if (els.customersMeta instanceof HTMLElement) {
    els.customersMeta.textContent = `${NUMBER_FMT.format(total)} Zeilen`;
  }

  if (els.customersBody instanceof HTMLElement) {
    const rows = items.map((item) => {
      const address = item?.primary_address && typeof item.primary_address === "object" ? item.primary_address : {};
      const emails = Array.isArray(item?.emails) ? item.emails : [];
      const phones = Array.isArray(item?.phones) ? item.phones : [];
      const markets = Array.isArray(item?.marketplaces) ? item.marketplaces : [];
      const topArticles = Array.isArray(item?.top_articles) ? item.top_articles : [];
      const repeat = Boolean(item?.repeat_customer);
      const profit = Number(item?.profit_total_cents || 0);
      const profitClass = profit < 0 ? "value-neg" : "value-pos";

      const contactLines = [];
      if (emails.length) {
        contactLines.push(`<div class="customer-contact-line">${escapeHtml(emails[0])}</div>`);
      }
      if (phones.length) {
        contactLines.push(`<div class="customer-contact-line">${escapeHtml(phones[0])}</div>`);
      }

      const addressLines = [];
      const street = String(address?.street || "").trim();
      const cityLine = [String(address?.postcode || "").trim(), String(address?.city || "").trim()].filter(Boolean).join(" ");
      const country = String(address?.country || "").trim();
      if (street) {
        addressLines.push(`<div class="customer-addr-line">${escapeHtml(street)}</div>`);
      }
      if (cityLine) {
        addressLines.push(`<div class="customer-addr-line">${escapeHtml(cityLine)}</div>`);
      }
      if (country) {
        addressLines.push(`<div class="customer-addr-line">${escapeHtml(country)}</div>`);
      }

      const marketBadges = markets.map((market) => {
        const token = String(market || "").trim().toLowerCase();
        const badgeClass = token === "kaufland" ? "badge badge-sale" : "badge badge-invoice";
        return `<span class="${badgeClass}">${escapeHtml(token || "-")}</span>`;
      }).join("");

      const articleLines = topArticles.map((article) => `<div>${escapeHtml(article || "-")}</div>`).join("");

      return `<tr>
        <td>
          <div class="customer-name-main">${escapeHtml(item?.customer_name || "Unbekannt")}</div>
          <div class="cell-sub">${escapeHtml(item?.customer_id || "-")}</div>
        </td>
        <td><div class="customer-contact-lines">${contactLines.join("") || "-"}</div></td>
        <td><div class="customer-addr-lines">${addressLines.join("") || "-"}</div></td>
        <td><div class="customer-market-badges">${marketBadges || "-"}</div></td>
        <td>${escapeHtml(NUMBER_FMT.format(Number(item?.order_count || 0)))}</td>
        <td><span class="badge ${repeat ? "badge-sale" : "badge-default"}">${escapeHtml(repeat ? "Ja" : "Nein")}</span></td>
        <td>${escapeHtml(centsToMoney(item?.revenue_total_cents || 0))}</td>
        <td class="${profitClass}">${escapeHtml(centsToMoney(item?.profit_total_cents || 0))}</td>
        <td>${escapeHtml(formatDate(item?.last_order_date || ""))}</td>
        <td><div class="customer-top-articles">${articleLines || "-"}</div></td>
      </tr>`;
    }).join("");
    els.customersBody.innerHTML = rows || "<tr><td colspan=\"10\">Keine Kunden fuer den aktuellen Filter.</td></tr>";
  }
}

function normalizeCustomerGeoMode(value) {
  const token = String(value || "").trim().toLowerCase();
  return token === "globe" ? "globe" : "map";
}

function setCustomerGeoMode(mode, options = {}) {
  const resolved = normalizeCustomerGeoMode(mode);
  state.customerGeoMode = resolved;

  const mapActive = resolved === "map";
  if (els.customerGeoModeMapBtn instanceof HTMLElement) {
    els.customerGeoModeMapBtn.classList.toggle("active", mapActive);
  }
  if (els.customerGeoModeGlobeBtn instanceof HTMLElement) {
    els.customerGeoModeGlobeBtn.classList.toggle("active", !mapActive);
  }
  if (els.customerGeoMapView instanceof HTMLElement) {
    els.customerGeoMapView.classList.toggle("active", mapActive);
  }
  if (els.customerGeoGlobeView instanceof HTMLElement) {
    els.customerGeoGlobeView.classList.toggle("active", !mapActive);
  }

  /* Pause/resume globe render loop based on mode */
  if (mapActive) {
    pauseCustomerGlobe();
  } else if (state.activeTab === "customers") {
    resumeCustomerGlobe();
  }

  if (options && options.skipRender) {
    return;
  }

  if (resolved === "globe" && !state.customersLoadingPromise) {
    setCustomerGeoLoading(true, "Hex-Globus wird aufgebaut...");
    window.setTimeout(() => {
      try {
        renderCustomerGeo();
      } finally {
        setCustomerGeoLoading(false);
      }
    }, 16);
    return;
  }

  renderCustomerGeo();
}

function renderCustomerGeo() {
  const payload = state.customerGeoPayload || {};
  const summary = payload.summary && typeof payload.summary === "object" ? payload.summary : {};
  const points = Array.isArray(payload.points) ? payload.points : [];
  const resolved = Number(summary.orders_total || 0) - Number(summary.unresolved_orders_count || 0);
  const unresolved = Number(summary.unresolved_orders_count || 0);
  const rangeText = state.filters.from && state.filters.to
    ? `${formatDateTokenLabel(state.filters.from)} - ${formatDateTokenLabel(state.filters.to)}`
    : "Aktueller Filter";

  if (els.customerGeoSub instanceof HTMLElement) {
    els.customerGeoSub.textContent = `Punkte: ${NUMBER_FMT.format(points.length)} · Orders geolokalisiert: ${NUMBER_FMT.format(Math.max(resolved, 0))} · Unaufgeloest: ${NUMBER_FMT.format(Math.max(unresolved, 0))} · ${rangeText}`;
  }

  if (state.activeTab !== "customers") {
    return;
  }

  if (state.customerGeoMode === "globe") {
    renderCustomerGeoGlobe(points);
    return;
  }
  renderCustomerGeoMap(points);
}

function renderCustomerGeoMap(points) {
  if (!(els.customerGeoMapView instanceof HTMLElement) || typeof window.L === "undefined") {
    return;
  }

  if (!points.length) {
    if (state.customerLeafletMap && typeof state.customerLeafletMap.remove === "function") {
      state.customerLeafletMap.remove();
    }
    state.customerLeafletMap = null;
    state.customerLeafletLayer = null;
    state.customerLeafletAutoFitted = false;
    els.customerGeoMapView.innerHTML = '<div class="customer-geo-empty">Keine Ortsdaten fuer den aktuellen Filter.</div>';
    return;
  }

  if (els.customerGeoMapView.querySelector(".customer-geo-empty")) {
    els.customerGeoMapView.innerHTML = "";
  }

  if (!state.customerLeafletMap) {
    state.customerLeafletMap = L.map(els.customerGeoMapView, {
      zoomControl: true,
      worldCopyJump: true,
      minZoom: 1,
      maxZoom: 18,
    });

    L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
      subdomains: "abcd",
      maxZoom: 18,
      attribution: "&copy; <a href='https://www.openstreetmap.org/copyright'>OpenStreetMap</a> &copy; <a href='https://carto.com/'>CARTO</a>",
    }).addTo(state.customerLeafletMap);

    state.customerLeafletLayer = L.layerGroup().addTo(state.customerLeafletMap);
    state.customerLeafletMap.setView([20, 10], 2);
    state.customerLeafletAutoFitted = false;
  }

  if (state.customerLeafletLayer) {
    state.customerLeafletLayer.clearLayers();
  }

  const bounds = [];
  points.forEach((point) => {
    const lat = Number(point?.lat);
    const lng = Number(point?.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
      return;
    }
    const dominant = String(point?.dominant_marketplace || "").trim().toLowerCase();
    const color = dominant === "kaufland" ? "#1f8b5f" : "#2d5ea8";
    const orderCount = Number(point?.order_count || 0);
    const radius = Math.max(4, Math.min(22, 4 + Math.sqrt(Math.max(orderCount, 0)) * 1.8));
    const marker = L.circleMarker([lat, lng], {
      radius,
      color,
      weight: 1.4,
      fillColor: color,
      fillOpacity: 0.35,
    });

    const city = String(point?.city || "").trim();
    const country = String(point?.country || point?.country_code || "").trim();
    const label = [city, country].filter(Boolean).join(", ") || "Unbekannt";
    marker.bindPopup(
      `<strong>${escapeHtml(label)}</strong><br>` +
      `Orders: ${escapeHtml(NUMBER_FMT.format(orderCount))}<br>` +
      `Umsatz: ${escapeHtml(centsToMoney(point?.revenue_total_cents || 0))}<br>` +
      `Gewinn: ${escapeHtml(centsToMoney(point?.profit_total_cents || 0))}`
    );

    if (state.customerLeafletLayer) {
      marker.addTo(state.customerLeafletLayer);
    }
    bounds.push([lat, lng]);
  });

  if (state.customerLeafletMap) {
    state.customerLeafletMap.invalidateSize();
    if (bounds.length && !state.customerLeafletAutoFitted) {
      state.customerLeafletMap.fitBounds(bounds, { padding: [26, 26], maxZoom: 5 });
      state.customerLeafletAutoFitted = true;
    }
  }
}

function createCustomerGlobeInstance(container) {
  if (!(container instanceof HTMLElement)) {
    return null;
  }
  if (typeof window.Globe !== "function") {
    return null;
  }

  var w = container.clientWidth;
  var h = container.clientHeight;
  if (!w || !h) {
    throw new Error("Container hat keine Abmessungen (" + w + "x" + h + "). Bitte sicherstellen, dass das Element sichtbar ist.");
  }

  /* Attempt 1: full-quality renderer */
  try {
    var globe = new window.Globe(container, {
      rendererConfig: { antialias: true, alpha: true, powerPreference: "high-performance", failIfMajorPerformanceCaveat: false },
      waitForGlobeReady: false,
      animateIn: false
    });
    appendCustomerGeoLog("Globe erstellt (Full-Quality Renderer).");
    return globe;
  } catch (firstErr) {
    appendCustomerGeoLog("Full-Quality fehlgeschlagen: " + (firstErr instanceof Error ? firstErr.message : String(firstErr)) + " — versuche Fallback...");
    /* Clean up any leftover canvas from failed attempt */
    try {
      var leftover = container.querySelector("canvas");
      if (leftover) { container.removeChild(leftover); }
    } catch (_) { /* ignore */ }
  }

  /* Attempt 2: minimal renderer (no antialias, no alpha, default GPU) */
  try {
    var globe2 = new window.Globe(container, {
      rendererConfig: { antialias: false, alpha: false, powerPreference: "default", failIfMajorPerformanceCaveat: false },
      waitForGlobeReady: false,
      animateIn: false
    });
    appendCustomerGeoLog("Globe erstellt (Fallback Renderer, reduzierte Qualitaet).");
    return globe2;
  } catch (secondErr) {
    var msg2 = secondErr instanceof Error ? secondErr.message : String(secondErr);
    appendCustomerGeoLog("Fallback-Renderer ebenfalls fehlgeschlagen: " + msg2);
    /* Clean up any leftover canvas */
    try {
      var leftover2 = container.querySelector("canvas");
      if (leftover2) { container.removeChild(leftover2); }
    } catch (_) { /* ignore */ }
  }

  /* Both attempts failed — throw with diagnostic guidance */
  throw new Error(
    "WebGL-Kontext konnte nicht erstellt werden. "
    + "Moegliche Loesungen: (1) chrome://settings > System > Hardwarebeschleunigung aktivieren, "
    + "(2) chrome://flags/#ignore-gpu-blocklist auf Enabled setzen, "
    + "(3) chrome://flags/#use-angle auf OpenGL oder D3D11on12 setzen, "
    + "(4) Chrome komplett neu starten (alle Tabs schliessen)."
  );
}

function destroyCustomerGlobe() {
  if (!state.customerGlobe) {
    state.customerGlobeBaseLayerReady = false;
    return;
  }
  try {
    if (typeof state.customerGlobe.renderer === "function") {
      var renderer = state.customerGlobe.renderer();
      if (renderer) {
        renderer.dispose();
        renderer.forceContextLoss();
        if (renderer.domElement && renderer.domElement.parentNode) {
          renderer.domElement.parentNode.removeChild(renderer.domElement);
        }
      }
    }
    if (typeof state.customerGlobe._destructor === "function") {
      state.customerGlobe._destructor();
    }
  } catch (cleanupErr) {
    /* ignore cleanup errors */
  }
  state.customerGlobe = null;
  state.customerGlobeBaseLayerReady = false;
}

/**
 * Pause the globe's Three.js render loop to save GPU cycles
 * when the globe is not visible (tab switch or map mode).
 */
function pauseCustomerGlobe() {
  if (!state.customerGlobe) return;
  try {
    if (typeof state.customerGlobe.renderer === "function") {
      var r = state.customerGlobe.renderer();
      if (r && typeof r.setAnimationLoop === "function") {
        r.setAnimationLoop(null);
      }
    }
    if (typeof state.customerGlobe.controls === "function") {
      var c = state.customerGlobe.controls();
      if (c) c.enabled = false;
    }
  } catch (_) { /* ignore */ }
}

/**
 * Resume the globe's render loop when it becomes visible again.
 */
function resumeCustomerGlobe() {
  if (!state.customerGlobe) return;
  try {
    if (typeof state.customerGlobe.renderer === "function") {
      var r = state.customerGlobe.renderer();
      if (r && typeof r.setAnimationLoop === "function") {
        /* globe.gl's internal tick — re-kick the loop by passing its own render fn */
        var scene = typeof state.customerGlobe.scene === "function" ? state.customerGlobe.scene() : null;
        var camera = typeof state.customerGlobe.camera === "function" ? state.customerGlobe.camera() : null;
        if (scene && camera) {
          r.setAnimationLoop(function () { r.render(scene, camera); });
        }
      }
    }
    if (typeof state.customerGlobe.controls === "function") {
      var c = state.customerGlobe.controls();
      if (c) {
        c.enabled = true;
        if (typeof c.update === "function") c.update();
      }
    }
  } catch (_) { /* ignore */ }
}

function applyCustomerGlobeBaseLayer(features) {
  if (!state.customerGlobe || !Array.isArray(features) || !features.length) {
    return;
  }

  const cs = getComputedStyle(document.documentElement);
  const hexLandColor = cs.getPropertyValue("--th-globe-hex-land").trim();

  try {
    if (typeof state.customerGlobe.hexPolygonsData === "function") {
      state.customerGlobe.hexPolygonsData(features);
    }
    if (typeof state.customerGlobe.hexPolygonResolution === "function") {
      state.customerGlobe.hexPolygonResolution(3);
    }
    if (typeof state.customerGlobe.hexPolygonMargin === "function") {
      state.customerGlobe.hexPolygonMargin(0.22);
    }
    if (typeof state.customerGlobe.hexPolygonAltitude === "function") {
      state.customerGlobe.hexPolygonAltitude(0.0036);
    }
    if (typeof state.customerGlobe.hexPolygonColor === "function") {
      state.customerGlobe.hexPolygonColor(() => hexLandColor || "rgba(89, 101, 122, 0.42)");
    }
    if (typeof state.customerGlobe.hexPolygonUseDots === "function") {
      state.customerGlobe.hexPolygonUseDots(false);
    }
    state.customerGlobeBaseLayerReady = true;
    appendCustomerGeoLog("Hex-Basislayer aktiv (Kontinente).");
    renderCustomerGeoStatusInfo();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error || "Basislayer Fehler");
    appendCustomerGeoLog(`Hex-Basislayer Fehler: ${message}`);
    renderCustomerGeoStatusInfo();
  }
}

function ensureCustomerGlobeBaseLayer() {
  if (!state.customerGlobe) {
    return;
  }

  if (state.customerGlobeBaseLayerReady) {
    return;
  }

  if (Array.isArray(state.customerGlobeWorldFeatures) && state.customerGlobeWorldFeatures.length) {
    applyCustomerGlobeBaseLayer(state.customerGlobeWorldFeatures);
    return;
  }

  if (state.customerGlobeWorldLoadPromise) {
    return;
  }

  appendCustomerGeoLog("Kontinent-Hexlayer wird geladen...");
  state.customerGlobeWorldLoadPromise = fetch("https://unpkg.com/world-atlas@2/countries-110m.json")
    .then((response) => {
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      return response.json();
    })
    .then((atlas) => {
      const topo = window.topojson;
      if (!topo || typeof topo.feature !== "function") {
        throw new Error("topojson-client fehlt");
      }
      const objectKey = atlas?.objects?.countries ? "countries" : Object.keys(atlas?.objects || {})[0];
      if (!objectKey) {
        throw new Error("Keine Country-Geometrie im Atlas");
      }
      const collection = topo.feature(atlas, atlas.objects[objectKey]);
      const features = Array.isArray(collection?.features) ? collection.features : [];
      state.customerGlobeWorldFeatures = features;
      applyCustomerGlobeBaseLayer(features);
    })
    .catch((error) => {
      const message = error instanceof Error ? error.message : String(error || "Unbekannter Fehler");
      appendCustomerGeoLog(`Kontinent-Hexlayer fehlgeschlagen: ${message}`);
      renderCustomerGeoStatusInfo();
    })
    .finally(() => {
      state.customerGlobeWorldLoadPromise = null;
    });
}

function renderCustomerGeoGlobe(points) {
  if (!(els.customerGeoGlobeView instanceof HTMLElement)) {
    return;
  }

  if (typeof window.Globe === "undefined") {
    els.customerGeoGlobeView.innerHTML = '<div class="customer-geo-empty">Hex-Globus konnte nicht geladen werden (CDN evtl. blockiert).</div>';
    destroyCustomerGlobe();
    appendCustomerGeoLog("Hex-Globus Script fehlt (window.Globe).");
    renderCustomerGeoStatusInfo();
    return;
  }

  if (!(window.WebGLRenderingContext)) {
    els.customerGeoGlobeView.innerHTML = '<div class="customer-geo-empty">WebGL ist im Browser deaktiviert. Bitte Hardwarebeschleunigung aktivieren.</div>';
    destroyCustomerGlobe();
    appendCustomerGeoLog("Hex-Globus: WebGL nicht verfuegbar.");
    renderCustomerGeoStatusInfo();
    return;
  }

  if (!points.length) {
    els.customerGeoGlobeView.innerHTML = '<div class="customer-geo-empty">Keine Ortsdaten fuer den aktuellen Filter.</div>';
    destroyCustomerGlobe();
    appendCustomerGeoLog("Hex-Globus: keine Punkte.");
    renderCustomerGeoStatusInfo();
    return;
  }

  if (els.customerGeoGlobeView.querySelector(".customer-geo-empty")) {
    els.customerGeoGlobeView.innerHTML = "";
  }

  try {
    if (!state.customerGlobe) {
      appendCustomerGeoLog("Hex-Globus wird initialisiert...");
      state.customerGlobe = createCustomerGlobeInstance(els.customerGeoGlobeView);
      if (!state.customerGlobe) {
        throw new Error("Globe-Instanz konnte nicht erstellt werden.");
      }

      state.customerGlobeBaseLayerReady = false;

      const cs = getComputedStyle(document.documentElement);
      const globeBg = cs.getPropertyValue("--th-globe-bg").trim() || "#e9edf3";
      const globeAtmo = cs.getPropertyValue("--th-globe-atmosphere").trim() || "#9db3d0";
      const globeSurface = cs.getPropertyValue("--th-globe-surface").trim() || "#b8c1ce";
      const globeEmissive = cs.getPropertyValue("--th-globe-emissive").trim() || "#4f5a6b";
      const hexTopR = parseInt(cs.getPropertyValue("--th-globe-hex-top-r").trim(), 10) || 42;
      const hexTopG = parseInt(cs.getPropertyValue("--th-globe-hex-top-g").trim(), 10) || 108;
      const hexTopB = parseInt(cs.getPropertyValue("--th-globe-hex-top-b").trim(), 10) || 202;
      const hexSideR = parseInt(cs.getPropertyValue("--th-globe-hex-side-r").trim(), 10) || 32;
      const hexSideG = parseInt(cs.getPropertyValue("--th-globe-hex-side-g").trim(), 10) || 88;
      const hexSideB = parseInt(cs.getPropertyValue("--th-globe-hex-side-b").trim(), 10) || 176;

      if (typeof state.customerGlobe.backgroundColor === "function") {
        state.customerGlobe.backgroundColor(globeBg);
      }
      if (typeof state.customerGlobe.showGlobe === "function") {
        state.customerGlobe.showGlobe(false);
      }
      if (typeof state.customerGlobe.showAtmosphere === "function") {
        state.customerGlobe.showAtmosphere(false);
      }
      if (typeof state.customerGlobe.atmosphereColor === "function") {
        state.customerGlobe.atmosphereColor(globeAtmo);
      }
      if (typeof state.customerGlobe.atmosphereAltitude === "function") {
        state.customerGlobe.atmosphereAltitude(0.12);
      }
      if (typeof state.customerGlobe.showGraticules === "function") {
        state.customerGlobe.showGraticules(true);
      }
      if (typeof state.customerGlobe.hexBinResolution === "function") {
        state.customerGlobe.hexBinResolution(3);
      }
      if (typeof state.customerGlobe.hexMargin === "function") {
        state.customerGlobe.hexMargin(0.22);
      }
      if (typeof state.customerGlobe.hexTopCurvatureResolution === "function") {
        state.customerGlobe.hexTopCurvatureResolution(4);
      }
      if (typeof state.customerGlobe.hexTopColor === "function") {
        state.customerGlobe.hexTopColor((hex) => {
          const weight = Number(hex?.sumWeight || 0);
          const alpha = Math.min(0.95, 0.38 + (weight / 20));
          return `rgba(${hexTopR}, ${hexTopG}, ${hexTopB}, ${alpha.toFixed(3)})`;
        });
      }
      if (typeof state.customerGlobe.hexSideColor === "function") {
        state.customerGlobe.hexSideColor((hex) => {
          const weight = Number(hex?.sumWeight || 0);
          const alpha = Math.min(0.96, 0.4 + (weight / 18));
          return `rgba(${hexSideR}, ${hexSideG}, ${hexSideB}, ${alpha.toFixed(3)})`;
        });
      }
      if (typeof state.customerGlobe.hexAltitude === "function") {
        state.customerGlobe.hexAltitude((hex) => {
          const weight = Number(hex?.sumWeight || 0);
          return Math.min(0.24, Math.max(0.01, weight * 0.0075));
        });
      }
      if (typeof state.customerGlobe.hexLabel === "function") {
        state.customerGlobe.hexLabel((hex) => {
          const weight = Number(hex?.sumWeight || 0);
          const lat = Number(hex?.center?.lat || 0);
          const lng = Number(hex?.center?.lng || 0);
          return `Orders: ${NUMBER_FMT.format(weight)}<br>Lat/Lng: ${lat.toFixed(2)}, ${lng.toFixed(2)}`;
        });
      }

      if (typeof window.THREE !== "undefined" && typeof state.customerGlobe.globeMaterial === "function") {
        const material = state.customerGlobe.globeMaterial();
        if (material) {
          material.color = new window.THREE.Color(globeSurface);
          material.emissive = new window.THREE.Color(globeEmissive);
          material.emissiveIntensity = 0.24;
          material.shininess = 0.45;
        }
      }

      if (typeof state.customerGlobe.pointOfView === "function") {
        state.customerGlobe.pointOfView({ lat: 20, lng: 10, altitude: 2.2 }, 0);
      }
      if (state.customerGlobe.controls && typeof state.customerGlobe.controls === "function") {
        const controls = state.customerGlobe.controls();
        controls.autoRotate = false;
        controls.enablePan = false;
        controls.enableDamping = true;
        controls.dampingFactor = 0.08;
        controls.minDistance = 110;
        controls.maxDistance = 520;
      }
    }

    ensureCustomerGlobeBaseLayer();

    const globePoints = points.map((point) => ({
      lat: Number(point?.lat || 0),
      lng: Number(point?.lng || 0),
      weight: Number(point?.weight || point?.order_count || 0),
    })).filter((point) => Number.isFinite(point.lat) && Number.isFinite(point.lng) && Number.isFinite(point.weight));

    if (typeof state.customerGlobe.width === "function") {
      state.customerGlobe.width(els.customerGeoGlobeView.clientWidth || 640);
    }
    if (typeof state.customerGlobe.height === "function") {
      state.customerGlobe.height(els.customerGeoGlobeView.clientHeight || 420);
    }
    if (typeof state.customerGlobe.hexBinPointsData !== "function") {
      throw new Error("hexBinPointsData API nicht verfuegbar.");
    }
    state.customerGlobe.hexBinPointsData(globePoints);
    if (typeof state.customerGlobe.hexBinPointLat === "function") {
      state.customerGlobe.hexBinPointLat((d) => d.lat);
    }
    if (typeof state.customerGlobe.hexBinPointLng === "function") {
      state.customerGlobe.hexBinPointLng((d) => d.lng);
    }
    if (typeof state.customerGlobe.hexBinPointWeight === "function") {
      state.customerGlobe.hexBinPointWeight((d) => d.weight);
    }

    appendCustomerGeoLog(`Hex-Globus aktiv (${NUMBER_FMT.format(globePoints.length)} Punkte).`);
    renderCustomerGeoStatusInfo();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error || "Unbekannter Fehler");
    destroyCustomerGlobe();
    appendCustomerGeoLog(`Hex-Globus Fehler: ${message}`);

    /* Auto-fallback to Leaflet map if WebGL context creation failed */
    if (!state.customerGlobeWebGLUnavailable) {
      state.customerGlobeWebGLUnavailable = true;
      appendCustomerGeoLog("WebGL nicht verfuegbar — wechsle automatisch zur Kartenansicht.");
      /* Disable the globe toggle button */
      if (els.customerGeoModeGlobeBtn instanceof HTMLElement) {
        els.customerGeoModeGlobeBtn.disabled = true;
        els.customerGeoModeGlobeBtn.title = "Hex-Globus nicht verfuegbar (WebGL wird vom Browser blockiert)";
      }
      setCustomerGeoMode("map");
      return;
    }

    els.customerGeoGlobeView.innerHTML = `<div class="customer-geo-empty">Hex-Globus Fehler: ${escapeHtml(message)}</div>`;
    renderCustomerGeoStatusInfo();
  }
}
