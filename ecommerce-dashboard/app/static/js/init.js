"use strict";

/* ── Init & Event Binding ── */

/* ── Health Check ── */

async function loadHealth() {
  const health = await fetchJson(`${API_BASE}/health`);
  const runtime = health?.sync_status?.runtime || {};
  const bootstrap = health?.sync_status?.bootstrap_sources || {};
  const liveProviders = health?.live_sync_status?.providers || {};
  const liveBackground = health?.live_sync_status?.background || {};
  const bookkeepingModule = health?.bookkeeping_module || {};
  const shopifyFlag = runtime?.shopify_db?.exists ? "OK" : "MISSING";
  const kauflandFlag = runtime?.kaufland_db?.exists ? "OK" : "MISSING";
  const bookkeepingFlag = runtime?.bookkeeping_db?.exists ? "OK" : "MISSING";
  const shopifyBootstrap = bootstrap?.shopify_db?.exists ? "OK" : "MISSING";
  const kauflandBootstrap = bootstrap?.kaufland_db?.exists ? "OK" : "MISSING";
  const bookkeepingBootstrap = bootstrap?.bookkeeping_db?.exists ? "OK" : "MISSING";
  const shopifyLiveReady = liveProviders?.shopify?.configured ? "READY" : "MISSING ENV";
  const kauflandLiveReady = liveProviders?.kaufland?.configured ? "READY" : "MISSING ENV";
  const autoLiveState = liveBackground?.enabled
    ? (liveBackground?.thread_alive ? "RUNNING" : "STOPPED")
    : "DISABLED";
  const autoLiveInterval = Number(liveBackground?.interval_seconds || 0);
  const autoLiveMode = String(liveBackground?.last_mode || "-").toUpperCase();
  const autoLiveStatus = String(liveBackground?.last_status || "never_started").toUpperCase();
  const cycleCount = Number(liveBackground?.cycle_count || 0);
  const nextReconcileIn = Number(liveBackground?.next_reconcile_in_cycles || 0);
  const moduleStatus = String(bookkeepingModule?.mode || "integrated").toUpperCase();
  const lastSyncAt = String(liveBackground?.last_success_at || liveBackground?.last_finished_at || "").trim();

  // --- Build last-sync provider details ---
  const lastResult = liveBackground?.last_live_result || {};
  const providers = lastResult?.providers || {};
  const providerLines = [];
  for (const [name, info] of Object.entries(providers)) {
    if (!info || info.status === "skipped") continue;
    const seen = Number(info.orders_seen || 0);
    const ins = Number(info.orders_inserted || 0);
    const upd = Number(info.orders_updated || 0);
    const unch = Number(info.orders_unchanged || 0);
    const dur = info.duration_seconds != null ? `${Number(info.duration_seconds).toFixed(1)}s` : "-";
    const filter = info.updated_at_min || info.ts_created_from_iso || null;
    const filterLabel = filter ? ` ab ${formatDate(filter, "-")}` : "";
    providerLines.push(
      `&nbsp;&nbsp;${name}: ${seen} gesehen, ${ins} neu, ${upd} geändert, ${unch} unverändert (${dur}${filterLabel})`
    );
  }

  if (els.sourceInfo instanceof HTMLElement) {
    els.sourceInfo.innerHTML = [
      `Local Shopify DB: <strong>${shopifyFlag}</strong>`,
      `Local Kaufland DB: <strong>${kauflandFlag}</strong>`,
      `Local Buchungen DB: <strong>${bookkeepingFlag}</strong>`,
      `Bootstrap Shopify: <strong>${shopifyBootstrap}</strong>`,
      `Bootstrap Kaufland: <strong>${kauflandBootstrap}</strong>`,
      `Bootstrap Buchungen: <strong>${bookkeepingBootstrap}</strong>`,
      `Live Shopify: <strong>${shopifyLiveReady}</strong>`,
      `Live Kaufland: <strong>${kauflandLiveReady}</strong>`,
      `Auto Live Sync: <strong>${autoLiveState}</strong> (alle ${NUMBER_FMT.format(autoLiveInterval)}s)`,
      `Auto Live Last: <strong>${autoLiveStatus}</strong> [${autoLiveMode}] — Zyklus ${cycleCount}`,
      ...(nextReconcileIn > 0 ? [`Nächster Reconcile: in ${nextReconcileIn} Zyklen`] : []),
      ...(providerLines.length > 0 ? ["Letzte Ergebnisse:", ...providerLines] : []),
      `Buchungen Modus: <strong>${moduleStatus}</strong>`
    ].join("<br>");
  }
  if (lastSyncAt) {
    const modeLabel = autoLiveMode === "DELTA" ? "Delta" : autoLiveMode === "RECONCILE" ? "Reconcile" : autoLiveMode;
    setLastSyncInfo(`Letzter Sync: ${formatDate(lastSyncAt, "-")} (${autoLiveStatus}, ${modeLabel})`);
  }
}

/* ── Navigation & Refresh ── */

function setActiveTab(tab) {
  state.activeTab = tab;
  const analyticsActive = tab === "analytics";
  const ordersActive = tab === "orders";
  const customersActive = tab === "customers";
  const bookingsActive = tab === "bookings";
  const googleAdsActive = tab === "googleads";
  const ebayActive = tab === "ebay";

  els.tabAnalyticsBtn.classList.toggle("active", analyticsActive);
  els.tabOrdersBtn.classList.toggle("active", ordersActive);
  els.tabCustomersBtn.classList.toggle("active", customersActive);
  els.tabBookingsBtn.classList.toggle("active", bookingsActive);
  els.tabGoogleAdsBtn.classList.toggle("active", googleAdsActive);
  els.tabEbayBtn.classList.toggle("active", ebayActive);

  els.bookingsSubnav.classList.toggle("visible", bookingsActive);

  els.analyticsPanel.classList.toggle("active", analyticsActive);
  els.ordersPanel.classList.toggle("active", ordersActive);
  els.customersPanel.classList.toggle("active", customersActive);
  els.bookingsPanel.classList.toggle("active", bookingsActive);
  els.googleAdsPanel.classList.toggle("active", googleAdsActive);
  els.ebayPanel.classList.toggle("active", ebayActive);

  if (bookingsActive) {
    setBookingsSubtab(state.bookingsSubtab);
  }

  /* Pause globe render loop when leaving customers tab */
  if (!customersActive) {
    pauseCustomerGlobe();
  }

  if (customersActive) {
    setCustomerGeoMode(state.customerGeoMode, { skipRender: true });
    const needsCustomers = state.customersPayload === null || state.customersNeedsReload;
    const needsGeo = state.customerGeoPayload === null || state.customerGeoNeedsReload;
    if (needsCustomers || needsGeo) {
      setCustomerGeoLoading(true, "Kundenkarte wird geladen...");
      ensureCustomersDataLoaded(false);
    }
    window.setTimeout(() => {
      renderCustomerGeo();
      if (state.customerLeafletMap) {
        state.customerLeafletMap.invalidateSize();
      }
      if (state.customerGlobe && els.customerGeoGlobeView instanceof HTMLElement) {
        if (typeof state.customerGlobe.width === "function") {
          state.customerGlobe.width(els.customerGeoGlobeView.clientWidth || 640);
        }
        if (typeof state.customerGlobe.height === "function") {
          state.customerGlobe.height(els.customerGeoGlobeView.clientHeight || 420);
        }
      }
      /* Resume globe render loop if globe mode is active */
      if (state.customerGeoMode !== "map") {
        resumeCustomerGlobe();
      }
    }, 0);
  }

  window.requestAnimationFrame(() => {
    if (typeof syncOrdersTableViewportHeight === "function") {
      syncOrdersTableViewportHeight();
    }
  });
}

function syncSingleTableWrapViewportHeight(panel, tableWrap) {
  if (!(panel instanceof HTMLElement) || !(tableWrap instanceof HTMLElement)) {
    return;
  }
  const mainWrapper = document.querySelector(".main-content-wrapper");
  if (!panel.classList.contains("active")) {
    tableWrap.style.removeProperty("height");
    tableWrap.style.removeProperty("max-height");
    return;
  }

  const rootStyle = getComputedStyle(document.documentElement);
  const pageGap = parseFloat(rootStyle.getPropertyValue("--page-v-pad")) || 24;
  const bottomGap = pageGap + 2;
  const rect = tableWrap.getBoundingClientRect();
  const wrapperBottom = mainWrapper instanceof HTMLElement
    ? mainWrapper.getBoundingClientRect().bottom
    : window.innerHeight;
  const sidebarBottom = els.mainSidebar instanceof HTMLElement
    ? els.mainSidebar.getBoundingClientRect().bottom
    : window.innerHeight;
  const targetBottom = Math.min(wrapperBottom, sidebarBottom);
  const availableHeight = Math.max(320, Math.floor(targetBottom - rect.top - 2));
  tableWrap.style.height = `${availableHeight}px`;
  tableWrap.style.maxHeight = `${availableHeight}px`;
}

function syncOrdersTableViewportHeight() {
  if (els.ordersPanel instanceof HTMLElement) {
    syncSingleTableWrapViewportHeight(
      els.ordersPanel,
      els.ordersPanel.querySelector(".table-wrap")
    );
  }

  if (els.bookingsTransactionsPanel instanceof HTMLElement && els.bookingsBody instanceof HTMLElement) {
    syncSingleTableWrapViewportHeight(
      els.bookingsTransactionsPanel,
      els.bookingsBody.closest(".table-wrap")
    );
  }

  if (els.bookingsOrdersPanel instanceof HTMLElement && els.bookingOrdersBody instanceof HTMLElement) {
    syncSingleTableWrapViewportHeight(
      els.bookingsOrdersPanel,
      els.bookingOrdersBody.closest(".table-wrap")
    );
  }
}

function normalizeTab(value) {
  const token = String(value || "").trim().toLowerCase();
  if (token === "orders" || token === "bookings" || token === "analytics" || token === "googleads" || token === "google-ads" || token === "customers" || token === "ebay") {
    if (token === "google-ads") {
      return "googleads";
    }
    return token;
  }
  return "analytics";
}

function normalizeBookingsSubtab(value) {
  const token = String(value || "").trim().toLowerCase();
  if (token === "orders" || token === "transactions" || token === "templates" || token === "accounts" || token === "documents") {
    return token;
  }
  return "transactions";
}

function applyInitialViewFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const path = String(window.location.pathname || "").toLowerCase();
  let initialTab = normalizeTab(params.get("tab"));
  state.bookingsFullView = path === "/bookings/full" || params.get("full") === "1";

  if (path === "/bookings" || state.bookingsFullView) {
    initialTab = "bookings";
  } else if (path === "/orders") {
    initialTab = "orders";
  } else if (path === "/customers") {
    initialTab = "customers";
  } else if (path === "/google-ads") {
    initialTab = "googleads";
  } else if (path === "/ebay") {
    initialTab = "ebay";
  } else if (path === "/analytics") {
    initialTab = "analytics";
  }

  state.bookingsSubtab = normalizeBookingsSubtab(params.get("subtab"));
  document.body.classList.toggle("bookings-full", state.bookingsFullView);
  setActiveTab(initialTab);
}

function clearScheduledFilterRefresh() {
  if (filterRefreshTimerId) {
    window.clearTimeout(filterRefreshTimerId);
    filterRefreshTimerId = 0;
  }
}

function scheduleFilterRefresh(delayMs = 320) {
  const delay = Number.isFinite(Number(delayMs)) ? Math.max(0, Number(delayMs)) : 0;
  clearScheduledFilterRefresh();
  filterRefreshTimerId = window.setTimeout(() => {
    filterRefreshTimerId = 0;
    refreshFilterData();
  }, delay);
}

async function refreshFilterData() {
  if (filterRefreshRunning) {
    filterRefreshQueued = true;
    return;
  }
  filterRefreshRunning = true;
  try {
    const shouldLoadCustomers = state.activeTab === "customers";
    const tasks = [
      loadOrders(),
      loadAnalytics(),
      loadGoogleAds(),
      loadBookings(),
      loadBookingOrders(),
    ];

    if (shouldLoadCustomers) {
      setCustomerGeoLoading(true, "Kundenorte werden aktualisiert...");
      tasks.push(loadCustomers(), loadCustomerGeoLocations());
    } else {
      state.customersNeedsReload = true;
      state.customerGeoNeedsReload = true;
    }

    await Promise.all(tasks);
    rerender();
  } catch (error) {
    setStatus(`Filteraktualisierung fehlgeschlagen: ${error.message}`, "error");
  } finally {
    setCustomerGeoLoading(false);
    filterRefreshRunning = false;
    if (filterRefreshQueued) {
      filterRefreshQueued = false;
      refreshFilterData();
    }
  }
}

async function refreshBookingsTransactionsOnly(withStatus = false) {
  try {
    const tasks = [loadBookings()];
    if (state.bookingClass === "monthly") {
      tasks.push(loadMonthlyInvoices());
    }
    await Promise.all(tasks);
    refreshBookingFormOptions();
    renderBookings();
    if (state.bookingClass === "monthly") {
      renderMonthlyInvoices();
    }
    if (withStatus) {
      setStatus("Transaktionsfilter aktualisiert.", "ok");
    }
  } catch (error) {
    setStatus(`Transaktionsfilter fehlgeschlagen: ${error.message}`, "error");
  }
}

let refreshAllRunning = false;

async function refreshAll() {
  if (refreshAllRunning) { return; }
  refreshAllRunning = true;
  clearScheduledFilterRefresh();
  setStatus("Lade Daten...", "info");
  try {
    const shouldLoadCustomers = state.activeTab === "customers";
    const tasks = [
      loadHealth(),
      loadOrders(),
      loadAnalytics(),
      loadGoogleAds(),
      loadBookings(),
      loadBookingOrders(),
      loadBookkeepingLedgerOrders(),
      loadBookingAccounts(),
      loadBookingTemplates(),
      loadBookingDocuments(),
      loadMonthlyInvoices(),
      loadEbay(),
    ];

    if (shouldLoadCustomers) {
      setCustomerGeoLoading(true, "Kundenorte werden geladen...");
      tasks.push(loadCustomers(), loadCustomerGeoLocations());
    } else {
      state.customersNeedsReload = true;
      state.customerGeoNeedsReload = true;
    }

    await Promise.all(tasks);
    rerender();
    if (!shouldLoadCustomers) {
      window.setTimeout(() => {
        if (state.customersNeedsReload || state.customerGeoNeedsReload) {
          appendCustomerGeoLog("Background-Prefetch gestartet.");
          ensureCustomersDataLoaded(false);
        }
      }, 650);
    }
    setStatus("Daten aktualisiert.", "ok");
  } catch (error) {
    setStatus(`Fehler beim Laden: ${error.message}`, "error");
  } finally {
    setCustomerGeoLoading(false);
    refreshAllRunning = false;
  }
}

async function runSourceSync() {
  setStatus("Synchronisiere lokale Datenquellen...", "info");
  try {
    const payload = await fetchJson(`${API_BASE}/sync/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ force: false, include_documents: true }),
    });
    const results = payload && typeof payload.results === "object" ? payload.results : {};
    const copiedDbs = [
      results.shopify_db?.copied,
      results.kaufland_db?.copied,
      results.bookkeeping_db?.copied,
    ].filter(Boolean).length;
    const copiedDocs = Number(results.bookkeeping_documents?.copied_files || 0);
    const failedSources = Object.entries(results)
      .filter(([, value]) => {
        if (!value || typeof value !== "object") {
          return false;
        }
        const status = String(value.status || "").toLowerCase();
        return status === "error";
      })
      .map(([key, value]) => {
        const reason = value && typeof value === "object" ? String(value.reason || "") : "";
        return reason ? `${key}: ${reason}` : key;
      });

    await refreshAll();

    if (failedSources.length) {
      setStatus(
        `Sync teilweise fehlgeschlagen (${failedSources.join(" | ")}).`,
        "error",
      );
      setLastSyncInfo(`Letzter Sync: Fehler/Teilfehler (${formatDate(new Date().toISOString(), "-")})`);
      return;
    }

    setStatus(
      `Sync fertig: ${copiedDbs} DB(s) aktualisiert, ${NUMBER_FMT.format(copiedDocs)} Dokument(e) kopiert.`,
      "ok",
    );
    setLastSyncInfo(`Letzter Sync: Quellen-Sync (${formatDate(new Date().toISOString(), "-")})`);
  } catch (error) {
    setStatus(`Sync fehlgeschlagen: ${error.message}`, "error");
    setLastSyncInfo(`Letzter Sync: Quellen-Sync fehlgeschlagen (${formatDate(new Date().toISOString(), "-")})`);
  }
}

async function runLiveSync() {
  setStatus("Starte Live API Sync fuer Shopify/Kaufland...", "info");
  try {
    const payload = await fetchJson(`${API_BASE}/sync/live/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        shopify: true,
        kaufland: true,
        shopify_status: "any",
        shopify_page_limit: 250,
        shopify_max_pages: 500,
        kaufland_storefront: "de",
        kaufland_page_limit: 100,
        kaufland_max_pages: 5000,
      }),
    });

    const shopifyStatus = String(payload?.results?.shopify?.status || "unknown");
    const kauflandStatus = String(payload?.results?.kaufland?.status || "unknown");
    const shopifySummary = payload?.results?.shopify?.summary || {};
    const kauflandSummary = payload?.results?.kaufland?.summary || {};
    const shopifyDetail = shopifySummary.orders_seen != null
      ? ` (${shopifySummary.orders_seen} gesehen, ${shopifySummary.orders_inserted || 0} neu, ${shopifySummary.orders_updated || 0} geändert)`
      : "";
    const kauflandDetail = kauflandSummary.orders_seen != null
      ? ` (${kauflandSummary.orders_seen} gesehen, ${kauflandSummary.orders_inserted || 0} neu, ${kauflandSummary.orders_updated || 0} geändert)`
      : "";
    await refreshAll();
    setStatus(`Live Sync fertig: Shopify=${shopifyStatus}${shopifyDetail}, Kaufland=${kauflandStatus}${kauflandDetail}.`, "ok");
    setLastSyncInfo(`Letzter Sync: Live API (${formatDate(new Date().toISOString(), "-")})`);
  } catch (error) {
    setStatus(`Live Sync fehlgeschlagen: ${error.message}`, "error");
    setLastSyncInfo(`Letzter Sync: Live API fehlgeschlagen (${formatDate(new Date().toISOString(), "-")})`);
  }
}

/* ── Datenverwaltung Modal ── */

function closeSettingsPanel() {
  const panel = document.getElementById("settingsPanel");
  if (panel instanceof HTMLElement) {
    panel.classList.remove("active");
  }
  if (els.sidebarSettingsBtn instanceof HTMLElement) {
    els.sidebarSettingsBtn.classList.remove("active");
  }
}

function openDataModal() {
  if (!(els.dataModal instanceof HTMLElement)) return;
  closeSettingsPanel();
  resetRestoreUI();
  els.dataModal.classList.add("active");
  els.dataModal.setAttribute("aria-hidden", "false");
}

function closeDataModal() {
  if (!(els.dataModal instanceof HTMLElement)) return;
  els.dataModal.classList.remove("active");
  els.dataModal.setAttribute("aria-hidden", "true");
}

function resetRestoreUI() {
  if (els.restoreFileInput instanceof HTMLInputElement) {
    els.restoreFileInput.value = "";
  }
  if (els.restoreFileLabel) {
    els.restoreFileLabel.textContent = "ZIP-Datei waehlen...";
  }
  if (els.restoreConfirmSection) {
    els.restoreConfirmSection.style.display = "none";
  }
  if (els.restoreProgress) {
    els.restoreProgress.style.display = "none";
  }
  if (els.restoreResult) {
    els.restoreResult.style.display = "none";
    els.restoreResult.className = "data-restore-result";
    els.restoreResult.textContent = "";
  }
}

function formatFileSize(bytes) {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  return (bytes / (1024 * 1024)).toFixed(1) + " MB";
}

function onRestoreFileSelected() {
  const input = els.restoreFileInput;
  if (!(input instanceof HTMLInputElement) || !input.files || input.files.length === 0) return;

  const file = input.files[0];
  if (!file.name.toLowerCase().endsWith(".zip")) {
    setStatus("Nur ZIP-Dateien werden akzeptiert.", "error");
    resetRestoreUI();
    return;
  }

  if (els.restoreFileLabel) {
    els.restoreFileLabel.textContent = file.name;
  }

  if (els.restoreFileInfo) {
    els.restoreFileInfo.textContent = `Datei: ${file.name} (${formatFileSize(file.size)})`;
  }

  if (els.restoreConfirmSection) {
    els.restoreConfirmSection.style.display = "";
  }
  if (els.restoreResult) {
    els.restoreResult.style.display = "none";
  }
}

async function executeRestore() {
  const input = els.restoreFileInput;
  if (!(input instanceof HTMLInputElement) || !input.files || input.files.length === 0) {
    setStatus("Keine Datei ausgewaehlt.", "error");
    return;
  }

  const file = input.files[0];

  // Hide confirm, show progress
  if (els.restoreConfirmSection) {
    els.restoreConfirmSection.style.display = "none";
  }
  if (els.restoreProgress) {
    els.restoreProgress.style.display = "flex";
  }
  if (els.restoreResult) {
    els.restoreResult.style.display = "none";
  }

  try {
    const formData = new FormData();
    formData.append("file", file);

    const response = await fetch(`${API_BASE}/exports/restore`, {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      let errorMsg = `HTTP ${response.status}`;
      try { const errBody = await response.json(); errorMsg = errBody.detail || errorMsg; } catch (e) { /* ignore */ }
      if (els.restoreProgress) {
        els.restoreProgress.style.display = "none";
      }
      if (els.restoreResult) {
        els.restoreResult.style.display = "";
        els.restoreResult.className = "data-restore-result error";
        els.restoreResult.innerHTML = `<strong>Fehler:</strong> ${escapeHtml(errorMsg)}`;
      }
      setStatus(`Wiederherstellung fehlgeschlagen: ${errorMsg}`, "error");
      return;
    }

    const result = await response.json();

    if (els.restoreProgress) {
      els.restoreProgress.style.display = "none";
    }

    if (result.success) {
      const summary = result.summary || {};
      const dbCount = summary.databases_restored || 0;
      const dbTotal = summary.databases_total || 0;
      const storageFiles = summary.storage_files_restored || 0;

      if (els.restoreResult) {
        els.restoreResult.style.display = "";
        els.restoreResult.className = "data-restore-result success";
        els.restoreResult.innerHTML =
          `<strong>Wiederherstellung erfolgreich!</strong><br>` +
          `Datenbanken: ${dbCount}/${dbTotal} wiederhergestellt<br>` +
          `Dateien: ${storageFiles} wiederhergestellt<br>` +
          `Backup vom: ${summary.backup_generated_at || "unbekannt"}`;
      }

      setStatus("Wiederherstellung erfolgreich. Daten werden neu geladen...", "ok");

      // Refresh all data after restore
      setTimeout(async () => {
        try {
          await refreshAll();
          setStatus("Wiederherstellung abgeschlossen. Alle Daten neu geladen.", "ok");
        } catch (err) {
          setStatus("Wiederherstellung abgeschlossen, aber Daten konnten nicht neu geladen werden. Seite neu laden.", "error");
        }
      }, 500);

    } else {
      const errorMsg = result.error || "Wiederherstellung fehlgeschlagen";

      if (els.restoreResult) {
        els.restoreResult.style.display = "";
        els.restoreResult.className = "data-restore-result error";
        els.restoreResult.innerHTML = `<strong>Fehler:</strong> ${escapeHtml(errorMsg)}`;
      }

      setStatus(`Wiederherstellung fehlgeschlagen: ${errorMsg}`, "error");
    }

  } catch (err) {
    if (els.restoreProgress) {
      els.restoreProgress.style.display = "none";
    }
    if (els.restoreResult) {
      els.restoreResult.style.display = "";
      els.restoreResult.className = "data-restore-result error";
      els.restoreResult.innerHTML = `<strong>Netzwerkfehler:</strong> ${escapeHtml(err.message)}`;
    }
    setStatus(`Wiederherstellung fehlgeschlagen: ${err.message}`, "error");
  }

  // Reset file input for next use
  if (input instanceof HTMLInputElement) {
    input.value = "";
  }
  if (els.restoreFileLabel) {
    els.restoreFileLabel.textContent = "ZIP-Datei waehlen...";
  }
}

async function runPeriodExport() {
  const fromDate = String(state.filters.from || "").trim();
  const toDate = String(state.filters.to || "").trim();
  if (!fromDate || !toDate) {
    setStatus("Bitte zuerst Von und Bis setzen fuer den Zeitraum-Export.", "error");
    return;
  }

  const params = new URLSearchParams();
  params.set("from", fromDate);
  params.set("to", toDate);
  if (state.filters.marketplace) {
    params.set("marketplace", state.filters.marketplace);
  }
  if (state.filters.q) {
    params.set("q", state.filters.q);
  }

  try {
    await triggerDownload(`${API_BASE}/exports/period?${params.toString()}`, "combined_period_export.zip");
    setStatus("Zeitraum-Export heruntergeladen (ZIP mit CSV + Belegen).", "ok");
  } catch (error) {
    setStatus(`Zeitraum-Export fehlgeschlagen: ${error.message}`, "error");
  }
}

async function runFullBackupExport() {
  try {
    await triggerDownload(`${API_BASE}/exports/backup`, "combined_full_backup.zip");
    setStatus("Vollbackup heruntergeladen (ZIP Snapshot).", "ok");
  } catch (error) {
    setStatus(`Vollbackup fehlgeschlagen: ${error.message}`, "error");
  }
}

/* ── Event Binding & Boot ── */

function bindEvents() {
  /* Sidebar toggle */
  if (els.sidebarToggleBtn) {
    els.sidebarToggleBtn.addEventListener("click", () => {
      if (els.mainSidebar) {
        els.mainSidebar.classList.toggle("collapsed");
        const isCollapsed = els.mainSidebar.classList.contains("collapsed");
        els.sidebarToggleBtn.setAttribute("aria-label", isCollapsed ? "Sidebar ausklappen" : "Sidebar einklappen");
        els.sidebarToggleBtn.title = isCollapsed ? "Ausklappen" : "Einklappen";
        
        const pageLayout = document.querySelector(".page-layout");
        if (pageLayout) {
          pageLayout.classList.toggle("sidebar-collapsed", isCollapsed);
        }
      }
    });
  }

  /* Status panel */
  if (els.sidebarStatusBtn) {
    els.sidebarStatusBtn.addEventListener("click", async () => {
      const panel = document.getElementById("statusPanel");
      if (panel) {
        panel.classList.toggle("active");
        const isActive = panel.classList.contains("active");
        els.sidebarStatusBtn.classList.toggle("active", isActive);
        
        if (isActive) {
          try {
            await loadHealth();
            await Promise.allSettled([
              loadGoogleAds(),
              ensureCustomersDataLoaded(false),
            ]);
            rerender();
          } catch (e) {
            console.error("Failed to load status:", e);
          }
        }
      }
    });
  }

  if (document.getElementById("closeStatusBtn")) {
    document.getElementById("closeStatusBtn").addEventListener("click", () => {
      const panel = document.getElementById("statusPanel");
      if (panel) {
        panel.classList.remove("active");
        if (els.sidebarStatusBtn) els.sidebarStatusBtn.classList.remove("active");
      }
    });
  }

  /* Settings panel */
  if (els.sidebarSettingsBtn) {
    els.sidebarSettingsBtn.addEventListener("click", () => {
      const panel = document.getElementById("settingsPanel");
      if (panel) {
        panel.classList.toggle("active");
        const isActive = panel.classList.contains("active");
        els.sidebarSettingsBtn.classList.toggle("active", isActive);
      }
    });
  }

  if (document.getElementById("closeSettingsBtn")) {
    document.getElementById("closeSettingsBtn").addEventListener("click", () => {
      closeSettingsPanel();
    });
  }

  /* Close modals on backdrop click */
  document.addEventListener("click", (e) => {
    if (e.target.classList.contains("sidebar-modal")) {
      e.target.classList.remove("active");
      if (els.sidebarStatusBtn) els.sidebarStatusBtn.classList.remove("active");
      if (els.sidebarSettingsBtn) els.sidebarSettingsBtn.classList.remove("active");
    }
  });

  /* Sync button - trigger source sync */
  const syncBtnEl = document.getElementById("sidebarSyncBtn");
  if (syncBtnEl) {
    syncBtnEl.addEventListener("click", async () => {
      syncBtnEl.disabled = true;
      syncBtnEl.innerHTML = '<svg viewBox="0 0 24 24" aria-hidden="true" class="spinning"><path d="M12 4V1L8 5l4 4V6c3.31 0 6 2.69 6 6 0 1.01-.25 1.97-.7 2.8l1.46 1.46C19.54 15.03 20 13.57 20 12c0-4.42-3.58-8-8-8zm0 14c-3.31 0-6-2.69-6-6 0-1.01.25-1.97.7-2.8L5.24 7.74C4.46 8.97 4 10.43 4 12c0 4.42 3.58 8 8 8v3l4-4-4-4v3z"/></svg><span>Sync...</span>';
      try {
        const resp = await fetch("/api/sync/run", { method: "POST", headers: { "Content-Type": "application/json" } });
        const data = await resp.json();
        if (data.ok || data.results) {
          setStatus("Quellen synchronisiert", "ok");
          setTimeout(() => refreshAll(), 500);
        } else {
          setStatus("Sync fehlgeschlagen", "error");
        }
      } catch (err) {
        setStatus("Sync Fehler: " + err.message, "error");
      }
      syncBtnEl.disabled = false;
      syncBtnEl.innerHTML = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 4V1L8 5l4 4V6c3.31 0 6 2.69 6 6 0 1.01-.25 1.97-.7 2.8l1.46 1.46C19.54 15.03 20 13.57 20 12c0-4.42-3.58-8-8-8zm0 14c-3.31 0-6-2.69-6-6 0-1.01.25-1.97.7-2.8L5.24 7.74C4.46 8.97 4 10.43 4 12c0 4.42 3.58 8 8 8v3l4-4-4-4v3z"/></svg><span>Sync</span>';
    });
  }

  /* Settings buttons */
  const btnSyncSources = document.getElementById("btnSyncSources");
  if (btnSyncSources) {
    btnSyncSources.addEventListener("click", async () => {
      btnSyncSources.disabled = true;
      btnSyncSources.textContent = "Syncing...";
      try {
        const resp = await fetchJson("/api/sync/run", { method: "POST" });
        if (resp.ok) {
          setStatus("Quellen synchronisiert", "ok");
          setTimeout(() => refreshAll(), 500);
        } else {
          setStatus("Sync fehlgeschlagen", "error");
        }
      } catch (err) {
        setStatus("Sync Fehler", "error");
      }
      btnSyncSources.disabled = false;
      btnSyncSources.textContent = "Quellen synchronisieren";
    });
  }

  const btnSyncLive = document.getElementById("btnSyncLive");
  if (btnSyncLive) {
    btnSyncLive.addEventListener("click", async () => {
      btnSyncLive.disabled = true;
      btnSyncLive.textContent = "Live Sync...";
      try {
        const resp = await fetchJson("/api/sync/live/run", { method: "POST" });
        if (resp.ok) {
          setStatus("Live Sync gestartet", "ok");
        } else {
          setStatus("Live Sync fehlgeschlagen", "error");
        }
      } catch (err) {
        setStatus("Live Sync Fehler", "error");
      }
      btnSyncLive.disabled = false;
      btnSyncLive.textContent = "Live-API Sync starten";
    });
  }

  const btnReloadData = document.getElementById("btnReloadData");
  if (btnReloadData) {
    btnReloadData.addEventListener("click", () => {
      refreshAll();
      setStatus("Daten neu geladen", "ok");
    });
  }

  const btnBackupData = document.getElementById("btnBackupData");
  if (btnBackupData) {
    btnBackupData.addEventListener("click", async () => {
      btnBackupData.disabled = true;
      btnBackupData.textContent = "Backup...";
      try {
        const resp = await fetchJson("/api/exports/backup", { method: "POST" });
        if (resp.ok) {
          setStatus("Backup erstellt", "ok");
        } else {
          setStatus("Backup fehlgeschlagen", "error");
        }
      } catch (err) {
        setStatus("Backup Fehler", "error");
      }
      btnBackupData.disabled = false;
      btnBackupData.textContent = "Backup erstellen";
    });
  }

  /* Credentials save */
  const btnSaveCredentials = document.getElementById("btnSaveCredentials");
  if (btnSaveCredentials) {
    btnSaveCredentials.addEventListener("click", async () => {
      const domain = document.getElementById("credShopifyDomain")?.value?.trim();
      const clientId = document.getElementById("credShopifyClientId")?.value?.trim();
      const clientSecret = document.getElementById("credShopifyClientSecret")?.value?.trim();
      const kauflandClientKey = document.getElementById("credKauflandClientKey")?.value?.trim();
      const kauflandSecretKey = document.getElementById("credKauflandSecretKey")?.value?.trim();
      
      try {
        const resp = await fetch("/api/sync/credentials", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            shopify_domain: domain,
            shopify_client_id: clientId,
            shopify_client_secret: clientSecret,
            shopify_api_version: "2025-01",
            kaufland_client_key: kauflandClientKey,
            kaufland_secret_key: kauflandSecretKey,
          }),
        });
        const data = await resp.json();
        
        const statusEl = document.getElementById("credentialsStatus");
        if (statusEl) {
          if (data.ok) {
            statusEl.textContent = "Credentials gespeichert!";
            statusEl.className = "credentials-status success";
          } else {
            statusEl.textContent = "Fehler: " + (data.message || "");
            statusEl.className = "credentials-status error";
          }
          setTimeout(() => {
            statusEl.textContent = "";
            statusEl.className = "credentials-status";
          }, 3000);
        }
        
        setStatus(data.ok ? "Credentials gespeichert!" : "Fehler beim Speichern", data.ok ? "ok" : "error");
      } catch (err) {
        setStatus("Fehler: " + err.message, "error");
      }
    });
  }

  /* Load saved credentials on page load */
  (async function loadSavedCredentials() {
    try {
      const resp = await fetch("/api/sync/credentials");
      const data = await resp.json();
      if (data.ok && data.has_credentials) {
        const domainEl = document.getElementById("credShopifyDomain");
        const clientIdEl = document.getElementById("credShopifyClientId");
        const kauflandKeyEl = document.getElementById("credKauflandClientKey");
        
        if (domainEl) domainEl.placeholder = data.shopify_configured ? "Shopify konfiguriert" : "";
        if (clientIdEl) clientIdEl.placeholder = data.shopify_configured ? "Client ID gesetzt" : "";
        if (kauflandKeyEl) kauflandKeyEl.placeholder = data.kaufland_configured ? "Kaufland konfiguriert" : "";
      }
    } catch (e) {}
  })();

  if (els.dateRangeBtn) {
    els.dateRangeBtn.addEventListener("click", () => {
      const open = els.dateRangeMenu instanceof HTMLElement && els.dateRangeMenu.classList.contains("active");
      setDateRangeMenuOpen(!open);
      if (!open) {
        setExportMenuOpen(false);
        setChannelMenuOpen(false);
        setSearchModalOpen(false);
      }
    });
  }

  if (els.dateRangeMenu) {
    els.dateRangeMenu.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      if (target.id === "dateMonthPrevBtn") {
        const anchor = String(state.dateRangeUi.anchorMonth || "").trim() || monthTokenFromDate(new Date());
        state.dateRangeUi.anchorMonth = shiftMonthToken(anchor, -1);
        renderDateRangeCalendars();
        return;
      }
      if (target.id === "dateMonthNextBtn") {
        const anchor = String(state.dateRangeUi.anchorMonth || "").trim() || monthTokenFromDate(new Date());
        state.dateRangeUi.anchorMonth = shiftMonthToken(anchor, 1);
        renderDateRangeCalendars();
        return;
      }

      const dayButton = target.closest("[data-date-token]");
      if (dayButton instanceof HTMLElement) {
        const token = String(dayButton.dataset.dateToken || "").trim();
        if (token) {
          selectCustomDateToken(token);
        }
        return;
      }

      const preset = String(target.dataset.preset || "").trim();
      if (preset) {
        applyDatePreset(preset);
        setDateRangeMenuOpen(false);
        refreshFilterData();
      }
    });
  }

  if (els.dateCustomApplyBtn) {
    els.dateCustomApplyBtn.addEventListener("click", () => {
      if (!applyCustomDateRange()) {
        return;
      }
      setDateRangeMenuOpen(false);
      refreshFilterData();
    });
  }

  if (els.dataModalOpenBtn) {
    els.dataModalOpenBtn.addEventListener("click", () => {
      openDataModal();
    });
  }

  if (els.channelMenuBtn) {
    els.channelMenuBtn.addEventListener("click", () => {
      const open = els.channelMenu instanceof HTMLElement && els.channelMenu.classList.contains("active");
      setChannelMenuOpen(!open);
      if (!open) {
        setDateRangeMenuOpen(false);
        setExportMenuOpen(false);
        setSearchModalOpen(false);
      }
    });
  }

  if (els.channelMenu) {
    els.channelMenu.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const channel = String(target.dataset.channel || "").trim().toLowerCase();
      if (target.dataset.channel === undefined) {
        return;
      }
      setMarketplaceFilter(channel);
      setChannelMenuOpen(false);
      refreshFilterData();
    });
  }

  /* ── Data Modal (Export + Restore) Event Listeners ── */
  if (els.dataModalCloseBtn) {
    els.dataModalCloseBtn.addEventListener("click", closeDataModal);
  }
  if (els.dataModal) {
    els.dataModal.addEventListener("click", (e) => {
      if (e.target === els.dataModal) closeDataModal();
    });
  }
  if (els.dataExportPeriodBtn) {
    els.dataExportPeriodBtn.addEventListener("click", async () => {
      closeDataModal();
      await runPeriodExport();
    });
  }
  if (els.dataExportBackupBtn) {
    els.dataExportBackupBtn.addEventListener("click", async () => {
      closeDataModal();
      await runFullBackupExport();
    });
  }
  if (els.restoreFileInput) {
    els.restoreFileInput.addEventListener("change", onRestoreFileSelected);
  }
  if (els.restoreCancelBtn) {
    els.restoreCancelBtn.addEventListener("click", resetRestoreUI);
  }
  if (els.restoreConfirmBtn) {
    els.restoreConfirmBtn.addEventListener("click", executeRestore);
  }

  if (els.trendGranularityGroup) {
    els.trendGranularityGroup.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const button = target.closest("[data-trend-granularity]");
      if (!(button instanceof HTMLElement)) {
        return;
      }
      const next = normalizeTrendGranularity(button.dataset.trendGranularity || "auto");
      if (next === state.trendGranularity) {
        return;
      }
      setTrendGranularity(next);
      refreshFilterData();
    });
  }

  if (els.customerGeoModeGroup) {
    els.customerGeoModeGroup.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const button = target.closest("[data-customer-geo-mode]");
      if (!(button instanceof HTMLElement)) {
        return;
      }
      const next = normalizeCustomerGeoMode(button.dataset.customerGeoMode || "map");
      if (next === state.customerGeoMode) {
        return;
      }
      if (next === "globe" && state.customerGlobeWebGLUnavailable) {
        return;
      }
      setCustomerGeoMode(next);
    });
  }

  {
    let resizeRafId = 0;
    window.addEventListener("resize", () => {
      if (resizeRafId) return;
      resizeRafId = requestAnimationFrame(() => {
        resizeRafId = 0;
        if (typeof syncOrdersTableViewportHeight === "function") {
          syncOrdersTableViewportHeight();
        }
        if (state.customerLeafletMap) {
          state.customerLeafletMap.invalidateSize();
        }
        if (state.customerGlobe && els.customerGeoGlobeView instanceof HTMLElement) {
          if (typeof state.customerGlobe.width === "function") {
            state.customerGlobe.width(els.customerGeoGlobeView.clientWidth || 640);
          }
          if (typeof state.customerGlobe.height === "function") {
            state.customerGlobe.height(els.customerGeoGlobeView.clientHeight || 420);
          }
        }
      });
    });
  }

  if (els.searchOpenBtn) {
    els.searchOpenBtn.addEventListener("click", () => {
      setDateRangeMenuOpen(false);
      setExportMenuOpen(false);
      setChannelMenuOpen(false);
      setSearchModalOpen(true);
    });
  }

  if (els.closeSearchModalBtn) {
    els.closeSearchModalBtn.addEventListener("click", () => {
      setSearchModalOpen(false);
    });
  }

  if (els.searchModal) {
    els.searchModal.addEventListener("click", (event) => {
      if (event.target === els.searchModal) {
        setSearchModalOpen(false);
      }
    });
  }

  if (els.clearSearchBtn) {
    els.clearSearchBtn.addEventListener("click", () => {
      state.filters.q = "";
      if (els.searchInput instanceof HTMLInputElement) {
        els.searchInput.value = "";
        els.searchInput.focus();
      }
      updateSearchTriggerState();
      scheduleFilterRefresh(0);
    });
  }

  if (els.searchInput) {
    els.searchInput.addEventListener("input", () => {
      state.filters.q = String(els.searchInput.value || "").trim();
      updateSearchTriggerState();
      scheduleFilterRefresh();
    });
    els.searchInput.addEventListener("keydown", (event) => {
      if (event.key !== "Enter") {
        return;
      }
      event.preventDefault();
      scheduleFilterRefresh(0);
      setSearchModalOpen(false);
    });
  }

  if (els.marketplaceSelect) {
    els.marketplaceSelect.addEventListener("change", () => {
      setMarketplaceFilter(els.marketplaceSelect.value);
      refreshFilterData();
    });
  }

  if (els.ordersFilterBtn) {
    els.ordersFilterBtn.addEventListener("click", () => {
      const open = els.ordersFilterDropdown instanceof HTMLElement &&
        els.ordersFilterDropdown.getAttribute("aria-hidden") === "false";
      setOrdersFilterOpen(!open);
    });
  }

  if (els.ordersFilterDropdown) {
    els.ordersFilterDropdown.addEventListener("click", (e) => {
      const chip = e.target.closest(".ofd-chip[data-filter-group]");
      if (!chip) return;
      const group = chip.getAttribute("data-filter-group");
      const value = chip.getAttribute("data-value");
      if (group === "status") {
        if (state.filters.orderStatus.has(value)) {
          state.filters.orderStatus.delete(value);
        } else {
          state.filters.orderStatus.add(value);
        }
        chip.classList.toggle("active", state.filters.orderStatus.has(value));
      } else if (group === "payment") {
        if (state.filters.orderPayment.has(value)) {
          state.filters.orderPayment.delete(value);
        } else {
          state.filters.orderPayment.add(value);
        }
        chip.classList.toggle("active", state.filters.orderPayment.has(value));
      } else if (group === "returns") {
        state.filters.returnsOnly = !state.filters.returnsOnly;
        chip.classList.toggle("active", state.filters.returnsOnly);
      } else if (group === "hidecanceled") {
        state.filters.hideCanceled = !state.filters.hideCanceled;
        chip.classList.toggle("active", state.filters.hideCanceled);
      } else if (group === "purchasecost") {
        if (value === "has") {
          state.filters.hasPurchaseCost = !state.filters.hasPurchaseCost;
          if (state.filters.hasPurchaseCost) state.filters.noPurchaseCost = false;
        } else {
          state.filters.noPurchaseCost = !state.filters.noPurchaseCost;
          if (state.filters.noPurchaseCost) state.filters.hasPurchaseCost = false;
        }
        // sync chip active states for both purchasecost chips
        if (els.ordersFilterDropdown) {
          els.ordersFilterDropdown.querySelectorAll("[data-filter-group='purchasecost']").forEach((c) => {
            const v = c.getAttribute("data-value");
            c.classList.toggle("active", v === "has" ? state.filters.hasPurchaseCost : state.filters.noPurchaseCost);
          });
        }
      } else if (group === "invoice") {
        if (value === "has") {
          state.filters.hasInvoice = !state.filters.hasInvoice;
          if (state.filters.hasInvoice) state.filters.noInvoice = false;
        } else {
          state.filters.noInvoice = !state.filters.noInvoice;
          if (state.filters.noInvoice) state.filters.hasInvoice = false;
        }
        if (els.ordersFilterDropdown) {
          els.ordersFilterDropdown.querySelectorAll("[data-filter-group='invoice']").forEach((c) => {
            const v = c.getAttribute("data-value");
            c.classList.toggle("active", v === "has" ? state.filters.hasInvoice : state.filters.noInvoice);
          });
        }
      }
      updateOrdersFilterBadge();
      renderOrders();
    });
  }

  if (els.ordersFilterClearBtn) {
    els.ordersFilterClearBtn.addEventListener("click", () => {
      state.filters.orderStatus.clear();
      state.filters.orderPayment.clear();
      state.filters.returnsOnly = false;
      state.filters.hasPurchaseCost = false;
      state.filters.noPurchaseCost = false;
      state.filters.hasInvoice = false;
      state.filters.noInvoice = false;
      // Reset hideCanceled to default (true)
      state.filters.hideCanceled = true;
      if (els.ordersFilterDropdown) {
        els.ordersFilterDropdown.querySelectorAll(".ofd-chip").forEach((c) => {
          const group = c.getAttribute("data-filter-group");
          const isDefault = group === "hidecanceled";
          c.classList.toggle("active", isDefault);
        });
      }
      updateOrdersFilterBadge();
      renderOrders();
    });
  }

  /* -- badge filter click (replaces old dropdown filters) -- */
  if (els.bookingTxLegend instanceof HTMLElement) {
    els.bookingTxLegend.addEventListener("click", (e) => {
      const item = e.target.closest(".tx-legend-item[data-filter-category]");
      if (!item) return;
      const cat = item.getAttribute("data-filter-category") || "";
      const current = String(state.bookingTxFilters.category || "");
      state.bookingTxFilters.category = (cat === current) ? "" : cat;
      state.bookingTxFilters.type = "";
      refreshBookingsTransactionsOnly(true);
    });
  }

  if (els.refreshBtn) {
    els.refreshBtn.addEventListener("click", () => {
      refreshAll();
    });
  }

  els.syncLiveBtn.addEventListener("click", () => {
    runLiveSync();
  });

  els.syncSourcesBtn.addEventListener("click", () => {
    runSourceSync();
  });

  if (els.sourcePanelToggleBtn) {
    els.sourcePanelToggleBtn.addEventListener("click", () => {
      const currentlyOpen = els.sourcePanel instanceof HTMLElement && els.sourcePanel.classList.contains("active");
      setSourcePanelOpen(!currentlyOpen);
    });
  }

  if (els.sourcePanelCloseBtn) {
    els.sourcePanelCloseBtn.addEventListener("click", () => {
      setSourcePanelOpen(false);
    });
  }

  /* ── Theme Modal Event Listeners ── */
  const themeModalOpenBtn = document.getElementById("themeModalOpenBtn");
  const themeModalCloseBtn = document.getElementById("themeModalCloseBtn");
  const themeModal = document.getElementById("themeModal");

  if (themeModalOpenBtn) {
    themeModalOpenBtn.addEventListener("click", openThemeModal);
  }

  if (themeModalCloseBtn) {
    themeModalCloseBtn.addEventListener("click", closeThemeModal);
  }

  if (themeModal) {
    themeModal.addEventListener("click", (e) => {
      if (e.target === themeModal) closeThemeModal();
    });
  }

  document.querySelectorAll(".theme-card[data-theme-id]").forEach(card => {
    card.addEventListener("click", () => {
      const themeId = card.getAttribute("data-theme-id") || "";
      applyTheme(themeId);
    });
  });

  /* ── Custom Theme Editor ── */
  const customThemeCard = document.getElementById("customThemeCard");
  if (customThemeCard) customThemeCard.addEventListener("click", openCustomThemeEditor);

  const cteBackBtn = document.getElementById("cteBackBtn");
  if (cteBackBtn) cteBackBtn.addEventListener("click", () => closeCustomThemeEditor(false));

  const cteSaveBtn = document.getElementById("cteSaveBtn");
  if (cteSaveBtn) cteSaveBtn.addEventListener("click", () => closeCustomThemeEditor(true));

  const cteResetBtn = document.getElementById("cteResetBtn");
  if (cteResetBtn) cteResetBtn.addEventListener("click", () => {
    const sel = document.getElementById("cteBaseSelect");
    cteSwitchBase(sel ? sel.value : "");
  });

  const cteBaseSelect = document.getElementById("cteBaseSelect");
  if (cteBaseSelect) cteBaseSelect.addEventListener("change", (e) => cteSwitchBase(e.target.value));

  const cteBody = document.getElementById("cteBody");
  if (cteBody) cteBody.addEventListener("input", cteHandleInput);

  // Apply stored theme and mark active card on load
  {
    const stored = getStoredTheme();
    applyTheme(stored);
    const customData = getStoredCustomTheme();
    if (customData) updateCustomPreviewThumbnail(customData);
  }

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      if (_cteOpen) { closeCustomThemeEditor(false); return; }
      setSourcePanelOpen(false);
      setDateRangeMenuOpen(false);
      setExportMenuOpen(false);
      setChannelMenuOpen(false);
      setSearchModalOpen(false);
      closeThemeModal();
      closeDataModal();
    }
  });

  document.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Node)) {
      return;
    }
    if (els.dateRangeBtn instanceof HTMLElement && els.dateRangeMenu instanceof HTMLElement) {
      if (!els.dateRangeBtn.contains(target) && !els.dateRangeMenu.contains(target)) {
        setDateRangeMenuOpen(false);
      }
    }
    if (els.channelMenuBtn instanceof HTMLElement && els.channelMenu instanceof HTMLElement) {
      if (!els.channelMenuBtn.contains(target) && !els.channelMenu.contains(target)) {
        setChannelMenuOpen(false);
      }
    }
    if (els.sammelMonthBtn instanceof HTMLElement && els.sammelMonthMenu instanceof HTMLElement) {
      if (!els.sammelMonthBtn.contains(target) && !els.sammelMonthMenu.contains(target)) {
        setSammelMonthMenuOpen(false);
      }
    }
    if (els.ordersFilterBtn instanceof HTMLElement && els.ordersFilterDropdown instanceof HTMLElement) {
      if (!els.ordersFilterBtn.contains(target) && !els.ordersFilterDropdown.contains(target)) {
        setOrdersFilterOpen(false);
      }
    }
  });

  els.tabAnalyticsBtn.addEventListener("click", () => setActiveTab("analytics"));
  els.tabOrdersBtn.addEventListener("click", () => setActiveTab("orders"));
  els.tabCustomersBtn.addEventListener("click", () => setActiveTab("customers"));
  els.tabBookingsBtn.addEventListener("click", () => setActiveTab("bookings"));
  els.tabGoogleAdsBtn.addEventListener("click", () => setActiveTab("googleads"));
  els.tabEbayBtn.addEventListener("click", () => setActiveTab("ebay"));
  els.bookingsTransactionsBtn.addEventListener("click", () => setBookingsSubtab("transactions"));
  els.bookingsOrdersBtn.addEventListener("click", () => setBookingsSubtab("orders"));
  els.bookingsTemplatesBtn.addEventListener("click", () => setBookingsSubtab("templates"));
  els.bookingsAccountsBtn.addEventListener("click", () => setBookingsSubtab("accounts"));
  els.bookingsDocumentsBtn.addEventListener("click", () => setBookingsSubtab("documents"));

  /* Booking class subtab bar - old horizontal bar */
  if (els.bookingClassControl) {
    els.bookingClassControl.addEventListener("click", (e) => {
      const btn = e.target.closest("[data-booking-class]");
      if (!btn) return;
      const cls = btn.getAttribute("data-booking-class") || "automatic";
      setBookingClass(cls);
      refreshBookingsTransactionsOnly(false);
      /* Also load monthly invoices when switching to monthly */
      if (cls === "monthly") {
        loadMonthlyInvoices().then(() => renderMonthlyInvoices()).catch(() => {});
      }
    });
  }

  /* Booking class subnav in sidebar */
  if (els.bookingClassSubnav) {
    els.bookingClassSubnav.addEventListener("click", (e) => {
      const btn = e.target.closest("[data-booking-class]");
      if (!btn) return;
      const cls = btn.getAttribute("data-booking-class") || "automatic";
      setBookingClass(cls);
      refreshBookingsTransactionsOnly(false);
      if (cls === "monthly") {
        loadMonthlyInvoices().then(() => renderMonthlyInvoices()).catch(() => {});
      }
    });
  }

  /* Sammelrechnung create button */
  if (els.createSammelBtn) {
    els.createSammelBtn.addEventListener("click", () => {
      createMonthlyInvoice();
    });
  }

  /* Sammelrechnung month picker */
  if (els.sammelMonthBtn) {
    els.sammelMonthBtn.addEventListener("click", () => {
      const isOpen = els.sammelMonthBtn.getAttribute("aria-expanded") === "true";
      setSammelMonthMenuOpen(!isOpen);
    });
  }
  if (els.sammelYearPrevBtn) {
    els.sammelYearPrevBtn.addEventListener("click", () => {
      _sammelPickerYear--;
      renderSammelMonthGrid();
    });
  }
  if (els.sammelYearNextBtn) {
    els.sammelYearNextBtn.addEventListener("click", () => {
      _sammelPickerYear++;
      renderSammelMonthGrid();
    });
  }
  if (els.sammelMonthGrid) {
    els.sammelMonthGrid.addEventListener("click", (e) => {
      const btn = e.target instanceof HTMLElement ? e.target.closest("[data-month]") : null;
      if (!btn) return;
      selectSammelMonth(btn.dataset.month);
    });
  }

  /* Sammelrechnung file input — show selected filename */
  if (els.createSammelFile) {
    els.createSammelFile.addEventListener("change", () => {
      const name = els.createSammelFile.files?.[0]?.name || "";
      if (els.createSammelFileName instanceof HTMLElement) {
        els.createSammelFileName.textContent = name || "Optional";
      }
    });
  }

  /* Sammelrechnung table actions (delete + upload + detail on row click) */
  if (els.sammelrechnungBody) {
    els.sammelrechnungBody.addEventListener("click", (e) => {
      const target = e.target instanceof HTMLElement ? e.target : null;
      if (!target) return;
      const row = target.closest("tr[data-invoice-id]");
      if (!row) return;
      const invoiceId = row.dataset.invoiceId;

      if (target.closest("[data-action='delete-invoice']")) {
        deleteMonthlyInvoice(invoiceId);
        return;
      }
      if (target.closest("[data-action='upload-invoice-doc']")) {
        uploadSammelrechnungDocument(invoiceId);
        return;
      }
      if (target.closest("[data-action='preview-document']")) {
        return; /* handled by document preview logic */
      }
      /* Click on row (not on interactive elements) → open detail */
      const interactive = target.closest("input, select, button, a, label, textarea");
      if (interactive) return;
      openMonthlyInvoiceDetail(invoiceId);
    });
  }

  document.querySelectorAll(".bookings-tools-toggle").forEach((toggleBtn) => {
    toggleBtn.addEventListener("click", () => {
      const targetId = toggleBtn.getAttribute("data-target");
      const panel = targetId ? document.getElementById(targetId) : null;
      if (!panel) return;
      const isOpen = panel.classList.contains("open");
      panel.classList.toggle("open", !isOpen);
      toggleBtn.setAttribute("aria-expanded", String(!isOpen));
    });
  });

  els.createBookingTxBtn.addEventListener("click", () => {
    createBookingTransaction();
  });
  els.createTemplateBtn.addEventListener("click", () => {
    createBookingTemplate();
  });
  els.createAccountBtn.addEventListener("click", () => {
    createBookingAccount();
  });
  els.uploadBookingDocumentBtn.addEventListener("click", () => {
    uploadBookingDocument();
  });
  if (els.googleAdsUploadBtn) {
    els.googleAdsUploadBtn.addEventListener("click", () => {
      uploadGoogleAdsCsv();
    });
  }
  if (els.googleAdsResetBtn) {
    els.googleAdsResetBtn.addEventListener("click", () => {
      resetGoogleAdsData();
    });
  }

  /* ── Polling settings ── */
  if (els.pollingToggle instanceof HTMLInputElement) {
    els.pollingToggle.addEventListener("change", () => {
      const enabled = els.pollingToggle.checked;
      state.pollingEnabled = enabled;
      savePollingSettings();
      if (enabled) {
        startPolling();
      } else {
        stopPolling();
      }
    });
  }
  if (els.pollingIntervalInput instanceof HTMLInputElement) {
    els.pollingIntervalInput.addEventListener("change", () => {
      const val = parseInt(els.pollingIntervalInput.value, 10);
      if (val >= 5 && val <= 3600) {
        state.pollingIntervalSec = val;
        savePollingSettings();
        if (state.pollingEnabled) {
          stopPolling();
          startPolling();
        }
      }
    });
  }
  if (els.googleAdsReportInput instanceof HTMLInputElement) {
    els.googleAdsReportInput.addEventListener("change", () => {
      syncGoogleAdsFileLabels();
    });
  }
  if (els.googleAdsAssignmentInput instanceof HTMLInputElement) {
    els.googleAdsAssignmentInput.addEventListener("change", () => {
      syncGoogleAdsFileLabels();
    });
  }

  if (els.ebayShopFilter) {
    els.ebayShopFilter.addEventListener("change", () => {
      renderEbayOrders();
    });
  }
  if (els.ebayCategoryFilter) {
    els.ebayCategoryFilter.addEventListener("change", () => {
      renderEbayOrders();
    });
  }

  els.ordersBody.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const row = target.closest("tr[data-marketplace][data-order-id]");
    if (!(row instanceof HTMLElement)) {
      return;
    }

    const interactive = target.closest("input, select, button, a, label, textarea");
    if (interactive) {
      return;
    }
    openDetails(row);
  });

  els.ordersBody.addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) {
      return;
    }
    if (target.classList.contains("purchase-input")) {
      const row = target.closest("tr[data-marketplace][data-order-id]");
      if (!(row instanceof HTMLElement)) {
        return;
      }
      updateOrderProfitPreview(row);
      savePurchase(row);
      return;
    }
    if (!target.classList.contains("invoice-file-input")) {
      return;
    }
    const row = target.closest("tr[data-marketplace][data-order-id]");
    if (!(row instanceof HTMLElement)) {
      return;
    }
    const file = target.files && target.files[0] ? target.files[0] : null;
    if (!file) {
      return;
    }
    uploadInvoice(row);
  });

  els.ordersBody.addEventListener("input", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement) || !target.classList.contains("purchase-input")) {
      return;
    }
    const row = target.closest("tr[data-marketplace][data-order-id]");
    if (!(row instanceof HTMLElement)) {
      return;
    }
    updateOrderProfitPreview(row);
  });

  els.ordersBody.addEventListener("keydown", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement) || !target.classList.contains("purchase-input")) {
      return;
    }
    if (event.key !== "Enter") {
      return;
    }
    event.preventDefault();
    const row = target.closest("tr[data-marketplace][data-order-id]");
    if (!(row instanceof HTMLElement)) {
      return;
    }
    savePurchase(row);
  });

  els.bookingsBody.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const action = target.dataset.action;
    if (action === "preview-document") {
      event.preventDefault();
      openPreviewModal(target.dataset.url, target.dataset.filename, target.dataset.mime);
      return;
    }
    const interactive = target.closest("input, select, button, a, label, textarea");
    if (interactive) {
      return;
    }
    const detailsRow = target.closest("tr[data-booking-id]");
    if (detailsRow instanceof HTMLElement) {
      openBookingTransactionDetailsById(detailsRow.dataset.bookingId);
    }
  });

  /* Auto-save bookings on change */
  els.bookingsBody.addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    const row = target.closest("tr[data-booking-id]");
    if (row instanceof HTMLElement) saveBooking(row);
  });

  els.bookingOrdersBody.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const action = target.dataset.action;
    if (action !== "details") {
      return;
    }
    const row = target.closest("tr[data-marketplace][data-order-id]");
    if (!(row instanceof HTMLElement)) {
      return;
    }
    openDetails(row);
  });

  els.bookingTemplatesBody.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const action = target.dataset.action;
    if (!action) {
      return;
    }
    const row = target.closest("tr[data-template-id]");
    if (!(row instanceof HTMLElement)) {
      return;
    }
    if (action === "generate-template") {
      runBookingTemplate(row);
      return;
    }
    if (action === "generate-template-backfill") {
      runBookingTemplateBackfill(row);
    }
  });

  /* Auto-save templates on change */
  els.bookingTemplatesBody.addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    const row = target.closest("tr[data-template-id]");
    if (row instanceof HTMLElement) saveBookingTemplate(row);
  });

  /* Auto-save accounts on change */
  els.bookingAccountsBody.addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    const row = target.closest("tr[data-account-id]");
    if (row instanceof HTMLElement) saveBookingAccount(row);
  });

  els.bookingDocumentsBody.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    if (target.dataset.action !== "preview-document") {
      return;
    }
    event.preventDefault();
    openPreviewModal(target.dataset.url, target.dataset.filename, target.dataset.mime);
  });

  els.detailsContent.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const action = target.dataset.action;
    if (action === "preview-document") {
      event.preventDefault();
      openPreviewModal(target.dataset.url, target.dataset.filename, target.dataset.mime);
      return;
    }
    if (action === "save-booking-modal") {
      /* Button removed (auto-save on close), but keep handler as fallback */
      saveBookingFromDetailsModal();
      return;
    }
    if (action === "delete-booking-modal") {
      deleteBookingFromDetailsModal();
      return;
    }
    if (action === "save-invoice-modal") {
      saveMonthlyInvoiceFromDetail();
      return;
    }
    if (action === "delete-invoice-modal") {
      deleteMonthlyInvoiceFromDetail();
      return;
    }
    if (action === "open-order") {
      const provider = (target.dataset.provider || "").trim().toLowerCase();
      const externalOrderId = (target.dataset.externalOrderId || "").trim();
      if (!provider || !externalOrderId) return;
      /* Look up internal order_id from state.orders cache */
      const match = state.orders.find((o) => {
        return String(o.marketplace || "").trim().toLowerCase() === provider
          && String(o.external_order_id || "").trim() === externalOrderId;
      });
      if (!match) {
        setStatus(`Order ${provider} | ${externalOrderId} nicht im Orders-Cache gefunden. Bitte erst den Orders-Tab laden.`, "error");
        return;
      }
      state.returnToTransactionId = state.bookingDetailsTransactionId || null;
      state.returnToOrderKey = null;
      openOrderDetailById(match.marketplace, match.order_id);
      return;
    }
    /* Click on a transaction row inside Order detail → open transaction detail */
    if (state.detailsMode === "order") {
      const interactive = target.closest("input, select, button, a, label, textarea");
      if (interactive) return;
      const txRow = target.closest("tr[data-tx-id]");
      if (txRow instanceof HTMLElement && txRow.dataset.txId) {
        state.returnToOrderKey = {
          marketplace: state.orderDetailMarketplace || "",
          orderId: state.orderDetailOrderId || "",
        };
        openBookingTransactionDetailsById(txRow.dataset.txId);
        return;
      }
    }
    /* Click on a transaction row inside Sammelrechnung detail → open transaction detail */
    if (state.detailsMode === "monthly-invoice") {
      const interactive = target.closest("input, select, button, a, label, textarea");
      if (interactive) return;
      const txRow = target.closest("tr[data-tx-id]");
      if (txRow instanceof HTMLElement && txRow.dataset.txId) {
        state.returnToInvoiceId = state.monthlyInvoiceDetailId || null;
        openBookingTransactionDetailsById(txRow.dataset.txId);
      }
    }
  });

  els.closeModalBtn.addEventListener("click", () => {
    closeDetailsModal();
  });

  els.detailsModal.addEventListener("click", (event) => {
    if (event.target === els.detailsModal) {
      closeDetailsModal();
    }
  });

  els.closePreviewBtn.addEventListener("click", () => {
    closePreviewModal();
  });

  document.getElementById("previewZoomIn").addEventListener("click", () => {
    previewZoom = Math.min(PREVIEW_ZOOM_MAX, previewZoom + PREVIEW_ZOOM_STEP);
    updatePreviewZoom();
  });
  document.getElementById("previewZoomOut").addEventListener("click", () => {
    previewZoom = Math.max(PREVIEW_ZOOM_MIN, previewZoom - PREVIEW_ZOOM_STEP);
    updatePreviewZoom();
  });
  document.getElementById("previewZoomReset").addEventListener("click", () => {
    previewZoom = 1;
    updatePreviewZoom();
  });
  els.previewBody.addEventListener("wheel", (event) => {
    const img = els.previewBody.querySelector(".preview-image");
    if (!img) return;
    event.preventDefault();
    const delta = event.deltaY < 0 ? PREVIEW_ZOOM_STEP : -PREVIEW_ZOOM_STEP;
    previewZoom = Math.min(PREVIEW_ZOOM_MAX, Math.max(PREVIEW_ZOOM_MIN, previewZoom + delta));
    updatePreviewZoom();
  }, { passive: false });

  els.previewModal.addEventListener("click", (event) => {
    if (event.target === els.previewModal) {
      closePreviewModal();
    }
  });
}

/* ── Cross-Device Polling ── */

const POLLING_STORAGE_KEY = "dash-combined.polling";

function loadPollingSettings() {
  try {
    const raw = localStorage.getItem(POLLING_STORAGE_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      state.pollingEnabled = !!parsed.enabled;
      const iv = parseInt(parsed.intervalSec, 10);
      if (iv >= 5 && iv <= 3600) {
        state.pollingIntervalSec = iv;
      }
    }
  } catch { /* ignore */ }
}

function savePollingSettings() {
  try {
    localStorage.setItem(POLLING_STORAGE_KEY, JSON.stringify({
      enabled: state.pollingEnabled,
      intervalSec: state.pollingIntervalSec,
    }));
  } catch { /* ignore */ }
}

function applyPollingSettingsToUi() {
  if (els.pollingToggle instanceof HTMLInputElement) {
    els.pollingToggle.checked = state.pollingEnabled;
  }
  if (els.pollingIntervalInput instanceof HTMLInputElement) {
    els.pollingIntervalInput.value = String(state.pollingIntervalSec);
  }
}

async function pollChangestamp() {
  try {
    const data = await fetchJson(`${API_BASE}/sync/changestamp`);
    const stamp = data && typeof data.stamp === "number" ? data.stamp : 0;
    if (state.pollingLastStamp === 0) {
      /* First poll — just store the stamp, no refresh needed */
      state.pollingLastStamp = stamp;
      return;
    }
    if (stamp !== state.pollingLastStamp) {
      state.pollingLastStamp = stamp;
      await refreshFilterData();
    }
  } catch { /* ignore network errors during polling */ }
}

function startPolling() {
  stopPolling();
  if (!state.pollingEnabled || state.pollingIntervalSec < 5) return;
  state.pollingTimerId = window.setInterval(() => {
    pollChangestamp();
  }, state.pollingIntervalSec * 1000);
}

function stopPolling() {
  if (state.pollingTimerId) {
    window.clearInterval(state.pollingTimerId);
    state.pollingTimerId = 0;
  }
}

async function boot() {
  bindEvents();
  initAllCustomSelects();
  initAnalyticsDragReorder();
  applyDatePreset(state.filters.datePreset || "last_30_days");
  setMarketplaceFilter(state.filters.marketplace || "");
  if (els.searchInput instanceof HTMLInputElement) {
    els.searchInput.value = String(state.filters.q || "");
  }
  updateSearchTriggerState();
  setTrendGranularity(state.trendGranularity || "auto");
  setCustomerGeoMode(state.customerGeoMode || "map", { skipRender: true });
  setSourcePanelOpen(false);
  setDateRangeMenuOpen(false);
  setChannelMenuOpen(false);
  setSearchModalOpen(false);
  syncGoogleAdsFileLabels();
  setLastSyncInfo(state.lastSyncInfoText);
  if (!els.createBookingDate.value) {
    const now = new Date();
    now.setMinutes(now.getMinutes() - now.getTimezoneOffset());
    els.createBookingDate.value = now.toISOString().slice(0, 10);
  }
  setBookingClass(state.bookingClass || "automatic");
  initSammelrechnungDefaults();
  initSammelPreviewListeners();
  applyInitialViewFromUrl();
  loadPollingSettings();
  applyPollingSettingsToUi();
  await refreshAll();
  if (state.pollingEnabled) {
    startPolling();
  }
}

/* Close custom selects on outside click */
document.addEventListener("click", (e) => {
  if (!e.target.closest(".custom-select-trigger") && !e.target.closest(".custom-select-menu")) {
    closeAllCustomSelects();
  }
});

/* Auto-init custom selects added dynamically to the DOM */
new MutationObserver((mutations) => {
  for (const m of mutations) {
    for (const node of m.addedNodes) {
      if (node.nodeType !== 1) continue;
      if (node.matches && node.matches(".control select:not([hidden]):not([data-customized])")) {
        initCustomSelect(node);
      }
      if (node.querySelectorAll) {
        node.querySelectorAll(".control select:not([hidden]):not([data-customized])").forEach((sel) => {
          initCustomSelect(sel);
        });
      }
    }
  }
}).observe(document.body, { childList: true, subtree: true });

boot().catch((error) => {
  setStatus(`Fehler beim Start: ${error.message}`, "error");
});
