import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
} from "react";

import { useDashboardShellState } from "@/app/dashboard-shell-state";
import {
  DASHBOARD_VERSION,
  buildPeriodExportUrl,
  buildStatusSnapshot,
  fetchCredentialsState,
  fetchCustomerGeoStatusHtml,
  fetchGoogleAdsPanelStatusHtmlForFilters,
  fetchHealthStatus,
  loadPollingSettings,
  loadAdminToken,
  persistAdminToken,
  persistPollingSettings,
  runLiveSyncRequest,
  runRestore,
  runSourceSyncRequest,
  triggerDownload,
  type PollingSettings,
  type RestoreResultState,
  type StatusLevel,
  type StatusSnapshot,
} from "@/app/dashboard-controls-api";

type StatusMessage = {
  text: string;
  level: StatusLevel;
};

type RestoreState = {
  file: File | null;
  label: string;
  info: string;
  showConfirm: boolean;
  loading: boolean;
  result: RestoreResultState | null;
};

function defaultStatusSnapshot(): StatusSnapshot {
  return {
    version: DASHBOARD_VERSION,
    lastSyncInfo: "Letzter Sync: -",
    sourceInfoHtml: "Lade Quellenstatus...",
    googleAdsStatusInfoHtml: "Lade Google Ads Status...",
    customerGeoStatusInfoHtml: "Noch nicht geladen.",
  };
}

function defaultRestoreState(): RestoreState {
  return {
    file: null,
    label: "ZIP-Datei waehlen...",
    info: "",
    showConfirm: false,
    loading: false,
    result: null,
  };
}

function formatFileSize(bytes: number) {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(1)} KB`;
  }
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function classNames(...parts: Array<string | false | null | undefined>) {
  return parts.filter(Boolean).join(" ");
}

export function DashboardControls() {
  const { filters, closeSettingsPanelRequestToken, requestOpenThemeModal, requestRefresh } = useDashboardShellState();
  const [isStatusOpen, setStatusOpen] = useState(false);
  const [isSettingsOpen, setSettingsOpen] = useState(false);
  const [isDataOpen, setDataOpen] = useState(false);
  const [statusSnapshot, setStatusSnapshot] = useState<StatusSnapshot>(() => defaultStatusSnapshot());
  const [statusMessage, setStatusMessage] = useState<StatusMessage>({ text: "", level: "info" });
  const [credentialsPlaceholderState, setCredentialsPlaceholderState] = useState({
    shopifyConfigured: false,
    kauflandConfigured: false,
    storage: "environment",
  });
  const [credentialsStatus, setCredentialsStatus] = useState({ text: "", level: "info" as StatusLevel });
  const [restoreState, setRestoreState] = useState<RestoreState>(() => defaultRestoreState());
  const [pollingSettings, setPollingSettings] = useState<PollingSettings>(() => loadPollingSettings());
  const [adminToken, setAdminToken] = useState(() => loadAdminToken());
  const [syncBusy, setSyncBusy] = useState(false);

  const pollingTimerRef = useRef<number | null>(null);
  const pollingLastStampRef = useRef(0);
  const credentialsTimerRef = useRef<number | null>(null);

  const statusBoxClassName = useMemo(() => {
    const suffix = statusMessage.level === "error" ? "status-error" : statusMessage.level === "ok" ? "status-ok" : "status-info";
    return classNames("status", suffix);
  }, [statusMessage.level]);

  const isAnalyticsRoute = window.location.pathname === "/analytics";

  const applyStatus = useCallback((message: string, level: StatusLevel = "info") => {
    setStatusMessage({ text: String(message || "").trim(), level });
  }, []);

  const updateLastSyncInfo = useCallback((message: string) => {
    setStatusSnapshot((current) => ({
      ...current,
      lastSyncInfo: String(message || "").trim() || "Letzter Sync: -",
    }));
  }, []);

  const loadStatusPanel = useCallback(async () => {
    const [health, googleAdsStatusInfoHtml, customerGeoStatusInfoHtml] = await Promise.all([
      fetchHealthStatus(),
      fetchGoogleAdsPanelStatusHtmlForFilters(filters),
      fetchCustomerGeoStatusHtml(filters),
    ]);
    setStatusSnapshot(buildStatusSnapshot(health, {
      googleAdsStatusInfoHtml,
      customerGeoStatusInfoHtml,
    }));
  }, [filters]);

  const refreshAll = useCallback(async () => {
    applyStatus("Lade Daten...", "info");
    try {
      await Promise.all([
        loadStatusPanel(),
        Promise.resolve().then(() => {
          requestRefresh();
        }),
      ]);
      applyStatus("Daten aktualisiert.", "ok");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Daten konnten nicht aktualisiert werden.";
      applyStatus(`Fehler beim Laden: ${message}`, "error");
      throw error;
    }
  }, [applyStatus, loadStatusPanel, requestRefresh]);

  const closeStatusPanel = useCallback(() => {
    setStatusOpen(false);
  }, []);

  const openStatusPanel = useCallback(async () => {
    setSettingsOpen(false);
    setStatusOpen(true);
    try {
      await loadStatusPanel();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Status konnte nicht geladen werden.";
      applyStatus(`Status konnte nicht geladen werden: ${message}`, "error");
    }
  }, [applyStatus, loadStatusPanel]);

  const closeSettingsPanel = useCallback(() => {
    setSettingsOpen(false);
  }, []);

  const openSettingsPanel = useCallback(() => {
    setStatusOpen(false);
    setSettingsOpen((current) => !current);
  }, []);

  const toggleStatusPanel = useCallback(() => {
    if (isStatusOpen) {
      closeStatusPanel();
      return;
    }
    void openStatusPanel();
  }, [closeStatusPanel, isStatusOpen, openStatusPanel]);

  const closeDataModal = useCallback(() => {
    setDataOpen(false);
  }, []);

  const openDataModal = useCallback(() => {
    setSettingsOpen(false);
    setRestoreState(defaultRestoreState());
    setDataOpen(true);
  }, []);

  useEffect(() => {
    if (closeSettingsPanelRequestToken === 0) {
      return;
    }
    setSettingsOpen(false);
  }, [closeSettingsPanelRequestToken]);

  const runSourceSync = useCallback(async () => {
    setSyncBusy(true);
    applyStatus("Synchronisiere lokale Datenquellen...", "info");
    try {
      const payload = await runSourceSyncRequest();
      const results = payload && typeof payload.results === "object" ? payload.results as Record<string, Record<string, unknown>> : {};
      const copiedDbs = [results.shopify_db?.copied, results.kaufland_db?.copied, results.bookkeeping_db?.copied].filter(Boolean).length;
      const copiedDocs = Number(results.bookkeeping_documents?.copied_files || 0);
      updateLastSyncInfo(`Letzter Sync: Quellen-Sync (${new Intl.DateTimeFormat("de-DE").format(new Date())})`);
      await refreshAll();
      applyStatus(`Sync fertig: ${copiedDbs} DB(s) aktualisiert, ${copiedDocs.toLocaleString("de-DE")} Dokument(e) kopiert.`, "ok");
      return payload;
    } catch (error) {
      const message = error instanceof Error ? error.message : "Sync fehlgeschlagen";
      applyStatus(`Sync fehlgeschlagen: ${message}`, "error");
      throw error;
    } finally {
      setSyncBusy(false);
    }
  }, [applyStatus, refreshAll, updateLastSyncInfo]);

  const runLiveSync = useCallback(async () => {
    setSyncBusy(true);
    applyStatus("Starte Live API Sync fuer Shopify/Kaufland...", "info");
    try {
      const payload = await runLiveSyncRequest();
      const results = payload && typeof payload.results === "object" ? payload.results as Record<string, Record<string, unknown>> : {};
      const shopifyStatus = String(results.shopify?.status || "unknown");
      const kauflandStatus = String(results.kaufland?.status || "unknown");
      const shopifySummary = results.shopify?.summary && typeof results.shopify.summary === "object"
        ? results.shopify.summary as Record<string, unknown>
        : null;
      const kauflandSummary = results.kaufland?.summary && typeof results.kaufland.summary === "object"
        ? results.kaufland.summary as Record<string, unknown>
        : null;
      const shopifyDetail = shopifySummary && "orders_seen" in shopifySummary
        ? ` (${Number(shopifySummary.orders_seen || 0)} gesehen, ${Number(shopifySummary.orders_inserted || 0)} neu, ${Number(shopifySummary.orders_updated || 0)} geaendert)`
        : "";
      const kauflandDetail = kauflandSummary && "orders_seen" in kauflandSummary
        ? ` (${Number(kauflandSummary.orders_seen || 0)} gesehen, ${Number(kauflandSummary.orders_inserted || 0)} neu, ${Number(kauflandSummary.orders_updated || 0)} geaendert)`
        : "";
      updateLastSyncInfo(`Letzter Sync: Live API (${new Intl.DateTimeFormat("de-DE").format(new Date())})`);
      await refreshAll();
      applyStatus(`Live Sync fertig: Shopify=${shopifyStatus}${shopifyDetail}, Kaufland=${kauflandStatus}${kauflandDetail}.`, "ok");
      return payload;
    } catch (error) {
      const message = error instanceof Error ? error.message : "Live Sync fehlgeschlagen";
      applyStatus(`Live Sync fehlgeschlagen: ${message}`, "error");
      throw error;
    } finally {
      setSyncBusy(false);
    }
  }, [applyStatus, refreshAll, updateLastSyncInfo]);

  const handlePollingToggle = useCallback((enabled: boolean) => {
    const nextSettings = {
      ...pollingSettings,
      enabled,
    };
    setPollingSettings(nextSettings);
    persistPollingSettings(nextSettings);
  }, [pollingSettings]);

  const handlePollingInterval = useCallback((value: number) => {
    const nextInterval = Number.isFinite(value) ? Math.max(5, Math.min(3600, value)) : pollingSettings.intervalSec;
    const nextSettings = {
      ...pollingSettings,
      intervalSec: nextInterval,
    };
    setPollingSettings(nextSettings);
    persistPollingSettings(nextSettings);
  }, [pollingSettings]);

  const loadCredentials = useCallback(async () => {
    try {
      const payload = await fetchCredentialsState();
      setCredentialsPlaceholderState({
        shopifyConfigured: Boolean(payload.shopify_configured),
        kauflandConfigured: Boolean(payload.kaufland_configured),
        storage: String(payload.storage || "environment"),
      });
    } catch (_error) {
      setCredentialsPlaceholderState({
        shopifyConfigured: false,
        kauflandConfigured: false,
        storage: "environment",
      });
    }
  }, []);

  useEffect(() => {
    void loadCredentials();
  }, [loadCredentials]);

  useEffect(() => {
    if (!isStatusOpen) {
      return;
    }
    void loadStatusPanel();
  }, [isStatusOpen, loadStatusPanel]);

  useEffect(() => {
    const statusButton = document.getElementById("sidebarStatusBtn");
    const settingsButton = document.getElementById("sidebarSettingsBtn");
    const syncButton = document.getElementById("sidebarSyncBtn");

    const handleStatusClick = () => {
      toggleStatusPanel();
    };
    const handleSettingsClick = () => {
      openSettingsPanel();
    };
    const handleSyncClick = () => {
      void runSourceSync();
    };

    statusButton?.addEventListener("click", handleStatusClick);
    settingsButton?.addEventListener("click", handleSettingsClick);
    syncButton?.addEventListener("click", handleSyncClick);

    return () => {
      statusButton?.removeEventListener("click", handleStatusClick);
      settingsButton?.removeEventListener("click", handleSettingsClick);
      syncButton?.removeEventListener("click", handleSyncClick);
    };
  }, [openSettingsPanel, runSourceSync, toggleStatusPanel]);

  useEffect(() => {
    const statusButton = document.getElementById("sidebarStatusBtn");
    const settingsButton = document.getElementById("sidebarSettingsBtn");
    const syncButton = document.getElementById("sidebarSyncBtn");

    if (statusButton instanceof HTMLElement) {
      statusButton.classList.toggle("active", isStatusOpen);
      statusButton.setAttribute("aria-expanded", isStatusOpen ? "true" : "false");
    }
    if (settingsButton instanceof HTMLElement) {
      settingsButton.classList.toggle("active", isSettingsOpen);
      settingsButton.setAttribute("aria-expanded", isSettingsOpen ? "true" : "false");
    }
    if (syncButton instanceof HTMLButtonElement) {
      syncButton.disabled = syncBusy;
      syncButton.classList.toggle("active", syncBusy);
      const label = syncButton.querySelector("span");
      if (label instanceof HTMLElement) {
        label.textContent = syncBusy ? "Sync..." : "Sync";
      }
    }
  }, [isSettingsOpen, isStatusOpen, syncBusy]);

  useEffect(() => {
    return () => {
      if (pollingTimerRef.current) {
        window.clearInterval(pollingTimerRef.current);
        pollingTimerRef.current = null;
      }
      if (credentialsTimerRef.current) {
        window.clearTimeout(credentialsTimerRef.current);
        credentialsTimerRef.current = null;
      }
    };
  }, []);

  useEffect(() => {
    if (pollingTimerRef.current) {
      window.clearInterval(pollingTimerRef.current);
      pollingTimerRef.current = null;
    }
    if (!pollingSettings.enabled) {
      return;
    }

    pollingTimerRef.current = window.setInterval(async () => {
      try {
        const response = await fetch("/api/sync/changestamp", { headers: { Accept: "application/json" } });
        if (!response.ok) {
          return;
        }
        const payload = await response.json() as { stamp?: number };
        const nextStamp = Number(payload.stamp || 0);
        if (pollingLastStampRef.current === 0) {
          pollingLastStampRef.current = nextStamp;
          return;
        }
        if (nextStamp !== pollingLastStampRef.current) {
          pollingLastStampRef.current = nextStamp;
          requestRefresh();
          if (isStatusOpen) {
            await loadStatusPanel();
          }
        }
      } catch (_error) {
        // Ignore polling errors.
      }
    }, pollingSettings.intervalSec * 1000);

    return () => {
      if (pollingTimerRef.current) {
        window.clearInterval(pollingTimerRef.current);
        pollingTimerRef.current = null;
      }
    };
  }, [isStatusOpen, loadStatusPanel, pollingSettings.enabled, pollingSettings.intervalSec, requestRefresh]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape") {
        return;
      }
      closeStatusPanel();
      closeSettingsPanel();
      closeDataModal();
    };
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [closeDataModal, closeSettingsPanel, closeStatusPanel]);

  const handleCredentialsHelp = useCallback(() => {
    setCredentialsStatus({
      text: "Credentials werden nur noch ueber Umgebungsvariablen oder Home-Assistant-Optionen bereitgestellt, nicht mehr im Browser gespeichert.",
      level: "info",
    });
    if (credentialsTimerRef.current) {
      window.clearTimeout(credentialsTimerRef.current);
    }
    credentialsTimerRef.current = window.setTimeout(() => {
      setCredentialsStatus({ text: "", level: "info" });
    }, 5000);
  }, []);

  const handleAdminTokenSave = useCallback(() => {
    persistAdminToken(adminToken);
    applyStatus(adminToken.trim() ? "Admin-Token lokal gespeichert." : "Admin-Token entfernt.", "ok");
  }, [adminToken, applyStatus]);

  const handleRestoreFileChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0] || null;
    if (!file) {
      setRestoreState(defaultRestoreState());
      return;
    }
    if (!file.name.toLowerCase().endsWith(".zip")) {
      applyStatus("Nur ZIP-Dateien werden akzeptiert.", "error");
      setRestoreState(defaultRestoreState());
      return;
    }
    setRestoreState({
      file,
      label: file.name,
      info: `Datei: ${file.name} (${formatFileSize(file.size)})`,
      showConfirm: true,
      loading: false,
      result: null,
    });
  }, [applyStatus]);

  const handleRestoreConfirm = useCallback(async () => {
    if (!restoreState.file) {
      applyStatus("Keine Datei ausgewaehlt.", "error");
      return;
    }

    setRestoreState((current) => ({
      ...current,
      loading: true,
      showConfirm: false,
      result: null,
    }));
    const result = await runRestore(restoreState.file);
    setRestoreState((current) => ({
      ...current,
      loading: false,
      label: "ZIP-Datei waehlen...",
      file: null,
      result,
    }));
    if (result.kind === "success") {
      applyStatus("Wiederherstellung erfolgreich. Daten werden neu geladen...", "ok");
      await refreshAll();
      applyStatus("Wiederherstellung abgeschlossen. Alle Daten neu geladen.", "ok");
    } else {
      applyStatus("Wiederherstellung fehlgeschlagen.", "error");
    }
  }, [applyStatus, refreshAll, restoreState.file]);

  const handlePeriodExport = useCallback(async () => {
    if (!filters.from || !filters.to) {
      applyStatus("Bitte zuerst Von und Bis setzen fuer den Zeitraum-Export.", "error");
      return;
    }
    closeDataModal();
    try {
      await triggerDownload(buildPeriodExportUrl(filters), "combined_period_export.zip");
      applyStatus("Zeitraum-Export heruntergeladen (ZIP mit CSV + Belegen).", "ok");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Zeitraum-Export fehlgeschlagen";
      applyStatus(`Zeitraum-Export fehlgeschlagen: ${message}`, "error");
    }
  }, [applyStatus, closeDataModal, filters]);

  const handleBackupExport = useCallback(async () => {
    closeDataModal();
    try {
      await triggerDownload("/api/exports/backup", "combined_full_backup.zip");
      applyStatus("Vollbackup heruntergeladen (ZIP Snapshot).", "ok");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Vollbackup fehlgeschlagen";
      applyStatus(`Vollbackup fehlgeschlagen: ${message}`, "error");
    }
  }, [applyStatus, closeDataModal]);

  return (
    <>
      <div
        id="statusPanel"
        className={classNames("sidebar-modal", isStatusOpen && "active")}
        aria-hidden={isStatusOpen ? "false" : "true"}
        data-react-owned="true"
        onClick={(event) => {
          if (event.target === event.currentTarget) {
            closeStatusPanel();
          }
        }}
      >
        <div className="sidebar-modal-content">
          <div className="sidebar-modal-header">
            <h2>Status</h2>
            <button id="closeStatusBtn" className="sidebar-modal-close" type="button" onClick={() => closeStatusPanel()}>&times;</button>
          </div>
          <div className="sidebar-modal-body">
            <div className="settings-group">
              <div className="settings-group-label">System</div>
              <div className="status-item"><span>Version:</span> <span id="statusVersion">{statusSnapshot.version}</span></div>
              <div id="lastSyncInfo" className="settings-status-line">{statusSnapshot.lastSyncInfo}</div>
              <div className="settings-source-row">
                <span className="settings-source-label">Datenquellen</span>
                <div id="sourceInfo" className="settings-source-detail" dangerouslySetInnerHTML={{ __html: statusSnapshot.sourceInfoHtml }} />
              </div>
              <div className="settings-source-row">
                <span className="settings-source-label">Google Ads</span>
                <div id="googleAdsStatusInfo" className="settings-source-detail" dangerouslySetInnerHTML={{ __html: statusSnapshot.googleAdsStatusInfoHtml }} />
              </div>
              <div className="settings-source-row">
                <span className="settings-source-label">Kunden-Geo</span>
                <div id="customerGeoStatusInfo" className="settings-source-detail" dangerouslySetInnerHTML={{ __html: statusSnapshot.customerGeoStatusInfoHtml }} />
              </div>
            </div>
          </div>
        </div>
      </div>

      <div
        id="settingsPanel"
        className={classNames("sidebar-modal", isSettingsOpen && "active")}
        aria-hidden={isSettingsOpen ? "false" : "true"}
        data-react-owned="true"
        onClick={(event) => {
          if (event.target === event.currentTarget) {
            closeSettingsPanel();
          }
        }}
      >
        <div className="sidebar-modal-content">
          <div className="sidebar-modal-header">
            <h2>Einstellungen</h2>
            <button id="closeSettingsBtn" className="sidebar-modal-close" type="button" onClick={() => closeSettingsPanel()}>&times;</button>
          </div>
          <div className="sidebar-modal-body">
            <div className="settings-group">
              <div className="settings-group-label">Ansicht</div>
              <button id="layoutEditMenuBtn" className="settings-item" type="button" disabled={!isAnalyticsRoute}>Layout anpassen</button>
              <button id="themeModalOpenBtn" className="settings-item" type="button" onClick={() => requestOpenThemeModal()}>Design</button>
            </div>
            <div className="settings-group">
              <div className="settings-group-label">Aktionen</div>
              <button id="refreshBtn" className="settings-item" type="button" onClick={() => void refreshAll()}>Alles neu laden</button>
              <button id="syncLiveBtn" className="settings-item settings-item-accent" type="button" disabled={syncBusy} onClick={() => void runLiveSync()}>Live API Sync</button>
              <button id="syncSourcesBtn" className="settings-item" type="button" disabled={syncBusy} onClick={() => void runSourceSync()}>Quellen synchronisieren</button>
              <button id="dataModalOpenBtn" className="settings-item" type="button" onClick={() => openDataModal()}>Datenverwaltung</button>
            </div>
            <div className="settings-group">
              <div className="settings-group-label">Auto-Aktualisierung</div>
              <label className="settings-toggle-row">
                <span className="settings-toggle-label">Aktiv</span>
                <input
                  id="pollingToggle"
                  type="checkbox"
                  className="settings-toggle-input"
                  checked={pollingSettings.enabled}
                  onChange={(event) => {
                    handlePollingToggle(event.target.checked);
                  }}
                />
                <span className="settings-toggle-switch" />
              </label>
              <label className="settings-inline-row">
                <span className="settings-toggle-label">Intervall (Sek.)</span>
                <input
                  id="pollingIntervalInput"
                  type="number"
                  min="5"
                  max="3600"
                  value={pollingSettings.intervalSec}
                  className="settings-inline-input"
                  onChange={(event) => {
                    handlePollingInterval(Number(event.target.value));
                  }}
                />
              </label>
            </div>
            <div className="settings-section">
              <h3>API Credentials</h3>
              <div className="credentials-form">
                <div className="credentials-group">
                  <h4>Shopify</h4>
                  <div className="settings-status-line">{credentialsPlaceholderState.shopifyConfigured ? "Shopify ist ueber die Laufzeitumgebung konfiguriert." : "Shopify ist aktuell nicht ueber die Laufzeitumgebung konfiguriert."}</div>
                </div>
                <div className="credentials-group">
                  <h4>Kaufland</h4>
                  <div className="settings-status-line">{credentialsPlaceholderState.kauflandConfigured ? "Kaufland ist ueber die Laufzeitumgebung konfiguriert." : "Kaufland ist aktuell nicht ueber die Laufzeitumgebung konfiguriert."}</div>
                </div>
                <div className="settings-status-line">Quelle: {credentialsPlaceholderState.storage === "environment" ? "Umgebungsvariablen / Home-Assistant-Optionen" : credentialsPlaceholderState.storage}</div>
                <button id="btnSaveCredentials" className="btn-inline primary" type="button" onClick={() => handleCredentialsHelp()}>Hinweis anzeigen</button>
                <div id="credentialsStatus" className={classNames("credentials-status", credentialsStatus.level === "ok" && "success", credentialsStatus.level === "error" && "error")}>{credentialsStatus.text}</div>
              </div>
            </div>
            <div className="settings-section">
              <h3>Admin Zugriff</h3>
              <div className="credentials-form">
                <div className="settings-status-line">Gespeichert nur lokal in diesem Browser. Wird fuer geschuetzte Sync-, Backup-, Restore- und Schreibaktionen genutzt.</div>
                <input
                  id="adminTokenInput"
                  type="password"
                  className="settings-inline-input"
                  value={adminToken}
                  placeholder="APP_ADMIN_TOKEN"
                  onChange={(event) => {
                    setAdminToken(event.target.value);
                  }}
                />
                <div className="data-restore-actions">
                  <button id="adminTokenSaveBtn" className="btn-inline primary" type="button" onClick={() => handleAdminTokenSave()}>Token speichern</button>
                  <button id="adminTokenClearBtn" className="btn-inline ghost" type="button" onClick={() => { setAdminToken(""); persistAdminToken(""); applyStatus("Admin-Token entfernt.", "ok"); }}>Token entfernen</button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div
        id="dataModal"
        className={classNames("modal", isDataOpen && "active")}
        role="dialog"
        aria-modal="true"
        aria-hidden={isDataOpen ? "false" : "true"}
        data-react-owned="true"
        onClick={(event) => {
          if (event.target === event.currentTarget) {
            closeDataModal();
          }
        }}
      >
        <div className="modal-card data-modal-card">
          <div className="modal-head">
            <div className="modal-title">Datenverwaltung</div>
            <button id="dataModalCloseBtn" className="modal-close" type="button" aria-label="Schliessen" onClick={() => closeDataModal()}>&times;</button>
          </div>
          <div className="modal-body data-modal-body">
            <div className="data-section">
              <div className="data-section-head"><span>Export</span></div>
              <div className="data-section-body">
                <button id="dataExportPeriodBtn" className="data-action-btn" type="button" onClick={() => void handlePeriodExport()}>
                  <div className="data-action-label">Zeitraum-Export</div>
                  <div className="data-action-desc">CSV-Dateien + Belege fuer den aktuellen Filterbereich</div>
                </button>
                <button id="dataExportBackupBtn" className="data-action-btn" type="button" onClick={() => void handleBackupExport()}>
                  <div className="data-action-label">Vollbackup</div>
                  <div className="data-action-desc">Kompletter Snapshot aller Datenbanken, Belege und Dokumente</div>
                </button>
              </div>
            </div>

            <div className="data-section">
              <div className="data-section-head"><span>Wiederherstellen</span></div>
              <div className="data-section-body">
                <div className="data-restore-info">Lade eine Vollbackup-ZIP-Datei hoch, um alle Datenbanken und Dateien wiederherzustellen. Vor der Wiederherstellung wird automatisch ein Sicherheitsbackup angelegt.</div>
                <div className="data-restore-upload">
                  <label htmlFor="restoreFileInput" className="data-upload-label">
                    <span id="restoreFileLabel">{restoreState.label}</span>
                  </label>
                  <input id="restoreFileInput" type="file" accept=".zip" style={{ display: "none" }} onChange={handleRestoreFileChange} />
                </div>
                <div id="restoreConfirmSection" className="data-restore-confirm" style={{ display: restoreState.showConfirm ? undefined : "none" }}>
                  <div id="restoreFileInfo" className="data-restore-file-info">{restoreState.info}</div>
                  <div className="data-restore-warning"><strong>Achtung:</strong> Alle aktuellen Daten werden ueberschrieben. Ein Sicherheitsbackup wird automatisch erstellt.</div>
                  <div className="data-restore-actions">
                    <button id="restoreCancelBtn" className="btn-inline ghost" type="button" onClick={() => setRestoreState(defaultRestoreState())}>Abbrechen</button>
                    <button id="restoreConfirmBtn" className="btn-inline data-restore-confirm-btn" type="button" onClick={() => void handleRestoreConfirm()}>Wiederherstellen</button>
                  </div>
                </div>
                <div id="restoreProgress" className="data-restore-progress" style={{ display: restoreState.loading ? "flex" : "none" }}>
                  <div className="data-restore-spinner" />
                  <span>Wiederherstellung laeuft...</span>
                </div>
                <div
                  id="restoreResult"
                  className={classNames("data-restore-result", restoreState.result?.kind === "success" && "success", restoreState.result?.kind === "error" && "error")}
                  style={{ display: restoreState.result ? undefined : "none" }}
                  dangerouslySetInnerHTML={{ __html: restoreState.result?.html || "" }}
                />
              </div>
            </div>
          </div>
        </div>
      </div>

      <div id="statusBox" className={statusBoxClassName} style={{ display: statusMessage.text ? undefined : "none" }}>
        {statusMessage.text}
      </div>
    </>
  );
}
