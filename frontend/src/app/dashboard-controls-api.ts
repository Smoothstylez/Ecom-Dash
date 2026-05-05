import type { ShellFilters } from "@/app/dashboard-shell-state";

export type StatusLevel = "info" | "ok" | "error";

export type HealthPayload = {
  sync_status?: {
    runtime?: Record<string, { exists?: boolean }>;
    bootstrap_sources?: Record<string, { exists?: boolean }>;
  };
  live_sync_status?: {
    providers?: Record<string, { configured?: boolean }>;
    background?: {
      enabled?: boolean;
      thread_alive?: boolean;
      interval_seconds?: number;
      last_mode?: string;
      last_status?: string;
      cycle_count?: number;
      next_reconcile_in_cycles?: number;
      last_success_at?: string;
      last_finished_at?: string;
      last_live_result?: {
        providers?: Record<string, {
          status?: string;
          orders_seen?: number;
          orders_inserted?: number;
          orders_updated?: number;
          orders_unchanged?: number;
          duration_seconds?: number;
          updated_at_min?: string;
          ts_created_from_iso?: string;
        }>;
      };
    };
  };
  bookkeeping_module?: {
    mode?: string;
  };
};

export type StatusSnapshot = {
  version: string;
  lastSyncInfo: string;
  sourceInfoHtml: string;
  googleAdsStatusInfoHtml: string;
  customerGeoStatusInfoHtml: string;
};

export type CredentialsPayload = {
  shopifyDomain: string;
  shopifyClientId: string;
  shopifyClientSecret: string;
  kauflandClientKey: string;
  kauflandSecretKey: string;
};

export type CredentialsResponse = {
  ok?: boolean;
  message?: string;
  has_credentials?: boolean;
  shopify_configured?: boolean;
  kaufland_configured?: boolean;
};

export type RestoreResultState = {
  kind: "success" | "error";
  html: string;
};

export type PollingSettings = {
  enabled: boolean;
  intervalSec: number;
};

export const DASHBOARD_VERSION = "0.3.0";
export const POLLING_STORAGE_KEY = "dash-combined.polling";

const API_BASE = "/api";

function ensureErrorMessage(error: unknown, fallback: string) {
  if (error instanceof Error && error.message) {
    return error.message;
  }
  return fallback;
}

function escapeHtml(value: unknown) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatDateTime(value: string | null | undefined) {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return `${new Intl.DateTimeFormat("de-DE").format(date)} ${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
}

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    ...init,
    headers: {
      Accept: "application/json",
      ...(init?.headers || {}),
    },
  });

  if (!response.ok) {
    let detail = "";
    try {
      const payload = await response.json() as { detail?: string; error?: string; message?: string };
      detail = String(payload?.detail || payload?.error || payload?.message || "").trim();
    } catch (_error) {
      detail = "";
    }
    throw new Error(detail || `Request failed: ${response.status}`);
  }

  return (await response.json()) as T;
}

export async function triggerDownload(url: string, fallbackName: string) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Download fehlgeschlagen: ${response.status}`);
  }

  const blob = await response.blob();
  const objectUrl = URL.createObjectURL(blob);
  const downloadLink = document.createElement("a");
  downloadLink.href = objectUrl;
  const disposition = response.headers.get("Content-Disposition") || "";
  const match = disposition.match(/filename\*=UTF-8''([^;]+)|filename="?([^";]+)"?/i);
  const filename = decodeURIComponent(match?.[1] || match?.[2] || fallbackName);
  downloadLink.download = filename;
  document.body.appendChild(downloadLink);
  downloadLink.click();
  downloadLink.remove();
  window.setTimeout(() => {
    URL.revokeObjectURL(objectUrl);
  }, 1000);
}

export function buildStatusSnapshot(
  health: HealthPayload,
  options?: {
    googleAdsStatusInfoHtml?: string;
    customerGeoStatusInfoHtml?: string;
  },
): StatusSnapshot {
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

  const providerLines: string[] = [];
  const providers = liveBackground?.last_live_result?.providers || {};
  for (const [name, info] of Object.entries(providers)) {
    if (!info || info.status === "skipped") {
      continue;
    }
    const seen = Number(info.orders_seen || 0);
    const inserted = Number(info.orders_inserted || 0);
    const updated = Number(info.orders_updated || 0);
    const unchanged = Number(info.orders_unchanged || 0);
    const duration = info.duration_seconds != null ? `${Number(info.duration_seconds).toFixed(1)}s` : "-";
    const filter = info.updated_at_min || info.ts_created_from_iso || null;
    const filterLabel = filter ? ` ab ${formatDateTime(filter)}` : "";
    providerLines.push(`&nbsp;&nbsp;${escapeHtml(name)}: ${escapeHtml(seen)} gesehen, ${escapeHtml(inserted)} neu, ${escapeHtml(updated)} geaendert, ${escapeHtml(unchanged)} unveraendert (${escapeHtml(duration)}${escapeHtml(filterLabel)})`);
  }

  const modeLabel = autoLiveMode === "DELTA" ? "Delta" : autoLiveMode === "RECONCILE" ? "Reconcile" : autoLiveMode;

  return {
    version: DASHBOARD_VERSION,
    lastSyncInfo: lastSyncAt
      ? `Letzter Sync: ${formatDateTime(lastSyncAt)} (${autoLiveStatus}, ${modeLabel})`
      : "Letzter Sync: -",
    sourceInfoHtml: [
      `Local Shopify DB: <strong>${escapeHtml(shopifyFlag)}</strong>`,
      `Local Kaufland DB: <strong>${escapeHtml(kauflandFlag)}</strong>`,
      `Local Buchungen DB: <strong>${escapeHtml(bookkeepingFlag)}</strong>`,
      `Bootstrap Shopify: <strong>${escapeHtml(shopifyBootstrap)}</strong>`,
      `Bootstrap Kaufland: <strong>${escapeHtml(kauflandBootstrap)}</strong>`,
      `Bootstrap Buchungen: <strong>${escapeHtml(bookkeepingBootstrap)}</strong>`,
      `Live Shopify: <strong>${escapeHtml(shopifyLiveReady)}</strong>`,
      `Live Kaufland: <strong>${escapeHtml(kauflandLiveReady)}</strong>`,
      `Auto Live Sync: <strong>${escapeHtml(autoLiveState)}</strong> (alle ${escapeHtml(autoLiveInterval)}s)`,
      `Auto Live Last: <strong>${escapeHtml(autoLiveStatus)}</strong> [${escapeHtml(autoLiveMode)}] - Zyklus ${escapeHtml(cycleCount)}`,
      ...(nextReconcileIn > 0 ? [`Naechster Reconcile: in ${escapeHtml(nextReconcileIn)} Zyklen`] : []),
      ...(providerLines.length > 0 ? ["Letzte Ergebnisse:", ...providerLines] : []),
      `Buchungen Modus: <strong>${escapeHtml(moduleStatus)}</strong>`,
    ].join("<br>"),
    googleAdsStatusInfoHtml: options?.googleAdsStatusInfoHtml || "Lade Google Ads Status...",
    customerGeoStatusInfoHtml: options?.customerGeoStatusInfoHtml || "Noch nicht geladen.",
  };
}

export function loadPollingSettings(): PollingSettings {
  try {
    const raw = localStorage.getItem(POLLING_STORAGE_KEY);
    if (!raw) {
      return { enabled: false, intervalSec: 30 };
    }
    const parsed = JSON.parse(raw) as Partial<PollingSettings>;
    const intervalSec = Number(parsed.intervalSec || 30);
    return {
      enabled: Boolean(parsed.enabled),
      intervalSec: intervalSec >= 5 && intervalSec <= 3600 ? intervalSec : 30,
    };
  } catch (_error) {
    return { enabled: false, intervalSec: 30 };
  }
}

export function persistPollingSettings(settings: PollingSettings) {
  try {
    localStorage.setItem(POLLING_STORAGE_KEY, JSON.stringify(settings));
  } catch (_error) {
    // Ignore storage failures.
  }
}

export function buildPeriodExportUrl(filters: ShellFilters) {
  const params = new URLSearchParams();
  const from = String(filters.from || "").trim();
  const to = String(filters.to || "").trim();
  if (from) {
    params.set("from", from);
  }
  if (to) {
    params.set("to", to);
  }
  if (filters.marketplace) {
    params.set("marketplace", filters.marketplace);
  }
  if (filters.q) {
    params.set("q", filters.q);
  }
  return `${API_BASE}/exports/period?${params.toString()}`;
}

export async function fetchHealthStatus(): Promise<HealthPayload> {
  return fetchJson<HealthPayload>(`${API_BASE}/health`);
}

export async function fetchSyncStatus() {
  return fetchJson<Record<string, unknown>>(`${API_BASE}/sync/status`);
}

export async function fetchLiveSyncStatus() {
  return fetchJson<Record<string, unknown>>(`${API_BASE}/sync/live/status`);
}

export async function fetchLiveBackgroundStatus() {
  return fetchJson<Record<string, unknown>>(`${API_BASE}/sync/live/background/status`);
}

export async function runSourceSyncRequest() {
  return fetchJson<Record<string, unknown>>(`${API_BASE}/sync/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ force: false, include_documents: true }),
  });
}

export async function runLiveSyncRequest() {
  return fetchJson<Record<string, unknown>>(`${API_BASE}/sync/live/run`, {
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
}

export async function fetchGoogleAdsPanelStatusHtml() {
  return fetchGoogleAdsPanelStatusHtmlForFilters();
}

function buildFilterQuery(filters?: Partial<ShellFilters>) {
  const params = new URLSearchParams();
  if (filters?.from) {
    params.set("from", filters.from);
  }
  if (filters?.to) {
    params.set("to", filters.to);
  }
  if (filters?.marketplace) {
    params.set("marketplace", filters.marketplace);
  }
  if (filters?.q) {
    params.set("q", filters.q);
  }
  return params;
}

export async function fetchGoogleAdsPanelStatusHtmlForFilters(filters?: Partial<ShellFilters>) {
  try {
    const params = buildFilterQuery(filters);
    const payload = await fetchJson<{
      kpis?: Record<string, number>;
      imports?: {
        report?: { filename?: string; imported_at?: string; meta?: Record<string, number | string | undefined> };
        assignment?: { filename?: string; imported_at?: string; meta?: Record<string, number | string | undefined> };
      };
    }>(params.size ? `${API_BASE}/google-ads/analytics?${params.toString()}` : `${API_BASE}/google-ads/analytics`);

    const imports = payload.imports || {};
    const reportImport = imports.report || {};
    const assignmentImport = imports.assignment || {};
    const reportMeta = reportImport.meta || {};
    const assignmentMeta = assignmentImport.meta || {};
    const reportFilename = String(reportImport.filename || "-").trim() || "-";
    const reportImportedAt = reportImport.imported_at ? formatDateTime(reportImport.imported_at) : "-";
    const reportFrom = String(reportMeta.report_from_day || "").trim();
    const reportTo = String(reportMeta.report_to_day || "").trim();
    const reportLast = String(reportMeta.report_to_day || reportMeta.last_non_zero_day || "").trim();
    const reportRange = reportFrom && reportTo ? `${escapeHtml(reportFrom)} - ${escapeHtml(reportTo)}` : "-";
    const reportLastLabel = reportLast || "-";
    const reportRows = Number(reportMeta.rows || 0);
    const nonZeroRows = Number(reportMeta.non_zero_rows || 0);
    const assignmentFilename = String(assignmentImport.filename || "-").trim() || "-";
    const assignmentImportedAt = assignmentImport.imported_at ? formatDateTime(assignmentImport.imported_at) : "-";
    const assignmentRows = Number(assignmentMeta.rows || 0);

    return [
      `Report: <strong>${escapeHtml(reportFilename)}</strong>`,
      `Zeitraum: <strong>${reportRange}</strong>`,
      `Letztes Datum: <strong>${escapeHtml(reportLastLabel)}</strong>`,
      `Zeilen: <strong>${escapeHtml(reportRows)}</strong> (mit Kosten: ${escapeHtml(nonZeroRows)})`,
      `Import: <strong>${escapeHtml(reportImportedAt)}</strong>`,
      `Zuweisung: <strong>${escapeHtml(assignmentFilename)}</strong> (${escapeHtml(assignmentRows)} Zeilen, ${escapeHtml(assignmentImportedAt)})`,
    ].join(" &middot; ");
  } catch (_error) {
    return "Google Ads Status konnte nicht geladen werden.";
  }
}

export async function fetchCustomerGeoStatusHtml(filters?: Partial<ShellFilters>) {
  try {
    const params = buildFilterQuery(filters);
    params.set("limit", "2000");
    const startedAt = performance.now();
    const payload = await fetchJson<{
      summary?: Record<string, unknown>;
    }>(`${API_BASE}/customers/locations?${params.toString()}`);
    const summary = payload.summary || {};
    const browserLoadMs = Math.max(0, Math.round(performance.now() - startedAt));
    return [
      `Orders: <strong>${escapeHtml(Number(summary.orders_total || 0).toLocaleString("de-DE"))}</strong> · Punkte: <strong>${escapeHtml(Number(summary.points_total || 0).toLocaleString("de-DE"))}</strong>`,
      `Quelle Koordinaten: <strong>${escapeHtml(Number(summary.resolved_source_coordinates_count || 0).toLocaleString("de-DE"))}</strong> · Geocoded: <strong>${escapeHtml(Number(summary.resolved_geocoded_count || 0).toLocaleString("de-DE"))}</strong> · Country-Fallback: <strong>${escapeHtml(Number(summary.resolved_country_centroid_count || 0).toLocaleString("de-DE"))}</strong>`,
      `Unaufgeloest: <strong>${escapeHtml(Number(summary.unresolved_orders_count || 0).toLocaleString("de-DE"))}</strong> · Geocode Lauf: <strong>${escapeHtml(Number(summary.geocode_successes || 0).toLocaleString("de-DE"))}</strong>/${escapeHtml(Number(summary.geocode_attempts || 0).toLocaleString("de-DE"))} · Geo-Cache Orte: <strong>${escapeHtml(Number(summary.cache_location_hits || 0).toLocaleString("de-DE"))}</strong>`,
      `API: <strong>${summary.cache_hit ? "Cache" : "Frisch"}</strong> · Server: <strong>${escapeHtml(Number(summary.generated_in_ms || 0).toLocaleString("de-DE"))} ms</strong> · Browser: <strong>${escapeHtml(browserLoadMs.toLocaleString("de-DE"))} ms</strong>`,
    ].join("<br>");
  } catch (_error) {
    return "Kunden-Geo Status konnte nicht geladen werden.";
  }
}

export async function saveCredentials(payload: CredentialsPayload) {
  return fetchJson<CredentialsResponse>(`${API_BASE}/sync/credentials`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      shopify_domain: payload.shopifyDomain,
      shopify_client_id: payload.shopifyClientId,
      shopify_client_secret: payload.shopifyClientSecret,
      shopify_api_version: "2025-01",
      kaufland_client_key: payload.kauflandClientKey,
      kaufland_secret_key: payload.kauflandSecretKey,
    }),
  });
}

export async function fetchCredentialsState() {
  return fetchJson<CredentialsResponse>(`${API_BASE}/sync/credentials`);
}

export async function runRestore(file: File): Promise<RestoreResultState> {
  const formData = new FormData();
  formData.append("file", file);

  try {
    const response = await fetch(`${API_BASE}/exports/restore`, {
      method: "POST",
      body: formData,
    });

    const payload = await response.json() as {
      success?: boolean;
      error?: string;
      detail?: string;
      summary?: Record<string, unknown>;
    };

    if (!response.ok || !payload.success) {
      const message = String(payload.error || payload.detail || `HTTP ${response.status}`).trim() || `HTTP ${response.status}`;
      return {
        kind: "error",
        html: `<strong>Fehler:</strong> ${escapeHtml(message)}`,
      };
    }

    const summary = payload.summary || {};
    return {
      kind: "success",
      html: `<strong>Wiederherstellung erfolgreich!</strong><br>Datenbanken: ${escapeHtml(summary.databases_restored || 0)}/${escapeHtml(summary.databases_total || 0)} wiederhergestellt<br>Dateien: ${escapeHtml(summary.storage_files_restored || 0)} wiederhergestellt<br>Backup vom: ${escapeHtml(summary.backup_generated_at || "unbekannt")}`,
    };
  } catch (error) {
    return {
      kind: "error",
      html: `<strong>Netzwerkfehler:</strong> ${escapeHtml(ensureErrorMessage(error, "Unbekannter Fehler"))}`,
    };
  }
}
