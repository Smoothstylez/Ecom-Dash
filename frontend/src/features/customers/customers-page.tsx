import { formatDateToken, formatMoneyFromCents, formatPercent, NUMBER_FORMATTER } from "@/features/analytics/format";
import { useTheme } from "@/shared/theme/theme-provider";
import { useDashboardShellState } from "@/app/dashboard-shell-state";
import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";

import {
  fetchCustomerLocations,
  fetchCustomersOverview,
  type CustomerGeoPoint,
  type CustomerGeoSummary,
  type CustomerItem,
  type CustomerKpis,
} from "./api";
import { loadCustomerGlobeLibraries, loadCustomerMapLibrary } from "./customer-geo-libs";

declare global {
  interface Window {
    L?: {
      map: (element: HTMLElement, options?: Record<string, unknown>) => CustomerLeafletMap;
      tileLayer: (url: string, options?: Record<string, unknown>) => { addTo: (map: CustomerLeafletMap) => void };
      layerGroup: () => CustomerLeafletLayer;
      circleMarker: (coords: [number, number], options?: Record<string, unknown>) => CustomerLeafletMarker;
    };
    Globe?: new (container: HTMLElement, options?: Record<string, unknown>) => CustomerGlobeInstance;
    topojson?: {
      feature: (atlas: unknown, object: unknown) => { features?: unknown[] };
    };
    THREE?: {
      Color: new (value: string) => unknown;
    };
  }
}

type CustomerLeafletMap = {
  setView: (coords: [number, number], zoom: number) => void;
  fitBounds: (bounds: Array<[number, number]>, options?: Record<string, unknown>) => void;
  invalidateSize: () => void;
  remove: () => void;
};

type CustomerLeafletLayer = {
  addTo: (map: CustomerLeafletMap) => CustomerLeafletLayer;
  clearLayers: () => void;
};

type CustomerLeafletMarker = {
  bindPopup: (html: string) => void;
  addTo: (layer: CustomerLeafletLayer) => void;
};

type CustomerGlobeRenderer = {
  dispose?: () => void;
  forceContextLoss?: () => void;
  domElement?: {
    parentNode?: {
      removeChild?: (node: unknown) => void;
    };
  };
  render?: (scene: unknown, camera: unknown) => void;
  setAnimationLoop?: (callback: ((time?: number) => void) | null) => void;
};

type CustomerGlobeControls = {
  enabled?: boolean;
  autoRotate?: boolean;
  enablePan?: boolean;
  enableDamping?: boolean;
  dampingFactor?: number;
  minDistance?: number;
  maxDistance?: number;
  update?: () => void;
};

type CustomerGlobeHex = {
  sumWeight?: number;
  center?: {
    lat?: number;
    lng?: number;
  };
};

type CustomerGlobePoint = {
  lat: number;
  lng: number;
  weight: number;
};

type CustomerGlobeInstance = {
  backgroundColor?: (value: string) => CustomerGlobeInstance;
  atmosphereColor?: (value: string) => CustomerGlobeInstance;
  hexTopColor?: (fn: (hex?: CustomerGlobeHex) => string) => CustomerGlobeInstance;
  hexSideColor?: (fn: (hex?: CustomerGlobeHex) => string) => CustomerGlobeInstance;
  hexPolygonColor?: (fn: () => string) => CustomerGlobeInstance;
  globeMaterial?: () => {
    color?: unknown;
    emissive?: unknown;
    emissiveIntensity?: number;
    shininess?: number;
  } | null;
  renderer?: () => CustomerGlobeRenderer | null;
  controls?: () => CustomerGlobeControls | null;
  scene?: () => unknown;
  camera?: () => unknown;
  showGlobe?: (value: boolean) => CustomerGlobeInstance;
  showAtmosphere?: (value: boolean) => CustomerGlobeInstance;
  atmosphereAltitude?: (value: number) => CustomerGlobeInstance;
  showGraticules?: (value: boolean) => CustomerGlobeInstance;
  hexBinResolution?: (value: number) => CustomerGlobeInstance;
  hexMargin?: (value: number) => CustomerGlobeInstance;
  hexTopCurvatureResolution?: (value: number) => CustomerGlobeInstance;
  hexAltitude?: (fn: (hex?: CustomerGlobeHex) => number) => CustomerGlobeInstance;
  hexLabel?: (fn: (hex?: CustomerGlobeHex) => string) => CustomerGlobeInstance;
  pointOfView?: (value: Record<string, number>, duration?: number) => CustomerGlobeInstance;
  width?: (value: number) => CustomerGlobeInstance;
  height?: (value: number) => CustomerGlobeInstance;
  hexBinPointsData?: (points: CustomerGlobePoint[]) => CustomerGlobeInstance;
  hexBinPointLat?: (fn: (point: CustomerGlobePoint) => number) => CustomerGlobeInstance;
  hexBinPointLng?: (fn: (point: CustomerGlobePoint) => number) => CustomerGlobeInstance;
  hexBinPointWeight?: (fn: (point: CustomerGlobePoint) => number) => CustomerGlobeInstance;
  hexPolygonsData?: (features: unknown[]) => CustomerGlobeInstance;
  hexPolygonResolution?: (value: number) => CustomerGlobeInstance;
  hexPolygonMargin?: (value: number) => CustomerGlobeInstance;
  hexPolygonAltitude?: (value: number) => CustomerGlobeInstance;
  hexPolygonUseDots?: (value: boolean) => CustomerGlobeInstance;
  _destructor?: () => void;
};

type CustomersOverview = {
  total: number;
  kpis: CustomerKpis;
  items: CustomerItem[];
};

type CustomersGeo = {
  summary: CustomerGeoSummary;
  points: CustomerGeoPoint[];
};

type GeoMode = "map" | "globe";

type GeoLogEntry = {
  message: string;
};

type GeoStatus = {
  loading: boolean;
  text: string;
  browserLoadMs: number;
  log: GeoLogEntry[];
};

const INITIAL_OVERVIEW: CustomersOverview = {
  total: 0,
  kpis: {},
  items: [],
};

const INITIAL_GEO: CustomersGeo = {
  summary: {},
  points: [],
};

function safeText(value: unknown) {
  return String(value || "").trim();
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

function formatFixed(value: number) {
  return Number(value || 0).toLocaleString("de-DE", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function normalizeOverview(payload?: Partial<CustomersOverview>): CustomersOverview {
  return {
    total: Number(payload?.total || 0),
    kpis: payload?.kpis && typeof payload.kpis === "object" ? payload.kpis : {},
    items: Array.isArray(payload?.items) ? payload.items : [],
  };
}

function normalizeGeo(payload?: Partial<CustomersGeo>): CustomersGeo {
  return {
    summary: payload?.summary && typeof payload.summary === "object" ? payload.summary : {},
    points: Array.isArray(payload?.points) ? payload.points : [],
  };
}

function normalizeGeoMode(value: string): GeoMode {
  return String(value || "").trim().toLowerCase() === "globe" ? "globe" : "map";
}

function logGeoEvent(message: string, current: GeoLogEntry[]) {
  const timestamp = new Date();
  const hh = String(timestamp.getHours()).padStart(2, "0");
  const mm = String(timestamp.getMinutes()).padStart(2, "0");
  const ss = String(timestamp.getSeconds()).padStart(2, "0");
  const next = [{ message: `${hh}:${mm}:${ss} ${safeText(message)}` }, ...current];
  return next.slice(0, 6);
}

function escapeHtml(value: string) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function customerGeoStatusHtml(summary: CustomerGeoSummary, status: GeoStatus) {
  const lines = [
    `Orders: <strong>${escapeHtml(NUMBER_FORMATTER.format(Number(summary.orders_total || 0)))}</strong> · Punkte: <strong>${escapeHtml(NUMBER_FORMATTER.format(Number(summary.points_total || 0)))}</strong>`,
    `Quelle Koordinaten: <strong>${escapeHtml(NUMBER_FORMATTER.format(Number(summary.resolved_source_coordinates_count || 0)))}</strong> · Geocoded: <strong>${escapeHtml(NUMBER_FORMATTER.format(Number(summary.resolved_geocoded_count || 0)))}</strong> · Country-Fallback: <strong>${escapeHtml(NUMBER_FORMATTER.format(Number(summary.resolved_country_centroid_count || 0)))}</strong>`,
    `Unaufgeloest: <strong>${escapeHtml(NUMBER_FORMATTER.format(Number(summary.unresolved_orders_count || 0)))}</strong> · Geocode Lauf: <strong>${escapeHtml(NUMBER_FORMATTER.format(Number(summary.geocode_successes || 0)))}</strong>/${escapeHtml(NUMBER_FORMATTER.format(Number(summary.geocode_attempts || 0)))} · Geo-Cache Orte: <strong>${escapeHtml(NUMBER_FORMATTER.format(Number(summary.cache_location_hits || 0)))}</strong>`,
    `API: <strong>${summary.cache_hit ? "Cache" : "Frisch"}</strong> · Server: <strong>${escapeHtml(NUMBER_FORMATTER.format(Number(summary.generated_in_ms || 0)))} ms</strong> · Browser: <strong>${escapeHtml(NUMBER_FORMATTER.format(Number(status.browserLoadMs || 0)))} ms</strong>`,
  ];
  if (status.log.length) {
    lines.push(`Log: ${status.log.slice(0, 3).map((entry) => escapeHtml(entry.message)).join(" | ")}`);
  }
  return lines.join("<br>");
}

function LineStack({ values, className }: { values: string[]; className?: string }) {
  const lines = values.map((value) => safeText(value)).filter(Boolean);

  if (!lines.length) {
    return <span>-</span>;
  }

  return (
    <>
      {lines.map((line, index) => (
        <div key={`${line}:${index}`} className={className}>{line}</div>
      ))}
    </>
  );
}

function KpiCard({ title, value, subtext }: { title: string; value: string; subtext: string }) {
  return (
    <article className="card kpi">
      <div className="kpi-name">{title}</div>
      <div className="kpi-value">{value}</div>
      <div className="kpi-sub">{subtext}</div>
    </article>
  );
}

function applyGlobeThemeColors(globe: CustomerGlobeInstance | null) {
  if (!globe) {
    return;
  }

  const styles = getComputedStyle(document.documentElement);
  const background = styles.getPropertyValue("--th-globe-bg").trim();
  const atmosphere = styles.getPropertyValue("--th-globe-atmosphere").trim();
  const land = styles.getPropertyValue("--th-globe-hex-land").trim();
  const surface = styles.getPropertyValue("--th-globe-surface").trim();
  const emissive = styles.getPropertyValue("--th-globe-emissive").trim();
  const topR = parseInt(styles.getPropertyValue("--th-globe-hex-top-r").trim(), 10) || 42;
  const topG = parseInt(styles.getPropertyValue("--th-globe-hex-top-g").trim(), 10) || 108;
  const topB = parseInt(styles.getPropertyValue("--th-globe-hex-top-b").trim(), 10) || 202;
  const sideR = parseInt(styles.getPropertyValue("--th-globe-hex-side-r").trim(), 10) || 32;
  const sideG = parseInt(styles.getPropertyValue("--th-globe-hex-side-g").trim(), 10) || 88;
  const sideB = parseInt(styles.getPropertyValue("--th-globe-hex-side-b").trim(), 10) || 176;

  globe.backgroundColor?.(background);
  globe.atmosphereColor?.(atmosphere);
  globe.hexTopColor?.((hex) => {
    const weight = Number(hex?.sumWeight || 0);
    const alpha = Math.min(0.95, 0.38 + (weight / 20));
    return `rgba(${topR}, ${topG}, ${topB}, ${alpha.toFixed(3)})`;
  });
  globe.hexSideColor?.((hex) => {
    const weight = Number(hex?.sumWeight || 0);
    const alpha = Math.min(0.96, 0.4 + (weight / 18));
    return `rgba(${sideR}, ${sideG}, ${sideB}, ${alpha.toFixed(3)})`;
  });
  globe.hexPolygonColor?.(() => land || "rgba(89, 101, 122, 0.42)");

  const material = globe.globeMaterial?.();
  if (material && window.THREE) {
    if (surface) {
      material.color = new window.THREE.Color(surface);
    }
    if (emissive) {
      material.emissive = new window.THREE.Color(emissive);
    }
  }
}

function destroyCustomerGlobe(globe: CustomerGlobeInstance | null) {
  if (!globe) {
    return;
  }
  try {
    const renderer = globe.renderer?.();
    renderer?.dispose?.();
    renderer?.forceContextLoss?.();
    if (renderer?.domElement?.parentNode?.removeChild && renderer.domElement) {
      renderer.domElement.parentNode.removeChild(renderer.domElement);
    }
    globe._destructor?.();
  } catch {
    // Ignore cleanup errors.
  }
}

export function CustomersPage() {
  const { filters: shellFilters, refreshRequestToken } = useDashboardShellState();
  const { theme, themeVersion } = useTheme();
  const [overview, setOverview] = useState<CustomersOverview>(INITIAL_OVERVIEW);
  const [geo, setGeo] = useState<CustomersGeo>(INITIAL_GEO);
  const [loadingOverview, setLoadingOverview] = useState(true);
  const [loadingGeo, setLoadingGeo] = useState(true);
  const [overviewError, setOverviewError] = useState("");
  const [geoError, setGeoError] = useState("");
  const [geoMode, setGeoModeState] = useState<GeoMode>("map");
  const [geoStatus, setGeoStatus] = useState<GeoStatus>({
    loading: false,
    text: "Lade Kundendaten...",
    browserLoadMs: 0,
    log: [],
  });
  const [leafletReady, setLeafletReady] = useState(() => Boolean(window.L));
  const [globeRuntimeReady, setGlobeRuntimeReady] = useState(() => typeof window.Globe === "function" && Boolean(window.topojson?.feature));
  const [globeUnavailableReason, setGlobeUnavailableReason] = useState("");

  const mapViewRef = useRef<HTMLDivElement | null>(null);
  const globeViewRef = useRef<HTMLDivElement | null>(null);
  const leafletMapRef = useRef<CustomerLeafletMap | null>(null);
  const leafletLayerRef = useRef<CustomerLeafletLayer | null>(null);
  const leafletAutoFittedRef = useRef(false);
  const globeRef = useRef<CustomerGlobeInstance | null>(null);
  const globeWorldFeaturesRef = useRef<unknown[] | null>(null);
  const globeWorldLoadPromiseRef = useRef<Promise<void> | null>(null);
  const lastRefreshRequestTokenRef = useRef(refreshRequestToken);

  const query = useMemo(() => ({
    from: shellFilters.from,
    to: shellFilters.to,
    marketplace: shellFilters.marketplace,
    q: shellFilters.q,
    limit: 2000,
  }), [shellFilters.from, shellFilters.marketplace, shellFilters.q, shellFilters.to]);

  const customersCount = Math.max(Number(overview.kpis.customers_count || overview.total || 0), 0);
  const coverageEmail = Number(overview.kpis.with_email_count || 0);
  const coveragePhone = Number(overview.kpis.with_phone_count || 0);
  const coverageAddress = Number(overview.kpis.with_address_count || 0);
  const coverageCross = Number(overview.kpis.cross_market_customers_count || 0);

  const geoSubtitle = useMemo(() => {
    const summary = geo.summary;
    const resolved = Number(summary.orders_total || 0) - Number(summary.unresolved_orders_count || 0);
    const unresolved = Number(summary.unresolved_orders_count || 0);
    const rangeText = query.from && query.to
      ? `${formatDateToken(query.from)} - ${formatDateToken(query.to)}`
      : "Aktueller Filter";
    return `Punkte: ${NUMBER_FORMATTER.format(geo.points.length)} · Orders geolokalisiert: ${NUMBER_FORMATTER.format(Math.max(resolved, 0))} · Unaufgeloest: ${NUMBER_FORMATTER.format(Math.max(unresolved, 0))} · ${rangeText}`;
  }, [geo.points.length, geo.summary, query.from, query.to]);

  useEffect(() => {
    if (leafletReady) {
      return;
    }

    let cancelled = false;
    void loadCustomerMapLibrary()
      .then(() => {
        if (!cancelled) {
          setLeafletReady(true);
        }
      })
      .catch((error) => {
        if (cancelled) {
          return;
        }
        const message = error instanceof Error ? error.message : "Leaflet konnte nicht geladen werden.";
        setGeoError((current) => current || message);
        setGeoStatus((current) => ({
          ...current,
          log: logGeoEvent(`Karten-Lib Fehler: ${message}`, current.log),
        }));
      });

    return () => {
      cancelled = true;
    };
  }, [leafletReady]);

  useEffect(() => {
    if (globeRuntimeReady || globeUnavailableReason) {
      return;
    }

    let cancelled = false;
    void loadCustomerGlobeLibraries()
      .then(() => {
        if (!cancelled) {
          setGlobeRuntimeReady(true);
        }
      })
      .catch((error) => {
        if (cancelled) {
          return;
        }
        const message = error instanceof Error ? error.message : "Hex-Globus konnte nicht geladen werden.";
        setGlobeUnavailableReason(message);
        setGeoStatus((current) => ({
          ...current,
          log: logGeoEvent(`Globus-Lib Fehler: ${message}`, current.log),
        }));
      });

    return () => {
      cancelled = true;
    };
  }, [globeRuntimeReady, globeUnavailableReason]);

  useEffect(() => {
    const statusElement = document.getElementById("customerGeoStatusInfo");
    if (statusElement instanceof HTMLElement) {
      statusElement.innerHTML = customerGeoStatusHtml(geo.summary, geoStatus);
    }
  }, [geo.summary, geoStatus]);

  useEffect(() => {
    const nextOverviewRequest = fetchCustomersOverview(query)
      .then((payload) => {
        setOverview(normalizeOverview(payload));
        setOverviewError("");
      })
      .catch((error: Error) => {
        setOverview(INITIAL_OVERVIEW);
        setOverviewError(error.message);
      })
      .finally(() => {
        setLoadingOverview(false);
      });

    const startedAt = performance.now();
    setGeoStatus((current) => ({
      ...current,
      loading: true,
      text: "Kundenkarte wird geladen...",
      log: logGeoEvent("Start: /api/customers/locations", current.log),
    }));

    const nextGeoRequest = fetchCustomerLocations(query)
      .then((payload) => {
        const normalized = normalizeGeo(payload);
        setGeo(normalized);
        setGeoError("");
        setGeoStatus((current) => ({
          ...current,
          loading: false,
          text: "Kundendaten geladen.",
          browserLoadMs: Math.max(0, Math.round(performance.now() - startedAt)),
          log: logGeoEvent(
            `Fertig: ${NUMBER_FORMATTER.format(Number(normalized.summary.points_total || 0))} Punkte / ${NUMBER_FORMATTER.format(Number(normalized.summary.orders_total || 0))} Orders (${NUMBER_FORMATTER.format(Math.max(0, Math.round(performance.now() - startedAt)))} ms)`,
            current.log,
          ),
        }));
      })
      .catch((error: Error) => {
        setGeo(INITIAL_GEO);
        setGeoError(error.message);
        setGeoStatus((current) => ({
          ...current,
          loading: false,
          text: "Kundenkarte konnte nicht geladen werden.",
          browserLoadMs: Math.max(0, Math.round(performance.now() - startedAt)),
          log: logGeoEvent(`Fehler: ${error.message}`, current.log),
        }));
      })
      .finally(() => {
        setLoadingGeo(false);
      });

    setLoadingOverview(true);
    setLoadingGeo(true);

    void nextOverviewRequest;
    void nextGeoRequest;
  }, [query]);

  useEffect(() => {
    if (refreshRequestToken === 0 || lastRefreshRequestTokenRef.current === refreshRequestToken) {
      return;
    }
    lastRefreshRequestTokenRef.current = refreshRequestToken;

    setLoadingOverview(true);
    setLoadingGeo(true);
    fetchCustomersOverview(query)
      .then((payload) => {
        setOverview(normalizeOverview(payload));
        setOverviewError("");
      })
      .catch((error: Error) => {
        setOverview(INITIAL_OVERVIEW);
        setOverviewError(error.message);
      })
      .finally(() => {
        setLoadingOverview(false);
      });

    const startedAt = performance.now();
    setGeoStatus((current) => ({
      ...current,
      loading: true,
      text: "Kundenorte werden aktualisiert...",
      log: logGeoEvent("Kundenorte werden aktualisiert...", current.log),
    }));
    fetchCustomerLocations({ ...query, refresh: true })
      .then((payload) => {
        const normalized = normalizeGeo(payload);
        setGeo(normalized);
        setGeoError("");
        setGeoStatus((current) => ({
          ...current,
          loading: false,
          text: "Kundenkarte aktualisiert.",
          browserLoadMs: Math.max(0, Math.round(performance.now() - startedAt)),
          log: logGeoEvent("Kundenkarte aktualisiert.", current.log),
        }));
      })
      .catch((error: Error) => {
        setGeoError(error.message);
        setGeoStatus((current) => ({
          ...current,
          loading: false,
          text: "Kundenkarte konnte nicht aktualisiert werden.",
          browserLoadMs: Math.max(0, Math.round(performance.now() - startedAt)),
          log: logGeoEvent(`Fehler: ${error.message}`, current.log),
        }));
      })
      .finally(() => {
        setLoadingGeo(false);
      });
  }, [query, refreshRequestToken]);

  useEffect(() => {
    applyGlobeThemeColors(globeRef.current);
  }, [theme, themeVersion]);

  useEffect(() => {
    const handleResize = () => {
      leafletMapRef.current?.invalidateSize();
      const globeView = globeViewRef.current;
      if (globeRef.current && globeView instanceof HTMLElement) {
        globeRef.current.width?.(globeView.clientWidth || 640);
        globeRef.current.height?.(globeView.clientHeight || 420);
      }
    };

    window.addEventListener("resize", handleResize);
    return () => {
      window.removeEventListener("resize", handleResize);
    };
  }, []);

  useEffect(() => {
    return () => {
      destroyCustomerGlobe(globeRef.current);
      globeRef.current = null;
      leafletMapRef.current?.remove();
      leafletMapRef.current = null;
      leafletLayerRef.current = null;
    };
  }, []);

  useEffect(() => {
    const mapView = mapViewRef.current;
    if (!(mapView instanceof HTMLElement) || geoMode !== "map") {
      return;
    }
    if (!leafletReady) {
      mapView.innerHTML = '<div class="customer-geo-empty">Kartenbibliothek wird geladen...</div>';
      return;
    }
    const L = window.L;
    if (!L) {
      return;
    }

    if (!geo.points.length) {
      if (leafletMapRef.current) {
        leafletMapRef.current.remove();
      }
      leafletMapRef.current = null;
      leafletLayerRef.current = null;
      leafletAutoFittedRef.current = false;
      mapView.innerHTML = '<div class="customer-geo-empty">Keine Ortsdaten fuer den aktuellen Filter.</div>';
      return;
    }

    if (mapView.querySelector(".customer-geo-empty")) {
      mapView.innerHTML = "";
    }

    if (!leafletMapRef.current) {
      const nextMap = L.map(mapView, {
        zoomControl: true,
        worldCopyJump: true,
        minZoom: 1,
        maxZoom: 18,
      });
      L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
        subdomains: "abcd",
        maxZoom: 18,
        attribution: "&copy; <a href='https://www.openstreetmap.org/copyright'>OpenStreetMap</a> &copy; <a href='https://carto.com/'>CARTO</a>",
      }).addTo(nextMap);
      const nextLayer = L.layerGroup().addTo(nextMap);
      nextMap.setView([20, 10], 2);
      leafletMapRef.current = nextMap;
      leafletLayerRef.current = nextLayer;
      leafletAutoFittedRef.current = false;
    }

    leafletLayerRef.current?.clearLayers();
    const bounds: Array<[number, number]> = [];
    for (const point of geo.points) {
      const lat = Number(point.lat);
      const lng = Number(point.lng);
      if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
        continue;
      }
      const dominant = safeText(point.dominant_marketplace).toLowerCase();
      const color = dominant === "kaufland" ? "#1f8b5f" : "#2d5ea8";
      const orderCount = Number(point.order_count || 0);
      const radius = Math.max(4, Math.min(22, 4 + Math.sqrt(Math.max(orderCount, 0)) * 1.8));
      const marker = L.circleMarker([lat, lng], {
        radius,
        color,
        weight: 1.4,
        fillColor: color,
        fillOpacity: 0.35,
      });
      const label = [safeText(point.city), safeText(point.country || point.country_code)].filter(Boolean).join(", ") || "Unbekannt";
      marker.bindPopup(
        `<strong>${escapeHtml(label)}</strong><br>Orders: ${escapeHtml(NUMBER_FORMATTER.format(orderCount))}<br>Umsatz: ${escapeHtml(formatMoneyFromCents(Number(point.revenue_total_cents || 0)))}<br>Gewinn: ${escapeHtml(formatMoneyFromCents(Number(point.profit_total_cents || 0)))}`,
      );
      if (leafletLayerRef.current) {
        marker.addTo(leafletLayerRef.current);
      }
      bounds.push([lat, lng]);
    }

    leafletMapRef.current?.invalidateSize();
    if (bounds.length && !leafletAutoFittedRef.current) {
      leafletMapRef.current?.fitBounds(bounds, { padding: [26, 26], maxZoom: 5 });
      leafletAutoFittedRef.current = true;
    }
  }, [geo.points, geoMode, leafletReady]);

  useEffect(() => {
    const globeView = globeViewRef.current;
    if (!(globeView instanceof HTMLElement) || geoMode !== "globe") {
      const renderer = globeRef.current?.renderer?.();
      renderer?.setAnimationLoop?.(null);
      const controls = globeRef.current?.controls?.();
      if (controls) {
        controls.enabled = false;
      }
      return;
    }

    if (!globeRuntimeReady) {
      globeView.innerHTML = '<div class="customer-geo-empty">Hex-Globus wird geladen...</div>';
      return;
    }

    if (!geo.points.length) {
      globeView.innerHTML = '<div class="customer-geo-empty">Keine Ortsdaten fuer den aktuellen Filter.</div>';
      destroyCustomerGlobe(globeRef.current);
      globeRef.current = null;
      return;
    }

    if (typeof window.Globe !== "function") {
      globeView.innerHTML = '<div class="customer-geo-empty">Hex-Globus konnte nicht geladen werden (CDN evtl. blockiert).</div>';
      return;
    }

    if (!(window.WebGLRenderingContext)) {
      globeView.innerHTML = '<div class="customer-geo-empty">WebGL ist im Browser deaktiviert. Bitte Hardwarebeschleunigung aktivieren.</div>';
      setGlobeUnavailableReason("Hex-Globus nicht verfuegbar (WebGL wird vom Browser blockiert)");
      setGeoModeState("map");
      return;
    }

    if (globeView.querySelector(".customer-geo-empty")) {
      globeView.innerHTML = "";
    }

    if (!globeRef.current) {
      try {
        globeRef.current = new window.Globe(globeView, {
          rendererConfig: { antialias: true, alpha: true, powerPreference: "high-performance", failIfMajorPerformanceCaveat: false },
          waitForGlobeReady: false,
          animateIn: false,
        });
      } catch {
        try {
          globeRef.current = new window.Globe(globeView, {
            rendererConfig: { antialias: false, alpha: false, powerPreference: "default", failIfMajorPerformanceCaveat: false },
            waitForGlobeReady: false,
            animateIn: false,
          });
        } catch (error) {
          const message = error instanceof Error ? error.message : "Unbekannter Fehler";
          globeView.innerHTML = `<div class="customer-geo-empty">Hex-Globus Fehler: ${escapeHtml(message)}</div>`;
          setGlobeUnavailableReason(`Hex-Globus konnte nicht initialisiert werden: ${message}`);
          setGeoModeState("map");
          return;
        }
      }

      globeRef.current.showGlobe?.(false);
      globeRef.current.showAtmosphere?.(false);
      globeRef.current.atmosphereAltitude?.(0.12);
      globeRef.current.showGraticules?.(true);
      globeRef.current.hexBinResolution?.(4);
      globeRef.current.hexMargin?.(0.22);
      globeRef.current.hexTopCurvatureResolution?.(4);
      globeRef.current.hexAltitude?.((hex) => {
        const weight = Number(hex?.sumWeight || 0);
        return Math.min(0.24, Math.max(0.01, weight * 0.0075));
      });
      globeRef.current.hexLabel?.((hex) => {
        const weight = Number(hex?.sumWeight || 0);
        const lat = Number(hex?.center?.lat || 0);
        const lng = Number(hex?.center?.lng || 0);
        return `Orders: ${NUMBER_FORMATTER.format(weight)}<br>Lat/Lng: ${lat.toFixed(2)}, ${lng.toFixed(2)}`;
      });
      globeRef.current.pointOfView?.({ lat: 20, lng: 10, altitude: 2.2 }, 0);
      const controls = globeRef.current.controls?.();
      if (controls) {
        controls.autoRotate = false;
        controls.enablePan = false;
        controls.enableDamping = true;
        controls.dampingFactor = 0.08;
        controls.minDistance = 110;
        controls.maxDistance = 520;
      }
    }

    applyGlobeThemeColors(globeRef.current);

    if (!globeWorldFeaturesRef.current && !globeWorldLoadPromiseRef.current) {
      globeWorldLoadPromiseRef.current = fetch("https://unpkg.com/world-atlas@2/countries-50m.json")
        .then((response) => {
          if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
          }
          return response.json();
        })
        .then((atlas) => {
          if (!window.topojson?.feature) {
            return;
          }
          const objectKey = atlas?.objects?.countries ? "countries" : Object.keys(atlas?.objects || {})[0];
          if (!objectKey) {
            return;
          }
          const collection = window.topojson.feature(atlas, atlas.objects[objectKey]);
          const features = Array.isArray(collection?.features) ? collection.features : [];
          globeWorldFeaturesRef.current = features;
          globeRef.current?.hexPolygonsData?.(features);
          globeRef.current?.hexPolygonResolution?.(4);
          globeRef.current?.hexPolygonMargin?.(0.12);
          globeRef.current?.hexPolygonAltitude?.(0.0036);
          globeRef.current?.hexPolygonUseDots?.(false);
        })
        .finally(() => {
          globeWorldLoadPromiseRef.current = null;
        });
    } else if (globeWorldFeaturesRef.current) {
      globeRef.current.hexPolygonsData?.(globeWorldFeaturesRef.current);
      globeRef.current.hexPolygonResolution?.(4);
      globeRef.current.hexPolygonMargin?.(0.12);
      globeRef.current.hexPolygonAltitude?.(0.0036);
      globeRef.current.hexPolygonUseDots?.(false);
    }

    const points = geo.points
      .map((point) => ({
        lat: Number(point.lat || 0),
        lng: Number(point.lng || 0),
        weight: Number(point.weight || point.order_count || 0),
      }))
      .filter((point) => Number.isFinite(point.lat) && Number.isFinite(point.lng) && Number.isFinite(point.weight));

    globeRef.current.width?.(globeView.clientWidth || 640);
    globeRef.current.height?.(globeView.clientHeight || 420);
    globeRef.current.hexBinPointsData?.(points);
    globeRef.current.hexBinPointLat?.((point) => point.lat);
    globeRef.current.hexBinPointLng?.((point) => point.lng);
    globeRef.current.hexBinPointWeight?.((point) => point.weight);

    const renderer = globeRef.current.renderer?.();
    const scene = globeRef.current.scene?.();
    const camera = globeRef.current.camera?.();
    if (renderer?.setAnimationLoop && scene && camera) {
      renderer.setAnimationLoop(() => {
        renderer.render?.(scene, camera);
      });
    }
    const controls = globeRef.current.controls?.();
    if (controls) {
      controls.enabled = true;
      controls.update?.();
    }
  }, [geo.points, geoMode, globeRuntimeReady, theme, themeVersion]);

  const topContent = (
    <>
      <section className="kpi-grid" id="customersReactTop">
        <KpiCard title="Kunden (gemerged)" value={NUMBER_FORMATTER.format(customersCount)} subtext="Shopify + Kaufland zusammengefuehrt" />
        <KpiCard
          title="Wiederkehrend"
          value={NUMBER_FORMATTER.format(Number(overview.kpis.repeat_customers_count || 0))}
          subtext={formatPercent(Number(overview.kpis.repeat_customers_rate_pct || 0))}
        />
        <KpiCard
          title="Orders pro Kunde"
          value={formatFixed(Number(overview.kpis.avg_orders_per_customer || 0))}
          subtext={`Orders gesamt: ${NUMBER_FORMATTER.format(Number(overview.kpis.orders_total_count || 0))}`}
        />
        <KpiCard
          title="Umsatz pro Kunde"
          value={formatMoneyFromCents(Number(overview.kpis.avg_revenue_per_customer_cents || 0))}
          subtext={`Umsatz gesamt: ${formatMoneyFromCents(Number(overview.kpis.revenue_total_cents || 0))}`}
        />
      </section>

      <section className="analytics-insights-grid" style={{ marginTop: 12 }}>
        <article className="card insight-card">
          <h2 className="chart-title">Datenabdeckung</h2>
          <p className="chart-sub">Wie vollstaendig sind Kontakt- und Adressdaten je gemergtem Kunden.</p>
          <div className="status-pill-grid">
            <div className="status-pill">
              <div className="status-pill-label">Mit E-Mail</div>
              <div className="status-pill-value">{`${NUMBER_FORMATTER.format(coverageEmail)} (${formatPercent((coverageEmail / Math.max(customersCount, 1)) * 100)})`}</div>
            </div>
            <div className="status-pill">
              <div className="status-pill-label">Mit Telefon</div>
              <div className="status-pill-value">{`${NUMBER_FORMATTER.format(coveragePhone)} (${formatPercent((coveragePhone / Math.max(customersCount, 1)) * 100)})`}</div>
            </div>
            <div className="status-pill">
              <div className="status-pill-label">Mit Adresse</div>
              <div className="status-pill-value">{`${NUMBER_FORMATTER.format(coverageAddress)} (${formatPercent((coverageAddress / Math.max(customersCount, 1)) * 100)})`}</div>
            </div>
            <div className="status-pill">
              <div className="status-pill-label">Cross-Channel</div>
              <div className="status-pill-value">{`${NUMBER_FORMATTER.format(coverageCross)} (${formatPercent((coverageCross / Math.max(customersCount, 1)) * 100)})`}</div>
            </div>
          </div>
        </article>

        <article className="card insight-card">
          <h2 className="chart-title">Marketplace Verteilung</h2>
          <p className="chart-sub">Anzahl gemergter Kunden, die in Shopify bzw. Kaufland Orders haben.</p>
          <div className="payment-method-list">
            {[
              { label: "Shopify", count: Number(overview.kpis.shopify_customers_count || 0) },
              { label: "Kaufland", count: Number(overview.kpis.kaufland_customers_count || 0) },
            ].map((item) => {
              const share = customersCount > 0 ? (item.count / customersCount) * 100 : 0;
              return (
                <div key={item.label} className="payment-method-row">
                  <span className="payment-method-name">{item.label}</span>
                  <span className="payment-method-count">{NUMBER_FORMATTER.format(item.count)}</span>
                  <span className="payment-method-share">{formatPercent(share)}</span>
                </div>
              );
            })}
          </div>
        </article>
      </section>
    </>
  );

  const geoCard = (
    <section className="card chart-card customer-geo-card" style={{ marginTop: 12 }}>
      <div className="chart-head-row">
        <div>
          <h2 className="chart-title">Bestell-Herkunft</h2>
          <p id="customerGeoSub" className="chart-sub">{geoSubtitle || "Weltkarte mit Zoom/Scroll fuer Bestellorte."}</p>
        </div>
        <div id="customerGeoModeGroup" className="customer-geo-mode" role="tablist" aria-label="Kartenmodus">
          <button
            id="customerGeoModeMapBtn"
            className={`customer-geo-mode-btn${geoMode === "map" ? " active" : ""}`}
            type="button"
            data-customer-geo-mode="map"
            onClick={() => {
              setGeoModeState("map");
            }}
          >
            Karte
          </button>
          <button
            id="customerGeoModeGlobeBtn"
            className={`customer-geo-mode-btn${geoMode === "globe" ? " active" : ""}`}
            type="button"
            data-customer-geo-mode="globe"
            disabled={!globeRuntimeReady || Boolean(globeUnavailableReason)}
            title={!globeRuntimeReady ? "Hex-Globus wird geladen..." : globeUnavailableReason || undefined}
            onClick={() => {
              if (globeRuntimeReady && !globeUnavailableReason) {
                setGeoModeState("globe");
              }
            }}
          >
            Hex-Globus
          </button>
        </div>
      </div>
      <div className="customer-geo-stage">
        <div id="customerGeoMapView" ref={mapViewRef} className={`customer-geo-view${geoMode === "map" ? " active" : ""}`} />
        <div id="customerGeoGlobeView" ref={globeViewRef} className={`customer-geo-view${geoMode === "globe" ? " active" : ""}`} />
        <div id="customerGeoLoadingOverlay" className={`customer-geo-loading${geoStatus.loading ? " active" : ""}`} aria-live="polite" aria-hidden={geoStatus.loading ? "false" : "true"}>
          <div className="customer-geo-loading-card">
            <div id="customerGeoLoadingText" className="customer-geo-loading-text">{geoStatus.text}</div>
            <div className="customer-geo-loading-bar" />
          </div>
        </div>
      </div>
      {geoError ? (
        <div className="table-meta" style={{ color: "var(--danger, #c44)", marginTop: 10 }}>
          Kundenkarte konnte nicht geladen werden: {geoError}
        </div>
      ) : null}
    </section>
  );

  const listCard = (
    <section className="card table-card" id="customersReactBottom" style={{ marginTop: 12 }}>
      <div className="table-head">
        <h2 className="table-title">Kundenliste</h2>
        <div className="table-meta">{loadingOverview ? "..." : `${NUMBER_FORMATTER.format(overview.total)} Zeilen`}</div>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Kunde</th>
              <th>Kontakt</th>
              <th>Adresse</th>
              <th>Channel</th>
              <th>Orders</th>
              <th>Repeat</th>
              <th>Umsatz</th>
              <th>Gewinn</th>
              <th>Letzte Bestellung</th>
              <th>Top Artikel</th>
            </tr>
          </thead>
          <tbody>
            {loadingOverview ? (
              <tr>
                <td colSpan={10}>Kunden werden geladen...</td>
              </tr>
            ) : overview.items.length ? overview.items.map((item, index) => {
              const address = item.primary_address && typeof item.primary_address === "object" ? item.primary_address : {};
              const markets = Array.isArray(item.marketplaces) ? item.marketplaces : [];
              const profit = Number(item.profit_total_cents || 0);
              const profitClass = profit < 0 ? "value-neg" : "value-pos";

              return (
                <tr key={`${safeText(item.customer_id || item.customer_name || "customer")}:${index}`}>
                  <td>
                    <div className="customer-name-main">{safeText(item.customer_name) || "Unbekannt"}</div>
                    <div className="cell-sub">{safeText(item.customer_id) || "-"}</div>
                  </td>
                  <td>
                    <div className="customer-contact-lines">
                      <LineStack
                        className="customer-contact-line"
                        values={[
                          ...(Array.isArray(item.emails) ? item.emails.slice(0, 1).map((value) => safeText(value)) : []),
                          ...(Array.isArray(item.phones) ? item.phones.slice(0, 1).map((value) => safeText(value)) : []),
                        ]}
                      />
                    </div>
                  </td>
                  <td>
                    <div className="customer-addr-lines">
                      <LineStack
                        className="customer-addr-line"
                        values={[
                          safeText(address.street),
                          [safeText(address.postcode), safeText(address.city)].filter(Boolean).join(" "),
                          safeText(address.country),
                        ]}
                      />
                    </div>
                  </td>
                  <td>
                    <div className="customer-market-badges">
                      {markets.length ? markets.map((market) => {
                        const token = safeText(market).toLowerCase();
                        const badgeClass = token === "kaufland" ? "badge badge-sale" : "badge badge-invoice";
                        return <span key={`${safeText(item.customer_id || item.customer_name)}:${token}`} className={badgeClass}>{token || "-"}</span>;
                      }) : "-"}
                    </div>
                  </td>
                  <td>{NUMBER_FORMATTER.format(Number(item.order_count || 0))}</td>
                  <td><span className={`badge ${item.repeat_customer ? "badge-sale" : "badge-default"}`}>{item.repeat_customer ? "Ja" : "Nein"}</span></td>
                  <td>{formatMoneyFromCents(Number(item.revenue_total_cents || 0))}</td>
                  <td className={profitClass}>{formatMoneyFromCents(Number(item.profit_total_cents || 0))}</td>
                  <td>{formatDateTime(item.last_order_date)}</td>
                  <td>
                    <div className="customer-top-articles">
                      <LineStack values={Array.isArray(item.top_articles) ? item.top_articles.map((value) => safeText(value)) : []} />
                    </div>
                  </td>
                </tr>
              );
            }) : (
              <tr>
                <td colSpan={10}>Keine Kunden fuer den aktuellen Filter.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      {overviewError ? (
        <div className="table-meta" style={{ color: "var(--danger, #c44)", marginTop: 10 }}>
          Kunden konnten nicht geladen werden: {overviewError}
        </div>
      ) : null}
    </section>
  );

  return (
    <div id="customersPanel" className="tab-panel active" data-react-customers-mounted="true">
      {topContent}
      {geoCard}
      {listCard}
    </div>
  );
}
