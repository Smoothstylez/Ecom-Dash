export const CUSTOM_THEME_STORAGE_KEY = "dash-combined.custom-theme";
export const CUSTOM_STYLE_ID = "custom-theme-style";

export const BUILT_IN_THEME_OPTIONS = [
  { id: "", name: "Warm Light", description: "Das klassische warme Design", previewClass: "tp-warm-light" },
  { id: "dark", name: "Dark", description: "Dunkles Design, augenschonend", previewClass: "tp-dark" },
  { id: "nord", name: "Nord", description: "Arktisch kuehl, inspiriert von Nord", previewClass: "tp-nord" },
  { id: "cool", name: "Cool Light", description: "Modern, klar und kuehle Toene", previewClass: "tp-cool" },
  { id: "espresso", name: "Espresso", description: "Warme dunkle Kaffeetoene", previewClass: "tp-espresso" },
  { id: "vision", name: "Vision", description: "Glassmorphism, Navy + Violet-Akzente", previewClass: "tp-vision" },
  { id: "focus", name: "Focus", description: "Minimal, weiss mit farbigen Akzenten", previewClass: "tp-focus" },
] as const;

export const CUSTOM_THEME_GROUPS = [
  ["Hintergrund", [
    ["--th-page-bg", "Seite"],
    ["--th-page-bg-mid", "Seite Mitte"],
    ["--th-page-bg-end", "Seite Ende"],
    ["--th-page-glow-1", "Glow 1"],
    ["--th-page-glow-2", "Glow 2"],
  ]],
  ["Oberflaechen", [
    ["--th-card-from", "Karte von"],
    ["--th-card-to", "Karte bis"],
    ["--th-card-border", "Karte Rahmen"],
    ["--th-panel", "Panel"],
    ["--th-panel-border", "Panel Rahmen"],
    ["--th-surface", "Flaeche"],
    ["--th-surface-alt", "Flaeche Alt"],
    ["--th-surface-warm", "Flaeche Warm"],
    ["--th-surface-deep", "Flaeche Tief"],
    ["--th-surface-white", "Flaeche Weiss"],
    ["--th-overlay", "Overlay"],
    ["--th-drawer-head", "Drawer Kopf"],
    ["--th-thead", "Tabellenkopf"],
  ]],
  ["Text", [
    ["--th-ink", "Text 1"],
    ["--th-ink-2", "Text 2"],
    ["--th-ink-3", "Text 3"],
    ["--th-ink-4", "Text 4"],
    ["--th-ink-5", "Text 5"],
  ]],
  ["Linien", [
    ["--th-line", "Linie 1"],
    ["--th-line-2", "Linie 2"],
    ["--th-line-3", "Linie 3"],
  ]],
  ["Akzente", [
    ["--th-accent", "Akzent 1"],
    ["--th-accent-2", "Akzent 2"],
    ["--th-accent-3", "Akzent 3"],
  ]],
  ["Semantisch", [
    ["--th-ok", "Erfolg"],
    ["--th-warn", "Warnung"],
  ]],
  ["Buttons", [
    ["--th-btn-primary-from", "Primaer von"],
    ["--th-btn-primary-to", "Primaer bis"],
    ["--th-btn-primary-border", "Primaer Rahmen"],
    ["--th-btn-secondary-from", "Sekundaer von"],
    ["--th-btn-secondary-to", "Sekundaer bis"],
    ["--th-btn-secondary-border", "Sekundaer Rahmen"],
    ["--th-btn-neutral-from", "Neutral von"],
    ["--th-btn-neutral-to", "Neutral bis"],
    ["--th-btn-neutral-border", "Neutral Rahmen"],
    ["--th-btn-danger-from", "Gefahr von"],
    ["--th-btn-danger-to", "Gefahr bis"],
    ["--th-btn-danger-border", "Gefahr Rahmen"],
    ["--th-btn-soft-bg", "Soft HG"],
    ["--th-btn-soft-bg-end", "Soft HG Ende"],
    ["--th-btn-soft-border", "Soft Rahmen"],
    ["--th-btn-soft-text", "Soft Text"],
    ["--th-btn-text", "Button Text"],
  ]],
  ["Interaktiv", [
    ["--th-hover-bg", "Hover HG"],
    ["--th-hover-border", "Hover Rahmen"],
    ["--th-active-bg", "Aktiv HG"],
    ["--th-active-border", "Aktiv Rahmen"],
    ["--th-active-text", "Aktiv Text"],
    ["--th-focus-ring", "Fokus Ring"],
    ["--th-focus-border", "Fokus Rahmen"],
    ["--th-range-bg", "Bereich HG"],
    ["--th-range-border", "Bereich Rahmen"],
    ["--th-range-edge", "Bereich Rand"],
  ]],
  ["Status", [
    ["--th-info-bg", "Info HG"],
    ["--th-info-border", "Info Rahmen"],
    ["--th-info-text", "Info Text"],
    ["--th-ok-bg", "OK HG"],
    ["--th-ok-border", "OK Rahmen"],
    ["--th-ok-text", "OK Text"],
    ["--th-error-bg", "Fehler HG"],
    ["--th-error-border", "Fehler Rahmen"],
    ["--th-error-text", "Fehler Text"],
  ]],
  ["Badges", [
    ["--th-badge-sale-bg", "Verkauf HG"],
    ["--th-badge-sale-border", "Verkauf Rahmen"],
    ["--th-badge-sale-text", "Verkauf Text"],
    ["--th-badge-fee-bg", "Gebuehr HG"],
    ["--th-badge-fee-border", "Gebuehr Rahmen"],
    ["--th-badge-fee-text", "Gebuehr Text"],
    ["--th-badge-cogs-bg", "Warenk. HG"],
    ["--th-badge-cogs-border", "Warenk. Rahmen"],
    ["--th-badge-cogs-text", "Warenk. Text"],
    ["--th-badge-invoice-bg", "Rechnung HG"],
    ["--th-badge-invoice-border", "Rechnung Rahmen"],
    ["--th-badge-invoice-text", "Rechnung Text"],
    ["--th-badge-refund-bg", "Erstattung HG"],
    ["--th-badge-refund-border", "Erstattung Rahmen"],
    ["--th-badge-refund-text", "Erstattung Text"],
    ["--th-badge-subscription-bg", "Abo HG"],
    ["--th-badge-subscription-border", "Abo Rahmen"],
    ["--th-badge-subscription-text", "Abo Text"],
    ["--th-badge-default-bg", "Standard HG"],
    ["--th-badge-default-border", "Standard Rahmen"],
    ["--th-badge-default-text", "Standard Text"],
  ]],
  ["Zeilen-Akzente", [
    ["--th-row-sale", "Verkauf"],
    ["--th-row-fee", "Gebuehr"],
    ["--th-row-cogs", "Wareneinsatz"],
    ["--th-row-invoice", "Rechnung"],
    ["--th-row-refund", "Erstattung"],
    ["--th-row-subscription", "Abo"],
    ["--th-row-other", "Sonstige"],
  ]],
  ["Charts", [
    ["--th-chart-1", "Linie 1"],
    ["--th-chart-1-fill", "Linie 1 Fuellung"],
    ["--th-chart-2", "Linie 2"],
    ["--th-chart-2-fill", "Linie 2 Fuellung"],
    ["--th-chart-grid", "Raster"],
    ["--th-chart-label", "Beschriftung"],
    ["--th-donut-shopify", "Donut Shopify"],
    ["--th-donut-kaufland", "Donut Kaufland"],
    ["--th-donut-fees", "Donut Gebuehren"],
    ["--th-donut-purchase", "Donut Einkauf"],
    ["--th-donut-profit", "Donut Gewinn"],
  ]],
  ["Globus", [
    ["--th-globe-bg", "Hintergrund"],
    ["--th-globe-atmosphere", "Atmosphaere"],
    ["--th-globe-surface", "Oberflaeche"],
    ["--th-globe-emissive", "Leuchten"],
    ["--th-globe-hex-land", "Hex Land"],
    ["--th-globe-hex-top-r", "Hex Oben R"],
    ["--th-globe-hex-top-g", "Hex Oben G"],
    ["--th-globe-hex-top-b", "Hex Oben B"],
    ["--th-globe-hex-side-r", "Hex Seite R"],
    ["--th-globe-hex-side-g", "Hex Seite G"],
    ["--th-globe-hex-side-b", "Hex Seite B"],
  ]],
  ["Sonstiges", [
    ["--th-shadow-rgb", "Schatten RGB"],
    ["--th-shadow", "Schatten"],
    ["--th-modal-backdrop", "Modal HG"],
    ["--th-modal-border", "Modal Rahmen"],
    ["--th-radius", "Radius"],
    ["--th-geo-stage", "Geo Buehne"],
    ["--th-loading-bar", "Ladeleiste"],
    ["--th-loading-accent-rgb", "Lade-Akzent RGB"],
    ["--th-link", "Link"],
    ["--th-link-hover", "Link Hover"],
    ["--th-select-arrow", "Select Pfeil"],
    ["--th-today-border", "Heute Rahmen"],
    ["--th-kpi-orb", "KPI Orb"],
  ]],
] as const;

export type CustomThemeValues = Record<string, string>;

export const CUSTOM_THEME_ALL_PROPS = CUSTOM_THEME_GROUPS.flatMap((group) => group[1].map((entry) => entry[0]));

export function readStoredCustomTheme() {
  try {
    const raw = localStorage.getItem(CUSTOM_THEME_STORAGE_KEY);
    if (!raw) {
      return null;
    }
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed as CustomThemeValues : null;
  } catch (_error) {
    return null;
  }
}

export function persistCustomTheme(values: CustomThemeValues | null) {
  try {
    if (values && Object.keys(values).length) {
      localStorage.setItem(CUSTOM_THEME_STORAGE_KEY, JSON.stringify(values));
    } else {
      localStorage.removeItem(CUSTOM_THEME_STORAGE_KEY);
    }
  } catch (_error) {
    // Ignore storage write failures.
  }
}

export function injectCustomThemeStyle(values: CustomThemeValues | null) {
  const existing = document.getElementById(CUSTOM_STYLE_ID);
  if (!values || !Object.keys(values).length) {
    existing?.remove();
    return;
  }

  const styleElement = existing instanceof HTMLStyleElement ? existing : document.createElement("style");
  styleElement.id = CUSTOM_STYLE_ID;
  const lines = Object.entries(values).map(([key, value]) => `  ${key}: ${value};`);
  styleElement.textContent = `[data-theme="custom"] {\n${lines.join("\n")}\n}`;
  if (!(existing instanceof HTMLStyleElement)) {
    document.head.appendChild(styleElement);
  }
}

export function isHexColor(value: string) {
  return /^#([0-9a-fA-F]{3,4}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})$/.test(String(value || "").trim());
}

export function normalizeHexColor(value: string) {
  const trimmed = String(value || "").trim();
  if (trimmed.length === 4) {
    return `#${trimmed[1]}${trimmed[1]}${trimmed[2]}${trimmed[2]}${trimmed[3]}${trimmed[3]}`;
  }
  return trimmed.slice(0, 7);
}

export function readBaseThemeValues(themeId: string) {
  const previousTheme = document.documentElement.getAttribute("data-theme") || "";
  const customStyle = document.getElementById(CUSTOM_STYLE_ID);
  if (customStyle instanceof HTMLStyleElement) {
    customStyle.disabled = true;
  }

  if (themeId) {
    document.documentElement.setAttribute("data-theme", themeId);
  } else {
    document.documentElement.removeAttribute("data-theme");
  }

  const styles = getComputedStyle(document.documentElement);
  const values: CustomThemeValues = {};
  CUSTOM_THEME_ALL_PROPS.forEach((prop) => {
    values[prop] = styles.getPropertyValue(prop).trim();
  });

  if (previousTheme) {
    document.documentElement.setAttribute("data-theme", previousTheme);
  } else {
    document.documentElement.removeAttribute("data-theme");
  }

  if (customStyle instanceof HTMLStyleElement) {
    customStyle.disabled = false;
  }
  return values;
}
