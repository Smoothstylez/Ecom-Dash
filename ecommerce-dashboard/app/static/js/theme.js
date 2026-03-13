"use strict";

/* ── Theme System ── */

const THEME_STORAGE_KEY = "dash-combined.theme";
const CUSTOM_THEME_KEY  = "dash-combined.custom-theme";
const CUSTOM_STYLE_ID   = "custom-theme-style";

/* ── Custom Theme Variable Groups (14 groups, 130 vars) ── */
const CTE_GROUPS = [
  ["Hintergrund", [
    ["--th-page-bg",      "Seite"],
    ["--th-page-bg-mid",  "Seite Mitte"],
    ["--th-page-bg-end",  "Seite Ende"],
    ["--th-page-glow-1",  "Glow 1"],
    ["--th-page-glow-2",  "Glow 2"]
  ]],
  ["Oberflaechen", [
    ["--th-card-from",     "Karte von"],
    ["--th-card-to",       "Karte bis"],
    ["--th-card-border",   "Karte Rahmen"],
    ["--th-panel",         "Panel"],
    ["--th-panel-border",  "Panel Rahmen"],
    ["--th-surface",       "Flaeche"],
    ["--th-surface-alt",   "Flaeche Alt"],
    ["--th-surface-warm",  "Flaeche Warm"],
    ["--th-surface-deep",  "Flaeche Tief"],
    ["--th-surface-white", "Flaeche Weiss"],
    ["--th-overlay",       "Overlay"],
    ["--th-drawer-head",   "Drawer Kopf"],
    ["--th-thead",         "Tabellenkopf"]
  ]],
  ["Text", [
    ["--th-ink",   "Text 1"],
    ["--th-ink-2", "Text 2"],
    ["--th-ink-3", "Text 3"],
    ["--th-ink-4", "Text 4"],
    ["--th-ink-5", "Text 5"]
  ]],
  ["Linien", [
    ["--th-line",   "Linie 1"],
    ["--th-line-2", "Linie 2"],
    ["--th-line-3", "Linie 3"]
  ]],
  ["Akzente", [
    ["--th-accent",   "Akzent 1"],
    ["--th-accent-2", "Akzent 2"],
    ["--th-accent-3", "Akzent 3"]
  ]],
  ["Semantisch", [
    ["--th-ok",   "Erfolg"],
    ["--th-warn", "Warnung"]
  ]],
  ["Buttons", [
    ["--th-btn-primary-from",     "Primaer von"],
    ["--th-btn-primary-to",       "Primaer bis"],
    ["--th-btn-primary-border",   "Primaer Rahmen"],
    ["--th-btn-secondary-from",   "Sekundaer von"],
    ["--th-btn-secondary-to",     "Sekundaer bis"],
    ["--th-btn-secondary-border", "Sekundaer Rahmen"],
    ["--th-btn-neutral-from",     "Neutral von"],
    ["--th-btn-neutral-to",       "Neutral bis"],
    ["--th-btn-neutral-border",   "Neutral Rahmen"],
    ["--th-btn-danger-from",      "Gefahr von"],
    ["--th-btn-danger-to",        "Gefahr bis"],
    ["--th-btn-danger-border",    "Gefahr Rahmen"],
    ["--th-btn-soft-bg",          "Soft HG"],
    ["--th-btn-soft-bg-end",      "Soft HG Ende"],
    ["--th-btn-soft-border",      "Soft Rahmen"],
    ["--th-btn-soft-text",        "Soft Text"],
    ["--th-btn-text",             "Button Text"]
  ]],
  ["Interaktiv", [
    ["--th-hover-bg",     "Hover HG"],
    ["--th-hover-border", "Hover Rahmen"],
    ["--th-active-bg",     "Aktiv HG"],
    ["--th-active-border", "Aktiv Rahmen"],
    ["--th-active-text",   "Aktiv Text"],
    ["--th-focus-ring",   "Fokus Ring"],
    ["--th-focus-border", "Fokus Rahmen"],
    ["--th-range-bg",     "Bereich HG"],
    ["--th-range-border", "Bereich Rahmen"],
    ["--th-range-edge",   "Bereich Rand"]
  ]],
  ["Status", [
    ["--th-info-bg",     "Info HG"],
    ["--th-info-border", "Info Rahmen"],
    ["--th-info-text",   "Info Text"],
    ["--th-ok-bg",       "OK HG"],
    ["--th-ok-border",   "OK Rahmen"],
    ["--th-ok-text",     "OK Text"],
    ["--th-error-bg",     "Fehler HG"],
    ["--th-error-border", "Fehler Rahmen"],
    ["--th-error-text",   "Fehler Text"]
  ]],
  ["Badges", [
    ["--th-badge-sale-bg",             "Verkauf HG"],
    ["--th-badge-sale-border",         "Verkauf Rahmen"],
    ["--th-badge-sale-text",           "Verkauf Text"],
    ["--th-badge-fee-bg",             "Gebuehr HG"],
    ["--th-badge-fee-border",         "Gebuehr Rahmen"],
    ["--th-badge-fee-text",           "Gebuehr Text"],
    ["--th-badge-cogs-bg",            "Warenk. HG"],
    ["--th-badge-cogs-border",        "Warenk. Rahmen"],
    ["--th-badge-cogs-text",          "Warenk. Text"],
    ["--th-badge-invoice-bg",         "Rechnung HG"],
    ["--th-badge-invoice-border",     "Rechnung Rahmen"],
    ["--th-badge-invoice-text",       "Rechnung Text"],
    ["--th-badge-refund-bg",          "Erstattung HG"],
    ["--th-badge-refund-border",      "Erstattung Rahmen"],
    ["--th-badge-refund-text",        "Erstattung Text"],
    ["--th-badge-subscription-bg",    "Abo HG"],
    ["--th-badge-subscription-border","Abo Rahmen"],
    ["--th-badge-subscription-text",  "Abo Text"],
    ["--th-badge-default-bg",         "Standard HG"],
    ["--th-badge-default-border",     "Standard Rahmen"],
    ["--th-badge-default-text",       "Standard Text"]
  ]],
  ["Zeilen-Akzente", [
    ["--th-row-sale",         "Verkauf"],
    ["--th-row-fee",          "Gebuehr"],
    ["--th-row-cogs",         "Wareneinsatz"],
    ["--th-row-invoice",      "Rechnung"],
    ["--th-row-refund",       "Erstattung"],
    ["--th-row-subscription", "Abo"],
    ["--th-row-other",        "Sonstige"]
  ]],
  ["Charts", [
    ["--th-chart-1",        "Linie 1"],
    ["--th-chart-1-fill",   "Linie 1 Fuellung"],
    ["--th-chart-2",        "Linie 2"],
    ["--th-chart-2-fill",   "Linie 2 Fuellung"],
    ["--th-chart-grid",     "Raster"],
    ["--th-chart-label",    "Beschriftung"],
    ["--th-donut-shopify",  "Donut Shopify"],
    ["--th-donut-kaufland", "Donut Kaufland"],
    ["--th-donut-fees",     "Donut Gebuehren"],
    ["--th-donut-purchase", "Donut Einkauf"],
    ["--th-donut-profit",   "Donut Gewinn"]
  ]],
  ["Globus", [
    ["--th-globe-bg",         "Hintergrund"],
    ["--th-globe-atmosphere", "Atmosphaere"],
    ["--th-globe-surface",    "Oberflaeche"],
    ["--th-globe-emissive",   "Leuchten"],
    ["--th-globe-hex-land",   "Hex Land"],
    ["--th-globe-hex-top-r",  "Hex Oben R"],
    ["--th-globe-hex-top-g",  "Hex Oben G"],
    ["--th-globe-hex-top-b",  "Hex Oben B"],
    ["--th-globe-hex-side-r", "Hex Seite R"],
    ["--th-globe-hex-side-g", "Hex Seite G"],
    ["--th-globe-hex-side-b", "Hex Seite B"]
  ]],
  ["Sonstiges", [
    ["--th-shadow-rgb",         "Schatten RGB"],
    ["--th-shadow",             "Schatten"],
    ["--th-modal-backdrop",     "Modal HG"],
    ["--th-modal-border",       "Modal Rahmen"],
    ["--th-radius",             "Radius"],
    ["--th-geo-stage",          "Geo Buehne"],
    ["--th-loading-bar",        "Ladeleiste"],
    ["--th-loading-accent-rgb", "Lade-Akzent RGB"],
    ["--th-link",               "Link"],
    ["--th-link-hover",         "Link Hover"],
    ["--th-select-arrow",       "Select Pfeil"],
    ["--th-today-border",       "Heute Rahmen"],
    ["--th-kpi-orb",            "KPI Orb"]
  ]]
];

const CTE_ALL_PROPS = CTE_GROUPS.flatMap(function(g) { return g[1].map(function(v) { return v[0]; }); });

/* ── Helpers ── */

function getStoredTheme() {
  try { return localStorage.getItem(THEME_STORAGE_KEY) || ""; } catch { return ""; }
}

function getStoredCustomTheme() {
  try { var r = localStorage.getItem(CUSTOM_THEME_KEY); return r ? JSON.parse(r) : null; } catch { return null; }
}

function saveCustomThemeData(v) {
  try { localStorage.setItem(CUSTOM_THEME_KEY, JSON.stringify(v)); } catch {}
}

function cteIsHex(v) {
  return /^#([0-9a-fA-F]{3,4}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})$/.test((v || "").trim());
}

function cteHexTo6(v) {
  var h = (v || "").trim();
  if (h.length === 4) h = "#" + h[1]+h[1] + h[2]+h[2] + h[3]+h[3];
  return h.slice(0, 7);
}

function escAttr(s) {
  return s.replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;");
}

/* ── Custom Theme Style Injection ── */

function injectCustomThemeStyle(values) {
  var el = document.getElementById(CUSTOM_STYLE_ID);
  if (!el) { el = document.createElement("style"); el.id = CUSTOM_STYLE_ID; document.head.appendChild(el); }
  var lines = [];
  for (var k in values) { if (values.hasOwnProperty(k)) lines.push("  " + k + ": " + values[k] + ";"); }
  el.textContent = '[data-theme="custom"] {\n' + lines.join("\n") + "\n}";
}

function removeCustomThemeStyle() {
  var el = document.getElementById(CUSTOM_STYLE_ID);
  if (el) el.remove();
}

function readBaseThemeValues(themeId) {
  var prev = document.documentElement.getAttribute("data-theme") || "";
  var customEl = document.getElementById(CUSTOM_STYLE_ID);
  if (customEl) customEl.disabled = true;
  if (themeId) { document.documentElement.setAttribute("data-theme", themeId); }
  else { document.documentElement.removeAttribute("data-theme"); }
  var cs = getComputedStyle(document.documentElement);
  var vals = {};
  CTE_ALL_PROPS.forEach(function(p) { vals[p] = cs.getPropertyValue(p).trim(); });
  if (prev) { document.documentElement.setAttribute("data-theme", prev); }
  else { document.documentElement.removeAttribute("data-theme"); }
  if (customEl) customEl.disabled = false;
  return vals;
}

/* ── Apply Theme ── */

function applyTheme(themeId) {
  if (themeId) {
    document.documentElement.setAttribute("data-theme", themeId);
  } else {
    document.documentElement.removeAttribute("data-theme");
  }
  try { localStorage.setItem(THEME_STORAGE_KEY, themeId); } catch {}

  applyChartThemeColors();
  applyGlobeThemeColors();

  /* Built-in cards */
  document.querySelectorAll(".theme-card[data-theme-id]").forEach(function(c) {
    c.classList.toggle("active", c.getAttribute("data-theme-id") === themeId);
  });
  /* Custom card */
  var cc = document.getElementById("customThemeCard");
  if (cc) cc.classList.toggle("active", themeId === "custom");
}

function applyChartThemeColors() {
  var cs = getComputedStyle(document.documentElement);
  var c1 = cs.getPropertyValue("--th-chart-1").trim();
  var c1f = cs.getPropertyValue("--th-chart-1-fill").trim();
  var c2 = cs.getPropertyValue("--th-chart-2").trim();
  var c2f = cs.getPropertyValue("--th-chart-2-fill").trim();
  var grid = cs.getPropertyValue("--th-chart-grid").trim();
  var label = cs.getPropertyValue("--th-chart-label").trim();

  if (typeof Chart !== "undefined") {
    Object.values(Chart.instances || {}).forEach(function(chart) {
      if (!chart || !chart.data || !chart.data.datasets) return;
      if (chart.config && chart.config.type === "doughnut") return;
      chart.data.datasets.forEach(function(ds, i) {
        if (i === 0) { ds.borderColor = c1; ds.backgroundColor = c1f; }
        else if (i === 1) { ds.borderColor = c2; ds.backgroundColor = c2f; }
      });
      if (chart.options && chart.options.scales) {
        ["x", "y"].forEach(function(axis) {
          if (chart.options.scales[axis]) {
            if (chart.options.scales[axis].grid) chart.options.scales[axis].grid.color = grid;
            if (chart.options.scales[axis].ticks) chart.options.scales[axis].ticks.color = label;
          }
        });
      }
      if (chart.options && chart.options.plugins && chart.options.plugins.legend && chart.options.plugins.legend.labels) {
        chart.options.plugins.legend.labels.color = label;
      }
      chart.update("none");
    });
  }

  renderDonutMarketplace();
  renderDonutRevenue();
}

/* ── Theme Modal ── */

function openThemeModal() {
  var m = document.getElementById("themeModal");
  if (m) { m.classList.add("active"); m.setAttribute("aria-hidden", "false"); }
  setSourcePanelOpen(false);
}

function closeThemeModal() {
  if (_cteOpen) { closeCustomThemeEditor(false); return; }
  var m = document.getElementById("themeModal");
  if (m) { m.classList.remove("active"); m.setAttribute("aria-hidden", "true"); }
}

/* ── Custom Theme Editor ── */

var _cteValues = null;
var _ctePreTheme = "";
var _cteOpen = false;

function openCustomThemeEditor() {
  _ctePreTheme = getStoredTheme();
  _cteOpen = true;

  var saved = getStoredCustomTheme();
  if (saved) {
    _cteValues = {};
    for (var k in saved) { if (saved.hasOwnProperty(k)) _cteValues[k] = saved[k]; }
  } else {
    _cteValues = readBaseThemeValues(_ctePreTheme === "custom" ? "" : _ctePreTheme);
  }

  injectCustomThemeStyle(_cteValues);
  document.documentElement.setAttribute("data-theme", "custom");

  var baseSelect = document.getElementById("cteBaseSelect");
  if (baseSelect) baseSelect.value = saved ? "" : (_ctePreTheme === "custom" ? "" : _ctePreTheme);

  buildCteBody();

  var gv = document.getElementById("themeGridView");
  var ed = document.getElementById("customThemeEditor");
  var mc = document.querySelector(".theme-modal-card");
  if (gv) gv.style.display = "none";
  if (ed) ed.style.display = "";
  if (mc) mc.classList.add("cte-active");

  applyChartThemeColors();
  applyGlobeThemeColors();
}

function closeCustomThemeEditor(save) {
  _cteOpen = false;

  if (save) {
    saveCustomThemeData(_cteValues);
    injectCustomThemeStyle(_cteValues);
    updateCustomPreviewThumbnail(_cteValues);
    try { localStorage.setItem(THEME_STORAGE_KEY, "custom"); } catch {}
    applyTheme("custom");
  } else {
    if (_ctePreTheme !== "custom") {
      removeCustomThemeStyle();
      var saved = getStoredCustomTheme();
      if (saved) injectCustomThemeStyle(saved);
    }
    applyTheme(_ctePreTheme);
  }

  var gv = document.getElementById("themeGridView");
  var ed = document.getElementById("customThemeEditor");
  var mc = document.querySelector(".theme-modal-card");
  if (gv) gv.style.display = "";
  if (ed) ed.style.display = "none";
  if (mc) mc.classList.remove("cte-active");

  var body = document.getElementById("cteBody");
  if (body) body.innerHTML = "";
}

function cteSwitchBase(themeId) {
  _cteValues = readBaseThemeValues(themeId);
  injectCustomThemeStyle(_cteValues);
  document.documentElement.setAttribute("data-theme", "custom");
  buildCteBody();
  applyChartThemeColors();
  applyGlobeThemeColors();
}

function buildCteBody() {
  var body = document.getElementById("cteBody");
  if (!body) return;
  var h = "";
  CTE_GROUPS.forEach(function(group, gi) {
    var label = group[0], vars = group[1];
    var op = gi === 0 ? " open" : "";
    h += '<details class="cte-group"' + op + '><summary class="cte-group-title">' +
         label + ' <span class="cte-group-count">' + vars.length + '</span></summary>' +
         '<div class="cte-group-grid">';
    vars.forEach(function(entry) {
      var prop = entry[0], lbl = entry[1];
      var val = _cteValues[prop] || "";
      if (cteIsHex(val)) {
        var hex6 = cteHexTo6(val);
        h += '<div class="cte-field"><label class="cte-label">' + lbl +
             '</label><div class="cte-color-wrap"><input type="color" class="cte-color" data-prop="' +
             prop + '" value="' + hex6 + '"><input type="text" class="cte-text cte-text-hex" data-prop="' +
             prop + '" value="' + escAttr(val) + '" spellcheck="false"></div></div>';
      } else {
        h += '<div class="cte-field"><label class="cte-label">' + lbl +
             '</label><input type="text" class="cte-text" data-prop="' +
             prop + '" value="' + escAttr(val) + '" spellcheck="false"></div>';
      }
    });
    h += '</div></details>';
  });
  body.innerHTML = h;
}

function cteHandleInput(e) {
  var el = e.target;
  var prop = el.getAttribute("data-prop");
  if (!prop) return;
  _cteValues[prop] = el.value;

  var wrap = el.closest(".cte-color-wrap");
  if (wrap) {
    if (el.type === "color") {
      var t = wrap.querySelector(".cte-text-hex");
      if (t) t.value = el.value;
    } else if (el.classList.contains("cte-text-hex") && cteIsHex(el.value)) {
      var c = wrap.querySelector(".cte-color");
      if (c) c.value = cteHexTo6(el.value);
    }
  }

  injectCustomThemeStyle(_cteValues);
  applyChartThemeColors();
  applyGlobeThemeColors();
}

function updateCustomPreviewThumbnail(values) {
  var p = document.querySelector(".tp-custom");
  if (!p || !values) return;
  p.style.background = values["--th-page-bg"] || "";
  var bar = p.querySelector(".tp-bar");
  if (bar) bar.style.background = "linear-gradient(90deg, " +
    (values["--th-btn-primary-from"] || "#666") + ", " +
    (values["--th-btn-primary-to"] || "#444") + ")";
  p.querySelectorAll(".tp-card-mock").forEach(function(m) {
    m.style.background = values["--th-card-from"] || "";
    m.style.border = "1px solid " + (values["--th-card-border"] || "transparent");
  });
}

/* ── IIFE: Apply stored theme + inject custom style immediately ── */
(function() {
  var stored = getStoredTheme();
  var customData = getStoredCustomTheme();
  if (customData) injectCustomThemeStyle(customData);
  if (stored) document.documentElement.setAttribute("data-theme", stored);
})();
