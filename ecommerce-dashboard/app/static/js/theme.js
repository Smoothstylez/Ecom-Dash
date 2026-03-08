"use strict";

/* ── Theme System ── */

const THEME_STORAGE_KEY = "dash-combined.theme";

function getStoredTheme() {
  try { return localStorage.getItem(THEME_STORAGE_KEY) || ""; } catch { return ""; }
}

function applyTheme(themeId) {
  if (themeId) {
    document.documentElement.setAttribute("data-theme", themeId);
  } else {
    document.documentElement.removeAttribute("data-theme");
  }
  try { localStorage.setItem(THEME_STORAGE_KEY, themeId); } catch {}

  // Update chart colors if charts exist
  applyChartThemeColors();

  // Update globe colors if globe exists
  applyGlobeThemeColors();

  // Update theme modal active state
  const cards = document.querySelectorAll(".theme-card[data-theme-id]");
  cards.forEach(c => {
    c.classList.toggle("active", c.getAttribute("data-theme-id") === themeId);
  });
}

function applyChartThemeColors() {
  const cs = getComputedStyle(document.documentElement);
  const c1 = cs.getPropertyValue("--th-chart-1").trim();
  const c1f = cs.getPropertyValue("--th-chart-1-fill").trim();
  const c2 = cs.getPropertyValue("--th-chart-2").trim();
  const c2f = cs.getPropertyValue("--th-chart-2-fill").trim();
  const grid = cs.getPropertyValue("--th-chart-grid").trim();
  const label = cs.getPropertyValue("--th-chart-label").trim();

  if (typeof Chart !== "undefined") {
    Object.values(Chart.instances || {}).forEach(chart => {
      if (!chart || !chart.data || !chart.data.datasets) return;
      /* Skip donut charts — they will be fully re-rendered below */
      if (chart.config && chart.config.type === "doughnut") return;
      chart.data.datasets.forEach((ds, i) => {
        if (i === 0) {
          ds.borderColor = c1;
          ds.backgroundColor = c1f;
        } else if (i === 1) {
          ds.borderColor = c2;
          ds.backgroundColor = c2f;
        }
      });
      if (chart.options && chart.options.scales) {
        ["x", "y"].forEach(axis => {
          if (chart.options.scales[axis]) {
            if (chart.options.scales[axis].grid) {
              chart.options.scales[axis].grid.color = grid;
            }
            if (chart.options.scales[axis].ticks) {
              chart.options.scales[axis].ticks.color = label;
            }
          }
        });
      }
      if (chart.options && chart.options.plugins && chart.options.plugins.legend && chart.options.plugins.legend.labels) {
        chart.options.plugins.legend.labels.color = label;
      }
      chart.update("none");
    });
  }

  /* Re-render donut charts to pick up new theme colors */
  renderDonutMarketplace();
  renderDonutRevenue();
}

function openThemeModal() {
  const m = document.getElementById("themeModal");
  if (m) {
    m.classList.add("active");
    m.setAttribute("aria-hidden", "false");
  }
  setSourcePanelOpen(false);
}

function closeThemeModal() {
  const m = document.getElementById("themeModal");
  if (m) {
    m.classList.remove("active");
    m.setAttribute("aria-hidden", "true");
  }
}

// Apply stored theme immediately
(function() {
  const stored = getStoredTheme();
  if (stored) {
    document.documentElement.setAttribute("data-theme", stored);
  }
})();
