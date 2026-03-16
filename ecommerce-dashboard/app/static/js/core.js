"use strict";

/* ── Constants ── */
const API_BASE = "api";
const DATE_FMT = new Intl.DateTimeFormat("de-DE");
const MONEY_FMT = new Intl.NumberFormat("de-DE", { style: "currency", currency: "EUR" });
const NUMBER_FMT = new Intl.NumberFormat("de-DE");

/* ── State ── */
const state = {
  filters: {
    datePreset: "last_30_days",
    from: "",
    to: "",
    q: "",
    marketplace: "",
    returnsOnly: false,
    orderStatus: new Set(),
    orderPayment: new Set(),
  },
  dateRangeUi: {
    anchorMonth: "",
    customFrom: "",
    customTo: "",
  },
  bookingTxFilters: {
    type: "",
    category: "",
  },
  bookingClass: "automatic",
  activeTab: "analytics",
  orders: [],
  ordersTotal: 0,
  customersPayload: null,
  customersNeedsReload: true,
  customerGeoPayload: null,
  customerGeoNeedsReload: true,
  customerGeoLastSummary: null,
  customerGeoLastLoadMs: 0,
  customerGeoLog: [],
  customerGeoMode: "map",
  customerGlobeWebGLUnavailable: false,
  analytics: null,
  bookings: [],
  bookingsTotal: 0,
  bookingsAllItems: [],
  bookingOrders: [],
  bookingOrdersTotal: 0,
  bookingTemplates: [],
  bookingTemplatesTotal: 0,
  bookingAccounts: [],
  bookingAccountsTotal: 0,
  bookingDocuments: [],
  bookingDocumentsTotal: 0,
  monthlyInvoices: [],
  monthlyInvoicesTotal: 0,
  bookkeepingLedgerOrders: [],
  bookkeepingLedgerOrdersTotal: 0,
  bookingsSubtab: "transactions",
  bookingsFullView: false,
  detailsMode: "",
  bookingDetailsTransactionId: null,
  trendChart: null,
  donutMarketplaceChart: null,
  donutRevenueChart: null,
  trendGranularity: "auto",
  trendGranularityResolved: "day",
  googleAds: null,
  googleAdsTrendChart: null,
  googleAdsProfitChart: null,
  googleAdsRoasChart: null,
  googleAdsCumulChart: null,
  googleAdsProductDetailChart: null,
  googleAdsExpandedProductKey: null,
  ebaySummary: null,
  ebayOrders: [],
  ebayOrdersTotal: 0,
  customerLeafletMap: null,
  customerLeafletLayer: null,
  customerLeafletAutoFitted: false,
  customerGlobe: null,
  customerGlobeBaseLayerReady: false,
  customerGlobeWorldFeatures: null,
  customerGlobeWorldLoadPromise: null,
  customersLoadingPromise: null,
  lastSyncInfoText: "Letzter Sync: -",
  kpiAnimatedValues: {},
  kpiAnimationFrames: {},
  /* Cross-device polling */
  pollingEnabled: false,
  pollingIntervalSec: 30,
  pollingTimerId: 0,
  pollingLastStamp: 0,
};

/* ── DOM References ── */
const els = {
  dateRangeBtn: document.getElementById("dateRangeBtn"),
  dateRangeMenu: document.getElementById("dateRangeMenu"),
  dateRangePreviewText: document.getElementById("dateRangePreviewText"),
  dateMonthPrevBtn: document.getElementById("dateMonthPrevBtn"),
  dateMonthNextBtn: document.getElementById("dateMonthNextBtn"),
  dateMonthLabelA: document.getElementById("dateMonthLabelA"),
  dateMonthLabelB: document.getElementById("dateMonthLabelB"),
  dateCalendarA: document.getElementById("dateCalendarA"),
  dateCalendarB: document.getElementById("dateCalendarB"),
  dateCustomApplyBtn: document.getElementById("dateCustomApplyBtn"),
  fromDate: document.getElementById("fromDate"),
  toDate: document.getElementById("toDate"),
  searchInput: document.getElementById("searchInput"),
  searchOpenBtn: document.getElementById("searchOpenBtn"),
  searchModal: document.getElementById("searchModal"),
  closeSearchModalBtn: document.getElementById("closeSearchModalBtn"),
  clearSearchBtn: document.getElementById("clearSearchBtn"),
  channelMenuBtn: document.getElementById("channelMenuBtn"),
  channelMenu: document.getElementById("channelMenu"),
  marketplaceSelect: document.getElementById("marketplaceSelect"),
  refreshBtn: document.getElementById("refreshBtn"),
  exportMenuBtn: document.getElementById("exportMenuBtn"),
  exportMenu: document.getElementById("exportMenu"),
  dataModalOpenBtn: document.getElementById("dataModalOpenBtn"),
  dataModal: document.getElementById("dataModal"),
  dataModalCloseBtn: document.getElementById("dataModalCloseBtn"),
  dataExportPeriodBtn: document.getElementById("dataExportPeriodBtn"),
  dataExportBackupBtn: document.getElementById("dataExportBackupBtn"),
  restoreFileInput: document.getElementById("restoreFileInput"),
  restoreFileLabel: document.getElementById("restoreFileLabel"),
  restoreConfirmSection: document.getElementById("restoreConfirmSection"),
  restoreFileInfo: document.getElementById("restoreFileInfo"),
  restoreCancelBtn: document.getElementById("restoreCancelBtn"),
  restoreConfirmBtn: document.getElementById("restoreConfirmBtn"),
  restoreProgress: document.getElementById("restoreProgress"),
  restoreResult: document.getElementById("restoreResult"),
  ordersFilterBtn: document.getElementById("ordersFilterBtn"),
  ordersFilterDropdown: document.getElementById("ordersFilterDropdown"),
  ordersFilterBadge: document.getElementById("ordersFilterBadge"),
  ordersFilterClearBtn: document.getElementById("ordersFilterClearBtn"),
  syncLiveBtn: document.getElementById("syncLiveBtn"),
  syncSourcesBtn: document.getElementById("syncSourcesBtn"),
  sourcePanelToggleBtn: document.getElementById("sourcePanelToggleBtn"),
  sourcePanelCloseBtn: document.getElementById("sourcePanelCloseBtn"),
  sourcePanel: document.getElementById("sourcePanel"),
  lastSyncInfo: document.getElementById("lastSyncInfo"),
  sourceInfo: document.getElementById("sourceInfo"),
  googleAdsStatusInfo: document.getElementById("googleAdsStatusInfo"),
  customerGeoStatusInfo: document.getElementById("customerGeoStatusInfo"),
  statusBox: document.getElementById("statusBox"),

  tabAnalyticsBtn: document.getElementById("tabAnalyticsBtn"),
  tabOrdersBtn: document.getElementById("tabOrdersBtn"),
  tabCustomersBtn: document.getElementById("tabCustomersBtn"),
  tabBookingsBtn: document.getElementById("tabBookingsBtn"),
  tabGoogleAdsBtn: document.getElementById("tabGoogleAdsBtn"),
  tabEbayBtn: document.getElementById("tabEbayBtn"),

  analyticsPanel: document.getElementById("analyticsPanel"),
  ordersPanel: document.getElementById("ordersPanel"),
  customersPanel: document.getElementById("customersPanel"),
  bookingsPanel: document.getElementById("bookingsPanel"),
  bookingsSubnav: document.getElementById("bookingsSubnav"),
  bookingClassSubnav: document.getElementById("bookingClassSubnav"),
  googleAdsPanel: document.getElementById("googleAdsPanel"),
  ebayPanel: document.getElementById("ebayPanel"),
  ebayKpiOrders: document.getElementById("ebayKpiOrders"),
  ebayKpiOrdersSub: document.getElementById("ebayKpiOrdersSub"),
  ebayKpiRevenue: document.getElementById("ebayKpiRevenue"),
  ebayKpiRevenueSub: document.getElementById("ebayKpiRevenueSub"),
  ebayKpiCosts: document.getElementById("ebayKpiCosts"),
  ebayKpiCostsSub: document.getElementById("ebayKpiCostsSub"),
  ebayKpiProfit: document.getElementById("ebayKpiProfit"),
  ebayKpiProfitSub: document.getElementById("ebayKpiProfitSub"),
  ebayShopsMeta: document.getElementById("ebayShopsMeta"),
  ebayShopsBody: document.getElementById("ebayShopsBody"),
  ebayShopFilter: document.getElementById("ebayShopFilter"),
  ebayCategoryFilter: document.getElementById("ebayCategoryFilter"),
  ebayOrdersMeta: document.getElementById("ebayOrdersMeta"),
  ebayOrdersBody: document.getElementById("ebayOrdersBody"),
  ebayTopArticlesMeta: document.getElementById("ebayTopArticlesMeta"),
  ebayTopArticlesBody: document.getElementById("ebayTopArticlesBody"),
  ebayImportInfo: document.getElementById("ebayImportInfo"),
  bookingsTransactionsBtn: document.getElementById("bookingsTransactionsBtn"),
  bookingsOrdersBtn: document.getElementById("bookingsOrdersBtn"),
  bookingsTemplatesBtn: document.getElementById("bookingsTemplatesBtn"),
  bookingsAccountsBtn: document.getElementById("bookingsAccountsBtn"),
  bookingsDocumentsBtn: document.getElementById("bookingsDocumentsBtn"),
  bookingsNewBtn: document.getElementById("bookingsNewBtn"),
  bookingsTransactionsPanel: document.getElementById("bookingsTransactionsPanel"),
  bookingsOrdersPanel: document.getElementById("bookingsOrdersPanel"),
  bookingsTemplatesPanel: document.getElementById("bookingsTemplatesPanel"),
  bookingsAccountsPanel: document.getElementById("bookingsAccountsPanel"),
  bookingsDocumentsPanel: document.getElementById("bookingsDocumentsPanel"),

  /* Booking class segmented control */
  bookingClassBar: document.getElementById("bookingClassBar"),
  bookingClassControl: document.getElementById("bookingClassControl"),
  bookingClassAllBtn: document.getElementById("bookingClassAllBtn"),
  bookingClassAutoBtn: document.getElementById("bookingClassAutoBtn"),
  bookingClassMonthlyBtn: document.getElementById("bookingClassMonthlyBtn"),
  bookingClassSingleBtn: document.getElementById("bookingClassSingleBtn"),

  /* Sammelrechnung */
  sammelrechnungSection: document.getElementById("sammelrechnungSection"),
  sammelrechnungMeta: document.getElementById("sammelrechnungMeta"),
  sammelrechnungBody: document.getElementById("sammelrechnungBody"),
  sammelrechnungTools: document.getElementById("sammelrechnungTools"),
  createSammelProvider: document.getElementById("createSammelProvider"),
  sammelMonthBtn: document.getElementById("sammelMonthBtn"),
  sammelMonthMenu: document.getElementById("sammelMonthMenu"),
  sammelYearPrevBtn: document.getElementById("sammelYearPrevBtn"),
  sammelYearNextBtn: document.getElementById("sammelYearNextBtn"),
  sammelYearLabel: document.getElementById("sammelYearLabel"),
  sammelMonthGrid: document.getElementById("sammelMonthGrid"),
  createSammelAmount: document.getElementById("createSammelAmount"),
  createSammelNotes: document.getElementById("createSammelNotes"),
  createSammelFile: document.getElementById("createSammelFile"),
  createSammelFileName: document.getElementById("createSammelFileName"),
  createSammelBtn: document.getElementById("createSammelBtn"),
  sammelPreview: document.getElementById("sammelPreview"),

  kpiOrders: document.getElementById("kpiOrders"),
  kpiRevenue: document.getElementById("kpiRevenue"),
  kpiAfterFees: document.getElementById("kpiAfterFees"),
  kpiPurchase: document.getElementById("kpiPurchase"),
  kpiProfit: document.getElementById("kpiProfit"),
  kpiProfitSub: document.getElementById("kpiProfitSub"),
  kpiChannels: document.getElementById("kpiChannels"),
  kpiOrdersTrend: document.getElementById("kpiOrdersTrend"),
  kpiRevenueTrend: document.getElementById("kpiRevenueTrend"),
  kpiAfterFeesTrend: document.getElementById("kpiAfterFeesTrend"),
  kpiPurchaseTrend: document.getElementById("kpiPurchaseTrend"),
  kpiProfitTrend: document.getElementById("kpiProfitTrend"),
  purchaseHeatmap: document.getElementById("purchaseHeatmap"),
  insightAov: document.getElementById("insightAov"),
  insightProfitPerOrder: document.getElementById("insightProfitPerOrder"),
  insightFeeRate: document.getElementById("insightFeeRate"),
  insightReturnRate: document.getElementById("insightReturnRate"),
  insightRepeatRate: document.getElementById("insightRepeatRate"),
  insightPurchaseCoverage: document.getElementById("insightPurchaseCoverage"),
  insightUniqueCustomers: document.getElementById("insightUniqueCustomers"),
  insightMissingPurchase: document.getElementById("insightMissingPurchase"),
  marketplaceCompareBody: document.getElementById("marketplaceCompareBody"),
  statusCompletedLike: document.getElementById("statusCompletedLike"),
  statusPendingLike: document.getElementById("statusPendingLike"),
  statusReturnLike: document.getElementById("statusReturnLike"),
  statusOther: document.getElementById("statusOther"),
  paymentMethodsList: document.getElementById("paymentMethodsList"),
  trendChartTitle: document.getElementById("trendChartTitle"),
  trendChartSub: document.getElementById("trendChartSub"),
  trendGranularityGroup: document.getElementById("trendGranularityGroup"),
  trendChartCanvas: document.getElementById("trendChart"),
  donutMarketplaceCanvas: document.getElementById("donutMarketplace"),
  donutMarketplaceCenterValue: document.getElementById("donutMarketplaceCenterValue"),
  donutRevenueCanvas: document.getElementById("donutRevenue"),
  donutRevenueCenterValue: document.getElementById("donutRevenueCenterValue"),
  topArticlesBody: document.getElementById("topArticlesBody"),

  ordersBody: document.getElementById("ordersBody"),
  ordersMeta: document.getElementById("ordersMeta"),

  customersKpiCount: document.getElementById("customersKpiCount"),
  customersKpiRepeat: document.getElementById("customersKpiRepeat"),
  customersKpiRepeatSub: document.getElementById("customersKpiRepeatSub"),
  customersKpiOrdersPer: document.getElementById("customersKpiOrdersPer"),
  customersKpiOrdersTotal: document.getElementById("customersKpiOrdersTotal"),
  customersKpiRevenuePer: document.getElementById("customersKpiRevenuePer"),
  customersKpiRevenueTotal: document.getElementById("customersKpiRevenueTotal"),
  customersCoverageEmail: document.getElementById("customersCoverageEmail"),
  customersCoveragePhone: document.getElementById("customersCoveragePhone"),
  customersCoverageAddress: document.getElementById("customersCoverageAddress"),
  customersCoverageCross: document.getElementById("customersCoverageCross"),
  customersMarketplaceList: document.getElementById("customersMarketplaceList"),
  customerGeoSub: document.getElementById("customerGeoSub"),
  customerGeoModeGroup: document.getElementById("customerGeoModeGroup"),
  customerGeoModeMapBtn: document.getElementById("customerGeoModeMapBtn"),
  customerGeoModeGlobeBtn: document.getElementById("customerGeoModeGlobeBtn"),
  customerGeoMapView: document.getElementById("customerGeoMapView"),
  customerGeoGlobeView: document.getElementById("customerGeoGlobeView"),
  customerGeoLoadingOverlay: document.getElementById("customerGeoLoadingOverlay"),
  customerGeoLoadingText: document.getElementById("customerGeoLoadingText"),
  customersMeta: document.getElementById("customersMeta"),
  customersBody: document.getElementById("customersBody"),

  bookingsBody: document.getElementById("bookingsBody"),
  bookingsMeta: document.getElementById("bookingsMeta"),
  bookingTxLegend: document.getElementById("bookingTxLegend"),
  bookingOrdersBody: document.getElementById("bookingOrdersBody"),
  bookingOrdersMeta: document.getElementById("bookingOrdersMeta"),
  bookingTemplatesBody: document.getElementById("bookingTemplatesBody"),
  bookingTemplatesMeta: document.getElementById("bookingTemplatesMeta"),
  bookingAccountsBody: document.getElementById("bookingAccountsBody"),
  bookingAccountsMeta: document.getElementById("bookingAccountsMeta"),
  bookingDocumentsBody: document.getElementById("bookingDocumentsBody"),
  bookingDocumentsMeta: document.getElementById("bookingDocumentsMeta"),

  createBookingDate: document.getElementById("createBookingDate"),
  createBookingType: document.getElementById("createBookingType"),
  createBookingDirection: document.getElementById("createBookingDirection"),
  createBookingAmount: document.getElementById("createBookingAmount"),
  createBookingProvider: document.getElementById("createBookingProvider"),
  createBookingStatus: document.getElementById("createBookingStatus"),
  createBookingReference: document.getElementById("createBookingReference"),
  createBookingOrder: document.getElementById("createBookingOrder"),
  createBookingAccount: document.getElementById("createBookingAccount"),
  createBookingTemplate: document.getElementById("createBookingTemplate"),
  createBookingNotes: document.getElementById("createBookingNotes"),
  createBookingTxBtn: document.getElementById("createBookingTxBtn"),

  templateNameInput: document.getElementById("templateNameInput"),
  templateTypeInput: document.getElementById("templateTypeInput"),
  templateDirectionInput: document.getElementById("templateDirectionInput"),
  templateAmountInput: document.getElementById("templateAmountInput"),
  templateProviderInput: document.getElementById("templateProviderInput"),
  templateCounterpartyInput: document.getElementById("templateCounterpartyInput"),
  templateScheduleInput: document.getElementById("templateScheduleInput"),
  templateStartDateInput: document.getElementById("templateStartDateInput"),
  templateDayInput: document.getElementById("templateDayInput"),
  templateAccountInput: document.getElementById("templateAccountInput"),
  templateNotesInput: document.getElementById("templateNotesInput"),
  createTemplateBtn: document.getElementById("createTemplateBtn"),

  accountNameInput: document.getElementById("accountNameInput"),
  accountProviderInput: document.getElementById("accountProviderInput"),
  accountActiveInput: document.getElementById("accountActiveInput"),
  createAccountBtn: document.getElementById("createAccountBtn"),

  bookingDocumentFileInput: document.getElementById("bookingDocumentFileInput"),
  bookingDocumentTxInput: document.getElementById("bookingDocumentTxInput"),
  bookingDocumentNotesInput: document.getElementById("bookingDocumentNotesInput"),
  uploadBookingDocumentBtn: document.getElementById("uploadBookingDocumentBtn"),
  googleAdsReportInput: document.getElementById("googleAdsReportInput"),
  googleAdsAssignmentInput: document.getElementById("googleAdsAssignmentInput"),
  googleAdsReportFileLabel: document.getElementById("googleAdsReportFileLabel"),
  googleAdsAssignmentFileLabel: document.getElementById("googleAdsAssignmentFileLabel"),
  googleAdsUploadBtn: document.getElementById("googleAdsUploadBtn"),
  googleAdsResetBtn: document.getElementById("googleAdsResetBtn"),
  /* Polling settings */
  pollingToggle: document.getElementById("pollingToggle"),
  pollingIntervalInput: document.getElementById("pollingIntervalInput"),
  googleAdsImportMeta: document.getElementById("googleAdsImportMeta"),
  googleAdsStatusInfo: document.getElementById("googleAdsStatusInfo"),
  googleAdsKpiCostTotal: document.getElementById("googleAdsKpiCostTotal"),
  googleAdsKpiCostSplit: document.getElementById("googleAdsKpiCostSplit"),
  googleAdsKpiRevenue: document.getElementById("googleAdsKpiRevenue"),
  googleAdsKpiProfitAfter: document.getElementById("googleAdsKpiProfitAfter"),
  googleAdsKpiProfitBefore: document.getElementById("googleAdsKpiProfitBefore"),
  googleAdsKpiRoas: document.getElementById("googleAdsKpiRoas"),
  googleAdsKpiMissing: document.getElementById("googleAdsKpiMissing"),
  googleAdsKpiCostPerOrder: document.getElementById("googleAdsKpiCostPerOrder"),
  googleAdsKpiOrderCount: document.getElementById("googleAdsKpiOrderCount"),
  googleAdsKpiAdsShare: document.getElementById("googleAdsKpiAdsShare"),
  googleAdsTrendSub: document.getElementById("googleAdsTrendSub"),
  googleAdsTrendChartCanvas: document.getElementById("googleAdsTrendChart"),
  googleAdsProfitChartCanvas: document.getElementById("googleAdsProfitChart"),
  googleAdsProfitChartSub: document.getElementById("googleAdsProfitChartSub"),
  googleAdsRoasChartCanvas: document.getElementById("googleAdsRoasChart"),
  googleAdsRoasChartSub: document.getElementById("googleAdsRoasChartSub"),
  googleAdsCumulChartCanvas: document.getElementById("googleAdsCumulChart"),
  googleAdsCumulSub: document.getElementById("googleAdsCumulSub"),
  googleAdsProductsMeta: document.getElementById("googleAdsProductsMeta"),
  googleAdsProductsBody: document.getElementById("googleAdsProductsBody"),
  googleAdsMissingMeta: document.getElementById("googleAdsMissingMeta"),
  googleAdsMissingBody: document.getElementById("googleAdsMissingBody"),

  detailsModal: document.getElementById("detailsModal"),
  detailsTitle: document.getElementById("detailsTitle"),
  detailsContent: document.getElementById("detailsContent"),
  closeModalBtn: document.getElementById("closeModalBtn"),

  previewModal: document.getElementById("previewModal"),
  previewTitle: document.getElementById("previewTitle"),
  previewMeta: document.getElementById("previewMeta"),
  previewBody: document.getElementById("previewBody"),
  closePreviewBtn: document.getElementById("closePreviewBtn"),
};

/* ── Status & Panel ── */
function setStatus(message, level = "info") {
  const className = level === "error" ? "status-error" : (level === "ok" ? "status-ok" : "status-info");
  if (els.statusBox instanceof HTMLElement) {
    els.statusBox.className = `status ${className}`;
    els.statusBox.textContent = message;
    return;
  }
  if (level === "error") {
    setLastSyncInfo(`Status: Fehler - ${String(message || "").trim()}`);
  }
}

function setLastSyncInfo(text) {
  state.lastSyncInfoText = String(text || "").trim() || "Letzter Sync: -";
  if (els.lastSyncInfo instanceof HTMLElement) {
    els.lastSyncInfo.textContent = state.lastSyncInfoText;
  }
}

function setSourcePanelOpen(isOpen) {
  const open = Boolean(isOpen);
  if (!(els.sourcePanel instanceof HTMLElement) || !(els.sourcePanelToggleBtn instanceof HTMLElement)) {
    return;
  }
  els.sourcePanel.classList.toggle("active", open);
  els.sourcePanel.setAttribute("aria-hidden", open ? "false" : "true");
  els.sourcePanelToggleBtn.setAttribute("aria-expanded", open ? "true" : "false");
}

/* ── Utility Functions ── */
function centsToMoney(cents) {
  const value = Number(cents || 0) / 100;
  return MONEY_FMT.format(value);
}

function centsToInputValue(cents) {
  const value = Number(cents || 0) / 100;
  return Number.isFinite(value) ? value.toFixed(2) : "";
}

function parsePurchaseEur(rawValue) {
  const raw = String(rawValue || "").trim();
  if (!raw) {
    return { ok: true, value: null, cents: 0 };
  }
  const compact = raw.replace(/\s+/g, "");
  const normalized = compact.includes(",") && compact.includes(".")
    ? compact.replace(/\./g, "").replace(",", ".")
    : compact.replace(",", ".");
  const numeric = Number(normalized);
  if (!Number.isFinite(numeric) || numeric < 0) {
    return { ok: false, message: "Einkaufspreis ist ungueltig." };
  }
  return { ok: true, value: numeric, cents: Math.round(numeric * 100) };
}

function updateOrderProfitPreview(row) {
  if (!(row instanceof HTMLElement)) {
    return;
  }
  const input = row.querySelector(".purchase-input");
  const profitCell = row.querySelector(".order-profit-cell");
  if (!(input instanceof HTMLInputElement) || !(profitCell instanceof HTMLElement)) {
    return;
  }

  const parsed = parsePurchaseEur(input.value);
  if (!parsed.ok) {
    return;
  }

  const afterFeesCents = Number(row.dataset.afterFeesCents || 0);
  const purchaseCents = Number(parsed.cents || 0);
  const profitCents = afterFeesCents - purchaseCents;
  profitCell.textContent = centsToMoney(profitCents);
  profitCell.classList.toggle("value-neg", profitCents < 0);
  profitCell.classList.toggle("value-pos", profitCents >= 0);
}

function formatDate(value) {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return `${DATE_FMT.format(date)} ${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function safeText(value, fallback = "-") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function detailRows(items) {
  return items.map(([label, value]) => {
    return `<div class="detail-row"><span>${escapeHtml(label)}</span><strong>${escapeHtml(safeText(value))}</strong></div>`;
  }).join("");
}

function renderAddressCard(title, address) {
  const addressObj = address && typeof address === "object" ? address : {};
  const name = [addressObj.first_name, addressObj.last_name].filter(Boolean).join(" ");
  /* Kaufland: street + house_number | Shopify: address1 (+ address2) */
  const street = [addressObj.street, addressObj.house_number].filter(Boolean).join(" ")
    || [addressObj.address1, addressObj.address2].filter(Boolean).join(", ");
  /* Kaufland: postcode | Shopify: zip */
  const plz = addressObj.postcode || addressObj.zip || "";
  const company = addressObj.company || "";
  return `<article class="detail-card">
    <h3>${escapeHtml(title)}</h3>
    <div class="detail-kv">
      ${detailRows([
        ["Name", name || addressObj.name || "-"],
        ...(company ? [["Firma", company]] : []),
        ["Strasse", street || "-"],
        ["PLZ", plz || "-"],
        ["Stadt", addressObj.city || "-"],
        ["Land", addressObj.country || addressObj.country_code || "-"],
        ["Telefon", addressObj.phone || "-"],
      ])}
    </div>
  </article>`;
}

function parseJsonLoose(value) {
  if (value && typeof value === "object") {
    return value;
  }
  if (typeof value !== "string" || !value.trim()) {
    return null;
  }
  try {
    return JSON.parse(value);
  } catch (_error) {
    return null;
  }
}

function sanitizeUrl(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  try {
    const parsed = new URL(text);
    if (parsed.protocol === "http:" || parsed.protocol === "https:") {
      return parsed.href;
    }
    return "";
  } catch (_error) {
    return "";
  }
}

function inferMimeTypeFromFilename(filename) {
  const name = String(filename || "").trim().toLowerCase();
  if (!name) {
    return "";
  }
  if (name.endsWith(".pdf")) {
    return "application/pdf";
  }
  if (name.endsWith(".png")) {
    return "image/png";
  }
  if (name.endsWith(".jpg") || name.endsWith(".jpeg")) {
    return "image/jpeg";
  }
  if (name.endsWith(".webp")) {
    return "image/webp";
  }
  if (name.endsWith(".gif")) {
    return "image/gif";
  }
  if (name.endsWith(".bmp")) {
    return "image/bmp";
  }
  return "";
}

function detectPreviewKind(mimeType, filename) {
  const mime = String(mimeType || "").trim().toLowerCase();
  if (mime.startsWith("image/")) {
    return "image";
  }
  if (mime.includes("pdf")) {
    return "pdf";
  }
  const inferred = inferMimeTypeFromFilename(filename);
  if (inferred.startsWith("image/")) {
    return "image";
  }
  if (inferred.includes("pdf")) {
    return "pdf";
  }
  return "";
}

function renderDocumentActions(downloadUrl, filename, mimeType, previewable = true) {
  const url = String(downloadUrl || "").trim();
  if (!url) {
    return "-";
  }
  const previewKind = previewable ? detectPreviewKind(mimeType, filename) : "";
  const previewBtn = previewKind
    ? `<button class="btn-inline ghost" data-action="preview-document" data-url="${escapeHtml(url)}" data-filename="${escapeHtml(filename || "Beleg")}" data-mime="${escapeHtml(mimeType || "")}" type="button">Preview</button>`
    : "";
  return `<span class="doc-actions">
    ${previewBtn}
    <a href="${escapeHtml(url)}" target="_blank" rel="noreferrer">Download</a>
  </span>`;
}

function toIsoFromLocalInput(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }
  return parsed.toISOString();
}

function toLocalInputFromIso(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }
  const localCopy = new Date(parsed.getTime() - parsed.getTimezoneOffset() * 60000);
  return localCopy.toISOString().slice(0, 16);
}

function parseEuroToCents(value) {
  let raw = String(value || "").trim();
  /* Handle German format: 1.234,56 → 1234.56 */
  if (raw.includes(",")) {
    raw = raw.replace(/\./g, "").replace(",", ".");
  }
  const numeric = Number(raw);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return null;
  }
  return Math.round(numeric * 100);
}

/* ── Date Helpers & Calendar ── */
function toDateInputValue(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    return text;
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }
  const localCopy = new Date(parsed.getTime() - parsed.getTimezoneOffset() * 60000);
  return localCopy.toISOString().slice(0, 10);
}

function dateTokenFromDate(value) {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, "0");
  const day = String(value.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function addDays(baseDate, deltaDays) {
  const copy = new Date(baseDate);
  copy.setDate(copy.getDate() + deltaDays);
  return copy;
}

function datePresetLabel(token) {
  const key = String(token || "").trim();
  if (key === "today") return "Heute";
  if (key === "yesterday") return "Gestern";
  if (key === "last_7_days") return "Letzte 7 Tage";
  if (key === "last_30_days") return "Letzte 30 Tage";
  if (key === "last_90_days") return "Letzte 90 Tage";
  if (key === "this_month") return "Dieser Monat";
  if (key === "last_month") return "Letzter Monat";
  if (key === "this_year") return "Dieses Jahr";
  if (key === "all_time") return "Alle Zeit";
  return "Zeitraum";
}

function parseDateToken(token) {
  const text = String(token || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    return null;
  }
  const parsed = new Date(`${text}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  parsed.setHours(0, 0, 0, 0);
  return parsed;
}

function monthTokenFromDate(dateValue) {
  return `${dateValue.getFullYear()}-${String(dateValue.getMonth() + 1).padStart(2, "0")}`;
}

function monthDateFromToken(token) {
  const text = String(token || "").trim();
  if (!/^\d{4}-\d{2}$/.test(text)) {
    return null;
  }
  const year = Number(text.slice(0, 4));
  const month = Number(text.slice(5, 7));
  if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) {
    return null;
  }
  return new Date(year, month - 1, 1);
}

function shiftMonthToken(token, delta) {
  const base = monthDateFromToken(token) || new Date();
  base.setDate(1);
  base.setMonth(base.getMonth() + delta);
  return monthTokenFromDate(base);
}

function formatDateTokenLabel(token) {
  const parsed = parseDateToken(token);
  if (!parsed) {
    return "-";
  }
  return DATE_FMT.format(parsed);
}

function updateDatePresetMenuSelection(token) {
  if (!(els.dateRangeMenu instanceof HTMLElement)) {
    return;
  }
  const selected = String(token || "").trim();
  const buttons = els.dateRangeMenu.querySelectorAll("[data-preset]");
  buttons.forEach((button) => {
    if (!(button instanceof HTMLElement)) {
      return;
    }
    button.classList.toggle("active", button.dataset.preset === selected);
  });
}

function updateDateRangePreview() {
  if (!(els.dateRangePreviewText instanceof HTMLElement)) {
    return;
  }
  const fromToken = String(state.dateRangeUi.customFrom || "").trim();
  const toToken = String(state.dateRangeUi.customTo || "").trim();
  const fromText = formatDateTokenLabel(fromToken);
  const toText = formatDateTokenLabel(toToken || fromToken);
  els.dateRangePreviewText.textContent = `${fromText} -> ${toText}`;
}

function renderDateCalendarMonth(targetEl, labelEl, monthDate) {
  if (!(targetEl instanceof HTMLElement) || !(labelEl instanceof HTMLElement)) {
    return;
  }
  const year = monthDate.getFullYear();
  const month = monthDate.getMonth();
  const monthLabel = monthDate.toLocaleDateString("de-DE", { month: "long", year: "numeric" });
  labelEl.textContent = monthLabel.charAt(0).toUpperCase() + monthLabel.slice(1);

  const firstWeekday = (new Date(year, month, 1).getDay() + 6) % 7;
  const daysInMonth = new Date(year, month + 1, 0).getDate();

  const fromToken = String(state.dateRangeUi.customFrom || "").trim();
  const toTokenRaw = String(state.dateRangeUi.customTo || "").trim();
  const toToken = toTokenRaw || fromToken;

  const cells = [];
  for (let idx = 0; idx < firstWeekday; idx += 1) {
    cells.push('<button class="date-day empty" type="button" tabindex="-1" aria-hidden="true"></button>');
  }

  const todayToken = dateTokenFromDate(new Date());
  for (let day = 1; day <= daysInMonth; day += 1) {
    const token = `${year}-${String(month + 1).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
    const inRange = fromToken && toToken && token >= fromToken && token <= toToken;
    const isEdge = token === fromToken || token === toToken;
    const isToday = token === todayToken;
    const classes = ["date-day"];
    if (inRange) {
      classes.push("in-range");
    }
    if (isEdge) {
      classes.push("range-edge");
    }
    if (isToday) {
      classes.push("today");
    }
    cells.push(`<button class="${classes.join(" ")}" data-date-token="${token}" type="button">${day}</button>`);
  }

  while (cells.length % 7 !== 0) {
    cells.push('<button class="date-day empty" type="button" tabindex="-1" aria-hidden="true"></button>');
  }

  targetEl.innerHTML = cells.join("");
}

function renderDateRangeCalendars() {
  const anchorToken = String(state.dateRangeUi.anchorMonth || "").trim();
  const firstMonthDate = monthDateFromToken(anchorToken) || new Date();
  firstMonthDate.setDate(1);
  const secondMonthDate = new Date(firstMonthDate.getFullYear(), firstMonthDate.getMonth() + 1, 1);
  state.dateRangeUi.anchorMonth = monthTokenFromDate(firstMonthDate);

  renderDateCalendarMonth(els.dateCalendarA, els.dateMonthLabelA, firstMonthDate);
  renderDateCalendarMonth(els.dateCalendarB, els.dateMonthLabelB, secondMonthDate);
  updateDateRangePreview();
}

function selectCustomDateToken(token) {
  const selected = String(token || "").trim();
  if (!selected) {
    return;
  }
  const fromToken = String(state.dateRangeUi.customFrom || "").trim();
  const toToken = String(state.dateRangeUi.customTo || "").trim();

  if (!fromToken || (fromToken && toToken)) {
    state.dateRangeUi.customFrom = selected;
    state.dateRangeUi.customTo = "";
  } else if (selected < fromToken) {
    state.dateRangeUi.customFrom = selected;
    state.dateRangeUi.customTo = fromToken;
  } else {
    state.dateRangeUi.customTo = selected;
  }
  state.filters.datePreset = "custom";
  updateDatePresetMenuSelection("");
  renderDateRangeCalendars();
}

function setDateRangeMenuOpen(isOpen) {
  if (!(els.dateRangeMenu instanceof HTMLElement) || !(els.dateRangeBtn instanceof HTMLElement)) {
    return;
  }
  const open = Boolean(isOpen);
  els.dateRangeMenu.classList.toggle("active", open);
  els.dateRangeMenu.setAttribute("aria-hidden", open ? "false" : "true");
  els.dateRangeBtn.setAttribute("aria-expanded", open ? "true" : "false");
  if (open) {
    renderDateRangeCalendars();
  }
}

function setExportMenuOpen(isOpen) {
  if (!(els.exportMenu instanceof HTMLElement) || !(els.exportMenuBtn instanceof HTMLElement)) {
    return;
  }
  const open = Boolean(isOpen);
  els.exportMenu.classList.toggle("active", open);
  els.exportMenu.setAttribute("aria-hidden", open ? "false" : "true");
  els.exportMenuBtn.setAttribute("aria-expanded", open ? "true" : "false");
  els.exportMenuBtn.classList.toggle("active", open);
}

function channelLabel(value) {
  const token = String(value || "").trim().toLowerCase();
  if (token === "shopify") {
    return "Shopify";
  }
  if (token === "kaufland") {
    return "Kaufland";
  }
  return "Alle";
}

function updateChannelMenuSelection(value) {
  if (!(els.channelMenu instanceof HTMLElement)) {
    return;
  }
  const selected = String(value || "").trim().toLowerCase();
  const buttons = els.channelMenu.querySelectorAll("[data-channel]");
  buttons.forEach((button) => {
    if (!(button instanceof HTMLElement)) {
      return;
    }
    const token = String(button.dataset.channel || "").trim().toLowerCase();
    button.classList.toggle("active", token === selected);
  });
}

function setMarketplaceFilter(value) {
  const normalized = String(value || "").trim().toLowerCase();
  state.filters.marketplace = normalized;
  if (els.marketplaceSelect instanceof HTMLSelectElement) {
    els.marketplaceSelect.value = normalized;
  }
  if (els.channelMenuBtn instanceof HTMLElement) {
    els.channelMenuBtn.textContent = channelLabel(normalized);
  }
  updateChannelMenuSelection(normalized);
}

/* ── Custom Select (native <select> → visual dropdown) ── */

function initCustomSelect(sel) {
  if (!sel || sel.dataset.customized || sel.hidden) return;
  sel.dataset.customized = "1";

  const control = sel.closest(".control");
  if (!control) return;
  control.classList.add("control-menu-wrap");

  const trigger = document.createElement("button");
  trigger.type = "button";
  trigger.className = "control-menu-trigger custom-select-trigger";
  trigger.setAttribute("aria-expanded", "false");

  const menu = document.createElement("div");
  menu.className = "control-menu custom-select-menu";
  menu.setAttribute("aria-hidden", "true");

  function buildItems() {
    menu.innerHTML = "";
    for (let i = 0; i < sel.options.length; i++) {
      const opt = sel.options[i];
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "menu-item";
      if (opt.selected) btn.classList.add("active");
      btn.textContent = opt.text;
      btn.dataset.value = opt.value;
      btn.addEventListener("click", () => {
        sel.value = opt.value;
        sel.dispatchEvent(new Event("change", { bubbles: true }));
        syncDisplay();
        closeCustomSelect(control);
      });
      menu.appendChild(btn);
    }
  }

  function syncDisplay() {
    const idx = sel.selectedIndex;
    trigger.textContent = idx >= 0 ? sel.options[idx].text : "";
    const items = menu.querySelectorAll(".menu-item");
    items.forEach((b, i) => b.classList.toggle("active", i === idx));
  }

  trigger.addEventListener("click", (e) => {
    e.stopPropagation();
    const isOpen = menu.classList.contains("active");
    if (isOpen) {
      closeCustomSelect(control);
    } else {
      closeAllCustomSelects();
      menu.classList.add("active");
      menu.setAttribute("aria-hidden", "false");
      trigger.setAttribute("aria-expanded", "true");
      control.style.zIndex = "40";
    }
  });

  sel.style.display = "none";
  control.insertBefore(trigger, sel);
  control.insertBefore(menu, sel);
  buildItems();
  syncDisplay();

  const observer = new MutationObserver(() => {
    buildItems();
    syncDisplay();
  });
  observer.observe(sel, { childList: true, subtree: true, characterData: true });

  const valueDesc = Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, "value");
  const idxDesc = Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, "selectedIndex");

  Object.defineProperty(sel, "value", {
    get() { return valueDesc.get.call(this); },
    set(v) {
      valueDesc.set.call(this, v);
      syncDisplay();
    },
    configurable: true,
  });

  Object.defineProperty(sel, "selectedIndex", {
    get() { return idxDesc.get.call(this); },
    set(v) {
      idxDesc.set.call(this, v);
      syncDisplay();
    },
    configurable: true,
  });

  sel._customSelect = { buildItems, syncDisplay, trigger, menu };
}

function closeCustomSelect(control) {
  const menu = control.querySelector(".custom-select-menu");
  const trigger = control.querySelector(".custom-select-trigger");
  if (menu) {
    menu.classList.remove("active");
    menu.setAttribute("aria-hidden", "true");
  }
  if (trigger) trigger.setAttribute("aria-expanded", "false");
  control.style.zIndex = "";
}

function closeAllCustomSelects() {
  document.querySelectorAll(".custom-select-menu.active").forEach((m) => {
    const wrap = m.closest(".control-menu-wrap");
    if (wrap) closeCustomSelect(wrap);
  });
}

function initAllCustomSelects(root) {
  const container = root || document;
  container.querySelectorAll(".control select:not([hidden]):not([data-customized])").forEach((sel) => {
    initCustomSelect(sel);
  });
}

function setChannelMenuOpen(isOpen) {
  if (!(els.channelMenu instanceof HTMLElement) || !(els.channelMenuBtn instanceof HTMLElement)) {
    return;
  }
  const open = Boolean(isOpen);
  els.channelMenu.classList.toggle("active", open);
  els.channelMenu.setAttribute("aria-hidden", open ? "false" : "true");
  els.channelMenuBtn.setAttribute("aria-expanded", open ? "true" : "false");
}

function setOrdersFilterOpen(isOpen) {
  if (!(els.ordersFilterDropdown instanceof HTMLElement) || !(els.ordersFilterBtn instanceof HTMLElement)) {
    return;
  }
  const open = Boolean(isOpen);
  els.ordersFilterDropdown.setAttribute("aria-hidden", open ? "false" : "true");
  els.ordersFilterBtn.setAttribute("aria-expanded", open ? "true" : "false");
}

function getActiveOrdersFilterCount() {
  return state.filters.orderStatus.size + state.filters.orderPayment.size + (state.filters.returnsOnly ? 1 : 0);
}

function updateOrdersFilterBadge() {
  if (!(els.ordersFilterBadge instanceof HTMLElement)) return;
  const count = getActiveOrdersFilterCount();
  if (count > 0) {
    els.ordersFilterBadge.textContent = String(count);
    els.ordersFilterBadge.hidden = false;
  } else {
    els.ordersFilterBadge.hidden = true;
  }
}

function setSearchModalOpen(isOpen) {
  if (!(els.searchModal instanceof HTMLElement) || !(els.searchOpenBtn instanceof HTMLElement)) {
    return;
  }
  const open = Boolean(isOpen);
  els.searchModal.classList.toggle("active", open);
  els.searchModal.setAttribute("aria-hidden", open ? "false" : "true");
  els.searchOpenBtn.setAttribute("aria-expanded", open ? "true" : "false");
  if (open && els.searchInput instanceof HTMLInputElement) {
    requestAnimationFrame(() => {
      els.searchInput.focus();
      els.searchInput.select();
    });
  }
}

function updateSearchTriggerState() {
  const hasQuery = Boolean(state.filters.q);
  if (els.searchOpenBtn instanceof HTMLElement) {
    els.searchOpenBtn.classList.toggle("active", hasQuery);
  }
  if (els.clearSearchBtn instanceof HTMLElement) {
    els.clearSearchBtn.style.display = hasQuery ? "" : "none";
  }
}

/* ── Date Preset & Custom Range ── */
function applyDatePreset(preset) {
  const token = String(preset || "last_30_days").trim();
  const today = new Date();
  today.setHours(0, 0, 0, 0);

  let fromDate = "";
  let toDate = dateTokenFromDate(today);

  if (token === "today") {
    fromDate = toDate;
  } else if (token === "yesterday") {
    fromDate = dateTokenFromDate(addDays(today, -1));
    toDate = fromDate;
  } else if (token === "last_7_days") {
    fromDate = dateTokenFromDate(addDays(today, -6));
  } else if (token === "last_30_days") {
    fromDate = dateTokenFromDate(addDays(today, -29));
  } else if (token === "last_90_days") {
    fromDate = dateTokenFromDate(addDays(today, -89));
  } else if (token === "this_month") {
    fromDate = dateTokenFromDate(new Date(today.getFullYear(), today.getMonth(), 1));
  } else if (token === "last_month") {
    const firstCurrentMonth = new Date(today.getFullYear(), today.getMonth(), 1);
    const lastPrevMonth = addDays(firstCurrentMonth, -1);
    fromDate = dateTokenFromDate(new Date(lastPrevMonth.getFullYear(), lastPrevMonth.getMonth(), 1));
    toDate = dateTokenFromDate(lastPrevMonth);
  } else if (token === "this_year") {
    fromDate = dateTokenFromDate(new Date(today.getFullYear(), 0, 1));
  } else if (token === "all_time") {
    fromDate = "1970-01-01";
  } else {
    fromDate = dateTokenFromDate(addDays(today, -29));
  }

  state.filters.datePreset = token;
  state.filters.from = fromDate;
  state.filters.to = toDate;
  state.dateRangeUi.customFrom = fromDate;
  state.dateRangeUi.customTo = toDate;
  state.dateRangeUi.anchorMonth = fromDate ? fromDate.slice(0, 7) : monthTokenFromDate(today);

  if (els.fromDate instanceof HTMLInputElement) {
    els.fromDate.value = fromDate;
  }
  if (els.toDate instanceof HTMLInputElement) {
    els.toDate.value = toDate;
  }
  if (els.dateRangeBtn instanceof HTMLElement) {
    els.dateRangeBtn.textContent = datePresetLabel(token);
  }
  updateDatePresetMenuSelection(token);
  updateDateRangePreview();
  renderDateRangeCalendars();
}

function applyCustomDateRange() {
  const fromDate = String(state.dateRangeUi.customFrom || "").trim();
  const toDateRaw = String(state.dateRangeUi.customTo || "").trim();
  const toDate = toDateRaw || fromDate;
  if (!fromDate || !toDate) {
    setStatus("Bitte Von und Bis fuer den Custom-Zeitraum setzen.", "error");
    return false;
  }
  if (fromDate > toDate) {
    setStatus("Der Zeitraum ist ungueltig (Von > Bis).", "error");
    return false;
  }

  state.filters.datePreset = "custom";
  state.filters.from = fromDate;
  state.filters.to = toDate;
  state.dateRangeUi.customFrom = fromDate;
  state.dateRangeUi.customTo = toDate;
  state.dateRangeUi.anchorMonth = fromDate.slice(0, 7);

  if (els.fromDate instanceof HTMLInputElement) {
    els.fromDate.value = fromDate;
  }
  if (els.toDate instanceof HTMLInputElement) {
    els.toDate.value = toDate;
  }
  if (els.dateRangeBtn instanceof HTMLElement) {
    els.dateRangeBtn.textContent = `${formatDateTokenLabel(fromDate)} - ${formatDateTokenLabel(toDate)}`;
  }
  updateDatePresetMenuSelection("");
  updateDateRangePreview();
  renderDateRangeCalendars();
  return true;
}

/* ── Query & Fetch ── */
function buildQuery() {
  const params = new URLSearchParams();
  if (state.filters.from) {
    params.set("from", state.filters.from);
  }
  if (state.filters.to) {
    params.set("to", state.filters.to);
  }
  if (state.filters.q) {
    params.set("q", state.filters.q);
  }
  if (state.filters.marketplace) {
    params.set("marketplace", state.filters.marketplace);
  }
  return params;
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    let message = payload?.detail || payload?.error || `HTTP ${response.status}`;
    if (message && typeof message === "object") {
      message = message.message || JSON.stringify(message);
    }
    throw new Error(message);
  }
  return payload;
}

/* ── KPI Animation ── */
function animateKpiValue(key, targetValue, renderValue, duration = 700) {
  const target = Number(targetValue);
  const normalizedTarget = Number.isFinite(target) ? target : 0;
  const previous = Number(state.kpiAnimatedValues[key]);
  const start = Number.isFinite(previous) ? previous : 0;

  if (Math.abs(start - normalizedTarget) < 0.0001) {
    state.kpiAnimatedValues[key] = normalizedTarget;
    renderValue(normalizedTarget);
    return;
  }

  const previousFrame = Number(state.kpiAnimationFrames[key] || 0);
  if (previousFrame) {
    cancelAnimationFrame(previousFrame);
  }

  const startTime = performance.now();
  const tick = (now) => {
    const progress = Math.min((now - startTime) / duration, 1);
    const eased = 1 - Math.pow(1 - progress, 3);
    const current = start + (normalizedTarget - start) * eased;
    state.kpiAnimatedValues[key] = current;
    renderValue(current);
    if (progress < 1) {
      state.kpiAnimationFrames[key] = requestAnimationFrame(tick);
      return;
    }
    state.kpiAnimatedValues[key] = normalizedTarget;
    state.kpiAnimationFrames[key] = 0;
    renderValue(normalizedTarget);
  };

  state.kpiAnimationFrames[key] = requestAnimationFrame(tick);
}

/* ── Download Helper ── */
function triggerDownload(url) {
  const link = document.createElement("a");
  link.href = url;
  link.target = "_blank";
  link.rel = "noreferrer";
  link.style.display = "none";
  document.body.appendChild(link);
  link.click();
  link.remove();
}

/* ── Return Status Helpers ── */
function normalizeReturnToken(value) {
  return String(value || "").trim().toLowerCase();
}

function isReturnLikeStatus(value) {
  const token = normalizeReturnToken(value);
  if (!token) {
    return false;
  }
  return [
    "cancel", "cancelled", "canceled", "void",
    "return", "returned", "refund", "refunded", "partially_refunded",
    "rma", "revoked",
    "returning",
  ].some((keyword) => token.includes(keyword));
}

/* ── Details & Preview Modals ── */
function closeDetailsModal() {
  /* Auto-save booking transaction changes (fire-and-forget).
     Must run BEFORE we clear state, because saveBookingFromDetailsModal
     reads state.bookingDetailsTransactionId synchronously on entry. */
  if (state.detailsMode === "booking-transaction") {
    saveBookingFromDetailsModal({ silent: true });
  }
  /* Auto-save monthly invoice changes (fire-and-forget). */
  if (state.detailsMode === "monthly-invoice") {
    saveMonthlyInvoiceFromDetail({ silent: true });
  }

  /* If we came from a transaction detail (via order drill-down), go back there */
  if (state.detailsMode === "order" && state.returnToTransactionId) {
    const txId = state.returnToTransactionId;
    state.returnToTransactionId = null;
    openBookingTransactionDetailsById(txId);
    return;
  }
  /* If we came from a Sammelrechnung detail, go back there instead of closing */
  if (state.detailsMode === "booking-transaction" && state.returnToInvoiceId) {
    const invoiceId = state.returnToInvoiceId;
    state.returnToInvoiceId = null;
    state.bookingDetailsTransactionId = null;
    openMonthlyInvoiceDetail(invoiceId);
    return;
  }
  /* If we came from an Order detail, go back there instead of closing */
  if (state.detailsMode === "booking-transaction" && state.returnToOrderKey) {
    const { marketplace, orderId } = state.returnToOrderKey;
    state.returnToOrderKey = null;
    state.bookingDetailsTransactionId = null;
    openOrderDetailById(marketplace, orderId);
    return;
  }
  state.detailsMode = "";
  state.bookingDetailsTransactionId = null;
  state.monthlyInvoiceDetailId = null;
  state.returnToInvoiceId = null;
  state.returnToTransactionId = null;
  state.returnToOrderKey = null;
  els.detailsModal.classList.remove("active");
  els.detailsModal.setAttribute("aria-hidden", "true");
}

let previewZoom = 1;
const PREVIEW_ZOOM_STEP = 0.25;
const PREVIEW_ZOOM_MIN = 0.25;
const PREVIEW_ZOOM_MAX = 5;

function updatePreviewZoom() {
  const img = els.previewBody.querySelector(".preview-image");
  if (img) {
    img.style.transform = `scale(${previewZoom})`;
    /* allow scrolling when zoomed in */
    img.style.maxWidth = previewZoom > 1 ? "none" : "100%";
    img.style.maxHeight = previewZoom > 1 ? "none" : "66vh";
  }
  const label = document.getElementById("previewZoomLevel");
  if (label) {
    label.textContent = `${Math.round(previewZoom * 100)}%`;
  }
}

function closePreviewModal() {
  els.previewModal.classList.remove("active");
  els.previewModal.setAttribute("aria-hidden", "true");
  els.previewBody.innerHTML = "-";
  els.previewTitle.textContent = "Beleg Preview";
  els.previewMeta.textContent = "-";
  previewZoom = 1;
  updatePreviewZoom();
}

function openPreviewModal(url, filename, mimeType) {
  const safeUrl = String(url || "").trim();
  if (!safeUrl) {
    setStatus("Preview-Link fehlt.", "error");
    return;
  }

  const kind = detectPreviewKind(mimeType, filename);
  if (!kind) {
    window.open(safeUrl, "_blank", "noopener,noreferrer");
    return;
  }

  previewZoom = 1;
  const safeName = String(filename || "Beleg").trim() || "Beleg";
  const metaText = [safeName, String(mimeType || inferMimeTypeFromFilename(filename) || "-")].join(" | ");
  els.previewTitle.textContent = `Preview: ${safeName}`;
  els.previewMeta.textContent = metaText;

  if (kind === "image") {
    const imgUrl = safeUrl + (safeUrl.includes("?") ? "&" : "?") + "disposition=inline";
    els.previewBody.innerHTML = `
      <img class="preview-image" src="${escapeHtml(imgUrl)}" alt="${escapeHtml(safeName)}">
    `;
  } else if (kind === "pdf") {
    const inlineUrl = safeUrl + (safeUrl.includes("?") ? "&" : "?") + "disposition=inline";
    const pdfUrl = `${inlineUrl}#toolbar=1&view=FitH`;
    els.previewBody.innerHTML = `
      <iframe class="preview-frame" src="${escapeHtml(pdfUrl)}" title="${escapeHtml(safeName)}"></iframe>
    `;
  } else {
    els.previewBody.innerHTML = `
      <div class="preview-fallback">
        <div>Direkte Preview nicht verfuegbar.</div>
        <a class="btn-inline primary" href="${escapeHtml(safeUrl)}" target="_blank" rel="noreferrer">Download oeffnen</a>
      </div>
    `;
  }

  updatePreviewZoom();
  els.previewModal.classList.add("active");
  els.previewModal.setAttribute("aria-hidden", "false");
}

/* ────────────────────────────────────────────
   Analytics Card Drag-Reorder
   ──────────────────────────────────────────── */

function initAnalyticsDragReorder() {
  const panel = document.getElementById("analyticsPanel");
  if (!panel) return;

  const STORAGE_KEY = "dash-combined.analytics-layout";
  let editing = false;
  let dragEl = null;

  /* ── Build toolbar (visible only in edit mode via CSS) ── */
  const toolbar = document.createElement("div");
  toolbar.className = "layout-toolbar";
  const editBtn = document.createElement("button");
  editBtn.type = "button";
  editBtn.className = "layout-edit-btn active";
  editBtn.textContent = "Fertig";
  toolbar.appendChild(editBtn);
  panel.insertBefore(toolbar, panel.firstChild);

  /* ── Helpers ── */
  function getSections() {
    return [...panel.querySelectorAll(":scope > [data-section-id]")];
  }

  function setEditing(on) {
    editing = on;
    panel.classList.toggle("layout-editing", editing);
    panel.querySelectorAll("[data-drag-group] > [data-card-id]").forEach((c) => {
      c.setAttribute("draggable", editing ? "true" : "false");
    });
    if (editing) updateMoveButtons();
  }

  /* ── Section reorder bars (created once, hidden until edit mode) ── */
  getSections().forEach((sec) => {
    const bar = document.createElement("div");
    bar.className = "section-reorder-bar";

    const upBtn = document.createElement("button");
    upBtn.type = "button";
    upBtn.className = "section-move-btn";
    upBtn.innerHTML = "&#9650;";
    upBtn.title = "Nach oben";
    upBtn.addEventListener("click", () => moveSection(sec, -1));

    const downBtn = document.createElement("button");
    downBtn.type = "button";
    downBtn.className = "section-move-btn";
    downBtn.innerHTML = "&#9660;";
    downBtn.title = "Nach unten";
    downBtn.addEventListener("click", () => moveSection(sec, 1));

    bar.appendChild(upBtn);
    bar.appendChild(downBtn);
    sec.insertBefore(bar, sec.firstChild);
  });

  function moveSection(sec, dir) {
    const sections = getSections();
    const idx = sections.indexOf(sec);
    const newIdx = idx + dir;
    if (newIdx < 0 || newIdx >= sections.length) return;
    const ref = sections[newIdx];
    if (dir < 0) {
      panel.insertBefore(sec, ref);
    } else {
      panel.insertBefore(sec, ref.nextSibling);
    }
    updateMoveButtons();
    saveOrder();
  }

  function updateMoveButtons() {
    const sections = getSections();
    sections.forEach((sec, i) => {
      const btns = sec.querySelectorAll(":scope > .section-reorder-bar > .section-move-btn");
      if (btns.length < 2) return;
      btns[0].disabled = i === 0;
      btns[1].disabled = i === sections.length - 1;
    });
  }

  /* ── Toggle edit mode ── */
  editBtn.addEventListener("click", () => setEditing(false));

  /* Settings-menu "Layout anpassen" entry */
  const layoutMenuBtn = document.getElementById("layoutEditMenuBtn");
  if (layoutMenuBtn) {
    layoutMenuBtn.addEventListener("click", () => {
      setSourcePanelOpen(false);
      setEditing(true);
    });
  }

  /* ── Drag & Drop (only active in edit mode) ── */
  panel.addEventListener("dragstart", (e) => {
    if (!editing) { e.preventDefault(); return; }
    const card = e.target.closest("[data-drag-group] > [data-card-id]");
    if (!card) return;

    /* never start drag from interactive children */
    if (e.target.closest("button, input, select, textarea, a, canvas, .custom-select-trigger, .custom-select-menu")) {
      e.preventDefault();
      return;
    }

    dragEl = card;
    e.dataTransfer.effectAllowed = "move";
    e.dataTransfer.setData("text/plain", card.dataset.cardId);
    requestAnimationFrame(() => card.classList.add("dragging"));
  });

  panel.addEventListener("dragover", (e) => {
    if (!dragEl) return;
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";

    const target = e.target.closest("[data-drag-group] > [data-card-id]");
    if (!target || target === dragEl) return;
    /* only reorder within the same section */
    if (target.parentElement !== dragEl.parentElement) return;

    /* clear previous highlight */
    panel.querySelectorAll(".drag-over").forEach((el) => el.classList.remove("drag-over"));
    target.classList.add("drag-over");

    /* live swap in DOM — CSS Grid reflows automatically */
    const grid = target.parentElement;
    const cards = [...grid.querySelectorAll(":scope > [data-card-id]")];
    const dragIdx = cards.indexOf(dragEl);
    const targetIdx = cards.indexOf(target);

    if (dragIdx < targetIdx) {
      grid.insertBefore(dragEl, target.nextSibling);
    } else {
      grid.insertBefore(dragEl, target);
    }
  });

  panel.addEventListener("drop", (e) => { e.preventDefault(); });

  panel.addEventListener("dragend", () => {
    if (dragEl) {
      dragEl.classList.remove("dragging");
      dragEl = null;
    }
    panel.querySelectorAll(".drag-over").forEach((el) => el.classList.remove("drag-over"));
    saveOrder();
  });

  /* ── Persistence ── */
  function saveOrder() {
    const data = {
      sections: getSections().map((s) => s.dataset.sectionId),
      cards: {},
    };
    panel.querySelectorAll("[data-drag-group]").forEach((grid) => {
      data.cards[grid.dataset.dragGroup] = [...grid.querySelectorAll(":scope > [data-card-id]")].map(
        (c) => c.dataset.cardId,
      );
    });
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
    } catch { /* quota exceeded */ }
  }

  function restoreOrder() {
    let saved;
    try {
      saved = JSON.parse(localStorage.getItem(STORAGE_KEY));
    } catch { return; }
    if (!saved || typeof saved !== "object") return;

    /* backward compat: old format was flat { groupName: [...cardIds] } */
    let sectionOrder = null;
    let cardOrder = saved;
    if (saved.sections && saved.cards) {
      sectionOrder = saved.sections;
      cardOrder = saved.cards;
    }

    /* restore section order */
    if (Array.isArray(sectionOrder)) {
      sectionOrder.forEach((id) => {
        const sec = panel.querySelector(`:scope > [data-section-id="${id}"]`);
        if (sec) panel.appendChild(sec);
      });
    }

    /* restore card order within grids */
    panel.querySelectorAll("[data-drag-group]").forEach((grid) => {
      const key = grid.dataset.dragGroup;
      const ids = cardOrder[key];
      if (!Array.isArray(ids)) return;

      const map = {};
      grid.querySelectorAll(":scope > [data-card-id]").forEach((c) => {
        map[c.dataset.cardId] = c;
      });

      /* append in saved order; cards not in the list stay at the end */
      ids.forEach((id) => {
        if (map[id]) grid.appendChild(map[id]);
      });
    });
  }

  /* ── Init ── */
  restoreOrder();
}
