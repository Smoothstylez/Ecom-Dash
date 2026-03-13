"use strict";

/* ── Bookings Tab ── */

/* ── Constants & Category Helpers ── */

const BOOKING_TX_CATEGORY_META = {
  sale: {
    label: "Sale",
    longLabel: "Sale (Umsatz)",
    badgeClass: "badge-sale",
    rowClass: "tx-row-sale",
  },
  fee: {
    label: "Fee",
    longLabel: "Fee",
    badgeClass: "badge-fee",
    rowClass: "tx-row-fee",
  },
  cogs: {
    label: "Produkteinkauf",
    longLabel: "Produkteinkauf (COGS)",
    badgeClass: "badge-cogs",
    rowClass: "tx-row-cogs",
  },
  invoice: {
    label: "Sonstige Rechnung",
    longLabel: "Sonstige Rechnung",
    badgeClass: "badge-invoice",
    rowClass: "tx-row-invoice",
  },
  subscription: {
    label: "Subscription",
    longLabel: "Subscription",
    badgeClass: "badge-subscription",
    rowClass: "tx-row-subscription",
  },
  refund: {
    label: "Refund",
    longLabel: "Refund",
    badgeClass: "badge-refund",
    rowClass: "tx-row-refund",
  },
  other: {
    label: "Sonstiges",
    longLabel: "Sonstiges",
    badgeClass: "badge-default",
    rowClass: "tx-row-other",
  },
};

const BOOKING_TX_TYPE_TO_CATEGORY = {
  SALE: "sale",
  FEE: "fee",
  COGS: "cogs",
  EXPENSE: "invoice",
  SUBSCRIPTION: "subscription",
  REFUND: "refund",
  PAYOUT: "other",
  ADJUSTMENT: "other",
};

const BOOKING_TX_TYPE_OPTIONS = ["SALE", "COGS", "FEE", "SUBSCRIPTION", "EXPENSE", "REFUND", "PAYOUT", "ADJUSTMENT"];
const BOOKING_TX_DIRECTION_OPTIONS = ["IN", "OUT"];
const BOOKING_TX_STATUS_OPTIONS = ["pending", "confirmed", "reconciled"];

/* ── Sammelrechnung Month Picker ── */
const SAMMEL_MONTH_NAMES_DE = [
  "Jan", "Feb", "Mär", "Apr", "Mai", "Jun",
  "Jul", "Aug", "Sep", "Okt", "Nov", "Dez",
];
const SAMMEL_MONTH_FULL_DE = [
  "Januar", "Februar", "März", "April", "Mai", "Juni",
  "Juli", "August", "September", "Oktober", "November", "Dezember",
];

/** Currently selected month token (e.g. "2026-02") — set by initSammelrechnungDefaults */
let _sammelSelectedMonth = "";
/** Year currently displayed in the month picker grid */
let _sammelPickerYear = new Date().getFullYear();

let filterRefreshTimerId = 0;
let filterRefreshRunning = false;
let filterRefreshQueued = false;

function normalizeBookingTxType(value) {
  return String(value || "").trim().toUpperCase();
}

function bookingTxCategoryKeyForType(type) {
  const normalized = normalizeBookingTxType(type);
  return BOOKING_TX_TYPE_TO_CATEGORY[normalized] || "other";
}

function bookingTxCategoryMetaForType(type) {
  const key = bookingTxCategoryKeyForType(type);
  return {
    key,
    ...BOOKING_TX_CATEGORY_META[key],
  };
}

function renderBookingTxLegend() {
  if (!(els.bookingTxLegend instanceof HTMLElement)) {
    return;
  }

  const allItems = state.bookingsAllItems || [];
  const activeCategory = String(state.bookingTxFilters.category || "").trim().toLowerCase();

  const counters = {
    sale: 0,
    fee: 0,
    cogs: 0,
    invoice: 0,
    subscription: 0,
    refund: 0,
    other: 0,
  };

  allItems.forEach((item) => {
    const key = bookingTxCategoryKeyForType(item?.type);
    counters[key] = Number(counters[key] || 0) + 1;
  });

  const total = allItems.length;
  const gesamtActive = !activeCategory ? " active" : "";
  let html = `<span class="tx-legend-item${gesamtActive}" data-filter-category="">` +
    `<span class="badge badge-default">Gesamt</span>` +
    `<span class="tx-legend-count">${escapeHtml(NUMBER_FMT.format(total))}</span></span>`;

  const order = ["sale", "fee", "cogs", "invoice", "subscription", "refund", "other"];
  order.forEach((key) => {
    const meta = BOOKING_TX_CATEGORY_META[key];
    const isActive = activeCategory === key ? " active" : "";
    html += `<span class="tx-legend-item${isActive}" data-filter-category="${escapeHtml(key)}">` +
      `<span class="badge ${meta.badgeClass}">${escapeHtml(meta.label)}</span>` +
      `<span class="tx-legend-count">${escapeHtml(NUMBER_FMT.format(counters[key]))}</span></span>`;
  });

  els.bookingTxLegend.innerHTML = html;
}

/* ── Period Key ── */

function currentPeriodKey() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
}

/* ── Period Helpers & Select Options ── */

function periodKeyFromDateLike(value) {
  const dateToken = toDateInputValue(value);
  if (!dateToken) {
    return "";
  }
  return dateToken.slice(0, 7);
}

function parsePeriodKeyToIndex(periodKey) {
  const token = String(periodKey || "").trim();
  if (!/^\d{4}-\d{2}$/.test(token)) {
    return null;
  }
  const year = Number(token.slice(0, 4));
  const month = Number(token.slice(5, 7));
  if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) {
    return null;
  }
  return { year, month };
}

function buildPeriodKeyRange(startPeriodKey, endPeriodKey) {
  const start = parsePeriodKeyToIndex(startPeriodKey);
  const end = parsePeriodKeyToIndex(endPeriodKey);
  if (!start || !end) {
    return [];
  }
  const startIndex = start.year * 12 + (start.month - 1);
  const endIndex = end.year * 12 + (end.month - 1);
  if (startIndex > endIndex) {
    return [];
  }
  const span = endIndex - startIndex + 1;
  if (span > 240) {
    return [];
  }

  const periods = [];
  for (let idx = startIndex; idx <= endIndex; idx += 1) {
    const year = Math.floor(idx / 12);
    const month = (idx % 12) + 1;
    periods.push(`${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}`);
  }
  return periods;
}

function renderSelectOptions(items, selectedValue, getLabel, emptyLabel = "-") {
  const selected = selectedValue == null ? "" : String(selectedValue);
  const options = [`<option value="">${escapeHtml(emptyLabel)}</option>`];
  items.forEach((item) => {
    const id = String(item.id || "").trim();
    if (!id) {
      return;
    }
    const isSelected = id === selected ? " selected" : "";
    options.push(`<option value="${escapeHtml(id)}"${isSelected}>${escapeHtml(getLabel(item))}</option>`);
  });
  return options.join("");
}

/* ── Load Functions ── */

async function loadBookings() {
  const params = new URLSearchParams();
  if (state.filters.from) {
    params.set("dateFrom", state.filters.from);
  }
  if (state.filters.to) {
    params.set("dateTo", state.filters.to);
  }
  if (state.filters.marketplace) {
    params.set("provider", state.filters.marketplace);
  }
  if (state.bookingTxFilters.type) {
    params.set("type", state.bookingTxFilters.type);
  }
  if (state.bookingClass && state.bookingClass !== "all") {
    params.set("bookingClass", state.bookingClass);
  }
  const payload = await fetchJson(`${API_BASE}/bookings/transactions?${params.toString()}`);
  let items = Array.isArray(payload.items) ? payload.items : [];

  state.bookingsAllItems = items;

  const selectedCategory = String(state.bookingTxFilters.category || "").trim().toLowerCase();
  if (selectedCategory) {
    items = items.filter((row) => bookingTxCategoryKeyForType(row?.type) === selectedCategory);
  }

  const needle = String(state.filters.q || "").trim().toLowerCase();
  if (needle) {
    items = items.filter((row) => {
      return [
        row.provider,
        row.counterparty_name,
        row.reference,
        row.notes,
        row.type,
      ].some((value) => String(value || "").toLowerCase().includes(needle));
    });
  }
  state.bookings = items;
  state.bookingsTotal = items.length;
}

async function loadBookingOrders() {
  const params = buildQuery();
  const payload = await fetchJson(`${API_BASE}/bookings/orders?${params.toString()}`);
  state.bookingOrders = Array.isArray(payload.items) ? payload.items : [];
  state.bookingOrdersTotal = Number(payload.total || state.bookingOrders.length || 0);
}

async function loadBookkeepingLedgerOrders() {
  const payload = await fetchJson(`${API_BASE}/bookings/ledger/orders`);
  state.bookkeepingLedgerOrders = Array.isArray(payload.items) ? payload.items : [];
  state.bookkeepingLedgerOrdersTotal = Number(payload.total || state.bookkeepingLedgerOrders.length || 0);
}

async function loadBookingAccounts() {
  const payload = await fetchJson(`${API_BASE}/bookings/payment-accounts`);
  state.bookingAccounts = Array.isArray(payload.items) ? payload.items : [];
  state.bookingAccountsTotal = Number(payload.total || state.bookingAccounts.length || 0);
}

async function loadBookingTemplates() {
  const payload = await fetchJson(`${API_BASE}/bookings/templates`);
  state.bookingTemplates = Array.isArray(payload.items) ? payload.items : [];
  state.bookingTemplatesTotal = Number(payload.total || state.bookingTemplates.length || 0);
}

async function loadBookingDocuments() {
  const payload = await fetchJson(`${API_BASE}/bookings/documents`);
  state.bookingDocuments = Array.isArray(payload.items) ? payload.items : [];
  state.bookingDocumentsTotal = Number(payload.total || state.bookingDocuments.length || 0);
}

/* ── Render Functions ── */

function bookingOrderLabel(order) {
  return `${String(order.provider || "-").toUpperCase()} | ${String(order.external_order_id || order.id || "-")}`;
}

function bookingAccountLabel(account) {
  const provider = String(account.provider || "").trim();
  return provider ? `${account.name} (${provider})` : `${account.name}`;
}

function bookingTemplateLabel(template) {
  const amount = Number(template.default_amount_gross || 0);
  const amountText = amount > 0 ? centsToMoney(amount) : "-";
  return `${template.name} | ${template.schedule} | ${amountText}`;
}

function bookingDocumentLabel(doc) {
  return doc.original_filename || doc.stored_filename || doc.id;
}

function bookingTransactionLabel(tx) {
  const when = formatDate(tx.date);
  const ref = tx.reference || tx.type || tx.id;
  return `${when} | ${ref}`;
}

function refreshBookingFormOptions() {
  els.createBookingOrder.innerHTML = renderSelectOptions(
    state.bookkeepingLedgerOrders,
    "",
    (order) => bookingOrderLabel(order),
    "Keine Zuordnung",
  );
  els.createBookingAccount.innerHTML = renderSelectOptions(
    state.bookingAccounts,
    "",
    (account) => bookingAccountLabel(account),
    "Ohne Konto",
  );
  els.createBookingTemplate.innerHTML = renderSelectOptions(
    state.bookingTemplates,
    "",
    (template) => bookingTemplateLabel(template),
    "Ohne Template",
  );
  els.templateAccountInput.innerHTML = renderSelectOptions(
    state.bookingAccounts,
    "",
    (account) => bookingAccountLabel(account),
    "Ohne Konto",
  );
  els.bookingDocumentTxInput.innerHTML = renderSelectOptions(
    state.bookings,
    "",
    (booking) => bookingTransactionLabel(booking),
    "Keine Verknuepfung",
  );
}

function renderBookings() {
  const accountOptions = (selected) => renderSelectOptions(
    state.bookingAccounts,
    selected,
    (account) => bookingAccountLabel(account),
    "Ohne Konto",
  );

  const rows = state.bookings.map((booking) => {
    const amountCents = Number(booking.amount_gross || 0);
    const documentPayload = booking.document && typeof booking.document === "object" ? booking.document : {};
    const typeCode = normalizeBookingTxType(booking.type) || "-";
    const categoryMeta = bookingTxCategoryMetaForType(booking.type);
    const docUrl = booking.document_id
      ? `${API_BASE}/bookings/documents/${encodeURIComponent(booking.document_id)}/download`
      : "";
    const docFilename = documentPayload.original_filename || booking.document_id || "";
    const docMime = documentPayload.mime_type || "";
    const docPreviewKind = docUrl ? detectPreviewKind(docMime, docFilename) : "";
    const documentHtml = booking.document_id
      ? (docPreviewKind
          ? `<a href="#" class="doc-link" data-action="preview-document" data-url="${escapeHtml(docUrl)}" data-filename="${escapeHtml(docFilename || "Beleg")}" data-mime="${escapeHtml(docMime)}">${escapeHtml(docFilename)}</a>`
          : `<a href="${escapeHtml(docUrl)}" target="_blank" rel="noreferrer" class="doc-link">${escapeHtml(docFilename)}</a>`)
      : "-";

    return `<tr class="${escapeHtml(categoryMeta.rowClass)}" data-booking-id="${escapeHtml(booking.id)}">
      <td>${escapeHtml(formatDate(booking.date))}</td>
      <td>
        <span class="badge ${escapeHtml(categoryMeta.badgeClass)}">${escapeHtml(typeCode)}</span>
        <div class="tx-type-sub">${escapeHtml(categoryMeta.longLabel)}</div>
      </td>
      <td>${escapeHtml(booking.provider || "-")}</td>
      <td>${escapeHtml(booking.direction || "-")}</td>
      <td>${escapeHtml(centsToMoney(amountCents))}</td>
      <td>${escapeHtml(booking.reference || "-")}</td>
      <td><span class="cell-truncate" title="${escapeHtml(booking.notes || "")}">${escapeHtml(booking.notes || "-")}</span></td>
      <td>
        <select class="booking-select" data-field="payment_account_id">${accountOptions(booking.payment_account_id)}</select>
      </td>
      <td>${documentHtml}</td>
    </tr>`;
  }).join("");

  els.bookingsBody.innerHTML = rows || "<tr><td colspan=\"9\">Keine Buchungen gefunden.</td></tr>";
  const filterHints = [];
  if (state.bookingTxFilters.type) {
    filterHints.push(`Typ ${state.bookingTxFilters.type}`);
  }
  if (state.bookingTxFilters.category) {
    const categoryMeta = BOOKING_TX_CATEGORY_META[state.bookingTxFilters.category];
    if (categoryMeta) {
      filterHints.push(`Gruppe ${categoryMeta.label}`);
    }
  }
  const suffix = filterHints.length ? ` (${filterHints.join(" | ")})` : "";
  els.bookingsMeta.textContent = `${NUMBER_FMT.format(state.bookingsTotal)} Zeilen${suffix}`;
  renderBookingTxLegend();
}

function renderBookingOrders() {
  const rows = state.bookingOrders.map((order) => {
    const profitClass = Number(order.profit_cents || 0) < 0 ? "value-neg" : "value-pos";
    const costDetails = [
      `Fees: ${centsToMoney(order.fees_cents || 0)}`,
      `Einkauf: ${centsToMoney(order.purchase_cents || 0)}`,
      `Zusatz-Buchungen: ${centsToMoney(order.bookkeeping_expense_cents || 0)}`,
    ];

    return `<tr data-marketplace="${escapeHtml(order.marketplace)}" data-order-id="${escapeHtml(order.order_id)}" data-external-order-id="${escapeHtml(order.external_order_id)}">
      <td>${escapeHtml(formatDate(order.order_date))}</td>
      <td>${escapeHtml(order.marketplace || "-")}</td>
      <td>${escapeHtml(order.external_order_id || order.order_id || "-")}</td>
      <td>${escapeHtml(order.customer || "-")}</td>
      <td>
        <div><strong>${escapeHtml(centsToMoney(order.revenue_cents || 0))}</strong></div>
        <div class="cell-sub">Buchungs-In: ${escapeHtml(centsToMoney(order.bookkeeping_income_cents || 0))}</div>
      </td>
      <td>
        <div><strong>${escapeHtml(centsToMoney(order.total_costs_cents || 0))}</strong></div>
        ${costDetails.map((line) => `<div class="cell-sub">${escapeHtml(line)}</div>`).join("")}
      </td>
      <td class="${profitClass}">
        <div><strong>${escapeHtml(centsToMoney(order.profit_cents || 0))}</strong></div>
        <div class="cell-sub">Match: ${escapeHtml(order.bookkeeping_matched_via || "none")}</div>
      </td>
      <td>${escapeHtml(NUMBER_FMT.format(Number(order.documents_count || 0)))}</td>
      <td>
        <button class="btn-inline" data-action="details" type="button">Details</button>
      </td>
    </tr>`;
  }).join("");

  els.bookingOrdersBody.innerHTML = rows || "<tr><td colspan=\"9\">Keine Bestellungen fuer aktuellen Filter.</td></tr>";
  els.bookingOrdersMeta.textContent = `${NUMBER_FMT.format(state.bookingOrdersTotal)} Zeilen`;
}

function renderBookingTemplates() {
  const rows = state.bookingTemplates.map((template) => {
    const amountEur = centsToInputValue(template.default_amount_gross || 0);
    const startDateValue = toDateInputValue(template.start_date);
    const defaultPeriodKey = periodKeyFromDateLike(template.start_date) || currentPeriodKey();
    const accountOptions = renderSelectOptions(
      state.bookingAccounts,
      template.payment_account_id,
      (account) => bookingAccountLabel(account),
      "Ohne Konto",
    );
    const activeValue = template.active ? "true" : "false";

    return `<tr data-template-id="${escapeHtml(template.id)}">
      <td><input class="booking-input notes" data-field="name" value="${escapeHtml(template.name || "")}"></td>
      <td>${escapeHtml(template.type || "-")}</td>
      <td>${escapeHtml(template.direction || "-")}</td>
      <td><input class="booking-input" data-field="counterparty_name" value="${escapeHtml(template.counterparty_name || "")}"></td>
      <td><input class="booking-input" data-field="start_date" type="date" value="${escapeHtml(startDateValue)}"></td>
      <td><input class="booking-input" data-field="default_amount_eur" type="number" step="0.01" min="0.01" value="${escapeHtml(amountEur)}"></td>
      <td>
        <select class="booking-select" data-field="schedule">
          <option value="monthly" ${template.schedule === "monthly" ? "selected" : ""}>monthly</option>
          <option value="quarterly" ${template.schedule === "quarterly" ? "selected" : ""}>quarterly</option>
          <option value="yearly" ${template.schedule === "yearly" ? "selected" : ""}>yearly</option>
        </select>
      </td>
      <td><select class="booking-select" data-field="payment_account_id">${accountOptions}</select></td>
      <td>
        <select class="booking-select" data-field="active">
          <option value="true" ${activeValue === "true" ? "selected" : ""}>true</option>
          <option value="false" ${activeValue === "false" ? "selected" : ""}>false</option>
        </select>
      </td>
      <td>
        <div class="inline-note">
          <input class="booking-input" data-field="period_key" type="month" value="${escapeHtml(defaultPeriodKey)}">
          <button class="btn-inline" data-action="generate-template" type="button">Run</button>
          <button class="btn-inline ghost" data-action="generate-template-backfill" type="button">Seit Start</button>
        </div>
      </td>
    </tr>`;
  }).join("");

  els.bookingTemplatesBody.innerHTML = rows || "<tr><td colspan=\"10\">Keine Templates vorhanden.</td></tr>";
  els.bookingTemplatesMeta.textContent = `${NUMBER_FMT.format(state.bookingTemplatesTotal)} Zeilen`;
}

function renderBookingAccounts() {
  const rows = state.bookingAccounts.map((account) => {
    const activeValue = account.is_active ? "true" : "false";
    return `<tr data-account-id="${escapeHtml(account.id)}">
      <td><input class="booking-input notes" data-field="name" value="${escapeHtml(account.name || "")}"></td>
      <td><input class="booking-input" data-field="provider" value="${escapeHtml(account.provider || "")}"></td>
      <td>
        <select class="booking-select" data-field="is_active">
          <option value="true" ${activeValue === "true" ? "selected" : ""}>true</option>
          <option value="false" ${activeValue === "false" ? "selected" : ""}>false</option>
        </select>
      </td>
    </tr>`;
  }).join("");

  els.bookingAccountsBody.innerHTML = rows || "<tr><td colspan=\"3\">Keine Konten vorhanden.</td></tr>";
  els.bookingAccountsMeta.textContent = `${NUMBER_FMT.format(state.bookingAccountsTotal)} Zeilen`;
}

function renderBookingDocuments() {
  const rows = state.bookingDocuments.map((document) => {
    const txCount = Number(document?._count?.transactions || 0);
    const actions = renderDocumentActions(
      `${API_BASE}/bookings/documents/${encodeURIComponent(document.id)}/download`,
      document.original_filename || document.id,
      document.mime_type,
      true,
    );
    return `<tr>
      <td>${escapeHtml(formatDate(document.uploaded_at))}</td>
      <td>${escapeHtml(document.original_filename || "-")}</td>
      <td>${escapeHtml(document.stored_filename || "-")}</td>
      <td>${escapeHtml(NUMBER_FMT.format(txCount))}</td>
      <td>${actions}</td>
    </tr>`;
  }).join("");

  els.bookingDocumentsBody.innerHTML = rows || "<tr><td colspan=\"5\">Keine Belege vorhanden.</td></tr>";
  els.bookingDocumentsMeta.textContent = `${NUMBER_FMT.format(state.bookingDocumentsTotal)} Zeilen`;
}

/* ── Booking Class Segment Control ── */

const SAMMELRECHNUNG_PROVIDERS = {
  paypal: "PayPal Fees",
  shopify_payments: "Shopify Payments Fees",
  kaufland: "Kaufland Fees",
  google_ads: "Google Ads",
  ebay: "eBay Fees",
};

function setBookingClass(bookingClass) {
  const allowed = new Set(["all", "automatic", "monthly", "single"]);
  state.bookingClass = allowed.has(bookingClass) ? bookingClass : "automatic";

  /* Update subtab-bar active state */
  if (els.bookingClassAllBtn) {
    els.bookingClassAllBtn.classList.toggle("active", state.bookingClass === "all");
  }
  if (els.bookingClassAutoBtn) {
    els.bookingClassAutoBtn.classList.toggle("active", state.bookingClass === "automatic");
  }
  if (els.bookingClassMonthlyBtn) {
    els.bookingClassMonthlyBtn.classList.toggle("active", state.bookingClass === "monthly");
  }
  if (els.bookingClassSingleBtn) {
    els.bookingClassSingleBtn.classList.toggle("active", state.bookingClass === "single");
  }

  /* Reset category / type filters when switching class */
  state.bookingTxFilters.category = "";
  state.bookingTxFilters.type = "";

  /* Show/hide the "Neue Transaktion" form — only for Einzeln */
  const txTools = document.getElementById("bookingsTransactionTools");
  if (txTools) {
    txTools.style.display = state.bookingClass === "single" ? "" : "none";
    if (state.bookingClass !== "single") {
      txTools.classList.remove("open");
    }
  }

  /* Show/hide Sammelrechnung section — only for Monatlich */
  if (els.sammelrechnungSection) {
    els.sammelrechnungSection.style.display = state.bookingClass === "monthly" ? "" : "none";
  }

  /* Update the unified "+" button */
  updateBookingNewBtnForClass();
}

function updateBookingNewBtnForClass() {
  if (!els.bookingsNewBtn) return;
  if (state.bookingsSubtab !== "transactions") return;

  const classCfg = {
    all:       null,
    automatic: null,
    monthly:   { target: "sammelrechnungTools", label: "Neue Sammelrechnung" },
    single:    { target: "bookingsTransactionTools", label: "Neue Transaktion" },
  };
  const cfg = classCfg[state.bookingClass];
  if (cfg) {
    els.bookingsNewBtn.style.display = "";
    els.bookingsNewBtn.setAttribute("data-target", cfg.target);
    els.bookingsNewBtn.setAttribute("aria-expanded", "false");
    const nodes = Array.from(els.bookingsNewBtn.childNodes);
    const textNode = nodes.filter((n) => n.nodeType === Node.TEXT_NODE).pop();
    if (textNode) {
      textNode.textContent = " " + cfg.label;
    }
  } else {
    /* Automatisch — no create button */
    els.bookingsNewBtn.style.display = "none";
  }

  /* Close any open tools panel */
  document.querySelectorAll(".bookings-tools.open").forEach((p) => p.classList.remove("open"));
}

/* ── Monthly Invoice (Sammelrechnung) CRUD ── */

async function loadMonthlyInvoices() {
  const payload = await fetchJson(`${API_BASE}/bookings/monthly-invoices`);
  state.monthlyInvoices = Array.isArray(payload.items) ? payload.items : [];
  state.monthlyInvoicesTotal = Number(payload.total || state.monthlyInvoices.length || 0);
}

function renderMonthlyInvoices() {
  if (!els.sammelrechnungBody) return;

  const rows = state.monthlyInvoices.map((inv) => {
    const invoiceCents = Number(inv.invoice_amount_cents || 0);
    const calculatedCents = Number(inv.calculated_sum_cents || 0);
    const diffCents = Number(inv.difference_cents || 0);
    const providerLabel = SAMMELRECHNUNG_PROVIDERS[inv.provider] || inv.provider || "-";
    const periodFrom = formatDate(inv.period_from);
    const periodTo = formatDate(inv.period_to);
    const statusBadge = inv.status === "matched"
      ? `<span class="badge badge-sale">Matched</span>`
      : inv.status === "mismatch"
        ? `<span class="badge badge-refund">Differenz</span>`
        : `<span class="badge badge-default">${escapeHtml(inv.status || "draft")}</span>`;
    const diffClass = diffCents !== 0 ? "value-neg" : "value-pos";

    /* Document column: upload button or document actions */
    let docCell;
    if (inv.document_id && inv.document) {
      const dlUrl = `${API_BASE}/bookings/documents/${encodeURIComponent(inv.document_id)}/download`;
      docCell = renderDocumentActions(dlUrl, inv.document.original_filename || "Beleg", inv.document.mime_type, true);
    } else if (inv.document_id) {
      /* document_id exists but document object not returned — show label */
      docCell = `<span class="badge badge-default">Beleg</span>`;
    } else {
      docCell = `<button class="btn-inline ghost" data-action="upload-invoice-doc" type="button">Hochladen</button>`;
    }

    return `<tr data-invoice-id="${escapeHtml(inv.id)}">
      <td>${escapeHtml(providerLabel)}</td>
      <td>${escapeHtml(periodFrom)} &ndash; ${escapeHtml(periodTo)}</td>
      <td>${escapeHtml(centsToMoney(invoiceCents))}</td>
      <td>${escapeHtml(centsToMoney(calculatedCents))}</td>
      <td class="${diffClass}">${escapeHtml(centsToMoney(diffCents))}</td>
      <td>${statusBadge}</td>
      <td>${docCell}</td>
      <td><span class="cell-truncate" title="${escapeHtml(inv.notes || "")}">${escapeHtml(inv.notes || "-")}</span></td>
      <td><button class="btn-inline danger" data-action="delete-invoice" type="button">Loeschen</button></td>
    </tr>`;
  }).join("");

  els.sammelrechnungBody.innerHTML = rows || `<tr><td colspan="9">Keine Sammelrechnungen vorhanden.</td></tr>`;
  if (els.sammelrechnungMeta) {
    els.sammelrechnungMeta.textContent = `${NUMBER_FMT.format(state.monthlyInvoicesTotal)} Zeilen`;
  }
}

/* ── Sammelrechnung Detail Modal ── */

function renderMonthlyInvoiceDetailHtml(inv) {
  const providerLabel = SAMMELRECHNUNG_PROVIDERS[inv.provider] || inv.provider || "-";
  const invoiceCents = Number(inv.invoice_amount_cents || 0);
  const calculatedCents = Number(inv.calculated_sum_cents || 0);
  const diffCents = Number(inv.difference_cents || 0);
  const statusLabel = inv.status === "matched" ? "Matched"
    : inv.status === "mismatch" ? "Differenz"
    : inv.status || "Entwurf";
  const statusBadgeClass = inv.status === "matched" ? "badge-sale"
    : inv.status === "mismatch" ? "badge-refund"
    : "badge-default";

  /* Document info */
  let docLabel = "-";
  let docActions = "";
  if (inv.document_id && inv.document) {
    docLabel = inv.document.original_filename || "Beleg";
    const dlUrl = `${API_BASE}/bookings/documents/${encodeURIComponent(inv.document_id)}/download`;
    docActions = renderDocumentActions(dlUrl, docLabel, inv.document.mime_type, true);
  }

  /* Provider select options */
  const providerOptions = Object.entries(SAMMELRECHNUNG_PROVIDERS).map(([key, label]) => {
    const sel = key === inv.provider ? " selected" : "";
    return `<option value="${escapeHtml(key)}"${sel}>${escapeHtml(label)}</option>`;
  }).join("");

  /* Document display for edit form */
  const currentDocHtml = inv.document_id && inv.document
    ? `<span class="sammel-edit-current-doc">${escapeHtml(inv.document.original_filename || "Beleg")} ${docActions}</span>`
    : `<span class="sammel-edit-current-doc">Kein Beleg</span>`;

  /* Linked transactions table */
  const txs = Array.isArray(inv.transactions) ? inv.transactions : [];
  let txTableHtml;
  if (txs.length === 0) {
    txTableHtml = `<p class="sammel-detail-empty">Keine verknuepften Transaktionen.</p>`;
  } else {
    const txRows = txs.map((tx) => {
      const typeCode = String(tx.type || "").toUpperCase();
      const catKey = BOOKING_TX_TYPE_TO_CATEGORY[typeCode] || "other";
      const catMeta = BOOKING_TX_CATEGORY_META[catKey] || BOOKING_TX_CATEGORY_META.other;
      return `<tr class="${escapeHtml(catMeta.rowClass)}" data-tx-id="${escapeHtml(String(tx.id || ""))}" style="cursor:pointer">
        <td>${escapeHtml(formatDate(tx.date))}</td>
        <td><span class="badge ${escapeHtml(catMeta.badgeClass)}">${escapeHtml(typeCode)}</span></td>
        <td>${escapeHtml(centsToMoney(tx.amount_gross || 0))}</td>
        <td>${escapeHtml(tx.counterparty_name || "-")}</td>
        <td>${escapeHtml(tx.reference || "-")}</td>
        <td><span class="cell-truncate" title="${escapeHtml(tx.notes || "")}">${escapeHtml(tx.notes || "-")}</span></td>
      </tr>`;
    }).join("");

    const txSum = txs.reduce((sum, tx) => sum + Number(tx.amount_gross || 0), 0);

    txTableHtml = `
      <div class="table-shell sammel-detail-table">
        <table>
          <thead>
            <tr>
              <th>Datum</th>
              <th>Typ</th>
              <th>Betrag</th>
              <th>Gegenpartei</th>
              <th>Referenz</th>
              <th>Notiz</th>
            </tr>
          </thead>
          <tbody>${txRows}</tbody>
          <tfoot>
            <tr>
              <td colspan="2"><strong>Summe (${NUMBER_FMT.format(txs.length)} Transaktion${txs.length === 1 ? "" : "en"})</strong></td>
              <td><strong>${escapeHtml(centsToMoney(txSum))}</strong></td>
              <td colspan="3"></td>
            </tr>
          </tfoot>
        </table>
      </div>`;
  }

  return `<div class="sammel-detail-shell" data-invoice-id="${escapeHtml(String(inv.id || ""))}">
    <section class="detail-grid">
      <article class="detail-card">
        <h3>Sammelrechnung</h3>
        <div class="detail-kv">
          ${detailRows([
            ["Provider", providerLabel],
            ["Zeitraum", formatDate(inv.period_from) + " \u2013 " + formatDate(inv.period_to)],
            ["Waehrung", inv.currency || "EUR"],
            ["Erstellt", formatDate(inv.created_at)],
            ["Aktualisiert", formatDate(inv.updated_at)],
          ])}
        </div>
      </article>
      <article class="detail-card">
        <h3>Abgleich</h3>
        <div class="detail-kv">
          <div class="detail-row"><span>Status</span><strong><span class="badge ${escapeHtml(statusBadgeClass)}">${escapeHtml(statusLabel)}</span></strong></div>
          ${detailRows([
            ["Rechnungsbetrag", centsToMoney(invoiceCents)],
            ["Berechnete Summe", centsToMoney(calculatedCents)],
            ["Differenz", centsToMoney(diffCents)],
          ])}
        </div>
      </article>
      <article class="detail-card">
        <h3>Beleg &amp; Notiz</h3>
        <div class="detail-kv">
          ${inv.document_id ? `<div class="detail-row"><span>Beleg</span><strong>${docActions || escapeHtml(docLabel)}</strong></div>` : `<div class="detail-row"><span>Beleg</span><strong>-</strong></div>`}
          <div class="detail-row"><span>Notiz</span><strong>${escapeHtml(inv.notes || "-")}</strong></div>
        </div>
      </article>
    </section>

    <section class="booking-detail-form">
      <h3>Sammelrechnung bearbeiten</h3>
      <div class="booking-detail-form-grid">
        <div class="control">
          <label for="sammelDetailProvider">Provider</label>
          <select id="sammelDetailProvider">${providerOptions}</select>
        </div>
        <div class="control">
          <label for="sammelDetailPeriodFrom">Zeitraum von</label>
          <input id="sammelDetailPeriodFrom" type="date" value="${escapeHtml((inv.period_from || "").substring(0, 10))}">
        </div>
        <div class="control">
          <label for="sammelDetailPeriodTo">Zeitraum bis</label>
          <input id="sammelDetailPeriodTo" type="date" value="${escapeHtml((inv.period_to || "").substring(0, 10))}">
        </div>
        <div class="control">
          <label for="sammelDetailAmount">Rechnungsbetrag (EUR)</label>
          <input id="sammelDetailAmount" type="text" inputmode="decimal" value="${escapeHtml(centsToInputValue(invoiceCents))}">
        </div>
        <div class="control">
          <label>Aktueller Beleg</label>
          <div>${currentDocHtml}</div>
        </div>
        <div class="control">
          <label for="sammelDetailFile">Neuen Beleg hochladen</label>
          <input id="sammelDetailFile" type="file" accept=".pdf,.png,.jpg,.jpeg,.webp">
        </div>
        <div class="control" style="grid-column: 1 / -1;">
          <label for="sammelDetailNotes">Notiz</label>
          <textarea id="sammelDetailNotes" class="booking-input notes" rows="2">${escapeHtml(inv.notes || "")}</textarea>
        </div>
      </div>
      <div class="booking-detail-actions">
        <button class="btn-inline danger" data-action="delete-invoice-modal" type="button">Sammelrechnung loeschen</button>
        <button class="btn-inline primary" data-action="save-invoice-modal" type="button">Speichern</button>
      </div>
    </section>

    <section class="sammel-detail-transactions">
      <h3>Verknuepfte Transaktionen</h3>
      ${txTableHtml}
    </section>
  </div>`;
}

async function openMonthlyInvoiceDetail(invoiceId) {
  const id = String(invoiceId || "").trim();
  if (!id) return;

  try {
    setStatus("Sammelrechnung wird geladen...", "info");
    const payload = await fetchJson(`${API_BASE}/bookings/monthly-invoices/${encodeURIComponent(id)}`);
    const inv = payload?.invoice;
    if (!inv) throw new Error("Sammelrechnung konnte nicht geladen werden.");

    const providerLabel = SAMMELRECHNUNG_PROVIDERS[inv.provider] || inv.provider || "";
    state.detailsMode = "monthly-invoice";
    state.monthlyInvoiceDetailId = id;
    els.detailsTitle.textContent = `Sammelrechnung – ${providerLabel}`;
    els.detailsContent.innerHTML = renderMonthlyInvoiceDetailHtml(inv);
    els.detailsModal.classList.add("active");
    els.detailsModal.setAttribute("aria-hidden", "false");
    setStatus("", "ok");
  } catch (error) {
    setStatus(`Sammelrechnung konnte nicht geladen werden: ${error.message}`, "error");
  }
}

async function saveMonthlyInvoiceFromDetail(options) {
  const silent = options && options.silent === true;
  const invoiceId = String(state.monthlyInvoiceDetailId || "").trim();
  if (!invoiceId) {
    if (!silent) setStatus("Keine Sammelrechnung im Detailfenster aktiv.", "error");
    return;
  }

  const providerEl = document.getElementById("sammelDetailProvider");
  const periodFromEl = document.getElementById("sammelDetailPeriodFrom");
  const periodToEl = document.getElementById("sammelDetailPeriodTo");
  const amountEl = document.getElementById("sammelDetailAmount");
  const notesEl = document.getElementById("sammelDetailNotes");
  const fileEl = document.getElementById("sammelDetailFile");

  if (!providerEl || !amountEl) {
    /* Detail form not in DOM */
    return;
  }

  const provider = String(providerEl.value || "").trim().toLowerCase();
  const periodFrom = String(periodFromEl?.value || "").trim();
  const periodTo = String(periodToEl?.value || "").trim();
  const amountCents = parseEuroToCents(amountEl.value || "");
  const notes = String(notesEl?.value || "").trim() || null;

  if (!provider) {
    if (!silent) setStatus("Provider ist erforderlich.", "error");
    return;
  }
  if (!periodFrom || !periodTo) {
    if (!silent) setStatus("Zeitraum ist erforderlich.", "error");
    return;
  }
  if (!amountCents) {
    if (!silent) setStatus("Rechnungsbetrag muss groesser 0 sein.", "error");
    return;
  }

  const payload = {
    provider,
    period_from: periodFrom,
    period_to: periodTo,
    invoice_amount_cents: amountCents,
    notes,
  };

  try {
    /* Upload new file if selected */
    const file = fileEl?.files?.[0];
    if (file) {
      const form = new FormData();
      form.append("file", file);
      const uploadResult = await fetchJson(`${API_BASE}/bookings/documents/upload`, {
        method: "POST",
        body: form,
      });
      const docId = uploadResult?.document?.id;
      if (docId) {
        payload.document_id = docId;
      }
    }

    const result = await fetchJson(`${API_BASE}/bookings/monthly-invoices/${encodeURIComponent(invoiceId)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    await Promise.all([loadMonthlyInvoices(), loadBookingDocuments()]);
    renderMonthlyInvoices();
    renderBookingDocuments();

    if (!silent) {
      /* Re-fetch full detail (with transactions) to re-render */
      try {
        const detailPayload = await fetchJson(`${API_BASE}/bookings/monthly-invoices/${encodeURIComponent(invoiceId)}`);
        const updated = detailPayload?.invoice;
        if (updated && state.detailsMode === "monthly-invoice") {
          const providerLabel = SAMMELRECHNUNG_PROVIDERS[updated.provider] || updated.provider || "";
          state.monthlyInvoiceDetailId = String(updated.id || invoiceId);
          els.detailsTitle.textContent = `Sammelrechnung – ${providerLabel}`;
          els.detailsContent.innerHTML = renderMonthlyInvoiceDetailHtml(updated);
        }
      } catch (_) { /* best-effort re-render */ }
      setStatus("Sammelrechnung gespeichert.", "ok");
    }
  } catch (error) {
    if (!silent) setStatus(`Speichern fehlgeschlagen: ${error.message}`, "error");
  }
}

async function deleteMonthlyInvoiceFromDetail() {
  const invoiceId = String(state.monthlyInvoiceDetailId || "").trim();
  if (!invoiceId) return;

  const confirmed = window.confirm("Sammelrechnung wirklich loeschen?");
  if (!confirmed) return;

  try {
    await fetchJson(`${API_BASE}/bookings/monthly-invoices/${encodeURIComponent(invoiceId)}`, {
      method: "DELETE",
    });
    setStatus("Sammelrechnung geloescht.", "ok");
    closeDetailsModal();
    await loadMonthlyInvoices();
    renderMonthlyInvoices();
  } catch (error) {
    setStatus(`Loeschen fehlgeschlagen: ${error.message}`, "error");
  }
}

async function createMonthlyInvoice() {
  const provider = els.createSammelProvider?.value || "";
  const periodFrom = sammelMonthPeriodFrom();
  const periodTo = sammelMonthPeriodTo();
  const rawAmount = els.createSammelAmount?.value || "";
  const amountCents = parseEuroToCents(rawAmount);
  const notes = String(els.createSammelNotes?.value || "").trim() || null;

  if (!provider) {
    setStatus("Provider ist erforderlich.", "error");
    return;
  }
  if (!periodFrom || !periodTo) {
    setStatus("Monat ist erforderlich.", "error");
    return;
  }
  if (!amountCents) {
    setStatus(`Rechnungsbetrag muss groesser 0 sein (Eingabe: "${rawAmount}").`, "error");
    return;
  }

  const payload = {
    provider,
    period_from: periodFrom,
    period_to: periodTo,
    invoice_amount_cents: amountCents,
    currency: "EUR",
    notes,
  };

  try {
    const result = await fetchJson(`${API_BASE}/bookings/monthly-invoices`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const inv = result?.invoice;
    const invoiceId = inv?.id;

    /* Upload document if file was selected */
    const file = els.createSammelFile?.files?.[0];
    if (file && invoiceId) {
      try {
        const form = new FormData();
        form.append("file", file);
        const uploadResult = await fetchJson(`${API_BASE}/bookings/documents/upload`, {
          method: "POST",
          body: form,
        });
        const docId = uploadResult?.document?.id;
        if (docId) {
          await fetchJson(`${API_BASE}/bookings/monthly-invoices/${encodeURIComponent(invoiceId)}`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ document_id: docId }),
          });
        }
      } catch (docErr) {
        setStatus(`Sammelrechnung angelegt, aber Beleg-Upload fehlgeschlagen: ${docErr.message}`, "warn");
      }
    }

    /* Reset form fields */
    els.createSammelAmount.value = "";
    els.createSammelNotes.value = "";
    if (els.createSammelFile) els.createSammelFile.value = "";
    if (els.createSammelFileName instanceof HTMLElement) els.createSammelFileName.textContent = "Optional";

    if (inv) {
      const statusText = inv.status === "matched" ? "Matched" : `Differenz: ${centsToMoney(inv.difference_cents || 0)}`;
      setStatus(`Sammelrechnung angelegt. ${statusText}`, inv.status === "matched" ? "ok" : "warn");
    } else {
      setStatus("Sammelrechnung angelegt.", "ok");
    }

    await loadMonthlyInvoices();
    renderMonthlyInvoices();
    await loadBookingDocuments();
    renderBookingDocuments();
  } catch (error) {
    setStatus(`Sammelrechnung konnte nicht angelegt werden: ${error.message}`, "error");
  }
}

async function deleteMonthlyInvoice(invoiceId) {
  const id = String(invoiceId || "").trim();
  if (!id) return;

  const confirmed = window.confirm("Sammelrechnung wirklich loeschen?");
  if (!confirmed) return;

  try {
    await fetchJson(`${API_BASE}/bookings/monthly-invoices/${encodeURIComponent(id)}`, {
      method: "DELETE",
    });
    await loadMonthlyInvoices();
    renderMonthlyInvoices();
    setStatus("Sammelrechnung geloescht.", "ok");
  } catch (error) {
    setStatus(`Sammelrechnung konnte nicht geloescht werden: ${error.message}`, "error");
  }
}

function uploadSammelrechnungDocument(invoiceId) {
  const id = String(invoiceId || "").trim();
  if (!id) return;

  const fileInput = document.createElement("input");
  fileInput.type = "file";
  fileInput.accept = ".pdf,.png,.jpg,.jpeg,.webp";
  fileInput.style.display = "none";
  document.body.appendChild(fileInput);

  fileInput.addEventListener("change", async () => {
    const file = fileInput.files?.[0];
    document.body.removeChild(fileInput);
    if (!file) return;

    try {
      setStatus("Beleg wird hochgeladen...", "info");
      /* Step 1: Upload file */
      const form = new FormData();
      form.append("file", file);
      const uploadResult = await fetchJson(`${API_BASE}/bookings/documents/upload`, {
        method: "POST",
        body: form,
      });
      const docId = uploadResult?.document?.id;
      if (!docId) {
        setStatus("Upload fehlgeschlagen: Keine Dokument-ID erhalten.", "error");
        return;
      }

      /* Step 2: Link document to monthly invoice */
      await fetchJson(`${API_BASE}/bookings/monthly-invoices/${encodeURIComponent(id)}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ document_id: docId }),
      });

      await loadMonthlyInvoices();
      renderMonthlyInvoices();
      /* Also refresh documents list if visible */
      await loadBookingDocuments();
      renderBookingDocuments();
      setStatus("Beleg erfolgreich hochgeladen und verknuepft.", "ok");
    } catch (error) {
      setStatus(`Beleg-Upload fehlgeschlagen: ${error.message}`, "error");
    }
  });

  fileInput.click();
}

/* ── Sammelrechnung Month Picker Logic ── */

function sammelMonthPeriodFrom() {
  if (!_sammelSelectedMonth) return "";
  return _sammelSelectedMonth + "-01";
}

function sammelMonthPeriodTo() {
  if (!_sammelSelectedMonth) return "";
  const d = monthDateFromToken(_sammelSelectedMonth);
  if (!d) return "";
  /* Last day of the selected month */
  const last = new Date(d.getFullYear(), d.getMonth() + 1, 0);
  return dateTokenFromDate(last);
}

function sammelMonthLabel() {
  if (!_sammelSelectedMonth) return "—";
  const d = monthDateFromToken(_sammelSelectedMonth);
  if (!d) return "—";
  return `${SAMMEL_MONTH_FULL_DE[d.getMonth()]} ${d.getFullYear()}`;
}

function setSammelMonthMenuOpen(open) {
  const btn = els.sammelMonthBtn;
  const menu = els.sammelMonthMenu;
  if (!(btn instanceof HTMLElement) || !(menu instanceof HTMLElement)) return;
  if (open) {
    btn.setAttribute("aria-expanded", "true");
    menu.setAttribute("aria-hidden", "false");
    menu.classList.add("active");
    renderSammelMonthGrid();
  } else {
    btn.setAttribute("aria-expanded", "false");
    menu.setAttribute("aria-hidden", "true");
    menu.classList.remove("active");
  }
}

function renderSammelMonthGrid() {
  const grid = els.sammelMonthGrid;
  const yearLabel = els.sammelYearLabel;
  if (!(grid instanceof HTMLElement)) return;
  if (yearLabel instanceof HTMLElement) {
    yearLabel.textContent = String(_sammelPickerYear);
  }
  const selectedD = monthDateFromToken(_sammelSelectedMonth);
  const selectedYear = selectedD ? selectedD.getFullYear() : -1;
  const selectedMonthIdx = selectedD ? selectedD.getMonth() : -1;

  const now = new Date();
  const currentYear = now.getFullYear();
  const currentMonth = now.getMonth();

  let html = "";
  for (let m = 0; m < 12; m++) {
    const isSelected = (_sammelPickerYear === selectedYear && m === selectedMonthIdx);
    const isCurrent = (_sammelPickerYear === currentYear && m === currentMonth);
    const token = `${_sammelPickerYear}-${String(m + 1).padStart(2, "0")}`;
    let cls = "menu-item sammel-month-btn";
    if (isSelected) cls += " active";
    if (isCurrent) cls += " today";
    html += `<button class="${cls}" type="button" data-month="${token}">${SAMMEL_MONTH_NAMES_DE[m]}</button>`;
  }
  grid.innerHTML = html;
}

function selectSammelMonth(token) {
  _sammelSelectedMonth = token;
  const d = monthDateFromToken(token);
  if (d) {
    _sammelPickerYear = d.getFullYear();
  }
  /* Update trigger label */
  if (els.sammelMonthBtn instanceof HTMLElement) {
    els.sammelMonthBtn.textContent = sammelMonthLabel();
  }
  setSammelMonthMenuOpen(false);
  scheduleSammelPreview();
}

function initSammelrechnungDefaults() {
  /* Default month: previous month */
  if (!_sammelSelectedMonth) {
    const now = new Date();
    const prevMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
    _sammelSelectedMonth = monthTokenFromDate(prevMonth);
    _sammelPickerYear = prevMonth.getFullYear();
  }
  /* Update trigger button label */
  if (els.sammelMonthBtn instanceof HTMLElement) {
    els.sammelMonthBtn.textContent = sammelMonthLabel();
  }
}

/* ── Sammelrechnung fee preview ── */
let _sammelPreviewTimer = null;

function scheduleSammelPreview() {
  if (_sammelPreviewTimer) clearTimeout(_sammelPreviewTimer);
  _sammelPreviewTimer = setTimeout(fetchSammelPreview, 350);
}

async function fetchSammelPreview() {
  const provider = els.createSammelProvider?.value || "";
  const periodFrom = sammelMonthPeriodFrom();
  const periodTo = sammelMonthPeriodTo();
  const previewEl = els.sammelPreview;

  if (!previewEl) return;

  if (!provider || !periodFrom || !periodTo) {
    previewEl.style.display = "none";
    previewEl.innerHTML = "";
    return;
  }

  try {
    const params = new URLSearchParams({ provider, periodFrom, periodTo });
    const data = await fetchJson(`${API_BASE}/bookings/transactions/sum?${params}`);
    const sumEur = centsToMoney(data.total_cents || 0);
    const count = data.transaction_count || 0;
    const txs = Array.isArray(data.transactions) ? data.transactions : [];

    let html =
      `<div class="sammel-preview-header">Erwartete Gebuehren: <span class="sammel-preview-sum">${sumEur}</span>` +
      ` <span class="sammel-preview-count">(${NUMBER_FMT.format(count)} Transaktion${count === 1 ? "" : "en"})</span></div>`;

    if (txs.length > 0) {
      const rows = txs.map((tx) =>
        `<tr>
          <td>${escapeHtml(formatDate(tx.date))}</td>
          <td>${escapeHtml(centsToMoney(tx.amount_gross || 0))}</td>
          <td class="cell-ref">${escapeHtml(tx.reference || "-")}</td>
        </tr>`
      ).join("");

      html += `
        <div class="sammel-preview-table">
          <table>
            <thead><tr><th>Datum</th><th>Betrag</th><th>Referenz</th></tr></thead>
            <tbody>${rows}</tbody>
            <tfoot>
              <tr>
                <td><strong>Summe</strong></td>
                <td><strong>${escapeHtml(sumEur)}</strong></td>
                <td></td>
              </tr>
            </tfoot>
          </table>
        </div>`;
    }

    previewEl.innerHTML = html;
    previewEl.style.display = "";
  } catch {
    previewEl.style.display = "none";
    previewEl.innerHTML = "";
  }
}

function initSammelPreviewListeners() {
  if (els.createSammelProvider) {
    els.createSammelProvider.addEventListener("change", scheduleSammelPreview);
  }
  /* Month picker triggers preview via selectSammelMonth() */
  /* Also trigger on initial defaults if month is pre-filled */
  scheduleSammelPreview();
}

function rerender() {
  renderKpis();
  renderKpiTrends();
  renderAnalyticsInsights();
  renderTrendChart();
  renderDonutMarketplace();
  renderDonutRevenue();
  renderTopArticles();
  renderPurchaseHeatmap();
  renderGoogleAds();
  renderCustomers();
  renderCustomerGeo();
  renderCustomerGeoStatusInfo();
  renderOrders();
  refreshBookingFormOptions();
  renderBookings();
  renderBookingOrders();
  renderBookingTemplates();
  renderBookingAccounts();
  renderBookingDocuments();
  renderMonthlyInvoices();
  renderEbay();
}

function setBookingsSubtab(tab) {
  const allowed = new Set(["transactions", "orders", "templates", "accounts", "documents"]);
  state.bookingsSubtab = allowed.has(tab) ? tab : "transactions";

  const transactionsActive = state.bookingsSubtab === "transactions";
  const ordersActive = state.bookingsSubtab === "orders";
  const templatesActive = state.bookingsSubtab === "templates";
  const accountsActive = state.bookingsSubtab === "accounts";
  const documentsActive = state.bookingsSubtab === "documents";

  els.bookingsTransactionsBtn.classList.toggle("active", transactionsActive);
  els.bookingsOrdersBtn.classList.toggle("active", ordersActive);
  els.bookingsTemplatesBtn.classList.toggle("active", templatesActive);
  els.bookingsAccountsBtn.classList.toggle("active", accountsActive);
  els.bookingsDocumentsBtn.classList.toggle("active", documentsActive);

  els.bookingsTransactionsPanel.classList.toggle("active", transactionsActive);
  els.bookingsOrdersPanel.classList.toggle("active", ordersActive);
  els.bookingsTemplatesPanel.classList.toggle("active", templatesActive);
  els.bookingsAccountsPanel.classList.toggle("active", accountsActive);
  els.bookingsDocumentsPanel.classList.toggle("active", documentsActive);

  /* close any open tools panel when switching subtabs */
  document.querySelectorAll(".bookings-tools.open").forEach((p) => p.classList.remove("open"));

  /* update the unified "+" button */
  if (transactionsActive) {
    /* Delegate to booking-class-aware logic */
    updateBookingNewBtnForClass();
  } else {
    const btnCfg = {
      templates:    { target: "bookingsTemplateTools",     label: "Neues Template" },
      accounts:     { target: "bookingsAccountTools",      label: "Neues Konto" },
      documents:    { target: "bookingsDocumentTools",     label: "Beleg hochladen" },
    };
    const cfg = btnCfg[state.bookingsSubtab];
    if (els.bookingsNewBtn) {
      if (cfg) {
        els.bookingsNewBtn.style.display = "";
        els.bookingsNewBtn.setAttribute("data-target", cfg.target);
        els.bookingsNewBtn.setAttribute("aria-expanded", "false");
        const nodes = Array.from(els.bookingsNewBtn.childNodes);
        const textNode = nodes.filter((n) => n.nodeType === Node.TEXT_NODE).pop();
        if (textNode) {
          textNode.textContent = " " + cfg.label;
        }
      } else {
        /* orders subtab — hide the button */
        els.bookingsNewBtn.style.display = "none";
      }
    }
  }

  /* Show/hide booking class bar — only visible on transactions subtab */
  if (els.bookingClassBar) {
    els.bookingClassBar.style.display = transactionsActive ? "" : "none";
  }
}

/* ── Transaction Detail Functions ── */

function renderBookingTransactionDetailPreview(transaction) {
  if (!transaction || !transaction.document_id) {
    return `<section class="booking-detail-preview">
      <h3>Beleg Preview</h3>
      <div class="booking-detail-note">Kein Beleg mit dieser Transaktion verknuepft.</div>
    </section>`;
  }

  const documentRow = transaction.document && typeof transaction.document === "object" ? transaction.document : {};
  const fileName = documentRow.original_filename || documentRow.stored_filename || transaction.document_id;
  const mimeType = documentRow.mime_type || "";
  const downloadUrl = `${API_BASE}/bookings/documents/${encodeURIComponent(transaction.document_id)}/download`;
  const previewKind = detectPreviewKind(mimeType, fileName);

  let previewHtml = `<div class="booking-detail-note">Preview fuer diesen Dateityp nicht verfuegbar.</div>`;
  const inlineUrl = downloadUrl + "?disposition=inline";
  if (previewKind === "image") {
    previewHtml = `<img class="booking-detail-preview-image" src="${escapeHtml(inlineUrl)}" alt="${escapeHtml(fileName)}">`;
  } else if (previewKind === "pdf") {
    const pdfUrl = `${inlineUrl}#toolbar=1&view=FitH`;
    previewHtml = `<iframe class="booking-detail-preview-frame" src="${escapeHtml(pdfUrl)}" title="${escapeHtml(fileName)}"></iframe>`;
  }

  return `<section class="booking-detail-preview">
    <h3>Beleg Preview</h3>
    <div class="booking-detail-note">Datei: ${escapeHtml(fileName)} | MIME: ${escapeHtml(mimeType || "-")}</div>
    ${previewHtml}
    <div>${renderDocumentActions(downloadUrl, fileName, mimeType, true)}</div>
  </section>`;
}

function renderBookingTransactionDetailHtml(transaction) {
  const tx = transaction && typeof transaction === "object" ? transaction : {};
  const txId = String(tx.id || "");
  const typeCode = normalizeBookingTxType(tx.type);
  const categoryMeta = bookingTxCategoryMetaForType(typeCode);
  const order = tx.order && typeof tx.order === "object" ? tx.order : null;
  const orderLabel = order
    ? `${safeText(order.provider, "-")} | ${safeText(order.external_order_id, "-")}`
    : "-";

  const typeOptions = BOOKING_TX_TYPE_OPTIONS.map((value) => {
    const selected = typeCode === value ? " selected" : "";
    return `<option value="${value}"${selected}>${value}</option>`;
  }).join("");

  const directionValue = String(tx.direction || "").toUpperCase();
  const directionOptions = BOOKING_TX_DIRECTION_OPTIONS.map((value) => {
    const selected = directionValue === value ? " selected" : "";
    return `<option value="${value}"${selected}>${value}</option>`;
  }).join("");

  const statusValue = String(tx.status || "").toLowerCase();
  const statusOptions = [`<option value="" ${statusValue === "" ? "selected" : ""}>-</option>`]
    .concat(BOOKING_TX_STATUS_OPTIONS.map((value) => {
      const selected = statusValue === value ? " selected" : "";
      return `<option value="${value}"${selected}>${value}</option>`;
    }))
    .join("");

  const accountOptions = renderSelectOptions(
    state.bookingAccounts,
    tx.payment_account_id,
    (account) => bookingAccountLabel(account),
    "Ohne Konto",
  );
  const templateOptions = renderSelectOptions(
    state.bookingTemplates,
    tx.template_id,
    (template) => bookingTemplateLabel(template),
    "Ohne Template",
  );

  return `<div class="booking-detail-shell" data-booking-detail-id="${escapeHtml(txId)}">
    <section class="detail-grid">
      <article class="detail-card">
        <h3>Transaktionsdaten</h3>
        <div class="detail-kv">
          ${detailRows([
            ["ID", txId || "-"],
            ["Datum", formatDate(tx.date)],
            ["Betrag", centsToMoney(tx.amount_gross || 0)],
            ["Richtung", tx.direction || "-"],
            ["Status", tx.status || "-"],
            ["Waehrung", tx.currency || "EUR"],
          ])}
        </div>
      </article>
      <article class="detail-card">
        <h3>Klassifikation</h3>
        <div class="detail-kv">
          <div class="detail-row">
            <span>Typ</span>
            <strong><span class="badge ${escapeHtml(categoryMeta.badgeClass)}">${escapeHtml(typeCode || "-")}</span></strong>
          </div>
          ${detailRows([
            ["Gruppe", categoryMeta.longLabel],
            ["Provider", tx.provider || "-"],
            ["Gegenpartei", tx.counterparty_name || "-"],
            ["Kategorie", tx.category || "-"],
            ["Referenz", tx.reference || "-"],
          ])}
        </div>
      </article>
      <article class="detail-card">
        <h3>Verknuepfungen</h3>
        <div class="detail-kv">
          ${order
            ? `<div class="detail-row"><span>Order</span><strong><span class="detail-order-link" data-action="open-order" data-provider="${escapeHtml(order.provider || "")}" data-external-order-id="${escapeHtml(order.external_order_id || "")}" style="cursor:pointer;text-decoration:underline;color:var(--th-accent)">${escapeHtml(orderLabel)}</span></strong></div>`
            : detailRows([["Order", "-"]])}
          ${detailRows([
            ["Template", tx.template?.name || tx.template_id || "-"],
            ["Konto", tx.payment_account?.name || tx.payment_account_id || "-"],
            ["Beleg", tx.document?.original_filename || tx.document_id || "-"],
            ["Source", tx.source || "-"],
            ["Source Key", tx.source_key || "-"],
          ])}
        </div>
      </article>
    </section>

    <section class="booking-detail-form">
      <h3>Transaktion bearbeiten</h3>
      <div class="booking-detail-form-grid">
        <div class="control">
          <label for="bookingDetailTxDate">Datum</label>
          <input id="bookingDetailTxDate" type="datetime-local" value="${escapeHtml(toLocalInputFromIso(tx.date))}">
        </div>
        <div class="control">
          <label for="bookingDetailTxType">Typ</label>
          <select id="bookingDetailTxType">${typeOptions}</select>
        </div>
        <div class="control">
          <label for="bookingDetailTxDirection">Richtung</label>
          <select id="bookingDetailTxDirection">${directionOptions}</select>
        </div>
        <div class="control">
          <label for="bookingDetailTxAmount">Betrag (EUR)</label>
          <input id="bookingDetailTxAmount" type="number" step="0.01" min="0.01" value="${escapeHtml(centsToInputValue(tx.amount_gross || 0))}">
        </div>
        <div class="control">
          <label for="bookingDetailTxProvider">Provider</label>
          <input id="bookingDetailTxProvider" type="text" value="${escapeHtml(tx.provider || "")}">
        </div>
        <div class="control">
          <label for="bookingDetailTxStatus">Status</label>
          <select id="bookingDetailTxStatus">${statusOptions}</select>
        </div>
        <div class="control">
          <label for="bookingDetailTxReference">Referenz</label>
          <input id="bookingDetailTxReference" type="text" value="${escapeHtml(tx.reference || "")}">
        </div>
        <div class="control">
          <label for="bookingDetailTxCounterparty">Gegenpartei</label>
          <input id="bookingDetailTxCounterparty" type="text" value="${escapeHtml(tx.counterparty_name || "")}">
        </div>
        <div class="control">
          <label for="bookingDetailTxCategory">Kategorie</label>
          <input id="bookingDetailTxCategory" type="text" value="${escapeHtml(tx.category || "")}">
        </div>
        <div class="control">
          <label for="bookingDetailTxAccount">Konto</label>
          <select id="bookingDetailTxAccount">${accountOptions}</select>
        </div>
        <div class="control">
          <label for="bookingDetailTxTemplate">Template</label>
          <select id="bookingDetailTxTemplate">${templateOptions}</select>
        </div>
        <div class="control">
          <label for="bookingDetailTxDocument">Beleg hochladen</label>
          <input id="bookingDetailTxDocumentFile" type="file" accept=".pdf,.png,.jpg,.jpeg,.webp,.txt,.csv,.zip,.doc,.docx">
        </div>
        <div class="control" style="grid-column: 1 / -1;">
          <label for="bookingDetailTxNotes">Notiz</label>
          <textarea id="bookingDetailTxNotes" class="booking-input notes" rows="3">${escapeHtml(tx.notes || "")}</textarea>
        </div>
      </div>
      <div class="booking-detail-actions">
        <button class="btn-inline danger" data-action="delete-booking-modal" type="button">Transaktion loeschen</button>
      </div>
    </section>

    ${renderBookingTransactionDetailPreview(tx)}
  </div>`;
}

async function openBookingTransactionDetailsById(transactionId) {
  const txId = String(transactionId || "").trim();
  if (!txId) {
    return;
  }
  try {
    const payload = await fetchJson(`${API_BASE}/bookings/transactions/${encodeURIComponent(txId)}`);
    const transaction = payload && typeof payload.transaction === "object" ? payload.transaction : null;
    if (!transaction) {
      throw new Error("Transaktion konnte nicht geladen werden.");
    }

    state.detailsMode = "booking-transaction";
    state.bookingDetailsTransactionId = String(transaction.id || txId);
    els.detailsTitle.textContent = `Transaktion ${state.bookingDetailsTransactionId}`;
    els.detailsContent.innerHTML = renderBookingTransactionDetailHtml(transaction);
    els.detailsModal.classList.add("active");
    els.detailsModal.setAttribute("aria-hidden", "false");
  } catch (error) {
    setStatus(`Transaktionsdetails konnten nicht geladen werden: ${error.message}`, "error");
  }
}

async function saveBookingFromDetailsModal(options) {
  const silent = options && options.silent === true;
  const txId = String(state.bookingDetailsTransactionId || "").trim();
  if (!txId) {
    if (!silent) setStatus("Keine Transaktion im Detailfenster aktiv.", "error");
    return;
  }

  const dateEl = document.getElementById("bookingDetailTxDate");
  const amountEl = document.getElementById("bookingDetailTxAmount");
  const providerEl = document.getElementById("bookingDetailTxProvider");
  if (!dateEl || !amountEl || !providerEl) {
    /* Detail form not in DOM (already closed or not a booking detail) */
    return;
  }

  const dateIso = toIsoFromLocalInput(dateEl.value || "");
  const amountCents = parseEuroToCents(amountEl.value || "");
  const provider = String(providerEl.value || "").trim();
  if (!dateIso) {
    if (!silent) setStatus("Datum ist ungueltig.", "error");
    return;
  }
  if (!amountCents) {
    if (!silent) setStatus("Betrag muss groesser 0 sein.", "error");
    return;
  }
  if (!provider) {
    if (!silent) setStatus("Provider ist erforderlich.", "error");
    return;
  }

  const payload = {
    date: dateIso,
    type: normalizeBookingTxType(document.getElementById("bookingDetailTxType")?.value || ""),
    direction: String(document.getElementById("bookingDetailTxDirection")?.value || "").trim().toUpperCase(),
    amount_gross: amountCents,
    provider,
    status: String(document.getElementById("bookingDetailTxStatus")?.value || "").trim().toLowerCase() || null,
    reference: String(document.getElementById("bookingDetailTxReference")?.value || "").trim() || null,
    counterparty_name: String(document.getElementById("bookingDetailTxCounterparty")?.value || "").trim() || null,
    category: String(document.getElementById("bookingDetailTxCategory")?.value || "").trim() || null,
    payment_account_id: String(document.getElementById("bookingDetailTxAccount")?.value || "").trim() || null,
    template_id: String(document.getElementById("bookingDetailTxTemplate")?.value || "").trim() || null,
    notes: String(document.getElementById("bookingDetailTxNotes")?.value || "").trim() || null,
  };

  try {
    /* Upload file first if selected, then link to transaction */
    const fileInput = document.getElementById("bookingDetailTxDocumentFile");
    const file = fileInput?.files?.[0];
    if (file) {
      const form = new FormData();
      form.append("file", file);
      form.append("transaction_id", txId);
      await fetchJson(`${API_BASE}/bookings/documents/upload`, {
        method: "POST",
        body: form,
      });
    }

    const result = await fetchJson(`${API_BASE}/bookings/transactions/${encodeURIComponent(txId)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    await Promise.all([loadBookings(), loadBookingOrders(), loadBookingDocuments(), loadOrders(), loadAnalytics()]);
    rerender();

    if (!silent) {
      const updated = result && typeof result.transaction === "object" ? result.transaction : null;
      if (updated && state.detailsMode === "booking-transaction") {
        state.bookingDetailsTransactionId = String(updated.id || txId);
        els.detailsTitle.textContent = `Transaktion ${state.bookingDetailsTransactionId}`;
        els.detailsContent.innerHTML = renderBookingTransactionDetailHtml(updated);
      }
    }

    if (!silent) setStatus(`Transaktion gespeichert: ${txId}`, "ok");
  } catch (error) {
    if (!silent) setStatus(`Transaktion konnte nicht gespeichert werden: ${error.message}`, "error");
  }
}

async function deleteBookingFromDetailsModal() {
  const txId = String(state.bookingDetailsTransactionId || "").trim();
  if (!txId) {
    setStatus("Keine Transaktion im Detailfenster aktiv.", "error");
    return;
  }

  const confirmed = window.confirm("Transaktion wirklich loeschen? Diese Aktion kann nicht rueckgaengig gemacht werden.");
  if (!confirmed) {
    return;
  }

  try {
    await fetchJson(`${API_BASE}/bookings/transactions/${encodeURIComponent(txId)}`, {
      method: "DELETE",
    });
    closeDetailsModal();
    await Promise.all([loadBookings(), loadBookingOrders(), loadBookingDocuments(), loadOrders(), loadAnalytics()]);
    rerender();
    setStatus(`Transaktion geloescht: ${txId}`, "ok");
  } catch (error) {
    setStatus(`Transaktion konnte nicht geloescht werden: ${error.message}`, "error");
  }
}

/* ── CRUD Operations ── */

async function saveBooking(row) {
  const bookingId = row.dataset.bookingId;
  const statusInput = row.querySelector('[data-field="status"]');
  const referenceInput = row.querySelector('[data-field="reference"]');
  const notesInput = row.querySelector('[data-field="notes"]');
  const accountInput = row.querySelector('[data-field="payment_account_id"]');
  const templateInput = row.querySelector('[data-field="template_id"]');
  const documentInput = row.querySelector('[data-field="document_id"]');

  const payload = {
    status: statusInput ? statusInput.value : undefined,
    reference: referenceInput ? referenceInput.value : undefined,
    notes: notesInput ? notesInput.value : undefined,
    payment_account_id: accountInput ? (accountInput.value || null) : undefined,
    template_id: templateInput ? (templateInput.value || null) : undefined,
    document_id: documentInput ? (documentInput.value || null) : undefined,
  };

  try {
    await fetchJson(`${API_BASE}/bookings/transactions/${encodeURIComponent(bookingId)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    await Promise.all([loadBookings(), loadBookingDocuments()]);
    rerender();
    setStatus(`Buchung gespeichert: ${bookingId}`, "ok");
  } catch (error) {
    setStatus(`Buchung konnte nicht gespeichert werden: ${error.message}`, "error");
  }
}

async function createBookingTransaction() {
  const dateIso = toIsoFromLocalInput(els.createBookingDate.value);
  const amountCents = parseEuroToCents(els.createBookingAmount.value);
  const provider = String(els.createBookingProvider.value || "").trim();
  if (!dateIso) {
    setStatus("Datum ist ungueltig.", "error");
    return;
  }
  if (!amountCents) {
    setStatus("Betrag muss groesser 0 sein.", "error");
    return;
  }
  if (!provider) {
    setStatus("Provider ist erforderlich.", "error");
    return;
  }

  const payload = {
    date: dateIso,
    type: els.createBookingType.value,
    direction: els.createBookingDirection.value,
    amount_gross: amountCents,
    currency: "EUR",
    provider,
    status: els.createBookingStatus.value,
    reference: String(els.createBookingReference.value || "").trim() || null,
    notes: String(els.createBookingNotes.value || "").trim() || null,
    order_id: els.createBookingOrder.value || null,
    payment_account_id: els.createBookingAccount.value || null,
    template_id: els.createBookingTemplate.value || null,
    source: "manual",
    booking_class: "single",
  };

  try {
    await fetchJson(`${API_BASE}/bookings/transactions`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    els.createBookingAmount.value = "";
    els.createBookingReference.value = "";
    els.createBookingNotes.value = "";
    await Promise.all([loadBookings(), loadBookingOrders(), loadBookkeepingLedgerOrders(), loadBookingDocuments()]);
    rerender();
    setStatus("Transaktion angelegt.", "ok");
  } catch (error) {
    setStatus(`Transaktion konnte nicht angelegt werden: ${error.message}`, "error");
  }
}

async function createBookingTemplate() {
  const name = String(els.templateNameInput.value || "").trim();
  const provider = String(els.templateProviderInput.value || "").trim();
  const counterpartyName = String(els.templateCounterpartyInput.value || "").trim();
  const startDate = String(els.templateStartDateInput.value || "").trim();
  const amountCents = parseEuroToCents(els.templateAmountInput.value);
  if (!name || !provider) {
    setStatus("Template Name und Provider sind erforderlich.", "error");
    return;
  }
  if (!amountCents) {
    setStatus("Template-Betrag muss groesser 0 sein.", "error");
    return;
  }

  const dayRaw = String(els.templateDayInput.value || "").trim();
  const dayValue = dayRaw ? Number(dayRaw) : null;
  const payload = {
    name,
    type: els.templateTypeInput.value,
    direction: els.templateDirectionInput.value,
    default_amount_gross: amountCents,
    currency: "EUR",
    provider,
    counterparty_name: counterpartyName || null,
    schedule: els.templateScheduleInput.value,
    start_date: startDate || null,
    day_of_month: Number.isFinite(dayValue) ? Math.max(1, Math.min(31, Math.round(dayValue))) : null,
    payment_account_id: els.templateAccountInput.value || null,
    notes_default: String(els.templateNotesInput.value || "").trim() || null,
    active: true,
  };

  try {
    await fetchJson(`${API_BASE}/bookings/templates`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    els.templateNameInput.value = "";
    els.templateAmountInput.value = "";
    els.templateCounterpartyInput.value = "";
    els.templateStartDateInput.value = "";
    els.templateNotesInput.value = "";
    await Promise.all([loadBookingTemplates(), loadBookings()]);
    rerender();
    setStatus("Template angelegt.", "ok");
  } catch (error) {
    setStatus(`Template konnte nicht angelegt werden: ${error.message}`, "error");
  }
}

async function saveBookingTemplate(row) {
  const templateId = row.dataset.templateId;
  const nameInput = row.querySelector('[data-field="name"]');
  const counterpartyInput = row.querySelector('[data-field="counterparty_name"]');
  const startDateInput = row.querySelector('[data-field="start_date"]');
  const amountInput = row.querySelector('[data-field="default_amount_eur"]');
  const scheduleInput = row.querySelector('[data-field="schedule"]');
  const paymentAccountInput = row.querySelector('[data-field="payment_account_id"]');
  const activeInput = row.querySelector('[data-field="active"]');

  const name = String(nameInput?.value || "").trim();
  const amountCents = parseEuroToCents(amountInput?.value || "");
  if (!name || !amountCents) {
    setStatus("Template Name und Betrag sind erforderlich.", "error");
    return;
  }

  const payload = {
    name,
    counterparty_name: String(counterpartyInput?.value || "").trim() || null,
    start_date: String(startDateInput?.value || "").trim() || null,
    default_amount_gross: amountCents,
    schedule: scheduleInput ? scheduleInput.value : "monthly",
    payment_account_id: paymentAccountInput ? (paymentAccountInput.value || null) : null,
    active: activeInput ? activeInput.value === "true" : true,
  };

  try {
    await fetchJson(`${API_BASE}/bookings/templates/${encodeURIComponent(templateId)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    await Promise.all([loadBookingTemplates(), loadBookings()]);
    rerender();
    setStatus(`Template gespeichert: ${templateId}`, "ok");
  } catch (error) {
    setStatus(`Template konnte nicht gespeichert werden: ${error.message}`, "error");
  }
}

async function runBookingTemplate(row) {
  const templateId = row.dataset.templateId;
  const periodInput = row.querySelector('[data-field="period_key"]');
  const periodKey = periodInput ? (periodInput.value || currentPeriodKey()) : currentPeriodKey();

  try {
    const result = await runTemplateGeneration(templateId, periodKey);
    await Promise.all([loadBookings(), loadBookingTemplates()]);
    rerender();
    if (result.status === "duplicate") {
      setStatus(`Template ${templateId} (${periodKey}) existiert bereits.`, "info");
      return;
    }
    setStatus(`Template ausgefuehrt: ${templateId} (${periodKey})`, "ok");
  } catch (error) {
    setStatus(`Template-Run fehlgeschlagen: ${error.message}`, "error");
  }
}

async function runTemplateGeneration(templateId, periodKey) {
  const response = await fetch(`${API_BASE}/bookings/templates/${encodeURIComponent(templateId)}/generate-transaction`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ period_key: periodKey, status: "pending" }),
  });
  const payload = await response.json().catch(() => ({}));
  if (response.ok) {
    return { status: "created", payload };
  }

  let message = payload?.detail || payload?.error || `HTTP ${response.status}`;
  if (message && typeof message === "object") {
    message = message.message || JSON.stringify(message);
  }
  if (response.status === 409) {
    return { status: "duplicate", message };
  }
  throw new Error(message);
}

async function runBookingTemplateBackfill(row) {
  const templateId = row.dataset.templateId;
  const startDateInput = row.querySelector('[data-field="start_date"]');
  const startPeriodKey = periodKeyFromDateLike(startDateInput ? startDateInput.value : "");
  if (!startPeriodKey) {
    setStatus("Bitte zuerst ein Startdatum im Template setzen.", "error");
    return;
  }

  const endPeriodKey = currentPeriodKey();
  const periods = buildPeriodKeyRange(startPeriodKey, endPeriodKey);
  if (!periods.length) {
    setStatus("Ungueltiger Zeitraum fuer Backfill.", "error");
    return;
  }

  setStatus(`Template ${templateId}: Nachziehen von ${startPeriodKey} bis ${endPeriodKey}...`, "info");
  let createdCount = 0;
  let duplicateCount = 0;

  for (const periodKey of periods) {
    try {
      const result = await runTemplateGeneration(templateId, periodKey);
      if (result.status === "created") {
        createdCount += 1;
      } else if (result.status === "duplicate") {
        duplicateCount += 1;
      }
    } catch (error) {
      setStatus(`Backfill bei ${periodKey} fehlgeschlagen: ${error.message}`, "error");
      return;
    }
  }

  await Promise.all([loadBookings(), loadBookingTemplates()]);
  rerender();
  setStatus(
    `Template ${templateId}: ${NUMBER_FMT.format(createdCount)} neu, ${NUMBER_FMT.format(duplicateCount)} bereits vorhanden.`,
    "ok",
  );
}

async function createBookingAccount() {
  const name = String(els.accountNameInput.value || "").trim();
  if (!name) {
    setStatus("Kontoname ist erforderlich.", "error");
    return;
  }
  const payload = {
    name,
    provider: String(els.accountProviderInput.value || "").trim() || null,
    is_active: els.accountActiveInput.value === "true",
  };

  try {
    await fetchJson(`${API_BASE}/bookings/payment-accounts`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    els.accountNameInput.value = "";
    els.accountProviderInput.value = "";
    await Promise.all([loadBookingAccounts(), loadBookingTemplates(), loadBookings()]);
    rerender();
    setStatus("Konto angelegt.", "ok");
  } catch (error) {
    setStatus(`Konto konnte nicht angelegt werden: ${error.message}`, "error");
  }
}

async function saveBookingAccount(row) {
  const accountId = row.dataset.accountId;
  const nameInput = row.querySelector('[data-field="name"]');
  const providerInput = row.querySelector('[data-field="provider"]');
  const activeInput = row.querySelector('[data-field="is_active"]');

  const name = String(nameInput?.value || "").trim();
  if (!name) {
    setStatus("Kontoname ist erforderlich.", "error");
    return;
  }

  const payload = {
    name,
    provider: String(providerInput?.value || "").trim() || null,
    is_active: activeInput ? activeInput.value === "true" : true,
  };

  try {
    await fetchJson(`${API_BASE}/bookings/payment-accounts/${encodeURIComponent(accountId)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    await Promise.all([loadBookingAccounts(), loadBookingTemplates(), loadBookings()]);
    rerender();
    setStatus(`Konto gespeichert: ${accountId}`, "ok");
  } catch (error) {
    setStatus(`Konto konnte nicht gespeichert werden: ${error.message}`, "error");
  }
}

async function uploadBookingDocument() {
  const file = els.bookingDocumentFileInput.files && els.bookingDocumentFileInput.files[0];
  if (!file) {
    setStatus("Bitte zuerst eine Datei auswaehlen.", "error");
    return;
  }

  const form = new FormData();
  form.append("file", file);
  const txId = String(els.bookingDocumentTxInput.value || "").trim();
  const notes = String(els.bookingDocumentNotesInput.value || "").trim();
  if (txId) {
    form.append("transaction_id", txId);
  }
  if (notes) {
    form.append("notes", notes);
  }

  try {
    await fetchJson(`${API_BASE}/bookings/documents/upload`, {
      method: "POST",
      body: form,
    });
    els.bookingDocumentFileInput.value = "";
    els.bookingDocumentNotesInput.value = "";
    await Promise.all([loadBookingDocuments(), loadBookings()]);
    rerender();
    setStatus("Beleg hochgeladen.", "ok");
  } catch (error) {
    setStatus(`Beleg-Upload fehlgeschlagen: ${error.message}`, "error");
  }
}
