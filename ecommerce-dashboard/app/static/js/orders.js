"use strict";

/* ── Orders Tab ── */

/* ── Fee-source indicator ── */

function feeSourceLabel(source) {
  if (!source || source === "api") return "";
  if (source === "estimated_fx")
    return `<span class="fee-estimated" data-tooltip="Geschätzt inkl. Währungsumrechnung — kann ungenau sein">~ </span>`;
  return `<span class="fee-estimated" data-tooltip="Geschätzt — kein API-Wert vorhanden">~ </span>`;
}

function feeSourceText(source) {
  switch (source) {
    case "api": return "API (exakt)";
    case "stored_estimate": return "Gespeicherte Schätzung";
    case "estimated": return "Geschätzt";
    case "estimated_fx": return "Geschätzt (inkl. Währungsumrechnung)";
    case "none": return "Keine Gebühren";
    default: return source || "-";
  }
}

/* ── Detail Rendering Helpers ── */

function pickImageSrc(candidate) {
  if (!candidate) {
    return "";
  }
  if (typeof candidate === "string") {
    return sanitizeUrl(candidate);
  }
  if (typeof candidate === "object") {
    return sanitizeUrl(candidate.src || candidate.url || candidate.image || "");
  }
  return "";
}

function collectDetailImages(detail) {
  const urls = [];
  const seen = new Set();
  const add = (candidate) => {
    const normalized = pickImageSrc(candidate);
    if (!normalized || seen.has(normalized)) {
      return;
    }
    seen.add(normalized);
    urls.push(normalized);
  };

  const lineItems = Array.isArray(detail?.line_items) ? detail.line_items : [];
  lineItems.forEach((item) => {
    if (!item || typeof item !== "object") {
      return;
    }
    add(item.image);
    add(item.image_src);
    add(item.featured_image);
    add(item.product_image);
    add(item.product_image_url);
    const raw = parseJsonLoose(item.raw_json);
    if (raw && typeof raw === "object") {
      add(raw.image);
      add(raw.image_src);
      add(raw.featured_image);
      add(raw.product_image);
      add(raw.product_image_url);
    }
  });

  const orderRaw = detail?.order_raw && typeof detail.order_raw === "object" ? detail.order_raw : {};
  const rawLineItems = Array.isArray(orderRaw.line_items) ? orderRaw.line_items : [];
  rawLineItems.forEach((item) => {
    if (!item || typeof item !== "object") {
      return;
    }
    add(item.image);
    add(item.image_src);
    add(item.featured_image);
    add(item.product_image);
    add(item.product_image_url);
  });

  const units = Array.isArray(detail?.units) ? detail.units : [];
  units.forEach((unit) => {
    if (!unit || typeof unit !== "object") {
      return;
    }
    add(unit.product_main_picture);
    const rawUnit = unit.raw && typeof unit.raw === "object" ? unit.raw : {};
    const rawProduct = rawUnit.product && typeof rawUnit.product === "object" ? rawUnit.product : {};
    add(rawProduct.main_picture);
  });

  return urls.slice(0, 8);
}

function renderImageCard(detail) {
  const images = collectDetailImages(detail);
  if (!images.length) {
    return `<article class="detail-card">
      <h3>Produktbild</h3>
      <div class="detail-kv">
        <div class="detail-row">
          <span>Bild</span>
          <strong>Kein Bild vorhanden</strong>
        </div>
      </div>
    </article>`;
  }

  const mainImage = images[0];
  const thumbs = images.slice(1, 6);
  const thumbsHtml = thumbs.length
    ? `<div class="detail-image-thumbs">
        ${thumbs.map((url) => `<a href="${escapeHtml(url)}" target="_blank" rel="noreferrer"><img src="${escapeHtml(url)}" alt="Produktbild Vorschau"></a>`).join("")}
      </div>`
    : "";

  return `<article class="detail-card">
    <h3>Produktbild</h3>
    <a href="${escapeHtml(mainImage)}" target="_blank" rel="noreferrer">
      <img class="detail-image-main" src="${escapeHtml(mainImage)}" alt="Produktbild">
    </a>
    ${thumbsHtml}
  </article>`;
}

function renderSimpleTable(title, headers, rows) {
  const headHtml = headers.map((header) => `<th>${escapeHtml(header)}</th>`).join("");
  const bodyHtml = rows.length
    ? rows.map((row) => `<tr>${row.map((cell) => `<td>${escapeHtml(safeText(cell))}</td>`).join("")}</tr>`).join("")
    : `<tr><td colspan="${headers.length}">Keine Daten</td></tr>`;
  return `<section class="detail-table-wrap">
    <h3 style="font-family: 'Space Grotesk', sans-serif; font-size: 0.9rem; margin-bottom: 6px;">${escapeHtml(title)}</h3>
    <table>
      <thead><tr>${headHtml}</tr></thead>
      <tbody>${bodyHtml}</tbody>
    </table>
  </section>`;
}

function renderBookkeepingBreakdown(detail, summary) {
  const breakdown = detail && typeof detail.bookkeeping_breakdown === "object"
    ? detail.bookkeeping_breakdown
    : null;

  if (!breakdown) {
    return "";
  }

  const dbAvailable = Boolean(breakdown.db_available);
  if (!dbAvailable) {
    return `<section class="detail-card">
      <h3>Buchungsaufstellung</h3>
      <div class="detail-kv">
        <div class="detail-row">
          <span>Status</span>
          <strong>Buchungsdatenbank nicht verfuegbar</strong>
        </div>
      </div>
    </section>`;
  }

  const typeBreakdown = Array.isArray(breakdown.type_breakdown) ? breakdown.type_breakdown : [];
  const txRows = Array.isArray(breakdown.transactions) ? breakdown.transactions : [];
  const docRows = Array.isArray(breakdown.documents) ? breakdown.documents : [];

  const matching = String(breakdown.matched_via || "none");
  const matchingLabel = matching === "order_id"
    ? "Direkt ueber order_id"
    : (matching === "reference_fallback" ? "Fallback ueber Referenz/Notiz" : "Kein Match");

  const totalRevenue = Number(summary.total_cents || 0);
  const marketplaceFees = Number(summary.fees_cents || 0);
  const purchase = Number(summary.purchase_cost_cents || 0);
  const bookingExpenses = Number(
    breakdown.additional_expense_total_cents != null
      ? breakdown.additional_expense_total_cents
      : (breakdown.expense_total_cents || 0)
  );
  const mirroredFees = Number(breakdown.mirrored_fee_total_cents || 0);
  const mirroredCogs = Number(breakdown.mirrored_cogs_total_cents || 0);
  const additionalFee = Number(
    breakdown.additional_fee_cents != null
      ? breakdown.additional_fee_cents
      : (breakdown.fee_total_cents || 0)
  );
  const additionalCogs = Number(
    breakdown.additional_cogs_cents != null
      ? breakdown.additional_cogs_cents
      : (breakdown.cogs_total_cents || 0)
  );
  const additionalOther = Number(
    breakdown.additional_other_cents != null
      ? breakdown.additional_other_cents
      : (breakdown.other_expenses_cents || 0)
  );
  const finalCosts = marketplaceFees + purchase + bookingExpenses;
  const finalProfit = totalRevenue - finalCosts;

  const typeRowsHtml = typeBreakdown.length
    ? typeBreakdown.map((entry) => `
        <tr>
          <td>${escapeHtml(safeText(entry.type))}</td>
          <td>${escapeHtml(safeText(entry.direction))}</td>
          <td>${escapeHtml(NUMBER_FMT.format(Number(entry.count || 0)))}</td>
          <td>${escapeHtml(centsToMoney(entry.total_cents || 0))}</td>
        </tr>
      `).join("")
    : `<tr><td colspan="4">Keine Typ-Aufteilung vorhanden.</td></tr>`;

  const txRowsHtml = txRows.length
    ? txRows.map((tx) => {
        const docUrl = tx.document_id
          ? `${API_BASE}/bookings/documents/${encodeURIComponent(tx.document_id)}/download`
          : "";
        const docActions = docUrl
          ? renderDocumentActions(docUrl, tx.document_original_filename || tx.document_id, tx.document_mime_type, true)
          : "-";
        return `
          <tr>
            <td>${escapeHtml(formatDate(tx.date))}</td>
            <td>${escapeHtml(safeText(tx.type))}</td>
            <td>${escapeHtml(safeText(tx.direction))}</td>
            <td>${escapeHtml(centsToMoney(tx.amount_gross || 0))}</td>
            <td>${escapeHtml(safeText(tx.reference))}</td>
            <td>${docActions}</td>
          </tr>
        `;
      }).join("")
    : `<tr><td colspan="6">Keine verknuepften Buchungstransaktionen.</td></tr>`;

  const docsRowsHtml = docRows.length
    ? docRows.map((doc) => {
        const actions = renderDocumentActions(
          doc.download_url,
          doc.original_filename || doc.stored_filename || doc.document_id,
          doc.mime_type,
          Boolean(doc.previewable),
        );
        return `
          <tr>
            <td>${escapeHtml(safeText(doc.original_filename || doc.stored_filename || doc.document_id))}</td>
            <td>${escapeHtml(safeText(doc.mime_type || "-"))}</td>
            <td>${escapeHtml(String(doc.previewable ? "Ja" : "Nein"))}</td>
            <td>${actions}</td>
          </tr>
        `;
      }).join("")
    : `<tr><td colspan="4">Keine Belege vorhanden.</td></tr>`;

  return `
    <section class="detail-grid">
      <article class="detail-card">
        <h3>Kostenaufstellung (inkl. Buchungen)</h3>
        <div class="detail-kv">
          ${detailRows([
            ["Umsatz", centsToMoney(totalRevenue)],
            ["Marketplace Fees", centsToMoney(marketplaceFees)],
            ["Einkauf", centsToMoney(purchase)],
            ["Zusatz-Buchungen", centsToMoney(bookingExpenses)],
            ["Gesamtkosten", centsToMoney(finalCosts)],
            ["Ergebnis", centsToMoney(finalProfit)],
          ])}
        </div>
      </article>
      <article class="detail-card">
        <h3>Buchungs-Match</h3>
        <div class="detail-kv">
          ${detailRows([
            ["Match-Strategie", matchingLabel],
            ["Einnahmen (Buchungen)", centsToMoney(breakdown.income_total_cents || 0)],
            ["Zusatz-Ausgaben (Buchungen)", centsToMoney(bookingExpenses)],
            ["Auto-Fees (bereits oben)", centsToMoney(mirroredFees)],
            ["Auto-COGS (bereits oben)", centsToMoney(mirroredCogs)],
            ["davon Zusatz-Fees", centsToMoney(additionalFee)],
            ["davon Zusatz-COGS", centsToMoney(additionalCogs)],
            ["davon Zusatz-Sonstige", centsToMoney(additionalOther)],
          ])}
        </div>
      </article>
    </section>
    <section class="detail-table-wrap">
      <h3 style="font-family: 'Space Grotesk', sans-serif; font-size: 0.9rem; margin-bottom: 6px;">Buchungs-Typen</h3>
      <table>
        <thead>
          <tr>
            <th>Typ</th>
            <th>Richtung</th>
            <th>Anzahl</th>
            <th>Summe</th>
          </tr>
        </thead>
        <tbody>${typeRowsHtml}</tbody>
      </table>
    </section>
    <section class="detail-table-wrap">
      <h3 style="font-family: 'Space Grotesk', sans-serif; font-size: 0.9rem; margin-bottom: 6px;">Buchungs-Transaktionen</h3>
      <table>
        <thead>
          <tr>
            <th>Datum</th>
            <th>Typ</th>
            <th>Richtung</th>
            <th>Betrag</th>
            <th>Referenz</th>
            <th>Beleg</th>
          </tr>
        </thead>
        <tbody>${txRowsHtml}</tbody>
      </table>
    </section>
    <section class="detail-table-wrap">
      <h3 style="font-family: 'Space Grotesk', sans-serif; font-size: 0.9rem; margin-bottom: 6px;">Belege aus Buchungen</h3>
      <table>
        <thead>
          <tr>
            <th>Datei</th>
            <th>MIME</th>
            <th>Preview</th>
            <th>Aktionen</th>
          </tr>
        </thead>
        <tbody>${docsRowsHtml}</tbody>
      </table>
    </section>
  `;
}

function renderDetailHtml(detail) {
  const summary = detail && typeof detail.summary === "object" ? detail.summary : {};
  const detailOrder = detail && typeof detail.order === "object" ? detail.order : {};
  const breakdown = detail && typeof detail.bookkeeping_breakdown === "object"
    ? detail.bookkeeping_breakdown
    : {};
  const isShopify = Array.isArray(detail?.line_items);
  const lineItems = Array.isArray(detail?.line_items) ? detail.line_items : [];
  const units = Array.isArray(detail?.units) ? detail.units : [];
  const transactions = Array.isArray(detail?.transactions) ? detail.transactions : [];
  const fulfillments = Array.isArray(detail?.fulfillments) ? detail.fulfillments : [];
  const refunds = Array.isArray(detail?.refunds) ? detail.refunds : [];

  const orderCode = summary.external_order_id || detailOrder.name || summary.order_id || detailOrder.id || detailOrder.id_order || "-";
  const customer = summary.customer || detail?.customer?.name || detail?.customer?.email || detailOrder.customer_email || "-";
  const payment = summary.payment_method || detailOrder.payment_method || "-";
  const status = summary.fulfillment_status || detailOrder.fulfillment_status || detailOrder.financial_status || "-";
  const invoice = summary && typeof summary.invoice === "object" ? summary.invoice : null;
  const invoiceDownloadUrl = invoice && summary.marketplace && summary.order_id
    ? `${API_BASE}/orders/${encodeURIComponent(summary.marketplace)}/${encodeURIComponent(summary.order_id)}/invoice/${encodeURIComponent(invoice.document_id)}/download`
    : "";
  const invoiceActions = invoice
    ? renderDocumentActions(invoiceDownloadUrl, invoice.original_filename || invoice.stored_filename || "Rechnung", invoice.mime_type, true)
    : "-";

  const financeRows = [
    ["Total", centsToMoney(summary.total_cents || 0)],
    ["Fees", centsToMoney(summary.fees_cents || 0)],
    ["Gebühren-Quelle", feeSourceText(summary.fee_source)],
    ["After Fees", centsToMoney(summary.after_fees_cents || 0)],
    ["Einkauf", centsToMoney(summary.purchase_cost_cents || 0)],
    ["Gewinn", centsToMoney(summary.profit_cents || 0)],
  ];
  if (breakdown && breakdown.db_available) {
    const additionalExpenses = breakdown.additional_expense_total_cents != null
      ? breakdown.additional_expense_total_cents
      : (breakdown.expense_total_cents || 0);
    financeRows.push(["Zusatz-Buchungen", centsToMoney(additionalExpenses)]);
  }

  let specificTables = "";
  if (isShopify) {
    specificTables += renderSimpleTable(
      "Line Items",
      ["Titel", "Menge", "Preis", "Status", "SKU"],
      lineItems.map((item) => [item.title, item.quantity, item.price, item.fulfillment_status, item.sku])
    );
    specificTables += renderSimpleTable(
      "Payment Transaktionen",
      ["Kind", "Status", "Gateway", "Amount", "Fee", "Net", "Payment"],
      transactions.map((item) => [item.kind, item.status, item.gateway, item.amount, item.fee_amount, item.net_amount, item.payment_method])
    );
    specificTables += renderSimpleTable(
      "Fulfillments",
      ["Status", "Tracking", "Carrier", "Created"],
      fulfillments.map((item) => [item.status, item.tracking_number, item.tracking_company, item.created_at])
    );
    specificTables += renderSimpleTable(
      "Refunds",
      ["Created", "Note", "Restock", "User"],
      refunds.map((item) => [item.created_at, item.note, item.restock, item.user_id])
    );
  } else {
    specificTables += renderSimpleTable(
      "Order Units",
      ["Unit ID", "Produkt", "Status", "Price", "Revenue Gross", "VAT"],
      units.map((item) => [item.id_order_unit, item.product_title, item.status, item.price, item.revenue_gross, item.vat])
    );
  }

  return `
    <section class="detail-grid">
      <article class="detail-card">
        <h3>Order Summary</h3>
        <div class="detail-kv">
          ${detailRows([
            ["Marketplace", summary.marketplace || "-"],
            ["Order", orderCode],
            ["Datum", formatDate(summary.order_date)],
            ["Kunde", customer],
            ["Payment", payment],
            ["Status", status],
          ])}
        </div>
      </article>
      <article class="detail-card">
        <h3>Finanzen</h3>
        <div class="detail-kv">
          ${detailRows(financeRows)}
        </div>
      </article>
      <article class="detail-card">
        <h3>Kunde</h3>
        <div class="detail-kv">
          ${detailRows([
            ["Name", detail?.customer?.name || [detail?.customer?.first_name, detail?.customer?.last_name].filter(Boolean).join(" ")],
            ["Email", detail?.customer?.email || detailOrder.customer_email || detailOrder.email],
            ["Buyer ID", detail?.customer?.buyer_id || detail?.customer?.id || detail?.customer?.id_buyer],
            ["Waehrung", summary.currency || detailOrder.currency || "EUR"],
          ])}
        </div>
      </article>
    </section>
    <section class="detail-grid">
      ${renderAddressCard("Lieferadresse", detail?.shipping_address)}
      ${renderAddressCard("Rechnungsadresse", detail?.billing_address)}
      ${renderImageCard(detail)}
      <article class="detail-card">
        <h3>Beleg</h3>
        <div class="detail-kv">
          ${detailRows([
            ["Datei", summary?.invoice?.original_filename || "-"],
            ["Upload", summary?.invoice?.uploaded_at ? formatDate(summary.invoice.uploaded_at) : "-"],
          ])}
          <div class="detail-row">
            <span>Aktionen</span>
            <strong>${invoiceActions}</strong>
          </div>
        </div>
      </article>
    </section>
    ${renderBookkeepingBreakdown(detail, summary)}
    ${specificTables}
    <details class="raw-json">
      <summary>Rohdaten (JSON)</summary>
      <pre>${escapeHtml(JSON.stringify(detail, null, 2))}</pre>
    </details>
  `;
}

/* ── Load ── */

async function loadOrders() {
  const params = buildQuery();
  params.set("limit", "5000");
  const payload = await fetchJson(`${API_BASE}/orders?${params.toString()}`);
  state.orders = Array.isArray(payload.items) ? payload.items : [];
  state.ordersTotal = Number(payload.total || state.orders.length || 0);
}

/* ── Render ── */

function renderOrders() {
  const rows = state.orders.map((order) => {
    const invoice = order.invoice;
    const invoiceHtml = invoice
      ? `<a class="order-invoice-link" href="${API_BASE}/orders/${encodeURIComponent(order.marketplace)}/${encodeURIComponent(order.order_id)}/invoice/${encodeURIComponent(invoice.document_id)}/download?disposition=inline" target="_blank" rel="noreferrer" title="${escapeHtml(invoice.original_filename || "Download")}">${escapeHtml(invoice.original_filename || "Download")}</a>`
      : "-";

    const marketplaceToken = String(order.marketplace || "").trim().toLowerCase();
    const marketRowClass = marketplaceToken === "shopify"
      ? "order-row-shopify"
      : (marketplaceToken === "kaufland" ? "order-row-kaufland" : "");
    const marketBadgeClass = marketplaceToken === "shopify"
      ? "badge-invoice"
      : (marketplaceToken === "kaufland" ? "badge-sale" : "badge-default");
    const profitClass = Number(order.profit_cents || 0) < 0 ? "value-neg" : "value-pos";

    return `<tr class="${escapeHtml(marketRowClass)}" data-marketplace="${escapeHtml(order.marketplace)}" data-order-id="${escapeHtml(order.order_id)}" data-after-fees-cents="${escapeHtml(String(Number(order.after_fees_cents || 0)))}">
      <td data-label="Datum">${escapeHtml(formatDate(order.order_date))}</td>
      <td data-label="Channel"><span class="badge ${escapeHtml(marketBadgeClass)}">${escapeHtml(order.marketplace)}</span></td>
      <td data-label="Order">${escapeHtml(order.external_order_id || order.order_id)}</td>
      <td data-label="Kunde">${escapeHtml(order.customer || "-")}</td>
      <td data-label="Artikel" title="${escapeHtml(order.article || "-")}">${escapeHtml(order.article || "-")}${(order.line_items_count || 1) > 1 ? ` <span class="cell-sub">(+${order.line_items_count - 1} weitere)</span>` : ''}</td>
      <td data-label="Finanzen">
        <div><strong>${escapeHtml(centsToMoney(order.total_cents || 0))}</strong></div>
        <div class="cell-sub">After: ${escapeHtml(centsToMoney(order.after_fees_cents || 0))}</div>
        <div class="cell-sub">${feeSourceLabel(order.fee_source)}Fees: ${escapeHtml(centsToMoney(order.fees_cents || 0))}</div>
      </td>
      <td data-label="Einkauf">
        <input class="purchase-input" type="number" step="0.01" min="0" value="${escapeHtml(centsToInputValue(order.purchase_cost_cents || 0))}">
      </td>
      <td class="order-profit-cell ${profitClass}" data-label="Gewinn">${escapeHtml(centsToMoney(order.profit_cents || 0))}</td>
      <td data-label="Status">${escapeHtml(order.fulfillment_status || "-")}</td>
      <td data-label="Rechnung">
        <div>${invoiceHtml}</div>
        <div class="invoice-file-wrap" style="margin-top: 5px;">
          <label class="file-picker-label">
            Datei waehlen
            <input class="invoice-file-input" type="file" accept=".pdf,.png,.jpg,.jpeg,.webp,.txt">
          </label>
        </div>
      </td>
    </tr>`;
  }).join("");

  els.ordersBody.innerHTML = rows || "<tr><td colspan=\"10\">Keine Orders fuer aktuellen Filter.</td></tr>";
  els.ordersMeta.textContent = `${NUMBER_FMT.format(state.ordersTotal)} Zeilen`;
}

/* ── Actions (Save/Upload/Open) ── */

async function savePurchase(row) {
  const marketplace = row.dataset.marketplace;
  const orderId = row.dataset.orderId;
  const input = row.querySelector(".purchase-input");
  const parsed = parsePurchaseEur(input instanceof HTMLInputElement ? input.value : "");
  if (!parsed.ok) {
    setStatus(parsed.message || "Einkaufspreis ist ungueltig.", "error");
    return;
  }
  const purchaseCostEur = parsed.value;

  try {
    await fetchJson(`${API_BASE}/orders/${encodeURIComponent(marketplace)}/${encodeURIComponent(orderId)}/purchase`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ purchase_cost_eur: purchaseCostEur }),
    });
    const tasks = [loadOrders(), loadAnalytics(), loadBookingOrders(), loadBookings()];
    if (state.activeTab === "customers") {
      tasks.push(loadCustomers(), loadCustomerGeoLocations());
    } else {
      state.customersNeedsReload = true;
      state.customerGeoNeedsReload = true;
    }
    await Promise.all(tasks);
    rerender();
    setStatus(`Einkauf gespeichert: ${marketplace} ${orderId}`, "ok");
  } catch (error) {
    setStatus(`Speichern fehlgeschlagen: ${error.message}`, "error");
  }
}

async function uploadInvoice(row) {
  if (row.dataset.invoiceUploading === "1") {
    return;
  }
  const marketplace = row.dataset.marketplace;
  const orderId = row.dataset.orderId;
  const input = row.querySelector(".invoice-file-input");
  const purchaseInput = row.querySelector(".purchase-input");
  const file = input?.files?.[0];
  if (!file) {
    return;
  }

  const parsed = parsePurchaseEur(purchaseInput instanceof HTMLInputElement ? purchaseInput.value : "");
  if (!parsed.ok) {
    setStatus(parsed.message || "Einkaufspreis ist ungueltig.", "error");
    return;
  }
  const purchaseCostEur = parsed.value;

  const form = new FormData();
  form.append("file", file);
  if (purchaseCostEur !== null) {
    form.append("purchase_cost_eur", String(purchaseCostEur));
    form.append("purchase_currency", "EUR");
  }

  row.dataset.invoiceUploading = "1";
  if (input instanceof HTMLInputElement) {
    input.disabled = true;
  }

  try {
    if (purchaseCostEur !== null) {
      await fetchJson(`${API_BASE}/orders/${encodeURIComponent(marketplace)}/${encodeURIComponent(orderId)}/purchase`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ purchase_cost_eur: purchaseCostEur }),
      });
    }
    await fetchJson(`${API_BASE}/orders/${encodeURIComponent(marketplace)}/${encodeURIComponent(orderId)}/invoice`, {
      method: "POST",
      body: form,
    });
    if (input) {
      input.value = "";
    }
    const tasks = [loadOrders(), loadAnalytics(), loadBookingOrders(), loadBookings(), loadBookingDocuments()];
    if (state.activeTab === "customers") {
      tasks.push(loadCustomers(), loadCustomerGeoLocations());
    } else {
      state.customersNeedsReload = true;
      state.customerGeoNeedsReload = true;
    }
    await Promise.all(tasks);
    rerender();
    const priceHint = purchaseCostEur !== null ? " inkl. Einkaufspreis" : "";
    setStatus(`Rechnung hochgeladen${priceHint}: ${marketplace} ${orderId}`, "ok");
  } catch (error) {
    if (input instanceof HTMLInputElement) {
      input.value = "";
    }
    setStatus(`Upload fehlgeschlagen: ${error.message}`, "error");
  } finally {
    row.dataset.invoiceUploading = "0";
    if (input instanceof HTMLInputElement) {
      input.disabled = false;
    }
  }
}

async function openDetails(row) {
  const marketplace = row.dataset.marketplace;
  const orderId = row.dataset.orderId;
  await openOrderDetailById(marketplace, orderId);
}

async function openOrderDetailById(marketplace, orderId) {
  try {
    const detail = await fetchJson(`${API_BASE}/orders/${encodeURIComponent(marketplace)}/${encodeURIComponent(orderId)}`);
    state.detailsMode = "order";
    state.bookingDetailsTransactionId = null;
    els.detailsTitle.textContent = `Details ${marketplace.toUpperCase()} ${orderId}`;
    els.detailsContent.innerHTML = renderDetailHtml(detail);
    els.detailsModal.classList.add("active");
    els.detailsModal.setAttribute("aria-hidden", "false");
  } catch (error) {
    setStatus(`Details konnten nicht geladen werden: ${error.message}`, "error");
  }
}
