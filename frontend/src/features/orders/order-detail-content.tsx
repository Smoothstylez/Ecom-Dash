import { formatMoneyFromCents, NUMBER_FORMATTER } from "@/features/analytics/format";
import { buildDashboardApiUrl } from "@/shared/runtime/base-path";

import type { OrderDetail, OrderSummary } from "./api";

type Dictionary = Record<string, unknown>;

function asRecord(value: unknown): Dictionary {
  return value && typeof value === "object" ? value as Dictionary : {};
}

function asRecordArray(value: unknown): Dictionary[] {
  return Array.isArray(value)
    ? value.filter((entry): entry is Dictionary => Boolean(entry) && typeof entry === "object")
    : [];
}

function text(value: unknown, fallback = "-") {
  const normalized = String(value ?? "").trim();
  return normalized || fallback;
}

function optionalText(value: unknown) {
  const normalized = String(value ?? "").trim();
  return normalized || undefined;
}

function numeric(value: unknown) {
  return Number(value || 0);
}

function formatDateTime(value: unknown) {
  if (!value) {
    return "-";
  }

  const date = new Date(String(value));
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }

  return `${new Intl.DateTimeFormat("de-DE").format(date)} ${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
}

function feeSourceText(source: unknown) {
  switch (String(source || "").trim()) {
    case "api":
      return "API (exakt)";
    case "stored_estimate":
      return "Gespeicherte Schaetzung";
    case "estimated":
      return "Geschaetzt";
    case "estimated_fx":
      return "Geschaetzt (inkl. Waehrungsumrechnung)";
    case "none":
      return "Keine Gebuehren";
    default:
      return text(source);
  }
}

function inferMimeTypeFromFilename(filename: unknown) {
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
  return "";
}

function detectPreviewKind(mimeType: unknown, filename: unknown) {
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

function DocumentActions({
  url,
  filename,
  mimeType,
  previewable = true,
}: {
  url?: string;
  filename?: string;
  mimeType?: string;
  previewable?: boolean;
}) {
  const href = optionalText(url);
  if (!href) {
    return <span>-</span>;
  }

  const previewKind = previewable ? detectPreviewKind(mimeType, filename) : "";
  return (
    <span className="doc-actions">
      {previewKind ? (
        <button
          className="btn-inline ghost"
          data-action="preview-document"
          data-url={href}
          data-filename={String(filename || "Beleg")}
          data-mime={String(mimeType || "")}
          type="button"
        >
          Preview
        </button>
      ) : null}
      <a href={href} target="_blank" rel="noreferrer">Download</a>
    </span>
  );
}

function DetailRows({ items }: { items: Array<[string, unknown]> }) {
  return (
    <>
      {items.map(([label, value]) => (
        <div key={label} className="detail-row">
          <span>{label}</span>
          <strong>{text(value)}</strong>
        </div>
      ))}
    </>
  );
}

function AddressCard({ title, address }: { title: string; address: unknown }) {
  const addressRecord = asRecord(address);
  const name = [optionalText(addressRecord.first_name), optionalText(addressRecord.last_name)].filter(Boolean).join(" ");
  const street = [optionalText(addressRecord.street), optionalText(addressRecord.house_number)].filter(Boolean).join(" ")
    || [optionalText(addressRecord.address1), optionalText(addressRecord.address2)].filter(Boolean).join(", ");

  return (
    <article className="detail-card">
      <h3>{title}</h3>
      <div className="detail-kv">
        <DetailRows
          items={[
            ["Name", name || addressRecord.name || "-"],
            ["Firma", addressRecord.company || "-"],
            ["Strasse", street || "-"],
            ["PLZ", addressRecord.postcode || addressRecord.zip || "-"],
            ["Stadt", addressRecord.city || "-"],
            ["Land", addressRecord.country || addressRecord.country_code || "-"],
            ["Telefon", addressRecord.phone || "-"],
          ]}
        />
      </div>
    </article>
  );
}

function pickImageSrc(candidate: unknown) {
  if (!candidate) {
    return "";
  }
  if (typeof candidate === "string") {
    return candidate.trim();
  }
  const record = asRecord(candidate);
  return String(record.src || record.url || record.image || "").trim();
}

function parseJsonRecord(value: unknown) {
  if (!value) {
    return {};
  }
  if (typeof value === "string") {
    try {
      return asRecord(JSON.parse(value));
    } catch (_error) {
      return {};
    }
  }
  return asRecord(value);
}

function collectDetailImages(detail: OrderDetail) {
  const urls: string[] = [];
  const seen = new Set<string>();

  const add = (candidate: unknown) => {
    const normalized = pickImageSrc(candidate);
    if (!normalized || seen.has(normalized)) {
      return;
    }
    seen.add(normalized);
    urls.push(normalized);
  };

  for (const item of asRecordArray(detail.line_items)) {
    add(item.image);
    add(item.image_src);
    add(item.featured_image);
    add(item.product_image);
    add(item.product_image_url);

    const raw = parseJsonRecord(item.raw_json);
    add(raw.image);
    add(raw.image_src);
    add(raw.featured_image);
    add(raw.product_image);
    add(raw.product_image_url);
  }

  const orderRaw = asRecord(detail.order_raw);
  for (const item of asRecordArray(orderRaw.line_items)) {
    add(item.image);
    add(item.image_src);
    add(item.featured_image);
    add(item.product_image);
    add(item.product_image_url);
  }

  for (const unit of asRecordArray(detail.units)) {
    add(unit.product_main_picture);
    const raw = asRecord(unit.raw);
    const product = asRecord(raw.product);
    add(product.main_picture);
  }

  return urls.slice(0, 8);
}

function ImageCard({ detail }: { detail: OrderDetail }) {
  const images = collectDetailImages(detail);
  if (!images.length) {
    return (
      <article className="detail-card">
        <h3>Produktbild</h3>
        <div className="detail-kv">
          <div className="detail-row">
            <span>Bild</span>
            <strong>Kein Bild vorhanden</strong>
          </div>
        </div>
      </article>
    );
  }

  const [mainImage, ...thumbs] = images;
  return (
    <article className="detail-card">
      <h3>Produktbild</h3>
      <a href={mainImage} target="_blank" rel="noreferrer">
        <img className="detail-image-main" src={mainImage} alt="Produktbild" />
      </a>
      {thumbs.length ? (
        <div className="detail-image-thumbs">
          {thumbs.slice(0, 5).map((url) => (
            <a key={url} href={url} target="_blank" rel="noreferrer">
              <img src={url} alt="Produktbild Vorschau" />
            </a>
          ))}
        </div>
      ) : null}
    </article>
  );
}

function SimpleTable({
  title,
  headers,
  rows,
}: {
  title: string;
  headers: string[];
  rows: Array<{ key: string; cells: unknown[]; dataTxId?: string }>;
}) {
  return (
    <section className="detail-table-wrap">
      <h3 style={{ fontFamily: "'Space Grotesk', sans-serif", fontSize: "0.9rem", marginBottom: 6 }}>{title}</h3>
      <table>
        <thead>
          <tr>
            {headers.map((header) => <th key={header}>{header}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.length ? rows.map((row) => (
            <tr key={row.key} data-tx-id={row.dataTxId} style={row.dataTxId ? { cursor: "pointer" } : undefined}>
              {row.cells.map((cell, index) => <td key={`${row.key}:${headers[index]}`}>{text(cell)}</td>)}
            </tr>
          )) : (
            <tr>
              <td colSpan={headers.length}>Keine Daten</td>
            </tr>
          )}
        </tbody>
      </table>
    </section>
  );
}

function buildInvoicePreviewUrl(summary: OrderSummary) {
  const marketplace = optionalText(summary.marketplace);
  const orderId = optionalText(summary.order_id);
  const invoice = asRecord(summary.invoice);
  const documentId = optionalText(invoice.document_id);
  if (!marketplace || !orderId || !documentId) {
    return "";
  }
  return buildDashboardApiUrl(`/api/orders/${encodeURIComponent(marketplace)}/${encodeURIComponent(orderId)}/invoice/${encodeURIComponent(documentId)}/download?disposition=inline`);
}

function BookkeepingBreakdown({ detail, summary }: { detail: OrderDetail; summary: OrderSummary }) {
  const breakdown = asRecord(detail.bookkeeping_breakdown);
  if (!Object.keys(breakdown).length) {
    return null;
  }

  if (!breakdown.db_available) {
    return (
      <section className="detail-card">
        <h3>Buchungsaufstellung</h3>
        <div className="detail-kv">
          <div className="detail-row">
            <span>Status</span>
            <strong>Buchungsdatenbank nicht verfuegbar</strong>
          </div>
        </div>
      </section>
    );
  }

  const typeBreakdown = asRecordArray(breakdown.type_breakdown);
  const transactions = asRecordArray(breakdown.transactions);
  const documents = asRecordArray(breakdown.documents);
  const matchedVia = String(breakdown.matched_via || "none");
  const matchedLabel = matchedVia === "order_id"
    ? "Direkt ueber order_id"
    : matchedVia === "reference_fallback"
      ? "Fallback ueber Referenz/Notiz"
      : "Kein Match";

  const totalRevenue = numeric(summary.total_cents);
  const marketplaceFees = numeric(summary.fees_cents);
  const purchase = numeric(summary.purchase_cost_cents);
  const bookingExpenses = numeric(
    breakdown.additional_expense_total_cents != null
      ? breakdown.additional_expense_total_cents
      : breakdown.expense_total_cents,
  );
  const mirroredFees = numeric(breakdown.mirrored_fee_total_cents);
  const mirroredCogs = numeric(breakdown.mirrored_cogs_total_cents);
  const additionalFee = numeric(
    breakdown.additional_fee_cents != null
      ? breakdown.additional_fee_cents
      : breakdown.fee_total_cents,
  );
  const additionalCogs = numeric(
    breakdown.additional_cogs_cents != null
      ? breakdown.additional_cogs_cents
      : breakdown.cogs_total_cents,
  );
  const additionalOther = numeric(
    breakdown.additional_other_cents != null
      ? breakdown.additional_other_cents
      : breakdown.other_expenses_cents,
  );
  const finalCosts = marketplaceFees + purchase + bookingExpenses;
  const finalProfit = totalRevenue - finalCosts;

  return (
    <>
      <section className="detail-grid">
        <article className="detail-card">
          <h3>Kostenaufstellung (inkl. Buchungen)</h3>
          <div className="detail-kv">
            <DetailRows
              items={[
                ["Umsatz", formatMoneyFromCents(totalRevenue)],
                ["Marketplace Fees", formatMoneyFromCents(marketplaceFees)],
                ["Einkauf", formatMoneyFromCents(purchase)],
                ["Zusatz-Buchungen", formatMoneyFromCents(bookingExpenses)],
                ["Gesamtkosten", formatMoneyFromCents(finalCosts)],
                ["Ergebnis", formatMoneyFromCents(finalProfit)],
              ]}
            />
          </div>
        </article>
        <article className="detail-card">
          <h3>Buchungs-Match</h3>
          <div className="detail-kv">
            <DetailRows
              items={[
                ["Match-Strategie", matchedLabel],
                ["Einnahmen (Buchungen)", formatMoneyFromCents(numeric(breakdown.income_total_cents))],
                ["Zusatz-Ausgaben (Buchungen)", formatMoneyFromCents(bookingExpenses)],
                ["Auto-Fees (bereits oben)", formatMoneyFromCents(mirroredFees)],
                ["Auto-COGS (bereits oben)", formatMoneyFromCents(mirroredCogs)],
                ["davon Zusatz-Fees", formatMoneyFromCents(additionalFee)],
                ["davon Zusatz-COGS", formatMoneyFromCents(additionalCogs)],
                ["davon Zusatz-Sonstige", formatMoneyFromCents(additionalOther)],
              ]}
            />
          </div>
        </article>
      </section>
      <SimpleTable
        title="Buchungs-Typen"
        headers={["Typ", "Richtung", "Anzahl", "Summe"]}
        rows={typeBreakdown.map((entry, index) => ({
          key: `${text(entry.type)}:${index}`,
          cells: [
            entry.type,
            entry.direction,
            NUMBER_FORMATTER.format(numeric(entry.count)),
            formatMoneyFromCents(numeric(entry.total_cents)),
          ],
        }))}
      />
      <section className="detail-table-wrap">
        <h3 style={{ fontFamily: "'Space Grotesk', sans-serif", fontSize: "0.9rem", marginBottom: 6 }}>Buchungs-Transaktionen</h3>
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
          <tbody>
            {transactions.length ? transactions.map((entry, index) => {
              const documentUrl = optionalText(entry.document_id)
                ? buildDashboardApiUrl(`/api/bookings/documents/${encodeURIComponent(String(entry.document_id))}/download`)
                : "";
              return (
                <tr key={`${text(entry.id, `tx-${index}`)}:${index}`} data-tx-id={optionalText(entry.id)} style={{ cursor: entry.id ? "pointer" : undefined }}>
                  <td>{formatDateTime(entry.date)}</td>
                  <td>{text(entry.type)}</td>
                  <td>{text(entry.direction)}</td>
                  <td>{formatMoneyFromCents(numeric(entry.amount_gross))}</td>
                  <td>{text(entry.reference)}</td>
                  <td>
                    <DocumentActions
                      url={documentUrl}
                      filename={text(entry.document_original_filename, text(entry.document_id))}
                      mimeType={text(entry.document_mime_type, "")}
                    />
                  </td>
                </tr>
              );
            }) : (
              <tr>
                <td colSpan={6}>Keine verknuepften Buchungstransaktionen.</td>
              </tr>
            )}
          </tbody>
        </table>
      </section>
      <section className="detail-table-wrap">
        <h3 style={{ fontFamily: "'Space Grotesk', sans-serif", fontSize: "0.9rem", marginBottom: 6 }}>Belege aus Buchungen</h3>
        <table>
          <thead>
            <tr>
              <th>Datei</th>
              <th>MIME</th>
              <th>Preview</th>
              <th>Aktionen</th>
            </tr>
          </thead>
          <tbody>
            {documents.length ? documents.map((entry, index) => (
              <tr key={`${text(entry.document_id, `doc-${index}`)}:${index}`}>
                <td>{text(entry.original_filename || entry.stored_filename || entry.document_id)}</td>
                <td>{text(entry.mime_type)}</td>
                <td>{entry.previewable ? "Ja" : "Nein"}</td>
                <td>
                  <DocumentActions
                    url={optionalText(entry.download_url)}
                    filename={text(entry.original_filename || entry.stored_filename || entry.document_id)}
                    mimeType={text(entry.mime_type, "")}
                    previewable={Boolean(entry.previewable)}
                  />
                </td>
              </tr>
            )) : (
              <tr>
                <td colSpan={4}>Keine Belege vorhanden.</td>
              </tr>
            )}
          </tbody>
        </table>
      </section>
    </>
  );
}

export function OrderDetailContent({
  detail,
  loading,
  error,
}: {
  detail: OrderDetail | null;
  loading: boolean;
  error: string;
}) {
  if (loading) {
    return <div>Lade Details...</div>;
  }

  if (error) {
    return (
      <div className="table-meta" style={{ color: "var(--danger, #c44)" }}>
        Details konnten nicht geladen werden: {error}
      </div>
    );
  }

  if (!detail) {
    return <div>-</div>;
  }

  const summary = detail.summary || {};
  const detailOrder = asRecord(detail.order);
  const customer = asRecord(detail.customer);
  const breakdown = asRecord(detail.bookkeeping_breakdown);
  const lineItems = asRecordArray(detail.line_items);
  const transactions = asRecordArray(detail.transactions);
  const fulfillments = asRecordArray(detail.fulfillments);
  const refunds = asRecordArray(detail.refunds);
  const units = asRecordArray(detail.units);
  const isShopify = lineItems.length > 0;

  const orderCode = text(
    summary.external_order_id
      || detailOrder.name
      || summary.order_id
      || detailOrder.id
      || detailOrder.id_order,
  );
  const customerName = text(
    summary.customer
      || customer.name
      || [optionalText(customer.first_name), optionalText(customer.last_name)].filter(Boolean).join(" ")
      || customer.email
      || detailOrder.customer_email,
  );
  const paymentMethod = text(summary.payment_method || detailOrder.payment_method);
  const status = text(summary.fulfillment_status || detailOrder.fulfillment_status || detailOrder.financial_status);
  const invoice = asRecord(summary.invoice);
  const invoiceUrl = buildInvoicePreviewUrl(summary);
  const purchaseSupplier = text(summary.purchase_supplier);
  const purchaseNotes = text(summary.purchase_notes);
  const financeRows: Array<[string, unknown]> = [
    ["Total", formatMoneyFromCents(numeric(summary.total_cents))],
    ["Fees", formatMoneyFromCents(numeric(summary.fees_cents))],
    ["Gebuehren-Quelle", feeSourceText(summary.fee_source)],
    ["After Fees", formatMoneyFromCents(numeric(summary.after_fees_cents))],
    ["Einkauf", formatMoneyFromCents(numeric(summary.purchase_cost_cents))],
    ["Gewinn", formatMoneyFromCents(numeric(summary.profit_cents))],
  ];

  if (breakdown.db_available) {
    financeRows.push([
      "Zusatz-Buchungen",
      formatMoneyFromCents(numeric(
        breakdown.additional_expense_total_cents != null
          ? breakdown.additional_expense_total_cents
          : breakdown.expense_total_cents,
      )),
    ]);
  }

  return (
    <>
      <section className="detail-grid">
        <article className="detail-card">
          <h3>Order Summary</h3>
          <div className="detail-kv">
            <DetailRows
              items={[
                ["Marketplace", summary.marketplace || "-"],
                ["Order", orderCode],
                ["Datum", formatDateTime(summary.order_date)],
                ["Kunde", customerName],
                ["Payment", paymentMethod],
                ["Status", status],
              ]}
            />
          </div>
        </article>
        <article className="detail-card">
          <h3>Finanzen</h3>
          <div className="detail-kv">
            <DetailRows items={financeRows} />
          </div>
        </article>
        <article className="detail-card">
          <h3>Beschaffung</h3>
          <div className="detail-kv">
            <DetailRows
              items={[
                ["Lieferant", purchaseSupplier],
                ["Notiz", purchaseNotes],
              ]}
            />
          </div>
        </article>
        <article className="detail-card">
          <h3>Kunde</h3>
          <div className="detail-kv">
            <DetailRows
              items={[
                ["Name", customer.name || [optionalText(customer.first_name), optionalText(customer.last_name)].filter(Boolean).join(" ") || "-"],
                ["Email", customer.email || detailOrder.customer_email || detailOrder.email || "-"],
                ["Buyer ID", customer.buyer_id || customer.id || customer.id_buyer || "-"],
                ["Waehrung", summary.currency || detailOrder.currency || "EUR"],
              ]}
            />
          </div>
        </article>
      </section>

      <section className="detail-grid">
        <AddressCard title="Lieferadresse" address={detail.shipping_address} />
        <AddressCard title="Rechnungsadresse" address={detail.billing_address} />
        <ImageCard detail={detail} />
        <article className="detail-card">
          <h3>Beleg</h3>
          <div className="detail-kv">
            <DetailRows
              items={[
                ["Datei", invoice.original_filename || "-"],
                ["Upload", invoice.uploaded_at ? formatDateTime(invoice.uploaded_at) : "-"],
              ]}
            />
            <div className="detail-row">
              <span>Aktionen</span>
              <strong>
                <DocumentActions
                  url={invoiceUrl}
                  filename={text(invoice.original_filename, "Rechnung")}
                  mimeType={text(invoice.mime_type, "")}
                />
              </strong>
            </div>
          </div>
        </article>
      </section>

      <BookkeepingBreakdown detail={detail} summary={summary} />

      {isShopify ? (
        <>
          <SimpleTable
            title="Line Items"
            headers={["Titel", "Menge", "Preis", "Status", "SKU"]}
            rows={lineItems.map((item, index) => ({
              key: `${text(item.title, `line-${index}`)}:${index}`,
              cells: [item.title, item.quantity, item.price, item.fulfillment_status, item.sku],
            }))}
          />
          <SimpleTable
            title="Payment Transaktionen"
            headers={["Kind", "Status", "Gateway", "Amount", "Fee", "Net", "Payment"]}
            rows={transactions.map((item, index) => ({
              key: `${text(item.id, `transaction-${index}`)}:${index}`,
              cells: [item.kind, item.status, item.gateway, item.amount, item.fee_amount, item.net_amount, item.payment_method],
            }))}
          />
          <SimpleTable
            title="Fulfillments"
            headers={["Status", "Tracking", "Carrier", "Created"]}
            rows={fulfillments.map((item, index) => ({
              key: `${text(item.id, `fulfillment-${index}`)}:${index}`,
              cells: [item.status, item.tracking_number, item.tracking_company, item.created_at],
            }))}
          />
          <SimpleTable
            title="Refunds"
            headers={["Created", "Note", "Restock", "User"]}
            rows={refunds.map((item, index) => ({
              key: `${text(item.id, `refund-${index}`)}:${index}`,
              cells: [item.created_at, item.note, item.restock, item.user_id],
            }))}
          />
        </>
      ) : (
        <SimpleTable
          title="Order Units"
          headers={["Unit ID", "Produkt", "Status", "Price", "Revenue Gross", "VAT"]}
          rows={units.map((item, index) => ({
            key: `${text(item.id_order_unit, `unit-${index}`)}:${index}`,
            cells: [item.id_order_unit, item.product_title, item.status, item.price, item.revenue_gross, item.vat],
          }))}
        />
      )}

      <details className="raw-json">
        <summary>Rohdaten (JSON)</summary>
        <pre>{JSON.stringify(detail, null, 2)}</pre>
      </details>
    </>
  );
}
