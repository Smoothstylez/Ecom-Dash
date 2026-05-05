import { useDashboardShellState, type BookingsSubtab } from "@/app/dashboard-shell-state";
import { useDashboardRuntime } from "@/app/dashboard-runtime";
import { formatMoneyFromCents, NUMBER_FORMATTER } from "@/features/analytics/format";
import {
  createBookingAccount,
  createBookingTemplate,
  createBookingTransaction,
  createMonthlyInvoice as createMonthlyInvoiceMutation,
  deleteMonthlyInvoice as deleteMonthlyInvoiceMutation,
  fetchBookingAccounts,
  fetchBookingDocuments,
  fetchBookingLedgerOrders,
  fetchBookingOrders,
  fetchBookingTransactionsSum,
  fetchBookingsTransactions,
  fetchBookingTemplates,
  fetchMonthlyInvoices,
  runBookingTemplate as runBookingTemplateMutation,
  type BookingAccountRow,
  type BookingTransactionsSumResponse,
  type BookingDocumentRow,
  type BookingOrderRow,
  type BookingRow,
  type BookingTemplateRow,
  type MonthlyInvoiceRow,
  type OptionItem,
  updateBookingAccount,
  updateMonthlyInvoice,
  updateBookingTemplate,
  updateBookingTransaction,
  uploadBookingDocument,
} from "@/features/bookings/api";
import { createPortal } from "react-dom";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

type BookingsPageProps = {
  panelElement: HTMLElement;
};

type BookingsData = {
  bookings: BookingRow[];
  bookingsTotal: number;
  bookingsAllItems: BookingRow[];
  bookingOrders: BookingOrderRow[];
  bookingOrdersTotal: number;
  bookingTemplates: BookingTemplateRow[];
  bookingTemplatesTotal: number;
  bookingAccounts: BookingAccountRow[];
  bookingAccountsTotal: number;
  bookingDocuments: BookingDocumentRow[];
  bookingDocumentsTotal: number;
  monthlyInvoices: MonthlyInvoiceRow[];
  monthlyInvoicesTotal: number;
  bookkeepingLedgerOrders: OptionItem[];
  bookkeepingLedgerOrdersTotal: number;
};

type StatusLevel = "info" | "ok" | "error";

const EMPTY_BOOKINGS_DATA: BookingsData = {
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
};

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
} as const;

const BOOKING_TX_TYPE_TO_CATEGORY: Record<string, keyof typeof BOOKING_TX_CATEGORY_META> = {
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

const SAMMELRECHNUNG_PROVIDERS: Record<string, string> = {
  paypal: "PayPal Fees",
  shopify_payments: "Shopify Payments Fees",
  kaufland: "Kaufland Fees",
  google_ads: "Google Ads",
  ebay: "eBay Fees",
};

type MonthlyInvoiceDraftState = {
  provider: string;
  monthToken: string;
  amount: string;
  notes: string;
  fileName: string;
};

type BookingsToolPanelId = "" | "bookingsTransactionTools" | "sammelrechnungTools" | "bookingsTemplateTools" | "bookingsAccountTools" | "bookingsDocumentTools";

type MonthlyInvoicePreviewState = {
  visible: boolean;
  loading: boolean;
  totalCents: number;
  transactionCount: number;
  transactions: BookingTransactionsSumResponse["transactions"];
};

function previousMonthToken() {
  const now = new Date();
  const previousMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  return `${previousMonth.getFullYear()}-${String(previousMonth.getMonth() + 1).padStart(2, "0")}`;
}

function defaultMonthlyInvoiceDraftState(): MonthlyInvoiceDraftState {
  return {
    provider: "paypal",
    monthToken: previousMonthToken(),
    amount: "",
    notes: "",
    fileName: "Optional",
  };
}

function defaultMonthlyInvoicePreviewState(): MonthlyInvoicePreviewState {
  return {
    visible: false,
    loading: false,
    totalCents: 0,
    transactionCount: 0,
    transactions: [],
  };
}

function bookingToolButtonConfig(bookingsSubtab: BookingsSubtab, bookingClass: string) {
  if (bookingsSubtab === "transactions") {
    if (bookingClass === "monthly") {
      return { target: "sammelrechnungTools" as const, label: "Neue Sammelrechnung" };
    }
    if (bookingClass === "single") {
      return { target: "bookingsTransactionTools" as const, label: "Neue Transaktion" };
    }
    return null;
  }
  if (bookingsSubtab === "templates") {
    return { target: "bookingsTemplateTools" as const, label: "Neues Template" };
  }
  if (bookingsSubtab === "accounts") {
    return { target: "bookingsAccountTools" as const, label: "Neues Konto" };
  }
  if (bookingsSubtab === "documents") {
    return { target: "bookingsDocumentTools" as const, label: "Beleg hochladen" };
  }
  return null;
}

function setButtonLabel(button: HTMLElement, label: string) {
  const nodes = Array.from(button.childNodes);
  const textNode = nodes.filter((node) => node.nodeType === Node.TEXT_NODE).pop();
  if (textNode) {
    textNode.textContent = ` ${label}`;
    return;
  }
  button.append(` ${label}`);
}

function normalizeBookingClass(value: string | undefined) {
  const token = String(value || "automatic").trim().toLowerCase();
  if (token === "all" || token === "automatic" || token === "monthly" || token === "single") {
    return token;
  }
  return "automatic";
}

function normalizeBookingCategory(value: string | undefined) {
  return String(value || "").trim().toLowerCase();
}

function normalizeBookingType(value: string | undefined) {
  return String(value || "").trim().toUpperCase();
}

function setStatusMessage(message: string, level: StatusLevel = "info") {
  const className = level === "error" ? "status-error" : level === "ok" ? "status-ok" : "status-info";
  const statusBox = document.getElementById("statusBox");
  if (statusBox instanceof HTMLElement) {
    statusBox.className = `status ${className}`;
    statusBox.textContent = message;
  }
}

function toIsoFromLocalInput(value: string) {
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

function parseEuroToCents(value: string) {
  let raw = String(value || "").trim();
  if (raw.includes(",")) {
    raw = raw.replace(/\./g, "").replace(",", ".");
  }
  const numeric = Number(raw);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return null;
  }
  return Math.round(numeric * 100);
}

function currentPeriodKey() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
}

function periodKeyFromDateLike(value: string) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  if (/^\d{4}-\d{2}$/.test(text)) {
    return text;
  }
  if (/^\d{4}-\d{2}-\d{2}/.test(text)) {
    return text.slice(0, 7);
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }
  return `${parsed.getFullYear()}-${String(parsed.getMonth() + 1).padStart(2, "0")}`;
}

function parsePeriodKeyToIndex(periodKey: string) {
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

function buildPeriodKeyRange(startPeriodKey: string, endPeriodKey: string) {
  const start = parsePeriodKeyToIndex(startPeriodKey);
  const end = parsePeriodKeyToIndex(endPeriodKey);
  if (!start || !end) {
    return [] as string[];
  }
  const startIndex = start.year * 12 + (start.month - 1);
  const endIndex = end.year * 12 + (end.month - 1);
  if (startIndex > endIndex) {
    return [] as string[];
  }

  const periods: string[] = [];
  for (let index = startIndex; index <= endIndex; index += 1) {
    const year = Math.floor(index / 12);
    const month = (index % 12) + 1;
    periods.push(`${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}`);
  }
  return periods;
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

function formatDate(value: string | undefined) {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return new Intl.DateTimeFormat("de-DE").format(date);
}

function centsToInputValue(cents: number | undefined) {
  const value = Number(cents || 0) / 100;
  return Number.isFinite(value) ? value.toFixed(2) : "";
}

function toLocalInputFromIso(value: string | undefined) {
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

function toDateInputValue(value: string | undefined) {
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

function appendInlineDisposition(url: string) {
  const safeUrl = String(url || "").trim();
  if (!safeUrl) {
    return "";
  }
  return `${safeUrl}${safeUrl.includes("?") ? "&" : "?"}disposition=inline`;
}

function monthDateFromToken(token: string) {
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

function monthTokenFromDate(date: Date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
}

function dateTokenFromDate(date: Date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
}

function sammelMonthPeriodFrom(monthToken: string) {
  return monthDateFromToken(monthToken) ? `${monthToken}-01` : "";
}

function sammelMonthPeriodTo(monthToken: string) {
  const date = monthDateFromToken(monthToken);
  if (!date) {
    return "";
  }
  return dateTokenFromDate(new Date(date.getFullYear(), date.getMonth() + 1, 0));
}

function sammelMonthLabel(monthToken: string) {
  const date = monthDateFromToken(monthToken);
  if (!date) {
    return "-";
  }
  return `${date.toLocaleDateString("de-DE", { month: "long" })} ${date.getFullYear()}`;
}

function inferMimeTypeFromFilename(filename: string | undefined) {
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

function detectPreviewKind(mimeType: string | undefined, filename: string | undefined) {
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

function bookingAccountLabel(account: OptionItem) {
  const provider = String(account.provider || "").trim();
  return provider ? `${String(account.name || "")} (${provider})` : String(account.name || "");
}

function bookingTemplateLabel(template: BookingTemplateRow | OptionItem) {
  const amount = Number(template.default_amount_gross || 0);
  const amountText = amount > 0 ? formatMoneyFromCents(amount) : "-";
  return `${String(template.name || "-")} | ${String(template.schedule || "-")} | ${amountText}`;
}

function bookingOrderLabel(order: OptionItem) {
  return `${String(order.provider || "-").toUpperCase()} | ${String(order.external_order_id || order.id || "-")}`;
}

function bookingTransactionLabel(transaction: BookingRow | OptionItem) {
  const when = formatDate(transaction.date);
  const reference = String(transaction.reference || transaction.type || transaction.id || "-");
  return `${when} | ${reference}`;
}

function detailText(value: unknown, fallback = "-") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function bookingTxCategoryMetaForType(type: string | undefined) {
  const key = BOOKING_TX_TYPE_TO_CATEGORY[normalizeBookingType(type)] || "other";
  return {
    key,
    ...BOOKING_TX_CATEGORY_META[key],
  };
}

function renderAccountOptions(options: OptionItem[], selected?: string) {
  return (
    <>
      <option value="">Ohne Konto</option>
      {options.map((account, index) => {
        const id = String(account.id || "").trim();
        if (!id) {
          return null;
        }
        return <option key={`${id}:${index}`} value={id}>{bookingAccountLabel(account)}</option>;
      })}
    </>
  );
}

function renderTemplateOptions(options: BookingTemplateRow[], selected?: string) {
  return (
    <>
      <option value="">Ohne Template</option>
      {options.map((template, index) => {
        const id = String(template.id || "").trim();
        if (!id) {
          return null;
        }
        return <option key={`${id}:${index}`} value={id}>{bookingTemplateLabel(template)}</option>;
      })}
    </>
  );
}

function renderOrderOptions(options: OptionItem[]) {
  return (
    <>
      <option value="">Keine Zuordnung</option>
      {options.map((order, index) => {
        const id = String(order.id || "").trim();
        if (!id) {
          return null;
        }
        return <option key={`${id}:${index}`} value={id}>{bookingOrderLabel(order)}</option>;
      })}
    </>
  );
}

function renderTransactionOptions(options: Array<BookingRow | OptionItem>) {
  return (
    <>
      <option value="">Keine Verknuepfung</option>
      {options.map((transaction, index) => {
        const id = String(transaction.id || "").trim();
        if (!id) {
          return null;
        }
        return <option key={`${id}:${index}`} value={id}>{bookingTransactionLabel(transaction)}</option>;
      })}
    </>
  );
}

function DetailRows({ items }: { items: Array<[string, unknown]> }) {
  return (
    <>
      {items.map(([label, value]) => (
        <div key={label} className="detail-row">
          <span>{label}</span>
          <strong>{detailText(value)}</strong>
        </div>
      ))}
    </>
  );
}

function DocumentActions({
  url,
  filename,
  mimeType,
}: {
  url: string;
  filename: string;
  mimeType?: string;
}) {
  const previewKind = detectPreviewKind(mimeType, filename);

  return (
    <span className="doc-actions">
      {previewKind ? (
        <button
          className="btn-inline ghost"
          data-action="preview-document"
          data-url={url}
          data-filename={filename}
          data-mime={String(mimeType || "")}
          type="button"
        >
          Preview
        </button>
      ) : null}
      <a href={url} target="_blank" rel="noreferrer">Download</a>
    </span>
  );
}

function BookingTransactionDetailPreview({ transaction }: { transaction: BookingRow }) {
  const documentId = String(transaction.document_id || "").trim();
  if (!documentId) {
    return (
      <section className="booking-detail-preview">
        <h3>Beleg Preview</h3>
        <div className="booking-detail-note">Kein Beleg mit dieser Transaktion verknuepft.</div>
      </section>
    );
  }

  const documentName = String(transaction.document?.original_filename || transaction.document?.stored_filename || documentId || "Beleg");
  const mimeType = String(transaction.document?.mime_type || "");
  const downloadUrl = `/api/bookings/documents/${encodeURIComponent(documentId)}/download`;
  const previewKind = detectPreviewKind(mimeType, documentName);
  const inlineUrl = appendInlineDisposition(downloadUrl);
  const previewUrl = previewKind === "pdf" ? `${inlineUrl}#toolbar=1&view=FitH` : inlineUrl;

  return (
    <section className="booking-detail-preview">
      <h3>Beleg Preview</h3>
      <div className="booking-detail-note">{`Datei: ${documentName} | MIME: ${mimeType || "-"}`}</div>
      {previewKind === "image" ? <img className="booking-detail-preview-image" src={previewUrl} alt={documentName} /> : null}
      {previewKind === "pdf" ? <iframe className="booking-detail-preview-frame" src={previewUrl} title={documentName} /> : null}
      {!previewKind ? <div className="booking-detail-note">Preview fuer diesen Dateityp nicht verfuegbar.</div> : null}
      <div>
        <DocumentActions url={downloadUrl} filename={documentName} mimeType={mimeType} />
      </div>
    </section>
  );
}

export function BookingsPage({ panelElement }: BookingsPageProps) {
  const { filters: shellFilters, bookingsSubtab, refreshRequestToken } = useDashboardShellState();
  const {
    bookingsDetailsApi,
    bookingsRefreshDetail,
    bookingsRefreshRequestToken,
    orderDetailsApi,
    previewModalApi,
    requestBookingsRefresh,
    setBookingsUiState,
  } = useDashboardRuntime();
  const [data, setData] = useState<BookingsData>(EMPTY_BOOKINGS_DATA);
  const [bookingClass, setBookingClass] = useState(() => normalizeBookingClass(undefined));
  const [bookingCategory, setBookingCategory] = useState(() => normalizeBookingCategory(undefined));
  const [bookingType, setBookingType] = useState(() => normalizeBookingType(undefined));
  const [openToolPanelId, setOpenToolPanelId] = useState<BookingsToolPanelId>("");
  const [monthlyInvoiceDraft, setMonthlyInvoiceDraft] = useState<MonthlyInvoiceDraftState>(() => defaultMonthlyInvoiceDraftState());
  const [monthlyInvoicePreview, setMonthlyInvoicePreview] = useState<MonthlyInvoicePreviewState>(() => defaultMonthlyInvoicePreviewState());
  const [monthlyInvoicePreviewNonce, setMonthlyInvoicePreviewNonce] = useState(0);
  const [isSammelMonthMenuOpen, setSammelMonthMenuOpen] = useState(false);
  const [sammelPickerYear, setSammelPickerYear] = useState(() => monthDateFromToken(defaultMonthlyInvoiceDraftState().monthToken)?.getFullYear() || new Date().getFullYear());
  const [refreshNonce, setRefreshNonce] = useState(0);
  const requestIdRef = useRef(0);
  const monthlyInvoiceUploadTargetIdRef = useRef("");
  const monthlyInvoiceDraftFileRef = useRef<File | null>(null);
  const lastRefreshRequestTokenRef = useRef(refreshRequestToken);
  const lastBookingsRefreshRequestTokenRef = useRef(bookingsRefreshRequestToken);

  const roots = useMemo(() => ({
    transactions: panelElement.querySelector("#bookingsTransactionsReactRoot"),
    monthlyInvoices: panelElement.querySelector("#bookingsMonthlyInvoicesReactRoot"),
    orders: panelElement.querySelector("#bookingsOrdersReactRoot"),
    templates: panelElement.querySelector("#bookingsTemplatesReactRoot"),
    accounts: panelElement.querySelector("#bookingsAccountsReactRoot"),
    documents: panelElement.querySelector("#bookingsDocumentsReactRoot"),
  }), [panelElement]);

  const ui = useMemo(() => ({
    newButton: panelElement.querySelector("#bookingsNewBtn"),
    bookingClassBar: panelElement.querySelector("#bookingClassBar"),
    bookingClassControl: panelElement.querySelector("#bookingClassControl"),
    bookingClassAllBtn: panelElement.querySelector("#bookingClassAllBtn"),
    bookingClassAutoBtn: panelElement.querySelector("#bookingClassAutoBtn"),
    bookingClassMonthlyBtn: panelElement.querySelector("#bookingClassMonthlyBtn"),
    bookingClassSingleBtn: panelElement.querySelector("#bookingClassSingleBtn"),
    bookingTxLegend: panelElement.querySelector("#bookingTxLegend"),
    transactionTools: panelElement.querySelector("#bookingsTransactionTools"),
    monthlyInvoiceSection: panelElement.querySelector("#sammelrechnungSection"),
    monthlyInvoiceTools: panelElement.querySelector("#sammelrechnungTools"),
    templateTools: panelElement.querySelector("#bookingsTemplateTools"),
    accountTools: panelElement.querySelector("#bookingsAccountTools"),
    documentTools: panelElement.querySelector("#bookingsDocumentTools"),
    createBookingOrder: panelElement.querySelector("#createBookingOrder"),
    createBookingAccount: panelElement.querySelector("#createBookingAccount"),
    createBookingTemplate: panelElement.querySelector("#createBookingTemplate"),
    templateAccountInput: panelElement.querySelector("#templateAccountInput"),
    bookingDocumentTxInput: panelElement.querySelector("#bookingDocumentTxInput"),
    createMonthlyInvoiceProvider: panelElement.querySelector("#createSammelProvider"),
    createMonthlyInvoiceAmount: panelElement.querySelector("#createSammelAmount"),
    createMonthlyInvoiceNotes: panelElement.querySelector("#createSammelNotes"),
    createMonthlyInvoiceFile: panelElement.querySelector("#createSammelFile"),
    createMonthlyInvoiceFileName: panelElement.querySelector("#createSammelFileName"),
    createMonthlyInvoiceButton: panelElement.querySelector("#createSammelBtn"),
    sammelMonthButton: panelElement.querySelector("#sammelMonthBtn"),
    sammelMonthMenu: panelElement.querySelector("#sammelMonthMenu"),
    sammelYearPrevBtn: panelElement.querySelector("#sammelYearPrevBtn"),
    sammelYearNextBtn: panelElement.querySelector("#sammelYearNextBtn"),
    sammelYearLabel: panelElement.querySelector("#sammelYearLabel"),
    sammelMonthGrid: panelElement.querySelector("#sammelMonthGrid"),
    sammelPreview: panelElement.querySelector("#sammelPreview"),
  }), [panelElement]);

  const query = useMemo(() => ({
    from: shellFilters.from,
    to: shellFilters.to,
    marketplace: shellFilters.marketplace,
    q: shellFilters.q,
    bookingClass,
    category: bookingCategory,
    type: bookingType,
  }), [bookingCategory, bookingClass, bookingType, shellFilters.from, shellFilters.marketplace, shellFilters.q, shellFilters.to]);

  useEffect(() => {
    panelElement.dataset.reactBookingsMounted = "true";
    return () => {
      delete panelElement.dataset.reactBookingsMounted;
    };
  }, [panelElement]);

  useEffect(() => {
    if (bookingsRefreshRequestToken === 0 || lastBookingsRefreshRequestTokenRef.current === bookingsRefreshRequestToken) {
      return;
    }
    lastBookingsRefreshRequestTokenRef.current = bookingsRefreshRequestToken;

    if (typeof bookingsRefreshDetail.bookingClass === "string") {
      setBookingClass(normalizeBookingClass(bookingsRefreshDetail.bookingClass));
    }
    if (typeof bookingsRefreshDetail.category === "string") {
      setBookingCategory(normalizeBookingCategory(bookingsRefreshDetail.category));
    }
    if (typeof bookingsRefreshDetail.bookingType === "string") {
      setBookingType(normalizeBookingType(bookingsRefreshDetail.bookingType));
    }
    setRefreshNonce((current) => current + 1);
  }, [bookingsRefreshDetail, bookingsRefreshRequestToken]);

  useEffect(() => {
    const nextDate = monthDateFromToken(monthlyInvoiceDraft.monthToken);
    if (!nextDate) {
      return;
    }
    setSammelPickerYear(nextDate.getFullYear());
  }, [monthlyInvoiceDraft.monthToken]);

  useEffect(() => {
    const toolConfig = bookingToolButtonConfig(bookingsSubtab, bookingClass);
    const nextOpenPanelId = toolConfig?.target || "";
    setOpenToolPanelId((current) => (current === nextOpenPanelId ? current : ""));
  }, [bookingClass, bookingsSubtab]);

  useEffect(() => {
    const toolConfig = bookingToolButtonConfig(bookingsSubtab, bookingClass);
    const classButtons = [
      [ui.bookingClassAllBtn, bookingClass === "all"],
      [ui.bookingClassAutoBtn, bookingClass === "automatic"],
      [ui.bookingClassMonthlyBtn, bookingClass === "monthly"],
      [ui.bookingClassSingleBtn, bookingClass === "single"],
    ] as const;

    classButtons.forEach(([element, active]) => {
      if (element instanceof HTMLElement) {
        element.classList.toggle("active", active);
      }
    });

    if (ui.bookingClassBar instanceof HTMLElement) {
      ui.bookingClassBar.style.display = bookingsSubtab === "transactions" ? "" : "none";
    }

    const transactionToolsOpen = openToolPanelId === "bookingsTransactionTools";
    const monthlyInvoiceToolsOpen = openToolPanelId === "sammelrechnungTools";
    const templateToolsOpen = openToolPanelId === "bookingsTemplateTools";
    const accountToolsOpen = openToolPanelId === "bookingsAccountTools";
    const documentToolsOpen = openToolPanelId === "bookingsDocumentTools";

    if (ui.transactionTools instanceof HTMLElement) {
      ui.transactionTools.style.display = bookingClass === "single" ? "" : "none";
      ui.transactionTools.classList.toggle("open", transactionToolsOpen);
    }
    if (ui.monthlyInvoiceSection instanceof HTMLElement) {
      ui.monthlyInvoiceSection.style.display = bookingClass === "monthly" ? "" : "none";
    }
    if (ui.monthlyInvoiceTools instanceof HTMLElement) {
      ui.monthlyInvoiceTools.classList.toggle("open", monthlyInvoiceToolsOpen);
    }
    if (ui.templateTools instanceof HTMLElement) {
      ui.templateTools.classList.toggle("open", templateToolsOpen);
    }
    if (ui.accountTools instanceof HTMLElement) {
      ui.accountTools.classList.toggle("open", accountToolsOpen);
    }
    if (ui.documentTools instanceof HTMLElement) {
      ui.documentTools.classList.toggle("open", documentToolsOpen);
    }

    if (ui.newButton instanceof HTMLElement) {
      if (toolConfig) {
        ui.newButton.style.display = "";
        ui.newButton.setAttribute("data-target", toolConfig.target);
        ui.newButton.setAttribute("aria-expanded", String(openToolPanelId === toolConfig.target));
        setButtonLabel(ui.newButton, toolConfig.label);
      } else {
        ui.newButton.style.display = "none";
        ui.newButton.setAttribute("aria-expanded", "false");
      }
    }
  }, [bookingClass, bookingsSubtab, openToolPanelId, ui.accountTools, ui.bookingClassAllBtn, ui.bookingClassAutoBtn, ui.bookingClassBar, ui.bookingClassMonthlyBtn, ui.bookingClassSingleBtn, ui.documentTools, ui.monthlyInvoiceSection, ui.monthlyInvoiceTools, ui.newButton, ui.templateTools, ui.transactionTools]);

  useEffect(() => {
    const provider = String(monthlyInvoiceDraft.provider || "").trim().toLowerCase();
    const periodFrom = sammelMonthPeriodFrom(monthlyInvoiceDraft.monthToken);
    const periodTo = sammelMonthPeriodTo(monthlyInvoiceDraft.monthToken);

    if (!provider || !periodFrom || !periodTo || bookingClass !== "monthly") {
      setMonthlyInvoicePreview(defaultMonthlyInvoicePreviewState());
      return;
    }

    let cancelled = false;
    setMonthlyInvoicePreview((current) => ({
      ...current,
      visible: true,
      loading: true,
    }));

    const timerId = window.setTimeout(() => {
      void fetchBookingTransactionsSum(provider, periodFrom, periodTo)
        .then((preview) => {
          if (cancelled) {
            return;
          }
          setMonthlyInvoicePreview({
            visible: true,
            loading: false,
            totalCents: Number(preview.total_cents || 0),
            transactionCount: Number(preview.transaction_count || 0),
            transactions: Array.isArray(preview.transactions) ? preview.transactions : [],
          });
        })
        .catch(() => {
          if (cancelled) {
            return;
          }
          setMonthlyInvoicePreview(defaultMonthlyInvoicePreviewState());
        });
    }, 350);

    return () => {
      cancelled = true;
      window.clearTimeout(timerId);
    };
  }, [bookingClass, monthlyInvoiceDraft.monthToken, monthlyInvoiceDraft.provider, monthlyInvoicePreviewNonce]);

  useEffect(() => {
    if (ui.createMonthlyInvoiceProvider instanceof HTMLSelectElement) {
      ui.createMonthlyInvoiceProvider.value = monthlyInvoiceDraft.provider;
    }
    if (ui.createMonthlyInvoiceAmount instanceof HTMLInputElement) {
      ui.createMonthlyInvoiceAmount.value = monthlyInvoiceDraft.amount;
    }
    if (ui.createMonthlyInvoiceNotes instanceof HTMLInputElement) {
      ui.createMonthlyInvoiceNotes.value = monthlyInvoiceDraft.notes;
    }
    if (ui.createMonthlyInvoiceFileName instanceof HTMLElement) {
      ui.createMonthlyInvoiceFileName.textContent = monthlyInvoiceDraft.fileName || "Optional";
    }
    if (ui.sammelMonthButton instanceof HTMLElement) {
      ui.sammelMonthButton.textContent = sammelMonthLabel(monthlyInvoiceDraft.monthToken);
      ui.sammelMonthButton.setAttribute("aria-expanded", String(isSammelMonthMenuOpen));
    }
    if (ui.sammelMonthMenu instanceof HTMLElement) {
      ui.sammelMonthMenu.setAttribute("aria-hidden", isSammelMonthMenuOpen ? "false" : "true");
      ui.sammelMonthMenu.classList.toggle("active", isSammelMonthMenuOpen);
    }
    if (ui.sammelYearLabel instanceof HTMLElement) {
      ui.sammelYearLabel.textContent = String(sammelPickerYear);
    }
    if (ui.sammelPreview instanceof HTMLElement) {
      ui.sammelPreview.style.display = monthlyInvoicePreview.visible ? "" : "none";
    }
  }, [isSammelMonthMenuOpen, monthlyInvoiceDraft.amount, monthlyInvoiceDraft.fileName, monthlyInvoiceDraft.monthToken, monthlyInvoiceDraft.notes, monthlyInvoiceDraft.provider, monthlyInvoicePreview.visible, sammelPickerYear, ui.createMonthlyInvoiceAmount, ui.createMonthlyInvoiceFileName, ui.createMonthlyInvoiceNotes, ui.createMonthlyInvoiceProvider, ui.sammelMonthButton, ui.sammelMonthMenu, ui.sammelPreview, ui.sammelYearLabel]);

  useEffect(() => {
    const newButton = ui.newButton;
    const bookingClassControl = ui.bookingClassControl;
    const monthlyInvoiceProvider = ui.createMonthlyInvoiceProvider;
    const monthlyInvoiceAmount = ui.createMonthlyInvoiceAmount;
    const monthlyInvoiceNotes = ui.createMonthlyInvoiceNotes;
    const monthlyInvoiceFile = ui.createMonthlyInvoiceFile;
    const monthlyInvoiceCreateButton = ui.createMonthlyInvoiceButton;
    const sammelMonthButton = ui.sammelMonthButton;
    const sammelYearPrevButton = ui.sammelYearPrevBtn;
    const sammelYearNextButton = ui.sammelYearNextBtn;
    const sammelMonthGrid = ui.sammelMonthGrid;

    const handleOutsideClick = (event: MouseEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) {
        return;
      }
      if (ui.sammelMonthButton instanceof HTMLElement && ui.sammelMonthMenu instanceof HTMLElement) {
        if (!ui.sammelMonthButton.contains(target) && !ui.sammelMonthMenu.contains(target)) {
          setSammelMonthMenuOpen(false);
        }
      }
    };

    const handleNewButtonClick = () => {
      const toolConfig = bookingToolButtonConfig(bookingsSubtab, bookingClass);
      if (!toolConfig) {
        return;
      }
      setOpenToolPanelId((current) => current === toolConfig.target ? "" : toolConfig.target);
    };

    const handleBookingClassClick = (event: Event) => {
      const target = event.target;
      const button = target instanceof HTMLElement ? target.closest<HTMLElement>("[data-booking-class]") : null;
      if (!button) {
        return;
      }
      const nextClass = normalizeBookingClass(button.dataset.bookingClass);
      setBookingClass(nextClass);
      setBookingCategory("");
      setBookingType("");
      setOpenToolPanelId("");
      setSammelMonthMenuOpen(false);
    };

    const handleMonthlyInvoiceProviderChange = () => {
      setMonthlyInvoiceDraft((current) => ({
        ...current,
        provider: monthlyInvoiceProvider instanceof HTMLSelectElement ? String(monthlyInvoiceProvider.value || "").trim().toLowerCase() : current.provider,
      }));
      setMonthlyInvoicePreviewNonce((current) => current + 1);
    };

    const handleMonthlyInvoiceAmountInput = () => {
      setMonthlyInvoiceDraft((current) => ({
        ...current,
        amount: monthlyInvoiceAmount instanceof HTMLInputElement ? String(monthlyInvoiceAmount.value || "") : current.amount,
      }));
    };

    const handleMonthlyInvoiceNotesInput = () => {
      setMonthlyInvoiceDraft((current) => ({
        ...current,
        notes: monthlyInvoiceNotes instanceof HTMLInputElement ? String(monthlyInvoiceNotes.value || "") : current.notes,
      }));
    };

    const handleMonthlyInvoiceFileChange = () => {
      const file = monthlyInvoiceFile instanceof HTMLInputElement ? monthlyInvoiceFile.files?.[0] || null : null;
      monthlyInvoiceDraftFileRef.current = file;
      setMonthlyInvoiceDraft((current) => ({
        ...current,
        fileName: file?.name || "Optional",
      }));
    };

    const handleSammelMonthButtonClick = () => {
      setSammelMonthMenuOpen((current) => !current);
    };

    const handleSammelYearPrevClick = () => {
      setSammelPickerYear((current) => current - 1);
    };

    const handleSammelYearNextClick = () => {
      setSammelPickerYear((current) => current + 1);
    };

    const handleSammelMonthGridClick = (event: Event) => {
      const target = event.target;
      const button = target instanceof HTMLElement ? target.closest<HTMLElement>("[data-month]") : null;
      const token = String(button?.dataset.month || "").trim();
      if (!token) {
        return;
      }
      setMonthlyInvoiceDraft((current) => ({
        ...current,
        monthToken: token,
      }));
      setSammelMonthMenuOpen(false);
      setMonthlyInvoicePreviewNonce((current) => current + 1);
    };

    const handleCreateMonthlyInvoice = () => {
      const provider = String(monthlyInvoiceDraft.provider || "").trim().toLowerCase();
      const periodFrom = sammelMonthPeriodFrom(monthlyInvoiceDraft.monthToken);
      const periodTo = sammelMonthPeriodTo(monthlyInvoiceDraft.monthToken);
      const rawAmount = String(monthlyInvoiceDraft.amount || "");
      const amountCents = parseEuroToCents(rawAmount);
      const notes = String(monthlyInvoiceDraft.notes || "").trim() || null;

      if (!provider) {
        setStatusMessage("Provider ist erforderlich.", "error");
        return;
      }
      if (!periodFrom || !periodTo) {
        setStatusMessage("Monat ist erforderlich.", "error");
        return;
      }
      if (!amountCents) {
        setStatusMessage(`Rechnungsbetrag muss groesser 0 sein (Eingabe: "${rawAmount}").`, "error");
        return;
      }

      void createMonthlyInvoiceMutation({
        provider,
        period_from: periodFrom,
        period_to: periodTo,
        invoice_amount_cents: amountCents,
        currency: "EUR",
        notes,
      })
        .then(async (result) => {
          const invoiceId = String(result.invoice?.id || "").trim();
          const file = monthlyInvoiceDraftFileRef.current;
          let documentLinked = false;
          let uploadWarning = "";

          if (file && invoiceId) {
            try {
              const form = new FormData();
              form.append("file", file);
              const uploadResult = await uploadBookingDocument(form);
              const documentId = String(uploadResult.document?.id || "").trim();
              if (documentId) {
                await updateMonthlyInvoice(invoiceId, { document_id: documentId });
                documentLinked = true;
              }
            } catch (error) {
              uploadWarning = error instanceof Error ? error.message : "Unbekannter Fehler";
            }
          }

          monthlyInvoiceDraftFileRef.current = null;
          if (ui.createMonthlyInvoiceFile instanceof HTMLInputElement) {
            ui.createMonthlyInvoiceFile.value = "";
          }
          setMonthlyInvoiceDraft((current) => ({
            ...current,
            amount: "",
            notes: "",
            fileName: "Optional",
          }));
          setOpenToolPanelId("");
          setRefreshNonce((current) => current + 1);
          setMonthlyInvoicePreviewNonce((current) => current + 1);

          const createdInvoice = result.invoice || null;
          if (createdInvoice) {
            const isMatched = String(createdInvoice.status || "").trim().toLowerCase() === "matched";
            const differenceLabel = formatMoneyFromCents(Number(createdInvoice.difference_cents || 0));
            const suffix = uploadWarning
              ? ` Beleg-Upload fehlgeschlagen: ${uploadWarning}.`
              : documentLinked ? " Beleg verknuepft." : "";
            setStatusMessage(
              isMatched
                ? `Sammelrechnung angelegt. Matched.${suffix}`
                : `Sammelrechnung angelegt. Differenz: ${differenceLabel}.${suffix}`,
              isMatched ? "ok" : "info",
            );
            return;
          }
          setStatusMessage("Sammelrechnung angelegt.", "ok");
        })
        .catch((error: Error) => {
          setStatusMessage(`Sammelrechnung konnte nicht angelegt werden: ${error.message}`, "error");
        });
    };

    newButton?.addEventListener("click", handleNewButtonClick);
    bookingClassControl?.addEventListener("click", handleBookingClassClick);
    monthlyInvoiceProvider?.addEventListener("change", handleMonthlyInvoiceProviderChange);
    monthlyInvoiceAmount?.addEventListener("input", handleMonthlyInvoiceAmountInput);
    monthlyInvoiceNotes?.addEventListener("input", handleMonthlyInvoiceNotesInput);
    monthlyInvoiceFile?.addEventListener("change", handleMonthlyInvoiceFileChange);
    monthlyInvoiceCreateButton?.addEventListener("click", handleCreateMonthlyInvoice);
    sammelMonthButton?.addEventListener("click", handleSammelMonthButtonClick);
    sammelYearPrevButton?.addEventListener("click", handleSammelYearPrevClick);
    sammelYearNextButton?.addEventListener("click", handleSammelYearNextClick);
    sammelMonthGrid?.addEventListener("click", handleSammelMonthGridClick);
    document.addEventListener("click", handleOutsideClick);

    return () => {
      newButton?.removeEventListener("click", handleNewButtonClick);
      bookingClassControl?.removeEventListener("click", handleBookingClassClick);
      monthlyInvoiceProvider?.removeEventListener("change", handleMonthlyInvoiceProviderChange);
      monthlyInvoiceAmount?.removeEventListener("input", handleMonthlyInvoiceAmountInput);
      monthlyInvoiceNotes?.removeEventListener("input", handleMonthlyInvoiceNotesInput);
      monthlyInvoiceFile?.removeEventListener("change", handleMonthlyInvoiceFileChange);
      monthlyInvoiceCreateButton?.removeEventListener("click", handleCreateMonthlyInvoice);
      sammelMonthButton?.removeEventListener("click", handleSammelMonthButtonClick);
      sammelYearPrevButton?.removeEventListener("click", handleSammelYearPrevClick);
      sammelYearNextButton?.removeEventListener("click", handleSammelYearNextClick);
      sammelMonthGrid?.removeEventListener("click", handleSammelMonthGridClick);
      document.removeEventListener("click", handleOutsideClick);
    };
  }, [bookingClass, bookingsSubtab, monthlyInvoiceDraft, ui.bookingClassControl, ui.createMonthlyInvoiceAmount, ui.createMonthlyInvoiceButton, ui.createMonthlyInvoiceFile, ui.createMonthlyInvoiceProvider, ui.createMonthlyInvoiceNotes, ui.newButton, ui.sammelMonthButton, ui.sammelMonthGrid, ui.sammelYearNextBtn, ui.sammelYearPrevBtn]);

  useEffect(() => {
    let cancelled = false;
    const nextRequestId = requestIdRef.current + 1;
    requestIdRef.current = nextRequestId;

    Promise.all([
      fetchBookingsTransactions(query),
      fetchBookingOrders(query),
      fetchBookingLedgerOrders(),
      fetchBookingAccounts(),
      fetchBookingTemplates(),
      fetchBookingDocuments(),
      fetchMonthlyInvoices(),
    ])
      .then(([
        bookingsPayload,
        ordersPayload,
        ledgerOrdersPayload,
        accountsPayload,
        templatesPayload,
        documentsPayload,
        monthlyInvoicesPayload,
      ]) => {
        if (cancelled || requestIdRef.current !== nextRequestId) {
          return;
        }

        const nextData: BookingsData = {
          bookings: Array.isArray(bookingsPayload.items) ? bookingsPayload.items : [],
          bookingsTotal: Number(bookingsPayload.total || 0),
          bookingsAllItems: Array.isArray(bookingsPayload.allItems) ? bookingsPayload.allItems : [],
          bookingOrders: Array.isArray(ordersPayload.items) ? ordersPayload.items : [],
          bookingOrdersTotal: Number(ordersPayload.total || ordersPayload.items.length || 0),
          bookingTemplates: Array.isArray(templatesPayload.items) ? templatesPayload.items : [],
          bookingTemplatesTotal: Number(templatesPayload.total || templatesPayload.items.length || 0),
          bookingAccounts: Array.isArray(accountsPayload.items) ? accountsPayload.items : [],
          bookingAccountsTotal: Number(accountsPayload.total || accountsPayload.items.length || 0),
          bookingDocuments: Array.isArray(documentsPayload.items) ? documentsPayload.items : [],
          bookingDocumentsTotal: Number(documentsPayload.total || documentsPayload.items.length || 0),
          monthlyInvoices: Array.isArray(monthlyInvoicesPayload.items) ? monthlyInvoicesPayload.items : [],
          monthlyInvoicesTotal: Number(monthlyInvoicesPayload.total || monthlyInvoicesPayload.items.length || 0),
          bookkeepingLedgerOrders: Array.isArray(ledgerOrdersPayload.items) ? ledgerOrdersPayload.items : [],
          bookkeepingLedgerOrdersTotal: Number(ledgerOrdersPayload.total || ledgerOrdersPayload.items.length || 0),
        };

        setData(nextData);
      })
      .catch(() => {
        if (cancelled || requestIdRef.current !== nextRequestId) {
          return;
        }
        setData(EMPTY_BOOKINGS_DATA);
      });

    return () => {
      cancelled = true;
    };
  }, [query, refreshNonce]);

  useEffect(() => {
    if (refreshRequestToken === 0 || lastRefreshRequestTokenRef.current === refreshRequestToken) {
      return;
    }
    lastRefreshRequestTokenRef.current = refreshRequestToken;
    setRefreshNonce((current) => current + 1);
  }, [refreshRequestToken]);

  useEffect(() => {
    setBookingsUiState({
      bookingClass,
      category: bookingCategory,
      bookingType,
    });
  }, [bookingCategory, bookingClass, bookingType, setBookingsUiState]);

  useEffect(() => {
    const legend = ui.bookingTxLegend;
    if (!(legend instanceof HTMLElement)) {
      return;
    }

    const handleLegendClick = (event: Event) => {
      const target = event.target;
      const item = target instanceof HTMLElement ? target.closest<HTMLElement>(".tx-legend-item[data-filter-category]") : null;
      if (!item) {
        return;
      }
      const nextCategory = normalizeBookingCategory(item.getAttribute("data-filter-category") || "");
      setBookingCategory((current) => current === nextCategory ? "" : nextCategory);
      setBookingType("");
      setStatusMessage("Transaktionsfilter aktualisiert.", "ok");
    };

    legend.addEventListener("click", handleLegendClick);
    return () => {
      legend.removeEventListener("click", handleLegendClick);
    };
  }, [ui.bookingTxLegend]);

  const requestRefresh = useCallback(() => {
    setRefreshNonce((current) => current + 1);
  }, []);

  useEffect(() => {
    const requestRefresh = () => {
      setRefreshNonce((current) => current + 1);
    };

    const createBookingButton = panelElement.querySelector("#createBookingTxBtn");
    const createTemplateButton = panelElement.querySelector("#createTemplateBtn");
    const createAccountButton = panelElement.querySelector("#createAccountBtn");
    const uploadDocumentButton = panelElement.querySelector("#uploadBookingDocumentBtn");
    const monthlyInvoiceUploadInput = document.getElementById("bookingMonthlyInvoiceUploadInput");
    const handleCreateBookingTransaction = () => {
      const dateInput = panelElement.querySelector("#createBookingDate");
      const typeInput = panelElement.querySelector("#createBookingType");
      const directionInput = panelElement.querySelector("#createBookingDirection");
      const amountInput = panelElement.querySelector("#createBookingAmount");
      const providerInput = panelElement.querySelector("#createBookingProvider");
      const statusInput = panelElement.querySelector("#createBookingStatus");
      const referenceInput = panelElement.querySelector("#createBookingReference");
      const orderInput = panelElement.querySelector("#createBookingOrder");
      const accountInput = panelElement.querySelector("#createBookingAccount");
      const templateInput = panelElement.querySelector("#createBookingTemplate");
      const notesInput = panelElement.querySelector("#createBookingNotes");

      const dateIso = dateInput instanceof HTMLInputElement ? toIsoFromLocalInput(dateInput.value) : "";
      const amountCents = amountInput instanceof HTMLInputElement ? parseEuroToCents(amountInput.value) : null;
      const provider = providerInput instanceof HTMLInputElement ? String(providerInput.value || "").trim() : "";

      if (!dateIso) {
        setStatusMessage("Datum ist ungueltig.", "error");
        return;
      }
      if (!amountCents) {
        setStatusMessage("Betrag muss groesser 0 sein.", "error");
        return;
      }
      if (!provider) {
        setStatusMessage("Provider ist erforderlich.", "error");
        return;
      }

      void createBookingTransaction({
        date: dateIso,
        type: typeInput instanceof HTMLSelectElement ? typeInput.value : "SALE",
        direction: directionInput instanceof HTMLSelectElement ? directionInput.value : "IN",
        amount_gross: amountCents,
        currency: "EUR",
        provider,
        status: statusInput instanceof HTMLSelectElement ? statusInput.value : "confirmed",
        reference: referenceInput instanceof HTMLInputElement ? String(referenceInput.value || "").trim() || null : null,
        notes: notesInput instanceof HTMLInputElement ? String(notesInput.value || "").trim() || null : null,
        order_id: orderInput instanceof HTMLSelectElement ? orderInput.value || null : null,
        payment_account_id: accountInput instanceof HTMLSelectElement ? accountInput.value || null : null,
        template_id: templateInput instanceof HTMLSelectElement ? templateInput.value || null : null,
        source: "manual",
        booking_class: "single",
      })
        .then(() => {
          if (amountInput instanceof HTMLInputElement) amountInput.value = "";
          if (referenceInput instanceof HTMLInputElement) referenceInput.value = "";
          if (notesInput instanceof HTMLInputElement) notesInput.value = "";
          requestRefresh();
          setStatusMessage("Transaktion angelegt.", "ok");
        })
        .catch((error: Error) => {
          setStatusMessage(`Transaktion konnte nicht angelegt werden: ${error.message}`, "error");
        });
    };

    const handleTransactionsChange = (event: Event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const row = target.closest("tr[data-booking-id]");
      if (!(row instanceof HTMLElement)) {
        return;
      }

      const bookingId = String(row.dataset.bookingId || "").trim();
      if (!bookingId) {
        return;
      }

      const statusInput = row.querySelector('[data-field="status"]');
      const referenceInput = row.querySelector('[data-field="reference"]');
      const notesInput = row.querySelector('[data-field="notes"]');
      const accountInput = row.querySelector('[data-field="payment_account_id"]');
      const templateInput = row.querySelector('[data-field="template_id"]');
      const documentInput = row.querySelector('[data-field="document_id"]');

      void updateBookingTransaction(bookingId, {
        status: statusInput instanceof HTMLInputElement || statusInput instanceof HTMLSelectElement ? statusInput.value : undefined,
        reference: referenceInput instanceof HTMLInputElement ? referenceInput.value : undefined,
        notes: notesInput instanceof HTMLInputElement || notesInput instanceof HTMLTextAreaElement ? notesInput.value : undefined,
        payment_account_id: accountInput instanceof HTMLSelectElement ? accountInput.value || null : undefined,
        template_id: templateInput instanceof HTMLSelectElement ? templateInput.value || null : undefined,
        document_id: documentInput instanceof HTMLSelectElement ? documentInput.value || null : undefined,
      })
        .then(() => {
          requestRefresh();
          setStatusMessage(`Buchung gespeichert: ${bookingId}`, "ok");
        })
        .catch((error: Error) => {
          setStatusMessage(`Buchung konnte nicht gespeichert werden: ${error.message}`, "error");
        });
    };

    const handleCreateTemplate = () => {
      const nameInput = panelElement.querySelector("#templateNameInput");
      const typeInput = panelElement.querySelector("#templateTypeInput");
      const directionInput = panelElement.querySelector("#templateDirectionInput");
      const amountInput = panelElement.querySelector("#templateAmountInput");
      const providerInput = panelElement.querySelector("#templateProviderInput");
      const counterpartyInput = panelElement.querySelector("#templateCounterpartyInput");
      const scheduleInput = panelElement.querySelector("#templateScheduleInput");
      const startDateInput = panelElement.querySelector("#templateStartDateInput");
      const dayInput = panelElement.querySelector("#templateDayInput");
      const accountInput = panelElement.querySelector("#templateAccountInput");
      const notesInput = panelElement.querySelector("#templateNotesInput");

      const name = nameInput instanceof HTMLInputElement ? String(nameInput.value || "").trim() : "";
      const provider = providerInput instanceof HTMLInputElement ? String(providerInput.value || "").trim() : "";
      const amountCents = amountInput instanceof HTMLInputElement ? parseEuroToCents(amountInput.value) : null;
      if (!name || !provider) {
        setStatusMessage("Template Name und Provider sind erforderlich.", "error");
        return;
      }
      if (!amountCents) {
        setStatusMessage("Template-Betrag muss groesser 0 sein.", "error");
        return;
      }

      const dayRaw = dayInput instanceof HTMLInputElement ? String(dayInput.value || "").trim() : "";
      const dayValue = dayRaw ? Number(dayRaw) : null;

      void createBookingTemplate({
        name,
        type: typeInput instanceof HTMLSelectElement ? typeInput.value : "SUBSCRIPTION",
        direction: directionInput instanceof HTMLSelectElement ? directionInput.value : "OUT",
        default_amount_gross: amountCents,
        currency: "EUR",
        provider,
        counterparty_name: counterpartyInput instanceof HTMLInputElement ? String(counterpartyInput.value || "").trim() || null : null,
        schedule: scheduleInput instanceof HTMLSelectElement ? scheduleInput.value : "monthly",
        start_date: startDateInput instanceof HTMLInputElement ? String(startDateInput.value || "").trim() || null : null,
        day_of_month: Number.isFinite(dayValue) ? Math.max(1, Math.min(31, Math.round(dayValue as number))) : null,
        payment_account_id: accountInput instanceof HTMLSelectElement ? accountInput.value || null : null,
        notes_default: notesInput instanceof HTMLInputElement ? String(notesInput.value || "").trim() || null : null,
        active: true,
      })
        .then(() => {
          if (nameInput instanceof HTMLInputElement) nameInput.value = "";
          if (amountInput instanceof HTMLInputElement) amountInput.value = "";
          if (counterpartyInput instanceof HTMLInputElement) counterpartyInput.value = "";
          if (startDateInput instanceof HTMLInputElement) startDateInput.value = "";
          if (notesInput instanceof HTMLInputElement) notesInput.value = "";
          requestRefresh();
          setStatusMessage("Template angelegt.", "ok");
        })
        .catch((error: Error) => {
          setStatusMessage(`Template konnte nicht angelegt werden: ${error.message}`, "error");
        });
    };

    const handleTemplatesChange = (event: Event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const row = target.closest("tr[data-template-id]");
      if (!(row instanceof HTMLElement)) {
        return;
      }

      const templateId = String(row.dataset.templateId || "").trim();
      const nameInput = row.querySelector('[data-field="name"]');
      const counterpartyInput = row.querySelector('[data-field="counterparty_name"]');
      const startDateInput = row.querySelector('[data-field="start_date"]');
      const amountInput = row.querySelector('[data-field="default_amount_eur"]');
      const scheduleInput = row.querySelector('[data-field="schedule"]');
      const paymentAccountInput = row.querySelector('[data-field="payment_account_id"]');
      const activeInput = row.querySelector('[data-field="active"]');

      const name = nameInput instanceof HTMLInputElement ? String(nameInput.value || "").trim() : "";
      const amountCents = amountInput instanceof HTMLInputElement ? parseEuroToCents(amountInput.value) : null;
      if (!templateId || !name || !amountCents) {
        setStatusMessage("Template Name und Betrag sind erforderlich.", "error");
        return;
      }

      void updateBookingTemplate(templateId, {
        name,
        counterparty_name: counterpartyInput instanceof HTMLInputElement ? String(counterpartyInput.value || "").trim() || null : null,
        start_date: startDateInput instanceof HTMLInputElement ? String(startDateInput.value || "").trim() || null : null,
        default_amount_gross: amountCents,
        schedule: scheduleInput instanceof HTMLSelectElement ? scheduleInput.value : "monthly",
        payment_account_id: paymentAccountInput instanceof HTMLSelectElement ? paymentAccountInput.value || null : null,
        active: activeInput instanceof HTMLSelectElement ? activeInput.value === "true" : true,
      })
        .then(() => {
          requestRefresh();
          setStatusMessage(`Template gespeichert: ${templateId}`, "ok");
        })
        .catch((error: Error) => {
          setStatusMessage(`Template konnte nicht gespeichert werden: ${error.message}`, "error");
        });
    };

    const handleTemplatesClick = (event: Event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const action = String(target.dataset.action || "").trim();
      if (!action) {
        return;
      }
      const row = target.closest("tr[data-template-id]");
      if (!(row instanceof HTMLElement)) {
        return;
      }

      const templateId = String(row.dataset.templateId || "").trim();
      if (!templateId) {
        return;
      }

      if (action === "generate-template") {
        const periodInput = row.querySelector('[data-field="period_key"]');
        const periodKey = periodInput instanceof HTMLInputElement ? periodInput.value || currentPeriodKey() : currentPeriodKey();
        void runBookingTemplateMutation(templateId, { period_key: periodKey, status: "pending" })
          .then((result) => {
            requestRefresh();
            if (result.status === "duplicate") {
              setStatusMessage(`Template ${templateId} (${periodKey}) existiert bereits.`, "info");
              return;
            }
            setStatusMessage(`Template ausgefuehrt: ${templateId} (${periodKey})`, "ok");
          })
          .catch((error: Error) => {
            setStatusMessage(`Template-Run fehlgeschlagen: ${error.message}`, "error");
          });
        return;
      }

      if (action === "generate-template-backfill") {
        const startDateInput = row.querySelector('[data-field="start_date"]');
        const startPeriodKey = startDateInput instanceof HTMLInputElement ? periodKeyFromDateLike(startDateInput.value) : "";
        if (!startPeriodKey) {
          setStatusMessage("Bitte zuerst ein Startdatum im Template setzen.", "error");
          return;
        }

        const endPeriodKey = currentPeriodKey();
        const periods = buildPeriodKeyRange(startPeriodKey, endPeriodKey);
        if (!periods.length) {
          setStatusMessage("Ungueltiger Zeitraum fuer Backfill.", "error");
          return;
        }

        void (async () => {
          let createdCount = 0;
          let duplicateCount = 0;
          for (const periodKey of periods) {
            const result = await runBookingTemplateMutation(templateId, { period_key: periodKey, status: "pending" });
            if (result.status === "created") {
              createdCount += 1;
            } else if (result.status === "duplicate") {
              duplicateCount += 1;
            }
          }
          requestRefresh();
          setStatusMessage(`Template ${templateId}: ${NUMBER_FORMATTER.format(createdCount)} neu, ${NUMBER_FORMATTER.format(duplicateCount)} bereits vorhanden.`, "ok");
        })().catch((error: Error) => {
          setStatusMessage(`Backfill fehlgeschlagen: ${error.message}`, "error");
        });
      }
    };

    const handleCreateAccount = () => {
      const nameInput = panelElement.querySelector("#accountNameInput");
      const providerInput = panelElement.querySelector("#accountProviderInput");
      const activeInput = panelElement.querySelector("#accountActiveInput");

      const name = nameInput instanceof HTMLInputElement ? String(nameInput.value || "").trim() : "";
      if (!name) {
        setStatusMessage("Kontoname ist erforderlich.", "error");
        return;
      }

      void createBookingAccount({
        name,
        provider: providerInput instanceof HTMLInputElement ? String(providerInput.value || "").trim() || null : null,
        is_active: activeInput instanceof HTMLSelectElement ? activeInput.value === "true" : true,
      })
        .then(() => {
          if (nameInput instanceof HTMLInputElement) nameInput.value = "";
          if (providerInput instanceof HTMLInputElement) providerInput.value = "";
          requestRefresh();
          setStatusMessage("Konto angelegt.", "ok");
        })
        .catch((error: Error) => {
          setStatusMessage(`Konto konnte nicht angelegt werden: ${error.message}`, "error");
        });
    };

    const handleAccountsChange = (event: Event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const row = target.closest("tr[data-account-id]");
      if (!(row instanceof HTMLElement)) {
        return;
      }

      const accountId = String(row.dataset.accountId || "").trim();
      const nameInput = row.querySelector('[data-field="name"]');
      const providerInput = row.querySelector('[data-field="provider"]');
      const activeInput = row.querySelector('[data-field="is_active"]');
      const name = nameInput instanceof HTMLInputElement ? String(nameInput.value || "").trim() : "";
      if (!accountId || !name) {
        setStatusMessage("Kontoname ist erforderlich.", "error");
        return;
      }

      void updateBookingAccount(accountId, {
        name,
        provider: providerInput instanceof HTMLInputElement ? String(providerInput.value || "").trim() || null : null,
        is_active: activeInput instanceof HTMLSelectElement ? activeInput.value === "true" : true,
      })
        .then(() => {
          requestRefresh();
          setStatusMessage(`Konto gespeichert: ${accountId}`, "ok");
        })
        .catch((error: Error) => {
          setStatusMessage(`Konto konnte nicht gespeichert werden: ${error.message}`, "error");
        });
    };

    const handleUploadDocument = () => {
      const fileInput = panelElement.querySelector("#bookingDocumentFileInput");
      const transactionInput = panelElement.querySelector("#bookingDocumentTxInput");
      const notesInput = panelElement.querySelector("#bookingDocumentNotesInput");

      const file = fileInput instanceof HTMLInputElement ? fileInput.files?.[0] : null;
      if (!file) {
        setStatusMessage("Bitte zuerst eine Datei auswaehlen.", "error");
        return;
      }

      const form = new FormData();
      form.append("file", file);
      const transactionId = transactionInput instanceof HTMLSelectElement ? String(transactionInput.value || "").trim() : "";
      const notes = notesInput instanceof HTMLInputElement ? String(notesInput.value || "").trim() : "";
      if (transactionId) {
        form.append("transaction_id", transactionId);
      }
      if (notes) {
        form.append("notes", notes);
      }

      void uploadBookingDocument(form)
        .then(() => {
          if (fileInput instanceof HTMLInputElement) fileInput.value = "";
          if (notesInput instanceof HTMLInputElement) notesInput.value = "";
          requestRefresh();
          setStatusMessage("Beleg hochgeladen.", "ok");
        })
        .catch((error: Error) => {
          setStatusMessage(`Beleg-Upload fehlgeschlagen: ${error.message}`, "error");
        });
    };

    const handleTransactionsClick = (event: Event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }

      const actionElement = target.closest<HTMLElement>("[data-action]");
      const action = String(actionElement?.dataset.action || "").trim();
      if (action === "preview-document") {
        event.preventDefault();
        previewModalApi?.open(actionElement?.dataset.url, actionElement?.dataset.filename, actionElement?.dataset.mime);
        return;
      }

      const interactive = target.closest("input, select, button, a, label, textarea");
      if (interactive) {
        return;
      }

      const row = target.closest("tr[data-booking-id]");
      const bookingId = row instanceof HTMLElement ? String(row.dataset.bookingId || "").trim() : "";
      if (!bookingId) {
        return;
      }

      if (bookingsDetailsApi) {
        bookingsDetailsApi.openTransactionById(bookingId);
        return;
      }
    };

    const handleOrdersClick = (event: Event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }

      const actionElement = target.closest<HTMLElement>("[data-action]");
      if (String(actionElement?.dataset.action || "").trim() !== "details") {
        return;
      }

      const row = target.closest("tr[data-marketplace][data-order-id]");
      if (!(row instanceof HTMLElement)) {
        return;
      }

      const marketplace = String(row.dataset.marketplace || "").trim();
      const orderId = String(row.dataset.orderId || "").trim();
      if (!marketplace || !orderId) {
        return;
      }

      if (!orderDetailsApi) {
        setStatusMessage("Order-Details sind aktuell nicht verfuegbar.", "error");
        return;
      }

      orderDetailsApi.open(marketplace, orderId);
    };

    const handleMonthlyInvoicesClick = (event: Event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const row = target.closest("tr[data-invoice-id]");
      if (!(row instanceof HTMLElement)) {
        return;
      }

      const invoiceId = String(row.dataset.invoiceId || "").trim();
      if (!invoiceId) {
        return;
      }

      const actionElement = target.closest<HTMLElement>("[data-action]");
      const action = String(actionElement?.dataset.action || "").trim();

      if (action === "preview-document") {
        event.preventDefault();
        previewModalApi?.open(actionElement?.dataset.url, actionElement?.dataset.filename, actionElement?.dataset.mime);
        return;
      }

      if (action === "delete-invoice") {
        event.preventDefault();
        const confirmed = window.confirm("Sammelrechnung wirklich loeschen?");
        if (!confirmed) {
          return;
        }

        void deleteMonthlyInvoiceMutation(invoiceId)
          .then(() => {
            requestRefresh();
            setStatusMessage("Sammelrechnung geloescht.", "ok");
          })
          .catch((error: Error) => {
            setStatusMessage(`Sammelrechnung konnte nicht geloescht werden: ${error.message}`, "error");
          });
        return;
      }

      if (action === "upload-invoice-doc") {
        event.preventDefault();
        monthlyInvoiceUploadTargetIdRef.current = invoiceId;
        if (monthlyInvoiceUploadInput instanceof HTMLInputElement) {
          monthlyInvoiceUploadInput.value = "";
          monthlyInvoiceUploadInput.click();
        }
        return;
      }

      const interactive = target.closest("input, select, button, a, label, textarea");
      if (interactive) {
        return;
      }

      if (bookingsDetailsApi) {
        bookingsDetailsApi.openMonthlyInvoiceById(invoiceId);
        return;
      }
    };

    const handleDocumentsClick = (event: Event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }

      const actionElement = target.closest<HTMLElement>("[data-action]");
      if (String(actionElement?.dataset.action || "").trim() !== "preview-document") {
        return;
      }

      event.preventDefault();
      previewModalApi?.open(actionElement?.dataset.url, actionElement?.dataset.filename, actionElement?.dataset.mime);
    };

    const handleMonthlyInvoiceUploadChange = () => {
      const invoiceId = String(monthlyInvoiceUploadTargetIdRef.current || "").trim();
      const file = monthlyInvoiceUploadInput instanceof HTMLInputElement ? monthlyInvoiceUploadInput.files?.[0] || null : null;
      monthlyInvoiceUploadTargetIdRef.current = "";
      if (monthlyInvoiceUploadInput instanceof HTMLInputElement) {
        monthlyInvoiceUploadInput.value = "";
      }
      if (!invoiceId || !file) {
        return;
      }

      const form = new FormData();
      form.append("file", file);

      void uploadBookingDocument(form)
        .then((uploadResult) => {
          const documentId = String(uploadResult.document?.id || "").trim();
          if (!documentId) {
            throw new Error("Keine Dokument-ID erhalten.");
          }
          return updateMonthlyInvoice(invoiceId, { document_id: documentId });
        })
        .then(() => {
          requestRefresh();
          setStatusMessage("Beleg erfolgreich hochgeladen und verknuepft.", "ok");
        })
        .catch((error: Error) => {
          setStatusMessage(`Beleg-Upload fehlgeschlagen: ${error.message}`, "error");
        });
    };

    createBookingButton?.addEventListener("click", handleCreateBookingTransaction);
    createTemplateButton?.addEventListener("click", handleCreateTemplate);
    createAccountButton?.addEventListener("click", handleCreateAccount);
    uploadDocumentButton?.addEventListener("click", handleUploadDocument);
    roots.transactions?.addEventListener("click", handleTransactionsClick);
    roots.orders?.addEventListener("click", handleOrdersClick);
    roots.transactions?.addEventListener("change", handleTransactionsChange);
    roots.monthlyInvoices?.addEventListener("click", handleMonthlyInvoicesClick);
    roots.documents?.addEventListener("click", handleDocumentsClick);
    monthlyInvoiceUploadInput?.addEventListener("change", handleMonthlyInvoiceUploadChange);
    roots.templates?.addEventListener("click", handleTemplatesClick);
    roots.templates?.addEventListener("change", handleTemplatesChange);
    roots.accounts?.addEventListener("change", handleAccountsChange);

    return () => {
      createBookingButton?.removeEventListener("click", handleCreateBookingTransaction);
      createTemplateButton?.removeEventListener("click", handleCreateTemplate);
      createAccountButton?.removeEventListener("click", handleCreateAccount);
      uploadDocumentButton?.removeEventListener("click", handleUploadDocument);
      roots.transactions?.removeEventListener("click", handleTransactionsClick);
      roots.orders?.removeEventListener("click", handleOrdersClick);
      roots.transactions?.removeEventListener("change", handleTransactionsChange);
      roots.monthlyInvoices?.removeEventListener("click", handleMonthlyInvoicesClick);
      roots.documents?.removeEventListener("click", handleDocumentsClick);
      monthlyInvoiceUploadInput?.removeEventListener("change", handleMonthlyInvoiceUploadChange);
      roots.templates?.removeEventListener("click", handleTemplatesClick);
      roots.templates?.removeEventListener("change", handleTemplatesChange);
      roots.accounts?.removeEventListener("change", handleAccountsChange);
    };
  }, [bookingsDetailsApi, orderDetailsApi, panelElement, previewModalApi, roots.accounts, roots.documents, roots.monthlyInvoices, roots.orders, roots.templates, roots.transactions]);

  const accountOptions = useMemo<OptionItem[]>(() => data.bookingAccounts, [data.bookingAccounts]);
  const bookingLegendContent = useMemo(() => {
    const counters = {
      sale: 0,
      fee: 0,
      cogs: 0,
      invoice: 0,
      subscription: 0,
      refund: 0,
      other: 0,
    };

    data.bookingsAllItems.forEach((booking) => {
      const key = bookingTxCategoryMetaForType(booking.type).key;
      counters[key] += 1;
    });

    return (
      <>
        <span className={`tx-legend-item${bookingCategory ? "" : " active"}`} data-filter-category="">
          <span className="badge badge-default">Gesamt</span>
          <span className="tx-legend-count">{NUMBER_FORMATTER.format(data.bookingsAllItems.length)}</span>
        </span>
        {(["sale", "fee", "cogs", "invoice", "subscription", "refund", "other"] as const).map((key) => {
          const meta = BOOKING_TX_CATEGORY_META[key];
          return (
            <span key={key} className={`tx-legend-item${bookingCategory === key ? " active" : ""}`} data-filter-category={key}>
              <span className={`badge ${meta.badgeClass}`}>{meta.label}</span>
              <span className="tx-legend-count">{NUMBER_FORMATTER.format(counters[key])}</span>
            </span>
          );
        })}
      </>
    );
  }, [bookingCategory, data.bookingsAllItems]);
  const createBookingOrderOptions = useMemo(() => renderOrderOptions(data.bookkeepingLedgerOrders), [data.bookkeepingLedgerOrders]);
  const createBookingAccountOptions = useMemo(() => renderAccountOptions(accountOptions), [accountOptions]);
  const createBookingTemplateOptions = useMemo(() => renderTemplateOptions(data.bookingTemplates), [data.bookingTemplates]);
  const templateAccountOptions = useMemo(() => renderAccountOptions(accountOptions), [accountOptions]);
  const bookingDocumentTransactionOptions = useMemo(() => renderTransactionOptions(data.bookings), [data.bookings]);

  const sammelMonthGridContent = useMemo(() => {
    const selectedDate = monthDateFromToken(monthlyInvoiceDraft.monthToken);
    const selectedYear = selectedDate?.getFullYear() || -1;
    const selectedMonthIndex = selectedDate?.getMonth() ?? -1;
    const today = new Date();
    const currentYear = today.getFullYear();
    const currentMonthIndex = today.getMonth();

    return Array.from({ length: 12 }, (_, monthIndex) => {
      const token = `${sammelPickerYear}-${String(monthIndex + 1).padStart(2, "0")}`;
      const className = [
        "menu-item",
        "sammel-month-btn",
        selectedYear === sammelPickerYear && selectedMonthIndex === monthIndex ? "active" : "",
        currentYear === sammelPickerYear && currentMonthIndex === monthIndex ? "today" : "",
      ].filter(Boolean).join(" ");
      return (
        <button key={token} className={className} type="button" data-month={token}>
          {new Date(sammelPickerYear, monthIndex, 1).toLocaleDateString("de-DE", { month: "short" })}
        </button>
      );
    });
  }, [monthlyInvoiceDraft.monthToken, sammelPickerYear]);

  const monthlyInvoicePreviewContent = useMemo(() => {
    if (!monthlyInvoicePreview.visible) {
      return null;
    }
    if (monthlyInvoicePreview.loading) {
      return <div className="sammel-preview-header">Erwartete Gebuehren werden geladen...</div>;
    }

    const transactions = Array.isArray(monthlyInvoicePreview.transactions) ? monthlyInvoicePreview.transactions : [];
    const totalText = formatMoneyFromCents(monthlyInvoicePreview.totalCents);

    return (
      <>
        <div className="sammel-preview-header">
          {"Erwartete Gebuehren: "}
          <span className="sammel-preview-sum">{totalText}</span>
          {" "}
          <span className="sammel-preview-count">({`${NUMBER_FORMATTER.format(monthlyInvoicePreview.transactionCount)} Transaktion${monthlyInvoicePreview.transactionCount === 1 ? "" : "en"}`})</span>
        </div>
        {transactions.length ? (
          <div className="sammel-preview-table">
            <table>
              <thead>
                <tr>
                  <th>Datum</th>
                  <th>Betrag</th>
                  <th>Referenz</th>
                </tr>
              </thead>
              <tbody>
                {transactions.map((transaction, index) => (
                  <tr key={`${String(transaction?.id || "tx")}:${index}`}>
                    <td>{formatDate(transaction?.date)}</td>
                    <td>{formatMoneyFromCents(Number(transaction?.amount_gross || 0))}</td>
                    <td className="cell-ref">{detailText(transaction?.reference)}</td>
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr>
                  <td><strong>Summe</strong></td>
                  <td><strong>{totalText}</strong></td>
                  <td></td>
                </tr>
              </tfoot>
            </table>
          </div>
        ) : null}
      </>
    );
  }, [monthlyInvoicePreview]);

  const transactionsContent = (
    <>
      <div className="table-head">
        <h3 className="table-title" style={{ fontSize: "0.98rem" }}>Transaktionen</h3>
        <div className="table-meta">{`${NUMBER_FORMATTER.format(data.bookingsTotal)} Zeilen`}</div>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Datum</th>
              <th>Typ</th>
              <th>Provider</th>
              <th>Richtung</th>
              <th>Betrag</th>
              <th>Referenz</th>
              <th>Notiz</th>
              <th>Konto</th>
              <th>Beleg</th>
            </tr>
          </thead>
          <tbody>
            {data.bookings.length ? data.bookings.map((booking, index) => {
              const docUrl = booking.document_id ? `/api/bookings/documents/${encodeURIComponent(String(booking.document_id || ""))}/download` : "";
              const documentName = String(booking.document?.original_filename || booking.document_id || "Beleg");
              const documentMimeType = String(booking.document?.mime_type || "");
              return (
                <tr key={`${String(booking.id || "booking")}:${index}`} data-booking-id={String(booking.id || "")}> 
                  <td>{formatDateTime(booking.date)}</td>
                  <td>{String(booking.type || "-")}</td>
                  <td>{String(booking.provider || "-")}</td>
                  <td>{String(booking.direction || "-")}</td>
                  <td>{formatMoneyFromCents(Number(booking.amount_gross || 0))}</td>
                  <td>{String(booking.reference || "-")}</td>
                  <td>{String(booking.notes || "-")}</td>
                  <td>
                    <select className="booking-select" data-field="payment_account_id" defaultValue={String(booking.payment_account_id || "")}>{renderAccountOptions(accountOptions, String(booking.payment_account_id || ""))}</select>
                  </td>
                  <td>
                    {docUrl ? <DocumentActions url={docUrl} filename={documentName} mimeType={documentMimeType} /> : "-"}
                  </td>
                </tr>
              );
            }) : <tr><td colSpan={9}>Keine Buchungen gefunden.</td></tr>}
          </tbody>
        </table>
      </div>
    </>
  );

  const monthlyInvoicesContent = (
    <>
      <div className="table-head">
        <h3 className="table-title" style={{ fontSize: "0.98rem" }}>Sammelrechnungen</h3>
        <div className="table-meta">{`${NUMBER_FORMATTER.format(data.monthlyInvoicesTotal)} Zeilen`}</div>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Provider</th>
              <th>Zeitraum</th>
              <th>Rechnungsbetrag</th>
              <th>Berechnete Summe</th>
              <th>Differenz</th>
              <th>Status</th>
              <th>Beleg</th>
              <th>Notiz</th>
              <th>Aktion</th>
            </tr>
          </thead>
          <tbody>
            {data.monthlyInvoices.length ? data.monthlyInvoices.map((invoice, index) => {
              const docUrl = invoice.document_id ? `/api/bookings/documents/${encodeURIComponent(String(invoice.document_id || ""))}/download` : "";
              const diff = Number(invoice.difference_cents || 0);
              const documentName = String(invoice.document?.original_filename || invoice.document_id || "Beleg");
              const documentMimeType = String(invoice.document?.mime_type || "");
              return (
                <tr key={`${String(invoice.id || "invoice")}:${index}`} data-invoice-id={String(invoice.id || "") }>
                  <td>{String(invoice.provider || "-")}</td>
                  <td>{`${formatDateTime(invoice.period_from)} - ${formatDateTime(invoice.period_to)}`}</td>
                  <td>{formatMoneyFromCents(Number(invoice.invoice_amount_cents || 0))}</td>
                  <td>{formatMoneyFromCents(Number(invoice.calculated_sum_cents || 0))}</td>
                  <td className={diff !== 0 ? "value-neg" : "value-pos"}>{formatMoneyFromCents(diff)}</td>
                  <td>{String(invoice.status || "draft")}</td>
                  <td>{docUrl ? <DocumentActions url={docUrl} filename={documentName} mimeType={documentMimeType} /> : <button className="btn-inline ghost" data-action="upload-invoice-doc" type="button">Hochladen</button>}</td>
                  <td>{String(invoice.notes || "-")}</td>
                  <td><button className="btn-inline danger" data-action="delete-invoice" type="button">Loeschen</button></td>
                </tr>
              );
            }) : <tr><td colSpan={9}>Keine Sammelrechnungen vorhanden.</td></tr>}
          </tbody>
        </table>
      </div>
    </>
  );

  const ordersContent = (
    <section className="card table-card">
      <div className="table-head">
        <h3 className="table-title" style={{ fontSize: "0.98rem" }}>Bestellungen mit Kostenaufschluesselung</h3>
        <div className="table-meta">{`${NUMBER_FORMATTER.format(data.bookingOrdersTotal)} Zeilen`}</div>
      </div>
      <div className="table-wrap">
        <table className="bookings-orders-table">
          <thead>
            <tr>
              <th>Datum</th>
              <th>Channel</th>
              <th>Order</th>
              <th>Kunde</th>
              <th>Einnahmen</th>
              <th>Kosten</th>
              <th>Gewinn</th>
              <th>Belege</th>
              <th>Aktion</th>
            </tr>
          </thead>
          <tbody>
            {data.bookingOrders.length ? data.bookingOrders.map((order, index) => (
              <tr key={`${String(order.marketplace || "market")}:${String(order.order_id || "order")}:${index}`} data-marketplace={String(order.marketplace || "")} data-order-id={String(order.order_id || "")}> 
                <td>{formatDateTime(order.order_date)}</td>
                <td>{String(order.marketplace || "-")}</td>
                <td>{String(order.external_order_id || order.order_id || "-")}</td>
                <td>{String(order.customer || "-")}</td>
                <td><div><strong>{formatMoneyFromCents(Number(order.revenue_cents || 0))}</strong></div><div className="cell-sub">{`Buchungs-In: ${formatMoneyFromCents(Number(order.bookkeeping_income_cents || 0))}`}</div></td>
                <td><div><strong>{formatMoneyFromCents(Number(order.total_costs_cents || 0))}</strong></div><div className="cell-sub">{`Fees: ${formatMoneyFromCents(Number(order.fees_cents || 0))}`}</div><div className="cell-sub">{`Einkauf: ${formatMoneyFromCents(Number(order.purchase_cents || 0))}`}</div><div className="cell-sub">{`Zusatz-Buchungen: ${formatMoneyFromCents(Number(order.bookkeeping_expense_cents || 0))}`}</div></td>
                <td className={Number(order.profit_cents || 0) < 0 ? "value-neg" : "value-pos"}><div><strong>{formatMoneyFromCents(Number(order.profit_cents || 0))}</strong></div><div className="cell-sub">{`Match: ${String(order.bookkeeping_matched_via || "none")}`}</div></td>
                <td>{NUMBER_FORMATTER.format(Number(order.documents_count || 0))}</td>
                <td><button className="btn-inline" data-action="details" type="button">Details</button></td>
              </tr>
            )) : <tr><td colSpan={9}>Keine Bestellungen fuer aktuellen Filter.</td></tr>}
          </tbody>
        </table>
      </div>
    </section>
  );

  const templatesContent = (
    <section className="card table-card">
      <div className="table-head">
        <h3 className="table-title" style={{ fontSize: "0.98rem" }}>Templates</h3>
        <div className="table-meta">{`${NUMBER_FORMATTER.format(data.bookingTemplatesTotal)} Zeilen`}</div>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Typ</th>
              <th>Richtung</th>
              <th>Gegenpartei</th>
              <th>Start</th>
              <th>Betrag</th>
              <th>Intervall</th>
              <th>Konto</th>
              <th>Aktiv</th>
              <th>Generieren</th>
            </tr>
          </thead>
          <tbody>
            {data.bookingTemplates.length ? data.bookingTemplates.map((template, index) => (
              <tr key={`${String(template.id || "template")}:${index}`} data-template-id={String(template.id || "")}> 
                <td><input className="booking-input notes" data-field="name" defaultValue={String(template.name || "")} /></td>
                <td>{String(template.type || "-")}</td>
                <td>{String(template.direction || "-")}</td>
                <td><input className="booking-input" data-field="counterparty_name" defaultValue={String(template.counterparty_name || "")} /></td>
                <td><input className="booking-input" data-field="start_date" type="date" defaultValue={String(template.start_date || "").slice(0, 10)} /></td>
                <td><input className="booking-input" data-field="default_amount_eur" type="number" step="0.01" min="0.01" defaultValue={centsToInputValue(Number(template.default_amount_gross || 0))} /></td>
                <td><select className="booking-select" data-field="schedule" defaultValue={String(template.schedule || "monthly")}><option value="monthly">monthly</option><option value="quarterly">quarterly</option><option value="yearly">yearly</option></select></td>
                <td><select className="booking-select" data-field="payment_account_id" defaultValue={String(template.payment_account_id || "")}>{renderAccountOptions(accountOptions, String(template.payment_account_id || ""))}</select></td>
                <td><select className="booking-select" data-field="active" defaultValue={template.active ? "true" : "false"}><option value="true">true</option><option value="false">false</option></select></td>
                <td><div className="inline-note"><input className="booking-input" data-field="period_key" type="month" defaultValue={String(template.start_date || "").slice(0, 7)} /><button className="btn-inline" data-action="generate-template" type="button">Run</button><button className="btn-inline ghost" data-action="generate-template-backfill" type="button">Seit Start</button></div></td>
              </tr>
            )) : <tr><td colSpan={10}>Keine Templates vorhanden.</td></tr>}
          </tbody>
        </table>
      </div>
    </section>
  );

  const accountsContent = (
    <section className="card table-card">
      <div className="table-head">
        <h3 className="table-title" style={{ fontSize: "0.98rem" }}>Konten</h3>
        <div className="table-meta">{`${NUMBER_FORMATTER.format(data.bookingAccountsTotal)} Zeilen`}</div>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Provider</th>
              <th>Aktiv</th>
            </tr>
          </thead>
          <tbody>
            {data.bookingAccounts.length ? data.bookingAccounts.map((account, index) => (
              <tr key={`${String(account.id || "account")}:${index}`} data-account-id={String(account.id || "")}> 
                <td><input className="booking-input notes" data-field="name" defaultValue={String(account.name || "")} /></td>
                <td><input className="booking-input" data-field="provider" defaultValue={String(account.provider || "")} /></td>
                <td><select className="booking-select" data-field="is_active" defaultValue={account.is_active ? "true" : "false"}><option value="true">true</option><option value="false">false</option></select></td>
              </tr>
            )) : <tr><td colSpan={3}>Keine Konten vorhanden.</td></tr>}
          </tbody>
        </table>
      </div>
    </section>
  );

  const documentsContent = (
    <section className="card table-card">
      <div className="table-head">
        <h3 className="table-title" style={{ fontSize: "0.98rem" }}>Belege</h3>
        <div className="table-meta">{`${NUMBER_FORMATTER.format(data.bookingDocumentsTotal)} Zeilen`}</div>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Upload</th>
              <th>Originalname</th>
              <th>Gespeichert</th>
              <th>Verknuepfungen</th>
              <th>Aktion</th>
            </tr>
          </thead>
          <tbody>
            {data.bookingDocuments.length ? data.bookingDocuments.map((document, index) => {
              const url = `/api/bookings/documents/${encodeURIComponent(String(document.id || ""))}/download`;
              const filename = String(document.original_filename || document.stored_filename || document.id || "Beleg");
              const mimeType = String(document.mime_type || "");
              return (
                <tr key={`${String(document.id || "document")}:${index}`}>
                  <td>{formatDateTime(document.uploaded_at)}</td>
                  <td>{String(document.original_filename || "-")}</td>
                  <td>{String(document.stored_filename || "-")}</td>
                  <td>{NUMBER_FORMATTER.format(Number(document._count?.transactions || 0))}</td>
                  <td><DocumentActions url={url} filename={filename} mimeType={mimeType} /></td>
                </tr>
              );
            }) : <tr><td colSpan={5}>Keine Belege vorhanden.</td></tr>}
          </tbody>
        </table>
      </div>
    </section>
  );

  return (
    <>
      {ui.bookingTxLegend instanceof HTMLElement ? createPortal(bookingLegendContent, ui.bookingTxLegend) : null}
      {ui.createBookingOrder instanceof HTMLElement ? createPortal(createBookingOrderOptions, ui.createBookingOrder) : null}
      {ui.createBookingAccount instanceof HTMLElement ? createPortal(createBookingAccountOptions, ui.createBookingAccount) : null}
      {ui.createBookingTemplate instanceof HTMLElement ? createPortal(createBookingTemplateOptions, ui.createBookingTemplate) : null}
      {ui.templateAccountInput instanceof HTMLElement ? createPortal(templateAccountOptions, ui.templateAccountInput) : null}
      {ui.bookingDocumentTxInput instanceof HTMLElement ? createPortal(bookingDocumentTransactionOptions, ui.bookingDocumentTxInput) : null}
      {roots.transactions instanceof HTMLElement ? createPortal(transactionsContent, roots.transactions) : null}
      {roots.monthlyInvoices instanceof HTMLElement ? createPortal(monthlyInvoicesContent, roots.monthlyInvoices) : null}
      {roots.orders instanceof HTMLElement ? createPortal(ordersContent, roots.orders) : null}
      {roots.templates instanceof HTMLElement ? createPortal(templatesContent, roots.templates) : null}
      {roots.accounts instanceof HTMLElement ? createPortal(accountsContent, roots.accounts) : null}
      {roots.documents instanceof HTMLElement ? createPortal(documentsContent, roots.documents) : null}
      {ui.sammelMonthGrid instanceof HTMLElement ? createPortal(sammelMonthGridContent, ui.sammelMonthGrid) : null}
      {ui.sammelPreview instanceof HTMLElement && monthlyInvoicePreviewContent ? createPortal(monthlyInvoicePreviewContent, ui.sammelPreview) : null}
      <input id="bookingMonthlyInvoiceUploadInput" type="file" accept=".pdf,.png,.jpg,.jpeg,.webp" style={{ display: "none" }} />
    </>
  );
}
