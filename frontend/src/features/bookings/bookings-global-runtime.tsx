import { useDashboardRuntime, type BookingsRefreshDetail } from "@/app/dashboard-runtime";
import { formatMoneyFromCents, NUMBER_FORMATTER } from "@/features/analytics/format";
import {
  deleteBookingTransaction,
  deleteMonthlyInvoice as deleteMonthlyInvoiceMutation,
  fetchBookingAccounts,
  fetchBookingTemplates,
  fetchBookingTransactionDetail,
  fetchMonthlyInvoiceDetail,
  type BookingRow,
  type BookingTemplateRow,
  type MonthlyInvoiceRow,
  type OptionItem,
  updateBookingTransaction,
  updateMonthlyInvoice,
  uploadBookingDocument,
} from "@/features/bookings/api";
import { buildDashboardApiUrl } from "@/shared/runtime/base-path";
import { createPortal } from "react-dom";
import { type MouseEvent as ReactMouseEvent, useCallback, useEffect, useMemo, useRef, useState } from "react";

type StatusLevel = "info" | "ok" | "error";

type BookingsDetailsMode = "" | "booking-transaction" | "monthly-invoice";

type BookingsDetailsState = {
  isOpen: boolean;
  mode: BookingsDetailsMode;
  loading: boolean;
  error: string;
  revision: number;
  title: string;
  transactionId: string;
  invoiceId: string;
  returnToInvoiceId: string;
  transaction: BookingRow | null;
  invoice: MonthlyInvoiceRow | null;
};

type BookingsGlobalRuntimeProps = {
  registerDetailApis?: boolean;
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
  SHIPPING: "fee",
  COGS: "cogs",
  EXPENSE: "invoice",
  SUBSCRIPTION: "subscription",
  REFUND: "refund",
  PAYOUT: "other",
  ADJUSTMENT: "other",
};

const BOOKING_TX_TYPE_OPTIONS = ["SALE", "COGS", "FEE", "SHIPPING", "SUBSCRIPTION", "EXPENSE", "REFUND", "PAYOUT", "ADJUSTMENT"];
const BOOKING_TX_DIRECTION_OPTIONS = ["IN", "OUT"];
const BOOKING_TX_STATUS_OPTIONS = ["pending", "confirmed", "reconciled"];

const SAMMELRECHNUNG_PROVIDERS: Record<string, string> = {
  paypal: "PayPal Fees",
  shopify_payments: "Shopify Payments Fees",
  kaufland: "Kaufland Fees",
  google_ads: "Google Ads",
  ebay: "eBay Fees",
};

function defaultDetailsState(): BookingsDetailsState {
  return {
    isOpen: false,
    mode: "",
    loading: false,
    error: "",
    revision: 0,
    title: "Order Details",
    transactionId: "",
    invoiceId: "",
    returnToInvoiceId: "",
    transaction: null,
    invoice: null,
  };
}

function setStatusMessage(message: string, level: StatusLevel = "info") {
  const className = level === "error" ? "status-error" : level === "ok" ? "status-ok" : "status-info";
  const statusBox = document.getElementById("statusBox");
  if (statusBox instanceof HTMLElement) {
    statusBox.className = `status ${className}`;
    statusBox.textContent = message;
    statusBox.style.display = "";
  }
}

function normalizeBookingType(value: string | undefined) {
  return String(value || "").trim().toUpperCase();
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

function renderAccountOptions(options: OptionItem[]) {
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

function renderTemplateOptions(options: BookingTemplateRow[]) {
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
  const downloadUrl = buildDashboardApiUrl(`/api/bookings/documents/${encodeURIComponent(documentId)}/download`);
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

export function BookingsGlobalRuntime({ registerDetailApis = true }: BookingsGlobalRuntimeProps) {
  const {
    bookingsRefreshDetail,
    detailsModalApi,
    orderDetailsApi,
    previewModalApi,
    registerBookingsDetailsApi,
    requestBookingsRefresh,
  } = useDashboardRuntime();
  const [detailsState, setDetailsState] = useState<BookingsDetailsState>(() => defaultDetailsState());
  const [detailBookingAccounts, setDetailBookingAccounts] = useState<OptionItem[]>([]);
  const [detailBookingTemplates, setDetailBookingTemplates] = useState<BookingTemplateRow[]>([]);
  const detailsRequestIdRef = useRef(0);
  const detailsStateRef = useRef(detailsState);
  const transactionReturnToInvoiceRef = useRef<Record<string, string>>({});

  useEffect(() => {
    detailsStateRef.current = detailsState;
  }, [detailsState]);

  const emitBookingsRefresh = useCallback((detail?: BookingsRefreshDetail) => {
    requestBookingsRefresh(detail);
  }, [requestBookingsRefresh]);

  const openDetailsModal = useCallback((title: string) => {
    if (detailsModalApi) {
      detailsModalApi.open(title);
      return;
    }
    const modal = document.getElementById("detailsModal");
    const titleElement = document.getElementById("detailsTitle");
    if (titleElement instanceof HTMLElement) {
      titleElement.textContent = title;
    }
    if (modal instanceof HTMLElement) {
      modal.classList.add("active");
      modal.setAttribute("aria-hidden", "false");
    }
  }, [detailsModalApi]);

  const closeDetailsModalShell = useCallback(() => {
    if (detailsModalApi) {
      detailsModalApi.close();
      return;
    }
    const modal = document.getElementById("detailsModal");
    if (modal instanceof HTMLElement) {
      modal.classList.remove("active");
      modal.setAttribute("aria-hidden", "true");
    }
  }, [detailsModalApi]);

  const openTransactionDetailById = useCallback(async (transactionId?: string, options?: { returnToInvoiceId?: string }) => {
    const txId = String(transactionId || "").trim();
    if (!txId) {
      return;
    }

    const inheritedReturnToInvoiceId = String(options?.returnToInvoiceId || transactionReturnToInvoiceRef.current[txId] || "").trim();
    delete transactionReturnToInvoiceRef.current[txId];

    const requestId = detailsRequestIdRef.current + 1;
    detailsRequestIdRef.current = requestId;
    const title = `Transaktion ${txId}`;
    openDetailsModal(title);
    setDetailsState({
      isOpen: true,
      mode: "booking-transaction",
      loading: true,
      error: "",
      revision: 0,
      title,
      transactionId: txId,
      invoiceId: "",
      returnToInvoiceId: inheritedReturnToInvoiceId,
      transaction: null,
      invoice: null,
    });

    try {
      const [payload, accountsPayload, templatesPayload] = await Promise.all([
        fetchBookingTransactionDetail(txId),
        fetchBookingAccounts(),
        fetchBookingTemplates(),
      ]);
      if (detailsRequestIdRef.current !== requestId) {
        return;
      }

      const transaction = payload.transaction || null;
      if (!transaction) {
        throw new Error("Transaktion konnte nicht geladen werden.");
      }

      setDetailBookingAccounts(Array.isArray(accountsPayload.items) ? accountsPayload.items : []);
      setDetailBookingTemplates(Array.isArray(templatesPayload.items) ? templatesPayload.items : []);

      const resolvedId = String(transaction.id || txId).trim() || txId;
      const resolvedTitle = `Transaktion ${resolvedId}`;
      setDetailsState({
        isOpen: true,
        mode: "booking-transaction",
        loading: false,
        error: "",
        revision: 1,
        title: resolvedTitle,
        transactionId: resolvedId,
        invoiceId: "",
        returnToInvoiceId: inheritedReturnToInvoiceId,
        transaction,
        invoice: null,
      });
      detailsModalApi?.setTitle(resolvedTitle);
    } catch (error) {
      if (detailsRequestIdRef.current !== requestId) {
        return;
      }
      setDetailsState({
        isOpen: true,
        mode: "booking-transaction",
        loading: false,
        error: error instanceof Error ? error.message : "Transaktionsdetails konnten nicht geladen werden.",
        revision: 0,
        title,
        transactionId: txId,
        invoiceId: "",
        returnToInvoiceId: inheritedReturnToInvoiceId,
        transaction: null,
        invoice: null,
      });
      setStatusMessage(`Transaktionsdetails konnten nicht geladen werden: ${error instanceof Error ? error.message : "Unbekannter Fehler"}`, "error");
    }
  }, [detailsModalApi, openDetailsModal]);

  const openMonthlyInvoiceById = useCallback(async (invoiceId?: string) => {
    const id = String(invoiceId || "").trim();
    if (!id) {
      return;
    }

    const requestId = detailsRequestIdRef.current + 1;
    detailsRequestIdRef.current = requestId;
    const title = "Sammelrechnung";
    openDetailsModal(title);
    setDetailsState({
      isOpen: true,
      mode: "monthly-invoice",
      loading: true,
      error: "",
      revision: 0,
      title,
      transactionId: "",
      invoiceId: id,
      returnToInvoiceId: "",
      transaction: null,
      invoice: null,
    });

    try {
      const payload = await fetchMonthlyInvoiceDetail(id);
      if (detailsRequestIdRef.current !== requestId) {
        return;
      }
      const invoice = payload.invoice || null;
      if (!invoice) {
        throw new Error("Sammelrechnung konnte nicht geladen werden.");
      }
      const providerLabel = SAMMELRECHNUNG_PROVIDERS[String(invoice.provider || "").trim()] || String(invoice.provider || "").trim() || "-";
      const resolvedTitle = `Sammelrechnung – ${providerLabel}`;
      setDetailsState({
        isOpen: true,
        mode: "monthly-invoice",
        loading: false,
        error: "",
        revision: 1,
        title: resolvedTitle,
        transactionId: "",
        invoiceId: String(invoice.id || id).trim() || id,
        returnToInvoiceId: "",
        transaction: null,
        invoice,
      });
      detailsModalApi?.setTitle(resolvedTitle);
    } catch (error) {
      if (detailsRequestIdRef.current !== requestId) {
        return;
      }
      setDetailsState({
        isOpen: true,
        mode: "monthly-invoice",
        loading: false,
        error: error instanceof Error ? error.message : "Sammelrechnung konnte nicht geladen werden.",
        revision: 0,
        title,
        transactionId: "",
        invoiceId: id,
        returnToInvoiceId: "",
        transaction: null,
        invoice: null,
      });
      setStatusMessage(`Sammelrechnung konnte nicht geladen werden: ${error instanceof Error ? error.message : "Unbekannter Fehler"}`, "error");
    }
  }, [detailsModalApi, openDetailsModal]);

  const closeBookingsDetails = useCallback(() => {
    detailsRequestIdRef.current += 1;
    const current = detailsStateRef.current;
    closeDetailsModalShell();
    setDetailsState(defaultDetailsState());
    if (current.mode === "booking-transaction" && current.returnToInvoiceId) {
      void openMonthlyInvoiceById(current.returnToInvoiceId);
      return;
    }
    if (current.mode === "booking-transaction" && orderDetailsApi?.hasPendingReturn()) {
      orderDetailsApi.reopenPendingReturn();
    }
  }, [closeDetailsModalShell, openMonthlyInvoiceById, orderDetailsApi]);

  const requestRefresh = useCallback(() => {
    emitBookingsRefresh();
  }, [emitBookingsRefresh]);

  const saveActiveDetails = useCallback(async (options?: { silent?: boolean }) => {
    const silent = options?.silent === true;
    const current = detailsStateRef.current;

    if (current.mode === "booking-transaction") {
      const txId = String(current.transactionId || "").trim();
      if (!txId) {
        if (!silent) {
          setStatusMessage("Keine Transaktion im Detailfenster aktiv.", "error");
        }
        return;
      }

      const dateElement = document.getElementById("bookingDetailTxDate");
      const amountElement = document.getElementById("bookingDetailTxAmount");
      const providerElement = document.getElementById("bookingDetailTxProvider");
      if (!(dateElement instanceof HTMLInputElement) || !(amountElement instanceof HTMLInputElement) || !(providerElement instanceof HTMLInputElement)) {
        return;
      }

      const dateIso = toIsoFromLocalInput(dateElement.value);
      const amountCents = parseEuroToCents(amountElement.value);
      const provider = String(providerElement.value || "").trim();
      if (!dateIso) {
        if (!silent) {
          setStatusMessage("Datum ist ungueltig.", "error");
        }
        return;
      }
      if (!amountCents) {
        if (!silent) {
          setStatusMessage("Betrag muss groesser 0 sein.", "error");
        }
        return;
      }
      if (!provider) {
        if (!silent) {
          setStatusMessage("Provider ist erforderlich.", "error");
        }
        return;
      }

      const payload: Record<string, unknown> = {
        date: dateIso,
        type: normalizeBookingType((document.getElementById("bookingDetailTxType") as HTMLSelectElement | null)?.value),
        direction: String((document.getElementById("bookingDetailTxDirection") as HTMLSelectElement | null)?.value || "").trim().toUpperCase(),
        amount_gross: amountCents,
        provider,
        status: String((document.getElementById("bookingDetailTxStatus") as HTMLSelectElement | null)?.value || "").trim().toLowerCase() || null,
        reference: String((document.getElementById("bookingDetailTxReference") as HTMLInputElement | null)?.value || "").trim() || null,
        counterparty_name: String((document.getElementById("bookingDetailTxCounterparty") as HTMLInputElement | null)?.value || "").trim() || null,
        category: String((document.getElementById("bookingDetailTxCategory") as HTMLInputElement | null)?.value || "").trim() || null,
        payment_account_id: String((document.getElementById("bookingDetailTxAccount") as HTMLSelectElement | null)?.value || "").trim() || null,
        template_id: String((document.getElementById("bookingDetailTxTemplate") as HTMLSelectElement | null)?.value || "").trim() || null,
        notes: String((document.getElementById("bookingDetailTxNotes") as HTMLTextAreaElement | null)?.value || "").trim() || null,
      };

      const fileInput = document.getElementById("bookingDetailTxDocumentFile");
      const file = fileInput instanceof HTMLInputElement ? fileInput.files?.[0] || null : null;
      if (file) {
        const form = new FormData();
        form.append("file", file);
        form.append("transaction_id", txId);
        await uploadBookingDocument(form);
      }

      const result = await updateBookingTransaction(txId, payload);
      requestRefresh();
      if (!silent) {
        setStatusMessage(`Transaktion gespeichert: ${txId}`, "ok");
      }

      const updated = result.transaction || null;
      if (updated && detailsStateRef.current.mode === "booking-transaction") {
        const resolvedId = String(updated.id || txId).trim() || txId;
        const nextTitle = `Transaktion ${resolvedId}`;
        setDetailsState((previous) => ({
          ...previous,
          loading: false,
          error: "",
          revision: previous.revision + 1,
          title: nextTitle,
          transactionId: resolvedId,
          transaction: updated,
        }));
        detailsModalApi?.setTitle(nextTitle);
      }
      return;
    }

    if (current.mode === "monthly-invoice") {
      const invoiceId = String(current.invoiceId || "").trim();
      if (!invoiceId) {
        if (!silent) {
          setStatusMessage("Keine Sammelrechnung im Detailfenster aktiv.", "error");
        }
        return;
      }

      const providerElement = document.getElementById("sammelDetailProvider");
      const periodFromElement = document.getElementById("sammelDetailPeriodFrom");
      const periodToElement = document.getElementById("sammelDetailPeriodTo");
      const amountElement = document.getElementById("sammelDetailAmount");
      const notesElement = document.getElementById("sammelDetailNotes");
      const fileElement = document.getElementById("sammelDetailFile");
      if (!(providerElement instanceof HTMLSelectElement) || !(periodFromElement instanceof HTMLInputElement) || !(periodToElement instanceof HTMLInputElement) || !(amountElement instanceof HTMLInputElement)) {
        return;
      }

      const provider = String(providerElement.value || "").trim().toLowerCase();
      const periodFrom = String(periodFromElement.value || "").trim();
      const periodTo = String(periodToElement.value || "").trim();
      const amountCents = parseEuroToCents(amountElement.value);
      const notes = notesElement instanceof HTMLTextAreaElement ? String(notesElement.value || "").trim() || null : null;
      if (!provider) {
        if (!silent) {
          setStatusMessage("Provider ist erforderlich.", "error");
        }
        return;
      }
      if (!periodFrom || !periodTo) {
        if (!silent) {
          setStatusMessage("Zeitraum ist erforderlich.", "error");
        }
        return;
      }
      if (!amountCents) {
        if (!silent) {
          setStatusMessage("Rechnungsbetrag muss groesser 0 sein.", "error");
        }
        return;
      }

      const payload: Record<string, unknown> = {
        provider,
        period_from: periodFrom,
        period_to: periodTo,
        invoice_amount_cents: amountCents,
        notes,
      };

      const file = fileElement instanceof HTMLInputElement ? fileElement.files?.[0] || null : null;
      if (file) {
        const form = new FormData();
        form.append("file", file);
        const uploadResult = await uploadBookingDocument(form);
        const documentId = String(uploadResult.document?.id || "").trim();
        if (documentId) {
          payload.document_id = documentId;
        }
      }

      const result = await updateMonthlyInvoice(invoiceId, payload);
      requestRefresh();
      if (!silent) {
        setStatusMessage("Sammelrechnung gespeichert.", "ok");
      }

      const updated = result.invoice || null;
      if (updated && detailsStateRef.current.mode === "monthly-invoice") {
        const providerLabel = SAMMELRECHNUNG_PROVIDERS[String(updated.provider || "").trim()] || String(updated.provider || "").trim() || "-";
        const nextTitle = `Sammelrechnung – ${providerLabel}`;
        setDetailsState((previous) => ({
          ...previous,
          loading: false,
          error: "",
          revision: previous.revision + 1,
          title: nextTitle,
          invoiceId: String(updated.id || invoiceId).trim() || invoiceId,
          invoice: updated,
        }));
        detailsModalApi?.setTitle(nextTitle);
      }
    }
  }, [detailsModalApi, requestRefresh]);

  const deleteActiveDetails = useCallback(async () => {
    const current = detailsStateRef.current;
    if (current.mode === "booking-transaction") {
      const txId = String(current.transactionId || "").trim();
      if (!txId) {
        setStatusMessage("Keine Transaktion im Detailfenster aktiv.", "error");
        return;
      }
      const confirmed = window.confirm("Transaktion wirklich loeschen? Diese Aktion kann nicht rueckgaengig gemacht werden.");
      if (!confirmed) {
        return;
      }
      await deleteBookingTransaction(txId);
      closeBookingsDetails();
      requestRefresh();
      setStatusMessage(`Transaktion geloescht: ${txId}`, "ok");
      return;
    }

    if (current.mode === "monthly-invoice") {
      const invoiceId = String(current.invoiceId || "").trim();
      if (!invoiceId) {
        setStatusMessage("Keine Sammelrechnung im Detailfenster aktiv.", "error");
        return;
      }
      const confirmed = window.confirm("Sammelrechnung wirklich loeschen?");
      if (!confirmed) {
        return;
      }
      await deleteMonthlyInvoiceMutation(invoiceId);
      closeBookingsDetails();
      requestRefresh();
      setStatusMessage("Sammelrechnung geloescht.", "ok");
    }
  }, [closeBookingsDetails, requestRefresh]);

  useEffect(() => {
    if (!registerDetailApis) {
      return;
    }

    registerBookingsDetailsApi({
      openTransactionById: (transactionId, options) => {
        void openTransactionDetailById(transactionId, options);
      },
      openMonthlyInvoiceById: (invoiceId) => {
        void openMonthlyInvoiceById(invoiceId);
      },
      close: closeBookingsDetails,
      saveActive: saveActiveDetails,
      deleteActive: deleteActiveDetails,
      isOpen: () => detailsStateRef.current.isOpen,
      getMode: () => detailsStateRef.current.mode,
    });

    return () => {
      registerBookingsDetailsApi(null);
    };
  }, [closeBookingsDetails, deleteActiveDetails, openMonthlyInvoiceById, openTransactionDetailById, registerBookingsDetailsApi, registerDetailApis, saveActiveDetails]);

  const handleDetailsContentClick = useCallback((event: ReactMouseEvent<HTMLDivElement>) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }

    const actionElement = target.closest<HTMLElement>("[data-action]");
    const action = String(actionElement?.dataset.action || "").trim();
    if (action === "preview-document") {
      event.preventDefault();
      event.stopPropagation();
      previewModalApi?.open(actionElement?.dataset.url, actionElement?.dataset.filename, actionElement?.dataset.mime);
      return;
    }
    if (action === "delete-booking-modal" || action === "delete-invoice-modal") {
      event.preventDefault();
      event.stopPropagation();
      void deleteActiveDetails().catch((error: Error) => {
        setStatusMessage(error.message, "error");
      });
      return;
    }
    if (action === "save-invoice-modal") {
      event.preventDefault();
      event.stopPropagation();
      void saveActiveDetails().catch((error: Error) => {
        setStatusMessage(`Speichern fehlgeschlagen: ${error.message}`, "error");
      });
      return;
    }
    if (action === "open-order") {
      event.preventDefault();
      event.stopPropagation();
      const provider = String(actionElement?.dataset.provider || "").trim().toLowerCase();
      const externalOrderId = String(actionElement?.dataset.externalOrderId || "").trim();
      if (!provider || !externalOrderId) {
        return;
      }
      if (!orderDetailsApi) {
        setStatusMessage("Order-Details sind aktuell nicht verfuegbar.", "error");
        return;
      }
      const transactionId = String(detailsStateRef.current.transactionId || "").trim();
      const returnToInvoiceId = String(detailsStateRef.current.returnToInvoiceId || "").trim();
      if (transactionId && returnToInvoiceId) {
        transactionReturnToInvoiceRef.current[transactionId] = returnToInvoiceId;
      }
      detailsRequestIdRef.current += 1;
      closeDetailsModalShell();
      setDetailsState(defaultDetailsState());
      orderDetailsApi.openByExternalId(provider, externalOrderId, transactionId);
      return;
    }

    const interactive = target.closest("input, select, button, a, label, textarea");
    if (interactive) {
      return;
    }

    if (detailsStateRef.current.mode === "monthly-invoice") {
      const txRow = target.closest("tr[data-tx-id]");
      const txId = txRow instanceof HTMLElement ? String(txRow.dataset.txId || "").trim() : "";
      const invoiceId = String(detailsStateRef.current.invoiceId || "").trim();
      if (txId) {
        event.stopPropagation();
        void openTransactionDetailById(txId, { returnToInvoiceId: invoiceId });
      }
    }
  }, [closeDetailsModalShell, deleteActiveDetails, openTransactionDetailById, orderDetailsApi, previewModalApi, saveActiveDetails]);

  const detailsContent = useMemo(() => {
    if (detailsState.mode === "booking-transaction") {
      if (detailsState.loading) {
        return <div data-react-bookings-details="booking-transaction">Transaktionsdetails werden geladen...</div>;
      }
      if (detailsState.error) {
        return <div data-react-bookings-details="booking-transaction">{detailsState.error}</div>;
      }
      const transaction = detailsState.transaction;
      if (!transaction) {
        return <div data-react-bookings-details="booking-transaction">Keine Transaktion gefunden.</div>;
      }

      const order = transaction.order || null;
      const orderLabel = order
        ? `${detailText(order.provider)} | ${detailText(order.external_order_id)}`
        : "-";
      const categoryMeta = bookingTxCategoryMetaForType(transaction.type);
      const transactionStatus = String(transaction.status || "").trim().toLowerCase();
      const documentId = String(transaction.document_id || "").trim();
      const documentName = String(transaction.document?.original_filename || documentId || "Beleg");
      const documentMimeType = String(transaction.document?.mime_type || "");
      const documentUrl = documentId ? buildDashboardApiUrl(`/api/bookings/documents/${encodeURIComponent(documentId)}/download`) : "";

      return (
        <div
          key={`booking-transaction:${detailsState.transactionId}:${detailsState.revision}`}
          data-react-bookings-details="booking-transaction"
          data-booking-detail-id={detailsState.transactionId}
          onClick={handleDetailsContentClick}
        >
          <section className="detail-grid">
            <article className="detail-card">
              <h3>Transaktionsdaten</h3>
              <div className="detail-kv">
                <DetailRows
                  items={[
                    ["ID", detailsState.transactionId || "-"],
                    ["Datum", formatDate(transaction.date)],
                    ["Betrag", formatMoneyFromCents(Number(transaction.amount_gross || 0))],
                    ["Richtung", transaction.direction || "-"],
                    ["Status", transaction.status || "-"],
                    ["Waehrung", transaction.currency || "EUR"],
                  ]}
                />
              </div>
            </article>
            <article className="detail-card">
              <h3>Klassifikation</h3>
              <div className="detail-kv">
                <div className="detail-row">
                  <span>Typ</span>
                  <strong><span className={`badge ${categoryMeta.badgeClass}`}>{normalizeBookingType(transaction.type) || "-"}</span></strong>
                </div>
                <DetailRows
                  items={[
                    ["Gruppe", categoryMeta.longLabel],
                    ["Provider", transaction.provider || "-"],
                    ["Gegenpartei", transaction.counterparty_name || "-"],
                    ["Kategorie", transaction.category || "-"],
                    ["Referenz", transaction.reference || "-"],
                  ]}
                />
              </div>
            </article>
            <article className="detail-card">
              <h3>Verknuepfungen</h3>
              <div className="detail-kv">
                <div className="detail-row">
                  <span>Order</span>
                  <strong>
                    {order ? (
                      <span
                        className="detail-order-link"
                        data-action="open-order"
                        data-provider={String(order.provider || "")}
                        data-external-order-id={String(order.external_order_id || "")}
                        style={{ cursor: "pointer", textDecoration: "underline", color: "var(--th-accent)" }}
                      >
                        {orderLabel}
                      </span>
                    ) : "-"}
                  </strong>
                </div>
                <DetailRows
                  items={[
                    ["Template", transaction.template?.name || (transaction as BookingRow & { template_id?: string }).template_id || "-"],
                    ["Konto", transaction.payment_account?.name || transaction.payment_account_name || transaction.payment_account_id || "-"],
                    ["Beleg", documentId ? documentName : "-"],
                    ["Source", transaction.source || "-"],
                    ["Source Key", transaction.source_key || "-"],
                  ]}
                />
                {documentUrl ? <div><DocumentActions url={documentUrl} filename={documentName} mimeType={documentMimeType} /></div> : null}
              </div>
            </article>
          </section>

          <section className="booking-detail-form">
            <h3>Transaktion bearbeiten</h3>
            <div className="booking-detail-form-grid">
              <div className="control">
                <label htmlFor="bookingDetailTxDate">Datum</label>
                <input id="bookingDetailTxDate" type="datetime-local" defaultValue={toLocalInputFromIso(transaction.date)} />
              </div>
              <div className="control">
                <label htmlFor="bookingDetailTxType">Typ</label>
                <select id="bookingDetailTxType" defaultValue={normalizeBookingType(transaction.type) || "SALE"}>
                  {BOOKING_TX_TYPE_OPTIONS.map((value) => <option key={value} value={value}>{value}</option>)}
                </select>
              </div>
              <div className="control">
                <label htmlFor="bookingDetailTxDirection">Richtung</label>
                <select id="bookingDetailTxDirection" defaultValue={String(transaction.direction || "").trim().toUpperCase() || "IN"}>
                  {BOOKING_TX_DIRECTION_OPTIONS.map((value) => <option key={value} value={value}>{value}</option>)}
                </select>
              </div>
              <div className="control">
                <label htmlFor="bookingDetailTxAmount">Betrag (EUR)</label>
                <input id="bookingDetailTxAmount" type="number" step="0.01" min="0.01" defaultValue={centsToInputValue(Number(transaction.amount_gross || 0))} />
              </div>
              <div className="control">
                <label htmlFor="bookingDetailTxProvider">Provider</label>
                <input id="bookingDetailTxProvider" type="text" defaultValue={String(transaction.provider || "")} />
              </div>
              <div className="control">
                <label htmlFor="bookingDetailTxStatus">Status</label>
                <select id="bookingDetailTxStatus" defaultValue={BOOKING_TX_STATUS_OPTIONS.includes(transactionStatus) ? transactionStatus : ""}>
                  <option value="">-</option>
                  {BOOKING_TX_STATUS_OPTIONS.map((value) => <option key={value} value={value}>{value}</option>)}
                </select>
              </div>
              <div className="control">
                <label htmlFor="bookingDetailTxReference">Referenz</label>
                <input id="bookingDetailTxReference" type="text" defaultValue={String(transaction.reference || "")} />
              </div>
              <div className="control">
                <label htmlFor="bookingDetailTxCounterparty">Gegenpartei</label>
                <input id="bookingDetailTxCounterparty" type="text" defaultValue={String(transaction.counterparty_name || "")} />
              </div>
              <div className="control">
                <label htmlFor="bookingDetailTxCategory">Kategorie</label>
                <input id="bookingDetailTxCategory" type="text" defaultValue={String(transaction.category || "")} />
              </div>
              <div className="control">
                <label htmlFor="bookingDetailTxAccount">Konto</label>
                <select id="bookingDetailTxAccount" defaultValue={String(transaction.payment_account_id || "")}>{renderAccountOptions(detailBookingAccounts)}</select>
              </div>
              <div className="control">
                <label htmlFor="bookingDetailTxTemplate">Template</label>
                <select id="bookingDetailTxTemplate" defaultValue={String((transaction as BookingRow & { template_id?: string }).template_id || "")}>{renderTemplateOptions(detailBookingTemplates)}</select>
              </div>
              <div className="control">
                <label htmlFor="bookingDetailTxDocument">Beleg hochladen</label>
                <input id="bookingDetailTxDocumentFile" type="file" accept=".pdf,.png,.jpg,.jpeg,.webp,.txt,.csv,.zip,.doc,.docx" />
              </div>
              <div className="control" style={{ gridColumn: "1 / -1" }}>
                <label htmlFor="bookingDetailTxNotes">Notiz</label>
                <textarea id="bookingDetailTxNotes" className="booking-input notes" rows={3} defaultValue={String(transaction.notes || "")} />
              </div>
            </div>
            <div className="booking-detail-actions">
              <button className="btn-inline danger" data-action="delete-booking-modal" type="button">Transaktion loeschen</button>
            </div>
          </section>

          <BookingTransactionDetailPreview transaction={transaction} />
        </div>
      );
    }

    if (detailsState.mode === "monthly-invoice") {
      if (detailsState.loading) {
        return <div data-react-bookings-details="monthly-invoice">Sammelrechnung wird geladen...</div>;
      }
      if (detailsState.error) {
        return <div data-react-bookings-details="monthly-invoice">{detailsState.error}</div>;
      }
      const invoice = detailsState.invoice;
      if (!invoice) {
        return <div data-react-bookings-details="monthly-invoice">Keine Sammelrechnung gefunden.</div>;
      }

      const providerKey = String(invoice.provider || "").trim();
      const providerLabel = SAMMELRECHNUNG_PROVIDERS[providerKey] || providerKey || "-";
      const invoiceAmount = Number(invoice.invoice_amount_cents || 0);
      const calculatedAmount = Number(invoice.calculated_sum_cents || 0);
      const differenceAmount = Number(invoice.difference_cents || 0);
      const statusLabel = invoice.status === "matched" ? "Matched" : invoice.status === "mismatch" ? "Differenz" : (invoice.status || "Entwurf");
      const statusBadgeClass = invoice.status === "matched" ? "badge-sale" : invoice.status === "mismatch" ? "badge-refund" : "badge-default";
      const documentId = String(invoice.document_id || "").trim();
      const documentName = String(invoice.document?.original_filename || documentId || "Beleg");
      const documentMimeType = String(invoice.document?.mime_type || "");
      const documentUrl = documentId ? buildDashboardApiUrl(`/api/bookings/documents/${encodeURIComponent(documentId)}/download`) : "";
      const linkedTransactions = Array.isArray(invoice.transactions) ? invoice.transactions : [];
      const linkedTransactionSum = linkedTransactions.reduce((sum, tx) => sum + Number(tx.amount_gross || 0), 0);

      return (
        <div
          key={`monthly-invoice:${detailsState.invoiceId}:${detailsState.revision}`}
          className="sammel-detail-shell"
          data-react-bookings-details="monthly-invoice"
          data-invoice-id={detailsState.invoiceId}
          onClick={handleDetailsContentClick}
        >
          <section className="detail-grid">
            <article className="detail-card">
              <h3>Sammelrechnung</h3>
              <div className="detail-kv">
                <DetailRows
                  items={[
                    ["Provider", providerLabel],
                    ["Zeitraum", `${formatDate(invoice.period_from)} - ${formatDate(invoice.period_to)}`],
                    ["Waehrung", invoice.currency || "EUR"],
                    ["Erstellt", formatDate(invoice.created_at)],
                    ["Aktualisiert", formatDate(invoice.updated_at)],
                  ]}
                />
              </div>
            </article>
            <article className="detail-card">
              <h3>Abgleich</h3>
              <div className="detail-kv">
                <div className="detail-row">
                  <span>Status</span>
                  <strong><span className={`badge ${statusBadgeClass}`}>{statusLabel}</span></strong>
                </div>
                <DetailRows
                  items={[
                    ["Rechnungsbetrag", formatMoneyFromCents(invoiceAmount)],
                    ["Berechnete Summe", formatMoneyFromCents(calculatedAmount)],
                    ["Differenz", formatMoneyFromCents(differenceAmount)],
                  ]}
                />
              </div>
            </article>
            <article className="detail-card">
              <h3>Beleg & Notiz</h3>
              <div className="detail-kv">
                <div className="detail-row">
                  <span>Beleg</span>
                  <strong>{documentUrl ? <span className="sammel-edit-current-doc">{documentName} <DocumentActions url={documentUrl} filename={documentName} mimeType={documentMimeType} /></span> : "-"}</strong>
                </div>
                <div className="detail-row">
                  <span>Notiz</span>
                  <strong>{detailText(invoice.notes)}</strong>
                </div>
              </div>
            </article>
          </section>

          <section className="booking-detail-form">
            <h3>Sammelrechnung bearbeiten</h3>
            <div className="booking-detail-form-grid">
              <div className="control">
                <label htmlFor="sammelDetailProvider">Provider</label>
                <select id="sammelDetailProvider" defaultValue={providerKey}>
                  {Object.entries(SAMMELRECHNUNG_PROVIDERS).map(([key, label]) => <option key={key} value={key}>{label}</option>)}
                </select>
              </div>
              <div className="control">
                <label htmlFor="sammelDetailPeriodFrom">Zeitraum von</label>
                <input id="sammelDetailPeriodFrom" type="date" defaultValue={toDateInputValue(invoice.period_from)} />
              </div>
              <div className="control">
                <label htmlFor="sammelDetailPeriodTo">Zeitraum bis</label>
                <input id="sammelDetailPeriodTo" type="date" defaultValue={toDateInputValue(invoice.period_to)} />
              </div>
              <div className="control">
                <label htmlFor="sammelDetailAmount">Rechnungsbetrag (EUR)</label>
                <input id="sammelDetailAmount" type="text" inputMode="decimal" defaultValue={centsToInputValue(invoiceAmount)} />
              </div>
              <div className="control">
                <label>Aktueller Beleg</label>
                <div>{documentUrl ? <span className="sammel-edit-current-doc">{documentName} <DocumentActions url={documentUrl} filename={documentName} mimeType={documentMimeType} /></span> : <span className="sammel-edit-current-doc">Kein Beleg</span>}</div>
              </div>
              <div className="control">
                <label htmlFor="sammelDetailFile">Neuen Beleg hochladen</label>
                <input id="sammelDetailFile" type="file" accept=".pdf,.png,.jpg,.jpeg,.webp" />
              </div>
              <div className="control" style={{ gridColumn: "1 / -1" }}>
                <label htmlFor="sammelDetailNotes">Notiz</label>
                <textarea id="sammelDetailNotes" className="booking-input notes" rows={2} defaultValue={String(invoice.notes || "")} />
              </div>
            </div>
            <div className="booking-detail-actions">
              <button className="btn-inline danger" data-action="delete-invoice-modal" type="button">Sammelrechnung loeschen</button>
              <button className="btn-inline primary" data-action="save-invoice-modal" type="button">Speichern</button>
            </div>
          </section>

          <section className="sammel-detail-transactions">
            <h3>Verknuepfte Transaktionen</h3>
            {linkedTransactions.length ? (
              <div className="table-shell sammel-detail-table">
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
                  <tbody>
                    {linkedTransactions.map((tx, index) => {
                      const txMeta = bookingTxCategoryMetaForType(tx.type);
                      return (
                        <tr key={`${String(tx.id || "tx")}:${index}`} className={txMeta.rowClass} data-tx-id={String(tx.id || "")} style={{ cursor: "pointer" }}>
                          <td>{formatDate(tx.date)}</td>
                          <td><span className={`badge ${txMeta.badgeClass}`}>{normalizeBookingType(tx.type) || "-"}</span></td>
                          <td>{formatMoneyFromCents(Number(tx.amount_gross || 0))}</td>
                          <td>{detailText(tx.counterparty_name)}</td>
                          <td>{detailText(tx.reference)}</td>
                          <td><span className="cell-truncate" title={String(tx.notes || "")}>{detailText(tx.notes)}</span></td>
                        </tr>
                      );
                    })}
                  </tbody>
                  <tfoot>
                    <tr>
                      <td colSpan={2}><strong>{`Summe (${NUMBER_FORMATTER.format(linkedTransactions.length)} Transaktion${linkedTransactions.length === 1 ? "" : "en"})`}</strong></td>
                      <td><strong>{formatMoneyFromCents(linkedTransactionSum)}</strong></td>
                      <td colSpan={3}></td>
                    </tr>
                  </tfoot>
                </table>
              </div>
            ) : <p className="sammel-detail-empty">Keine verknuepften Transaktionen.</p>}
          </section>
        </div>
      );
    }

    return null;
  }, [closeBookingsDetails, deleteActiveDetails, detailBookingAccounts, detailBookingTemplates, detailsState, handleDetailsContentClick, saveActiveDetails]);

  const detailsContentElement = document.getElementById("detailsContent");
  if (!registerDetailApis || !(detailsContentElement instanceof HTMLElement) || !detailsState.isOpen || !detailsContent) {
    return null;
  }

  return createPortal(detailsContent, detailsContentElement);
}
