import { useDeferredValue, useMemo, useState, type ReactNode } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowUpRight, CalendarRange, FileCheck2, FileText, FileUp, Landmark, ReceiptText, TableOfContents, Wallet } from "lucide-react";
import { PageFilterCard } from "@/components/page-filter-card";
import { Button } from "@/components/ui/button";
import { DataTableShell } from "@/components/ui/data-table-shell";
import { FilterChip } from "@/components/ui/filter-chip";
import { SurfaceCard } from "@/components/ui/surface-card";
import type {
  BookingDocument,
  BookingDocumentsPayload,
  BookingLedgerOrdersPayload,
  BookingOrderItem,
  BookingOrdersPayload,
  BookingPaymentAccount,
  BookingPaymentAccountsPayload,
  BookingTemplate,
  BookingTemplatesPayload,
  BookingTransaction,
  BookingTransactionsPayload,
  MonthlyInvoice,
  MonthlyInvoicesPayload,
} from "@/features/bookings/types";
import {
  BOOKING_TX_CATEGORY_META,
  bookingTransactionCategory,
  buildPeriodKeyRange,
  centsToInputValue,
  currentPeriodKey,
  filterBookingTransactions,
  monthRangeFromPeriodKey,
  parseEuroToCents,
  periodKeyFromDateLike,
} from "@/features/bookings/utils";
import { buildDashboardQuery, fetchJson } from "@/lib/api";
import { formatCurrencyFromCents, formatDate, formatNumber } from "@/lib/format";

type BookingSubtab = "transactions" | "orders" | "templates" | "accounts" | "documents";
type BookingClass = "all" | "automatic" | "monthly" | "single";

interface BookingTransactionFilters {
  q: string;
  dateFrom: string;
  dateTo: string;
  provider: string;
  type: string;
  bookingClass: BookingClass;
  category: string;
}

interface BookingOrderFilters {
  q: string;
  from: string;
  to: string;
  marketplace: string;
}

interface CreateTransactionFormState {
  date: string;
  type: string;
  direction: string;
  amount: string;
  provider: string;
  status: string;
  reference: string;
  notes: string;
  orderId: string;
  paymentAccountId: string;
  templateId: string;
}

interface CreateMonthlyInvoiceFormState {
  provider: string;
  periodKey: string;
  amount: string;
  notes: string;
  file: File | null;
}

interface CreateTemplateFormState {
  name: string;
  type: string;
  direction: string;
  amount: string;
  provider: string;
  counterpartyName: string;
  startDate: string;
  dayOfMonth: string;
  schedule: string;
  paymentAccountId: string;
  notes: string;
}

interface CreateAccountFormState {
  name: string;
  provider: string;
  isActive: boolean;
}

interface CreateDocumentFormState {
  file: File | null;
  transactionId: string;
  notes: string;
}

interface BookingMessage {
  tone: "ok" | "error" | "info";
  text: string;
}

interface TransactionSelectOption {
  id: string;
  label: string;
}

const BOOKING_SUBTABS: Array<{ key: BookingSubtab; label: string }> = [
  { key: "transactions", label: "Transaktionen" },
  { key: "orders", label: "Bestellungen" },
  { key: "templates", label: "Templates" },
  { key: "accounts", label: "Konten" },
  { key: "documents", label: "Belege" },
];

const BOOKING_TX_TYPE_OPTIONS = ["SALE", "COGS", "FEE", "SUBSCRIPTION", "EXPENSE", "REFUND", "PAYOUT", "ADJUSTMENT"] as const;
const BOOKING_TX_STATUS_OPTIONS = ["pending", "confirmed", "reconciled"] as const;
const TEMPLATE_TYPE_OPTIONS = ["SUBSCRIPTION", "EXPENSE", "FEE", "COGS", "SALE"] as const;
const TEMPLATE_DIRECTION_OPTIONS = ["OUT", "IN"] as const;
const TEMPLATE_SCHEDULE_OPTIONS = ["monthly", "quarterly", "yearly"] as const;
const MONTHLY_INVOICE_PROVIDER_OPTIONS = [
  { value: "paypal", label: "PayPal Fees" },
  { value: "shopify_payments", label: "Shopify Payments Fees" },
  { value: "kaufland", label: "Kaufland Fees" },
  { value: "google_ads", label: "Google Ads" },
  { value: "ebay", label: "eBay Fees" },
] as const;

function todayDateToken() {
  return new Date().toISOString().slice(0, 10);
}

function previousPeriodKey() {
  const now = new Date();
  const previous = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  return `${previous.getFullYear()}-${String(previous.getMonth() + 1).padStart(2, "0")}`;
}

function createDefaultTransactionFilters(): BookingTransactionFilters {
  return {
    q: "",
    dateFrom: "",
    dateTo: "",
    provider: "",
    type: "",
    bookingClass: "automatic",
    category: "",
  };
}

function createDefaultOrderFilters(): BookingOrderFilters {
  return {
    q: "",
    from: "",
    to: "",
    marketplace: "",
  };
}

function createDefaultTransactionForm(): CreateTransactionFormState {
  return {
    date: todayDateToken(),
    type: "SALE",
    direction: "IN",
    amount: "",
    provider: "shopify",
    status: "confirmed",
    reference: "",
    notes: "",
    orderId: "",
    paymentAccountId: "",
    templateId: "",
  };
}

function createDefaultMonthlyInvoiceForm(): CreateMonthlyInvoiceFormState {
  return {
    provider: "paypal",
    periodKey: previousPeriodKey(),
    amount: "",
    notes: "",
    file: null,
  };
}

function createDefaultTemplateForm(): CreateTemplateFormState {
  return {
    name: "",
    type: "SUBSCRIPTION",
    direction: "OUT",
    amount: "",
    provider: "",
    counterpartyName: "",
    startDate: "",
    dayOfMonth: "",
    schedule: "monthly",
    paymentAccountId: "",
    notes: "",
  };
}

function createDefaultAccountForm(): CreateAccountFormState {
  return {
    name: "",
    provider: "",
    isActive: true,
  };
}

function createDefaultDocumentForm(): CreateDocumentFormState {
  return {
    file: null,
    transactionId: "",
    notes: "",
  };
}

async function fetchBookingTransactions(filters: BookingTransactionFilters) {
  const query = buildDashboardQuery({
    dateFrom: filters.dateFrom || undefined,
    dateTo: filters.dateTo || undefined,
    provider: filters.provider || undefined,
    type: filters.type || undefined,
    bookingClass: filters.bookingClass === "all" ? undefined : filters.bookingClass,
  });

  return fetchJson<BookingTransactionsPayload>(`/api/bookings/transactions${query ? `?${query}` : ""}`);
}

async function fetchAllBookingTransactions() {
  const query = buildDashboardQuery({ bookingClass: undefined });
  return fetchJson<BookingTransactionsPayload>(`/api/bookings/transactions${query ? `?${query}` : ""}`);
}

async function fetchBookingOrders(filters: BookingOrderFilters) {
  const query = buildDashboardQuery({
    q: filters.q || undefined,
    from: filters.from || undefined,
    to: filters.to || undefined,
    marketplace: filters.marketplace || undefined,
  });

  return fetchJson<BookingOrdersPayload>(`/api/bookings/orders${query ? `?${query}` : ""}`);
}

async function fetchBookingAccounts() {
  return fetchJson<BookingPaymentAccountsPayload>("/api/bookings/payment-accounts");
}

async function fetchBookingTemplates() {
  return fetchJson<BookingTemplatesPayload>("/api/bookings/templates");
}

async function fetchBookingDocuments() {
  return fetchJson<BookingDocumentsPayload>("/api/bookings/documents");
}

async function fetchBookingLedgerOrders() {
  return fetchJson<BookingLedgerOrdersPayload>("/api/bookings/ledger/orders");
}

async function fetchMonthlyInvoices() {
  return fetchJson<MonthlyInvoicesPayload>("/api/bookings/monthly-invoices");
}

async function fetchMonthlyInvoicePreview(provider: string, periodFrom: string, periodTo: string) {
  const query = buildDashboardQuery({ provider, periodFrom, periodTo });
  return fetchJson<{
    provider?: string;
    period_from?: string;
    period_to?: string;
    total_cents?: number;
    transaction_count?: number;
    transactions?: Array<{ id?: string; date?: string; amount_gross?: number; reference?: string | null }>;
  }>(`/api/bookings/transactions/sum?${query}`);
}

async function updateTransactionAccount(input: { transactionId: string; paymentAccountId: string | null }) {
  return fetchJson(`/api/bookings/transactions/${encodeURIComponent(input.transactionId)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ payment_account_id: input.paymentAccountId }),
  });
}

async function createBookingTransaction(payload: {
  date: string;
  type: string;
  direction: string;
  amount_gross: number;
  currency: string;
  provider: string;
  status: string;
  reference: string | null;
  notes: string | null;
  order_id: string | null;
  payment_account_id: string | null;
  template_id: string | null;
  source: string;
  booking_class: string;
}) {
  return fetchJson("/api/bookings/transactions", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

async function createPaymentAccount(payload: { name: string; provider: string | null; is_active: boolean }) {
  return fetchJson("/api/bookings/payment-accounts", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

async function updatePaymentAccount(input: { paymentAccountId: string; payload: { name: string; provider: string | null; is_active: boolean } }) {
  return fetchJson(`/api/bookings/payment-accounts/${encodeURIComponent(input.paymentAccountId)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input.payload),
  });
}

async function createTemplate(payload: {
  name: string;
  type: string;
  direction: string;
  default_amount_gross: number;
  currency: string;
  provider: string;
  counterparty_name: string | null;
  schedule: string;
  start_date: string | null;
  day_of_month: number | null;
  payment_account_id: string | null;
  notes_default: string | null;
  active: boolean;
}) {
  return fetchJson("/api/bookings/templates", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

async function updateTemplate(input: {
  templateId: string;
  payload: {
    name: string;
    counterparty_name: string | null;
    start_date: string | null;
    default_amount_gross: number;
    schedule: string;
    payment_account_id: string | null;
    active: boolean;
  };
}) {
  return fetchJson(`/api/bookings/templates/${encodeURIComponent(input.templateId)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input.payload),
  });
}

async function generateTemplateTransaction(input: { templateId: string; periodKey: string }) {
  const response = await fetch(`/api/bookings/templates/${encodeURIComponent(input.templateId)}/generate-transaction`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ period_key: input.periodKey, status: "pending" }),
  });

  const payload = (await response.json().catch(() => ({}))) as { detail?: unknown; error?: unknown };
  if (response.ok) {
    return { status: "created" as const };
  }

  let message = `HTTP ${response.status}`;
  if (typeof payload.detail === "string") {
    message = payload.detail;
  } else if (typeof payload.error === "string") {
    message = payload.error;
  }

  if (response.status === 409) {
    return { status: "duplicate" as const, message };
  }
  throw new Error(message);
}

async function uploadBookingDocumentFile(input: {
  file: File;
  notes?: string | null;
  transaction_id?: string | null;
  provider?: string | null;
  transaction_type?: string | null;
  booking_date?: string | null;
  amount_cents?: number | null;
  currency?: string | null;
}) {
  const formData = new FormData();
  formData.append("file", input.file);
  if (input.notes) formData.append("notes", input.notes);
  if (input.transaction_id) formData.append("transaction_id", input.transaction_id);
  if (input.provider) formData.append("provider", input.provider);
  if (input.transaction_type) formData.append("transaction_type", input.transaction_type);
  if (input.booking_date) formData.append("booking_date", input.booking_date);
  if (typeof input.amount_cents === "number" && input.amount_cents > 0) formData.append("amount_cents", String(input.amount_cents));
  if (input.currency) formData.append("currency", input.currency);

  return fetchJson<{ ok?: boolean; document?: BookingDocument }>("/api/bookings/documents/upload", {
    method: "POST",
    body: formData,
  });
}

async function createMonthlyInvoice(payload: {
  provider: string;
  period_from: string;
  period_to: string;
  invoice_amount_cents: number;
  currency: string;
  notes: string | null;
}) {
  return fetchJson<{ ok?: boolean; invoice?: MonthlyInvoice }>("/api/bookings/monthly-invoices", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

async function updateMonthlyInvoice(input: {
  invoiceId: string;
  payload: {
    provider?: string;
    period_from?: string;
    period_to?: string;
    invoice_amount_cents?: number;
    currency?: string;
    notes?: string | null;
    document_id?: string | null;
  };
}) {
  return fetchJson<{ ok?: boolean; invoice?: MonthlyInvoice }>(`/api/bookings/monthly-invoices/${encodeURIComponent(input.invoiceId)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input.payload),
  });
}

async function deleteMonthlyInvoice(invoiceId: string) {
  return fetchJson(`/api/bookings/monthly-invoices/${encodeURIComponent(invoiceId)}`, {
    method: "DELETE",
  });
}

function invoiceProviderLabel(provider?: string | null) {
  return MONTHLY_INVOICE_PROVIDER_OPTIONS.find((option) => option.value === provider)?.label ?? provider ?? "-";
}

async function invalidateBookingsQueries(queryClient: ReturnType<typeof useQueryClient>) {
  await Promise.all([
    queryClient.invalidateQueries({ queryKey: ["bookings-transactions-preview"] }),
    queryClient.invalidateQueries({ queryKey: ["bookings-orders-preview"] }),
    queryClient.invalidateQueries({ queryKey: ["bookings-accounts-preview"] }),
    queryClient.invalidateQueries({ queryKey: ["bookings-templates-preview"] }),
    queryClient.invalidateQueries({ queryKey: ["bookings-documents-preview"] }),
    queryClient.invalidateQueries({ queryKey: ["bookings-ledger-orders-preview"] }),
    queryClient.invalidateQueries({ queryKey: ["bookings-all-transactions-preview"] }),
    queryClient.invalidateQueries({ queryKey: ["bookings-monthly-invoices-preview"] }),
    queryClient.invalidateQueries({ queryKey: ["bookings-monthly-preview"] }),
  ]);
}

export function BookingsPage() {
  const queryClient = useQueryClient();
  const [activeSubtab, setActiveSubtab] = useState<BookingSubtab>("transactions");
  const [transactionFilters, setTransactionFilters] = useState(createDefaultTransactionFilters);
  const [orderFilters, setOrderFilters] = useState(createDefaultOrderFilters);
  const [createTransactionForm, setCreateTransactionForm] = useState(createDefaultTransactionForm);
  const [createMonthlyInvoiceForm, setCreateMonthlyInvoiceForm] = useState(createDefaultMonthlyInvoiceForm);
  const [createTemplateForm, setCreateTemplateForm] = useState(createDefaultTemplateForm);
  const [createAccountForm, setCreateAccountForm] = useState(createDefaultAccountForm);
  const [createDocumentForm, setCreateDocumentForm] = useState(createDefaultDocumentForm);
  const [bookingMessage, setBookingMessage] = useState<BookingMessage | null>(null);

  const deferredTxSearch = useDeferredValue(transactionFilters.q);
  const deferredOrderSearch = useDeferredValue(orderFilters.q);
  const monthlyPreviewRange = monthRangeFromPeriodKey(createMonthlyInvoiceForm.periodKey);

  const transactionsQuery = useQuery({
    queryKey: [
      "bookings-transactions-preview",
      transactionFilters.dateFrom,
      transactionFilters.dateTo,
      transactionFilters.provider,
      transactionFilters.type,
      transactionFilters.bookingClass,
    ],
    queryFn: () => fetchBookingTransactions(transactionFilters),
    enabled: activeSubtab === "transactions",
  });

  const allTransactionsQuery = useQuery({
    queryKey: ["bookings-all-transactions-preview"],
    queryFn: fetchAllBookingTransactions,
    enabled: activeSubtab === "documents",
  });

  const ordersQuery = useQuery({
    queryKey: ["bookings-orders-preview", deferredOrderSearch, orderFilters.from, orderFilters.to, orderFilters.marketplace],
    queryFn: () =>
      fetchBookingOrders({
        ...orderFilters,
        q: deferredOrderSearch,
      }),
    enabled: activeSubtab === "orders",
  });

  const accountsQuery = useQuery({
    queryKey: ["bookings-accounts-preview"],
    queryFn: fetchBookingAccounts,
  });

  const templatesQuery = useQuery({
    queryKey: ["bookings-templates-preview"],
    queryFn: fetchBookingTemplates,
  });

  const documentsQuery = useQuery({
    queryKey: ["bookings-documents-preview"],
    queryFn: fetchBookingDocuments,
  });

  const ledgerOrdersQuery = useQuery({
    queryKey: ["bookings-ledger-orders-preview"],
    queryFn: fetchBookingLedgerOrders,
  });

  const monthlyInvoicesQuery = useQuery({
    queryKey: ["bookings-monthly-invoices-preview"],
    queryFn: fetchMonthlyInvoices,
    enabled: activeSubtab === "transactions" && transactionFilters.bookingClass === "monthly",
  });

  const monthlyPreviewQuery = useQuery({
    queryKey: ["bookings-monthly-preview", createMonthlyInvoiceForm.provider, monthlyPreviewRange.periodFrom, monthlyPreviewRange.periodTo],
    queryFn: () => fetchMonthlyInvoicePreview(createMonthlyInvoiceForm.provider, monthlyPreviewRange.periodFrom, monthlyPreviewRange.periodTo),
    enabled:
      activeSubtab === "transactions" &&
      transactionFilters.bookingClass === "monthly" &&
      Boolean(createMonthlyInvoiceForm.provider) &&
      Boolean(monthlyPreviewRange.periodFrom) &&
      Boolean(monthlyPreviewRange.periodTo),
  });

  const updateAccountMutation = useMutation({
    mutationFn: updateTransactionAccount,
    onSuccess: async () => {
      setBookingMessage({ tone: "ok", text: "Kontozuordnung wurde gespeichert." });
      await invalidateBookingsQueries(queryClient);
    },
    onError: (error: Error) => {
      setBookingMessage({ tone: "error", text: error.message });
    },
  });

  const createTransactionMutation = useMutation({
    mutationFn: createBookingTransaction,
    onSuccess: async () => {
      setCreateTransactionForm(createDefaultTransactionForm());
      setBookingMessage({ tone: "ok", text: "Transaktion angelegt." });
      await invalidateBookingsQueries(queryClient);
    },
    onError: (error: Error) => {
      setBookingMessage({ tone: "error", text: error.message });
    },
  });

  const createTemplateMutation = useMutation({
    mutationFn: createTemplate,
    onSuccess: async () => {
      setCreateTemplateForm(createDefaultTemplateForm());
      setBookingMessage({ tone: "ok", text: "Template angelegt." });
      await invalidateBookingsQueries(queryClient);
    },
    onError: (error: Error) => {
      setBookingMessage({ tone: "error", text: error.message });
    },
  });

  const updateTemplateMutation = useMutation({
    mutationFn: updateTemplate,
    onSuccess: async () => {
      setBookingMessage({ tone: "ok", text: "Template gespeichert." });
      await invalidateBookingsQueries(queryClient);
    },
    onError: (error: Error) => {
      setBookingMessage({ tone: "error", text: error.message });
    },
  });

  const runTemplateMutation = useMutation({
    mutationFn: async (input: { templateId: string; periodKey: string; backfillFrom?: string }) => {
      if (!input.backfillFrom) {
        return { created: 1, duplicates: 0, mode: "single" as const, result: await generateTemplateTransaction({ templateId: input.templateId, periodKey: input.periodKey }) };
      }

      const periods = buildPeriodKeyRange(input.backfillFrom, currentPeriodKey());
      let created = 0;
      let duplicates = 0;
      for (const periodKey of periods) {
        const result = await generateTemplateTransaction({ templateId: input.templateId, periodKey });
        if (result.status === "created") {
          created += 1;
        } else {
          duplicates += 1;
        }
      }
      return { created, duplicates, mode: "backfill" as const };
    },
    onSuccess: async (result) => {
      if (result.mode === "single" && result.result.status === "duplicate") {
        setBookingMessage({ tone: "info", text: "Template fuer diesen Zeitraum existiert bereits." });
      } else if (result.mode === "backfill") {
        setBookingMessage({
          tone: "ok",
          text: `Template-Backfill abgeschlossen: ${formatNumber(result.created)} neu, ${formatNumber(result.duplicates)} bereits vorhanden.`,
        });
      } else {
        setBookingMessage({ tone: "ok", text: "Template-Transaktion erzeugt." });
      }
      await invalidateBookingsQueries(queryClient);
    },
    onError: (error: Error) => {
      setBookingMessage({ tone: "error", text: error.message });
    },
  });

  const createAccountMutation = useMutation({
    mutationFn: createPaymentAccount,
    onSuccess: async () => {
      setCreateAccountForm(createDefaultAccountForm());
      setBookingMessage({ tone: "ok", text: "Konto angelegt." });
      await invalidateBookingsQueries(queryClient);
    },
    onError: (error: Error) => {
      setBookingMessage({ tone: "error", text: error.message });
    },
  });

  const updatePaymentAccountMutation = useMutation({
    mutationFn: updatePaymentAccount,
    onSuccess: async () => {
      setBookingMessage({ tone: "ok", text: "Konto gespeichert." });
      await invalidateBookingsQueries(queryClient);
    },
    onError: (error: Error) => {
      setBookingMessage({ tone: "error", text: error.message });
    },
  });

  const uploadDocumentMutation = useMutation({
    mutationFn: uploadBookingDocumentFile,
    onSuccess: async () => {
      setCreateDocumentForm(createDefaultDocumentForm());
      setBookingMessage({ tone: "ok", text: "Beleg hochgeladen." });
      await invalidateBookingsQueries(queryClient);
    },
    onError: (error: Error) => {
      setBookingMessage({ tone: "error", text: error.message });
    },
  });

  const createMonthlyInvoiceMutation = useMutation({
    mutationFn: async (input: {
      provider: string;
      periodKey: string;
      amountCents: number;
      notes: string | null;
      file: File | null;
    }) => {
      const range = monthRangeFromPeriodKey(input.periodKey);
      const created = await createMonthlyInvoice({
        provider: input.provider,
        period_from: range.periodFrom,
        period_to: range.periodTo,
        invoice_amount_cents: input.amountCents,
        currency: "EUR",
        notes: input.notes,
      });

      const invoiceId = created.invoice?.id;
      if (invoiceId && input.file) {
        const uploaded = await uploadBookingDocumentFile({ file: input.file, notes: input.notes });
        const documentId = uploaded.document?.id;
        if (documentId) {
          await updateMonthlyInvoice({ invoiceId: String(invoiceId), payload: { document_id: String(documentId) } });
        }
      }

      return created;
    },
    onSuccess: async () => {
      setCreateMonthlyInvoiceForm(createDefaultMonthlyInvoiceForm());
      setBookingMessage({ tone: "ok", text: "Sammelrechnung angelegt." });
      await invalidateBookingsQueries(queryClient);
    },
    onError: (error: Error) => {
      setBookingMessage({ tone: "error", text: error.message });
    },
  });

  const updateMonthlyInvoiceMutation = useMutation({
    mutationFn: async (input: {
      invoiceId: string;
      provider: string;
      periodKey: string;
      amountCents: number;
      notes: string | null;
      file: File | null;
    }) => {
      const range = monthRangeFromPeriodKey(input.periodKey);
      let documentId: string | null | undefined;
      if (input.file) {
        const uploaded = await uploadBookingDocumentFile({ file: input.file, notes: input.notes });
        documentId = uploaded.document?.id ? String(uploaded.document.id) : null;
      }

      return updateMonthlyInvoice({
        invoiceId: input.invoiceId,
        payload: {
          provider: input.provider,
          period_from: range.periodFrom,
          period_to: range.periodTo,
          invoice_amount_cents: input.amountCents,
          notes: input.notes,
          ...(documentId ? { document_id: documentId } : {}),
        },
      });
    },
    onSuccess: async () => {
      setBookingMessage({ tone: "ok", text: "Sammelrechnung gespeichert." });
      await invalidateBookingsQueries(queryClient);
    },
    onError: (error: Error) => {
      setBookingMessage({ tone: "error", text: error.message });
    },
  });

  const deleteMonthlyInvoiceMutation = useMutation({
    mutationFn: deleteMonthlyInvoice,
    onSuccess: async () => {
      setBookingMessage({ tone: "ok", text: "Sammelrechnung geloescht." });
      await invalidateBookingsQueries(queryClient);
    },
    onError: (error: Error) => {
      setBookingMessage({ tone: "error", text: error.message });
    },
  });

  const allTransactions = transactionsQuery.data?.items ?? [];
  const filteredTransactions = filterBookingTransactions(allTransactions, {
    query: deferredTxSearch,
    category: transactionFilters.category,
    type: transactionFilters.type,
  });
  const bookingOrders = ordersQuery.data?.items ?? [];
  const accounts = accountsQuery.data?.items ?? [];
  const templates = templatesQuery.data?.items ?? [];
  const documents = documentsQuery.data?.items ?? [];
  const ledgerOrders = ledgerOrdersQuery.data?.items ?? [];
  const allTransactionOptions = useMemo<TransactionSelectOption[]>(() => {
    const items = allTransactionsQuery.data?.items ?? [];
    return items.map((transaction) => ({
      id: String(transaction.id ?? ""),
      label: `${formatDate(transaction.date)} | ${transaction.reference || transaction.type || transaction.id || "-"}`,
    }));
  }, [allTransactionsQuery.data?.items]);
  const monthlyInvoices = monthlyInvoicesQuery.data?.items ?? [];

  const transactionProviderOptions = useMemo(() => {
    const tokens = new Set<string>();
    for (const item of allTransactions) {
      const provider = String(item.provider ?? "").trim();
      if (provider) tokens.add(provider);
    }
    if (transactionFilters.provider) tokens.add(transactionFilters.provider);
    return [{ value: "", label: "Alle" }, ...Array.from(tokens).sort().map((value) => ({ value, label: value }))];
  }, [allTransactions, transactionFilters.provider]);

  let incomingCents = 0;
  let outgoingCents = 0;
  let documentsCount = 0;
  let automaticCount = 0;
  for (const transaction of filteredTransactions) {
    const amount = Number(transaction.amount_gross ?? 0);
    if (String(transaction.direction ?? "").toUpperCase() === "IN") {
      incomingCents += amount;
    } else {
      outgoingCents += amount;
    }
    if (transaction.document_id) documentsCount += 1;
    if (transaction.booking_class === "automatic") automaticCount += 1;
  }

  let ordersRevenueCents = 0;
  let ordersCostCents = 0;
  let ordersProfitCents = 0;
  let ordersDocumentCount = 0;
  for (const order of bookingOrders) {
    ordersRevenueCents += Number(order.revenue_cents ?? 0);
    ordersCostCents += Number(order.total_costs_cents ?? 0);
    ordersProfitCents += Number(order.profit_cents ?? 0);
    ordersDocumentCount += Number(order.documents_count ?? 0);
  }

  const legacyHref =
    activeSubtab === "transactions"
      ? "/legacy?tab=bookings&subtab=transactions&full=1"
      : `/legacy?tab=bookings&subtab=${activeSubtab}`;

  return (
    <div className="space-y-5">
      <section className="grid gap-3 xl:grid-cols-[1.55fr_1fr]">
        <SurfaceCard
          description="Bookings ist jetzt vollstaendig als React-Arbeitsbereich auf derselben API-Basis verfuegbar: Transaktionen, Sammelrechnungen, Bestellungen, Templates, Konten und Belege."
          title="Bookings Migration"
        />

        <SurfaceCard
          action={
            <a href={legacyHref} rel="noreferrer">
              <Button variant="outline">
                Legacy Bookings
                <ArrowUpRight className="ml-2 h-4 w-4" />
              </Button>
            </a>
          }
          description="Das Legacy-Dashboard bleibt nur noch als Fallback unter `/legacy` erreichbar. Die Hauptarbeit fuer Bookings laeuft jetzt in React."
          title="Umstellung"
        />
      </section>

      {bookingMessage ? <MessageBanner message={bookingMessage} /> : null}

      <SurfaceCard title="Unterbereiche">
        <div className="flex flex-wrap gap-2">
          {BOOKING_SUBTABS.map((tab) => (
            <FilterChip active={activeSubtab === tab.key} key={tab.key} onClick={() => setActiveSubtab(tab.key)}>
              {tab.label}
            </FilterChip>
          ))}
        </div>
      </SurfaceCard>

      {activeSubtab === "transactions" ? (
        <div className="space-y-5">
          <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <MetricCard icon={TableOfContents} label="Transaktionen" value={formatNumber(filteredTransactions.length)} />
            <MetricCard icon={Wallet} label="Einnahmen" value={formatCurrencyFromCents(incomingCents)} />
            <MetricCard icon={Landmark} label="Ausgaben" value={formatCurrencyFromCents(outgoingCents)} />
            <MetricCard icon={FileCheck2} label="Mit Beleg" value={formatNumber(documentsCount)} subValue={`${formatNumber(automaticCount)} automatisch`} />
          </section>

          <section className="grid gap-3 xl:grid-cols-[1.1fr_0.9fr]">
            <PageFilterCard
              description="Datums- und Providerfilter laufen serverseitig gegen `/api/bookings/transactions`, die Textsuche wie im Legacy-Dashboard clientseitig."
              from={transactionFilters.dateFrom}
              marketplace={transactionFilters.provider}
              marketplaceLabel="Provider"
              marketplaceOptions={transactionProviderOptions}
              onFromChange={(value) => setTransactionFilters((current) => ({ ...current, dateFrom: value }))}
              onMarketplaceChange={(value) => setTransactionFilters((current) => ({ ...current, provider: value }))}
              onQueryChange={(value) => setTransactionFilters((current) => ({ ...current, q: value }))}
              onToChange={(value) => setTransactionFilters((current) => ({ ...current, dateTo: value }))}
              query={transactionFilters.q}
              queryPlaceholder="Provider, Referenz, Notiz"
              title="Basisfilter"
              to={transactionFilters.dateTo}
            />

            <SurfaceCard
              action={
                <Button
                  onClick={() => {
                    setTransactionFilters(createDefaultTransactionFilters());
                    setBookingMessage(null);
                  }}
                  size="sm"
                  variant="ghost"
                >
                  Reset
                </Button>
              }
              description="Buchungsklasse, Kategorie und Typ spiegeln die wichtigsten Legacy-Segmente wider."
              title="Weitere Filter"
            >
              <div className="space-y-4">
                <FilterGroup label="Buchungsklasse">
                  {(["all", "automatic", "monthly", "single"] as const).map((bookingClass) => (
                    <FilterChip
                      active={transactionFilters.bookingClass === bookingClass}
                      key={bookingClass}
                      onClick={() =>
                        setTransactionFilters((current) => ({
                          ...current,
                          bookingClass,
                          category: "",
                          type: "",
                        }))
                      }
                    >
                      {bookingClass}
                    </FilterChip>
                  ))}
                </FilterGroup>

                <FilterGroup label="Kategorie">
                  {Object.entries(BOOKING_TX_CATEGORY_META).map(([key, meta]) => (
                    <FilterChip
                      active={transactionFilters.category === key}
                      key={key}
                      onClick={() =>
                        setTransactionFilters((current) => ({
                          ...current,
                          category: current.category === key ? "" : key,
                        }))
                      }
                    >
                      {meta.label}
                    </FilterChip>
                  ))}
                </FilterGroup>

                <FilterGroup label="Typ">
                  {BOOKING_TX_TYPE_OPTIONS.map((type) => (
                    <FilterChip
                      active={transactionFilters.type === type}
                      key={type}
                      onClick={() =>
                        setTransactionFilters((current) => ({
                          ...current,
                          type: current.type === type ? "" : type,
                        }))
                      }
                    >
                      {type}
                    </FilterChip>
                  ))}
                </FilterGroup>
              </div>
            </SurfaceCard>
          </section>

          {transactionFilters.bookingClass === "single" ? (
            <SurfaceCard description="Neue manuelle Einzeltransaktion ueber denselben API-Endpoint wie im Legacy-Dashboard." title="Neue Transaktion">
              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
                <FormField label="Datum">
                  <input
                    className={fieldClassName}
                    onChange={(event) => setCreateTransactionForm((current) => ({ ...current, date: event.target.value }))}
                    type="date"
                    value={createTransactionForm.date}
                  />
                </FormField>
                <FormField label="Typ">
                  <select
                    className={fieldClassName}
                    onChange={(event) => setCreateTransactionForm((current) => ({ ...current, type: event.target.value }))}
                    value={createTransactionForm.type}
                  >
                    {BOOKING_TX_TYPE_OPTIONS.map((type) => (
                      <option key={type} value={type}>
                        {type}
                      </option>
                    ))}
                  </select>
                </FormField>
                <FormField label="Richtung">
                  <select
                    className={fieldClassName}
                    onChange={(event) => setCreateTransactionForm((current) => ({ ...current, direction: event.target.value }))}
                    value={createTransactionForm.direction}
                  >
                    <option value="IN">IN</option>
                    <option value="OUT">OUT</option>
                  </select>
                </FormField>
                <FormField label="Betrag (EUR)">
                  <input
                    className={fieldClassName}
                    inputMode="decimal"
                    onChange={(event) => setCreateTransactionForm((current) => ({ ...current, amount: event.target.value }))}
                    placeholder="0,00"
                    type="text"
                    value={createTransactionForm.amount}
                  />
                </FormField>
                <FormField label="Provider">
                  <input
                    className={fieldClassName}
                    onChange={(event) => setCreateTransactionForm((current) => ({ ...current, provider: event.target.value }))}
                    placeholder="shopify"
                    type="text"
                    value={createTransactionForm.provider}
                  />
                </FormField>
                <FormField label="Status">
                  <select
                    className={fieldClassName}
                    onChange={(event) => setCreateTransactionForm((current) => ({ ...current, status: event.target.value }))}
                    value={createTransactionForm.status}
                  >
                    {BOOKING_TX_STATUS_OPTIONS.map((status) => (
                      <option key={status} value={status}>
                        {status}
                      </option>
                    ))}
                  </select>
                </FormField>
                <FormField label="Referenz">
                  <input
                    className={fieldClassName}
                    onChange={(event) => setCreateTransactionForm((current) => ({ ...current, reference: event.target.value }))}
                    placeholder="Order-ID / Beleg-Nr."
                    type="text"
                    value={createTransactionForm.reference}
                  />
                </FormField>
                <FormField label="Order-Link (optional)">
                  <select
                    className={fieldClassName}
                    onChange={(event) => setCreateTransactionForm((current) => ({ ...current, orderId: event.target.value }))}
                    value={createTransactionForm.orderId}
                  >
                    <option value="">Keine Zuordnung</option>
                    {ledgerOrders.map((order) => (
                      <option key={order.id} value={order.id}>
                        {String(order.provider || "-").toUpperCase()} | {order.external_order_id || order.id}
                      </option>
                    ))}
                  </select>
                </FormField>
                <FormField label="Konto (optional)">
                  <select
                    className={fieldClassName}
                    onChange={(event) => setCreateTransactionForm((current) => ({ ...current, paymentAccountId: event.target.value }))}
                    value={createTransactionForm.paymentAccountId}
                  >
                    <option value="">Ohne Konto</option>
                    {accounts.map((account) => (
                      <option key={account.id} value={account.id}>
                        {account.name} ({account.provider || "-"})
                      </option>
                    ))}
                  </select>
                </FormField>
                <FormField label="Template (optional)">
                  <select
                    className={fieldClassName}
                    onChange={(event) => setCreateTransactionForm((current) => ({ ...current, templateId: event.target.value }))}
                    value={createTransactionForm.templateId}
                  >
                    <option value="">Ohne Template</option>
                    {templates.map((template) => (
                      <option key={template.id} value={template.id}>
                        {template.name}
                      </option>
                    ))}
                  </select>
                </FormField>
                <FormField className="md:col-span-2 xl:col-span-5" label="Notiz">
                  <input
                    className={fieldClassName}
                    onChange={(event) => setCreateTransactionForm((current) => ({ ...current, notes: event.target.value }))}
                    placeholder="Optional"
                    type="text"
                    value={createTransactionForm.notes}
                  />
                </FormField>
              </div>
              <div className="mt-4 flex justify-end">
                <Button
                  disabled={createTransactionMutation.isPending}
                  onClick={() => {
                    const amountCents = parseEuroToCents(createTransactionForm.amount);
                    if (!createTransactionForm.date || !createTransactionForm.provider.trim() || !amountCents) {
                      setBookingMessage({ tone: "error", text: "Datum, Provider und Betrag sind erforderlich." });
                      return;
                    }
                    setBookingMessage(null);
                    createTransactionMutation.mutate({
                      date: createTransactionForm.date,
                      type: createTransactionForm.type,
                      direction: createTransactionForm.direction,
                      amount_gross: amountCents,
                      currency: "EUR",
                      provider: createTransactionForm.provider.trim(),
                      status: createTransactionForm.status,
                      reference: createTransactionForm.reference.trim() || null,
                      notes: createTransactionForm.notes.trim() || null,
                      order_id: createTransactionForm.orderId || null,
                      payment_account_id: createTransactionForm.paymentAccountId || null,
                      template_id: createTransactionForm.templateId || null,
                      source: "manual",
                      booking_class: "single",
                    });
                  }}
                >
                  Transaktion anlegen
                </Button>
              </div>
            </SurfaceCard>
          ) : null}

          {transactionFilters.bookingClass === "monthly" ? (
            <div className="grid gap-3 xl:grid-cols-[1.05fr_0.95fr]">
              <SurfaceCard description="Neue Sammelrechnung inkl. optionalem Beleg-Upload und automatischer Reconciliation gegen automatische OUT-Transaktionen." title="Neue Sammelrechnung">
                <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
                  <FormField label="Provider">
                    <select
                      className={fieldClassName}
                      onChange={(event) => setCreateMonthlyInvoiceForm((current) => ({ ...current, provider: event.target.value }))}
                      value={createMonthlyInvoiceForm.provider}
                    >
                      {MONTHLY_INVOICE_PROVIDER_OPTIONS.map((option) => (
                        <option key={option.value} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>
                  </FormField>
                  <FormField label="Monat">
                    <input
                      className={fieldClassName}
                      onChange={(event) => setCreateMonthlyInvoiceForm((current) => ({ ...current, periodKey: event.target.value }))}
                      type="month"
                      value={createMonthlyInvoiceForm.periodKey}
                    />
                  </FormField>
                  <FormField label="Rechnungsbetrag (EUR)">
                    <input
                      className={fieldClassName}
                      inputMode="decimal"
                      onChange={(event) => setCreateMonthlyInvoiceForm((current) => ({ ...current, amount: event.target.value }))}
                      placeholder="0,00"
                      type="text"
                      value={createMonthlyInvoiceForm.amount}
                    />
                  </FormField>
                  <FormField label="Notiz">
                    <input
                      className={fieldClassName}
                      onChange={(event) => setCreateMonthlyInvoiceForm((current) => ({ ...current, notes: event.target.value }))}
                      placeholder="Optional"
                      type="text"
                      value={createMonthlyInvoiceForm.notes}
                    />
                  </FormField>
                  <FormField label="Beleg (optional)">
                    <input
                      accept=".pdf,.png,.jpg,.jpeg,.webp"
                      className={fileFieldClassName}
                      onChange={(event) => setCreateMonthlyInvoiceForm((current) => ({ ...current, file: event.target.files?.[0] ?? null }))}
                      type="file"
                    />
                  </FormField>
                </div>
                <div className="mt-4 flex justify-end">
                  <Button
                    disabled={createMonthlyInvoiceMutation.isPending}
                    onClick={() => {
                      const amountCents = parseEuroToCents(createMonthlyInvoiceForm.amount);
                      if (!createMonthlyInvoiceForm.provider || !createMonthlyInvoiceForm.periodKey || !amountCents) {
                        setBookingMessage({ tone: "error", text: "Provider, Monat und Rechnungsbetrag sind erforderlich." });
                        return;
                      }
                      setBookingMessage(null);
                      createMonthlyInvoiceMutation.mutate({
                        provider: createMonthlyInvoiceForm.provider,
                        periodKey: createMonthlyInvoiceForm.periodKey,
                        amountCents,
                        notes: createMonthlyInvoiceForm.notes.trim() || null,
                        file: createMonthlyInvoiceForm.file,
                      });
                    }}
                  >
                    Sammelrechnung anlegen
                  </Button>
                </div>
              </SurfaceCard>

              <SurfaceCard description="Vorschau der zu erwartenden automatischen OUT-Transaktionen fuer den gewaehlten Zeitraum." title="Monatsvorschau">
                {monthlyPreviewQuery.isLoading ? (
                  <p className="text-sm text-[var(--ink-4)]">Vorschau wird geladen...</p>
                ) : monthlyPreviewQuery.data ? (
                  <div className="space-y-4">
                    <div className="grid gap-3 sm:grid-cols-2">
                      <InfoTile label="Provider" value={invoiceProviderLabel(createMonthlyInvoiceForm.provider)} />
                      <InfoTile label="Zeitraum" value={`${monthlyPreviewRange.periodFrom} bis ${monthlyPreviewRange.periodTo}`} />
                      <InfoTile label="Erwartete Summe" value={formatCurrencyFromCents(monthlyPreviewQuery.data.total_cents)} />
                      <InfoTile label="Transaktionen" value={formatNumber(Number(monthlyPreviewQuery.data.transaction_count ?? 0))} />
                    </div>
                    <div className="rounded-[20px] border border-[var(--border)]">
                      <div className="max-h-[220px] overflow-auto">
                        <table className="min-w-full border-collapse text-sm">
                          <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
                            <tr>
                              <th className="px-4 py-3 font-medium">Datum</th>
                              <th className="px-4 py-3 font-medium">Betrag</th>
                              <th className="px-4 py-3 font-medium">Referenz</th>
                            </tr>
                          </thead>
                          <tbody>
                            {(monthlyPreviewQuery.data.transactions ?? []).map((transaction) => (
                              <tr className="border-t border-[var(--border)] text-[var(--ink-2)]" key={transaction.id}>
                                <td className="px-4 py-3">{formatDate(transaction.date)}</td>
                                <td className="px-4 py-3">{formatCurrencyFromCents(transaction.amount_gross)}</td>
                                <td className="px-4 py-3">{transaction.reference ?? "-"}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  </div>
                ) : (
                  <p className="text-sm text-[var(--ink-4)]">Keine Vorschau verfuegbar.</p>
                )}
              </SurfaceCard>

              <DataTableShell
                description="Vorhandene Sammelrechnungen fuer automatische Fee-/Kosten-Bloecke."
                meta={monthlyInvoicesQuery.isLoading ? "Lade..." : `${formatNumber(monthlyInvoices.length)} Zeilen`}
                title="Sammelrechnungen"
              >
                <table className="min-w-full border-collapse text-sm">
                  <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
                    <tr>
                      <th className="px-4 py-3 font-medium">Provider</th>
                      <th className="px-4 py-3 font-medium">Monat</th>
                      <th className="px-4 py-3 font-medium">Rechnungsbetrag</th>
                      <th className="px-4 py-3 font-medium">Berechnet</th>
                      <th className="px-4 py-3 font-medium">Differenz</th>
                      <th className="px-4 py-3 font-medium">Beleg</th>
                      <th className="px-4 py-3 font-medium">Notiz</th>
                      <th className="px-4 py-3 font-medium">Aktion</th>
                    </tr>
                  </thead>
                  <tbody>
                    {monthlyInvoicesQuery.isLoading ? (
                      <tr>
                        <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={8}>
                          Sammelrechnungen werden geladen...
                        </td>
                      </tr>
                    ) : monthlyInvoices.length ? (
                      monthlyInvoices.map((invoice) => (
                        <MonthlyInvoiceRow
                          invoice={invoice}
                          isDeleting={deleteMonthlyInvoiceMutation.isPending && deleteMonthlyInvoiceMutation.variables === invoice.id}
                          isSaving={updateMonthlyInvoiceMutation.isPending && updateMonthlyInvoiceMutation.variables?.invoiceId === invoice.id}
                          key={invoice.id}
                          onDelete={(invoiceId) => {
                            setBookingMessage(null);
                            deleteMonthlyInvoiceMutation.mutate(invoiceId);
                          }}
                          onSave={(payload) => {
                            setBookingMessage(null);
                            updateMonthlyInvoiceMutation.mutate(payload);
                          }}
                        />
                      ))
                    ) : (
                      <tr>
                        <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={8}>
                          Keine Sammelrechnungen vorhanden.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </DataTableShell>
            </div>
          ) : null}

          <DataTableShell
            description="Bookings-Transaktionen mit bestehender Kontozuordnung und Belegverknuepfung."
            meta={transactionsQuery.isLoading ? "Lade..." : `${formatNumber(filteredTransactions.length)} Zeilen`}
            title="Transaktionen"
          >
            <table className="min-w-full border-collapse text-sm">
              <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
                <tr>
                  <th className="px-4 py-3 font-medium">Datum</th>
                  <th className="px-4 py-3 font-medium">Typ</th>
                  <th className="px-4 py-3 font-medium">Provider</th>
                  <th className="px-4 py-3 font-medium">Richtung</th>
                  <th className="px-4 py-3 font-medium">Betrag</th>
                  <th className="px-4 py-3 font-medium">Referenz</th>
                  <th className="px-4 py-3 font-medium">Klasse</th>
                  <th className="px-4 py-3 font-medium">Konto</th>
                  <th className="px-4 py-3 font-medium">Beleg</th>
                </tr>
              </thead>
              <tbody>
                {transactionsQuery.isLoading ? (
                  <tr>
                    <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={9}>
                      Bookings-Transaktionen werden geladen...
                    </td>
                  </tr>
                ) : transactionsQuery.isError ? (
                  <tr>
                    <td className="px-4 py-4 text-[var(--danger)]" colSpan={9}>
                      Bookings-Transaktionen konnten nicht geladen werden: {transactionsQuery.error.message}
                    </td>
                  </tr>
                ) : filteredTransactions.length ? (
                  filteredTransactions.map((transaction) => (
                    <BookingTransactionRow
                      accounts={accounts}
                      isMutating={updateAccountMutation.isPending && updateAccountMutation.variables?.transactionId === transaction.id}
                      key={transaction.id}
                      onChangeAccount={(paymentAccountId) => {
                        setBookingMessage(null);
                        updateAccountMutation.mutate({ transactionId: String(transaction.id ?? ""), paymentAccountId });
                      }}
                      transaction={transaction}
                    />
                  ))
                ) : (
                  <tr>
                    <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={9}>
                      Keine Buchungen gefunden.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </DataTableShell>
        </div>
      ) : null}

      {activeSubtab === "orders" ? (
        <div className="space-y-5">
          <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <MetricCard icon={ReceiptText} label="Bestellungen" value={formatNumber(Number(ordersQuery.data?.total ?? bookingOrders.length))} />
            <MetricCard icon={Wallet} label="Einnahmen" value={formatCurrencyFromCents(ordersRevenueCents)} />
            <MetricCard icon={Landmark} label="Kosten" value={formatCurrencyFromCents(ordersCostCents)} />
            <MetricCard icon={FileCheck2} label="Belege" value={formatNumber(ordersDocumentCount)} subValue={formatCurrencyFromCents(ordersProfitCents)} />
          </section>

          <PageFilterCard
            description="Diese Filter laufen direkt ueber `/api/bookings/orders`."
            from={orderFilters.from}
            marketplace={orderFilters.marketplace}
            onFromChange={(value) => setOrderFilters((current) => ({ ...current, from: value }))}
            onMarketplaceChange={(value) => setOrderFilters((current) => ({ ...current, marketplace: value }))}
            onQueryChange={(value) => setOrderFilters((current) => ({ ...current, q: value }))}
            onToChange={(value) => setOrderFilters((current) => ({ ...current, to: value }))}
            query={orderFilters.q}
            queryPlaceholder="Order, Kunde, Artikel"
            title="Basisfilter"
            to={orderFilters.to}
          />

          <DataTableShell
            description="Kostenaufschluesselung pro Bestellung mit denselben Backend-Breakdowns wie im Legacy-Dashboard."
            meta={ordersQuery.isLoading ? "Lade..." : `${formatNumber(Number(ordersQuery.data?.total ?? bookingOrders.length))} Zeilen`}
            title="Bestellungen mit Kostenaufschluesselung"
          >
            <table className="min-w-full border-collapse text-sm">
              <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
                <tr>
                  <th className="px-4 py-3 font-medium">Datum</th>
                  <th className="px-4 py-3 font-medium">Channel</th>
                  <th className="px-4 py-3 font-medium">Order</th>
                  <th className="px-4 py-3 font-medium">Kunde</th>
                  <th className="px-4 py-3 font-medium">Einnahmen</th>
                  <th className="px-4 py-3 font-medium">Kosten</th>
                  <th className="px-4 py-3 font-medium">Gewinn</th>
                  <th className="px-4 py-3 font-medium">Belege</th>
                </tr>
              </thead>
              <tbody>
                {ordersQuery.isLoading ? (
                  <tr>
                    <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={8}>
                      Booking-Orders werden geladen...
                    </td>
                  </tr>
                ) : ordersQuery.isError ? (
                  <tr>
                    <td className="px-4 py-4 text-[var(--danger)]" colSpan={8}>
                      Booking-Orders konnten nicht geladen werden: {ordersQuery.error.message}
                    </td>
                  </tr>
                ) : bookingOrders.length ? (
                  bookingOrders.map((order) => <BookingOrderRow key={`${order.marketplace}-${order.external_order_id}`} order={order} />)
                ) : (
                  <tr>
                    <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={8}>
                      Keine Bestellungen fuer aktuellen Filter.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </DataTableShell>
        </div>
      ) : null}

      {activeSubtab === "templates" ? (
        <div className="space-y-5">
          <SurfaceCard description="Neue wiederkehrende Templates inklusive Kontoverknuepfung und Startdatum." title="Neues Template">
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
              <FormField label="Name">
                <input className={fieldClassName} onChange={(event) => setCreateTemplateForm((current) => ({ ...current, name: event.target.value }))} type="text" value={createTemplateForm.name} />
              </FormField>
              <FormField label="Typ">
                <select className={fieldClassName} onChange={(event) => setCreateTemplateForm((current) => ({ ...current, type: event.target.value }))} value={createTemplateForm.type}>
                  {TEMPLATE_TYPE_OPTIONS.map((type) => <option key={type} value={type}>{type}</option>)}
                </select>
              </FormField>
              <FormField label="Richtung">
                <select className={fieldClassName} onChange={(event) => setCreateTemplateForm((current) => ({ ...current, direction: event.target.value }))} value={createTemplateForm.direction}>
                  {TEMPLATE_DIRECTION_OPTIONS.map((direction) => <option key={direction} value={direction}>{direction}</option>)}
                </select>
              </FormField>
              <FormField label="Betrag (EUR)">
                <input className={fieldClassName} inputMode="decimal" onChange={(event) => setCreateTemplateForm((current) => ({ ...current, amount: event.target.value }))} type="text" value={createTemplateForm.amount} />
              </FormField>
              <FormField label="Provider">
                <input className={fieldClassName} onChange={(event) => setCreateTemplateForm((current) => ({ ...current, provider: event.target.value }))} type="text" value={createTemplateForm.provider} />
              </FormField>
              <FormField label="Counterparty">
                <input className={fieldClassName} onChange={(event) => setCreateTemplateForm((current) => ({ ...current, counterpartyName: event.target.value }))} type="text" value={createTemplateForm.counterpartyName} />
              </FormField>
              <FormField label="Startdatum">
                <input className={fieldClassName} onChange={(event) => setCreateTemplateForm((current) => ({ ...current, startDate: event.target.value }))} type="date" value={createTemplateForm.startDate} />
              </FormField>
              <FormField label="Tag im Monat">
                <input className={fieldClassName} max="31" min="1" onChange={(event) => setCreateTemplateForm((current) => ({ ...current, dayOfMonth: event.target.value }))} type="number" value={createTemplateForm.dayOfMonth} />
              </FormField>
              <FormField label="Schedule">
                <select className={fieldClassName} onChange={(event) => setCreateTemplateForm((current) => ({ ...current, schedule: event.target.value }))} value={createTemplateForm.schedule}>
                  {TEMPLATE_SCHEDULE_OPTIONS.map((schedule) => <option key={schedule} value={schedule}>{schedule}</option>)}
                </select>
              </FormField>
              <FormField label="Konto">
                <select className={fieldClassName} onChange={(event) => setCreateTemplateForm((current) => ({ ...current, paymentAccountId: event.target.value }))} value={createTemplateForm.paymentAccountId}>
                  <option value="">Ohne Konto</option>
                  {accounts.map((account) => <option key={account.id} value={account.id}>{account.name} ({account.provider || "-"})</option>)}
                </select>
              </FormField>
              <FormField className="md:col-span-2 xl:col-span-5" label="Notizen">
                <input className={fieldClassName} onChange={(event) => setCreateTemplateForm((current) => ({ ...current, notes: event.target.value }))} type="text" value={createTemplateForm.notes} />
              </FormField>
            </div>
            <div className="mt-4 flex justify-end">
              <Button
                disabled={createTemplateMutation.isPending}
                onClick={() => {
                  const amountCents = parseEuroToCents(createTemplateForm.amount);
                  if (!createTemplateForm.name.trim() || !createTemplateForm.provider.trim() || !amountCents) {
                    setBookingMessage({ tone: "error", text: "Template-Name, Provider und Betrag sind erforderlich." });
                    return;
                  }
                  const dayValue = createTemplateForm.dayOfMonth.trim() ? Number(createTemplateForm.dayOfMonth) : null;
                  setBookingMessage(null);
                  createTemplateMutation.mutate({
                    name: createTemplateForm.name.trim(),
                    type: createTemplateForm.type,
                    direction: createTemplateForm.direction,
                    default_amount_gross: amountCents,
                    currency: "EUR",
                    provider: createTemplateForm.provider.trim(),
                    counterparty_name: createTemplateForm.counterpartyName.trim() || null,
                    schedule: createTemplateForm.schedule,
                    start_date: createTemplateForm.startDate || null,
                    day_of_month: dayValue !== null && Number.isFinite(dayValue) ? Math.max(1, Math.min(31, Math.round(dayValue))) : null,
                    payment_account_id: createTemplateForm.paymentAccountId || null,
                    notes_default: createTemplateForm.notes.trim() || null,
                    active: true,
                  });
                }}
              >
                Template anlegen
              </Button>
            </div>
          </SurfaceCard>

          <DataTableShell description="Templates koennen gespeichert, fuer einen Zeitraum erzeugt oder seit Startdatum nachgezogen werden." meta={templatesQuery.isLoading ? "Lade..." : `${formatNumber(templates.length)} Zeilen`} title="Templates">
            <table className="min-w-full border-collapse text-sm">
              <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
                <tr>
                  <th className="px-4 py-3 font-medium">Name</th>
                  <th className="px-4 py-3 font-medium">Typ</th>
                  <th className="px-4 py-3 font-medium">Richtung</th>
                  <th className="px-4 py-3 font-medium">Counterparty</th>
                  <th className="px-4 py-3 font-medium">Start</th>
                  <th className="px-4 py-3 font-medium">Betrag</th>
                  <th className="px-4 py-3 font-medium">Schedule</th>
                  <th className="px-4 py-3 font-medium">Konto</th>
                  <th className="px-4 py-3 font-medium">Aktiv</th>
                  <th className="px-4 py-3 font-medium">Aktion</th>
                </tr>
              </thead>
              <tbody>
                {templatesQuery.isLoading ? (
                  <tr><td className="px-4 py-4 text-[var(--ink-4)]" colSpan={10}>Templates werden geladen...</td></tr>
                ) : templates.length ? (
                  templates.map((template) => (
                    <TemplateRow
                      accounts={accounts}
                      isRunning={runTemplateMutation.isPending && runTemplateMutation.variables?.templateId === template.id}
                      isSaving={updateTemplateMutation.isPending && updateTemplateMutation.variables?.templateId === template.id}
                      key={template.id}
                      onBackfill={(input) => {
                        setBookingMessage(null);
                        runTemplateMutation.mutate(input);
                      }}
                      onRun={(input) => {
                        setBookingMessage(null);
                        runTemplateMutation.mutate(input);
                      }}
                      onSave={(input) => {
                        setBookingMessage(null);
                        updateTemplateMutation.mutate(input);
                      }}
                      template={template}
                    />
                  ))
                ) : (
                  <tr><td className="px-4 py-4 text-[var(--ink-4)]" colSpan={10}>Keine Templates vorhanden.</td></tr>
                )}
              </tbody>
            </table>
          </DataTableShell>
        </div>
      ) : null}

      {activeSubtab === "accounts" ? (
        <div className="space-y-5">
          <SurfaceCard description="Konten fuer die Zuordnung von Transaktionen und Templates." title="Neues Konto">
            <div className="grid gap-3 md:grid-cols-3">
              <FormField label="Name">
                <input className={fieldClassName} onChange={(event) => setCreateAccountForm((current) => ({ ...current, name: event.target.value }))} type="text" value={createAccountForm.name} />
              </FormField>
              <FormField label="Provider">
                <input className={fieldClassName} onChange={(event) => setCreateAccountForm((current) => ({ ...current, provider: event.target.value }))} type="text" value={createAccountForm.provider} />
              </FormField>
              <FormField label="Aktiv">
                <select className={fieldClassName} onChange={(event) => setCreateAccountForm((current) => ({ ...current, isActive: event.target.value === "true" }))} value={createAccountForm.isActive ? "true" : "false"}>
                  <option value="true">true</option>
                  <option value="false">false</option>
                </select>
              </FormField>
            </div>
            <div className="mt-4 flex justify-end">
              <Button
                disabled={createAccountMutation.isPending}
                onClick={() => {
                  if (!createAccountForm.name.trim()) {
                    setBookingMessage({ tone: "error", text: "Kontoname ist erforderlich." });
                    return;
                  }
                  setBookingMessage(null);
                  createAccountMutation.mutate({
                    name: createAccountForm.name.trim(),
                    provider: createAccountForm.provider.trim() || null,
                    is_active: createAccountForm.isActive,
                  });
                }}
              >
                Konto anlegen
              </Button>
            </div>
          </SurfaceCard>

          <DataTableShell description="Vorhandene Payment Accounts fuer Templates und manuelle Transaktionen." meta={accountsQuery.isLoading ? "Lade..." : `${formatNumber(accounts.length)} Zeilen`} title="Konten">
            <table className="min-w-full border-collapse text-sm">
              <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
                <tr>
                  <th className="px-4 py-3 font-medium">Name</th>
                  <th className="px-4 py-3 font-medium">Provider</th>
                  <th className="px-4 py-3 font-medium">Aktiv</th>
                  <th className="px-4 py-3 font-medium">Aktion</th>
                </tr>
              </thead>
              <tbody>
                {accountsQuery.isLoading ? (
                  <tr><td className="px-4 py-4 text-[var(--ink-4)]" colSpan={4}>Konten werden geladen...</td></tr>
                ) : accounts.length ? (
                  accounts.map((account) => (
                    <AccountRow
                      account={account}
                      isSaving={updatePaymentAccountMutation.isPending && updatePaymentAccountMutation.variables?.paymentAccountId === account.id}
                      key={account.id}
                      onSave={(input) => {
                        setBookingMessage(null);
                        updatePaymentAccountMutation.mutate(input);
                      }}
                    />
                  ))
                ) : (
                  <tr><td className="px-4 py-4 text-[var(--ink-4)]" colSpan={4}>Keine Konten vorhanden.</td></tr>
                )}
              </tbody>
            </table>
          </DataTableShell>
        </div>
      ) : null}

      {activeSubtab === "documents" ? (
        <div className="space-y-5">
          <SurfaceCard description="Neue Belege koennen optional direkt einer bestehenden Transaktion zugeordnet werden." title="Beleg hochladen">
            <div className="grid gap-3 md:grid-cols-3">
              <FormField label="Datei">
                <input accept=".pdf,.png,.jpg,.jpeg,.webp,.txt" className={fileFieldClassName} onChange={(event) => setCreateDocumentForm((current) => ({ ...current, file: event.target.files?.[0] ?? null }))} type="file" />
              </FormField>
              <FormField label="Transaktion (optional)">
                <select className={fieldClassName} onChange={(event) => setCreateDocumentForm((current) => ({ ...current, transactionId: event.target.value }))} value={createDocumentForm.transactionId}>
                  <option value="">Keine Verknuepfung</option>
                  {allTransactionOptions.map((transaction) => (
                    <option key={transaction.id} value={transaction.id}>{transaction.label}</option>
                  ))}
                </select>
              </FormField>
              <FormField label="Notiz">
                <input className={fieldClassName} onChange={(event) => setCreateDocumentForm((current) => ({ ...current, notes: event.target.value }))} type="text" value={createDocumentForm.notes} />
              </FormField>
            </div>
            <div className="mt-4 flex justify-end">
              <Button
                disabled={uploadDocumentMutation.isPending}
                onClick={() => {
                  if (!createDocumentForm.file) {
                    setBookingMessage({ tone: "error", text: "Bitte zuerst eine Datei auswaehlen." });
                    return;
                  }
                  setBookingMessage(null);
                  uploadDocumentMutation.mutate({
                    file: createDocumentForm.file,
                    transaction_id: createDocumentForm.transactionId || null,
                    notes: createDocumentForm.notes.trim() || null,
                  });
                }}
              >
                <FileUp className="mr-2 h-4 w-4" />
                Beleg hochladen
              </Button>
            </div>
          </SurfaceCard>

          <DataTableShell description="Alle hochgeladenen Belege inklusive verknuepfter Transaktionsanzahl." meta={documentsQuery.isLoading ? "Lade..." : `${formatNumber(documents.length)} Zeilen`} title="Belege">
            <table className="min-w-full border-collapse text-sm">
              <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
                <tr>
                  <th className="px-4 py-3 font-medium">Upload</th>
                  <th className="px-4 py-3 font-medium">Original</th>
                  <th className="px-4 py-3 font-medium">Gespeichert</th>
                  <th className="px-4 py-3 font-medium">Verknuepfungen</th>
                  <th className="px-4 py-3 font-medium">Aktion</th>
                </tr>
              </thead>
              <tbody>
                {documentsQuery.isLoading ? (
                  <tr><td className="px-4 py-4 text-[var(--ink-4)]" colSpan={5}>Belege werden geladen...</td></tr>
                ) : documents.length ? (
                  documents.map((document) => <DocumentRow document={document} key={document.id} />)
                ) : (
                  <tr><td className="px-4 py-4 text-[var(--ink-4)]" colSpan={5}>Keine Belege vorhanden.</td></tr>
                )}
              </tbody>
            </table>
          </DataTableShell>
        </div>
      ) : null}
    </div>
  );
}

function MessageBanner({ message }: { message: BookingMessage }) {
  const className =
    message.tone === "ok"
      ? "border-[color:rgba(39,134,86,0.22)] bg-[color:rgba(239,250,244,0.96)] text-[color:#13613f]"
      : message.tone === "info"
        ? "border-[color:rgba(39,104,196,0.18)] bg-[color:rgba(237,245,255,0.92)] text-[color:#234f90]"
        : "border-[color:rgba(183,72,55,0.24)] bg-[color:rgba(255,241,238,0.92)] text-[var(--danger)]";

  return <div className={`rounded-[20px] border px-4 py-3 text-sm ${className}`}>{message.text}</div>;
}

function MetricCard({
  label,
  value,
  icon: Icon,
  subValue,
}: {
  label: string;
  value: string;
  icon: typeof ReceiptText;
  subValue?: string;
}) {
  return (
    <SurfaceCard>
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">{label}</p>
          <p className="mt-3 font-[var(--font-display)] text-2xl font-semibold text-[var(--ink)]">{value}</p>
          {subValue ? <p className="mt-2 text-xs text-[var(--ink-4)]">{subValue}</p> : null}
        </div>
        <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface-2)] p-2.5 text-[var(--ink-3)]">
          <Icon className="h-4 w-4" />
        </div>
      </div>
    </SurfaceCard>
  );
}

function FilterGroup({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div>
      <p className="mb-2 text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">{label}</p>
      <div className="flex flex-wrap gap-2">{children}</div>
    </div>
  );
}

function FormField({ label, children, className = "" }: { label: string; children: ReactNode; className?: string }) {
  return (
    <label className={`block ${className}`}>
      <span className="mb-2 block text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">{label}</span>
      {children}
    </label>
  );
}

function InfoTile({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface-2)] px-4 py-3">
      <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">{label}</p>
      <p className="mt-2 break-words text-sm font-medium text-[var(--ink)]">{value}</p>
    </div>
  );
}

function BookingTransactionRow({
  transaction,
  accounts,
  onChangeAccount,
  isMutating,
}: {
  transaction: BookingTransaction;
  accounts: BookingPaymentAccount[];
  onChangeAccount: (paymentAccountId: string | null) => void;
  isMutating: boolean;
}) {
  const amount = Number(transaction.amount_gross ?? 0);
  const documentHref = transaction.document_id
    ? `/api/bookings/documents/${encodeURIComponent(String(transaction.document_id))}/download?disposition=inline`
    : "";
  const accountOptions = [{ id: "", name: "Ohne Konto", provider: "" }, ...accounts];

  return (
    <tr className="border-t border-[var(--border)] align-top text-[var(--ink-2)]">
      <td className="px-4 py-3 text-[var(--ink)]">{formatDate(transaction.date)}</td>
      <td className="px-4 py-3">
        <div className="font-medium text-[var(--ink)]">{transaction.type ?? "-"}</div>
        <div className="mt-1 text-xs text-[var(--ink-4)]">{BOOKING_TX_CATEGORY_META[bookingTransactionCategory(transaction.type)].label}</div>
      </td>
      <td className="px-4 py-3">{transaction.provider ?? "-"}</td>
      <td className="px-4 py-3">{transaction.direction ?? "-"}</td>
      <td className="px-4 py-3 font-medium text-[var(--ink)]">{formatCurrencyFromCents(amount)}</td>
      <td className="px-4 py-3">
        <div className="font-medium text-[var(--ink)]">{transaction.reference ?? "-"}</div>
        <div className="mt-1 max-w-[220px] text-xs text-[var(--ink-4)]">{transaction.notes ?? transaction.counterparty_name ?? "-"}</div>
      </td>
      <td className="px-4 py-3">
        <div className="capitalize">{transaction.booking_class ?? "-"}</div>
        <div className="mt-1 text-xs text-[var(--ink-4)]">{transaction.status ?? "-"}</div>
      </td>
      <td className="px-4 py-3">
        <select className={fieldClassName} disabled={isMutating} onChange={(event) => onChangeAccount(event.target.value || null)} value={transaction.payment_account_id ?? ""}>
          {accountOptions.map((account) => (
            <option key={account.id || "none"} value={account.id || ""}>
              {account.id ? `${account.name} (${account.provider || "-"})` : "Ohne Konto"}
            </option>
          ))}
        </select>
      </td>
      <td className="px-4 py-3">
        {documentHref ? (
          <a className="text-sm font-medium text-[color:#1a6cc6] underline-offset-4 hover:underline" href={documentHref} rel="noreferrer" target="_blank">
            {transaction.document?.original_filename ?? transaction.document_id}
          </a>
        ) : (
          <span className="text-[var(--ink-4)]">-</span>
        )}
      </td>
    </tr>
  );
}

function BookingOrderRow({ order }: { order: BookingOrderItem }) {
  const profit = Number(order.profit_cents ?? 0);
  const costDetails = [
    `Fees: ${formatCurrencyFromCents(order.fees_cents)}`,
    `Einkauf: ${formatCurrencyFromCents(order.purchase_cents)}`,
    `Zusatz-Buchungen: ${formatCurrencyFromCents(order.bookkeeping_expense_cents)}`,
  ];

  return (
    <tr className="border-t border-[var(--border)] align-top text-[var(--ink-2)]">
      <td className="px-4 py-3 text-[var(--ink)]">{formatDate(order.order_date)}</td>
      <td className="px-4 py-3 capitalize">{order.marketplace ?? "-"}</td>
      <td className="px-4 py-3 font-medium text-[var(--ink)]">{order.external_order_id ?? order.order_id ?? "-"}</td>
      <td className="px-4 py-3">{order.customer ?? "-"}</td>
      <td className="px-4 py-3">
        <div className="font-medium text-[var(--ink)]">{formatCurrencyFromCents(order.revenue_cents)}</div>
        <div className="mt-1 text-xs text-[var(--ink-4)]">Buchungs-In: {formatCurrencyFromCents(order.bookkeeping_income_cents)}</div>
      </td>
      <td className="px-4 py-3">
        <div className="font-medium text-[var(--ink)]">{formatCurrencyFromCents(order.total_costs_cents)}</div>
        {costDetails.map((detail) => (
          <div className="mt-1 text-xs text-[var(--ink-4)]" key={detail}>
            {detail}
          </div>
        ))}
      </td>
      <td className={`px-4 py-3 font-medium ${profit < 0 ? "text-[var(--danger)]" : "text-[var(--success)]"}`}>
        <div>{formatCurrencyFromCents(order.profit_cents)}</div>
        <div className="mt-1 text-xs text-[var(--ink-4)]">Match: {order.bookkeeping_matched_via ?? "none"}</div>
      </td>
      <td className="px-4 py-3">{formatNumber(Number(order.documents_count ?? 0))}</td>
    </tr>
  );
}

function TemplateRow({
  template,
  accounts,
  onSave,
  onRun,
  onBackfill,
  isSaving,
  isRunning,
}: {
  template: BookingTemplate;
  accounts: BookingPaymentAccount[];
  onSave: (input: { templateId: string; payload: { name: string; counterparty_name: string | null; start_date: string | null; default_amount_gross: number; schedule: string; payment_account_id: string | null; active: boolean } }) => void;
  onRun: (input: { templateId: string; periodKey: string }) => void;
  onBackfill: (input: { templateId: string; periodKey: string; backfillFrom: string }) => void;
  isSaving: boolean;
  isRunning: boolean;
}) {
  const [name, setName] = useState(String(template.name ?? ""));
  const [counterpartyName, setCounterpartyName] = useState(String(template.counterparty_name ?? ""));
  const [startDate, setStartDate] = useState(String(template.start_date ?? "").slice(0, 10));
  const [amount, setAmount] = useState(centsToInputValue(template.default_amount_gross));
  const [schedule, setSchedule] = useState(String(template.schedule ?? "monthly"));
  const [paymentAccountId, setPaymentAccountId] = useState(String(template.payment_account_id ?? ""));
  const [active, setActive] = useState(template.active ? "true" : "false");
  const [periodKey, setPeriodKey] = useState(periodKeyFromDateLike(template.start_date) || currentPeriodKey());

  return (
    <tr className="border-t border-[var(--border)] align-top text-[var(--ink-2)]">
      <td className="px-4 py-3"><input className={fieldClassName} onChange={(event) => setName(event.target.value)} type="text" value={name} /></td>
      <td className="px-4 py-3">{template.type ?? "-"}</td>
      <td className="px-4 py-3">{template.direction ?? "-"}</td>
      <td className="px-4 py-3"><input className={fieldClassName} onChange={(event) => setCounterpartyName(event.target.value)} type="text" value={counterpartyName} /></td>
      <td className="px-4 py-3"><input className={fieldClassName} onChange={(event) => setStartDate(event.target.value)} type="date" value={startDate} /></td>
      <td className="px-4 py-3"><input className={fieldClassName} inputMode="decimal" onChange={(event) => setAmount(event.target.value)} type="text" value={amount} /></td>
      <td className="px-4 py-3">
        <select className={fieldClassName} onChange={(event) => setSchedule(event.target.value)} value={schedule}>
          {TEMPLATE_SCHEDULE_OPTIONS.map((option) => <option key={option} value={option}>{option}</option>)}
        </select>
      </td>
      <td className="px-4 py-3">
        <select className={fieldClassName} onChange={(event) => setPaymentAccountId(event.target.value)} value={paymentAccountId}>
          <option value="">Ohne Konto</option>
          {accounts.map((account) => <option key={account.id} value={account.id}>{account.name} ({account.provider || "-"})</option>)}
        </select>
      </td>
      <td className="px-4 py-3">
        <select className={fieldClassName} onChange={(event) => setActive(event.target.value)} value={active}>
          <option value="true">true</option>
          <option value="false">false</option>
        </select>
      </td>
      <td className="px-4 py-3">
        <div className="flex min-w-[190px] flex-col gap-2">
          <input className={fieldClassName} onChange={(event) => setPeriodKey(event.target.value)} type="month" value={periodKey} />
          <div className="flex flex-wrap gap-2">
            <Button
              disabled={isSaving}
              onClick={() => {
                const amountCents = parseEuroToCents(amount);
                if (!name.trim() || !amountCents) return;
                onSave({
                  templateId: String(template.id ?? ""),
                  payload: {
                    name: name.trim(),
                    counterparty_name: counterpartyName.trim() || null,
                    start_date: startDate || null,
                    default_amount_gross: amountCents,
                    schedule,
                    payment_account_id: paymentAccountId || null,
                    active: active === "true",
                  },
                });
              }}
              size="sm"
            >
              Speichern
            </Button>
            <Button disabled={isRunning} onClick={() => onRun({ templateId: String(template.id ?? ""), periodKey: periodKey || currentPeriodKey() })} size="sm" variant="outline">Run</Button>
            <Button
              disabled={isRunning || !periodKeyFromDateLike(startDate)}
              onClick={() => onBackfill({ templateId: String(template.id ?? ""), periodKey: periodKey || currentPeriodKey(), backfillFrom: periodKeyFromDateLike(startDate) })}
              size="sm"
              variant="ghost"
            >
              Seit Start
            </Button>
          </div>
        </div>
      </td>
    </tr>
  );
}

function AccountRow({
  account,
  onSave,
  isSaving,
}: {
  account: BookingPaymentAccount;
  onSave: (input: { paymentAccountId: string; payload: { name: string; provider: string | null; is_active: boolean } }) => void;
  isSaving: boolean;
}) {
  const [name, setName] = useState(String(account.name ?? ""));
  const [provider, setProvider] = useState(String(account.provider ?? ""));
  const [isActive, setIsActive] = useState(account.is_active ? "true" : "false");

  return (
    <tr className="border-t border-[var(--border)] text-[var(--ink-2)]">
      <td className="px-4 py-3"><input className={fieldClassName} onChange={(event) => setName(event.target.value)} type="text" value={name} /></td>
      <td className="px-4 py-3"><input className={fieldClassName} onChange={(event) => setProvider(event.target.value)} type="text" value={provider} /></td>
      <td className="px-4 py-3">
        <select className={fieldClassName} onChange={(event) => setIsActive(event.target.value)} value={isActive}>
          <option value="true">true</option>
          <option value="false">false</option>
        </select>
      </td>
      <td className="px-4 py-3">
        <Button disabled={isSaving} onClick={() => onSave({ paymentAccountId: String(account.id ?? ""), payload: { name: name.trim(), provider: provider.trim() || null, is_active: isActive === "true" } })} size="sm">
          Speichern
        </Button>
      </td>
    </tr>
  );
}

function DocumentRow({ document }: { document: BookingDocument }) {
  const href = `/api/bookings/documents/${encodeURIComponent(String(document.id ?? ""))}/download?disposition=inline`;
  return (
    <tr className="border-t border-[var(--border)] text-[var(--ink-2)]">
      <td className="px-4 py-3">{formatDate(document.uploaded_at)}</td>
      <td className="px-4 py-3 text-[var(--ink)]">{document.original_filename ?? "-"}</td>
      <td className="px-4 py-3">{document.stored_filename ?? "-"}</td>
      <td className="px-4 py-3">{formatNumber(Number(document._count?.transactions ?? 0))}</td>
      <td className="px-4 py-3">
        <a className="text-sm font-medium text-[color:#1a6cc6] underline-offset-4 hover:underline" href={href} rel="noreferrer" target="_blank">
          Oeffnen
        </a>
      </td>
    </tr>
  );
}

function MonthlyInvoiceRow({
  invoice,
  onSave,
  onDelete,
  isSaving,
  isDeleting,
}: {
  invoice: MonthlyInvoice;
  onSave: (input: { invoiceId: string; provider: string; periodKey: string; amountCents: number; notes: string | null; file: File | null }) => void;
  onDelete: (invoiceId: string) => void;
  isSaving: boolean;
  isDeleting: boolean;
}) {
  const [provider, setProvider] = useState(String(invoice.provider ?? "paypal"));
  const [periodKey, setPeriodKey] = useState(periodKeyFromDateLike(invoice.period_from) || currentPeriodKey());
  const [amount, setAmount] = useState(centsToInputValue(invoice.invoice_amount_cents));
  const [notes, setNotes] = useState(String(invoice.notes ?? ""));
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const difference = Number(invoice.difference_cents ?? 0);
  const documentHref = invoice.document_id ? `/api/bookings/documents/${encodeURIComponent(String(invoice.document_id))}/download?disposition=inline` : "";

  return (
    <tr className="border-t border-[var(--border)] align-top text-[var(--ink-2)]">
      <td className="px-4 py-3">
        <select className={fieldClassName} onChange={(event) => setProvider(event.target.value)} value={provider}>
          {MONTHLY_INVOICE_PROVIDER_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
        </select>
      </td>
      <td className="px-4 py-3"><input className={fieldClassName} onChange={(event) => setPeriodKey(event.target.value)} type="month" value={periodKey} /></td>
      <td className="px-4 py-3"><input className={fieldClassName} inputMode="decimal" onChange={(event) => setAmount(event.target.value)} type="text" value={amount} /></td>
      <td className="px-4 py-3">{formatCurrencyFromCents(invoice.calculated_sum_cents)}</td>
      <td className={`px-4 py-3 font-medium ${difference === 0 ? "text-[var(--success)]" : "text-[var(--danger)]"}`}>{formatCurrencyFromCents(invoice.difference_cents)}</td>
      <td className="px-4 py-3">
        <div className="space-y-2">
          {documentHref ? (
            <a className="text-sm font-medium text-[color:#1a6cc6] underline-offset-4 hover:underline" href={documentHref} rel="noreferrer" target="_blank">
              {invoice.document?.original_filename ?? invoice.document_id}
            </a>
          ) : (
            <span className="text-[var(--ink-4)]">Kein Beleg</span>
          )}
          <input accept=".pdf,.png,.jpg,.jpeg,.webp" className={fileFieldClassName} onChange={(event) => setSelectedFile(event.target.files?.[0] ?? null)} type="file" />
        </div>
      </td>
      <td className="px-4 py-3"><input className={fieldClassName} onChange={(event) => setNotes(event.target.value)} type="text" value={notes} /></td>
      <td className="px-4 py-3">
        <div className="flex flex-wrap gap-2">
          <Button
            disabled={isSaving}
            onClick={() => {
              const amountCents = parseEuroToCents(amount);
              if (!amountCents) return;
              onSave({ invoiceId: String(invoice.id ?? ""), provider, periodKey, amountCents, notes: notes.trim() || null, file: selectedFile });
            }}
            size="sm"
          >
            Speichern
          </Button>
          <Button disabled={isDeleting} onClick={() => onDelete(String(invoice.id ?? ""))} size="sm" variant="outline">
            Loeschen
          </Button>
        </div>
      </td>
    </tr>
  );
}

const fieldClassName =
  "w-full rounded-2xl border border-[var(--border-strong)] bg-[var(--surface)] px-3 py-2 text-sm text-[var(--ink)] outline-none";
const fileFieldClassName =
  "block w-full rounded-2xl border border-[var(--border-strong)] bg-[var(--surface)] px-3 py-2 text-sm text-[var(--ink)] file:mr-3 file:rounded-xl file:border-0 file:bg-[var(--surface-2)] file:px-3 file:py-2 file:text-xs file:font-medium";
