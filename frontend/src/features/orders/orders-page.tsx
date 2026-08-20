import { useDashboardShellState } from "@/app/dashboard-shell-state";
import { useDashboardRuntime } from "@/app/dashboard-runtime";
import { formatDateTime, formatMoneyFromCents, NUMBER_FORMATTER } from "@/features/analytics/format";
import { isAmazonFba, marketplaceLabel } from "@/shared/marketplace";
import { useEffect, useMemo, useRef, useState, type ChangeEvent, type ReactNode } from "react";

import {
  fetchOrders,
  type OrdersQuery,
  updateOrderPurchase,
  uploadOrderInvoice,
  type OrderSummary,
} from "./api";

type OrderFilterState = {
  status: Set<string>;
  payment: Set<string>;
  returnsOnly: boolean;
  hideCanceled: boolean;
  hasPurchaseCost: boolean;
  noPurchaseCost: boolean;
  hasInvoice: boolean;
  noInvoice: boolean;
};

type StatusMessage = {
  text: string;
  level: "info" | "ok" | "error";
};

type DraftState = Record<string, string>;
type VatDraftState = Record<string, string>;
type DeductibleDraftState = Record<string, boolean>;
type BusyState = Record<string, boolean>;
type PendingBlurSaveState = Record<string, boolean>;

const ORDERS_PAGE_SIZE = 150;

const EMPTY_FILTERS: OrderFilterState = {
  status: new Set(),
  payment: new Set(),
  returnsOnly: false,
  hideCanceled: true,
  hasPurchaseCost: false,
  noPurchaseCost: false,
  hasInvoice: false,
  noInvoice: false,
};

const STATUS_OPTIONS = [
  "fulfilled",
  "need_to_be_sent",
  "sent",
  "paid",
  "received",
  "cancelled",
  "refunded",
] as const;

const PAYMENT_OPTIONS = [
  "Shopify Payments",
  "PayPal",
  "Kaufland Settlement",
  "Mastercard",
  "Visa",
] as const;

function rowKey(order: OrderSummary) {
  return `${String(order.marketplace || "")}:${String(order.order_id || "")}`;
}

function orderMarketplaceLabel(order: OrderSummary) {
  return isAmazonFba(order.marketplace, order.fulfillment_channel, order.fulfillment_type, order.is_fba)
    ? "Amazon FBA"
    : marketplaceLabel(order.marketplace);
}

function centsToInputValue(cents: number | undefined) {
  const value = Number(cents || 0) / 100;
  return Number.isFinite(value) ? value.toFixed(2) : "";
}


function renderFeeSourceLabel(source: string | undefined): ReactNode {
  if (!source || source === "api" || source === "amazon_finance") {
    return null;
  }

  const tooltip = source === "estimated_fx"
    ? "Geschaetzt inkl. Waehrungsumrechnung - kann ungenau sein"
    : "Geschaetzt - kein API-Wert vorhanden";

  return <span className="fee-estimated" data-tooltip={tooltip}>~ </span>;
}

function parsePurchaseEur(rawValue: string) {
  const raw = String(rawValue || "").trim();
  if (!raw) {
    return { ok: true as const, value: null, cents: 0 };
  }

  const compact = raw.replace(/\s+/g, "");
  const normalized = compact.includes(",") && compact.includes(".")
    ? compact.replace(/\./g, "").replace(",", ".")
    : compact.replace(",", ".");
  const numeric = Number(normalized);
  if (!Number.isFinite(numeric) || numeric < 0) {
    return { ok: false as const, message: "Einkaufspreis ist ungueltig." };
  }

  return {
    ok: true as const,
    value: numeric,
    cents: Math.round(numeric * 100),
  };
}

function isReturnLikeStatus(value: string | undefined) {
  const token = String(value || "").trim().toLowerCase();
  if (!token) {
    return false;
  }
  return [
    "cancel",
    "cancelled",
    "canceled",
    "void",
    "return",
    "returned",
    "refund",
    "refunded",
    "partially_refunded",
    "rma",
    "revoked",
    "returning",
  ].some((keyword) => token.includes(keyword));
}

function getActiveOrdersFilterCount(filters: OrderFilterState) {
  return filters.status.size
    + filters.payment.size
    + (filters.returnsOnly ? 1 : 0)
    + (filters.hasPurchaseCost ? 1 : 0)
    + (filters.noPurchaseCost ? 1 : 0)
    + (filters.hasInvoice ? 1 : 0)
    + (filters.noInvoice ? 1 : 0)
    + (filters.hideCanceled ? 0 : 1);
}

function invoiceHref(order: OrderSummary) {
  const marketplace = String(order.marketplace || "").trim();
  const orderId = String(order.order_id || "").trim();
  const documentId = String(order.invoice?.document_id || "").trim();
  if (!marketplace || !orderId || !documentId) {
    return "";
  }
  return `/api/orders/${encodeURIComponent(marketplace)}/${encodeURIComponent(orderId)}/invoice/${encodeURIComponent(documentId)}/download?disposition=inline`;
}

function cloneFilters(current: OrderFilterState): OrderFilterState {
  return {
    status: new Set(current.status),
    payment: new Set(current.payment),
    returnsOnly: current.returnsOnly,
    hideCanceled: current.hideCanceled,
    hasPurchaseCost: current.hasPurchaseCost,
    noPurchaseCost: current.noPurchaseCost,
    hasInvoice: current.hasInvoice,
    noInvoice: current.noInvoice,
  };
}

type OrdersPageProps = {
  isActive: boolean;
};

export function OrdersPage({ isActive }: OrdersPageProps) {
  const { filters: shellFilters, refreshRequestToken } = useDashboardShellState();
  const { orderDetailsApi } = useDashboardRuntime();
  const [items, setItems] = useState<OrderSummary[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [filters, setFilters] = useState<OrderFilterState>(() => cloneFilters(EMPTY_FILTERS));
  const [filterOpen, setFilterOpen] = useState(false);
  const [statusMessage, setStatusMessage] = useState<StatusMessage | null>(null);
  const [drafts, setDrafts] = useState<DraftState>({});
  const [vatDrafts, setVatDrafts] = useState<VatDraftState>({});
  const [deductibleDrafts, setDeductibleDrafts] = useState<DeductibleDraftState>({});
  const [savingPurchase, setSavingPurchase] = useState<BusyState>({});
  const [uploadingInvoice, setUploadingInvoice] = useState<BusyState>({});
  const [pageIndex, setPageIndex] = useState(0);
  const [debouncedQ, setDebouncedQ] = useState("");
  const skipNextBlurSaveRef = useRef<PendingBlurSaveState>({});
  const lastRefreshRequestTokenRef = useRef(refreshRequestToken);
  const abortRef = useRef<AbortController | null>(null);
  const refreshRequestIdRef = useRef(0);

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedQ(shellFilters.q ?? ""), 300);
    return () => clearTimeout(timer);
  }, [shellFilters.q]);

  const activeStatusFilters = useMemo(() => Array.from(filters.status), [filters.status]);
  const activePaymentFilters = useMemo(() => Array.from(filters.payment), [filters.payment]);

  const statusQueryValue = useMemo(() => {
    if (filters.returnsOnly) {
      return "returns";
    }
    return activeStatusFilters.length === 1 ? activeStatusFilters[0] : "";
  }, [activeStatusFilters, filters.returnsOnly]);

  const query = useMemo<OrdersQuery>(() => ({
    from: shellFilters.from,
    to: shellFilters.to,
    marketplace: shellFilters.marketplace,
    q: debouncedQ,
    status: statusQueryValue || undefined,
    payment: activePaymentFilters,
    hideCanceled: filters.hideCanceled && !filters.returnsOnly && !activeStatusFilters.some((value) => value === "cancelled" || value === "canceled" || value === "refunded"),
    hasPurchaseCost: filters.hasPurchaseCost,
    noPurchaseCost: filters.noPurchaseCost,
    hasInvoice: filters.hasInvoice,
    noInvoice: filters.noInvoice,
    limit: ORDERS_PAGE_SIZE,
    offset: pageIndex * ORDERS_PAGE_SIZE,
  }), [activePaymentFilters, activeStatusFilters, filters.hasInvoice, filters.hasPurchaseCost, filters.hideCanceled, filters.noInvoice, filters.noPurchaseCost, filters.returnsOnly, pageIndex, shellFilters.from, shellFilters.marketplace, debouncedQ, shellFilters.to, statusQueryValue]);

  const currentPage = pageIndex + 1;
  const totalPages = Math.max(1, Math.ceil(total / ORDERS_PAGE_SIZE));
  const pageStart = total > 0 ? (pageIndex * ORDERS_PAGE_SIZE) + 1 : 0;
  const pageEnd = total > 0 ? Math.min(total, pageStart + items.length - 1) : 0;
  const activeFilters = getActiveOrdersFilterCount(filters);
  const metaText = total > 0
    ? `${NUMBER_FORMATTER.format(pageStart)}-${NUMBER_FORMATTER.format(pageEnd)} / ${NUMBER_FORMATTER.format(total)} Zeilen`
    : "0 Zeilen";

  useEffect(() => {
    setPageIndex(0);
  }, [filters, shellFilters.from, shellFilters.marketplace, debouncedQ, shellFilters.to]);

  useEffect(() => {
    if (!isActive) {
      return;
    }
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    const signal = controller.signal;

    setLoading(true);

    fetchOrders(query, signal)
      .then((payload) => {
        if (signal.aborted) return;
        const nextItems = Array.isArray(payload.items) ? payload.items : [];
        applyOrdersPayload(nextItems, Number(payload.total || nextItems.length || 0));
        setError("");
      })
      .catch((nextError: Error) => {
        if (signal.aborted || nextError.name === "AbortError") return;
        setItems([]);
        setTotal(0);
        setError(nextError.message);
      })
      .finally(() => {
        if (!signal.aborted) {
          setLoading(false);
        }
      });

    return () => {
      controller.abort();
    };
  }, [isActive, query]);

  useEffect(() => {
    if (refreshRequestToken === 0 || lastRefreshRequestTokenRef.current === refreshRequestToken) {
      return;
    }
    if (!isActive) {
      return;
    }
    lastRefreshRequestTokenRef.current = refreshRequestToken;
    const refreshRequestId = refreshRequestIdRef.current + 1;
    refreshRequestIdRef.current = refreshRequestId;
    const controller = new AbortController();
    const signal = controller.signal;

    setLoading(true);
    void fetchOrders(query, signal)
      .then((payload) => {
        if (signal.aborted || refreshRequestId !== refreshRequestIdRef.current) {
          return;
        }
        const nextItems = Array.isArray(payload.items) ? payload.items : [];
        applyOrdersPayload(nextItems, Number(payload.total || nextItems.length || 0));
        setError("");
      })
      .catch((nextError: Error) => {
        if (signal.aborted || nextError.name === "AbortError" || refreshRequestId !== refreshRequestIdRef.current) {
          return;
        }
        setItems([]);
        setTotal(0);
        setError(nextError.message);
      })
      .finally(() => {
        if (!signal.aborted && refreshRequestId === refreshRequestIdRef.current) {
          setLoading(false);
        }
      });

    return () => {
      controller.abort();
    };
  }, [isActive, query, refreshRequestToken]);

  useEffect(() => {
    const handleDocumentClick = (event: MouseEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) {
        return;
      }
      const trigger = document.getElementById("ordersFilterBtn");
      const dropdown = document.getElementById("ordersFilterDropdown");
      if (trigger instanceof HTMLElement && dropdown instanceof HTMLElement) {
        if (!trigger.contains(target) && !dropdown.contains(target)) {
          setFilterOpen(false);
        }
      }
    };

    document.addEventListener("click", handleDocumentClick);
    return () => {
      document.removeEventListener("click", handleDocumentClick);
    };
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    const handleShipmentUpdated = (event: Event) => {
      const payload = event instanceof CustomEvent ? event.detail : null;
      const summary = payload && typeof payload === "object"
        ? payload.summary as OrderSummary | undefined
        : undefined;
      const marketplace = String(summary?.marketplace || "").trim();
      const orderId = String(summary?.order_id || "").trim();
      if (!marketplace || !orderId) {
        return;
      }
      const nextKey = `${marketplace}:${orderId}`;
      setItems((current) => current.map((order) => (
        rowKey(order) === nextKey
          ? { ...order, ...summary }
          : order
      )));
    };
    window.addEventListener("orders:shipment-updated", handleShipmentUpdated as EventListener);
    return () => {
      window.removeEventListener("orders:shipment-updated", handleShipmentUpdated as EventListener);
    };
  }, []);

  function setStatus(text: string, level: StatusMessage["level"]) {
    setStatusMessage(text ? { text, level } : null);
  }

  function mutateFilters(mutator: (next: OrderFilterState) => void) {
    setFilters((current) => {
      const next = cloneFilters(current);
      mutator(next);
      return next;
    });
    setPageIndex(0);
  }

  function updateOrder(orderKeyValue: string, updater: (order: OrderSummary) => OrderSummary) {
    setItems((current) => current.map((order) => (rowKey(order) === orderKeyValue ? updater(order) : order)));
  }

  function applyOrdersPayload(nextItems: OrderSummary[], nextTotal?: number) {
    setItems(nextItems);
    setTotal(Number(nextTotal || nextItems.length || 0));
    const nextDrafts: DraftState = {};
    const nextVatDrafts: VatDraftState = {};
    const nextDeductibleDrafts: DeductibleDraftState = {};
    for (const order of nextItems) {
      const key = rowKey(order);
      nextDrafts[key] = key in drafts ? drafts[key] : centsToInputValue(order.purchase_cost_cents);
      nextVatDrafts[key] = key in vatDrafts ? vatDrafts[key] : centsToInputValue(order.purchase_vat_cents);
      nextDeductibleDrafts[key] = key in deductibleDrafts ? deductibleDrafts[key] : Boolean(order.purchase_is_vat_deductible);
    }
    setDrafts(nextDrafts);
    setVatDrafts(nextVatDrafts);
    setDeductibleDrafts(nextDeductibleDrafts);
  }

  function applyUploadedInvoiceToOrder(order: OrderSummary, uploadResult: Record<string, unknown>, purchaseCostCents: number | null) {
    const enrichment = uploadResult.enrichment;
    const enrichmentPayload = enrichment && typeof enrichment === "object"
      ? enrichment as Record<string, unknown>
      : {};
    const documentId = String(enrichmentPayload.invoice_document_id || "").trim();
    const nextInvoice = documentId
      ? {
          document_id: documentId,
          original_filename: String(enrichmentPayload.original_filename || "").trim() || order.invoice?.original_filename,
          stored_filename: String(enrichmentPayload.stored_filename || "").trim() || order.invoice?.stored_filename,
          mime_type: String(enrichmentPayload.mime_type || "").trim() || order.invoice?.mime_type,
          uploaded_at: String(enrichmentPayload.uploaded_at || "").trim() || order.invoice?.uploaded_at,
        }
      : order.invoice ?? null;

    updateOrder(rowKey(order), (currentOrder) => {
      const nextPurchase = purchaseCostCents ?? Number(currentOrder.purchase_cost_cents || 0);
      const nextPurchaseVat = Number(enrichmentPayload.purchase_vat_cents || currentOrder.purchase_vat_cents || 0);
      const nextPurchaseIsVatDeductible = Boolean(
        enrichmentPayload.purchase_is_vat_deductible ?? currentOrder.purchase_is_vat_deductible,
      );
      return {
        ...currentOrder,
        purchase_cost_cents: nextPurchase,
        purchase_vat_cents: nextPurchaseVat,
        purchase_is_vat_deductible: nextPurchaseIsVatDeductible,
        profit_cents: Number(currentOrder.after_fees_cents || 0) - nextPurchase,
        invoice: nextInvoice,
      };
    });
  }

  async function handleSavePurchase(order: OrderSummary) {
    const key = rowKey(order);
    const parsed = parsePurchaseEur(drafts[key] ?? centsToInputValue(order.purchase_cost_cents));
    const parsedVat = parsePurchaseEur(vatDrafts[key] ?? centsToInputValue(order.purchase_vat_cents));
    if (!parsed.ok) {
      setStatus(parsed.message || "Einkaufspreis ist ungueltig.", "error");
      return;
    }
    if (!parsedVat.ok) {
      setStatus(parsedVat.message || "Vorsteuer ist ungueltig.", "error");
      return;
    }

    setSavingPurchase((current) => ({ ...current, [key]: true }));
    try {
      await updateOrderPurchase(String(order.marketplace || ""), String(order.order_id || ""), parsed.value, {
        purchaseVatEur: parsedVat.value,
        purchaseIsVatDeductible: Boolean(deductibleDrafts[key]),
      });
      const nextPurchaseCostCents = parsed.value === null ? 0 : parsed.cents;
      const nextPurchaseVatCents = parsedVat.value === null ? 0 : parsedVat.cents;
      updateOrder(key, (currentOrder) => ({
        ...currentOrder,
        purchase_cost_cents: nextPurchaseCostCents,
        purchase_vat_cents: nextPurchaseVatCents,
        purchase_is_vat_deductible: Boolean(deductibleDrafts[key]),
        profit_cents: Number(currentOrder.after_fees_cents || 0) - nextPurchaseCostCents,
      }));
      setDrafts((current) => ({
        ...current,
        [key]: parsed.value === null ? "" : centsToInputValue(nextPurchaseCostCents),
      }));
      setVatDrafts((current) => ({
        ...current,
        [key]: parsedVat.value === null ? "" : centsToInputValue(nextPurchaseVatCents),
      }));
      setStatus(`Einkauf gespeichert: ${order.marketplace} ${order.order_id}`, "ok");
    } catch (nextError) {
      setStatus(`Speichern fehlgeschlagen: ${nextError instanceof Error ? nextError.message : "Unbekannter Fehler"}`, "error");
    } finally {
      setSavingPurchase((current) => ({ ...current, [key]: false }));
    }
  }

  async function handleInvoiceUpload(order: OrderSummary, file: File | null) {
    if (!file) {
      return;
    }

    const key = rowKey(order);
    const parsed = parsePurchaseEur(drafts[key] ?? centsToInputValue(order.purchase_cost_cents));
    const parsedVat = parsePurchaseEur(vatDrafts[key] ?? centsToInputValue(order.purchase_vat_cents));
    if (!parsed.ok) {
      setStatus(parsed.message || "Einkaufspreis ist ungueltig.", "error");
      return;
    }
    if (!parsedVat.ok) {
      setStatus(parsedVat.message || "Vorsteuer ist ungueltig.", "error");
      return;
    }

    const form = new FormData();
    form.append("file", file);
    if (parsed.value !== null) {
      form.append("purchase_cost_eur", String(parsed.value));
      form.append("purchase_currency", "EUR");
    }
    if (parsedVat.value !== null) {
      form.append("purchase_vat_eur", String(parsedVat.value));
    }
    form.append("purchase_is_vat_deductible", String(Boolean(deductibleDrafts[key])));

    setUploadingInvoice((current) => ({ ...current, [key]: true }));
    try {
      const uploadResult = await uploadOrderInvoice(String(order.marketplace || ""), String(order.order_id || ""), form);
      const nextPurchaseCostCents = parsed.value === null ? null : parsed.cents;
      applyUploadedInvoiceToOrder(order, uploadResult, nextPurchaseCostCents);
      setDrafts((current) => ({
        ...current,
        [key]: parsed.value === null ? "" : centsToInputValue(parsed.cents),
      }));
      setVatDrafts((current) => ({
        ...current,
        [key]: parsedVat.value === null ? "" : centsToInputValue(parsedVat.cents),
      }));
      const priceHint = parsed.value !== null ? " inkl. Einkaufspreis" : "";
      setStatus(`Rechnung hochgeladen${priceHint}: ${order.marketplace} ${order.order_id}`, "ok");
    } catch (nextError) {
      setStatus(`Upload fehlgeschlagen: ${nextError instanceof Error ? nextError.message : "Unbekannter Fehler"}`, "error");
    } finally {
      setUploadingInvoice((current) => ({ ...current, [key]: false }));
    }
  }

  return (
    <div id="ordersPanel" className="tab-panel active" data-react-orders-mounted="true">
      {statusMessage ? (
        <div className={`status ${statusMessage.level === "error" ? "status-error" : statusMessage.level === "ok" ? "status-ok" : "status-info"}`}>
          {statusMessage.text}
        </div>
      ) : null}

      {error ? (
        <section className="card" style={{ marginTop: 12, padding: 16 }}>
          <div className="table-meta" style={{ color: "var(--danger, #c44)" }}>
            Orders konnten nicht geladen werden: {error}
          </div>
        </section>
      ) : null}

      <section className="card table-card" style={{ marginTop: statusMessage || error ? 12 : 0 }}>
        <div className="table-head">
          <h2 className="table-title">Kombinierte Orders</h2>
          <div className="orders-head-actions">
            <div className="orders-filter-wrap">
              <button
                id="ordersFilterBtn"
                className="orders-filter-btn"
                type="button"
                aria-expanded={filterOpen ? "true" : "false"}
                aria-controls="ordersFilterDropdown"
                title="Filter"
                onClick={() => {
                  setFilterOpen((current) => !current);
                }}
              >
                <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M1.5 2.5h13l-5 6v4l-3 1.5V8.5z" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></svg>
                <span className="orders-filter-label">Filter</span>
                <span id="ordersFilterBadge" className="orders-filter-badge" hidden={activeFilters <= 0}>{activeFilters > 0 ? activeFilters : ""}</span>
              </button>
              <div id="ordersFilterDropdown" className="orders-filter-dropdown" aria-hidden={filterOpen ? "false" : "true"}>
                <div className="ofd-section">
                  <div className="ofd-section-title">Status</div>
                  <div className="ofd-chips" id="ordersStatusChips">
                    {STATUS_OPTIONS.map((value) => (
                      <button
                        key={value}
                        className={`ofd-chip${filters.status.has(value) ? " active" : ""}`}
                        data-filter-group="status"
                        data-value={value}
                        type="button"
                        onClick={() => {
                          mutateFilters((next) => {
                            if (next.status.has(value)) {
                              next.status.delete(value);
                            } else {
                              next.status = new Set([value]);
                              next.returnsOnly = false;
                            }
                          });
                        }}
                      >
                        {value}
                      </button>
                    ))}
                  </div>
                </div>
                <div className="ofd-section">
                  <div className="ofd-section-title">Zahlungsart</div>
                  <div className="ofd-chips" id="ordersPaymentChips">
                    {PAYMENT_OPTIONS.map((value) => (
                      <button
                        key={value}
                        className={`ofd-chip${filters.payment.has(value) ? " active" : ""}`}
                        data-filter-group="payment"
                        data-value={value}
                        type="button"
                        onClick={() => {
                          mutateFilters((next) => {
                            if (next.payment.has(value)) {
                              next.payment.delete(value);
                            } else {
                              next.payment.add(value);
                            }
                          });
                        }}
                      >
                        {value}
                      </button>
                    ))}
                  </div>
                </div>
                <div className="ofd-section">
                  <div className="ofd-section-title">Sonstiges</div>
                  <div className="ofd-chips">
                    <button className={`ofd-chip${filters.returnsOnly ? " active" : ""}`} type="button" onClick={() => {
                      mutateFilters((next) => {
                        next.returnsOnly = !next.returnsOnly;
                        if (next.returnsOnly) {
                          next.status.clear();
                        }
                      });
                    }}>Retouren / Cancel</button>
                    <button className={`ofd-chip${filters.hideCanceled ? " active" : ""}`} type="button" onClick={() => {
                      mutateFilters((next) => {
                        next.hideCanceled = !next.hideCanceled;
                      });
                    }}>Stornierte ausblenden</button>
                  </div>
                </div>
                <div className="ofd-section">
                  <div className="ofd-section-title">Einkaufspreis</div>
                  <div className="ofd-chips">
                    <button className={`ofd-chip${filters.hasPurchaseCost ? " active" : ""}`} type="button" onClick={() => {
                      mutateFilters((next) => {
                        next.hasPurchaseCost = !next.hasPurchaseCost;
                        if (next.hasPurchaseCost) {
                          next.noPurchaseCost = false;
                        }
                      });
                    }}>Preis eingetragen</button>
                    <button className={`ofd-chip${filters.noPurchaseCost ? " active" : ""}`} type="button" onClick={() => {
                      mutateFilters((next) => {
                        next.noPurchaseCost = !next.noPurchaseCost;
                        if (next.noPurchaseCost) {
                          next.hasPurchaseCost = false;
                        }
                      });
                    }}>Preis fehlt</button>
                  </div>
                </div>
                <div className="ofd-section">
                  <div className="ofd-section-title">Rechnung</div>
                  <div className="ofd-chips">
                    <button className={`ofd-chip${filters.hasInvoice ? " active" : ""}`} type="button" onClick={() => {
                      mutateFilters((next) => {
                        next.hasInvoice = !next.hasInvoice;
                        if (next.hasInvoice) {
                          next.noInvoice = false;
                        }
                      });
                    }}>Rechnung vorhanden</button>
                    <button className={`ofd-chip${filters.noInvoice ? " active" : ""}`} type="button" onClick={() => {
                      mutateFilters((next) => {
                        next.noInvoice = !next.noInvoice;
                        if (next.noInvoice) {
                          next.hasInvoice = false;
                        }
                      });
                    }}>Rechnung fehlt</button>
                  </div>
                </div>
                <div className="ofd-footer">
                  <button
                    id="ordersFilterClearBtn"
                    className="ofd-clear-btn"
                    type="button"
                    onClick={() => {
                      setFilters(cloneFilters(EMPTY_FILTERS));
                      setPageIndex(0);
                    }}
                  >
                    Alle Filter zuruecksetzen
                  </button>
                </div>
              </div>
            </div>
            <div id="ordersMeta" className="table-meta" style={{ minWidth: 96, textAlign: "right" }}>{loading ? "..." : metaText}</div>
          </div>
        </div>
        <div className="table-wrap">
          <table className="orders-table">
            <thead>
              <tr>
                <th>Datum</th>
                <th>Channel</th>
                <th>Order</th>
                <th>Kunde</th>
                <th>Artikel</th>
                <th>Finanzen</th>
                <th>Einkauf</th>
                <th>Gewinn</th>
                <th>Status</th>
                <th>Rechnung</th>
              </tr>
            </thead>
            <tbody id="ordersBody" data-react-orders-mounted="true">
              {loading ? (
                <tr>
                  <td colSpan={10}>Orders werden geladen...</td>
                </tr>
              ) : items.length ? items.map((order) => {
                const key = rowKey(order);
                const marketplaceToken = String(order.marketplace || "").trim().toLowerCase();
                const rowClass = marketplaceToken === "shopify"
                  ? "order-row-shopify"
                  : marketplaceToken === "kaufland"
                    ? "order-row-kaufland"
                    : "";
                const badgeClass = marketplaceToken === "shopify"
                  ? "badge-invoice"
                  : marketplaceToken === "kaufland"
                    ? "badge-sale"
                    : "badge-default";
                const parsedDraft = parsePurchaseEur(drafts[key] ?? centsToInputValue(order.purchase_cost_cents));
                const previewProfit = parsedDraft.ok
                  ? Number(order.after_fees_cents || 0) - Number(parsedDraft.cents || 0)
                  : Number(order.profit_cents || 0);
                const previewProfitClass = previewProfit < 0 ? "value-neg" : "value-pos";
                const href = invoiceHref(order);

                return (
                  <tr
                    key={key}
                    data-react-orders-row="true"
                    className={rowClass}
                    data-marketplace={String(order.marketplace || "")}
                    data-order-id={String(order.order_id || "")}
                    data-after-fees-cents={String(Number(order.after_fees_cents || 0))}
                    onClick={(event) => {
                      const target = event.target;
                      if (!(target instanceof HTMLElement)) {
                        return;
                      }
                      if (target.closest("input, select, button, a, label, textarea")) {
                        return;
                      }
                      orderDetailsApi?.open(String(order.marketplace || ""), String(order.order_id || ""));
                    }}
                  >
                    <td
                      data-label="Datum"
                      onClick={() => {
                        orderDetailsApi?.open(String(order.marketplace || ""), String(order.order_id || ""));
                      }}
                    >
                      {formatDateTime(order.order_date)}
                    </td>
                    <td data-label="Channel"><span className={`badge ${badgeClass}`}>{orderMarketplaceLabel(order)}</span></td>
                    <td data-label="Order">{String(order.external_order_id || order.order_id || "-")}</td>
                    <td data-label="Kunde">{String(order.customer || "-")}</td>
                    <td data-label="Artikel" title={String(order.article || "-")}>{String(order.article || "-")}{Number(order.line_items_count || 1) > 1 ? <span className="cell-sub"> (+{Number(order.line_items_count || 1) - 1} weitere)</span> : null}</td>
                    <td data-label="Finanzen">
                      <div><strong>{formatMoneyFromCents(Number(order.total_cents || 0))}</strong></div>
                      <div className="cell-sub">After: {formatMoneyFromCents(Number(order.after_fees_cents || 0))}</div>
                      <div className="cell-sub">{renderFeeSourceLabel(order.fee_source)}Fees: {formatMoneyFromCents(Number(order.fees_cents || 0))}</div>
                    </td>
                    <td data-label="Einkauf">
                      <input
                        className="purchase-input"
                        type="number"
                        step="0.01"
                        min="0"
                        value={drafts[key] ?? centsToInputValue(order.purchase_cost_cents)}
                        disabled={Boolean(savingPurchase[key])}
                        onChange={(event) => {
                          const value = event.target.value;
                          setDrafts((current) => ({ ...current, [key]: value }));
                        }}
                        onBlur={() => {
                          if (skipNextBlurSaveRef.current[key]) {
                            delete skipNextBlurSaveRef.current[key];
                            return;
                          }
                          void handleSavePurchase(order);
                        }}
                        onKeyDown={(event) => {
                          if (event.key === "Enter") {
                            event.preventDefault();
                            skipNextBlurSaveRef.current[key] = true;
                            (event.currentTarget as HTMLInputElement).blur();
                            void handleSavePurchase(order);
                          }
                        }}
                      />
                      <div className="cell-sub" style={{ marginTop: 6 }}>
                        <label style={{ display: "flex", alignItems: "center", gap: 6 }}>
                          <span>VSt</span>
                          <input
                            className="purchase-input"
                            style={{ maxWidth: 92 }}
                            type="number"
                            step="0.01"
                            min="0"
                            value={vatDrafts[key] ?? centsToInputValue(order.purchase_vat_cents)}
                            disabled={Boolean(savingPurchase[key])}
                            onChange={(event) => {
                              const value = event.target.value;
                              setVatDrafts((current) => ({ ...current, [key]: value }));
                            }}
                          />
                          <input
                            type="checkbox"
                            checked={Boolean(deductibleDrafts[key])}
                            onChange={(event) => {
                              const checked = event.target.checked;
                              setDeductibleDrafts((current) => ({ ...current, [key]: checked }));
                            }}
                          />
                          <span>abziehbar</span>
                        </label>
                      </div>
                    </td>
                    <td className={`order-profit-cell ${previewProfitClass}`} data-label="Gewinn">{formatMoneyFromCents(previewProfit)}</td>
                    <td data-label="Status">
                      <div>{String(order.fulfillment_status || "-")}</div>
                      <div className="cell-sub">{Boolean(order.vat_applicable) ? "USt aktiv" : "KU / alt"}</div>
                    </td>
                    <td data-label="Rechnung">
                      <div>
                        {href ? (
                          <a className="order-invoice-link" href={href} target="_blank" rel="noreferrer" title={String(order.invoice?.original_filename || "Download")}>
                            {String(order.invoice?.original_filename || "Download")}
                          </a>
                        ) : "-"}
                      </div>
                      <div className="invoice-file-wrap" style={{ marginTop: 5 }}>
                        <label className="file-picker-label">
                          {uploadingInvoice[key] ? "Upload..." : "Datei waehlen"}
                          <input
                            className="invoice-file-input"
                            type="file"
                            accept=".pdf,.png,.jpg,.jpeg,.webp,.txt"
                            disabled={Boolean(uploadingInvoice[key])}
                            onChange={(event: ChangeEvent<HTMLInputElement>) => {
                              const file = event.target.files && event.target.files[0] ? event.target.files[0] : null;
                              void handleInvoiceUpload(order, file).finally(() => {
                                event.target.value = "";
                              });
                            }}
                          />
                        </label>
                      </div>
                    </td>
                  </tr>
                );
              }) : (
                <tr data-react-orders-empty="true">
                  <td colSpan={10}>Keine Orders fuer aktuellen Filter.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        {!loading && totalPages > 1 ? (
          <div className="orders-pagination-row">
            <div className="table-meta">
              {`Seite ${NUMBER_FORMATTER.format(currentPage)} von ${NUMBER_FORMATTER.format(totalPages)}`}
            </div>
            <div className="orders-pagination-actions">
              <button
                id="ordersPrevPageBtn"
                className="btn-inline ghost"
                type="button"
                disabled={pageIndex <= 0}
                onClick={() => {
                  setPageIndex((current) => Math.max(0, current - 1));
                }}
              >
                Vorherige
              </button>
              <button
                id="ordersNextPageBtn"
                className="btn-inline ghost"
                type="button"
                disabled={pageIndex >= totalPages - 1}
                onClick={() => {
                  setPageIndex((current) => Math.min(totalPages - 1, current + 1));
                }}
              >
                Naechste
              </button>
            </div>
          </div>
        ) : null}
      </section>
    </div>
  );
}
