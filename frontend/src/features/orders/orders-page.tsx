import { useDashboardShellState } from "@/app/dashboard-shell-state";
import { useDashboardRuntime } from "@/app/dashboard-runtime";
import { formatMoneyFromCents, NUMBER_FORMATTER } from "@/features/analytics/format";
import { useEffect, useMemo, useRef, useState, type ChangeEvent, type ReactNode } from "react";

import {
  fetchOrders,
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
type BusyState = Record<string, boolean>;

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

function centsToInputValue(cents: number | undefined) {
  const value = Number(cents || 0) / 100;
  return Number.isFinite(value) ? value.toFixed(2) : "";
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

function renderFeeSourceLabel(source: string | undefined): ReactNode {
  if (!source || source === "api") {
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

function filterOrders(items: OrderSummary[], filters: OrderFilterState) {
  return items.filter((order) => {
    const isCanceled = isReturnLikeStatus(order.fulfillment_status)
      || isReturnLikeStatus(order.financial_status)
      || isReturnLikeStatus(order.raw_status);

    if (filters.hideCanceled && !filters.returnsOnly && !filters.status.has("cancelled") && !filters.status.has("refunded") && !filters.status.has("canceled")) {
      if (isCanceled) {
        return false;
      }
    }

    if (filters.status.size) {
      const status = String(order.fulfillment_status || "").trim().toLowerCase();
      if (!filters.status.has(status)) {
        return false;
      }
    }

    if (filters.payment.size) {
      const payment = String(order.payment_method || "").trim();
      if (!filters.payment.has(payment)) {
        return false;
      }
    }

    if (filters.returnsOnly && !isCanceled) {
      return false;
    }

    if (filters.hasPurchaseCost && !(Number(order.purchase_cost_cents) > 0)) {
      return false;
    }
    if (filters.noPurchaseCost && Number(order.purchase_cost_cents) > 0) {
      return false;
    }
    if (filters.hasInvoice && !order.invoice) {
      return false;
    }
    if (filters.noInvoice && order.invoice) {
      return false;
    }

    return true;
  });
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

export function OrdersPage() {
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
  const [savingPurchase, setSavingPurchase] = useState<BusyState>({});
  const [uploadingInvoice, setUploadingInvoice] = useState<BusyState>({});
  const lastRefreshRequestTokenRef = useRef(refreshRequestToken);

  const query = useMemo(() => ({
    from: shellFilters.from,
    to: shellFilters.to,
    marketplace: shellFilters.marketplace,
    q: shellFilters.q,
    limit: 5000,
  }), [shellFilters.from, shellFilters.marketplace, shellFilters.q, shellFilters.to]);

  const filteredItems = useMemo(() => filterOrders(items, filters), [filters, items]);
  const activeFilters = getActiveOrdersFilterCount(filters);
  const metaText = activeFilters > 0 && filteredItems.length !== total
    ? `${NUMBER_FORMATTER.format(filteredItems.length)} / ${NUMBER_FORMATTER.format(total)} Zeilen`
    : `${NUMBER_FORMATTER.format(filteredItems.length)} Zeilen`;

  useEffect(() => {
    let cancelled = false;
    setLoading(true);

    fetchOrders(query)
      .then((payload) => {
        if (cancelled) {
          return;
        }
        const nextItems = Array.isArray(payload.items) ? payload.items : [];
        setItems(nextItems);
        setTotal(Number(payload.total || nextItems.length || 0));
        setDrafts((current) => {
          const next: DraftState = {};
          for (const order of nextItems) {
            const key = rowKey(order);
            next[key] = key in current ? current[key] : centsToInputValue(order.purchase_cost_cents);
          }
          return next;
        });
        setError("");
      })
      .catch((nextError: Error) => {
        if (cancelled) {
          return;
        }
        setItems([]);
        setTotal(0);
        setError(nextError.message);
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [query]);

  useEffect(() => {
    if (refreshRequestToken === 0 || lastRefreshRequestTokenRef.current === refreshRequestToken) {
      return;
    }
    lastRefreshRequestTokenRef.current = refreshRequestToken;

    setLoading(true);
    void fetchOrders(query)
      .then((payload) => {
        const nextItems = Array.isArray(payload.items) ? payload.items : [];
        setItems(nextItems);
        setTotal(Number(payload.total || nextItems.length || 0));
        setError("");
      })
      .catch((nextError: Error) => {
        setItems([]);
        setTotal(0);
        setError(nextError.message);
      })
      .finally(() => {
        setLoading(false);
      });
  }, [query, refreshRequestToken]);

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

  function setStatus(text: string, level: StatusMessage["level"]) {
    setStatusMessage(text ? { text, level } : null);
  }

  function mutateFilters(mutator: (next: OrderFilterState) => void) {
    setFilters((current) => {
      const next = cloneFilters(current);
      mutator(next);
      return next;
    });
  }

  function updateOrder(orderKeyValue: string, updater: (order: OrderSummary) => OrderSummary) {
    setItems((current) => current.map((order) => (rowKey(order) === orderKeyValue ? updater(order) : order)));
  }

  async function handleSavePurchase(order: OrderSummary) {
    const key = rowKey(order);
    const parsed = parsePurchaseEur(drafts[key] ?? centsToInputValue(order.purchase_cost_cents));
    if (!parsed.ok) {
      setStatus(parsed.message || "Einkaufspreis ist ungueltig.", "error");
      return;
    }

    setSavingPurchase((current) => ({ ...current, [key]: true }));
    try {
      await updateOrderPurchase(String(order.marketplace || ""), String(order.order_id || ""), parsed.value);
      const nextPurchaseCostCents = parsed.value === null ? 0 : parsed.cents;
      updateOrder(key, (currentOrder) => ({
        ...currentOrder,
        purchase_cost_cents: nextPurchaseCostCents,
        profit_cents: Number(currentOrder.after_fees_cents || 0) - nextPurchaseCostCents,
      }));
      setDrafts((current) => ({
        ...current,
        [key]: parsed.value === null ? "" : centsToInputValue(nextPurchaseCostCents),
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
    if (!parsed.ok) {
      setStatus(parsed.message || "Einkaufspreis ist ungueltig.", "error");
      return;
    }

    const form = new FormData();
    form.append("file", file);
    if (parsed.value !== null) {
      form.append("purchase_cost_eur", String(parsed.value));
      form.append("purchase_currency", "EUR");
    }

    setUploadingInvoice((current) => ({ ...current, [key]: true }));
    try {
      await uploadOrderInvoice(String(order.marketplace || ""), String(order.order_id || ""), form);
      const refresh = await fetchOrders(query);
      const nextItems = Array.isArray(refresh.items) ? refresh.items : [];
      setItems(nextItems);
      setTotal(Number(refresh.total || nextItems.length || 0));
      setDrafts((current) => {
        const next: DraftState = { ...current };
        for (const nextOrder of nextItems) {
          next[rowKey(nextOrder)] = centsToInputValue(nextOrder.purchase_cost_cents);
        }
        return next;
      });
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
                              next.status.add(value);
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
              ) : filteredItems.length ? filteredItems.map((order) => {
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
                    <td data-label="Channel"><span className={`badge ${badgeClass}`}>{String(order.marketplace || "-")}</span></td>
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
                          void handleSavePurchase(order);
                        }}
                        onKeyDown={(event) => {
                          if (event.key === "Enter") {
                            event.preventDefault();
                            void handleSavePurchase(order);
                          }
                        }}
                      />
                    </td>
                    <td className={`order-profit-cell ${previewProfitClass}`} data-label="Gewinn">{formatMoneyFromCents(previewProfit)}</td>
                    <td data-label="Status">{String(order.fulfillment_status || "-")}</td>
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
      </section>
    </div>
  );
}
