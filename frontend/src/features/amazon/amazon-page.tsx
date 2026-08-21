import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";

import { formatDateTime, formatMoneyFromCents, formatRelativeTime } from "@/features/analytics/format";
import { fetchJson } from "@/shared/api/client";
import { buildDashboardApiUrl } from "@/shared/runtime/base-path";
import { useDashboardShellState } from "@/app/dashboard-shell-state";
import { useAmazonDetailModal } from "./use-amazon-detail-modal";
import { AmazonInventoryPage } from "./amazon-inventory-page";

type AmazonStatus = {
  configured?: boolean;
  missing?: string[];
  counts?: Record<string, number>;
  last_sync?: { completed_at?: string; status?: string; error_message?: string };
  pending_order_items?: number;
  rate_limits?: Record<string, {
    blocked_until?: string | null;
    last_throttle_at?: string | null;
  }>;
  auto_refresh?: {
    enabled?: boolean;
    in_flight?: boolean;
    tasks?: Record<string, {
      last_status?: string;
      last_finished_at?: string | null;
      last_success_at?: string | null;
      next_eligible_at?: string | null;
      backoff_seconds?: number;
      last_error?: string | null;
    }>;
  };
};

type FinanceEvent = {
  id: string;
  event_type: string;
  posted_date?: string;
  currency?: string;
  sales_cents: number;
  fees_cents: number;
  net_cents: number;
  financial_finality?: string;
  deferral_reason?: string | null;
  maturity_date?: string | null;
  sales_net_cents?: number;
  fees_net_cents?: number;
  fees_vat_cents?: number;
  components?: Array<{ name?: string; amount_cents?: number }>;
};

type FinanceOverview = {
  totals_by_currency?: Record<string, Record<string, number>>;
  operational_totals_by_currency?: Record<string, { sales_net_cents: number; fees_net_cents: number; fees_vat_cents: number }>;
  released_totals_by_currency?: Record<string, { sales_net_cents: number; fees_net_cents: number; fees_vat_cents: number }>;
  events?: FinanceEvent[];
};

type InboundShipment = {
  shipment_id: string;
  shipment_name: string;
  status: string;
  status_label: string;
  destination_fulfillment_center_id?: string;
  quantity_shipped: number;
  quantity_received: number;
  sku_count: number;
  invoice_count: number;
  assigned_cost_cents: number;
  transport_currency?: string | null;
  transport_quote_cents?: number | null;
  cost_status?: "missing" | "entered" | "confirmed";
};

type InboundShipmentDetail = {
  shipment: InboundShipment & { plan_id?: string };
  items: Array<{ seller_sku: string; fnsku: string; asin: string; quantity_shipped: number; quantity_received: number }>;
  costs: Array<{ id: string; cost_type: string; amount_cents: number; currency: string; status: string }>;
  invoices: Array<{ id: string; supplier_name: string; invoice_number: string; gross_cents: number; net_cents: number; vat_cents: number; document_path: string }>;
  invoice_lines: Array<{ id: string; invoice_id: string; seller_sku: string; fnsku: string; asin: string; title: string; quantity: number; gross_cents: number; net_cents: number; vat_cents: number }>;
  cost_allocations: Array<{ id: string; seller_sku: string; fnsku: string; quantity: number; net_cents: number; currency: string; allocation_method: string }>;
};

type InboundCost = {
  id: string;
  shipment_id?: string | null;
  source_event_id?: string | null;
  cost_type: string;
  amount_cents: number;
  currency: string;
  status: string;
};

type InvoiceDraft = {
  file: File;
  supplier: string;
  invoiceNumber: string;
  gross: string;
  net: string;
  vat: string;
  status: "idle" | "uploading" | "error";
  error: string;
};

type InvoiceLineDraft = {
  invoiceId: string;
  gross: string;
  net: string;
  vat: string;
};

const SHIPMENT_FILTER_OPTIONS: Array<{ value: string; label: string }> = [
  { value: "not_sent", label: "Nicht versendet" },
  { value: "in_transit", label: "Unterwegs" },
  { value: "receiving", label: "Empfang läuft" },
  { value: "received", label: "Empfangen" },
];

const FINANCE_EVENT_TYPE_LABELS: Record<string, string> = {
  "ModernTransaction:Shipment": "Verkauf",
  "ModernTransaction:Refund": "Rückerstattung",
  "ModernTransaction:AdjustmentEvent": "Anpassung",
  "ModernTransaction:ServiceFeeEvent": "Servicegebühr",
  SettlementReportLine: "Abrechnung",
};

const FEE_COMPONENT_LABELS: Record<string, string> = {
  FBAPerUnitFulfillmentFee: "FBA-Versandgebühr",
  Commission: "Verkaufsprovision",
  GiftWrapCommission: "Geschenkverpackung",
  ShippingHB: "Versandkosten",
  FBAStorageFee: "FBA-Lagergebühr",
  FBAInboundTransportationFee: "FBA-Einlagerungstransport",
};

function financeEventTypeLabel(eventType: string) {
  return FINANCE_EVENT_TYPE_LABELS[eventType] || eventType;
}

function feeComponentLabel(name: string) {
  return FEE_COMPONENT_LABELS[name] || name;
}

function classNames(...parts: Array<string | false | null | undefined>) {
  return parts.filter(Boolean).join(" ");
}

function count(value: number | undefined) {
  return new Intl.NumberFormat("de-DE").format(Number(value || 0));
}

function shipmentCostStatusLabel(costStatus: InboundShipment["cost_status"]) {
  if (costStatus === "confirmed") {
    return "Kosten bestaetigt";
  }
  if (costStatus === "entered") {
    return "Rechnung erfasst";
  }
  return "Rechnung fehlt";
}

function parseEuroCents(value: string) {
  const normalized = value.trim().replace(",", ".");
  if (!normalized) {
    return null;
  }
  const amount = Number(normalized);
  return Number.isFinite(amount) ? Math.round(amount * 100) : null;
}

function formatCentsInput(cents: number) {
  return String(cents / 100).replace(".", ",");
}

export function AmazonPage() {
  const { refreshRequestToken } = useDashboardShellState();
  const [activeTab, setActiveTab] = useState<"overview" | "inventory">("overview");
  const [status, setStatus] = useState<AmazonStatus | null>(null);
  const [finance, setFinance] = useState<FinanceOverview | null>(null);
  const [shipments, setShipments] = useState<InboundShipment[]>([]);
  const [inboundCosts, setInboundCosts] = useState<InboundCost[]>([]);
  const [shipmentFilters, setShipmentFilters] = useState<Set<string>>(new Set());
  const [shipmentFilterOpen, setShipmentFilterOpen] = useState(false);
  const [selectedShipment, setSelectedShipment] = useState<InboundShipmentDetail | null>(null);
  const [shipmentLoading, setShipmentLoading] = useState(false);
  const [invoiceDrafts, setInvoiceDrafts] = useState<Record<string, InvoiceDraft>>({});
  const [invoiceLineDrafts, setInvoiceLineDrafts] = useState<Record<string, InvoiceLineDraft>>({});
  const [invoiceMessage, setInvoiceMessage] = useState("");
  const [error, setError] = useState("");
  const invoiceFileInputRef = useRef<HTMLInputElement>(null);
  const invoiceDraftIdRef = useRef(0);

  async function refreshAmazonData(signal?: AbortSignal) {
    const controller = signal ? null : new AbortController();
    const requestSignal = signal || controller?.signal;
    try {
      const [nextStatus, nextFinance, nextShipments, nextCosts] = await Promise.all([
        fetchJson<AmazonStatus>(buildDashboardApiUrl("/api/amazon/status"), { signal: requestSignal }),
        fetchJson<FinanceOverview>(buildDashboardApiUrl("/api/amazon/finance"), { signal: requestSignal }),
        fetchJson<{ items?: InboundShipment[] }>(buildDashboardApiUrl("/api/amazon/inbound/shipments"), { signal: requestSignal }),
        fetchJson<{ items?: InboundCost[] }>(buildDashboardApiUrl("/api/amazon/inbound/costs"), { signal: requestSignal }),
      ]);
      setStatus(nextStatus);
      setFinance(nextFinance);
      setShipments(nextShipments.items || []);
      setInboundCosts(nextCosts.items || []);
    } catch (requestError: unknown) {
      if ((requestError as Error).name !== "AbortError") {
        setError(requestError instanceof Error ? requestError.message : "Amazon-Daten konnten nicht geladen werden.");
      }
    }
  }

  useEffect(() => {
    const controller = new AbortController();
    void refreshAmazonData(controller.signal);
    return () => controller.abort();
  }, [refreshRequestToken]);

  const lastUpdatedText = useMemo(() => {
    const tasks = Object.values(status?.auto_refresh?.tasks || {});
    const timestamps = tasks
      .map((task) => task.last_finished_at)
      .filter((value): value is string => Boolean(value))
      .sort()
      .reverse();
    if (!timestamps.length) {
      return "Noch kein Sync";
    }
    return `Zuletzt aktualisiert ${formatRelativeTime(timestamps[0])}`;
  }, [status]);

  const isThrottled = useMemo(() => {
    const tasks = Object.values(status?.auto_refresh?.tasks || {});
    if (tasks.some((task) => Number(task.backoff_seconds || 0) > 0)) {
      return true;
    }
    return Object.values(status?.rate_limits || {}).some((bucket) => Boolean(bucket.blocked_until));
  }, [status]);

  const visibleShipments = shipments.filter((shipment) => {
    if (shipmentFilters.size === 0) {
      return true;
    }
    if (shipmentFilters.has("not_sent") && shipment.status === "READY_TO_SHIP") return true;
    if (shipmentFilters.has("in_transit") && ["SHIPPED", "IN_TRANSIT", "DELIVERED", "CHECKED_IN"].includes(shipment.status)) return true;
    if (shipmentFilters.has("receiving") && shipment.status === "RECEIVING") return true;
    if (shipmentFilters.has("received") && shipment.status === "CLOSED") return true;
    return false;
  });

  const detailTitle = selectedShipment ? selectedShipment.shipment.shipment_id : "";
  const detailPortalTarget = useAmazonDetailModal(activeTab === "overview" && Boolean(selectedShipment), detailTitle, () => {
    setSelectedShipment(null);
    setInvoiceDrafts({});
    setInvoiceLineDrafts({});
  });

  async function openShipment(shipmentId: string, preserveDrafts = false) {
    setShipmentLoading(true);
    setInvoiceMessage("");
    if (!preserveDrafts) {
      setInvoiceDrafts({});
      setInvoiceLineDrafts({});
    }
    try {
      const detail = await fetchJson<InboundShipmentDetail>(buildDashboardApiUrl(`/api/amazon/inbound/shipments/${encodeURIComponent(shipmentId)}`));
      setSelectedShipment(detail);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Shipment konnte nicht geladen werden.");
    } finally {
      setShipmentLoading(false);
    }
  }

  function addInvoiceFiles(files: FileList | null) {
    const selectedFiles = files ? Array.from(files) : [];
    if (!selectedFiles.length) {
      return;
    }
    setInvoiceDrafts((current) => {
      const next = { ...current };
      selectedFiles.forEach((file) => {
        const key = `invoice-draft-${invoiceDraftIdRef.current++}`;
        next[key] = { file, supplier: "", invoiceNumber: "", gross: "", net: "", vat: "", status: "idle", error: "" };
      });
      return next;
    });
    if (invoiceFileInputRef.current) {
      invoiceFileInputRef.current.value = "";
    }
  }

  function updateInvoiceDraft(key: string, patch: Partial<InvoiceDraft>) {
    setInvoiceDrafts((current) => (current[key] ? { ...current, [key]: { ...current[key], ...patch } } : current));
  }

  function removeInvoiceDraft(key: string) {
    setInvoiceDrafts((current) => {
      const next = { ...current };
      delete next[key];
      return next;
    });
  }

  function updateInvoiceLineDraft(key: string, patch: Partial<InvoiceLineDraft>, existing?: InboundShipmentDetail["invoice_lines"][number]) {
    setInvoiceLineDrafts((current) => {
      const initial: InvoiceLineDraft = existing ? {
        invoiceId: existing.invoice_id,
        gross: formatCentsInput(existing.gross_cents),
        net: formatCentsInput(existing.net_cents),
        vat: formatCentsInput(existing.vat_cents),
      } : {
        invoiceId: selectedShipment?.invoices[0]?.id || "",
        gross: "",
        net: "",
        vat: "",
      };
      return { ...current, [key]: { ...(current[key] || initial), ...patch } };
    });
  }

  async function uploadInvoiceDraft(key: string) {
    if (!selectedShipment) {
      return;
    }
    const draft = invoiceDrafts[key];
    if (!draft || !draft.supplier.trim()) {
      updateInvoiceDraft(key, { status: "error", error: "Bitte Lieferant angeben." });
      return;
    }
    const grossCents = parseEuroCents(draft.gross);
    const netCents = parseEuroCents(draft.net);
    const vatCents = parseEuroCents(draft.vat);
    if (grossCents === null || netCents === null || vatCents === null || grossCents < 0 || netCents < 0 || vatCents < 0 || grossCents !== netCents + vatCents) {
      updateInvoiceDraft(key, { status: "error", error: "Brutto muss Netto plus USt entsprechen." });
      return;
    }
    updateInvoiceDraft(key, { status: "uploading", error: "" });
    const form = new FormData();
    form.append("file", draft.file);
    form.append("supplier_name", draft.supplier.trim());
    form.append("invoice_number", draft.invoiceNumber.trim());
    form.append("gross_cents", String(grossCents));
    form.append("net_cents", String(netCents));
    form.append("vat_cents", String(vatCents));
    try {
      await fetchJson(buildDashboardApiUrl(`/api/amazon/inbound/shipments/${encodeURIComponent(selectedShipment.shipment.shipment_id)}/invoices`), {
        method: "POST",
        body: form,
      });
      removeInvoiceDraft(key);
      setInvoiceMessage("Rechnung gespeichert.");
      await openShipment(selectedShipment.shipment.shipment_id, true);
      setShipments((current) => current.map((shipment) => shipment.shipment_id === selectedShipment.shipment.shipment_id ? { ...shipment, invoice_count: shipment.invoice_count + 1 } : shipment));
    } catch (requestError) {
      updateInvoiceDraft(key, { status: "error", error: requestError instanceof Error ? requestError.message : "Rechnung konnte nicht gespeichert werden." });
    }
  }

  async function confirmCost(costId: string, shipmentId: string) {
    try {
      await fetchJson(buildDashboardApiUrl(`/api/amazon/inbound/costs/${encodeURIComponent(costId)}/confirm?shipment_id=${encodeURIComponent(shipmentId)}`), { method: "POST" });
      setInboundCosts((current) => current.map((cost) => cost.id === costId ? { ...cost, shipment_id: shipmentId, status: "confirmed" } : cost));
      await openShipment(shipmentId);
    } catch (requestError) {
      setInvoiceMessage(requestError instanceof Error ? requestError.message : "Kosten konnten nicht zugeordnet werden.");
    }
  }

  async function addInvoiceLine(item: InboundShipmentDetail["items"][number], existing?: InboundShipmentDetail["invoice_lines"][number]) {
    if (!selectedShipment) {
      return;
    }
    const key = `${item.seller_sku}:${item.fnsku}`;
    const draft = invoiceLineDrafts[key] || (existing ? {
      invoiceId: existing.invoice_id,
      gross: formatCentsInput(existing.gross_cents),
      net: formatCentsInput(existing.net_cents),
      vat: formatCentsInput(existing.vat_cents),
    } : {
      invoiceId: selectedShipment.invoices[0]?.id || "",
      gross: "",
      net: "",
      vat: "",
    });
    const grossCents = parseEuroCents(draft.gross);
    const netCents = parseEuroCents(draft.net);
    const vatCents = parseEuroCents(draft.vat);
    if (!draft.invoiceId || grossCents === null || netCents === null || vatCents === null || grossCents < 0 || netCents < 0 || vatCents < 0 || grossCents !== netCents + vatCents || item.quantity_received <= 0) {
      setInvoiceMessage("Zuerst Rechnung waehlen und gueltige Brutto-, Netto- und USt-Betraege eingeben.");
      return;
    }
    try {
      await fetchJson(buildDashboardApiUrl(`/api/amazon/inbound/invoices/${encodeURIComponent(draft.invoiceId)}/lines`), {
        method: "POST",
        body: JSON.stringify({
          seller_sku: item.seller_sku,
          fnsku: item.fnsku,
          asin: item.asin,
          title: "",
          quantity: item.quantity_received,
          gross_cents: grossCents,
          net_cents: netCents,
          vat_cents: vatCents,
        }),
        headers: { "Content-Type": "application/json" },
      });
      setInvoiceMessage("Rechnungsposition gespeichert.");
      await openShipment(selectedShipment.shipment.shipment_id);
    } catch (requestError) {
      setInvoiceMessage(requestError instanceof Error ? requestError.message : "Rechnungsposition konnte nicht gespeichert werden.");
    }
  }

  async function confirmProductCosts() {
    if (!selectedShipment) return;
    try {
      await fetchJson(buildDashboardApiUrl(`/api/amazon/inbound/shipments/${encodeURIComponent(selectedShipment.shipment.shipment_id)}/cost-confirmation`), { method: "POST" });
      setInvoiceMessage("Produktkosten bestaetigt und FIFO-Lots erzeugt.");
      await openShipment(selectedShipment.shipment.shipment_id);
    } catch (requestError) {
      setInvoiceMessage(requestError instanceof Error ? requestError.message : "Produktkosten konnten nicht bestaetigt werden.");
    }
  }

  return (
    <section className="page" aria-label="Amazon FBA">
      {error ? <div className="table-meta" style={{ color: "var(--danger, #c44)" }}>{error}</div> : null}
      <div className="kpi-grid">
        <article className="kpi-card"><span>Operativer Netto-Umsatz (EUR)</span><strong>{formatMoneyFromCents(finance?.operational_totals_by_currency?.EUR?.sales_net_cents || 0)}</strong><small>inklusive vorläufiger Deferred-Verkäufe</small></article>
        <article className="kpi-card"><span>Operative Amazon-Gebühren netto</span><strong>{formatMoneyFromCents(finance?.operational_totals_by_currency?.EUR?.fees_net_cents || 0)}</strong><small>Gebühren-USt. separat: {formatMoneyFromCents(finance?.operational_totals_by_currency?.EUR?.fees_vat_cents || 0)}</small></article>
        <article className="kpi-card"><span>Freigegebener Netto-Umsatz (EUR)</span><strong>{formatMoneyFromCents(finance?.released_totals_by_currency?.EUR?.sales_net_cents || 0)}</strong><small>RELEASED und DEFERRED_RELEASED</small></article>
        <article className="kpi-card"><span>Freigegebene Gebühren netto</span><strong>{formatMoneyFromCents(finance?.released_totals_by_currency?.EUR?.fees_net_cents || 0)}</strong><small>für Settlement verfügbar, nicht zwingend Bankeingang</small></article>
      </div>
      <div id="amazonTabGroup" className="trend-granularity" role="tablist" aria-label="Amazon Ansicht" style={{ marginTop: "1rem" }}>
        <button
          className={classNames("segmented-btn", activeTab === "overview" && "active")}
          type="button"
          onClick={() => setActiveTab("overview")}
        >
          Übersicht
        </button>
        <button
          className={classNames("segmented-btn", activeTab === "inventory" && "active")}
          type="button"
          onClick={() => {
            setActiveTab("inventory");
            setSelectedShipment(null);
          }}
        >
          Bestand
        </button>
      </div>
      {activeTab === "inventory" ? <AmazonInventoryPage /> : null}
      {activeTab === "overview" ? (
        <>
          <section className="card table-card" style={{ marginTop: "1rem" }}>
            <div className="table-head">
              <h2 className="table-title">FBA-Sendungen</h2>
              <div className="orders-head-actions">
                <div className="orders-filter-wrap">
                  <button
                    className="orders-filter-btn"
                    type="button"
                    aria-expanded={shipmentFilterOpen ? "true" : "false"}
                    title="Filter"
                    onClick={() => setShipmentFilterOpen((current) => !current)}
                  >
                    <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M1.5 2.5h13l-5 6v4l-3 1.5V8.5z" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></svg>
                    <span className="orders-filter-label">Filter</span>
                    <span className="orders-filter-badge" hidden={shipmentFilters.size <= 0}>{shipmentFilters.size > 0 ? shipmentFilters.size : ""}</span>
                  </button>
                  <div className="orders-filter-dropdown" aria-hidden={shipmentFilterOpen ? "false" : "true"}>
                    <div className="ofd-section">
                      <div className="ofd-section-title">Status</div>
                      <div className="ofd-chips">
                        {SHIPMENT_FILTER_OPTIONS.map((option) => (
                          <button
                            key={option.value}
                            className={classNames("ofd-chip", shipmentFilters.has(option.value) && "active")}
                            type="button"
                            onClick={() => {
                              setShipmentFilters((current) => {
                                const next = new Set(current);
                                if (next.has(option.value)) {
                                  next.delete(option.value);
                                } else {
                                  next.add(option.value);
                                }
                                return next;
                              });
                            }}
                          >
                            {option.label}
                          </button>
                        ))}
                      </div>
                    </div>
                    <div className="ofd-footer">
                      <button className="ofd-clear-btn" type="button" onClick={() => setShipmentFilters(new Set())}>
                        Alle Filter zuruecksetzen
                      </button>
                    </div>
                  </div>
                </div>
                <div className="table-meta" style={{ minWidth: 96, textAlign: "right" }}>
                  {lastUpdatedText}
                  {isThrottled ? <span title="Amazon-Limit aktiv" style={{ marginLeft: 6 }}>⚠</span> : null}
                  {status?.pending_order_items ? <div className="cell-sub">{count(status.pending_order_items)} Bestellung(en) ohne Artikeldaten</div> : null}
                </div>
              </div>
            </div>
            <div className="table-wrap">
              <table className="amazon-shipment-table">
                <thead><tr><th>Shipment</th><th>Status</th><th>Menge</th><th>Kosten</th></tr></thead>
                <tbody>
                  {visibleShipments.length ? visibleShipments.map((shipment) => (
                    <tr key={shipment.shipment_id} onClick={() => void openShipment(shipment.shipment_id)} style={{ cursor: "pointer" }}>
                      <td>
                        <strong>{shipment.shipment_id}</strong>
                        <div className="cell-sub">{shipment.shipment_name || "Amazon FBA"}</div>
                        <div className="cell-sub">
                          Ziel: {shipment.destination_fulfillment_center_id || "-"} · Angebot: {shipment.transport_quote_cents == null ? "-" : formatMoneyFromCents(shipment.transport_quote_cents)}
                        </div>
                      </td>
                      <td><span className="status-badge">{shipment.status_label}</span></td>
                      <td>{count(shipment.quantity_received)} / {count(shipment.quantity_shipped)}</td>
                      <td><span className="status-badge">{shipmentCostStatusLabel(shipment.cost_status)}</span></td>
                    </tr>
                  )) : <tr><td colSpan={4}>Noch keine FBA-Sendungen synchronisiert.</td></tr>}
                </tbody>
              </table>
            </div>
          </section>
          {inboundCosts.some((cost) => !cost.shipment_id) ? (
            <section className="card" style={{ padding: "1.25rem", marginTop: "1rem" }}>
              <h2>Amazon-Inbound-Kosten ohne Shipment-Zuordnung</h2>
              <p className="page-subtitle">Diese Finance-Ereignisse sind echt, werden aber erst nach deiner Bestätigung den Einstandskosten zugerechnet.</p>
              <div className="table-wrap">
                <table className="orders-table">
                  <thead><tr><th>Typ</th><th>Betrag</th><th>Status</th><th>Zuordnen</th></tr></thead>
                  <tbody>{inboundCosts.filter((cost) => !cost.shipment_id).map((cost) => <tr key={cost.id}>
                    <td>{cost.cost_type}</td>
                    <td>{formatMoneyFromCents(cost.amount_cents)}</td>
                    <td>{cost.status}</td>
                    <td>{selectedShipment ? <button type="button" className="button" onClick={() => void confirmCost(cost.id, selectedShipment.shipment.shipment_id)}>Dem geöffneten Shipment zuordnen</button> : <span className="table-meta">Shipment öffnen</span>}</td>
                  </tr>)}</tbody>
                </table>
              </div>
            </section>
          ) : null}
          <section className="card" style={{ padding: "1.25rem", marginTop: "1rem" }}>
            <h2>Amazon-Einnahmen und Ausgaben</h2>
            <p className="page-subtitle">Ereignisse sind noch nicht automatisch gebucht und muessen vor der Buchhaltung geprueft werden.</p>
            <div className="table-wrap">
              <table className="orders-table">
                <thead><tr><th>Datum</th><th>Typ</th><th>Details</th><th>Erlos</th><th>Gebuehr</th><th>Saldo</th><th>Status</th></tr></thead>
                <tbody>
                  {finance?.events?.length ? finance.events.map((event) => (
                    <tr key={event.id}>
                      <td>{formatDateTime(event.posted_date)}</td>
                      <td>{financeEventTypeLabel(event.event_type)}</td>
                      <td>
                        <div>{event.components?.map((component) => feeComponentLabel(component.name || "")).filter(Boolean).join(", ") || "-"}</div>
                        <div className="cell-sub">Netto-Umsatz {formatMoneyFromCents(event.sales_net_cents || 0)} · Fee netto {formatMoneyFromCents(event.fees_net_cents || 0)} · Fee-USt. {formatMoneyFromCents(event.fees_vat_cents || 0)}</div>
                        {event.financial_finality === "deferred" ? <div className="cell-sub">Vorläufig{event.deferral_reason ? ` · ${event.deferral_reason}` : ""}{event.maturity_date ? ` · geplant ${formatDateTime(event.maturity_date)}` : ""}</div> : null}
                      </td>
                      <td>{formatMoneyFromCents(event.sales_cents || 0)}</td>
                      <td>{formatMoneyFromCents(event.fees_cents || 0)}</td>
                      <td>{formatMoneyFromCents(event.net_cents || 0)}</td>
                      <td>{event.financial_finality || "pending"}</td>
                    </tr>
                  )) : <tr><td colSpan={7}>Noch keine Amazon-Finanzereignisse importiert.</td></tr>}
                </tbody>
              </table>
            </div>
          </section>
        </>
      ) : null}
      {activeTab === "overview" && detailPortalTarget && selectedShipment ? createPortal(
        <div className="amazon-shipment-modal">
          {shipmentLoading ? <p>Shipment wird geladen...</p> : null}
          <p className="page-subtitle">{selectedShipment.shipment.status_label} · {count(selectedShipment.shipment.quantity_received)} von {count(selectedShipment.shipment.quantity_shipped)} empfangen</p>
          <div className="detail-table-wrap">
            <table className="amazon-shipment-detail-table">
              <thead><tr><th>SKU</th><th>FNSKU</th><th>ASIN</th><th>Empfangen</th><th>Versendet</th></tr></thead>
              <tbody>{selectedShipment.items.map((item) => <tr key={`${item.seller_sku}:${item.fnsku}`}><td>{item.seller_sku}</td><td>{item.fnsku}</td><td>{item.asin || "-"}</td><td>{count(item.quantity_received)}</td><td>{count(item.quantity_shipped)}</td></tr>)}</tbody>
            </table>
          </div>
          <h3>Rechnungen für dieses Shipment</h3>
          <label className="file-picker-label">
            Dateien waehlen
            <input
              ref={invoiceFileInputRef}
              className="invoice-file-input"
              type="file"
              multiple
              onChange={(event) => addInvoiceFiles(event.target.files)}
            />
          </label>
          {Object.entries(invoiceDrafts).map(([key, draft]) => (
            <div key={key} className="detail-card" style={{ marginTop: "0.75rem" }}>
              <div className="table-meta">{draft.file.name}</div>
              <div style={{ display: "flex", gap: "0.6rem", alignItems: "center", flexWrap: "wrap", marginTop: "0.4rem" }}>
                <input aria-label={`Lieferant ${draft.file.name}`} placeholder="Lieferant" value={draft.supplier} onChange={(event) => updateInvoiceDraft(key, { supplier: event.target.value })} />
                <input aria-label={`Rechnungsnummer ${draft.file.name}`} placeholder="Rechnungsnr." value={draft.invoiceNumber} onChange={(event) => updateInvoiceDraft(key, { invoiceNumber: event.target.value })} />
                <input aria-label={`Brutto ${draft.file.name}`} placeholder="Brutto EUR" inputMode="decimal" value={draft.gross} onChange={(event) => updateInvoiceDraft(key, { gross: event.target.value })} />
                <input aria-label={`Netto ${draft.file.name}`} placeholder="Netto EUR" inputMode="decimal" value={draft.net} onChange={(event) => updateInvoiceDraft(key, { net: event.target.value })} />
                <input aria-label={`USt ${draft.file.name}`} placeholder="USt EUR" inputMode="decimal" value={draft.vat} onChange={(event) => updateInvoiceDraft(key, { vat: event.target.value })} />
                <button type="button" className="button button-primary" disabled={draft.status === "uploading"} onClick={() => void uploadInvoiceDraft(key)}>
                  {draft.status === "uploading" ? "Lädt..." : "Hochladen"}
                </button>
                <button type="button" className="button" onClick={() => removeInvoiceDraft(key)}>Entfernen</button>
              </div>
              {draft.error ? <p className="table-meta" style={{ color: "var(--danger, #c44)" }}>{draft.error}</p> : null}
            </div>
          ))}
          {invoiceMessage ? <p className="table-meta">{invoiceMessage}</p> : null}
          {selectedShipment.invoices.length ? <p className="table-meta">{selectedShipment.invoices.length} Beleg(e) gespeichert.</p> : null}
          <h3>Rechnungspositionen und SKU-Kosten</h3>
          <div className="detail-table-wrap">
            <table className="amazon-shipment-detail-table">
              <thead><tr><th>SKU</th><th>FNSKU</th><th>Empfangen</th><th>Rechnung</th><th>Brutto</th><th>Netto</th><th>USt</th><th>Aktion</th></tr></thead>
              <tbody>{selectedShipment.items.map((item) => {
                const key = `${item.seller_sku}:${item.fnsku}`;
                const existing = selectedShipment.invoice_lines.find((line) => line.seller_sku === item.seller_sku && line.fnsku === item.fnsku);
                const lineDraft = invoiceLineDrafts[key] || (existing ? {
                  invoiceId: existing.invoice_id,
                  gross: formatCentsInput(existing.gross_cents),
                  net: formatCentsInput(existing.net_cents),
                  vat: formatCentsInput(existing.vat_cents),
                } : {
                  invoiceId: selectedShipment.invoices[0]?.id || "",
                  gross: "",
                  net: "",
                  vat: "",
                });
                const confirmed = selectedShipment.cost_allocations.length > 0;
                const invoice = selectedShipment.invoices.find((candidate) => candidate.id === lineDraft.invoiceId);
                return <tr key={`cost:${key}`}>
                  <td>{item.seller_sku}</td>
                  <td>{item.fnsku}</td>
                  <td>{count(item.quantity_received)}</td>
                  {confirmed ? <>
                    <td>{invoice?.invoice_number || "-"}</td>
                    <td>{formatMoneyFromCents(existing?.gross_cents || 0)}</td>
                    <td>{formatMoneyFromCents(existing?.net_cents || 0)}</td>
                    <td>{formatMoneyFromCents(existing?.vat_cents || 0)}</td>
                    <td><span className="table-meta">bestaetigt</span></td>
                  </> : <>
                    <td>
                      <select
                        aria-label={`Rechnung ${item.seller_sku}`}
                        value={lineDraft.invoiceId}
                        disabled={Boolean(existing)}
                        onChange={(event) => updateInvoiceLineDraft(key, { invoiceId: event.target.value }, existing)}
                      >
                        <option value="">Rechnung waehlen</option>
                        {selectedShipment.invoices.map((candidate) => <option key={candidate.id} value={candidate.id}>{candidate.invoice_number || candidate.supplier_name || candidate.id}</option>)}
                      </select>
                    </td>
                    <td><input aria-label={`Brutto ${item.seller_sku}`} placeholder="Brutto EUR" inputMode="decimal" value={lineDraft.gross} onChange={(event) => updateInvoiceLineDraft(key, { gross: event.target.value }, existing)} /></td>
                    <td><input aria-label={`Netto ${item.seller_sku}`} placeholder="Netto EUR" inputMode="decimal" value={lineDraft.net} onChange={(event) => updateInvoiceLineDraft(key, { net: event.target.value }, existing)} /></td>
                    <td><input aria-label={`USt ${item.seller_sku}`} placeholder="USt EUR" inputMode="decimal" value={lineDraft.vat} onChange={(event) => updateInvoiceLineDraft(key, { vat: event.target.value }, existing)} /></td>
                    <td><button type="button" className="button" onClick={() => void addInvoiceLine(item, existing)}>{existing ? "Position aktualisieren" : "Position speichern"}</button></td>
                  </>}
                </tr>;
              })}</tbody>
            </table>
          </div>
          {selectedShipment.cost_allocations.length ? <p className="table-meta">Produktkosten bereits bestaetigt; FIFO-Lots sind erzeugt.</p> : (
            <button
              type="button"
              className="button button-primary"
              disabled={!selectedShipment.invoices.length || selectedShipment.invoice_lines.length !== selectedShipment.items.filter((item) => item.quantity_received > 0).length || !["RECEIVING", "CLOSED"].includes(selectedShipment.shipment.status)}
              onClick={() => void confirmProductCosts()}
            >
              Kosten bestaetigen und FIFO-Lots erzeugen
            </button>
          )}
        </div>,
        detailPortalTarget,
      ) : null}
    </section>
  );
}
