import { useEffect, useState } from "react";

import { formatMoneyFromCents } from "@/features/analytics/format";
import { fetchJson } from "@/shared/api/client";
import { buildDashboardApiUrl } from "@/shared/runtime/base-path";

type AmazonStatus = {
  configured?: boolean;
  missing?: string[];
  counts?: Record<string, number>;
  last_sync?: { completed_at?: string; status?: string; error_message?: string };
};

type InventorySummary = {
  captured_at?: string | null;
  totals?: Record<string, number>;
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
  components?: Array<{ name?: string; amount_cents?: number }>;
};

type FinanceOverview = {
  totals_by_currency?: Record<string, Record<string, number>>;
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
};

type InboundShipmentDetail = {
  shipment: InboundShipment & { plan_id?: string };
  items: Array<{ seller_sku: string; fnsku: string; asin: string; quantity_shipped: number; quantity_received: number }>;
  costs: Array<{ id: string; cost_type: string; amount_cents: number; currency: string; status: string }>;
  invoices: Array<{ id: string; supplier_name: string; invoice_number: string; gross_cents: number; document_path: string }>;
  invoice_lines: Array<{ id: string; invoice_id: string; seller_sku: string; fnsku: string; asin: string; title: string; quantity: number; net_cents: number; vat_cents: number }>;
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

function count(value: number | undefined) {
  return new Intl.NumberFormat("de-DE").format(Number(value || 0));
}

export function AmazonPage() {
  const [status, setStatus] = useState<AmazonStatus | null>(null);
  const [inventory, setInventory] = useState<InventorySummary | null>(null);
  const [finance, setFinance] = useState<FinanceOverview | null>(null);
  const [shipments, setShipments] = useState<InboundShipment[]>([]);
  const [inboundCosts, setInboundCosts] = useState<InboundCost[]>([]);
  const [shipmentFilter, setShipmentFilter] = useState("all");
  const [selectedShipment, setSelectedShipment] = useState<InboundShipmentDetail | null>(null);
  const [shipmentLoading, setShipmentLoading] = useState(false);
  const [invoiceFile, setInvoiceFile] = useState<File | null>(null);
  const [invoiceSupplier, setInvoiceSupplier] = useState("");
  const [invoiceGross, setInvoiceGross] = useState("");
  const [invoiceNet, setInvoiceNet] = useState("");
  const [invoiceVat, setInvoiceVat] = useState("");
  const [invoiceLineNet, setInvoiceLineNet] = useState<Record<string, string>>({});
  const [invoiceMessage, setInvoiceMessage] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    const controller = new AbortController();
    Promise.all([
      fetchJson<AmazonStatus>(buildDashboardApiUrl("/api/amazon/status"), { signal: controller.signal }),
      fetchJson<InventorySummary>(buildDashboardApiUrl("/api/amazon/inventory"), { signal: controller.signal }),
      fetchJson<FinanceOverview>(buildDashboardApiUrl("/api/amazon/finance"), { signal: controller.signal }),
      fetchJson<{ items?: InboundShipment[] }>(buildDashboardApiUrl("/api/amazon/inbound/shipments"), { signal: controller.signal }),
      fetchJson<{ items?: InboundCost[] }>(buildDashboardApiUrl("/api/amazon/inbound/costs"), { signal: controller.signal }),
    ]).then(([nextStatus, nextInventory, nextFinance, nextShipments, nextCosts]) => {
      setStatus(nextStatus);
      setInventory(nextInventory);
      setFinance(nextFinance);
      setShipments(nextShipments.items || []);
      setInboundCosts(nextCosts.items || []);
    }).catch((requestError: unknown) => {
      if ((requestError as Error).name !== "AbortError") {
        setError(requestError instanceof Error ? requestError.message : "Amazon-Daten konnten nicht geladen werden.");
      }
    });
    return () => controller.abort();
  }, []);

  const visibleShipments = shipments.filter((shipment) => {
    if (shipmentFilter === "all") return true;
    if (shipmentFilter === "not_sent") return shipment.status === "READY_TO_SHIP";
    if (shipmentFilter === "in_transit") return ["SHIPPED", "IN_TRANSIT", "DELIVERED", "CHECKED_IN"].includes(shipment.status);
    if (shipmentFilter === "receiving") return shipment.status === "RECEIVING";
    if (shipmentFilter === "received") return shipment.status === "CLOSED";
    return true;
  });

  async function openShipment(shipmentId: string) {
    setShipmentLoading(true);
    setInvoiceMessage("");
    try {
      const detail = await fetchJson<InboundShipmentDetail>(buildDashboardApiUrl(`/api/amazon/inbound/shipments/${encodeURIComponent(shipmentId)}`));
      setSelectedShipment(detail);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Shipment konnte nicht geladen werden.");
    } finally {
      setShipmentLoading(false);
    }
  }

  async function uploadInvoice() {
    if (!selectedShipment || !invoiceFile || !invoiceSupplier.trim()) {
      setInvoiceMessage("Bitte Lieferant und Datei angeben.");
      return;
    }
    const form = new FormData();
    form.append("file", invoiceFile);
    form.append("supplier_name", invoiceSupplier.trim());
    form.append("gross_cents", String(Math.round(Number(invoiceGross.replace(",", ".") || 0) * 100)));
    form.append("net_cents", String(Math.round(Number(invoiceNet.replace(",", ".") || 0) * 100)));
    form.append("vat_cents", String(Math.round(Number(invoiceVat.replace(",", ".") || 0) * 100)));
    try {
      await fetchJson(buildDashboardApiUrl(`/api/amazon/inbound/shipments/${encodeURIComponent(selectedShipment.shipment.shipment_id)}/invoices`), {
        method: "POST",
        body: form,
      });
      setInvoiceFile(null);
      setInvoiceSupplier("");
      setInvoiceGross("");
      setInvoiceNet("");
      setInvoiceVat("");
      setInvoiceMessage("Rechnung gespeichert.");
      await openShipment(selectedShipment.shipment.shipment_id);
      setShipments((current) => current.map((shipment) => shipment.shipment_id === selectedShipment.shipment.shipment_id ? { ...shipment, invoice_count: shipment.invoice_count + 1 } : shipment));
    } catch (requestError) {
      setInvoiceMessage(requestError instanceof Error ? requestError.message : "Rechnung konnte nicht gespeichert werden.");
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

  async function addInvoiceLine(item: InboundShipmentDetail["items"][number]) {
    if (!selectedShipment) {
      return;
    }
    const invoice = selectedShipment.invoices[0];
    const key = `${item.seller_sku}:${item.fnsku}`;
    const netCents = Math.round(Number(String(invoiceLineNet[key] || "0").replace(",", ".")) * 100);
    if (!invoice || netCents < 0 || item.quantity_received <= 0) {
      setInvoiceMessage("Zuerst Rechnung speichern und einen gueltigen Nettobetrag eingeben.");
      return;
    }
    try {
      await fetchJson(buildDashboardApiUrl(`/api/amazon/inbound/invoices/${encodeURIComponent(invoice.id)}/lines`), {
        method: "POST",
        body: JSON.stringify({
          seller_sku: item.seller_sku,
          fnsku: item.fnsku,
          asin: item.asin,
          title: "",
          quantity: item.quantity_received,
          net_cents: netCents,
          vat_cents: 0,
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
      <div className="page-header">
        <div>
          <p className="eyebrow">Amazon FBA</p>
          <h1>Bestand, Beschaffung und Settlement-Pruefung</h1>
          <p className="page-subtitle">Getrennte FBA-Quelle mit FIFO und nicht automatisch gebuchten Finanzvorschlaegen.</p>
        </div>
      </div>
      {error ? <div className="table-meta" style={{ color: "var(--danger, #c44)" }}>{error}</div> : null}
      <div className="kpi-grid">
        <article className="kpi-card"><span>SP-API</span><strong>{status?.configured ? "Verbunden" : "Nicht konfiguriert"}</strong><small>{status?.last_sync?.status || "Noch kein Sync"}</small></article>
        <article className="kpi-card"><span>FBA verfuegbar</span><strong>{count(inventory?.totals?.fulfillable)}</strong><small>dedupliziert je FNSKU</small></article>
        <article className="kpi-card"><span>Inbound</span><strong>{count((inventory?.totals?.inbound_working || 0) + (inventory?.totals?.inbound_shipped || 0))}</strong><small>working + shipped</small></article>
        <article className="kpi-card"><span>FBA-Sendungen</span><strong>{count(shipments.length)}</strong><small>inklusive nicht versendet</small></article>
        <article className="kpi-card"><span>Verkaufserloese (EUR)</span><strong>{formatMoneyFromCents(finance?.totals_by_currency?.EUR?.sales_cents || 0)}</strong><small>aus Amazon-Finanzereignissen</small></article>
        <article className="kpi-card"><span>Amazon-Gebuehren (EUR)</span><strong>{formatMoneyFromCents(finance?.totals_by_currency?.EUR?.fees_cents || 0)}</strong><small>inklusive FBA-Gebuehren</small></article>
      </div>
      <section className="card" style={{ padding: "1.25rem", marginTop: "1rem" }}>
        <div style={{ display: "flex", justifyContent: "space-between", gap: "1rem", alignItems: "center", flexWrap: "wrap" }}>
          <div>
            <h2>FBA-Sendungen</h2>
            <p className="page-subtitle">Das Shipment ist die Beschaffungs- und Wareneingangsquelle. Nicht versendete Sendungen bleiben sichtbar.</p>
          </div>
          <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap" }}>
            {[['all', 'Alle'], ['not_sent', 'Nicht versendet'], ['in_transit', 'Unterwegs'], ['receiving', 'Empfang läuft'], ['received', 'Empfangen']].map(([value, label]) => (
              <button key={value} type="button" className={shipmentFilter === value ? "button button-primary" : "button"} onClick={() => setShipmentFilter(value)}>{label}</button>
            ))}
          </div>
        </div>
        <div className="table-wrap">
          <table className="orders-table">
            <thead><tr><th>Shipment</th><th>Status</th><th>Ziel</th><th>SKUs</th><th>Menge</th><th>Transportangebot</th><th>Belege</th></tr></thead>
            <tbody>
              {visibleShipments.length ? visibleShipments.map((shipment) => (
                <tr key={shipment.shipment_id} onClick={() => void openShipment(shipment.shipment_id)} style={{ cursor: "pointer" }}>
                  <td><strong>{shipment.shipment_id}</strong><br /><small>{shipment.shipment_name || "Amazon FBA"}</small></td>
                  <td><span className="status-badge">{shipment.status_label}</span></td>
                  <td>{shipment.destination_fulfillment_center_id || "-"}</td>
                  <td>{count(shipment.sku_count)}</td>
                  <td>{count(shipment.quantity_received)} / {count(shipment.quantity_shipped)}</td>
                  <td>{shipment.transport_quote_cents == null ? "-" : formatMoneyFromCents(shipment.transport_quote_cents)}</td>
                  <td>{count(shipment.invoice_count)}</td>
                </tr>
              )) : <tr><td colSpan={7}>Noch keine FBA-Sendungen synchronisiert.</td></tr>}
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
      {selectedShipment ? (
        <section className="card" style={{ padding: "1.25rem", marginTop: "1rem" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: "1rem" }}>
            <div><h2>{selectedShipment.shipment.shipment_id}</h2><p className="page-subtitle">{selectedShipment.shipment.status_label} · {count(selectedShipment.shipment.quantity_received)} von {count(selectedShipment.shipment.quantity_shipped)} empfangen</p></div>
            <button type="button" className="button" onClick={() => setSelectedShipment(null)}>Schließen</button>
          </div>
          {shipmentLoading ? <p>Shipment wird geladen...</p> : null}
          <div className="table-wrap">
            <table className="orders-table">
              <thead><tr><th>SKU</th><th>FNSKU</th><th>ASIN</th><th>Empfangen</th><th>Versendet</th></tr></thead>
              <tbody>{selectedShipment.items.map((item) => <tr key={`${item.seller_sku}:${item.fnsku}`}><td>{item.seller_sku}</td><td>{item.fnsku}</td><td>{item.asin || "-"}</td><td>{count(item.quantity_received)}</td><td>{count(item.quantity_shipped)}</td></tr>)}</tbody>
            </table>
          </div>
          <h3>Rechnung für dieses Shipment</h3>
          <div style={{ display: "flex", gap: "0.6rem", alignItems: "center", flexWrap: "wrap" }}>
            <input aria-label="Lieferant" placeholder="Lieferant" value={invoiceSupplier} onChange={(event) => setInvoiceSupplier(event.target.value)} />
            <input aria-label="Brutto" placeholder="Brutto EUR" inputMode="decimal" value={invoiceGross} onChange={(event) => setInvoiceGross(event.target.value)} />
            <input aria-label="Netto" placeholder="Netto EUR" inputMode="decimal" value={invoiceNet} onChange={(event) => setInvoiceNet(event.target.value)} />
            <input aria-label="Umsatzsteuer" placeholder="USt EUR" inputMode="decimal" value={invoiceVat} onChange={(event) => setInvoiceVat(event.target.value)} />
            <input aria-label="Rechnung" type="file" onChange={(event) => setInvoiceFile(event.target.files?.[0] || null)} />
            <button type="button" className="button button-primary" onClick={() => void uploadInvoice()}>Rechnung hochladen</button>
          </div>
          {invoiceMessage ? <p className="table-meta">{invoiceMessage}</p> : null}
           {selectedShipment.invoices.length ? <p className="table-meta">{selectedShipment.invoices.length} Beleg(e) gespeichert.</p> : null}
           <h3>Rechnungspositionen und SKU-Kosten</h3>
           <div className="table-wrap">
             <table className="orders-table">
               <thead><tr><th>SKU</th><th>FNSKU</th><th>Empfangen</th><th>Netto</th><th>Aktion</th></tr></thead>
               <tbody>{selectedShipment.items.map((item) => {
                 const key = `${item.seller_sku}:${item.fnsku}`;
                 const existing = selectedShipment.invoice_lines.find((line) => line.seller_sku === item.seller_sku && line.fnsku === item.fnsku);
                 return <tr key={`cost:${key}`}>
                   <td>{item.seller_sku}</td>
                   <td>{item.fnsku}</td>
                   <td>{count(item.quantity_received)}</td>
                   <td>
                     <input
                       aria-label={`Netto ${item.seller_sku}`}
                       placeholder="Netto EUR"
                       inputMode="decimal"
                       value={existing ? String(existing.net_cents / 100).replace(".", ",") : invoiceLineNet[key] || ""}
                       disabled={Boolean(existing)}
                       onChange={(event) => setInvoiceLineNet((current) => ({ ...current, [key]: event.target.value }))}
                     />
                   </td>
                   <td>{existing ? <span className="table-meta">gespeichert</span> : <button type="button" className="button" onClick={() => void addInvoiceLine(item)}>Position speichern</button>}</td>
                 </tr>;
               })}</tbody>
             </table>
           </div>
           {selectedShipment.cost_allocations.length ? <p className="table-meta">Produktkosten bereits bestaetigt; FIFO-Lots sind erzeugt.</p> : (
             <button
               type="button"
               className="button button-primary"
               disabled={!selectedShipment.invoices.length || selectedShipment.invoice_lines.length !== selectedShipment.items.length || !["RECEIVING", "CLOSED", "DELIVERED"].includes(selectedShipment.shipment.status)}
               onClick={() => void confirmProductCosts()}
             >
               Kosten bestaetigen und FIFO-Lots erzeugen
             </button>
           )}
        </section>
      ) : null}
      <section className="card" style={{ padding: "1.25rem", marginTop: "1rem" }}>
        <h2>Amazon-Einnahmen und Ausgaben</h2>
        <p className="page-subtitle">Ereignisse sind noch nicht automatisch gebucht und muessen vor der Buchhaltung geprueft werden.</p>
        <div className="table-wrap">
          <table className="orders-table">
            <thead><tr><th>Datum</th><th>Typ</th><th>Details</th><th>Erlos</th><th>Gebuehr</th><th>Saldo</th><th>Status</th></tr></thead>
            <tbody>
              {finance?.events?.length ? finance.events.map((event) => <tr key={event.id}><td>{event.posted_date || "-"}</td><td>{event.event_type}</td><td>{event.components?.map((component) => component.name).filter(Boolean).join(", ") || "-"}</td><td>{formatMoneyFromCents(event.sales_cents || 0)}</td><td>{formatMoneyFromCents(event.fees_cents || 0)}</td><td>{formatMoneyFromCents(event.net_cents || 0)}</td><td>{event.financial_finality || "pending"}</td></tr>) : <tr><td colSpan={7}>Noch keine Amazon-Finanzereignisse importiert.</td></tr>}
            </tbody>
          </table>
        </div>
      </section>
    </section>
  );
}
