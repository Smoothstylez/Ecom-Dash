import { useEffect, useRef, useState } from "react";
import { withAdminHeaders } from "@/shared/api/admin-auth";
import { fetchJson } from "@/shared/api/client";
import { buildDashboardApiUrl } from "@/shared/runtime/base-path";
import { formatMoneyFromCents as money } from "@/features/analytics/format";
import "./amazon-pool.css";

type Product = { id: string; name: string; available_quantity: number; reserved_quantity: number; in_transit_quantity: number; amazon_costed_quantity: number; average_home_cost_cents: number | null; sales_net_cents: number; margin_cents: number; margin_complete: boolean };
type Invoice = { id: string; supplier: string; number: string; invoice_date: string; currency: string; gross_cents: number; notes?: string };
type Line = { id: string; invoice_id: string; product_id: string; quantity: number; tax_status: string; gross_cents: number; net_cents: number; vat_cents: number; effective_cost_cents: number; deductible_vat_cents: number | null };
type Receipt = { id: string; line_id: string; quantity: number; available_quantity: number };
type Item = { id: string; shipment_id: string; seller_sku: string; quantity_shipped: number; quantity_received: number; status: string };
type Listing = { marketplace_id: string; seller_sku: string; asin: string; title?: string; product_id?: string };
type Transfer = { id: string; shipment_id: string; status: string; package_reference: string; freight_cents: number };
type Metrics = { marketplace_id: string; seller_sku: string; sales_net_cents: number; fees_net_cents: number; cogs_cents: number; margin_cents: number; margin_complete: boolean };
type Pool = { available_costs: { id: string; shipment_id: string; amount_cents: number; cost_type: string }[]; products: Product[]; invoices: Invoice[]; lines: Line[]; receipts: Receipt[]; shipment_items: Item[]; available_listings: Listing[]; listings: Listing[]; transfers: Transfer[]; transfer_lines: { transfer_id: string; quantity: number; received_quantity: number }[]; payments: { invoice_id: string; amount_cents: number }[]; documents: { id: string; invoice_id: string; filename: string }[]; listing_metrics: Metrics[]; shipment_metrics: (Metrics & { shipment_id: string; currency: string; quantity_sold: number })[]; legacy_invoices: { id: string; invoice_number: string; supplier_name: string; shipment_id: string }[]; issues: { shipment_id: string; quantity_received: number; booked_quantity: number }[] };
type DraftLine = { product_id: string; quantity: string; gross: string; vat: string; tax: string };
const moneyIn = (value: number, currency: string) => new Intl.NumberFormat("de-DE", { style: "currency", currency }).format(value / 100);
const today = () => new Date().toISOString().slice(0, 10);
const api = (path = "") => buildDashboardApiUrl(`/api/amazon/pool${path}`);
const cents = (text: string) => {
  const normalized = text.trim().replace(",", ".");
  if (!/^\d+(\.\d{1,2})?$/.test(normalized)) throw new Error("Bitte einen positiven Geldbetrag mit höchstens zwei Nachkommastellen eingeben.");
  return Math.round(Number(normalized) * 100);
};
const initialLine = (): DraftLine => ({ product_id: "", quantity: "1", gross: "", vat: "0", tax: "review" });

export function AmazonPoolPage() {
  const retryKeys = useRef(new Map<string, string>());
  const [data, setData] = useState<Pool | null>(null);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [busy, setBusy] = useState(false);
  const [tab, setTab] = useState<"products" | "purchases" | "shipments">("purchases");
  const [productName, setProductName] = useState("");
  const [mapProduct, setMapProduct] = useState("");
  const [mapListing, setMapListing] = useState("");
  const [supplier, setSupplier] = useState("");
  const [number, setNumber] = useState("");
  const [invoiceDate, setInvoiceDate] = useState(today());
  const [currency, setCurrency] = useState("EUR");
  const [invoiceNotes, setInvoiceNotes] = useState("");
  const [fx, setFx] = useState("1");
  const [fxReference, setFxReference] = useState("");
  const [freight, setFreight] = useState("0");
  const [draftLines, setDraftLines] = useState<DraftLine[]>([initialLine()]);
  const [selectedInvoice, setSelectedInvoice] = useState("");
  const [receiptLine, setReceiptLine] = useState("");
  const [receiptQty, setReceiptQty] = useState("1");
  const [receiptDate, setReceiptDate] = useState(today());
  const [payment, setPayment] = useState("");
  const [paymentDate, setPaymentDate] = useState(today());
  const [account, setAccount] = useState("");
  const [shipment, setShipment] = useState("");
  const [marketplace, setMarketplace] = useState("");
  const [packageRef, setPackageRef] = useState("");
  const [quantities, setQuantities] = useState<Record<string, string>>({});
  const [dispatchCosts, setDispatchCosts] = useState<Record<string, string>>({});
  const [sourceCosts, setSourceCosts] = useState<Record<string, string>>({});
  const [transportReason, setTransportReason] = useState("");
  const [transportPreview, setTransportPreview] = useState<{ payload: object; old_cost_cents: number; new_cost_cents: number } | null>(null);
  const [dispatchDate, setDispatchDate] = useState(today());
  const [correctionLine, setCorrectionLine] = useState("");
  const [correctionGross, setCorrectionGross] = useState("");
  const [correctionVat, setCorrectionVat] = useState("");
  const [correctionDeductible, setCorrectionDeductible] = useState("");
  const [correctionReason, setCorrectionReason] = useState("");
  const [preview, setPreview] = useState<{ payload: object; old_cost_cents: number; new_cost_cents: number } | null>(null);
  const [legacyInvoice, setLegacyInvoice] = useState("");
  const [freightShares, setFreightShares] = useState("");
  const [file, setFile] = useState<File | null>(null);
  const reload = async () => setData(await fetchJson<Pool>(api()));
  useEffect(() => { const c = new AbortController(); void fetchJson<Pool>(api(), { signal: c.signal }).then(setData).catch(e => { if (!c.signal.aborted) setError(String(e.message)); }); return () => c.abort(); }, []);
  const run = async (action: () => Promise<unknown>, success: string) => {
    setBusy(true); setError(""); setMessage("");
    try { await action(); await reload(); setMessage(success); } catch (e) { setError(e instanceof Error ? e.message : String(e)); await reload().catch(() => undefined); } finally { setBusy(false); }
  };
  const post = async <T,>(path: string, body: object): Promise<T> => {
    const key = JSON.stringify([path, body]);
    const requestId = retryKeys.current.get(key) || crypto.randomUUID();
    retryKeys.current.set(key, requestId);
    const result = await fetchJson<T>(api(path), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ request_id: requestId, ...body }) });
    const status = (result as { bookkeeping?: { status: string } })?.bookkeeping?.status;
    if (status === "unavailable") throw new Error("Im Einkaufspool gespeichert. Buchhaltungsanbindung derzeit nicht verfügbar; später erneut abgleichen.");
    retryKeys.current.delete(key);
    return result;
  };
  const upload = async (id: string, document: File) => { const body = new FormData(); body.append("file", document); return fetchJson(api(`/invoices/${id}/documents`), { method: "POST", body }); };
  const productOptions = data?.products.map(p => <option key={p.id} value={p.id}>{p.name}</option>);
  const chosen = data?.invoices.find(i => i.id === selectedInvoice);
  const name = (id: string) => data?.products.find(p => p.id === id)?.name || id;
  const modifyLine = (index: number, changes: Partial<DraftLine>) => setDraftLines(lines => lines.map((l, i) => i === index ? { ...l, ...changes } : l));
  if (!data) return <p role="status">{error || "Einkaufspool wird geladen …"}</p>;

  return <section className="amazon-pool">
    <header><h2>Einkaufspool</h2><p>Einkäufe erfassen, Bestand bei dir verfolgen und Teilmengen auf FBA-Sendungen verteilen.</p></header>
    {error && <p role="alert" className="pool-error">{error}</p>}{message && <p role="status">{message}</p>}
    <nav aria-label="Einkaufspool"><button className="button" onClick={() => setTab("purchases")}>Einkäufe</button><button className="button" onClick={() => setTab("products")}>Produkte & Listings</button><button className="button" onClick={() => setTab("shipments")}>FBA-Zuordnung</button></nav>
    <fieldset disabled={busy}>
    {tab === "products" && <>
      <form onSubmit={e => { e.preventDefault(); void run(async () => { await post("/products", { name: productName }); setProductName(""); }, "Produkt angelegt."); }} className="pool-form"><label>Internes Produkt<input required value={productName} onChange={e => setProductName(e.target.value)} placeholder="z. B. CarlinKit MINI Ultra" /></label><button className="button">Produkt anlegen</button></form>
      <form className="pool-form" onSubmit={e => { e.preventDefault(); void run(async () => { const listing = data.available_listings[Number(mapListing)]; if (!listing) throw new Error("Listing auswählen"); await post("/listings", { product_id: mapProduct, marketplace_id: listing.marketplace_id, seller_sku: listing.seller_sku, asin: listing.asin }); }, "Listing zugeordnet."); }}>
        <label>Produkt<select required value={mapProduct} onChange={e => setMapProduct(e.target.value)}><option value="">Auswählen</option>{productOptions}</select></label>
        <label>Amazon-Listing<select required value={mapListing} onChange={e => setMapListing(e.target.value)}><option value="">Auswählen</option>{data.available_listings.map((l, i) => <option key={i} value={i}>{l.seller_sku} · {l.asin} · {l.marketplace_id}</option>)}</select></label><button className="button">Zuordnen</button>
      </form>
      <div className="pool-scroll"><table><thead><tr><th>Produkt</th><th>Bei dir verfügbar</th><th>Ø Kosten Eigenbestand</th><th>Netto-Umsatz</th><th>Deckungsbeitrag</th></tr></thead><tbody>{data.products.map(p => <tr key={p.id}><td>{p.name}<small>{data.listings.filter(l => l.product_id === p.id).map(l => l.seller_sku).join(", ") || "Noch keine Listings zugeordnet"}</small></td><td>{p.available_quantity}<small>{p.reserved_quantity} reserviert · {p.in_transit_quantity} unterwegs · {p.amazon_costed_quantity} bei Amazon kostenmäßig erfasst</small></td><td>{p.average_home_cost_cents === null ? "—" : money(p.average_home_cost_cents)}</td><td>{money(p.sales_net_cents)}</td><td>{money(p.margin_cents)}{!p.margin_complete && <small>Kosten / Finanzdaten unvollständig</small>}</td></tr>)}</tbody></table></div>
      <h3>Listing-Auswertung</h3><p>Erstattungen sind enthalten. Mehrere Artikel einer Bestellung werden anteilig nach Warenwert zugeordnet.</p>
      <div className="pool-scroll"><table><thead><tr><th>Listing</th><th>Netto-Umsatz</th><th>Gebührenkosten</th><th>Wareneinsatz</th><th>Deckungsbeitrag</th></tr></thead><tbody>{data.listing_metrics.map(m => <tr key={`${m.marketplace_id}:${m.seller_sku}`}><td>{m.seller_sku}<small>{m.marketplace_id}</small></td><td>{money(m.sales_net_cents)}</td><td>{money(m.fees_net_cents)}</td><td>{money(m.cogs_cents)}</td><td>{money(m.margin_cents)}{!m.margin_complete && <small>Vorläufig / unvollständig</small>}</td></tr>)}</tbody></table></div>
    </>}
    {tab === "purchases" && <>
      <details open={!data.invoices.length}><summary>Neue Eingangsrechnung</summary>
        <p>Eine Rechnung kann mehrere Produkte und spätere Sendungen abdecken. Produkte zunächst unter „Produkte & Listings“ anlegen.</p>
        <form onSubmit={e => { e.preventDefault(); void run(async () => {
          const invoice = await post<{ id: string }>("/invoices", { supplier, number, invoice_date: invoiceDate, currency, fx_rate: currency === "EUR" ? "1" : fx, fx_reference: fxReference, notes: invoiceNotes, freight_cents: cents(freight), freight_allocations: freightShares.trim() ? freightShares.split(";").map(cents) : null, lines: draftLines.map(l => { const gross = cents(l.gross), vat = cents(l.vat); return { product_id: l.product_id, quantity: Number(l.quantity), gross_cents: gross, net_cents: gross - vat, vat_cents: vat, deductible_vat_cents: l.tax === "review" ? null : l.tax === "yes" ? vat : 0 }; }) });
          setSelectedInvoice(invoice.id); setNumber(""); setInvoiceNotes(""); setDraftLines([initialLine()]);
          if (file) { try { await upload(invoice.id, file); setFile(null); } catch { throw new Error("Rechnung gespeichert. Belegupload fehlgeschlagen; den Beleg unten bei der ausgewählten Rechnung erneut hinzufügen."); } }
        }, "Rechnung erfasst. Tatsächlichen Wareneingang separat buchen."); }}>
          <div className="pool-form"><label>Lieferant<input required value={supplier} onChange={e => setSupplier(e.target.value)} /></label><label>Rechnungsnummer<input required value={number} onChange={e => setNumber(e.target.value)} /></label><label>Rechnungsdatum<input required type="date" value={invoiceDate} onChange={e => setInvoiceDate(e.target.value)} /></label><label>Währung<input required maxLength={3} value={currency} onChange={e => setCurrency(e.target.value.toUpperCase())} /></label>
          {currency !== "EUR" && <><label>EUR je Währungseinheit<input required value={fx} onChange={e => setFx(e.target.value)} /></label><label>Kursbeleg / Referenz<input required value={fxReference} onChange={e => setFxReference(e.target.value)} /></label></>}</div>
          {draftLines.map((line, index) => <div className="pool-form pool-line" key={index}><label>Produkt<select required value={line.product_id} onChange={e => modifyLine(index, { product_id: e.target.value })}><option value="">Auswählen</option>{productOptions}</select></label><label>Stück<input type="number" min="1" step="1" required value={line.quantity} onChange={e => modifyLine(index, { quantity: e.target.value })} /></label><label>Positionsbetrag brutto<input required inputMode="decimal" value={line.gross} onChange={e => modifyLine(index, { gross: e.target.value })} /></label><label>Enthaltene Steuer<input required inputMode="decimal" value={line.vat} onChange={e => modifyLine(index, { vat: e.target.value })} /></label><label>Vorsteuer abziehbar<select value={line.tax} onChange={e => modifyLine(index, { tax: e.target.value })}><option value="review">Noch prüfen</option><option value="yes">Ja, vollständig</option><option value="no">Nein / keine Steuer</option></select></label>{draftLines.length > 1 && <button type="button" className="button" onClick={() => setDraftLines(draftLines.filter((_, i) => i !== index))}>Entfernen</button>}</div>)}
          <button type="button" className="button" onClick={() => setDraftLines([...draftLines, initialLine()])}>Weitere Position</button>
          <div className="pool-form"><label>Zusätzlicher Versand zu dir (wirtschaftliche Kosten in EUR)<input value={freight} onChange={e => setFreight(e.target.value)} inputMode="decimal" /></label><label>Manuelle Versandanteile je Position, EUR (optional; mit Semikolon)<input value={freightShares} onChange={e => setFreightShares(e.target.value)} placeholder="z. B. 3,50; 6,50" /></label><label>Interne Notiz<textarea value={invoiceNotes} onChange={e => setInvoiceNotes(e.target.value)} rows={3} placeholder="z. B. eine Einheit privat/Testzweck, nicht für Amazon" /></label><label>Rechnungsbeleg<input type="file" onChange={e => setFile(e.target.files?.[0] || null)} /></label></div><p>Bereits im Positionsbetrag enthaltenen Versand nicht nochmals eintragen. Gemeinsame Versandkosten werden nach Warenwert verteilt.</p><button className="button">Rechnung erfassen</button>
        </form>
      </details>
      {data.legacy_invoices?.length > 0 && <details><summary>Vorhandene Sendungsrechnungen übernehmen ({data.legacy_invoices.length})</summary><form className="pool-form" onSubmit={e => { e.preventDefault(); void run(() => post("/legacy-migration", { legacy_invoice_id: legacyInvoice, marketplace_id: marketplace, received_at: receiptDate }), "Altrechnung übernommen; ursprüngliche Belege und Kosten bleiben erhalten."); }}><label>Altrechnung<select required value={legacyInvoice} onChange={e => setLegacyInvoice(e.target.value)}><option value="">Auswählen</option>{data.legacy_invoices.map(i => <option key={i.id} value={i.id}>{i.supplier_name} · {i.invoice_number} · {i.shipment_id}</option>)}</select></label><label>Marketplace<select required value={marketplace} onChange={e => setMarketplace(e.target.value)}><option value="">Auswählen</option>{[...new Set(data.listings.map(l => l.marketplace_id))].map(m => <option key={m}>{m}</option>)}</select></label><label>Wareneingang am<input required type="date" value={receiptDate} onChange={e => setReceiptDate(e.target.value)} /></label><button className="button">Altrechnung übernehmen</button></form></details>}
      <h3>Rechnungen und Wareneingänge</h3><label>Rechnung auswählen<select value={selectedInvoice} onChange={e => { setSelectedInvoice(e.target.value); setReceiptLine(""); }}><option value="">Auswählen</option>{data.invoices.map(i => <option key={i.id} value={i.id}>{i.invoice_date.slice(0, 10)} · {i.supplier} · {i.number}</option>)}</select></label>
      {chosen && <><p>Rechnungsbetrag: {moneyIn(chosen.gross_cents, chosen.currency)} · Erfasste Zahlungen: {moneyIn(data.payments.filter(p => p.invoice_id === chosen.id).reduce((sum, p) => sum + p.amount_cents, 0), chosen.currency)}</p>{chosen.notes && <p><strong>Interne Notiz:</strong> {chosen.notes}</p>}
      <ul>{data.lines.filter(l => l.invoice_id === chosen.id).map(l => <li key={l.id}>{name(l.product_id)}: {data.receipts.filter(r => r.line_id === l.id).reduce((sum, r) => sum + r.quantity, 0)} / {l.quantity} Stück eingegangen {l.tax_status === "review_required" && "· Vorsteuer ungeklärt"}</li>)}</ul>
      <form className="pool-form" onSubmit={e => { e.preventDefault(); void run(() => post("/receipts", { line_id: receiptLine, quantity: Number(receiptQty), received_at: receiptDate }), "Wareneingang gebucht."); }}><label>Position<select required value={receiptLine} onChange={e => setReceiptLine(e.target.value)}><option value="">Auswählen</option>{data.lines.filter(l => l.invoice_id === chosen.id).map(l => <option key={l.id} value={l.id}>{name(l.product_id)}</option>)}</select></label><label>Erhaltene Stück<input required type="number" min="1" step="1" value={receiptQty} onChange={e => setReceiptQty(e.target.value)} /></label><label>Eingang am<input type="date" required value={receiptDate} onChange={e => setReceiptDate(e.target.value)} /></label><button className="button">Wareneingang buchen</button></form>
      <form className="pool-form" onSubmit={e => { e.preventDefault(); void run(() => post("/payments", { invoice_id: chosen.id, amount_cents: cents(payment), paid_at: paymentDate, account_reference: account }), "Zahlung erfasst; kein zusätzlicher Wareneinsatz gebucht."); }}><label>Zahlbetrag ({chosen.currency})<input required value={payment} onChange={e => setPayment(e.target.value)} inputMode="decimal" /></label><label>Gezahlt am<input required type="date" value={paymentDate} onChange={e => setPaymentDate(e.target.value)} /></label><label>Konto / Zahlungsreferenz<input required value={account} onChange={e => setAccount(e.target.value)} /></label><button className="button">Zahlung erfassen</button></form>
      <details><summary>Kosten oder Vorsteuer korrigieren</summary><p>Die Vorschau zeigt die neue Kostenbasis. Bestands- und bereits zugeordnete Verkaufskosten werden mit Änderungsnachweis angepasst.</p>
        <form onSubmit={e => { e.preventDefault(); void run(async () => { const line = data.lines.find(l => l.id === correctionLine); if (!line) throw new Error("Position auswählen"); const gross = cents(correctionGross), vat = cents(correctionVat); const payload = { line_id: line.id, expected_cost_cents: line.effective_cost_cents, gross_cents: gross, net_cents: gross-vat, vat_cents: vat, deductible_vat_cents: cents(correctionDeductible), reason: correctionReason }; const result = await post<{ old_cost_cents: number; new_cost_cents: number }>("/revisions", { ...payload, preview: true }); setPreview({ payload, ...result }); }, "Korrekturvorschau berechnet; noch nicht übernommen."); }}>
          <div className="pool-form"><label>Korrekturposition<select required value={correctionLine} onChange={e => { setCorrectionLine(e.target.value); setPreview(null); const l = data.lines.find(l => l.id === e.target.value); if (l) { setCorrectionGross((l.gross_cents/100).toFixed(2)); setCorrectionVat((l.vat_cents/100).toFixed(2)); setCorrectionDeductible(((l.deductible_vat_cents || 0)/100).toFixed(2)); } }}><option value="">Auswählen</option>{data.lines.filter(l => l.invoice_id === chosen.id).map(l => <option value={l.id} key={l.id}>{name(l.product_id)}</option>)}</select></label><label>Korrigierter Bruttobetrag<input required value={correctionGross} onChange={e => { setCorrectionGross(e.target.value); setPreview(null); }} /></label><label>Steuerbetrag<input required value={correctionVat} onChange={e => { setCorrectionVat(e.target.value); setPreview(null); }} /></label><label>Abziehbare Vorsteuer<input required value={correctionDeductible} onChange={e => { setCorrectionDeductible(e.target.value); setPreview(null); }} /></label><label>Begründung<input required value={correctionReason} onChange={e => { setCorrectionReason(e.target.value); setPreview(null); }} /></label></div><button className="button">Vorschau berechnen</button>
        </form>{preview && <p>Wirtschaftliche Positionskosten: {money(preview.old_cost_cents)} → {money(preview.new_cost_cents)} <button className="button" onClick={() => void run(async () => { await post("/revisions", { ...preview.payload, preview: false }); setPreview(null); }, "Korrektur mit Änderungsnachweis übernommen.")}>Korrektur übernehmen</button></p>}
      </details>
      <h4>Belege</h4>{data.documents.filter(d => d.invoice_id === chosen.id).map(d => <p key={d.id}><button type="button" className="button" onClick={() => void run(async () => { const response = await fetch(api(`/documents/${d.id}`), withAdminHeaders({})); if (!response.ok) throw new Error("Beleg konnte nicht geladen werden"); const url = URL.createObjectURL(await response.blob()); const a = document.createElement("a"); a.href = url; a.download = d.filename; a.click(); setTimeout(() => URL.revokeObjectURL(url), 1000); }, "Beleg heruntergeladen.")}>{d.filename}</button></p>)}<label>Weiteren Beleg hinzufügen<input type="file" onChange={e => { const document = e.target.files?.[0]; if (document) void run(() => upload(chosen.id, document), "Beleg gespeichert."); e.target.value = ""; }} /></label>
      </>}
    </>}
    {tab === "shipments" && <>
      <p>Produkte zunächst mit der passenden Marketplace-/SKU-Kombination verbinden. Reservierte Stücke stehen keiner zweiten Sendung zur Verfügung.</p>
      <form onSubmit={e => { e.preventDefault(); void run(() => post("/transfers", { shipment_id: shipment, marketplace_id: marketplace, package_reference: packageRef, lines: data.shipment_items.filter(i => i.shipment_id === shipment && Number(quantities[i.id]) > 0).map(i => ({ shipment_item_id: i.id, quantity: Number(quantities[i.id]) })) }), "FIFO-Bestand reserviert."); }}>
        <div className="pool-form"><label>FBA-Sendung<select required value={shipment} onChange={e => { setShipment(e.target.value); setQuantities({}); }}><option value="">Auswählen</option>{[...new Set(data.shipment_items.filter(i => !["CANCELLED", "DELETED"].includes(i.status)).map(i => i.shipment_id))].map(id => <option key={id}>{id}</option>)}</select></label><label>Marketplace<select required value={marketplace} onChange={e => setMarketplace(e.target.value)}><option value="">Auswählen</option>{[...new Set(data.listings.map(l => l.marketplace_id))].map(id => <option key={id}>{id}</option>)}</select></label><label>Paketreferenz (optional)<input value={packageRef} onChange={e => setPackageRef(e.target.value)} /></label></div>
        {data.shipment_items.filter(i => i.shipment_id === shipment).map(i => <label className="pool-quantity" key={i.id}>{i.seller_sku} · Amazon: {i.quantity_received}/{i.quantity_shipped} erhalten<input aria-label={`Menge ${i.seller_sku}`} type="number" min="0" max={i.quantity_shipped} step="1" value={quantities[i.id] || ""} onChange={e => setQuantities({ ...quantities, [i.id]: e.target.value })} /></label>)}<button className="button">Aus Einkaufspool reservieren</button>
      </form>
      <label>Versanddatum<input type="date" value={dispatchDate} onChange={e => setDispatchDate(e.target.value)} /></label>
      {data.transfers.map(t => <article className="pool-transfer" key={t.id}><strong>{t.shipment_id} {t.package_reference}</strong><p>{t.status === "reserved" ? "Reserviert" : t.status === "dispatched" ? "Versendet" : "Reservierung freigegeben"} · {data.transfer_lines.filter(l => l.transfer_id === t.id).reduce((sum, l) => sum + l.received_quantity, 0)} / {data.transfer_lines.filter(l => l.transfer_id === t.id).reduce((sum, l) => sum + l.quantity, 0)} bei Amazon eingebucht</p>{t.status === "reserved" && <div className="pool-form"><label>Versandkosten zu Amazon (EUR)<input inputMode="decimal" value={dispatchCosts[t.id] || "0"} onChange={e => setDispatchCosts({ ...dispatchCosts, [t.id]: e.target.value })} /></label><label>Amazon-Transportkosten zuordnen (optional)<select value={sourceCosts[t.id] || ""} onChange={e => { setSourceCosts({ ...sourceCosts, [t.id]: e.target.value }); const cost = data.available_costs?.find(c => c.id === e.target.value); if (cost) setDispatchCosts({ ...dispatchCosts, [t.id]: (Math.abs(cost.amount_cents)/100).toFixed(2) }); }}><option value="">Manueller Versandbetrag</option>{data.available_costs?.filter(c => c.shipment_id === t.shipment_id).map(c => <option key={c.id} value={c.id}>{c.cost_type} · {money(Math.abs(c.amount_cents))}</option>)}</select></label><button className="button" onClick={() => void run(() => post("/transfers/action", { transfer_id: t.id, action: "dispatch", dispatched_at: dispatchDate, freight_cents: cents(dispatchCosts[t.id] || "0"), source_cost_id: sourceCosts[t.id] || null }), "Versand gebucht.")}>Als versendet buchen</button><button className="button" onClick={() => void run(() => post("/transfers/action", { transfer_id: t.id, action: "cancel" }), "Bestand wieder verfügbar.")}>Reservierung freigeben</button></div>}{t.status === "dispatched" && <details><summary>Versandkosten nachtragen / korrigieren ({money(t.freight_cents)})</summary><div className="pool-form"><label>Neue Versandkosten (EUR)<input value={dispatchCosts[t.id] ?? (t.freight_cents/100).toFixed(2)} onChange={e => { setDispatchCosts({ ...dispatchCosts, [t.id]: e.target.value }); setTransportPreview(null); }} /></label><label>Begründung<input value={transportReason} onChange={e => { setTransportReason(e.target.value); setTransportPreview(null); }} /></label><button className="button" onClick={() => void run(async () => { const payload = { transfer_id: t.id, expected_freight_cents: t.freight_cents, freight_cents: cents(dispatchCosts[t.id] ?? (t.freight_cents/100).toFixed(2)), reason: transportReason }; const result = await post<{ old_cost_cents: number; new_cost_cents: number }>("/transfer-cost-revisions", { ...payload, preview: true }); setTransportPreview({ payload, ...result }); }, "Versandkorrektur berechnet; noch nicht übernommen.")}>Vorschau</button></div></details>}</article>)}
      {transportPreview && <p>Versandkosten: {money(transportPreview.old_cost_cents)} → {money(transportPreview.new_cost_cents)} <button className="button" onClick={() => void run(async () => { await post("/transfer-cost-revisions", { ...transportPreview.payload, preview: false }); setTransportPreview(null); }, "Versandkosten mit Änderungsnachweis aktualisiert.")}>Versandkorrektur übernehmen</button></p>}
      {/* transfers rendered above */}
      <button className="button" onClick={() => void run(async () => { const result = await post<{ cost_issues: unknown[] }>("/reconcile", {}); if (result.cost_issues.length) setMessage(`${result.cost_issues.length} Bestellungen haben noch unvollständige Einkaufskosten.`); }, "Amazon-Empfang und FIFO-Kosten abgeglichen. Offene Differenzen siehe unten.")}>Empfang und Verkaufskosten abgleichen</button>
      <div className="pool-scroll"><table><thead><tr><th>Sendung</th><th>Verkaufte Stück (FIFO)</th><th>Netto-Umsatz</th><th>Wareneinsatz inkl. Transport</th><th>Deckungsbeitrag</th></tr></thead><tbody>{data.shipment_metrics?.map(m => <tr key={`${m.shipment_id}:${m.currency}`}><td>{m.shipment_id}</td><td>{m.quantity_sold}</td><td>{money(m.sales_net_cents)}</td><td>{money(m.cogs_cents)}</td><td>{money(m.margin_cents)}{!m.margin_complete && <small>Vorläufig / unvollständig</small>}</td></tr>)}</tbody></table></div>
      <p>Sendungskosten und Verkäufe werden rechnerisch nach FIFO zugeordnet; nicht als Nachweis des physisch verkauften Exemplars.</p>
      {data.issues.length > 0 && <details><summary>{data.issues.length} offene Empfangsdifferenzen</summary><ul>{data.issues.map((i, n) => <li key={n}>{i.shipment_id}: Amazon {i.quantity_received}, Einkaufszuordnung {i.booked_quantity}</li>)}</ul></details>}
    </>}
    </fieldset>
  </section>;
}
