import { useEffect, useState } from "react";
import { createPortal } from "react-dom";

import { formatMoneyFromCents, formatPercent } from "@/features/analytics/format";
import { fetchJson } from "@/shared/api/client";
import { buildDashboardApiUrl } from "@/shared/runtime/base-path";
import { useAmazonDetailModal } from "./use-amazon-detail-modal";

type SkuSummary = {
  sku_key: string;
  seller_sku: string;
  asin: string;
  title: string;
  quantity_sold: number;
  sales_cents: number;
  cogs_cents: number;
  margin_cents: number;
  margin_percent: number | null;
  fulfillable_quantity: number;
  inbound_working_quantity: number;
  inbound_shipped_quantity: number;
  reserved_quantity: number;
};

type SkuShipment = {
  shipment_id: string;
  shipment_name: string;
  label: string;
  quantity_shipped: number;
  quantity_received: number;
};

type SkuDetail = SkuSummary & {
  fee_per_unit_cents: number | null;
  quantity_sold_last_30_days: number;
  days_of_stock: number | null;
  shipments: SkuShipment[];
};

function count(value: number | undefined) {
  return new Intl.NumberFormat("de-DE").format(Number(value || 0));
}

function marginClassName(marginCents: number) {
  return `order-profit-cell ${marginCents < 0 ? "value-neg" : "value-pos"}`;
}

export function AmazonInventoryPage() {
  const [items, setItems] = useState<SkuSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [selectedSku, setSelectedSku] = useState<SkuDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    (async () => {
      try {
        const response = await fetchJson<{ items?: SkuSummary[] }>(buildDashboardApiUrl("/api/amazon/inventory/skus"), { signal: controller.signal });
        setItems(response.items || []);
      } catch (requestError: unknown) {
        if ((requestError as Error).name !== "AbortError") {
          setError(requestError instanceof Error ? requestError.message : "Bestand konnte nicht geladen werden.");
        }
      } finally {
        setLoading(false);
      }
    })();
    return () => controller.abort();
  }, []);

  const detailPortalTarget = useAmazonDetailModal(Boolean(selectedSku), selectedSku ? (selectedSku.title || selectedSku.sku_key) : "", () => {
    setSelectedSku(null);
  });

  async function openSku(skuKey: string) {
    setDetailLoading(true);
    try {
      const detail = await fetchJson<SkuDetail>(buildDashboardApiUrl(`/api/amazon/inventory/skus/${encodeURIComponent(skuKey)}`));
      setSelectedSku(detail);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "SKU-Details konnten nicht geladen werden.");
    } finally {
      setDetailLoading(false);
    }
  }

  return (
    <section className="card table-card" style={{ marginTop: "1rem" }}>
      <div className="table-head">
        <h2 className="table-title">FBA Bestand</h2>
        <div className="table-meta">{loading ? "..." : `${count(items.length)} SKUs`}</div>
      </div>
      {error ? <div className="table-meta" style={{ color: "var(--danger, #c44)" }}>{error}</div> : null}
      <div className="table-wrap">
        <table className="orders-table">
          <thead><tr><th>SKU</th><th>Verfügbar</th><th>Inbound</th><th>Verkauft</th><th>Umsatz</th><th>Marge</th></tr></thead>
          <tbody>
            {items.length ? items.map((item) => (
              <tr key={item.sku_key} onClick={() => void openSku(item.sku_key)} style={{ cursor: "pointer" }}>
                <td><strong>{item.title || item.seller_sku || item.sku_key}</strong><br /><small>{item.seller_sku}</small></td>
                <td>{count(item.fulfillable_quantity)}</td>
                <td>{count(item.inbound_working_quantity + item.inbound_shipped_quantity)}</td>
                <td>{count(item.quantity_sold)}</td>
                <td>{formatMoneyFromCents(item.sales_cents)}</td>
                <td className={marginClassName(item.margin_cents)}>
                  {formatMoneyFromCents(item.margin_cents)}
                  {item.margin_percent != null ? <div className="cell-sub">{formatPercent(item.margin_percent, 1)}</div> : null}
                </td>
              </tr>
            )) : <tr><td colSpan={6}>{loading ? "Bestand wird geladen..." : "Noch keine SKU-Daten vorhanden."}</td></tr>}
          </tbody>
        </table>
      </div>
      {detailPortalTarget && selectedSku ? createPortal(
        <div>
          {detailLoading ? <p>SKU wird geladen...</p> : null}
          <p className="page-subtitle">{selectedSku.seller_sku} · {selectedSku.asin || "kein ASIN"}</p>
          <div className="detail-grid">
            <article className="detail-card">
              <h3>Verkauf</h3>
              <div className="detail-kv">
                <div className="detail-row"><span>Verkaufte Menge</span><strong>{count(selectedSku.quantity_sold)}</strong></div>
                <div className="detail-row"><span>Umsatz</span><strong>{formatMoneyFromCents(selectedSku.sales_cents)}</strong></div>
                <div className="detail-row"><span>Verkauft (30 Tage)</span><strong>{count(selectedSku.quantity_sold_last_30_days)}</strong></div>
              </div>
            </article>
            <article className="detail-card">
              <h3>Einkauf &amp; Marge</h3>
              <div className="detail-kv">
                <div className="detail-row"><span>Einkaufskosten (FIFO)</span><strong>{formatMoneyFromCents(selectedSku.cogs_cents)}</strong></div>
                <div className="detail-row"><span>Marge</span><strong>{formatMoneyFromCents(selectedSku.margin_cents)}</strong></div>
                <div className="detail-row"><span>Marge %</span><strong>{selectedSku.margin_percent != null ? formatPercent(selectedSku.margin_percent, 1) : "-"}</strong></div>
                <div className="detail-row"><span>Amazon-Gebühr / Stück</span><strong>{selectedSku.fee_per_unit_cents != null ? formatMoneyFromCents(selectedSku.fee_per_unit_cents) : "-"}</strong></div>
              </div>
            </article>
            <article className="detail-card">
              <h3>Bestand</h3>
              <div className="detail-kv">
                <div className="detail-row"><span>Verfügbar</span><strong>{count(selectedSku.fulfillable_quantity)}</strong></div>
                <div className="detail-row"><span>Inbound (working)</span><strong>{count(selectedSku.inbound_working_quantity)}</strong></div>
                <div className="detail-row"><span>Inbound (shipped)</span><strong>{count(selectedSku.inbound_shipped_quantity)}</strong></div>
                <div className="detail-row"><span>Reserviert</span><strong>{count(selectedSku.reserved_quantity)}</strong></div>
                <div className="detail-row"><span>Lagerreichweite</span><strong>{selectedSku.days_of_stock != null ? `${selectedSku.days_of_stock} Tage` : "-"}</strong></div>
              </div>
            </article>
          </div>
          <h3>Zugehörige Sendungen</h3>
          <div className="table-wrap">
            <table className="orders-table">
              <thead><tr><th>Shipment</th><th>Status</th><th>Versendet</th><th>Empfangen</th></tr></thead>
              <tbody>
                {selectedSku.shipments.length ? selectedSku.shipments.map((shipment) => (
                  <tr key={shipment.shipment_id}>
                    <td><strong>{shipment.shipment_id}</strong><br /><small>{shipment.shipment_name || "Amazon FBA"}</small></td>
                    <td><span className="status-badge">{shipment.label}</span></td>
                    <td>{count(shipment.quantity_shipped)}</td>
                    <td>{count(shipment.quantity_received)}</td>
                  </tr>
                )) : <tr><td colSpan={4}>Keine zugehörigen Sendungen.</td></tr>}
              </tbody>
            </table>
          </div>
        </div>,
        detailPortalTarget,
      ) : null}
    </section>
  );
}
