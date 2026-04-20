import { useDeferredValue, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ArrowUpRight, CheckCircle2, FileText, Funnel, PackageSearch, TrendingUp } from "lucide-react";
import { PageFilterCard } from "@/components/page-filter-card";
import { DataTableShell } from "@/components/ui/data-table-shell";
import { FilterChip } from "@/components/ui/filter-chip";
import { SurfaceCard } from "@/components/ui/surface-card";
import { Button } from "@/components/ui/button";
import { filterOrders, getOrdersClientFilterCount, isCanceledOrder } from "@/features/orders/filter-orders";
import { OrderDetailPanel } from "@/features/orders/order-detail-panel";
import {
  createDefaultOrdersClientFilters,
  createDefaultOrdersServerFilters,
  type OrdersPayload,
  type OrderSummary,
} from "@/features/orders/types";
import { fetchJson, buildDashboardQuery } from "@/lib/api";
import { formatCurrencyFromCents, formatDate, formatNumber } from "@/lib/format";

const orderStatusOptions = ["fulfilled", "need_to_be_sent", "sent", "paid", "received", "cancelled", "refunded"] as const;
const orderPaymentOptions = ["Shopify Payments", "PayPal", "Kaufland Settlement", "Mastercard", "Visa"] as const;

function feeSourcePrefix(source?: string) {
  if (!source || source === "api") {
    return "";
  }

  return source === "estimated_fx" ? "~ FX " : "~ ";
}

async function fetchOrders(filters: ReturnType<typeof createDefaultOrdersServerFilters>) {
  const query = buildDashboardQuery({
    limit: "5000",
    q: filters.q || undefined,
    marketplace: filters.marketplace || undefined,
    from: filters.from || undefined,
    to: filters.to || undefined,
  });

  const suffix = query ? `?${query}` : "";
  return fetchJson<OrdersPayload>(`/api/orders${suffix}`);
}

export function OrdersPage() {
  const [serverFilters, setServerFilters] = useState(createDefaultOrdersServerFilters);
  const [clientFilters, setClientFilters] = useState(createDefaultOrdersClientFilters);
  const [showAdvancedFilters, setShowAdvancedFilters] = useState(true);
  const [selectedOrder, setSelectedOrder] = useState<{ marketplace: string; orderId: string } | null>(null);
  const deferredSearch = useDeferredValue(serverFilters.q);

  const ordersQuery = useQuery({
    queryKey: ["orders-preview", deferredSearch, serverFilters.marketplace, serverFilters.from, serverFilters.to],
    queryFn: () =>
      fetchOrders({
        ...serverFilters,
        q: deferredSearch,
      }),
  });

  const allOrders = ordersQuery.data?.items ?? [];
  const filteredOrders = filterOrders(allOrders, clientFilters);
  const activeClientFilterCount = getOrdersClientFilterCount(clientFilters);

  let revenueTotalCents = 0;
  let profitTotalCents = 0;
  let invoiceCount = 0;
  let returnsCount = 0;

  for (const order of filteredOrders) {
    revenueTotalCents += Number(order.total_cents ?? 0);
    profitTotalCents += Number(order.profit_cents ?? 0);
    if (order.invoice) {
      invoiceCount += 1;
    }
    if (isCanceledOrder(order)) {
      returnsCount += 1;
    }
  }

  const summaryCards = [
    { label: "Sichtbare Orders", value: formatNumber(filteredOrders.length), icon: PackageSearch },
    { label: "Umsatz", value: formatCurrencyFromCents(revenueTotalCents), icon: TrendingUp },
    { label: "Gewinn", value: formatCurrencyFromCents(profitTotalCents), icon: CheckCircle2 },
    { label: "Rechnungen", value: formatNumber(invoiceCount), icon: FileText },
  ];

  return (
    <div className="space-y-5">
      <section className="grid gap-3 xl:grid-cols-[1.6fr_1fr]">
        <SurfaceCard
          description="Die React-Orders-Route deckt jetzt Liste, Detailpanel, Einkaufspflege und Rechnungs-Upload ueber die bestehenden APIs ab."
          title="Orders Migration Preview"
        />

        <SurfaceCard
          action={
            <a href="/legacy?tab=orders" rel="noreferrer">
              <Button variant="outline">
                Legacy Orders
                <ArrowUpRight className="ml-2 h-4 w-4" />
              </Button>
            </a>
          }
          description="Das Legacy-Dashboard bleibt als Fallback erhalten, waehrend die React-Variante Schritt fuer Schritt dieselben Flows uebernimmt."
          title="Funktionssicherheit"
        />
      </section>

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        {summaryCards.map((card) => {
          const Icon = card.icon;

          return (
            <SurfaceCard key={card.label} className="p-0" contentClassName="p-5">
              <div className="flex items-start justify-between">
                <div>
                  <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">{card.label}</p>
                  <p className="mt-3 font-[var(--font-display)] text-2xl font-semibold text-[var(--ink)]">
                    {ordersQuery.isLoading ? "..." : card.value}
                  </p>
                </div>
                <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface-2)] p-2.5 text-[var(--ink-3)]">
                  <Icon className="h-4 w-4" />
                </div>
              </div>
            </SurfaceCard>
          );
        })}
      </section>

      <section className="grid gap-3 xl:grid-cols-[1.15fr_0.85fr]">
        <PageFilterCard
          description="Diese Filter laufen direkt ueber den bestehenden Orders-Endpunkt."
          from={serverFilters.from}
          marketplace={serverFilters.marketplace}
          onFromChange={(value) => setServerFilters((current) => ({ ...current, from: value }))}
          onMarketplaceChange={(value) => setServerFilters((current) => ({ ...current, marketplace: value }))}
          onQueryChange={(value) => setServerFilters((current) => ({ ...current, q: value }))}
          onToChange={(value) => setServerFilters((current) => ({ ...current, to: value }))}
          query={serverFilters.q}
          queryPlaceholder="Order, Kunde, Artikel"
          title="Basisfilter"
          to={serverFilters.to}
        />

        <SurfaceCard
          action={
            <div className="flex items-center gap-2">
              <button
                className="inline-flex items-center gap-2 rounded-2xl border border-[var(--border-strong)] bg-[var(--surface)] px-3 py-2 text-xs font-medium text-[var(--ink-3)] transition-colors hover:bg-[var(--surface-2)] hover:text-[var(--ink)]"
                onClick={() => setShowAdvancedFilters((current) => !current)}
                type="button"
              >
                <Funnel className="h-4 w-4" />
                {showAdvancedFilters ? "Ausblenden" : "Einblenden"}
              </button>
              <Button
                onClick={() => {
                  setServerFilters(createDefaultOrdersServerFilters());
                  setClientFilters(createDefaultOrdersClientFilters());
                }}
                size="sm"
                variant="ghost"
              >
                Reset
              </Button>
            </div>
          }
          description="Diese Filter entsprechen dem bisherigen Orders-Dropdown und laufen clientseitig auf den geladenen Orders."
          title={`Weitere Filter${activeClientFilterCount ? ` (${activeClientFilterCount})` : ""}`}
        >
          {showAdvancedFilters ? (
            <div className="space-y-4">
              <FilterGroup label="Status">
                {orderStatusOptions.map((status) => (
                  <FilterChip
                    active={clientFilters.orderStatus.includes(status)}
                    key={status}
                    onClick={() => {
                      setClientFilters((current) => ({
                        ...current,
                        orderStatus: current.orderStatus.includes(status)
                          ? current.orderStatus.filter((value) => value !== status)
                          : [...current.orderStatus, status],
                      }));
                    }}
                  >
                    {status}
                  </FilterChip>
                ))}
              </FilterGroup>

              <FilterGroup label="Zahlungsart">
                {orderPaymentOptions.map((payment) => (
                  <FilterChip
                    active={clientFilters.orderPayment.includes(payment)}
                    key={payment}
                    onClick={() => {
                      setClientFilters((current) => ({
                        ...current,
                        orderPayment: current.orderPayment.includes(payment)
                          ? current.orderPayment.filter((value) => value !== payment)
                          : [...current.orderPayment, payment],
                      }));
                    }}
                  >
                    {payment}
                  </FilterChip>
                ))}
              </FilterGroup>

              <FilterGroup label="Sonstiges">
                <FilterChip active={clientFilters.returnsOnly} onClick={() => setClientFilters((current) => ({ ...current, returnsOnly: !current.returnsOnly }))}>
                  Retouren / Cancel
                </FilterChip>
                <FilterChip active={clientFilters.hideCanceled} onClick={() => setClientFilters((current) => ({ ...current, hideCanceled: !current.hideCanceled }))}>
                  Stornierte ausblenden
                </FilterChip>
              </FilterGroup>

              <FilterGroup label="Einkaufspreis">
                <FilterChip
                  active={clientFilters.hasPurchaseCost}
                  onClick={() =>
                    setClientFilters((current) => ({
                      ...current,
                      hasPurchaseCost: !current.hasPurchaseCost,
                      noPurchaseCost: !current.hasPurchaseCost ? false : current.noPurchaseCost,
                    }))
                  }
                >
                  Preis eingetragen
                </FilterChip>
                <FilterChip
                  active={clientFilters.noPurchaseCost}
                  onClick={() =>
                    setClientFilters((current) => ({
                      ...current,
                      noPurchaseCost: !current.noPurchaseCost,
                      hasPurchaseCost: !current.noPurchaseCost ? false : current.hasPurchaseCost,
                    }))
                  }
                >
                  Preis fehlt
                </FilterChip>
              </FilterGroup>

              <FilterGroup label="Rechnung">
                <FilterChip
                  active={clientFilters.hasInvoice}
                  onClick={() =>
                    setClientFilters((current) => ({
                      ...current,
                      hasInvoice: !current.hasInvoice,
                      noInvoice: !current.hasInvoice ? false : current.noInvoice,
                    }))
                  }
                >
                  Rechnung vorhanden
                </FilterChip>
                <FilterChip
                  active={clientFilters.noInvoice}
                  onClick={() =>
                    setClientFilters((current) => ({
                      ...current,
                      noInvoice: !current.noInvoice,
                      hasInvoice: !current.noInvoice ? false : current.hasInvoice,
                    }))
                  }
                >
                  Rechnung fehlt
                </FilterChip>
              </FilterGroup>
            </div>
          ) : null}
        </SurfaceCard>
      </section>

      <DataTableShell
        description="Preview der migrierten Orders-Tabelle. Detailpanel, Einkaufspflege und Rechnungs-Upload laufen bereits ueber die bestehenden APIs."
        meta={
          ordersQuery.isLoading
            ? "Lade..."
            : filteredOrders.length !== Number(ordersQuery.data?.total ?? filteredOrders.length)
              ? `${formatNumber(filteredOrders.length)} / ${formatNumber(Number(ordersQuery.data?.total ?? 0))} Zeilen`
              : `${formatNumber(filteredOrders.length)} Zeilen`
        }
        title="Kombinierte Orders"
      >
        <table className="min-w-full border-collapse text-sm">
          <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
            <tr>
              <th className="px-4 py-3 font-medium">Datum</th>
              <th className="px-4 py-3 font-medium">Channel</th>
              <th className="px-4 py-3 font-medium">Order</th>
              <th className="px-4 py-3 font-medium">Kunde</th>
              <th className="px-4 py-3 font-medium">Artikel</th>
              <th className="px-4 py-3 font-medium">Finanzen</th>
              <th className="px-4 py-3 font-medium">Einkauf</th>
              <th className="px-4 py-3 font-medium">Gewinn</th>
              <th className="px-4 py-3 font-medium">Status</th>
              <th className="px-4 py-3 font-medium">Rechnung</th>
            </tr>
          </thead>
          <tbody>
            {ordersQuery.isLoading ? (
              <tr>
                <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={10}>
                  Orders werden geladen...
                </td>
              </tr>
            ) : ordersQuery.isError ? (
              <tr>
                <td className="px-4 py-4 text-[var(--danger)]" colSpan={10}>
                  Orders konnten nicht geladen werden: {ordersQuery.error.message}
                </td>
              </tr>
            ) : filteredOrders.length ? (
              filteredOrders.map((order) => (
                <OrderRow
                  key={`${order.marketplace}-${order.order_id}`}
                  onOpenDetails={() =>
                    setSelectedOrder({
                      marketplace: String(order.marketplace ?? ""),
                      orderId: String(order.order_id ?? ""),
                    })
                  }
                  order={order}
                />
              ))
            ) : (
              <tr>
                <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={10}>
                  Keine Orders fuer den aktuellen Filter.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </DataTableShell>

      <OrderDetailPanel onClose={() => setSelectedOrder(null)} selectedOrder={selectedOrder} />
    </div>
  );
}

function FilterGroup({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <p className="mb-2 text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">{label}</p>
      <div className="flex flex-wrap gap-2">{children}</div>
    </div>
  );
}

function OrderRow({ order, onOpenDetails }: { order: OrderSummary; onOpenDetails: () => void }) {
  const profit = Number(order.profit_cents ?? 0);
  const invoiceLabel = order.invoice?.original_filename ?? order.invoice?.stored_filename ?? "Rechnung";
  const invoiceHref = order.invoice?.document_id
    ? `/api/orders/${encodeURIComponent(String(order.marketplace ?? ""))}/${encodeURIComponent(String(order.order_id ?? ""))}/invoice/${encodeURIComponent(order.invoice.document_id)}/download?disposition=inline`
    : "";
  const lineItemsCount = Number(order.line_items_count ?? 1);

  return (
    <tr className="border-t border-[var(--border)] align-top text-[var(--ink-2)]">
      <td className="px-4 py-3 text-[var(--ink)]">{formatDate(order.order_date)}</td>
      <td className="px-4 py-3">
        <span
          className={`inline-flex rounded-full px-2.5 py-1 text-xs font-medium ${
            order.marketplace === "shopify"
              ? "bg-[color:rgba(237,245,255,0.95)] text-[color:#2d568f]"
              : "bg-[color:rgba(233,249,240,0.95)] text-[color:#136b45]"
          }`}
        >
          {order.marketplace ?? "-"}
        </span>
      </td>
      <td className="px-4 py-3 font-medium text-[var(--ink)]">
        <button className="text-left underline-offset-4 hover:underline" onClick={onOpenDetails} type="button">
          {order.external_order_id ?? order.order_id ?? "-"}
        </button>
        <div className="mt-1 text-xs text-[var(--ink-4)]">Details & Aktionen</div>
      </td>
      <td className="px-4 py-3">{order.customer ?? "-"}</td>
      <td className="px-4 py-3">
        <div className="max-w-[240px] text-[var(--ink)]">{order.article ?? "-"}</div>
        {lineItemsCount > 1 ? <div className="mt-1 text-xs text-[var(--ink-4)]">+{lineItemsCount - 1} weitere</div> : null}
      </td>
      <td className="px-4 py-3">
        <div className="font-medium text-[var(--ink)]">{formatCurrencyFromCents(order.total_cents)}</div>
        <div className="mt-1 text-xs text-[var(--ink-4)]">After: {formatCurrencyFromCents(order.after_fees_cents)}</div>
        <div className="mt-1 text-xs text-[var(--ink-4)]">
          {feeSourcePrefix(order.fee_source)}Fees: {formatCurrencyFromCents(order.fees_cents)}
        </div>
      </td>
      <td className="px-4 py-3">{Number(order.purchase_cost_cents ?? 0) > 0 ? formatCurrencyFromCents(order.purchase_cost_cents) : "-"}</td>
      <td className={`px-4 py-3 font-medium ${profit < 0 ? "text-[var(--danger)]" : "text-[var(--success)]"}`}>
        {formatCurrencyFromCents(order.profit_cents)}
      </td>
      <td className="px-4 py-3">{order.fulfillment_status ?? "-"}</td>
      <td className="px-4 py-3">
        {invoiceHref ? (
          <a className="text-sm font-medium text-[color:#1a6cc6] underline-offset-4 hover:underline" href={invoiceHref} rel="noreferrer" target="_blank">
            {invoiceLabel}
          </a>
        ) : (
          <span className="text-[var(--ink-4)]">Fehlt</span>
        )}
      </td>
    </tr>
  );
}
