import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowUpRight, FileUp, LoaderCircle, Save, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { DataTableShell } from "@/components/ui/data-table-shell";
import { SurfaceCard } from "@/components/ui/surface-card";
import type { OrderDetailPayload } from "@/features/orders/types";
import { fetchJson } from "@/lib/api";
import { formatCurrencyFromCents, formatDate } from "@/lib/format";

interface SelectedOrderRef {
  marketplace: string;
  orderId: string;
}

interface OrderDetailPanelProps {
  selectedOrder: SelectedOrderRef | null;
  onClose: () => void;
}

interface PurchaseMutationInput extends SelectedOrderRef {
  purchase_cost_eur: number | null;
  supplier_name: string;
  purchase_notes: string;
}

interface InvoiceMutationInput extends SelectedOrderRef {
  file: File;
  purchase_cost_eur: number | null;
  supplier_name: string;
  purchase_notes: string;
}

async function fetchOrderDetail(selection: SelectedOrderRef) {
  return fetchJson<OrderDetailPayload>(
    `/api/orders/${encodeURIComponent(selection.marketplace)}/${encodeURIComponent(selection.orderId)}`,
  );
}

async function savePurchase(input: PurchaseMutationInput) {
  return fetchJson(
    `/api/orders/${encodeURIComponent(input.marketplace)}/${encodeURIComponent(input.orderId)}/purchase`,
    {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        purchase_cost_eur: input.purchase_cost_eur,
        purchase_currency: "EUR",
        supplier_name: input.supplier_name || undefined,
        purchase_notes: input.purchase_notes || undefined,
      }),
    },
  );
}

async function uploadInvoice(input: InvoiceMutationInput) {
  const body = new FormData();
  body.append("file", input.file);
  if (input.purchase_cost_eur !== null) {
    body.append("purchase_cost_eur", String(input.purchase_cost_eur));
    body.append("purchase_currency", "EUR");
  }
  if (input.supplier_name.trim()) {
    body.append("supplier_name", input.supplier_name.trim());
  }
  if (input.purchase_notes.trim()) {
    body.append("notes", input.purchase_notes.trim());
  }

  return fetchJson(
    `/api/orders/${encodeURIComponent(input.marketplace)}/${encodeURIComponent(input.orderId)}/invoice`,
    {
      method: "POST",
      body,
    },
  );
}

function parsePurchaseValue(value: string) {
  const trimmed = value.trim().replace(",", ".");
  if (!trimmed) {
    return null;
  }

  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error("Einkaufspreis ist ungueltig.");
  }

  return parsed;
}

function addressLines(address: Record<string, unknown> | undefined) {
  if (!address) {
    return [] as string[];
  }

  const fullName = [address.first_name, address.last_name].filter(Boolean).join(" ") || String(address.full_name ?? "").trim();
  const street = String(address.street ?? "").trim();
  const cityLine = [String(address.postcode ?? "").trim(), String(address.city ?? "").trim()].filter(Boolean).join(" ");
  const country = String(address.country ?? "").trim();
  const phone = String(address.phone ?? "").trim();

  return [fullName, street, cityLine, country, phone].filter(Boolean);
}

export function OrderDetailPanel({ selectedOrder, onClose }: OrderDetailPanelProps) {
  const queryClient = useQueryClient();
  const [purchaseCostInput, setPurchaseCostInput] = useState("");
  const [supplierName, setSupplierName] = useState("");
  const [purchaseNotes, setPurchaseNotes] = useState("");
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [formMessage, setFormMessage] = useState<{ tone: "ok" | "error"; text: string } | null>(null);

  const detailQuery = useQuery({
    queryKey: ["order-detail-preview", selectedOrder?.marketplace, selectedOrder?.orderId],
    queryFn: () => fetchOrderDetail(selectedOrder as SelectedOrderRef),
    enabled: Boolean(selectedOrder),
  });

  const purchaseMutation = useMutation({
    mutationFn: savePurchase,
    onSuccess: async () => {
      setFormMessage({ tone: "ok", text: "Einkauf und Notizen wurden gespeichert." });
      await invalidateRelatedQueries(queryClient, selectedOrder);
    },
    onError: (error: Error) => {
      setFormMessage({ tone: "error", text: error.message });
    },
  });

  const invoiceMutation = useMutation({
    mutationFn: uploadInvoice,
    onSuccess: async () => {
      setSelectedFile(null);
      setFormMessage({ tone: "ok", text: "Rechnung wurde hochgeladen und die Order aktualisiert." });
      await invalidateRelatedQueries(queryClient, selectedOrder);
    },
    onError: (error: Error) => {
      setFormMessage({ tone: "error", text: error.message });
    },
  });

  useEffect(() => {
    const summary = detailQuery.data?.summary;
    if (!summary) {
      return;
    }

    const purchaseValue = Number(summary.purchase_cost_cents ?? 0);
    setPurchaseCostInput(purchaseValue > 0 ? (purchaseValue / 100).toFixed(2) : "");
    setSupplierName(String(summary.purchase_supplier ?? ""));
    setPurchaseNotes(String(summary.purchase_notes ?? ""));
    setFormMessage(null);
  }, [detailQuery.data?.summary, selectedOrder?.marketplace, selectedOrder?.orderId]);

  if (!selectedOrder) {
    return null;
  }

  const summary = detailQuery.data?.summary ?? {};
  const invoice = summary.invoice;
  const invoiceHref = invoice?.document_id
    ? `/api/orders/${encodeURIComponent(selectedOrder.marketplace)}/${encodeURIComponent(selectedOrder.orderId)}/invoice/${encodeURIComponent(invoice.document_id)}/download?disposition=inline`
    : "";
  const lineItems = Array.isArray(detailQuery.data?.line_items) ? detailQuery.data?.line_items : [];
  const units = Array.isArray(detailQuery.data?.units) ? detailQuery.data?.units : [];
  const transactions = Array.isArray(detailQuery.data?.transactions) ? detailQuery.data?.transactions : [];
  const bookkeeping = detailQuery.data?.bookkeeping_breakdown ?? {};
  const shippingAddress = detailQuery.data?.shipping_address as Record<string, unknown> | undefined;
  const billingAddress = detailQuery.data?.billing_address as Record<string, unknown> | undefined;
  const infoRows = [
    ["Marketplace", String(summary.marketplace ?? "-")],
    ["Order", String(summary.external_order_id ?? summary.order_id ?? selectedOrder.orderId)],
    ["Datum", formatDate(String(summary.order_date ?? ""))],
    ["Kunde", String(summary.customer ?? "-")],
    ["Payment", String(summary.payment_method ?? "-")],
    ["Status", String(summary.fulfillment_status ?? "-")],
  ] as const;
  const financeRows = [
    ["Umsatz", formatCurrencyFromCents(summary.total_cents)],
    ["Fees", formatCurrencyFromCents(summary.fees_cents)],
    ["After Fees", formatCurrencyFromCents(summary.after_fees_cents)],
    ["Einkauf", formatCurrencyFromCents(summary.purchase_cost_cents)],
    ["Gewinn", formatCurrencyFromCents(summary.profit_cents)],
  ] as const;

  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-[color:rgba(19,25,37,0.28)] backdrop-blur-[1px]">
      <div className="flex h-full w-full max-w-[980px] flex-col overflow-y-auto border-l border-[var(--border)] bg-[var(--panel)] shadow-[0_18px_50px_rgba(18,25,37,0.18)]">
        <div className="sticky top-0 z-10 border-b border-[var(--border)] bg-[var(--panel)] px-5 py-4">
          <div className="flex items-start justify-between gap-4">
            <div>
              <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">Order Details</p>
              <h2 className="mt-1 font-[var(--font-display)] text-2xl font-semibold text-[var(--ink)]">
                {String(summary.external_order_id ?? summary.order_id ?? selectedOrder.orderId)}
              </h2>
              <p className="mt-2 text-sm text-[var(--ink-4)]">
                {String(summary.marketplace ?? selectedOrder.marketplace).toUpperCase()} · {String(summary.customer ?? "Unbekannt")}
              </p>
            </div>
            <div className="flex items-center gap-2">
              <a href="/legacy?tab=orders" rel="noreferrer">
                <Button size="sm" variant="outline">
                  Legacy
                  <ArrowUpRight className="ml-2 h-4 w-4" />
                </Button>
              </a>
              <Button onClick={onClose} size="sm" variant="ghost">
                <X className="h-4 w-4" />
              </Button>
            </div>
          </div>
        </div>

        <div className="space-y-4 p-5">
          {detailQuery.isLoading ? (
            <SurfaceCard>
              <div className="flex items-center gap-3 text-sm text-[var(--ink-4)]">
                <LoaderCircle className="h-4 w-4 animate-spin" />
                Order-Details werden geladen...
              </div>
            </SurfaceCard>
          ) : detailQuery.isError ? (
            <SurfaceCard>
              <div className="text-sm text-[var(--danger)]">Order-Details konnten nicht geladen werden: {detailQuery.error.message}</div>
            </SurfaceCard>
          ) : (
            <>
              <section className="grid gap-4 xl:grid-cols-2">
                <SurfaceCard title="Order Summary">
                  <div className="grid gap-3 sm:grid-cols-2">
                    {infoRows.map(([label, value]) => (
                      <InfoTile key={label} label={label} value={value} />
                    ))}
                  </div>
                </SurfaceCard>

                <SurfaceCard title="Finanzen">
                  <div className="grid gap-3 sm:grid-cols-2">
                    {financeRows.map(([label, value]) => (
                      <InfoTile key={label} label={label} value={value} />
                    ))}
                  </div>
                </SurfaceCard>
              </section>

              <section className="grid gap-4 xl:grid-cols-[1.15fr_0.85fr]">
                <SurfaceCard description="Speichert Einkauf, Lieferant und Notizen direkt auf der bestehenden Orders-API." title="Einkauf & Rechnung">
                  <div className="grid gap-3 sm:grid-cols-2">
                    <label className="block sm:col-span-1">
                      <span className="mb-2 block text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">Einkaufspreis (EUR)</span>
                      <input
                        className="w-full rounded-2xl border border-[var(--border-strong)] bg-[var(--surface)] px-3 py-2.5 text-sm text-[var(--ink)] outline-none"
                        inputMode="decimal"
                        onChange={(event) => setPurchaseCostInput(event.target.value)}
                        placeholder="0.00"
                        type="text"
                        value={purchaseCostInput}
                      />
                    </label>
                    <label className="block sm:col-span-1">
                      <span className="mb-2 block text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">Lieferant</span>
                      <input
                        className="w-full rounded-2xl border border-[var(--border-strong)] bg-[var(--surface)] px-3 py-2.5 text-sm text-[var(--ink)] outline-none"
                        onChange={(event) => setSupplierName(event.target.value)}
                        placeholder="Lieferant oder Quelle"
                        type="text"
                        value={supplierName}
                      />
                    </label>
                    <label className="block sm:col-span-2">
                      <span className="mb-2 block text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">Notizen</span>
                      <textarea
                        className="min-h-[108px] w-full rounded-2xl border border-[var(--border-strong)] bg-[var(--surface)] px-3 py-2.5 text-sm text-[var(--ink)] outline-none"
                        onChange={(event) => setPurchaseNotes(event.target.value)}
                        placeholder="Einkauf, Lieferbedingungen oder Rechnungsnotizen"
                        value={purchaseNotes}
                      />
                    </label>
                    <label className="block sm:col-span-2">
                      <span className="mb-2 block text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">Rechnung hochladen</span>
                      <input
                        accept=".pdf,.png,.jpg,.jpeg,.webp,.txt"
                        className="block w-full rounded-2xl border border-[var(--border-strong)] bg-[var(--surface)] px-3 py-2.5 text-sm text-[var(--ink)] file:mr-3 file:rounded-xl file:border-0 file:bg-[var(--surface-2)] file:px-3 file:py-2 file:text-xs file:font-medium"
                        onChange={(event) => setSelectedFile(event.target.files?.[0] ?? null)}
                        type="file"
                      />
                      <p className="mt-2 text-xs text-[var(--ink-4)]">{selectedFile ? selectedFile.name : "Keine neue Datei ausgewaehlt."}</p>
                    </label>
                  </div>

                  {formMessage ? (
                    <div
                      className={`mt-4 rounded-2xl border px-4 py-3 text-sm ${
                        formMessage.tone === "ok"
                          ? "border-[color:rgba(39,134,86,0.22)] bg-[color:rgba(239,250,244,0.96)] text-[color:#13613f]"
                          : "border-[color:rgba(183,72,55,0.24)] bg-[color:rgba(255,241,238,0.92)] text-[var(--danger)]"
                      }`}
                    >
                      {formMessage.text}
                    </div>
                  ) : null}

                  <div className="mt-4 flex flex-wrap gap-2">
                    <Button
                      disabled={purchaseMutation.isPending || invoiceMutation.isPending}
                      onClick={() => {
                        try {
                          setFormMessage(null);
                          purchaseMutation.mutate({
                            marketplace: selectedOrder.marketplace,
                            orderId: selectedOrder.orderId,
                            purchase_cost_eur: parsePurchaseValue(purchaseCostInput),
                            supplier_name: supplierName,
                            purchase_notes: purchaseNotes,
                          });
                        } catch (error) {
                          setFormMessage({ tone: "error", text: error instanceof Error ? error.message : "Unbekannter Fehler" });
                        }
                      }}
                    >
                      <Save className="mr-2 h-4 w-4" />
                      Einkauf speichern
                    </Button>
                    <Button
                      disabled={!selectedFile || purchaseMutation.isPending || invoiceMutation.isPending}
                      onClick={() => {
                        if (!selectedFile) {
                          return;
                        }
                        try {
                          setFormMessage(null);
                          invoiceMutation.mutate({
                            marketplace: selectedOrder.marketplace,
                            orderId: selectedOrder.orderId,
                            file: selectedFile,
                            purchase_cost_eur: parsePurchaseValue(purchaseCostInput),
                            supplier_name: supplierName,
                            purchase_notes: purchaseNotes,
                          });
                        } catch (error) {
                          setFormMessage({ tone: "error", text: error instanceof Error ? error.message : "Unbekannter Fehler" });
                        }
                      }}
                      variant="secondary"
                    >
                      <FileUp className="mr-2 h-4 w-4" />
                      Rechnung hochladen
                    </Button>
                  </div>
                </SurfaceCard>

                <SurfaceCard title="Verknuepfte Dateien & Buchungen">
                  <div className="space-y-4 text-sm text-[var(--ink-4)]">
                    <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface-2)] p-4">
                      <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">Rechnung</p>
                      {invoiceHref ? (
                        <a className="mt-2 inline-flex items-center gap-2 font-medium text-[color:#1a6cc6] underline-offset-4 hover:underline" href={invoiceHref} rel="noreferrer" target="_blank">
                          {invoice?.original_filename ?? invoice?.stored_filename ?? "Rechnung oeffnen"}
                          <ArrowUpRight className="h-4 w-4" />
                        </a>
                      ) : (
                        <p className="mt-2">Noch keine Rechnung verknuepft.</p>
                      )}
                    </div>

                    <div className="grid gap-3 sm:grid-cols-2">
                      <InfoTile label="Buchungs-Match" value={String(bookkeeping.matched_via ?? "-")} />
                      <InfoTile label="DB verfuegbar" value={bookkeeping.db_available ? "Ja" : "Nein"} />
                      <InfoTile label="Buchungs-Einnahmen" value={formatCurrencyFromCents(bookkeeping.income_total_cents)} />
                      <InfoTile
                        label="Zusatz-Ausgaben"
                        value={formatCurrencyFromCents(bookkeeping.additional_expense_total_cents ?? bookkeeping.expense_total_cents)}
                      />
                    </div>
                  </div>
                </SurfaceCard>
              </section>

              <section className="grid gap-4 xl:grid-cols-2">
                <SurfaceCard title="Lieferadresse">
                  <AddressBlock lines={addressLines(shippingAddress)} />
                </SurfaceCard>
                <SurfaceCard title="Rechnungsadresse">
                  <AddressBlock lines={addressLines(billingAddress)} />
                </SurfaceCard>
              </section>

              <DataTableShell
                description={lineItems.length ? "Line Items aus Shopify." : "Order Units aus Kaufland."}
                meta={`${lineItems.length || units.length} Zeilen`}
                title={lineItems.length ? "Line Items" : "Order Units"}
              >
                <table className="min-w-full border-collapse text-sm">
                  <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
                    <tr>
                      <th className="px-4 py-3 font-medium">Titel</th>
                      <th className="px-4 py-3 font-medium">Menge / Status</th>
                      <th className="px-4 py-3 font-medium">Preis</th>
                      <th className="px-4 py-3 font-medium">SKU / Unit</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(lineItems.length ? lineItems : units).length ? (
                      (lineItems.length ? lineItems : units).map((row, index) => (
                        <tr className="border-t border-[var(--border)] text-[var(--ink-2)]" key={`${index}-${String(row.id ?? row.id_order_unit ?? row.title ?? row.product_title ?? "row")}`}>
                          <td className="px-4 py-3 text-[var(--ink)]">{String(row.title ?? row.product_title ?? "-")}</td>
                          <td className="px-4 py-3">{String(row.quantity ?? row.status ?? "-")}</td>
                          <td className="px-4 py-3">{String(row.price ?? row.revenue_gross ?? "-")}</td>
                          <td className="px-4 py-3">{String(row.sku ?? row.id_order_unit ?? "-")}</td>
                        </tr>
                      ))
                    ) : (
                      <tr>
                        <td className="px-4 py-4 text-[var(--ink-4)]" colSpan={4}>
                          Keine Detailzeilen verfuegbar.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </DataTableShell>

              {transactions.length ? (
                <DataTableShell description="Shopify Payment-Transaktionen." meta={`${transactions.length} Zeilen`} title="Payment Transaktionen">
                  <table className="min-w-full border-collapse text-sm">
                    <thead className="bg-[var(--surface-2)] text-left text-xs uppercase tracking-[0.16em] text-[var(--ink-4)]">
                      <tr>
                        <th className="px-4 py-3 font-medium">Kind</th>
                        <th className="px-4 py-3 font-medium">Status</th>
                        <th className="px-4 py-3 font-medium">Gateway</th>
                        <th className="px-4 py-3 font-medium">Amount</th>
                        <th className="px-4 py-3 font-medium">Net</th>
                      </tr>
                    </thead>
                    <tbody>
                      {transactions.map((row, index) => (
                        <tr className="border-t border-[var(--border)] text-[var(--ink-2)]" key={`${index}-${String(row.id ?? row.kind ?? "tx")}`}>
                          <td className="px-4 py-3 text-[var(--ink)]">{String(row.kind ?? "-")}</td>
                          <td className="px-4 py-3">{String(row.status ?? "-")}</td>
                          <td className="px-4 py-3">{String(row.gateway ?? "-")}</td>
                          <td className="px-4 py-3">{String(row.amount ?? "-")}</td>
                          <td className="px-4 py-3">{String(row.net_amount ?? "-")}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </DataTableShell>
              ) : null}
            </>
          )}
        </div>
      </div>
    </div>
  );
}

async function invalidateRelatedQueries(queryClient: ReturnType<typeof useQueryClient>, selectedOrder: SelectedOrderRef | null) {
  await Promise.all([
    queryClient.invalidateQueries({ queryKey: ["orders-preview"] }),
    queryClient.invalidateQueries({ queryKey: ["analytics-preview"] }),
    queryClient.invalidateQueries({ queryKey: ["customers-preview"] }),
    queryClient.invalidateQueries({ queryKey: ["customer-locations-preview"] }),
    selectedOrder
      ? queryClient.invalidateQueries({ queryKey: ["order-detail-preview", selectedOrder.marketplace, selectedOrder.orderId] })
      : Promise.resolve(),
  ]);
}

function InfoTile({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface-2)] px-4 py-3">
      <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">{label}</p>
      <p className="mt-2 break-words text-sm font-medium text-[var(--ink)]">{value || "-"}</p>
    </div>
  );
}

function AddressBlock({ lines }: { lines: string[] }) {
  if (!lines.length) {
    return <p className="text-sm text-[var(--ink-4)]">Keine Adressdaten verfuegbar.</p>;
  }

  return (
    <div className="space-y-1 text-sm text-[var(--ink-2)]">
      {lines.map((line, index) => (
        <div key={`${line}-${index}`}>{line}</div>
      ))}
    </div>
  );
}
