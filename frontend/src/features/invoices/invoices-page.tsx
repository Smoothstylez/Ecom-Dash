import { useEffect, useMemo, useRef, useState } from "react";

import { useDashboardShellState } from "@/app/dashboard-shell-state";
import { formatMoneyFromCents, NUMBER_FORMATTER } from "@/features/analytics/format";
import { fetchOrders, type OrderSummary } from "@/features/orders/api";

import {
  buildInvoicePreviewUrl,
  buildSalesInvoicePdfUrl,
  createInvoice,
  fetchInvoiceDraft,
  fetchInvoiceProfile,
  fetchVatReport,
  fetchInvoices,
  updateInvoiceProfile,
  type InvoiceDraft,
  type InvoiceSellerProfile,
  type SalesInvoice,
  type VatReport,
} from "./api";

type StatusMessage = {
  text: string;
  level: "info" | "ok" | "error";
};

type OrdersState = {
  loading: boolean;
  error: string;
  items: OrderSummary[];
};

type InvoicesState = {
  loading: boolean;
  error: string;
  items: SalesInvoice[];
};

const ORDER_LIMIT = 60;
const INVOICE_LIMIT = 120;
const TEMPLATE_OPTIONS = [
  { key: "clean", label: "Clean" },
  { key: "compact", label: "Compact" },
  { key: "brand", label: "Brand" },
] as const;

const EMPTY_PROFILE: InvoiceSellerProfile = {
  legal_name: "",
  street: "",
  address_line2: "",
  postcode: "",
  city: "",
  country: "DE",
  email: "",
  phone: "",
  vat_id: "",
  tax_number: "",
  tax_mode: "small_business",
  vat_effective_from: "",
  invoice_prefix: "RE",
  default_template: "clean",
  footer_note: "",
  payment_note: "",
  eu_invoicing_enabled: false,
};

function previousMonthToken() {
  const now = new Date();
  const previousMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  return `${previousMonth.getFullYear()}-${String(previousMonth.getMonth() + 1).padStart(2, "0")}`;
}

function isoToLocalDateTimeInput(value: string | undefined) {
  if (!value) {
    return "";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }
  const year = parsed.getFullYear();
  const month = String(parsed.getMonth() + 1).padStart(2, "0");
  const day = String(parsed.getDate()).padStart(2, "0");
  const hours = String(parsed.getHours()).padStart(2, "0");
  const minutes = String(parsed.getMinutes()).padStart(2, "0");
  return `${year}-${month}-${day}T${hours}:${minutes}`;
}

function localDateTimeInputToIso(value: string) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    return text;
  }
  return parsed.toISOString();
}

function orderRowKey(order: OrderSummary) {
  return `${String(order.marketplace || "").trim()}:${String(order.order_id || "").trim()}`;
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

function addressLines(value: unknown) {
  const record = value && typeof value === "object" ? value as Record<string, unknown> : {};
  const lines = [
    String(record.name || "").trim(),
    String(record.company || "").trim(),
    String(record.street || "").trim(),
    String(record.address_line2 || "").trim(),
    [String(record.postcode || "").trim(), String(record.city || "").trim()].filter(Boolean).join(" "),
    String(record.country || "").trim(),
  ].filter(Boolean);
  return lines.length ? lines : ["-"];
}

type InvoicesPageProps = {
  isActive: boolean;
};

export function InvoicesPage({ isActive }: InvoicesPageProps) {
  const { filters: shellFilters, refreshRequestToken } = useDashboardShellState();
  const [ordersState, setOrdersState] = useState<OrdersState>({ loading: true, error: "", items: [] });
  const [invoicesState, setInvoicesState] = useState<InvoicesState>({ loading: true, error: "", items: [] });
  const [profile, setProfile] = useState<InvoiceSellerProfile>(EMPTY_PROFILE);
  const [profileLoading, setProfileLoading] = useState(true);
  const [profileSaving, setProfileSaving] = useState(false);
  const [draftLoading, setDraftLoading] = useState(false);
  const [draftError, setDraftError] = useState("");
  const [draft, setDraft] = useState<InvoiceDraft | null>(null);
  const [vatReportMonth, setVatReportMonth] = useState(previousMonthToken());
  const [vatReport, setVatReport] = useState<VatReport | null>(null);
  const [vatReportLoading, setVatReportLoading] = useState(false);
  const [vatReportError, setVatReportError] = useState("");
  const [selectedOrderKey, setSelectedOrderKey] = useState("");
  const [selectedTemplate, setSelectedTemplate] = useState("clean");
  const [statusMessage, setStatusMessage] = useState<StatusMessage | null>(null);
  const [createBusy, setCreateBusy] = useState(false);
  const [previewNonce, setPreviewNonce] = useState(0);
  const lastRefreshTokenRef = useRef(refreshRequestToken);

  const selectedOrder = useMemo(() => {
    return ordersState.items.find((order) => orderRowKey(order) === selectedOrderKey) || null;
  }, [ordersState.items, selectedOrderKey]);

  const previewUrl = useMemo(() => {
    if (!selectedOrder) {
      return "";
    }
    return buildInvoicePreviewUrl(
      String(selectedOrder.marketplace || ""),
      String(selectedOrder.order_id || ""),
      selectedTemplate,
      previewNonce,
    );
  }, [previewNonce, selectedOrder, selectedTemplate]);

  useEffect(() => {
    if (!isActive) {
      return;
    }
    let cancelled = false;
    setProfileLoading(true);
    fetchInvoiceProfile()
      .then((payload) => {
        if (cancelled) {
          return;
        }
        const nextProfile = payload.profile && typeof payload.profile === "object"
          ? { ...EMPTY_PROFILE, ...payload.profile }
          : EMPTY_PROFILE;
        setProfile(nextProfile);
        setSelectedTemplate(String(nextProfile.default_template || "clean") || "clean");
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setProfile({ ...EMPTY_PROFILE });
          setStatusMessage({ text: `Profil konnte nicht geladen werden: ${error.message}`, level: "error" });
        }
      })
      .finally(() => {
        if (!cancelled) {
          setProfileLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [isActive, refreshRequestToken]);

  useEffect(() => {
    if (!isActive) {
      return;
    }
    let cancelled = false;
    setOrdersState((current) => ({ ...current, loading: true, error: "" }));
    setInvoicesState((current) => ({ ...current, loading: true, error: "" }));

    void fetchOrders({
      from: shellFilters.from,
      to: shellFilters.to,
      marketplace: shellFilters.marketplace,
      q: shellFilters.q,
      limit: ORDER_LIMIT,
      offset: 0,
    })
      .then((payload) => {
        if (cancelled) {
          return;
        }
        const items = Array.isArray(payload.items) ? payload.items : [];
        setOrdersState({ loading: false, error: "", items });
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setOrdersState({ loading: false, error: error.message, items: [] });
        }
      });

    void fetchInvoices({
      from: shellFilters.from,
      to: shellFilters.to,
      marketplace: shellFilters.marketplace,
      q: shellFilters.q,
      limit: INVOICE_LIMIT,
      offset: 0,
    })
      .then((payload) => {
        if (cancelled) {
          return;
        }
        setInvoicesState({
          loading: false,
          error: "",
          items: Array.isArray(payload.items) ? payload.items : [],
        });
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setInvoicesState({ loading: false, error: error.message, items: [] });
        }
      });

    return () => {
      cancelled = true;
    };
  }, [isActive, shellFilters.from, shellFilters.marketplace, shellFilters.q, shellFilters.to, refreshRequestToken]);

  useEffect(() => {
    if (!isActive || !vatReportMonth) {
      return;
    }
    let cancelled = false;
    setVatReportLoading(true);
    setVatReportError("");
    void fetchVatReport(vatReportMonth)
      .then((payload) => {
        if (!cancelled) {
          setVatReport(payload);
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setVatReport(null);
          setVatReportError(error.message);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setVatReportLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [isActive, refreshRequestToken, vatReportMonth]);

  useEffect(() => {
    if (!selectedOrderKey) {
      return;
    }
    if (selectedOrder) {
      return;
    }
    setSelectedOrderKey("");
    setDraft(null);
  }, [selectedOrder, selectedOrderKey]);

  useEffect(() => {
    if (!isActive || !selectedOrder) {
      setDraft(null);
      setDraftError("");
      return;
    }
    let cancelled = false;
    setDraftLoading(true);
    setDraftError("");
    void fetchInvoiceDraft(String(selectedOrder.marketplace || ""), String(selectedOrder.order_id || ""), selectedTemplate)
      .then((payload) => {
        if (!cancelled) {
          setDraft(payload);
          setPreviewNonce(Date.now());
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setDraft(null);
          setDraftError(error.message);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setDraftLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [isActive, selectedOrder, selectedTemplate]);

  useEffect(() => {
    if (refreshRequestToken === 0 || lastRefreshTokenRef.current === refreshRequestToken) {
      return;
    }
    lastRefreshTokenRef.current = refreshRequestToken;
    setStatusMessage(null);
  }, [refreshRequestToken]);

  function updateProfileField<Key extends keyof InvoiceSellerProfile>(field: Key, value: InvoiceSellerProfile[Key]) {
    setProfile((current) => ({ ...current, [field]: value }));
  }

  async function handleProfileSave() {
    setProfileSaving(true);
    try {
      const payload = await updateInvoiceProfile(profile);
      const nextProfile = payload.profile && typeof payload.profile === "object"
        ? { ...EMPTY_PROFILE, ...payload.profile }
        : { ...profile };
      setProfile(nextProfile);
      setSelectedTemplate(String(nextProfile.default_template || selectedTemplate) || "clean");
      setPreviewNonce(Date.now());
      setStatusMessage({ text: "Verkaeuferprofil gespeichert.", level: "ok" });
      if (selectedOrder) {
        const nextDraft = await fetchInvoiceDraft(String(selectedOrder.marketplace || ""), String(selectedOrder.order_id || ""), selectedTemplate);
        setDraft(nextDraft);
        setDraftError("");
      }
    } catch (error) {
      setStatusMessage({
        text: `Profil konnte nicht gespeichert werden: ${error instanceof Error ? error.message : "Unbekannter Fehler"}`,
        level: "error",
      });
    } finally {
      setProfileSaving(false);
    }
  }

  async function handleCreateInvoice() {
    if (!selectedOrder) {
      return;
    }
    setCreateBusy(true);
    try {
      const payload = await createInvoice(String(selectedOrder.marketplace || ""), String(selectedOrder.order_id || ""), selectedTemplate);
      const createdInvoice = payload.invoice || null;
      setStatusMessage({
        text: `Rechnung erstellt${createdInvoice?.invoice_number ? `: ${createdInvoice.invoice_number}` : "."}`,
        level: "ok",
      });
      const [nextDraft, nextInvoices] = await Promise.all([
        fetchInvoiceDraft(String(selectedOrder.marketplace || ""), String(selectedOrder.order_id || ""), selectedTemplate).catch(() => null),
        fetchInvoices({
          from: shellFilters.from,
          to: shellFilters.to,
          marketplace: shellFilters.marketplace,
          q: shellFilters.q,
          limit: INVOICE_LIMIT,
          offset: 0,
        }),
      ]);
      setDraft(nextDraft);
      setInvoicesState({
        loading: false,
        error: "",
        items: Array.isArray(nextInvoices.items) ? nextInvoices.items : [],
      });
      setPreviewNonce(Date.now());
    } catch (error) {
      setStatusMessage({
        text: `Rechnung konnte nicht erstellt werden: ${error instanceof Error ? error.message : "Unbekannter Fehler"}`,
        level: "error",
      });
    } finally {
      setCreateBusy(false);
    }
  }

  return (
    <div id="invoicesPanel" className="tab-panel active" data-react-invoices-mounted="true">
      {statusMessage ? (
        <div className={`status ${statusMessage.level === "error" ? "status-error" : statusMessage.level === "ok" ? "status-ok" : "status-info"}`}>
          {statusMessage.text}
        </div>
      ) : null}

      <section className="card" style={{ marginTop: statusMessage ? 12 : 0, padding: 16 }}>
        <div className="table-head" style={{ marginBottom: 12 }}>
              <div>
                <h2 className="table-title">Kundenrechnungen</h2>
                <div className="table-meta">Rechnungsprofil, manueller USt-Start und Monatsreport fuer die USt-Zahllast.</div>
              </div>
            </div>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))", gap: 16, alignItems: "start" }}>
          <article className="card table-card" style={{ margin: 0 }}>
            <div className="table-head">
              <h3 className="table-title">Bestellungen</h3>
              <div className="table-meta">{ordersState.loading ? "..." : `${NUMBER_FORMATTER.format(ordersState.items.length)} sichtbar`}</div>
            </div>
            {ordersState.error ? <div className="status status-error">{ordersState.error}</div> : null}
            <div className="table-wrap" style={{ maxHeight: 720, overflow: "auto" }}>
              <table>
                <thead>
                  <tr>
                    <th>Order</th>
                    <th>Kunde</th>
                    <th>Betrag</th>
                  </tr>
                </thead>
                <tbody>
                  {ordersState.loading ? (
                    <tr>
                      <td colSpan={3}>Bestellungen werden geladen...</td>
                    </tr>
                  ) : ordersState.items.length ? ordersState.items.map((order) => {
                    const key = orderRowKey(order);
                    const active = key === selectedOrderKey;
                    return (
                      <tr
                        key={key}
                        data-invoice-order-row="true"
                        style={active ? { background: "rgba(41, 94, 174, 0.10)" } : undefined}
                        onClick={() => {
                          setSelectedOrderKey(key);
                        }}
                      >
                        <td>
                          <div><strong>{String(order.external_order_id || order.order_id || "-")}</strong></div>
                          <div className="cell-sub">{String(order.marketplace || "-")}</div>
                          <div className="cell-sub">{formatDateTime(order.order_date)}</div>
                        </td>
                        <td>
                          <div>{String(order.customer || "-")}</div>
                          <div className="cell-sub" title={String(order.article || "-")}>{String(order.article || "-")}</div>
                        </td>
                        <td><strong>{formatMoneyFromCents(Number(order.total_cents || 0))}</strong></td>
                      </tr>
                    );
                  }) : (
                    <tr>
                      <td colSpan={3}>Keine Bestellungen fuer den aktuellen Filter.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </article>

          <div style={{ display: "grid", gap: 16 }}>
            <section className="card" style={{ padding: 16 }}>
              <div className="table-head" style={{ marginBottom: 12 }}>
                <h3 className="table-title">Verkaeuferprofil</h3>
                <div className="table-meta">Ein Profil fuer alle Rechnungen</div>
              </div>
              {profileLoading ? <div className="table-meta">Profil wird geladen...</div> : null}
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 12 }}>
                <label>
                  <div className="table-meta">Rechtlicher Name</div>
                  <input className="settings-inline-input" value={String(profile.legal_name || "")} onChange={(event) => updateProfileField("legal_name", event.target.value)} />
                </label>
                <label>
                  <div className="table-meta">E-Mail</div>
                  <input className="settings-inline-input" value={String(profile.email || "")} onChange={(event) => updateProfileField("email", event.target.value)} />
                </label>
                <label>
                  <div className="table-meta">Strasse</div>
                  <input className="settings-inline-input" value={String(profile.street || "")} onChange={(event) => updateProfileField("street", event.target.value)} />
                </label>
                <label>
                  <div className="table-meta">Adresszusatz</div>
                  <input className="settings-inline-input" value={String(profile.address_line2 || "")} onChange={(event) => updateProfileField("address_line2", event.target.value)} />
                </label>
                <label>
                  <div className="table-meta">PLZ</div>
                  <input className="settings-inline-input" value={String(profile.postcode || "")} onChange={(event) => updateProfileField("postcode", event.target.value)} />
                </label>
                <label>
                  <div className="table-meta">Ort</div>
                  <input className="settings-inline-input" value={String(profile.city || "")} onChange={(event) => updateProfileField("city", event.target.value)} />
                </label>
                <label>
                  <div className="table-meta">Land</div>
                  <input className="settings-inline-input" value={String(profile.country || "DE")} onChange={(event) => updateProfileField("country", event.target.value.toUpperCase())} />
                </label>
                <label>
                  <div className="table-meta">Telefon</div>
                  <input className="settings-inline-input" value={String(profile.phone || "")} onChange={(event) => updateProfileField("phone", event.target.value)} />
                </label>
                <label>
                  <div className="table-meta">Steuernummer</div>
                  <input className="settings-inline-input" value={String(profile.tax_number || "")} onChange={(event) => updateProfileField("tax_number", event.target.value)} />
                </label>
                <label>
                  <div className="table-meta">USt-IdNr.</div>
                  <input className="settings-inline-input" value={String(profile.vat_id || "")} onChange={(event) => updateProfileField("vat_id", event.target.value.toUpperCase())} />
                </label>
                <label>
                  <div className="table-meta">Steuermodus</div>
                  <select className="settings-inline-input" value={String(profile.tax_mode || "small_business")} onChange={(event) => updateProfileField("tax_mode", event.target.value)}>
                    <option value="small_business">Kleinunternehmer</option>
                    <option value="regular">Regelbesteuert</option>
                  </select>
                </label>
                <label>
                  <div className="table-meta">USt aktiv ab</div>
                  <input
                    className="settings-inline-input"
                    type="datetime-local"
                    value={isoToLocalDateTimeInput(String(profile.vat_effective_from || ""))}
                    onChange={(event) => updateProfileField("vat_effective_from", localDateTimeInputToIso(event.target.value))}
                  />
                </label>
                <label>
                  <div className="table-meta">Nummern-Praefix</div>
                  <input className="settings-inline-input" value={String(profile.invoice_prefix || "RE")} onChange={(event) => updateProfileField("invoice_prefix", event.target.value.toUpperCase())} />
                </label>
                <label>
                  <div className="table-meta">Standard-Template</div>
                  <select className="settings-inline-input" value={String(profile.default_template || "clean")} onChange={(event) => updateProfileField("default_template", event.target.value)}>
                    {TEMPLATE_OPTIONS.map((option) => <option key={option.key} value={option.key}>{option.label}</option>)}
                  </select>
                </label>
                <label style={{ display: "flex", alignItems: "end", gap: 8 }}>
                  <input
                    type="checkbox"
                    checked={Boolean(profile.eu_invoicing_enabled)}
                    onChange={(event) => updateProfileField("eu_invoicing_enabled", event.target.checked)}
                  />
                  <span className="table-meta">EU-Modus als vorbereitet markieren</span>
                </label>
              </div>
              <div style={{ display: "grid", gap: 12, marginTop: 12 }}>
                <label>
                  <div className="table-meta">Zahlungshinweis</div>
                  <textarea className="settings-inline-input" rows={2} value={String(profile.payment_note || "")} onChange={(event) => updateProfileField("payment_note", event.target.value)} />
                </label>
                <label>
                  <div className="table-meta">Footer / Zusatzhinweis</div>
                  <textarea className="settings-inline-input" rows={3} value={String(profile.footer_note || "")} onChange={(event) => updateProfileField("footer_note", event.target.value)} />
                </label>
              </div>
              <div style={{ display: "flex", justifyContent: "flex-end", marginTop: 12 }}>
                <button className="btn-inline primary" type="button" disabled={profileSaving} onClick={() => void handleProfileSave()}>
                  {profileSaving ? "Speichern..." : "Profil speichern"}
                </button>
              </div>
            </section>

            <section className="card" style={{ padding: 16 }}>
              <div className="table-head" style={{ marginBottom: 12 }}>
                <div>
                  <h3 className="table-title">Umsatzsteuer Report</h3>
                  <div className="table-meta">Basis: Order-Eingangszeitpunkt innerhalb des Monats</div>
                </div>
                <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                  <span className="table-meta">Monat</span>
                  <input className="settings-inline-input" type="month" value={vatReportMonth} onChange={(event) => setVatReportMonth(event.target.value)} />
                </div>
              </div>

              {vatReportError ? <div className="status status-error">{vatReportError}</div> : null}
              {vatReportLoading ? <div className="table-meta">USt-Report wird geladen...</div> : null}

              {vatReport ? (
                <div style={{ display: "grid", gap: 16 }}>
                  {vatReport.threshold_candidate ? (
                    <article className="card" style={{ margin: 0, padding: 12 }}>
                      <div className="table-meta">Schwellen-Hinweis</div>
                      <div style={{ fontWeight: 600 }}>{String(vatReport.threshold_candidate.external_order_id || vatReport.threshold_candidate.order_id || "-")}</div>
                      <div className="cell-sub">{String(vatReport.threshold_candidate.marketplace || "-")} | {formatDateTime(vatReport.threshold_candidate.order_date)}</div>
                      <div className="cell-sub">Kumuliert: {formatMoneyFromCents(Number(vatReport.threshold_candidate.cumulative_gross_cents || 0))}</div>
                    </article>
                  ) : null}

                  <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 12 }}>
                    <article className="card" style={{ margin: 0, padding: 12 }}>
                      <div className="table-meta">Ausgangs-USt</div>
                      <div style={{ fontWeight: 700, fontSize: "1.05rem" }}>{formatMoneyFromCents(Number(vatReport.totals?.output_vat_total_cents || 0))}</div>
                    </article>
                    <article className="card" style={{ margin: 0, padding: 12 }}>
                      <div className="table-meta">Vorsteuer Bestellungen</div>
                      <div style={{ fontWeight: 700, fontSize: "1.05rem" }}>{formatMoneyFromCents(Number(vatReport.totals?.deductible_purchase_vat_total_cents || 0))}</div>
                    </article>
                    <article className="card" style={{ margin: 0, padding: 12 }}>
                      <div className="table-meta">Vorsteuer Gebuehren</div>
                      <div style={{ fontWeight: 700, fontSize: "1.05rem" }}>{formatMoneyFromCents(Number(vatReport.totals?.monthly_fee_vat_total_cents || 0))}</div>
                    </article>
                    <article className="card" style={{ margin: 0, padding: 12 }}>
                      <div className="table-meta">Vorsteuer Sonstiges</div>
                      <div style={{ fontWeight: 700, fontSize: "1.05rem" }}>{formatMoneyFromCents(Number(vatReport.totals?.manual_input_vat_total_cents || 0))}</div>
                    </article>
                    <article className="card" style={{ margin: 0, padding: 12 }}>
                      <div className="table-meta">Ans Finanzamt</div>
                      <div style={{ fontWeight: 700, fontSize: "1.1rem" }}>{formatMoneyFromCents(Number(vatReport.totals?.vat_payable_total_cents || 0))}</div>
                    </article>
                  </div>

                  {Array.isArray(vatReport.warnings) && vatReport.warnings.length ? vatReport.warnings.map((message) => (
                    <div key={message} className="status status-info">{message}</div>
                  )) : null}

                  <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))", gap: 16 }}>
                    <article className="card" style={{ margin: 0, padding: 12 }}>
                      <div className="table-meta" style={{ marginBottom: 8 }}>USt-pflichtige Orders</div>
                      <div className="table-wrap" style={{ maxHeight: 360, overflow: "auto" }}>
                        <table>
                          <thead>
                            <tr>
                              <th>Order</th>
                              <th>Brutto</th>
                              <th>USt</th>
                              <th>VSt Einkauf</th>
                            </tr>
                          </thead>
                          <tbody>
                            {Array.isArray(vatReport.orders) && vatReport.orders.length ? vatReport.orders.map((order) => (
                              <tr key={`${String(order.marketplace || "")}:${String(order.order_id || "")}`}>
                                <td>
                                  <div><strong>{String(order.external_order_id || order.order_id || "-")}</strong></div>
                                  <div className="cell-sub">{String(order.marketplace || "-")} | {formatDateTime(order.order_date)}</div>
                                </td>
                                <td>{formatMoneyFromCents(Number(order.sales_gross_cents || 0))}</td>
                                <td>{formatMoneyFromCents(Number(order.sales_vat_cents || 0))}</td>
                                <td>{formatMoneyFromCents(Number(order.deductible_purchase_vat_cents || 0))}</td>
                              </tr>
                            )) : <tr><td colSpan={4}>Keine USt-pflichtigen Orders im Monat.</td></tr>}
                          </tbody>
                        </table>
                      </div>
                    </article>

                    <article className="card" style={{ margin: 0, padding: 12 }}>
                      <div className="table-meta" style={{ marginBottom: 8 }}>Vorsteuer aus Sammelrechnungen</div>
                      <div className="table-wrap" style={{ maxHeight: 360, overflow: "auto" }}>
                        <table>
                          <thead>
                            <tr>
                              <th>Provider</th>
                              <th>Brutto</th>
                              <th>VSt</th>
                            </tr>
                          </thead>
                          <tbody>
                            {Array.isArray(vatReport.monthly_fee_invoices) && vatReport.monthly_fee_invoices.length ? vatReport.monthly_fee_invoices.map((invoice) => (
                              <tr key={String(invoice.id || `${String(invoice.provider || "")}:${String(invoice.period_from || "")}`)}>
                                <td>
                                  <div><strong>{String(invoice.provider || "-")}</strong></div>
                                  <div className="cell-sub">{String(invoice.period_from || "-").slice(0, 10)} bis {String(invoice.period_to || "-").slice(0, 10)}</div>
                                </td>
                                <td>{formatMoneyFromCents(Number(invoice.invoice_amount_cents || 0))}</td>
                                <td>{formatMoneyFromCents(Number(invoice.vat_amount_cents || 0))}</td>
                              </tr>
                            )) : <tr><td colSpan={3}>Keine Sammelrechnungen mit Vorsteuer.</td></tr>}
                          </tbody>
                        </table>
                      </div>
                    </article>
                  </div>
                </div>
              ) : null}
            </section>

            <section className="card" style={{ padding: 16 }}>
              <div className="table-head" style={{ marginBottom: 12 }}>
                <div>
                  <h3 className="table-title">Rechnung erstellen</h3>
                  <div className="table-meta">{selectedOrder ? `Ausgewaehlt: ${String(selectedOrder.external_order_id || selectedOrder.order_id || "-")}` : "Waehle links zuerst eine Bestellung aus."}</div>
                </div>
                <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                  <span className="table-meta">Template</span>
                  <select className="settings-inline-input" value={selectedTemplate} onChange={(event) => setSelectedTemplate(event.target.value)}>
                    {TEMPLATE_OPTIONS.map((option) => <option key={option.key} value={option.key}>{option.label}</option>)}
                  </select>
                </div>
              </div>

              {draftError ? <div className="status status-error">{draftError}</div> : null}
              {draftLoading ? <div className="table-meta">Rechnungsentwurf wird geladen...</div> : null}
              {!selectedOrder ? <div className="table-meta">Noch keine Bestellung ausgewaehlt.</div> : null}

              {selectedOrder && draft ? (
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))", gap: 16 }}>
                  <div style={{ display: "grid", gap: 12 }}>
                    <article className="card" style={{ margin: 0, padding: 12 }}>
                      <div className="table-meta">Rechnungsnummer (Vorschau)</div>
                      <div style={{ fontWeight: 700, fontSize: "1.1rem" }}>{String(draft.invoice?.invoice_number_preview || "-")}</div>
                      <div className="cell-sub">{String(draft.invoice?.invoice_date || "-")} | Lieferung {String(draft.invoice?.delivery_date || "-")}</div>
                    </article>

                    <article className="card" style={{ margin: 0, padding: 12 }}>
                      <div className="table-meta">Kunde</div>
                      <div style={{ fontWeight: 600, marginBottom: 6 }}>{String(draft.customer?.name || "-")}</div>
                      {addressLines(draft.customer?.billing_address).map((line) => <div key={line} className="cell-sub">{line}</div>)}
                    </article>

                    <article className="card" style={{ margin: 0, padding: 12 }}>
                      <div className="table-meta">Validierung</div>
                      <div className="cell-sub">Billing-Quelle: {String(draft.validation?.billing_source || "-")}</div>
                      {draft.existing_invoice?.invoice_number ? <div className="cell-sub">Vorhandene Rechnung: {draft.existing_invoice.invoice_number}</div> : null}
                      {Array.isArray(draft.validation?.blockers) && draft.validation.blockers.length ? draft.validation.blockers.map((message) => (
                        <div key={`blocker:${message}`} className="status status-error" style={{ marginTop: 8 }}>{message}</div>
                      )) : <div className="status status-ok" style={{ marginTop: 8 }}>Keine Blocker erkannt.</div>}
                      {Array.isArray(draft.validation?.warnings) && draft.validation.warnings.length ? draft.validation.warnings.map((message) => (
                        <div key={`warning:${message}`} className="status status-info" style={{ marginTop: 8 }}>{message}</div>
                      )) : null}
                    </article>

                    <article className="card" style={{ margin: 0, padding: 12 }}>
                      <div className="table-meta">Summen</div>
                      <div style={{ fontWeight: 700, fontSize: "1.05rem" }}>{formatMoneyFromCents(Number(draft.totals?.gross_cents || 0))}</div>
                      <div className="cell-sub">Versand in Entwurf: {formatMoneyFromCents(Number(draft.totals?.shipping_cents || 0))}</div>
                      <div className="cell-sub">Quelle Steuerwerte: {formatMoneyFromCents(Number(draft.totals?.source_tax_cents || 0))}</div>
                    </article>

                    <div style={{ display: "flex", gap: 8 }}>
                      <button
                        id="invoiceCreateBtn"
                        className="btn-inline primary"
                        type="button"
                        disabled={createBusy || !draft.validation?.ready}
                        onClick={() => void handleCreateInvoice()}
                      >
                        {createBusy ? "Erstelle..." : "Rechnung final erstellen"}
                      </button>
                      {draft.existing_invoice?.id ? (
                        <a className="btn-inline ghost" href={buildSalesInvoicePdfUrl(String(draft.existing_invoice.id), "inline")} target="_blank" rel="noreferrer">Vorhandene PDF</a>
                      ) : null}
                    </div>
                  </div>

                  <div>
                    <div className="table-meta" style={{ marginBottom: 8 }}>PDF Preview vor dem Download</div>
                    {previewUrl ? (
                      <iframe
                        id="invoicePreviewFrame"
                        title="Invoice Preview"
                        src={previewUrl}
                        style={{ width: "100%", minHeight: 760, border: "1px solid rgba(148, 163, 184, 0.25)", borderRadius: 12, background: "#fff" }}
                      />
                    ) : (
                      <div className="table-meta">Noch keine Preview verfuegbar.</div>
                    )}
                  </div>
                </div>
              ) : null}
            </section>
          </div>
        </div>
      </section>

      <section className="card table-card" style={{ marginTop: 16 }}>
        <div className="table-head">
          <h3 className="table-title">Archiv</h3>
          <div className="table-meta">{invoicesState.loading ? "..." : `${NUMBER_FORMATTER.format(invoicesState.items.length)} Rechnungen geladen`}</div>
        </div>
        {invoicesState.error ? <div className="status status-error">{invoicesState.error}</div> : null}
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Rechnung</th>
                <th>Bestellung</th>
                <th>Kunde</th>
                <th>Land</th>
                <th>Template</th>
                <th>Gesamt</th>
                <th>Aktionen</th>
              </tr>
            </thead>
            <tbody>
              {invoicesState.loading ? (
                <tr>
                  <td colSpan={7}>Rechnungen werden geladen...</td>
                </tr>
              ) : invoicesState.items.length ? invoicesState.items.map((invoice) => (
                <tr key={String(invoice.id || `${String(invoice.invoice_number || "-")}:${String(invoice.source_order_id || "-")}`)} data-invoice-row="true">
                  <td>
                    <div><strong>{String(invoice.invoice_number || "-")}</strong></div>
                    <div className="cell-sub">{String(invoice.invoice_date || "-")}</div>
                  </td>
                  <td>
                    <div>{String(invoice.source_external_order_id || invoice.source_order_id || "-")}</div>
                    <div className="cell-sub">{String(invoice.marketplace || "-")}</div>
                  </td>
                  <td>{String(invoice.customer_name || "-")}</td>
                  <td>{String(invoice.customer_country || "-")}</td>
                  <td>{String(invoice.template_key || "-")}</td>
                  <td><strong>{formatMoneyFromCents(Number(invoice.total_gross_cents || 0))}</strong></td>
                  <td>
                    {invoice.id ? (
                      <span className="doc-actions">
                        <a href={buildSalesInvoicePdfUrl(String(invoice.id), "inline")} target="_blank" rel="noreferrer">Preview</a>
                        <a href={buildSalesInvoicePdfUrl(String(invoice.id), "attachment")} target="_blank" rel="noreferrer">Download</a>
                      </span>
                    ) : "-"}
                  </td>
                </tr>
              )) : (
                <tr>
                  <td colSpan={7}>Noch keine finalen Rechnungen vorhanden.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
