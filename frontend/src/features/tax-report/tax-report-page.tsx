import { useCallback, useEffect, useState } from "react";

import { formatMoneyFromCents } from "@/features/analytics/format";

import {
  amendReport,
  bulkOverrideZeroRates,
  fetchDocuments,
  fetchUstReport,
  fileReport,
  patchDocument,
  refreshReport,
  saveSettings,
  uploadDocument,
  type UstDocument,
  type UstReport,
} from "./api";
import {
  INPUT_VAT_BUCKETS,
  REGIME_OPTIONS,
  classLabel,
  issueLabel,
  monthLabel,
  toCents,
} from "./labels";
import { TaxReportMonthPicker } from "./tax-report-month-picker";

function cx(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(" ");
}

function previousMonthToken(): string {
  const now = new Date();
  now.setDate(1);
  now.setMonth(now.getMonth() - 1);
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
}

function DetailRows({ items }: { items: Array<[string, string]> }) {
  return (
    <>
      {items.map(([label, value]) => (
        <div key={label} className="detail-row">
          <span>{label}</span>
          <strong>{value}</strong>
        </div>
      ))}
    </>
  );
}

type TabToken = "uebersicht" | "kaufland" | "amazon" | "vorsteuer" | "belege";

const TABS: Array<{ token: TabToken; label: string }> = [
  { token: "uebersicht", label: "Übersicht" },
  { token: "kaufland", label: "Kaufland" },
  { token: "amazon", label: "Amazon" },
  { token: "vorsteuer", label: "Vorsteuer" },
  { token: "belege", label: "Eingangsrechnungen" },
];

const AMAZON_ORDER = [
  "de_b2c",
  "eu_b2b_intra_community_supply",
  "eu_b2c_home_rate",
  "returns",
  "deemed_supplier",
  "export",
  "unresolved",
];

const STATUS_LABELS: Record<string, { text: string; className: string }> = {
  draft: { text: "Entwurf", className: "badge badge-default" },
  ready: { text: "Bereit", className: "badge badge-invoice" },
  filed: { text: "Abgegeben", className: "badge badge-sale" },
};

export function TaxReportPage() {
  const [month, setMonth] = useState(previousMonthToken());
  const [tab, setTab] = useState<TabToken>("uebersicht");
  const [report, setReport] = useState<UstReport | null>(null);
  const [documents, setDocuments] = useState<UstDocument[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [status, setStatus] = useState("");
  const [showForm, setShowForm] = useState(false);
  const [regime, setRegime] = useState("unconfirmed");

  const [provider, setProvider] = useState("kaufland");
  const [docType, setDocType] = useState("fee");
  const [invoiceNumber, setInvoiceNumber] = useState("");
  const [invoiceDate, setInvoiceDate] = useState("");
  const [serviceDate, setServiceDate] = useState("");
  const [receivedDate, setReceivedDate] = useState("");
  const [grossValue, setGrossValue] = useState("");
  const [netValue, setNetValue] = useState("");
  const [vatValue, setVatValue] = useState("");
  const [file, setFile] = useState<File | null>(null);
  const [fileName, setFileName] = useState("Optional");

  const reload = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const payload = await fetchUstReport(month);
      setReport(payload);
      setRegime(String((payload.settings as Record<string, unknown>).eu_tax_regime || "unconfirmed"));
      const docs = await fetchDocuments(month);
      setDocuments(docs.items);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Laden fehlgeschlagen");
      setReport(null);
      setDocuments([]);
    } finally {
      setLoading(false);
    }
  }, [month]);

  useEffect(() => {
    void reload();
  }, [reload]);

  const run = async (action: () => Promise<unknown>, message: string) => {
    setStatus("");
    setError("");
    try {
      await action();
      setStatus(message);
      await reload();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Aktion fehlgeschlagen");
    }
  };

  const filed = report?.status === "filed";
  const blocked = Boolean(report?.blockers.length);
  const statusInfo = STATUS_LABELS[report?.status || "draft"] || STATUS_LABELS.draft;

  const kaufland = report?.sections.kaufland;
  const amazon = report?.sections.amazon || {};
  const inputVat = report?.sections.input_vat;
  const amazonNetto = Object.values(amazon).reduce((sum, bucket) => sum + (bucket?.net || 0), 0);
  const returnCount = (amazon.returns?.count || 0) + (kaufland?.returns.count || 0);
  const openCount = (report?.blockers.length || 0) + (report?.warnings.length || 0);
  const pendingOverrides = kaufland?.rate_overrides_pending || 0;

  const kpiItems: Array<{ name: string; value: string; sub: string; negative?: boolean }> = [
    {
      name: "Zahllast",
      value: formatMoneyFromCents(report?.totals.vat_payable_cents || 0),
      sub: (report?.totals.vat_payable_cents || 0) >= 0 ? "an das Finanzamt" : "Erstattung",
      negative: (report?.totals.vat_payable_cents || 0) < 0,
    },
    {
      name: "Ausgangs-USt",
      value: formatMoneyFromCents(report?.totals.output_vat_cents || 0),
      sub: "USt aus Verkäufen",
    },
    {
      name: "Vorsteuer",
      value: formatMoneyFromCents(report?.totals.input_vat_cents || 0),
      sub: "aus Einkauf und Gebühren",
    },
    {
      name: "Umsatz netto",
      value: formatMoneyFromCents((kaufland?.net_cents || 0) + amazonNetto),
      sub: "Kaufland und Amazon",
    },
    {
      name: "Retouren",
      value: String(returnCount),
      sub: "im Berichtsmonat",
    },
    {
      name: "Offene Punkte",
      value: String(openCount),
      sub: openCount ? "bitte prüfen" : "alles geklärt",
    },
  ];

  const resetForm = () => {
    setInvoiceNumber("");
    setInvoiceDate("");
    setServiceDate("");
    setReceivedDate("");
    setGrossValue("");
    setNetValue("");
    setVatValue("");
    setFile(null);
    setFileName("Optional");
  };

  return (
    <section className="page" aria-label="USt-Report">
      <div className="table-head" style={{ marginBottom: 12 }}>
        <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
          <TaxReportMonthPicker value={month} onChange={setMonth} />
          <span className={statusInfo.className}>{statusInfo.text}</span>
          {report?.revision ? (
            <span className="table-meta">
              {report.kind === "amendment" ? `Berichtigung Nr. ${report.revision}` : `Fassung ${report.revision}`}
            </span>
          ) : null}
        </div>
        <div className="orders-head-actions">
          <button className="btn-inline ghost" type="button" onClick={() => void reload()}>
            Aktualisieren
          </button>
          <button
            className="btn-inline secondary"
            type="button"
            disabled={!filed}
            onClick={() => void run(() => amendReport(month), "Berichtigung angelegt")}
          >
            Berichtigen
          </button>
          <button
            className="btn-inline primary"
            type="button"
            disabled={filed || blocked || loading}
            onClick={() => void run(() => fileReport(month), "Endgültig abgegeben")}
          >
            Endgültig abgeben
          </button>
        </div>
      </div>

      {error ? <div className="status status-error">{error}</div> : null}
      {status ? <div className="status status-ok">{status}</div> : null}
      {loading && !report ? <div className="status status-info">USt-Report wird geladen…</div> : null}

      <div className="kpi-grid">
        {kpiItems.map((item) => (
          <article className="card kpi" key={item.name}>
            <div className="kpi-name">{item.name}</div>
            <div className={cx("kpi-value", item.negative && "value-neg")}>{item.value}</div>
            <div className="kpi-sub">{item.sub}</div>
          </article>
        ))}
      </div>

      <div
        className="trend-granularity"
        role="tablist"
        aria-label="USt-Report Ansicht"
        style={{ marginTop: 12, justifyContent: "center", width: "100%" }}
      >
        {TABS.map((entry) => (
          <button
            key={entry.token}
            className={cx("segmented-btn", tab === entry.token && "active")}
            type="button"
            onClick={() => setTab(entry.token)}
          >
            {entry.label}
          </button>
        ))}
      </div>

      {tab === "uebersicht" && report ? (
        <>
          <section className="detail-grid" style={{ marginTop: 12 }}>
            <article className="detail-card">
              <h3>Ergebnis</h3>
              <div className="detail-kv">
                <DetailRows
                  items={[
                    ["Zahllast", formatMoneyFromCents(report.totals.vat_payable_cents)],
                    ["Ausgangs-USt", formatMoneyFromCents(report.totals.output_vat_cents)],
                    ["Vorsteuer", formatMoneyFromCents(report.totals.input_vat_cents)],
                    ["Berichtsmonat", monthLabel(month)],
                  ]}
                />
              </div>
            </article>
            <article className="detail-card">
              <h3>Kaufland</h3>
              <div className="detail-kv">
                <DetailRows
                  items={[
                    ["Umsatz nach Retouren", formatMoneyFromCents(kaufland?.revenue_after_returns_cents || 0)],
                    ["Netto", formatMoneyFromCents(kaufland?.net_cents || 0)],
                    ["USt (19 %)", formatMoneyFromCents(kaufland?.output_vat_cents || 0)],
                    [
                      "Retouren",
                      kaufland?.returns.count
                        ? String(kaufland.returns.count)
                        : kaufland?.returns_synced
                          ? "Keine"
                          : "Unbekannt",
                    ],
                    ["Positionen ohne Steuersatz", String(pendingOverrides)],
                  ]}
                />
              </div>
            </article>
            <article className="detail-card">
              <h3>Amazon</h3>
              <div className="detail-kv">
                <DetailRows
                  items={AMAZON_ORDER.filter((key) => amazon[key]).map((key) => [
                    classLabel(key),
                    `${amazon[key].count} · ${formatMoneyFromCents(amazon[key].net)}`,
                  ])}
                />
              </div>
            </article>
            <article className="detail-card">
              <h3>Vorsteuer</h3>
              <div className="detail-kv">
                <DetailRows
                  items={INPUT_VAT_BUCKETS.map(([key, label]) => [
                    label,
                    formatMoneyFromCents(Number((inputVat as Record<string, number> | undefined)?.[key] || 0)),
                  ])}
                />
              </div>
            </article>
          </section>

          <section className="card table-card" style={{ marginTop: 12 }}>
            <div className="table-head">
              <h2 className="table-title">Was noch offen ist</h2>
              <div className="table-meta">{openCount ? `${openCount} Punkt(e)` : "Alles geklärt"}</div>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Punkt</th>
                    <th>Hinweis</th>
                    <th>Aktion</th>
                  </tr>
                </thead>
                <tbody>
                  {[...(report.blockers || []), ...(report.warnings || [])].map((item) => {
                    const info = issueLabel(item.code);
                    const count = Number(item.count || 0);
                    return (
                      <tr key={`${item.code}-${String(item.count || 0)}`}>
                        <td>
                          <span className={info.kind === "blocker" ? "text-danger" : "text-warn"}>
                            {info.label}
                          </span>
                          {count ? <small className="table-meta"> · {count}</small> : null}
                        </td>
                        <td>{item.hint || info.hint}</td>
                        <td>
                          {item.code === "KAUFLAND_RATE_NEEDS_OVERRIDE" ? (
                            <button
                              className="btn-inline primary"
                              type="button"
                              onClick={() =>
                                void run(
                                  () =>
                                    bulkOverrideZeroRates({
                                      month,
                                      reason: "Falschfeld – alle 0-%-Positionen auf 19 % gesetzt",
                                      to_rate: 19,
                                    }),
                                  "Alle Positionen auf 19 % gesetzt",
                                )
                              }
                            >
                              Alle auf 19 % setzen
                            </button>
                          ) : item.code === "INPUT_VAT_PENDING_REVIEW" ? (
                            <button className="btn-inline ghost" type="button" onClick={() => setTab("belege")}>
                              Zu den Rechnungen
                            </button>
                          ) : (
                            <span className="table-meta">—</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                  {!openCount ? (
                    <tr>
                      <td colSpan={3}>Alles geklärt – der Bericht kann abgegeben werden.</td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </section>
        </>
      ) : null}

      {tab === "kaufland" && report ? (
        <section className="card table-card" style={{ marginTop: 12 }}>
          <div className="table-head">
            <h2 className="table-title">Kaufland-Verkäufe</h2>
            <div className="table-meta">{kaufland?.rows.length || 0} Positionen</div>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Position</th>
                  <th>Datum</th>
                  <th>Land</th>
                  <th>Umsatz</th>
                  <th>Netto</th>
                  <th>USt</th>
                  <th>Behandlung</th>
                </tr>
              </thead>
              <tbody>
                {(kaufland?.rows || []).map((row) => (
                  <tr key={String(row.id_order_unit)}>
                    <td>{String(row.id_order_unit)}</td>
                    <td>{String(row.booking_date)}</td>
                    <td>{String(row.shipping_country || "—")}</td>
                    <td>{formatMoneyFromCents(Number(row.gross_cents || 0))}</td>
                    <td>{formatMoneyFromCents(Number(row.net_cents || 0))}</td>
                    <td>{formatMoneyFromCents(Number(row.output_vat_cents || 0))}</td>
                    <td>{classLabel(String(row.tax_class))}</td>
                  </tr>
                ))}
                {!kaufland?.rows.length ? (
                  <tr>
                    <td colSpan={7}>Keine Kaufland-Positionen in diesem Monat.</td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {tab === "amazon" && report ? (
        <section className="card table-card" style={{ marginTop: 12 }}>
          <div className="table-head">
            <h2 className="table-title">Amazon-Umsätze</h2>
            <div className="table-meta">nach steuerlicher Behandlung</div>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Behandlung</th>
                  <th>Anzahl</th>
                  <th>Brutto</th>
                  <th>Netto</th>
                  <th>USt</th>
                </tr>
              </thead>
              <tbody>
                {AMAZON_ORDER.filter((key) => amazon[key]).map((key) => (
                  <tr key={key}>
                    <td>{classLabel(key)}</td>
                    <td>{amazon[key].count}</td>
                    <td>{formatMoneyFromCents(amazon[key].gross)}</td>
                    <td>{formatMoneyFromCents(amazon[key].net)}</td>
                    <td>{formatMoneyFromCents(amazon[key].output_vat)}</td>
                  </tr>
                ))}
                {!Object.keys(amazon).length ? (
                  <tr>
                    <td colSpan={5}>Keine Amazon-Transaktionen in diesem Monat.</td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {tab === "vorsteuer" && report ? (
        <>
          <section className="card table-card" style={{ marginTop: 12 }}>
            <div className="table-head">
              <h2 className="table-title">Vorsteuer im Berichtsmonat</h2>
              <div className="table-meta">
                {inputVat?.input_vat_incomplete
                  ? "Unterlagen noch unvollständig"
                  : "Vollständig"}
              </div>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Art</th>
                    <th>Betrag</th>
                  </tr>
                </thead>
                <tbody>
                  {INPUT_VAT_BUCKETS.map(([key, label]) => (
                    <tr key={key}>
                      <td>{label}</td>
                      <td>
                        {formatMoneyFromCents(Number((inputVat as Record<string, number> | undefined)?.[key] || 0))}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="detail-grid" style={{ marginTop: 12 }}>
            <article className="detail-card">
              <h3>EU-Verkaufsregel</h3>
              <div className="detail-kv">
                <div className="control" style={{ marginBottom: 8 }}>
                  <label htmlFor="taxReportRegime">Wie werden EU-Verkäufe besteuert?</label>
                  <select
                    id="taxReportRegime"
                    value={regime}
                    onChange={(event) => setRegime(event.target.value)}
                  >
                    {REGIME_OPTIONS.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </div>
                <button
                  className="btn-inline primary"
                  type="button"
                  onClick={() => void run(() => saveSettings({ eu_tax_regime: regime }), "Gespeichert")}
                >
                  Speichern
                </button>
              </div>
            </article>
          </section>
        </>
      ) : null}

      {tab === "belege" ? (
        <section className="card table-card" style={{ marginTop: 12 }}>
          <div className="table-head">
            <h2 className="table-title">Eingangsrechnungen</h2>
            <div className="orders-head-actions">
              <div className="table-meta">{documents.length} Zeilen</div>
              <button
                className="btn-inline primary"
                type="button"
                onClick={() => setShowForm((previous) => !previous)}
              >
                {showForm ? "Formular schliessen" : "Neue Eingangsrechnung"}
              </button>
            </div>
          </div>

          {showForm ? (
            <div className="bookings-tools open">
              <div className="bookings-form-grid">
                <div className="control">
                  <label htmlFor="docProvider">Anbieter</label>
                  <select id="docProvider" value={provider} onChange={(event) => setProvider(event.target.value)}>
                    <option value="kaufland">Kaufland</option>
                    <option value="amazon">Amazon</option>
                    <option value="other">Sonstiges</option>
                  </select>
                </div>
                <div className="control">
                  <label htmlFor="docType">Art</label>
                  <select id="docType" value={docType} onChange={(event) => setDocType(event.target.value)}>
                    <option value="fee">Gebühr</option>
                    <option value="purchase">Wareneinkauf</option>
                    <option value="damage_compensation">Nicht steuerbar</option>
                    <option value="other">Sonstiges</option>
                  </select>
                </div>
                <div className="control">
                  <label htmlFor="docNumber">Rechnungsnummer</label>
                  <input
                    id="docNumber"
                    value={invoiceNumber}
                    onChange={(event) => setInvoiceNumber(event.target.value)}
                    placeholder="z. B. R0226-23464200"
                  />
                </div>
                <div className="control">
                  <label htmlFor="docInvoiceDate">Rechnungsdatum</label>
                  <input
                    id="docInvoiceDate"
                    type="date"
                    value={invoiceDate}
                    onChange={(event) => setInvoiceDate(event.target.value)}
                  />
                </div>
                <div className="control">
                  <label htmlFor="docServiceDate">Leistungs-/Lieferdatum</label>
                  <input
                    id="docServiceDate"
                    type="date"
                    value={serviceDate}
                    onChange={(event) => setServiceDate(event.target.value)}
                  />
                </div>
                <div className="control">
                  <label htmlFor="docReceivedDate">Beleg verfügbar am</label>
                  <input
                    id="docReceivedDate"
                    type="date"
                    value={receivedDate}
                    onChange={(event) => setReceivedDate(event.target.value)}
                  />
                </div>
                <div className="control">
                  <label htmlFor="docGross">Brutto (EUR)</label>
                  <input
                    id="docGross"
                    inputMode="decimal"
                    placeholder="0,00"
                    value={grossValue}
                    onChange={(event) => setGrossValue(event.target.value)}
                  />
                </div>
                <div className="control">
                  <label htmlFor="docNet">Netto (EUR)</label>
                  <input
                    id="docNet"
                    inputMode="decimal"
                    placeholder="0,00"
                    value={netValue}
                    onChange={(event) => setNetValue(event.target.value)}
                  />
                </div>
                <div className="control">
                  <label htmlFor="docVat">Vorsteuer (EUR)</label>
                  <input
                    id="docVat"
                    inputMode="decimal"
                    placeholder="0,00"
                    value={vatValue}
                    onChange={(event) => setVatValue(event.target.value)}
                  />
                </div>
                <div className="control">
                  <label>Beleg</label>
                  <div className="sammel-file-field">
                    <label className="sammel-file-btn" htmlFor="docFile">
                      Datei waehlen
                    </label>
                    <input
                      id="docFile"
                      className="sammel-file-input"
                      type="file"
                      accept=".pdf,.png,.jpg,.jpeg,.webp"
                      onChange={(event) => {
                        const picked = event.target.files?.[0] || null;
                        setFile(picked);
                        setFileName(picked ? picked.name : "Optional");
                      }}
                    />
                    <div className="sammel-file-name">{fileName}</div>
                  </div>
                </div>
              </div>
              <div className="bookings-form-actions">
                <button
                  className="btn-inline primary"
                  type="button"
                  disabled={!invoiceNumber || !invoiceDate}
                  onClick={() => {
                    const form = new FormData();
                    form.set("provider", provider);
                    form.set("doc_type", docType);
                    form.set("invoice_number", invoiceNumber);
                    form.set("invoice_date", invoiceDate);
                    form.set("received_date", receivedDate);
                    form.set("service_date", serviceDate);
                    form.set("gross_cents", String(toCents(grossValue)));
                    form.set("net_cents", String(toCents(netValue) || toCents(grossValue)));
                    form.set("vat_cents", String(toCents(vatValue)));
                    form.set("deductible_vat_cents", String(toCents(vatValue)));
                    if (file) {
                      form.set("file", file);
                    }
                    void run(() => uploadDocument(form), "Eingangsrechnung gespeichert").then(resetForm);
                  }}
                >
                  Rechnung speichern
                </button>
              </div>
            </div>
          ) : null}

          <div className="table-wrap" style={{ marginTop: showForm ? 12 : 0 }}>
            <table>
              <thead>
                <tr>
                  <th>Rechnung</th>
                  <th>Anbieter</th>
                  <th>Art</th>
                  <th>Leistung</th>
                  <th>Abzug</th>
                  <th>Netto</th>
                  <th>Vorsteuer</th>
                  <th>Status</th>
                  <th>Aktion</th>
                </tr>
              </thead>
              <tbody>
                {documents.map((document) => (
                  <tr key={document.id}>
                    <td>{document.invoice_number}</td>
                    <td>{document.provider}</td>
                    <td>
                      {document.doc_type === "fee"
                        ? "Gebühr"
                        : document.doc_type === "purchase"
                          ? "Wareneinkauf"
                          : document.doc_type === "damage_compensation"
                            ? "Nicht steuerbar"
                            : "Sonstiges"}
                    </td>
                    <td>{document.service_date || document.period_to || document.invoice_date}</td>
                    <td>{monthLabel(document.deduction_month)}</td>
                    <td>{formatMoneyFromCents(document.net_cents)}</td>
                    <td>{formatMoneyFromCents(document.deductible_vat_cents)}</td>
                    <td>
                      <span
                        className={cx(
                          "badge",
                          document.input_vat_status === "confirmed"
                            ? "badge-sale"
                            : document.input_vat_status === "rejected"
                              ? "badge-refund"
                              : "badge-default",
                        )}
                      >
                        {document.input_vat_status === "confirmed"
                          ? "Freigegeben"
                          : document.input_vat_status === "rejected"
                            ? "Abgelehnt"
                            : document.input_vat_status === "non_deductible"
                              ? "Ohne Vorsteuer"
                              : "Zu prüfen"}
                      </span>
                    </td>
                    <td>
                      <span className="doc-actions">
                        {document.input_vat_status !== "confirmed" && document.doc_type !== "damage_compensation" ? (
                          <button
                            className="btn-inline primary"
                            type="button"
                            onClick={() =>
                              void run(
                                () => patchDocument(document.id, { input_vat_status: "confirmed" }),
                                "Freigegeben",
                              )
                            }
                          >
                            Freigeben
                          </button>
                        ) : (
                          <span className="table-meta">—</span>
                        )}
                      </span>
                    </td>
                  </tr>
                ))}
                {!documents.length ? (
                  <tr>
                    <td colSpan={9}>Keine Eingangsrechnungen im Abzugsmonat.</td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}
    </section>
  );
}
