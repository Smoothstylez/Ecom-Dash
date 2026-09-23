import { useCallback, useEffect, useState } from "react";

import {
  amendReport,
  bulkOverrideZeroRates,
  euro,
  fetchDocuments,
  fetchUstReport,
  fileReport,
  patchDocument,
  refreshReport,
  saveKauflandOverride,
  saveSettings,
  uploadDocument,
  type UstDocument,
  type UstReport,
} from "./api";

const EU_REGIME_OPTIONS = [
  { value: "unconfirmed", label: "Unbestätigt" },
  { value: "home_rate_under_threshold", label: "Heimsteuersatz unter Fernabsatzschwelle" },
  { value: "oss_destination", label: "OSS / Bestimmungslandoption" },
];

function previousMonthToken(): string {
  const now = new Date();
  now.setDate(1);
  now.setMonth(now.getMonth() - 1);
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
}

export function TaxReportPage() {
  const [month, setMonth] = useState(previousMonthToken());
  const [report, setReport] = useState<UstReport | null>(null);
  const [documents, setDocuments] = useState<UstDocument[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [status, setStatus] = useState("");
  const [overrideUnit, setOverrideUnit] = useState("");
  const [overrideReason, setOverrideReason] = useState("");
  const [euRegime, setEuRegime] = useState("unconfirmed");
  const [receivedDate, setReceivedDate] = useState("");
  const [invoiceNumber, setInvoiceNumber] = useState("");
  const [invoiceDate, setInvoiceDate] = useState("");
  const [serviceDate, setServiceDate] = useState("");
  const [grossCents, setGrossCents] = useState("");
  const [netCents, setNetCents] = useState("");
  const [vatCents, setVatCents] = useState("");
  const [provider, setProvider] = useState("kaufland");
  const [docType, setDocType] = useState("fee");
  const [file, setFile] = useState<File | null>(null);

  const reload = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const payload = await fetchUstReport(month);
      setReport(payload);
      const regime = String((payload.settings as Record<string, unknown>).eu_tax_regime || "unconfirmed");
      setEuRegime(regime);
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

  const pendingRows = (report?.sections.kaufland.rows || []).filter(
    (row) => row.tax_class === "kaufland_rate_needs_override",
  );
  const filed = report?.status === "filed";

  return (
    <div className="page" data-testid="tax-report-page">
      <div className="table-meta">
        Monatlicher USt-Report: Kaufland- und Amazon-Umsätze nach Retouren, Ausgangs-USt,
        innergemeinschaftliche Lieferungen und Vorsteuer aus Einkauf und Gebührenrechnungen.
      </div>

      <div className="table-header">
        <h3 className="table-title">USt-Report</h3>
        <div className="table-meta">
          <input
            className="settings-inline-input"
            type="month"
            aria-label="Berichtsmonat"
            value={month}
            onChange={(event) => setMonth(event.target.value)}
          />
          {report ? <span data-testid="tax-report-status">{report.status}</span> : null}
          {report?.revision ? (
            <span>
              Revision {report.revision} ({report.kind})
            </span>
          ) : null}
          <button type="button" onClick={() => void reload()}>
            Laden
          </button>
          <button type="button" disabled={filed} onClick={() => void run(() => refreshReport(month), "Neu berechnet")}>
            Neu berechnen
          </button>
          <button
            type="button"
            disabled={filed || Boolean(report?.blockers.length)}
            data-testid="file-report-btn"
            onClick={() => void run(() => fileReport(month), "Endgültig abgegeben")}
          >
            Endgültig festsetzen
          </button>
          <button type="button" disabled={!filed} onClick={() => void run(() => amendReport(month), "Berichtigung angelegt")}>
            Berichtigung anlegen
          </button>
        </div>
      </div>

      {loading ? <div className="table-meta">USt-Report wird geladen…</div> : null}
      {error ? <div className="table-meta" data-testid="tax-report-error">{error}</div> : null}
      {status ? <div className="table-meta">{status}</div> : null}

      {report?.blockers.length ? (
        <div className="table-meta" data-testid="tax-report-blockers">
          <strong>Abgabe gesperrt:</strong>
          <ul>
            {report.blockers.map((item) => (
              <li key={item.code}>
                {item.code}
                {item.count ? ` (${item.count})` : ""} — {item.hint}
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      {report ? (
        <>
          <div className="table-meta" data-testid="tax-report-totals">
            <div>Ausgangs-USt: {euro(report.totals.output_vat_cents)}</div>
            <div>Vorsteuer: {euro(report.totals.input_vat_cents)}</div>
            <div>
              <strong>Zahllast: {euro(report.totals.vat_payable_cents)}</strong>
            </div>
          </div>

          <h4 className="table-title">Kaufland</h4>
          <div className="table-meta">
            <div>Umsatz nach Retouren: {euro(report.sections.kaufland.revenue_after_returns_cents)}</div>
            <div>Netto: {euro(report.sections.kaufland.net_cents)}</div>
            <div>Ausgangs-USt: {euro(report.sections.kaufland.output_vat_cents)}</div>
            <div>Retouren (Buchungsmonat): {report.sections.kaufland.returns.count}</div>
            <div data-testid="kaufland-pending-overrides">
              Offene Steuersatzkorrekturen: {report.sections.kaufland.rate_overrides_pending}
            </div>
          </div>

          <h4 className="table-title">Amazon</h4>
          <div className="table-meta">
            {Object.entries(report.sections.amazon).map(([key, bucket]) => (
              <div key={key}>
                {key}: {bucket.count} · Brutto {euro(bucket.gross)} · Netto {euro(bucket.net)} · USt{" "}
                {euro(bucket.output_vat)}
              </div>
            ))}
          </div>

          <h4 className="table-title">Vorsteuer</h4>
          <div className="table-meta">
            <div>Wareneinkauf: {euro(report.sections.input_vat.purchases_cents)}</div>
            <div>Amazon-Gebühren: {euro(report.sections.input_vat.amazon_fees_cents)}</div>
            <div>Kaufland-Gebühren: {euro(report.sections.input_vat.kaufland_fees_cents)}</div>
            <div>Sonstiges: {euro(report.sections.input_vat.other_cents)}</div>
            <div>Nicht steuerbar: {euro(report.sections.input_vat.nontaxable_cents)}</div>
            <div data-testid="input-vat-incomplete">
              Unvollständig: {report.sections.input_vat.input_vat_incomplete ? "ja" : "nein"}
            </div>
          </div>

          {report.warnings.length ? (
            <div className="table-meta" data-testid="tax-report-warnings">
              <strong>Hinweise:</strong>
              <ul>
                {report.warnings.map((item) => (
                  <li key={item.code}>
                    {item.code}
                    {item.count ? ` (${item.count})` : ""} — {item.hint}
                  </li>
                ))}
              </ul>
            </div>
          ) : null}
        </>
      ) : null}

      <h4 className="table-title">Steuerliche Konfiguration</h4>
      <div className="table-meta">
        <label>
          EU-Regime{" "}
          <select value={euRegime} onChange={(event) => setEuRegime(event.target.value)}>
            {EU_REGIME_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>
        <button type="button" onClick={() => void run(() => saveSettings({ eu_tax_regime: euRegime }), "Gespeichert")}>
          Speichern
        </button>
      </div>

      <h4 className="table-title">Kaufland-Steuersatz korrigieren</h4>
      <div className="table-meta">
        <input
          className="settings-inline-input"
          placeholder="id_order_unit"
          aria-label="Order-Unit"
          value={overrideUnit}
          onChange={(event) => setOverrideUnit(event.target.value)}
        />
        <input
          className="settings-inline-input"
          placeholder="Begründung"
          aria-label="Begründung"
          value={overrideReason}
          onChange={(event) => setOverrideReason(event.target.value)}
        />
        <button
          type="button"
          disabled={!overrideUnit || !overrideReason}
          onClick={() =>
            void run(
              () => saveKauflandOverride({ id_order_unit: overrideUnit, to_rate: 19, reason: overrideReason }),
              "Korrektur gespeichert",
            )
          }
        >
          Auf 19 % setzen
        </button>
        <button
          type="button"
          disabled={!overrideReason}
          onClick={() => void run(() => bulkOverrideZeroRates({ month, reason: overrideReason }), "Alle 0-%-Felder korrigiert")}
        >
          Alle 0-%-Felder in {month}
        </button>
        {pendingRows.length ? (
          <div>
            Betroffen: {pendingRows.map((row) => String(row.id_order_unit)).join(", ")}
          </div>
        ) : null}
      </div>

      <h4 className="table-title">Eingangsrechnungen</h4>
      <div className="table-meta">
        <select value={provider} onChange={(event) => setProvider(event.target.value)}>
          <option value="kaufland">Kaufland</option>
          <option value="amazon">Amazon</option>
          <option value="other">Sonstiges</option>
        </select>
        <select value={docType} onChange={(event) => setDocType(event.target.value)}>
          <option value="fee">Gebühr</option>
          <option value="purchase">Wareneinkauf</option>
          <option value="damage_compensation">Nicht steuerbarer Schadensersatz</option>
          <option value="other">Sonstiges</option>
        </select>
        <input
          className="settings-inline-input"
          placeholder="Rechnungsnummer"
          aria-label="Rechnungsnummer"
          value={invoiceNumber}
          onChange={(event) => setInvoiceNumber(event.target.value)}
        />
        <input
          className="settings-inline-input"
          type="date"
          aria-label="Rechnungsdatum"
          value={invoiceDate}
          onChange={(event) => setInvoiceDate(event.target.value)}
        />
        <input
          className="settings-inline-input"
          type="date"
          aria-label="Leistungs-/Lieferdatum"
          value={serviceDate}
          onChange={(event) => setServiceDate(event.target.value)}
        />
        <input
          className="settings-inline-input"
          type="date"
          aria-label="Beleg verfügbar am"
          value={receivedDate}
          onChange={(event) => setReceivedDate(event.target.value)}
        />
        <input
          className="settings-inline-input"
          type="number"
          placeholder="Brutto (Cent)"
          aria-label="Brutto in Cent"
          value={grossCents}
          onChange={(event) => setGrossCents(event.target.value)}
        />
        <input
          className="settings-inline-input"
          type="number"
          placeholder="Netto (Cent)"
          aria-label="Netto in Cent"
          value={netCents}
          onChange={(event) => setNetCents(event.target.value)}
        />
        <input
          className="settings-inline-input"
          type="number"
          placeholder="USt (Cent)"
          aria-label="Umsatzsteuer in Cent"
          value={vatCents}
          onChange={(event) => setVatCents(event.target.value)}
        />
        <input
          type="file"
          aria-label="Beleg"
          onChange={(event) => setFile(event.target.files?.[0] ?? null)}
        />
        <button
          type="button"
          data-testid="upload-document-btn"
          disabled={!file || !invoiceNumber || !invoiceDate}
          onClick={() => {
            const form = new FormData();
            form.set("file", file as File);
            form.set("provider", provider);
            form.set("doc_type", docType);
            form.set("invoice_number", invoiceNumber);
            form.set("invoice_date", invoiceDate);
            form.set("received_date", receivedDate);
            form.set("service_date", serviceDate);
            form.set("gross_cents", grossCents || "0");
            form.set("net_cents", netCents || grossCents || "0");
            form.set("vat_cents", vatCents || "0");
            form.set("deductible_vat_cents", vatCents || "0");
            void run(() => uploadDocument(form), "Beleg gespeichert");
          }}
        >
          Beleg hochladen
        </button>
      </div>

      <table>
        <thead>
          <tr>
            <th>Rechnung</th>
            <th>Anbieter</th>
            <th>Art</th>
            <th>Leistungsdatum</th>
            <th>Beleg verfügbar</th>
            <th>Abzugsmonat</th>
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
              <td>{document.doc_type}</td>
              <td>{document.service_date || document.period_to || document.invoice_date}</td>
              <td>{document.received_date}</td>
              <td data-testid="document-deduction-month">{document.deduction_month}</td>
              <td>{euro(document.net_cents)}</td>
              <td>{euro(document.deductible_vat_cents)}</td>
              <td data-testid="document-status">{document.input_vat_status}</td>
              <td>
                <button
                  type="button"
                  disabled={document.input_vat_status === "confirmed"}
                  onClick={() =>
                    void run(
                      () => patchDocument(document.id, { input_vat_status: "confirmed" }),
                      "Freigegeben",
                    )
                  }
                >
                  Freigeben
                </button>
              </td>
            </tr>
          ))}
          {!documents.length ? (
            <tr>
              <td colSpan={10}>Keine Eingangsrechnungen im Abzugsmonat.</td>
            </tr>
          ) : null}
        </tbody>
      </table>
    </div>
  );
}
