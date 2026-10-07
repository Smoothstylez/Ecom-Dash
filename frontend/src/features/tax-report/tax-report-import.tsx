import { useEffect, useState } from "react";
import { fetchImportHistory, importReportFiles, type ImportItem } from "./api";
import { monthLabel, reviewReasonLabel } from "./labels";

const STATUS: Record<string, string> = {
  approved: "Automatisch übernommen", tax_imported: "Steuerreport zugeordnet",
  paired: "Zur Rechnung zugeordnet", duplicate: "Bereits berücksichtigt",
  paired_review: "Zugeordnet – Rechnung zu prüfen",
  waiting_pdf: "Original-PDF fehlt noch", needs_review: "Zu prüfen",
  conflict: "Abweichende Rechnung", error: "Nicht übernommen",
  evidence_only: "Umsatzbeleg erkannt", queued: "In Verarbeitung",
};

export function TaxReportImport({ onComplete }: { onComplete: (items: ImportItem[]) => void }) {
  const [files, setFiles] = useState<File[]>([]);
  const [items, setItems] = useState<ImportItem[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  useEffect(() => {
    let active = true;
    void fetchImportHistory().then(r => { if (active) setItems(r.items); }).catch(reason => {
      if (active) setError(reason instanceof Error ? reason.message : "Importverlauf nicht verfügbar");
    });
    return () => { active = false; };
  }, []);
  const start = async () => {
    setBusy(true);
    setError("");
    try {
      const result = await importReportFiles(files);
      setItems(result.items);
      setFiles([]);
      const input = document.getElementById("ustImportFiles") as HTMLInputElement | null;
      if (input) input.value = "";
      onComplete(result.items);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Import fehlgeschlagen");
    } finally { setBusy(false); }
  };
  return <section className="card table-card" aria-label="Reports und Belege importieren" style={{ marginTop: 12 }}>
    <div className="table-head">
      <div><h2 className="table-title">Reports und Belege importieren</h2>
        <div className="table-meta">PDF und CSV gemeinsam oder nacheinander hochladen. Anbieter, Rechnung, Zeitraum und Beträge werden automatisch zugeordnet.</div>
      </div>
    </div>
    <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
      <div className="sammel-file-field">
        <label className="sammel-file-btn" htmlFor="ustImportFiles">Dateien auswählen</label>
        <input id="ustImportFiles" className="sammel-file-input" type="file" accept=".pdf,.csv,.tsv,.txt" multiple disabled={busy}
          onChange={e => setFiles(Array.from(e.target.files || []))} />
        <div className="sammel-file-name">{files.length ? files.map(f => f.name).join(", ") : "Amazon-Steuerreports und Plattformrechnungen"}</div>
      </div>
      <button className="btn-inline primary" type="button" disabled={busy || !files.length} onClick={() => void start()}>
        {busy ? "Wird erkannt und zugeordnet…" : "Automatisch importieren"}
      </button>
    </div>
    <div className="table-meta" style={{ marginTop: 8 }}>Eindeutige Belege werden übernommen. Unklare Fälle bleiben prüfpflichtig; wiederholte Uploads werden nicht doppelt gebucht. Der Importverlauf zeigt alle Monate; der Berichtsmonat stammt aus dem Beleg.</div>
    {error ? <div className="status status-error" role="alert">{error}</div> : null}
    {items.length ? <div className="table-wrap" style={{ marginTop: 12 }}>
      <table><thead><tr><th>Datei</th><th>Zuordnung</th><th>Berichtsmonat</th><th>Ergebnis</th></tr></thead>
        <tbody>{items.map((item, index) => <tr key={`${item.id}-${index}`}>
          <td>{item.filename}</td>
          <td>{item.invoice_number || item.invoice_numbers?.join(", ") || (item.kind === "amazon_tax" ? `${item.total || 0} Steuertransaktionen` : "—")}
            {item.pairings?.map(pair => <div key={pair.invoice_number} className="table-meta">Original-PDF: {pair.pdf_filename || pair.invoice_number}</div>)}
          </td>
          <td>{item.deduction_month ? monthLabel(item.deduction_month) : item.months?.map(monthLabel).join(", ") || "—"}</td>
          <td><span className={`badge ${["approved", "tax_imported", "paired", "duplicate"].includes(item.status) ? "badge-sale" : "badge-default"}`}>{STATUS[item.status] || "Zu prüfen"}</span>
            {item.reasons?.length ? <div className="table-meta">{item.reasons.map(reviewReasonLabel).join(" ")}</div> : null}</td>
        </tr>)}</tbody>
      </table>
    </div> : null}
  </section>;
}
