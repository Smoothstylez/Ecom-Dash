/** Verstaendliche Bezeichnungen fuer die internen Steuerklassen und Hinweise. */

export const CLASS_LABELS: Record<string, string> = {
  de_b2c: "Deutschland-Verkäufe (19 %)",
  eu_b2b_intra_community_supply: "Innergemeinschaftliche Lieferungen (0 %)",
  eu_b2c_home_rate: "EU-Verkäufe mit deutscher USt",
  unresolved: "Zu prüfen",
  deemed_supplier: "Amazon als Steuerschuldner",
  export: "Ausfuhr außerhalb EU",
  returns: "Retouren & Erstattungen",
  unresolved_return_link: "Retoure ohne Ursprungsbeleg",
  pre_vat: "Vor USt-Pflicht",
  kaufland_rate_needs_override: "Steuersatz fehlt",
  unresolved_refund_date: "Erstattung ohne Buchungsdatum",
};

export type IssueInfo = { label: string; hint: string; kind: "blocker" | "warning" };

export const ISSUE_LABELS: Record<string, IssueInfo> = {
  AMAZON_TAX_DATA_INCOMPLETE: {
    label: "Finanzdaten und Amazon-Steuerreport noch nicht abgestimmt",
    hint: "Der Kontrollabgleich hat abweichende Verkaufs- oder Erstattungsbeträge gefunden. Originalreports bleiben Grundlage der Steuerberechnung.",
    kind: "warning",
  },
  AMAZON_SOURCE_UNAVAILABLE: {
    label: "Amazon-Datenabgleich fehlgeschlagen",
    hint: "Die Vollständigkeit der Amazon-Quelle konnte nicht geprüft werden.",
    kind: "warning",
  },
  FINANCE_RECONCILIATION_INCOMPLETE: {
    label: "Finanzabgleich unvollständig",
    hint: "Die Finanzquelle oder ihre Zuordnung ist noch unvollständig. Das verhindert die Verarbeitung gültiger Originalreports nicht.",
    kind: "warning",
  },
  AMAZON_VAT_START_AMBIGUOUS: {
    label: "Amazon-USt-Abgrenzung ungeklärt",
    hint: "Der ursprüngliche Bestellzeitpunkt fehlt oder ist am Starttag nicht genau genug.",
    kind: "blocker",
  },
  KAUFLAND_REFUND_DATE_MISSING: {
    label: "Kaufland-Erstattung ohne Buchungsdatum",
    hint: "Ohne belegtes Erstattungsdatum lässt sich der richtige Berichtsmonat nicht bestimmen.",
    kind: "blocker",
  },
  INPUT_VAT_SOURCE_UNAVAILABLE: {
    label: "Vorsteuer-Datenabgleich fehlgeschlagen",
    hint: "Die gebuchte Vorsteuer konnte nicht vollständig gelesen werden.",
    kind: "blocker",
  },
  AMAZON_TRANSACTION_DATE_MISSING: {
    label: "Amazon-Transaktion ohne Buchungsdatum",
    hint: "Die Transaktion wird nicht auf Verdacht einem Berichtsmonat zugerechnet.",
    kind: "blocker",
  },
  INPUT_VAT_INVOICE_CONFLICT: {
    label: "Eingangsrechnung widersprüchlich erfasst",
    hint: "Freigabe, Vorsteuerbetrag oder Abzugsmonat stimmen zwischen den Belegbereichen nicht überein.",
    kind: "blocker",
  },
  INPUT_VAT_ELIGIBILITY_UNRESOLVED: {
    label: "Vorsteuer-Abgrenzung zum USt-Beginn ungeklärt",
    hint: "Bei Gebühren oder Gutschriften fehlt die eindeutige Zuordnung zum ursprünglichen Geschäft.",
    kind: "blocker",
  },
  INPUT_VAT_START_ADJUSTMENT: {
    label: "Vorsteuer an USt-Beginn angepasst",
    hint: "Gebühren vor der USt-Pflicht sind nicht abziehbar. Gutschriften übernehmen die Behandlung des ursprünglichen Geschäfts.",
    kind: "warning",
  },
  FEE_RECONCILIATION_DIFFERENCE: {
    label: "Gebührenabgleich noch offen",
    hint: "Die vorhandenen Rechnungen und die Finanzdaten haben abweichende Summen oder Zeiträume. Daraus werden keine zusätzlichen Vorsteuerbeträge geschätzt.",
    kind: "warning",
  },
  FEE_SOURCE_ESTIMATE_DIFFERENCE: {
    label: "Gebührenschätzung weicht von Monatsrechnung ab",
    hint: "Die vollständige Kaufland-Monatsrechnung ist vorhanden. Für die Vorsteuer gelten ihre belegten Positionen; die Bestelldaten sind nur eine Schätzung.",
    kind: "warning",
  },
  AMAZON_UNRESOLVED: {
    label: "Amazon-Fälle zu prüfen",
    hint: "Die Steuerbehandlung ist nicht eindeutig und muss bestätigt werden.",
    kind: "blocker",
  },
  KAUFLAND_RATE_NEEDS_OVERRIDE: {
    label: "Kaufland-Positionen ohne Steuersatz",
    hint: "Falsches Feld – verlangt sind 19 %. Einmal korrigieren genügt.",
    kind: "blocker",
  },
  INPUT_VAT_PENDING_REVIEW: {
    label: "Eingangsrechnungen freigeben",
    hint: "Vorsteuer ist erst nach Freigabe abziehbar.",
    kind: "blocker",
  },
  TAX_MODE_NOT_REGULAR: {
    label: "USt-Einstellungen unvollständig",
    hint: "Regelbesteuerung ist nicht aktiviert.",
    kind: "blocker",
  },
  NO_VAT_START_DATE: {
    label: "USt-Startdatum fehlt",
    hint: "Bitte unter USt-Einstellungen setzen.",
    kind: "blocker",
  },
  UNRESOLVED_RETURN_LINK: {
    label: "Retoure ohne Ursprungsbeleg",
    hint: "Der ursprüngliche Verkauf wurde nicht gefunden.",
    kind: "blocker",
  },
  MISSING_FEE_INVOICE: {
    label: "Gebührenrechnung fehlt noch",
    hint: "Vorsteuer erst, wenn die Rechnung vorliegt. Die Ausgangs-USt ist davon nicht betroffen.",
    kind: "warning",
  },
  AMAZON_VAT_CALCULATION_MISSING: {
    label: "Amazon-USt mussten wir selbst berechnen",
    hint: "Amazon hat für diese Verkäufe keine Steuer ausgewiesen; deutsche 19 % wurden aus dem Kundenbrutto berechnet.",
    kind: "warning",
  },
  KAUFLAND_RETURNS_NOT_SYNCED: {
    label: "Kaufland-Retouren konnten nicht abgerufen werden",
    hint: "Bitte den Kaufland-Sync ausführen. Solange ist die Zahl der Retouren unvollständig.",
    kind: "warning",
  },
};

export const INPUT_VAT_BUCKETS: Array<[string, string]> = [
  ["purchases_cents", "Wareneinkauf"],
  ["amazon_fees_cents", "Amazon-Gebühren"],
  ["kaufland_fees_cents", "Kaufland-Gebühren"],
  ["other_cents", "Sonstiges"],
  ["nontaxable_cents", "Nicht steuerbar"],
];

export const REGIME_OPTIONS: Array<{ value: string; label: string }> = [
  { value: "unconfirmed", label: "Noch nicht festgelegt" },
  { value: "home_rate_under_threshold", label: "EU-Verkäufe unter der Fernabsatzgrenze" },
  { value: "oss_destination", label: "Bestimmungsland / OSS" },
];

export function classLabel(key: string): string {
  return CLASS_LABELS[key] || key;
}

export function reviewReasonLabel(reason: string): string {
  if (reason.startsWith("unbekannte_position:")) {
    const detail = reason.slice("unbekannte_position:".length);
    const counted = detail.match(/^(.*):(\d+)$/);
    return `Noch nicht zugeordnet: ${counted ? `${counted[1]} (${counted[2]} Positionen)` : detail}`;
  }
  const labels: Record<string, string> = {
    kontrollsumme_netto_verfehlt: "Nettosumme der Positionen weicht von der Rechnung ab",
    kontrollsumme_brutto_verfehlt: "Bruttosumme der Positionen weicht von der Rechnung ab",
    keine_positionen_gefunden: "Keine eindeutigen Rechnungspositionen erkannt",
    rechnungsnummer_fehlt: "Rechnungsnummer fehlt",
    originalrechnung_fehlt: "Bezug zur ursprünglichen Rechnung fehlt",
    leistungszeitraum_fehlt: "Leistungszeitraum fehlt",
    kein_template_erkannt: "Rechnungsformat noch nicht unterstützt",
  };
  return labels[reason.split(":", 1)[0]] || reason;
}

export function issueLabel(code: string): IssueInfo {
  return (
    ISSUE_LABELS[code] || {
      label: code,
      hint: "",
      kind: "warning",
    }
  );
}

export function monthLabel(token: string): string {
  const months = [
    "Januar", "Februar", "März", "April", "Mai", "Juni",
    "Juli", "August", "September", "Oktober", "November", "Dezember",
  ];
  const [year, month] = String(token || "").split("-");
  const index = Number(month) - 1;
  if (!year || Number.isNaN(index) || index < 0 || index > 11) {
    return token;
  }
  return `${months[index]} ${year}`;
}

export const MONTH_NAMES = [
  "Januar", "Februar", "März", "April", "Mai", "Juni",
  "Juli", "August", "September", "Oktober", "November", "Dezember",
];

/** Betrag in EUR als Text (2 Nachkommastellen) -> Cent. */
export function toCents(input: string): number {
  const normalized = String(input || "").trim().replace(/\s/g, "").replace(",", ".");
  if (!normalized) {
    return 0;
  }
  const parsed = Number(normalized);
  if (Number.isNaN(parsed)) {
    return 0;
  }
  return Math.round(parsed * 100);
}
