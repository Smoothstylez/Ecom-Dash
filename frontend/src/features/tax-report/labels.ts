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
};

export type IssueInfo = { label: string; hint: string; kind: "blocker" | "warning" };

export const ISSUE_LABELS: Record<string, IssueInfo> = {
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
    hint: "Amazon hat für EU-Verkäufe keine Steuer ausgewiesen, wir setzen deutsche 19 % an.",
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
