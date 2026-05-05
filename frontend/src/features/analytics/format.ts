export const MONEY_FORMATTER = new Intl.NumberFormat("de-DE", {
  style: "currency",
  currency: "EUR",
});

export const NUMBER_FORMATTER = new Intl.NumberFormat("de-DE");

export const DATE_FORMATTER = new Intl.DateTimeFormat("de-DE");

export function formatMoneyFromCents(cents: number) {
  return MONEY_FORMATTER.format(Number(cents || 0) / 100);
}

export function formatPercent(value: number, digits = 2) {
  const numeric = Number(value || 0);
  return `${numeric.toLocaleString("de-DE", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })}%`;
}

export function formatDateToken(value: string) {
  const parsed = parseDateToken(value);
  return parsed ? DATE_FORMATTER.format(parsed) : "-";
}

export function parseDateToken(value: string) {
  const token = String(value || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(token)) {
    return null;
  }

  const parsed = new Date(`${token}T00:00:00`);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

export function dateTokenFromDate(value: Date) {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, "0");
  const day = String(value.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

export function addDays(baseDate: Date, deltaDays: number) {
  const copy = new Date(baseDate);
  copy.setDate(copy.getDate() + deltaDays);
  return copy;
}

export function datePresetLabel(token: string) {
  if (token === "today") return "Heute";
  if (token === "yesterday") return "Gestern";
  if (token === "last_7_days") return "Letzte 7 Tage";
  if (token === "last_30_days") return "Letzte 30 Tage";
  if (token === "last_90_days") return "Letzte 90 Tage";
  if (token === "this_month") return "Dieser Monat";
  if (token === "last_month") return "Letzter Monat";
  if (token === "this_year") return "Dieses Jahr";
  if (token === "all_time") return "Alle Zeit";
  return "Zeitraum";
}

export function applyDatePreset(preset: string) {
  const token = String(preset || "last_30_days").trim();
  const today = new Date();
  today.setHours(0, 0, 0, 0);

  let from = "";
  let to = dateTokenFromDate(today);

  if (token === "today") {
    from = to;
  } else if (token === "yesterday") {
    from = dateTokenFromDate(addDays(today, -1));
    to = from;
  } else if (token === "last_7_days") {
    from = dateTokenFromDate(addDays(today, -6));
  } else if (token === "last_30_days") {
    from = dateTokenFromDate(addDays(today, -29));
  } else if (token === "last_90_days") {
    from = dateTokenFromDate(addDays(today, -89));
  } else if (token === "this_month") {
    from = dateTokenFromDate(new Date(today.getFullYear(), today.getMonth(), 1));
  } else if (token === "last_month") {
    const firstCurrentMonth = new Date(today.getFullYear(), today.getMonth(), 1);
    const lastPrevMonth = addDays(firstCurrentMonth, -1);
    from = dateTokenFromDate(new Date(lastPrevMonth.getFullYear(), lastPrevMonth.getMonth(), 1));
    to = dateTokenFromDate(lastPrevMonth);
  } else if (token === "this_year") {
    from = dateTokenFromDate(new Date(today.getFullYear(), 0, 1));
  } else if (token === "all_time") {
    from = "1970-01-01";
  } else {
    from = dateTokenFromDate(addDays(today, -29));
  }

  return { preset: token, from, to };
}

export function normalizeTrendGranularity(value: string) {
  const token = String(value || "").trim().toLowerCase();
  if (token === "day" || token === "daily") {
    return "day";
  }
  if (token === "week" || token === "weekly" || token === "woche") {
    return "week";
  }
  if (token === "month" || token === "monthly" || token === "monat") {
    return "month";
  }
  if (token === "year" || token === "yearly" || token === "jahr") {
    return "year";
  }
  return "auto";
}

export function trendGranularityLabel(value: string) {
  const token = normalizeTrendGranularity(value);
  if (token === "day") {
    return "Tag";
  }
  if (token === "week") {
    return "Woche";
  }
  if (token === "month") {
    return "Monat";
  }
  if (token === "year") {
    return "Jahr";
  }
  return "Auto";
}

export function isoWeekNumber(dateValue: Date) {
  const target = new Date(dateValue.getTime());
  target.setHours(0, 0, 0, 0);
  target.setDate(target.getDate() + 3 - ((target.getDay() + 6) % 7));
  const firstThursday = new Date(target.getFullYear(), 0, 4);
  firstThursday.setHours(0, 0, 0, 0);
  firstThursday.setDate(firstThursday.getDate() + 3 - ((firstThursday.getDay() + 6) % 7));
  return 1 + Math.round((target.getTime() - firstThursday.getTime()) / 604800000);
}
