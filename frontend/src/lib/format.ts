const numberFormatter = new Intl.NumberFormat("de-DE");
const currencyFormatter = new Intl.NumberFormat("de-DE", {
  style: "currency",
  currency: "EUR",
});
const dateFormatter = new Intl.DateTimeFormat("de-DE");

export function formatNumber(value?: number) {
  return numberFormatter.format(Number(value ?? 0));
}

export function formatCurrencyFromCents(value?: number) {
  return currencyFormatter.format(Number(value ?? 0) / 100);
}

export function formatPercent(value?: number, fractionDigits = 2) {
  return `${Number(value ?? 0).toLocaleString("de-DE", {
    minimumFractionDigits: fractionDigits,
    maximumFractionDigits: fractionDigits,
  })}%`;
}

export function formatDate(value?: string) {
  if (!value) {
    return "-";
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "-";
  }

  return dateFormatter.format(parsed);
}
