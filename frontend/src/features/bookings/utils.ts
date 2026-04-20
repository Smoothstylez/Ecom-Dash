import type { BookingTransaction } from "@/features/bookings/types";

export const BOOKING_TX_CATEGORY_META = {
  sale: { label: "Sale" },
  fee: { label: "Fee" },
  cogs: { label: "Produkteinkauf" },
  invoice: { label: "Sonstige Rechnung" },
  subscription: { label: "Subscription" },
  refund: { label: "Refund" },
  other: { label: "Sonstiges" },
} as const;

const BOOKING_TX_TYPE_TO_CATEGORY = {
  SALE: "sale",
  FEE: "fee",
  COGS: "cogs",
  EXPENSE: "invoice",
  SUBSCRIPTION: "subscription",
  REFUND: "refund",
  PAYOUT: "other",
  ADJUSTMENT: "other",
} as const;

export type BookingTxCategoryKey = keyof typeof BOOKING_TX_CATEGORY_META;

export function bookingTransactionCategory(type?: string): BookingTxCategoryKey {
  const normalized = String(type ?? "").trim().toUpperCase() as keyof typeof BOOKING_TX_TYPE_TO_CATEGORY;
  return BOOKING_TX_TYPE_TO_CATEGORY[normalized] ?? "other";
}

export function filterBookingTransactions(
  items: BookingTransaction[],
  filters: { query: string; category: string; type: string },
) {
  const needle = String(filters.query || "").trim().toLowerCase();
  const activeCategory = String(filters.category || "").trim().toLowerCase();
  const activeType = String(filters.type || "").trim().toUpperCase();

  return items.filter((item) => {
    if (activeType && String(item.type ?? "").trim().toUpperCase() !== activeType) {
      return false;
    }

    if (activeCategory && bookingTransactionCategory(item.type) !== activeCategory) {
      return false;
    }

    if (!needle) {
      return true;
    }

    return [item.provider, item.counterparty_name, item.reference, item.notes, item.type]
      .some((value) => String(value || "").toLowerCase().includes(needle));
  });
}

export function parseEuroToCents(value: string) {
  const raw = String(value || "").trim();
  if (!raw) {
    return 0;
  }

  const normalized = raw.replace(/\s+/g, "").replace(/\./g, "").replace(",", ".");
  const parsed = Number(normalized);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 0;
  }
  return Math.round(parsed * 100);
}

export function centsToInputValue(value?: number | null) {
  const cents = Number(value ?? 0);
  if (!Number.isFinite(cents) || cents <= 0) {
    return "";
  }
  return (cents / 100).toFixed(2);
}

export function currentPeriodKey() {
  const now = new Date();
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
}

export function periodKeyFromDateLike(value?: string | null) {
  const token = String(value || "").trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(token)) {
    return token.slice(0, 7);
  }
  return "";
}

export function buildPeriodKeyRange(startPeriodKey: string, endPeriodKey: string) {
  const parse = (periodKey: string) => {
    const token = String(periodKey || "").trim();
    if (!/^\d{4}-\d{2}$/.test(token)) {
      return null;
    }
    const year = Number(token.slice(0, 4));
    const month = Number(token.slice(5, 7));
    if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) {
      return null;
    }
    return { year, month };
  };

  const start = parse(startPeriodKey);
  const end = parse(endPeriodKey);
  if (!start || !end) {
    return [] as string[];
  }

  const startIndex = start.year * 12 + (start.month - 1);
  const endIndex = end.year * 12 + (end.month - 1);
  if (startIndex > endIndex) {
    return [] as string[];
  }

  const periods: string[] = [];
  for (let index = startIndex; index <= endIndex; index += 1) {
    const year = Math.floor(index / 12);
    const month = (index % 12) + 1;
    periods.push(`${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}`);
  }
  return periods;
}

export function monthRangeFromPeriodKey(periodKey: string) {
  const token = String(periodKey || "").trim();
  if (!/^\d{4}-\d{2}$/.test(token)) {
    return { periodFrom: "", periodTo: "" };
  }

  const [yearToken, monthToken] = token.split("-");
  const year = Number(yearToken);
  const month = Number(monthToken);
  if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) {
    return { periodFrom: "", periodTo: "" };
  }

  const lastDay = new Date(year, month, 0).getDate();
  return {
    periodFrom: `${yearToken}-${monthToken}-01`,
    periodTo: `${yearToken}-${monthToken}-${String(lastDay).padStart(2, "0")}`,
  };
}
