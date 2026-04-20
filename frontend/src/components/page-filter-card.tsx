import { Search } from "lucide-react";
import { SurfaceCard } from "@/components/ui/surface-card";

interface PageFilterCardProps {
  title: string;
  description: string;
  query: string;
  queryPlaceholder: string;
  queryLabel?: string;
  marketplace: string;
  marketplaceLabel?: string;
  marketplaceOptions?: Array<{ value: string; label: string }>;
  from: string;
  to: string;
  onQueryChange: (value: string) => void;
  onMarketplaceChange: (value: string) => void;
  onFromChange: (value: string) => void;
  onToChange: (value: string) => void;
}

const defaultMarketplaceOptions = [
  { value: "", label: "Alle" },
  { value: "shopify", label: "Shopify" },
  { value: "kaufland", label: "Kaufland" },
];

export function PageFilterCard({
  title,
  description,
  query,
  queryPlaceholder,
  queryLabel = "Suche",
  marketplace,
  marketplaceLabel = "Channel",
  marketplaceOptions = defaultMarketplaceOptions,
  from,
  to,
  onQueryChange,
  onMarketplaceChange,
  onFromChange,
  onToChange,
}: PageFilterCardProps) {
  return (
    <SurfaceCard description={description} title={title}>
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <label className="block">
          <span className="mb-2 block text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">{queryLabel}</span>
          <div className="flex items-center gap-2 rounded-2xl border border-[var(--border-strong)] bg-[var(--surface)] px-3 py-2.5">
            <Search className="h-4 w-4 text-[var(--ink-4)]" />
            <input
              className="w-full bg-transparent text-sm text-[var(--ink)] outline-none placeholder:text-[var(--ink-4)]"
              onChange={(event) => onQueryChange(event.target.value)}
              placeholder={queryPlaceholder}
              type="text"
              value={query}
            />
          </div>
        </label>

        <label className="block">
          <span className="mb-2 block text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">{marketplaceLabel}</span>
          <select
            className="w-full rounded-2xl border border-[var(--border-strong)] bg-[var(--surface)] px-3 py-2.5 text-sm text-[var(--ink)] outline-none"
            onChange={(event) => onMarketplaceChange(event.target.value)}
            value={marketplace}
          >
            {marketplaceOptions.map((option) => (
              <option key={option.value || "all"} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>

        <label className="block">
          <span className="mb-2 block text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">Von</span>
          <input
            className="w-full rounded-2xl border border-[var(--border-strong)] bg-[var(--surface)] px-3 py-2.5 text-sm text-[var(--ink)] outline-none"
            onChange={(event) => onFromChange(event.target.value)}
            type="date"
            value={from}
          />
        </label>

        <label className="block">
          <span className="mb-2 block text-xs uppercase tracking-[0.18em] text-[var(--ink-4)]">Bis</span>
          <input
            className="w-full rounded-2xl border border-[var(--border-strong)] bg-[var(--surface)] px-3 py-2.5 text-sm text-[var(--ink)] outline-none"
            onChange={(event) => onToChange(event.target.value)}
            type="date"
            value={to}
          />
        </label>
      </div>
    </SurfaceCard>
  );
}
