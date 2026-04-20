import type { ButtonHTMLAttributes } from "react";
import { cn } from "@/lib/utils";

interface FilterChipProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  active?: boolean;
}

export function FilterChip({ active = false, className, type = "button", ...props }: FilterChipProps) {
  return (
    <button
      className={cn(
        "rounded-full border px-3 py-2 text-xs font-medium transition-colors",
        active
          ? "border-transparent bg-[linear-gradient(145deg,var(--btn-primary-from),var(--btn-primary-to))] text-white shadow-sm"
          : "border-[var(--border-strong)] bg-[var(--surface)] text-[var(--ink-3)] hover:bg-[var(--surface-2)] hover:text-[var(--ink)]",
        className,
      )}
      type={type}
      {...props}
    />
  );
}
