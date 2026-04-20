import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

interface SurfaceCardProps {
  title?: string;
  description?: string;
  action?: ReactNode;
  children?: ReactNode;
  className?: string;
  contentClassName?: string;
}

export function SurfaceCard({
  title,
  description,
  action,
  children,
  className,
  contentClassName,
}: SurfaceCardProps) {
  const hasHeader = title || description || action;

  return (
    <section className={cn("rounded-[24px] border border-[var(--border)] bg-[var(--surface)] p-5", className)}>
      {hasHeader ? (
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div>
            {title ? <p className="font-[var(--font-display)] text-lg font-semibold text-[var(--ink)]">{title}</p> : null}
            {description ? <p className="mt-1 text-sm leading-6 text-[var(--ink-4)]">{description}</p> : null}
          </div>
          {action ? <div className="shrink-0">{action}</div> : null}
        </div>
      ) : null}
      {children ? <div className={cn(hasHeader ? "mt-4" : "", contentClassName)}>{children}</div> : null}
    </section>
  );
}
