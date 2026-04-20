import type { ReactNode } from "react";
import { SurfaceCard } from "@/components/ui/surface-card";

interface DataTableShellProps {
  title: string;
  description?: string;
  meta?: ReactNode;
  children: ReactNode;
}

export function DataTableShell({ title, description, meta, children }: DataTableShellProps) {
  return (
    <SurfaceCard
      action={
        meta ? (
          <span className="rounded-full border border-[var(--border)] bg-[var(--surface-2)] px-3 py-1 text-xs text-[var(--ink-4)]">
            {meta}
          </span>
        ) : null
      }
      description={description}
      title={title}
    >
      <div className="overflow-hidden rounded-[20px] border border-[var(--border)]">
        <div className="overflow-x-auto">{children}</div>
      </div>
    </SurfaceCard>
  );
}
