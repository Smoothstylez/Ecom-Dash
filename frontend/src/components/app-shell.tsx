import { BarChart3, BookOpenText, Boxes, FileChartColumnIncreasing, ShoppingCart, Users } from "lucide-react";
import type { ReactNode } from "react";
import { Link, appNavigation } from "@/router";
import { cn } from "@/lib/utils";

const navIcons = {
  "/analytics": BarChart3,
  "/orders": ShoppingCart,
  "/customers": Users,
  "/bookings": BookOpenText,
  "/google-ads": FileChartColumnIncreasing,
  "/ebay": Boxes,
} as const;

interface AppShellProps {
  children: ReactNode;
}

export function AppShell({ children }: AppShellProps) {
  return (
    <div className="min-h-screen bg-[var(--page-bg)] px-3 py-3 text-[var(--ink)] sm:px-4 sm:py-4">
      <div className="mx-auto flex min-h-[calc(100vh-1.5rem)] max-w-[1600px] gap-3">
        <aside className="hidden w-[248px] shrink-0 rounded-[24px] border border-[var(--border)] bg-[var(--panel)] p-4 shadow-[var(--shadow)] lg:flex lg:flex-col">
          <div className="mb-5 flex items-center gap-3 border-b border-[var(--border)] pb-4">
            <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-[linear-gradient(145deg,var(--btn-primary-from),var(--btn-primary-to))] font-[var(--font-display)] text-sm font-bold text-white">
              EC
            </div>
            <div>
              <p className="font-[var(--font-display)] text-base font-semibold text-[var(--ink)]">E-Commerce</p>
              <p className="text-xs text-[var(--ink-4)]">React Preview</p>
            </div>
          </div>

          <nav className="flex flex-1 flex-col gap-1.5">
            {appNavigation.map((item) => {
              const Icon = navIcons[item.to];

              return (
                <Link
                  key={item.to}
                  activeOptions={{ exact: true }}
                  activeProps={{
                    className:
                      "bg-[linear-gradient(145deg,var(--btn-primary-from),var(--btn-primary-to))] text-white border-transparent shadow-sm",
                  }}
                  className={cn(
                    "flex items-center gap-3 rounded-2xl border border-transparent px-3 py-3 text-sm font-medium text-[var(--ink-3)] transition-colors hover:bg-[var(--surface-2)] hover:text-[var(--ink)]",
                  )}
                  to={item.to}
                >
                  <Icon className="h-4 w-4" />
                  <span>{item.label}</span>
                </Link>
              );
            })}
          </nav>

          <div className="mt-5 rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4">
            <p className="font-[var(--font-display)] text-sm font-semibold text-[var(--ink)]">Migration Status</p>
            <p className="mt-2 text-sm leading-6 text-[var(--ink-4)]">
              Legacy bleibt produktiv. Neue Routes werden hier einzeln uebernommen und gegen die bestehenden APIs getestet.
            </p>
          </div>
        </aside>

        <div className="flex min-w-0 flex-1 flex-col gap-3">
          <header className="rounded-[24px] border border-[var(--border)] bg-[var(--panel)] px-5 py-4 shadow-[var(--shadow)]">
            <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div>
                <p className="text-xs font-medium uppercase tracking-[0.22em] text-[var(--ink-4)]">Preview Workspace</p>
                <h1 className="mt-1 font-[var(--font-display)] text-2xl font-semibold text-[var(--ink)]">
                  Frontend Migration auf React 19
                </h1>
              </div>
              <div className="flex flex-wrap items-center gap-2 text-xs text-[var(--ink-4)]">
                <span className="rounded-full border border-[var(--border)] bg-[var(--surface)] px-3 py-1.5">Backend unveraendert</span>
                <span className="rounded-full border border-[var(--border)] bg-[var(--surface)] px-3 py-1.5">Analytics als Referenz</span>
                <span className="rounded-full border border-[var(--border)] bg-[var(--surface)] px-3 py-1.5">Kleine testbare Schritte</span>
              </div>
            </div>
          </header>

          <main className="min-h-0 rounded-[28px] border border-[var(--border)] bg-[var(--panel)] p-4 shadow-[var(--shadow)] sm:p-5">
            {children}
          </main>
        </div>
      </div>
    </div>
  );
}
