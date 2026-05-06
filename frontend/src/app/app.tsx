import { useState } from "react";
import { DashboardShellStateProvider } from "@/app/dashboard-shell-state";
import { DashboardControls } from "@/app/dashboard-controls";
import { DashboardRuntimeProvider } from "@/app/dashboard-runtime";
import { DashboardSharedModals } from "@/app/dashboard-shared-modals";
import { DashboardThemeModal } from "@/app/dashboard-theme-modal";
import { AppShell } from "@/app/app-shell";
import { AnalyticsShell } from "@/features/analytics/analytics-shell";
import { BookingsGlobalRuntime } from "@/features/bookings/bookings-global-runtime";
import { BookingsShell } from "@/features/bookings/bookings-shell";
import { CustomersShell } from "@/features/customers/customers-shell";
import { EbayShell } from "@/features/ebay/ebay-shell";
import { GoogleAdsShell } from "@/features/google-ads/google-ads-shell";
import { OrdersShell } from "@/features/orders/orders-shell";
import { OrderDetailRuntime } from "@/features/orders/order-detail-runtime";
import { useRoute } from "@/shared/runtime/use-route";
import type { DashboardRoute } from "@/shared/runtime/dashboard-route";
import type { ReactElement } from "react";
import { ThemeProvider } from "@/shared/theme/theme-provider";

// Each route is mounted lazily on first visit, then kept alive (hidden via CSS).
// This prevents re-fetching data when switching tabs.
const ROUTE_SHELLS: Record<DashboardRoute, () => ReactElement> = {
  analytics: () => <AnalyticsShell />,
  orders: () => <OrdersShell />,
  customers: () => <CustomersShell />,
  bookings: () => <BookingsShell />,
  "google-ads": () => <GoogleAdsShell />,
  ebay: () => <EbayShell />,
};

export function App() {
  const [route, navigate] = useRoute();
  // Track which routes have been visited so we only mount them once.
  const [mounted, setMounted] = useState<Set<DashboardRoute>>(() => new Set([route]));

  function ensureMounted(r: DashboardRoute) {
    if (!mounted.has(r)) {
      setMounted((prev) => new Set([...prev, r]));
    }
  }

  // Ensure the current route is mounted before rendering.
  if (!mounted.has(route)) {
    ensureMounted(route);
  }

  return (
    <ThemeProvider>
      <DashboardShellStateProvider>
        <DashboardRuntimeProvider>
          <div className="app-root" data-dashboard-route={route}>
            <AppShell route={route} navigate={navigate}>
              {(Object.keys(ROUTE_SHELLS) as DashboardRoute[]).map((r) => {
                if (!mounted.has(r)) return null;
                const Shell = ROUTE_SHELLS[r];
                return (
                  <div key={r} style={r !== route ? { display: "none" } : undefined}>
                    <Shell />
                  </div>
                );
              })}
            </AppShell>
            <DashboardControls />
            <DashboardSharedModals />
            <DashboardThemeModal />
            <OrderDetailRuntime />
            <BookingsGlobalRuntime />
          </div>
        </DashboardRuntimeProvider>
      </DashboardShellStateProvider>
    </ThemeProvider>
  );
}
