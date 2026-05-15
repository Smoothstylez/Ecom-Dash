import { useMemo } from "react";
import { DashboardShellStateProvider } from "@/app/dashboard-shell-state";
import { DashboardControls } from "@/app/dashboard-controls";
import { ErrorBoundary } from "@/app/error-boundary";
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
import { ThemeProvider } from "@/shared/theme/theme-provider";

const HEAVY_ROUTES: ReadonlySet<DashboardRoute> = new Set([
  "orders",
  "customers",
  "bookings",
  "google-ads",
  "ebay",
]);

function renderRouteShell(route: DashboardRoute) {
  if (route === "analytics") {
    return <AnalyticsShell isActive={true} />;
  }
  if (route === "orders") {
    return <OrdersShell isActive={true} />;
  }
  if (route === "customers") {
    return <CustomersShell isActive={true} />;
  }
  if (route === "bookings") {
    return <BookingsShell isActive={true} />;
  }
  if (route === "google-ads") {
    return <GoogleAdsShell isActive={true} />;
  }
  return <EbayShell isActive={true} />;
}

export function App() {
  const [route, navigate] = useRoute();
  const routeKey = useMemo(() => {
    return HEAVY_ROUTES.has(route) ? `${route}:${window.location.search}` : route;
  }, [route]);

  return (
    <ThemeProvider>
      <DashboardShellStateProvider>
        <DashboardRuntimeProvider>
          <div className="app-root" data-dashboard-route={route}>
            <AppShell route={route} navigate={navigate}>
              <ErrorBoundary key={routeKey} fallbackTitle="Ansicht konnte nicht geladen werden">
                {renderRouteShell(route)}
              </ErrorBoundary>
            </AppShell>
            <DashboardControls route={route} />
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
