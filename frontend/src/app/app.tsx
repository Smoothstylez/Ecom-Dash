import { lazy, Suspense, useMemo } from "react";
import { DashboardShellStateProvider } from "@/app/dashboard-shell-state";
import { DashboardControls } from "@/app/dashboard-controls";
import { ErrorBoundary } from "@/app/error-boundary";
import { DashboardRuntimeProvider } from "@/app/dashboard-runtime";
import { DashboardSharedModals } from "@/app/dashboard-shared-modals";
import { DashboardThemeModal } from "@/app/dashboard-theme-modal";
import { AppShell } from "@/app/app-shell";
import { BookingsGlobalRuntime } from "@/features/bookings/bookings-global-runtime";
import { OrderDetailRuntime } from "@/features/orders/order-detail-runtime";
import { useRoute } from "@/shared/runtime/use-route";
import type { DashboardRoute } from "@/shared/runtime/dashboard-route";
import { ThemeProvider } from "@/shared/theme/theme-provider";

const AnalyticsShell = lazy(() => import("@/features/analytics/analytics-shell").then(m => ({ default: m.AnalyticsShell })));
const OrdersShell = lazy(() => import("@/features/orders/orders-shell").then(m => ({ default: m.OrdersShell })));
const SupportShell = lazy(() => import("@/features/support/support-shell").then(m => ({ default: m.SupportShell })));
const CustomersShell = lazy(() => import("@/features/customers/customers-shell").then(m => ({ default: m.CustomersShell })));
const InvoicesShell = lazy(() => import("@/features/invoices/invoices-shell").then(m => ({ default: m.InvoicesShell })));
const BookingsShell = lazy(() => import("@/features/bookings/bookings-shell").then(m => ({ default: m.BookingsShell })));
const GoogleAdsShell = lazy(() => import("@/features/google-ads/google-ads-shell").then(m => ({ default: m.GoogleAdsShell })));
const EbayShell = lazy(() => import("@/features/ebay/ebay-shell").then(m => ({ default: m.EbayShell })));

function RouteFallback() {
  return <div className="page"><div className="card" style={{ padding: "2rem", textAlign: "center" }}>Lade...</div></div>;
}

function renderRouteShell(route: DashboardRoute) {
  if (route === "analytics") {
    return <AnalyticsShell isActive={true} />;
  }
  if (route === "orders") {
    return <OrdersShell isActive={true} />;
  }
  if (route === "support") {
    return <SupportShell isActive={true} />;
  }
  if (route === "customers") {
    return <CustomersShell isActive={true} />;
  }
  if (route === "invoices") {
    return <InvoicesShell isActive={true} />;
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
    return `${route}:${window.location.search}`;
  }, [route]);

  return (
    <ThemeProvider>
      <DashboardShellStateProvider>
        <DashboardRuntimeProvider>
          <div className="app-root" data-dashboard-route={route}>
            <AppShell route={route} navigate={navigate}>
              <ErrorBoundary key={routeKey} fallbackTitle="Ansicht konnte nicht geladen werden">
                <Suspense fallback={<RouteFallback />}>
                  {renderRouteShell(route)}
                </Suspense>
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
