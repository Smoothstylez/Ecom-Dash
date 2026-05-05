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
import { resolveDashboardRoute } from "@/shared/runtime/dashboard-route";
import { ThemeProvider } from "@/shared/theme/theme-provider";

export function App() {
  const route = resolveDashboardRoute(window.location.pathname);

  let content = <AnalyticsShell />;
  if (route === "orders") {
    content = <OrdersShell />;
  } else if (route === "customers") {
    content = <CustomersShell />;
  } else if (route === "google-ads") {
    content = <GoogleAdsShell />;
  } else if (route === "ebay") {
    content = <EbayShell />;
  } else if (route === "bookings") {
    content = <BookingsShell />;
  }

  return (
    <ThemeProvider>
      <DashboardShellStateProvider>
        <DashboardRuntimeProvider>
          <div className="app-root" data-dashboard-route={route}>
            <AppShell route={route}>{content}</AppShell>
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
