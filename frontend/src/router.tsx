import { Suspense, lazy } from "react";
import {
  Link,
  Navigate,
  Outlet,
  createRootRouteWithContext,
  createRoute,
  createRouter,
} from "@tanstack/react-router";
import type { QueryClient } from "@tanstack/react-query";
import { AppShell } from "@/components/app-shell";

const AnalyticsPage = lazy(() => import("@/routes/analytics-page").then((module) => ({ default: module.AnalyticsPage })));
const OrdersPage = lazy(() => import("@/routes/orders-page").then((module) => ({ default: module.OrdersPage })));
const CustomersPage = lazy(() => import("@/routes/customers-page").then((module) => ({ default: module.CustomersPage })));
const BookingsPage = lazy(() => import("@/routes/bookings-page").then((module) => ({ default: module.BookingsPage })));
const GoogleAdsPage = lazy(() => import("@/routes/google-ads-page").then((module) => ({ default: module.GoogleAdsPage })));
const EbayPage = lazy(() => import("@/routes/ebay-page").then((module) => ({ default: module.EbayPage })));

const rootRoute = createRootRouteWithContext<{ queryClient: QueryClient }>()({
  component: () => (
    <AppShell>
      <Suspense fallback={<RouteFallback />}>
        <Outlet />
      </Suspense>
    </AppShell>
  ),
});

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: () => <Navigate to="/analytics" />,
});

const analyticsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/analytics",
  component: AnalyticsPage,
});

const ordersRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/orders",
  component: OrdersPage,
});

const customersRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/customers",
  component: CustomersPage,
});

const bookingsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/bookings",
  component: BookingsPage,
});

const googleAdsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/google-ads",
  component: GoogleAdsPage,
});

const ebayRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/ebay",
  component: EbayPage,
});

const routeTree = rootRoute.addChildren([
  indexRoute,
  analyticsRoute,
  ordersRoute,
  customersRoute,
  bookingsRoute,
  googleAdsRoute,
  ebayRoute,
]);

const routerBase =
  import.meta.env.BASE_URL && import.meta.env.BASE_URL !== "/"
    ? import.meta.env.BASE_URL.replace(/\/$/, "")
    : undefined;

export const router = createRouter({
  routeTree,
  basepath: routerBase,
  defaultPreload: "intent",
  scrollRestoration: true,
  defaultStructuralSharing: true,
  context: {
    queryClient: undefined as unknown as QueryClient,
  },
  Wrap: ({ children }) => <>{children}</>,
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

export const appNavigation = [
  { to: "/analytics", label: "Analytics" },
  { to: "/orders", label: "Orders" },
  { to: "/customers", label: "Kunden" },
  { to: "/bookings", label: "Buchungen" },
  { to: "/google-ads", label: "Google Ads" },
  { to: "/ebay", label: "eBay" },
] as const;

export { Link };

function RouteFallback() {
  return (
    <div className="rounded-[24px] border border-[var(--border)] bg-[var(--surface)] px-5 py-5 text-sm text-[var(--ink-4)]">
      Preview-Route wird geladen...
    </div>
  );
}
