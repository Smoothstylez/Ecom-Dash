export const DASHBOARD_ROUTES = [
  "analytics",
  "orders",
  "customers",
  "bookings",
  "google-ads",
  "ebay",
] as const;

export type DashboardRoute = (typeof DASHBOARD_ROUTES)[number];

export function resolveDashboardRoute(pathname: string): DashboardRoute {
  const normalized = String(pathname || "").trim().toLowerCase();

  if (normalized === "/orders") {
    return "orders";
  }
  if (normalized === "/customers") {
    return "customers";
  }
  if (normalized === "/bookings" || normalized === "/bookings/full") {
    return "bookings";
  }
  if (normalized === "/google-ads") {
    return "google-ads";
  }
  if (normalized === "/ebay") {
    return "ebay";
  }
  return "analytics";
}
