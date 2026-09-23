export const DASHBOARD_ROUTES = [
  "analytics",
  "orders",
  "support",
  "customers",
  "invoices",
  "tax-report",
  "bookings",
  "google-ads",
  "amazon",
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
  if (normalized === "/support") {
    return "support";
  }
  if (normalized === "/invoices") {
    return "invoices";
  }
  if (normalized === "/tax-report") {
    return "tax-report";
  }
  if (normalized === "/bookings" || normalized === "/bookings/full") {
    return "bookings";
  }
  if (normalized === "/google-ads") {
    return "google-ads";
  }
  if (normalized === "/amazon") {
    return "amazon";
  }
  if (normalized === "/ebay") {
    return "ebay";
  }
  return "analytics";
}
