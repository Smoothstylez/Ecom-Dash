import { useCallback, useEffect, useState } from "react";
import { resolveDashboardRoute, type DashboardRoute } from "@/shared/runtime/dashboard-route";

/**
 * Minimal client-side router.
 *
 * - Reads the current route from window.location.pathname.
 * - Navigates via history.pushState (no full page reload).
 * - Reacts to browser back/forward via the popstate event.
 */
export function useRoute(): [DashboardRoute, (path: string) => void] {
  const [route, setRoute] = useState<DashboardRoute>(() =>
    resolveDashboardRoute(window.location.pathname),
  );

  useEffect(() => {
    const handlePopState = () => {
      setRoute(resolveDashboardRoute(window.location.pathname));
    };
    window.addEventListener("popstate", handlePopState);
    return () => {
      window.removeEventListener("popstate", handlePopState);
    };
  }, []);

  const navigate = useCallback((path: string) => {
    // path may include a query string (e.g. /bookings/full?subtab=transactions)
    // resolveDashboardRoute only needs the pathname portion
    const [pathname] = path.split("?");
    const next = resolveDashboardRoute(pathname);
    const current = window.location.pathname + window.location.search;
    if (current !== path) {
      window.history.pushState(null, "", path);
    }
    setRoute(next);
  }, []);

  return [route, navigate];
}
