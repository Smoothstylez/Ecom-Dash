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
    const next = resolveDashboardRoute(path);
    if (window.location.pathname !== path) {
      window.history.pushState(null, "", path);
    }
    setRoute(next);
  }, []);

  return [route, navigate];
}
