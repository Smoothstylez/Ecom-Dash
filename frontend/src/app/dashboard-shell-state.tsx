import { createContext, useCallback, useContext, useMemo, useState, type PropsWithChildren } from "react";

import { applyDatePreset } from "@/features/analytics/format";

export type DatePreset =
  | "today"
  | "yesterday"
  | "last_7_days"
  | "last_30_days"
  | "last_90_days"
  | "this_month"
  | "last_month"
  | "this_year"
  | "all_time"
  | "custom";

export type BookingsSubtab = "transactions" | "orders" | "templates" | "accounts" | "documents";

export type ShellFilters = {
  datePreset: DatePreset;
  from: string;
  to: string;
  marketplace: string;
  q: string;
};

type DashboardShellStateContextValue = {
  filters: ShellFilters;
  setFilters: (next: ShellFilters) => void;
  bookingsSubtab: BookingsSubtab;
  setBookingsSubtab: (next: BookingsSubtab) => void;
  refreshRequestToken: number;
  requestRefresh: () => void;
  closeSettingsPanelRequestToken: number;
  requestCloseSettingsPanel: () => void;
  themeModalRequestToken: number;
  requestOpenThemeModal: () => void;
};

const DashboardShellStateContext = createContext<DashboardShellStateContextValue | null>(null);

function normalizeBookingsSubtab(value: string | null | undefined): BookingsSubtab {
  const token = String(value || "").trim().toLowerCase();
  if (token === "orders" || token === "templates" || token === "accounts" || token === "documents") {
    return token;
  }
  return "transactions";
}

function readInitialBookingsSubtab() {
  const params = new URLSearchParams(window.location.search);
  return normalizeBookingsSubtab(params.get("subtab"));
}

function readInitialFilters(): ShellFilters {
  const initialRange = applyDatePreset("last_30_days");
  return {
    datePreset: "last_30_days",
    from: initialRange.from,
    to: initialRange.to,
    marketplace: "",
    q: "",
  };
}

export function DashboardShellStateProvider({ children }: PropsWithChildren) {
  const [filters, setFilters] = useState<ShellFilters>(() => readInitialFilters());
  const [bookingsSubtab, setBookingsSubtab] = useState<BookingsSubtab>(() => readInitialBookingsSubtab());
  const [refreshRequestToken, setRefreshRequestToken] = useState(0);
  const [closeSettingsPanelRequestToken, setCloseSettingsPanelRequestToken] = useState(0);
  const [themeModalRequestToken, setThemeModalRequestToken] = useState(0);
  const requestRefresh = useCallback(() => {
    setRefreshRequestToken((current) => current + 1);
  }, []);
  const requestCloseSettingsPanel = useCallback(() => {
    setCloseSettingsPanelRequestToken((current) => current + 1);
  }, []);
  const requestOpenThemeModal = useCallback(() => {
    setThemeModalRequestToken((current) => current + 1);
  }, []);

  const value = useMemo<DashboardShellStateContextValue>(() => {
    return {
      filters,
      setFilters,
      bookingsSubtab,
      setBookingsSubtab,
      refreshRequestToken,
      requestRefresh,
      closeSettingsPanelRequestToken,
      requestCloseSettingsPanel,
      themeModalRequestToken,
      requestOpenThemeModal,
    };
  }, [bookingsSubtab, closeSettingsPanelRequestToken, filters, refreshRequestToken, requestCloseSettingsPanel, requestOpenThemeModal, requestRefresh, themeModalRequestToken]);

  return (
    <DashboardShellStateContext.Provider value={value}>
      {children}
    </DashboardShellStateContext.Provider>
  );
}

export function useDashboardShellState() {
  const context = useContext(DashboardShellStateContext);
  if (!context) {
    throw new Error("useDashboardShellState must be used inside DashboardShellStateProvider.");
  }
  return context;
}
