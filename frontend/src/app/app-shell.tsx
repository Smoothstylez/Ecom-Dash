import { useEffect, useRef, useState, type ReactNode } from "react";

import {
  useDashboardShellState,
  type DatePreset,
  type ShellFilters,
} from "@/app/dashboard-shell-state";
import { applyDatePreset, datePresetLabel, formatDateToken } from "@/features/analytics/format";
import { stripDashboardBasePath, withDashboardBasePath } from "@/shared/runtime/base-path";
import type { DashboardRoute } from "@/shared/runtime/dashboard-route";

type DateRangeUiState = {
  anchorMonth: string;
  customFrom: string;
  customTo: string;
};

const DATE_PRESETS: ReadonlyArray<Exclude<DatePreset, "custom">> = [
  "today",
  "yesterday",
  "last_7_days",
  "last_30_days",
  "last_90_days",
  "this_month",
  "last_month",
  "this_year",
  "all_time",
];

function classNames(...parts: Array<string | false>) {
  return parts.filter(Boolean).join(" ");
}

function monthTokenFromDate(dateValue: Date) {
  return `${dateValue.getFullYear()}-${String(dateValue.getMonth() + 1).padStart(2, "0")}`;
}

function monthDateFromToken(token: string) {
  const text = String(token || "").trim();
  if (!/^\d{4}-\d{2}$/.test(text)) {
    return null;
  }
  const year = Number(text.slice(0, 4));
  const month = Number(text.slice(5, 7));
  if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) {
    return null;
  }
  return new Date(year, month - 1, 1);
}

function shiftMonthToken(token: string, delta: number) {
  const base = monthDateFromToken(token) || new Date();
  base.setDate(1);
  base.setMonth(base.getMonth() + delta);
  return monthTokenFromDate(base);
}

function currentMonthToken() {
  return monthTokenFromDate(new Date());
}

function buildInitialDateRangeUi(filters: ShellFilters): DateRangeUiState {
  return {
    anchorMonth: filters.from ? filters.from.slice(0, 7) : currentMonthToken(),
    customFrom: filters.from,
    customTo: filters.to,
  };
}

function channelLabel(value: string) {
  const token = String(value || "").trim().toLowerCase();
  if (token === "shopify") {
    return "Shopify";
  }
  if (token === "kaufland") {
    return "Kaufland";
  }
  return "Alle";
}

function dateRangeButtonLabel(filters: ShellFilters) {
  if (filters.datePreset === "custom" && filters.from && filters.to) {
    return `${formatDateToken(filters.from)} - ${formatDateToken(filters.to)}`;
  }
  return datePresetLabel(filters.datePreset);
}

function dateRangePreviewLabel(dateRangeUi: DateRangeUiState) {
  const fromToken = String(dateRangeUi.customFrom || "").trim();
  const toToken = String(dateRangeUi.customTo || "").trim() || fromToken;
  return `${formatDateToken(fromToken)} -> ${formatDateToken(toToken)}`;
}

function buildCalendarCells(
  monthDate: Date,
  dateRangeUi: DateRangeUiState,
  onSelectDateToken: (token: string) => void,
) {
  const year = monthDate.getFullYear();
  const month = monthDate.getMonth();
  const firstWeekday = (new Date(year, month, 1).getDay() + 6) % 7;
  const daysInMonth = new Date(year, month + 1, 0).getDate();
  const fromToken = String(dateRangeUi.customFrom || "").trim();
  const toToken = String(dateRangeUi.customTo || "").trim() || fromToken;
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const todayToken = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-${String(today.getDate()).padStart(2, "0")}`;
  const cells: ReactNode[] = [];

  for (let idx = 0; idx < firstWeekday; idx += 1) {
    cells.push(
      <button
        key={`empty-${year}-${month}-${idx}`}
        className="date-day empty"
        type="button"
        tabIndex={-1}
        aria-hidden="true"
      />,
    );
  }

  for (let day = 1; day <= daysInMonth; day += 1) {
    const token = `${year}-${String(month + 1).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
    const inRange = fromToken && toToken && token >= fromToken && token <= toToken;
    const isEdge = token === fromToken || token === toToken;
    const isToday = token === todayToken;
    cells.push(
      <button
        key={token}
        className={classNames(
          "date-day",
          inRange && "in-range",
          isEdge && "range-edge",
          isToday && "today",
        )}
        data-date-token={token}
        type="button"
        onClick={() => {
          onSelectDateToken(token);
        }}
      >
        {day}
      </button>,
    );
  }

  while (cells.length % 7 !== 0) {
    cells.push(
      <button
        key={`pad-${year}-${month}-${cells.length}`}
        className="date-day empty"
        type="button"
        tabIndex={-1}
        aria-hidden="true"
      />,
    );
  }

  return cells;
}

type AppShellProps = {
  route: DashboardRoute;
  navigate: (path: string) => void;
  children: ReactNode;
};

export function AppShell({ route, navigate, children }: AppShellProps) {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const { filters, setFilters, bookingsSubtab, setBookingsSubtab, requestRefresh } = useDashboardShellState();
  const [isDateRangeMenuOpen, setDateRangeMenuOpen] = useState(false);
  const [isChannelMenuOpen, setChannelMenuOpen] = useState(false);
  const [isSearchOpen, setSearchOpen] = useState(false);
  const [dateRangeUi, setDateRangeUi] = useState<DateRangeUiState>(() => {
    return buildInitialDateRangeUi(filters);
  });

  const dateRangeBtnRef = useRef<HTMLButtonElement | null>(null);
  const dateRangeMenuRef = useRef<HTMLDivElement | null>(null);
  const channelMenuBtnRef = useRef<HTMLButtonElement | null>(null);
  const channelMenuRef = useRef<HTMLDivElement | null>(null);
  const searchDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const searchInputRef = useRef<HTMLInputElement | null>(null);

  const firstMonthDate = monthDateFromToken(dateRangeUi.anchorMonth) || new Date();
  firstMonthDate.setDate(1);
  const secondMonthDate = new Date(firstMonthDate.getFullYear(), firstMonthDate.getMonth() + 1, 1);
  const firstMonthLabel = firstMonthDate.toLocaleDateString("de-DE", { month: "long", year: "numeric" });
  const secondMonthLabel = secondMonthDate.toLocaleDateString("de-DE", { month: "long", year: "numeric" });

  useEffect(() => {
    const normalizedPath = stripDashboardBasePath(window.location.pathname).trim().toLowerCase();
    const isBookingsFull = normalizedPath === "/bookings/full";
    document.body.classList.toggle("bookings-full", isBookingsFull);

    return () => {
      document.body.classList.remove("bookings-full");
    };
  }, [route]);

  useEffect(() => {
    if (!isSearchOpen) {
      return;
    }
    const rafId = window.requestAnimationFrame(() => {
      searchInputRef.current?.focus();
      searchInputRef.current?.select();
    });
    return () => {
      window.cancelAnimationFrame(rafId);
    };
  }, [isSearchOpen]);

  useEffect(() => {
    const handleDocumentClick = (event: MouseEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) {
        return;
      }
      if (
        dateRangeBtnRef.current instanceof HTMLElement
        && dateRangeMenuRef.current instanceof HTMLElement
        && !dateRangeBtnRef.current.contains(target)
        && !dateRangeMenuRef.current.contains(target)
      ) {
        setDateRangeMenuOpen(false);
      }
      if (
        channelMenuBtnRef.current instanceof HTMLElement
        && channelMenuRef.current instanceof HTMLElement
        && !channelMenuBtnRef.current.contains(target)
        && !channelMenuRef.current.contains(target)
      ) {
        setChannelMenuOpen(false);
      }
    };

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape") {
        return;
      }
      setDateRangeMenuOpen(false);
      setChannelMenuOpen(false);
      setSearchOpen(false);
    };

    document.addEventListener("click", handleDocumentClick);
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("click", handleDocumentClick);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, []);

  function openDateRangeMenu() {
    setChannelMenuOpen(false);
    setSearchOpen(false);
    setDateRangeMenuOpen((current) => !current);
  }

  function openChannelMenu() {
    setDateRangeMenuOpen(false);
    setSearchOpen(false);
    setChannelMenuOpen((current) => !current);
  }

  function openSearch() {
    setDateRangeMenuOpen(false);
    setChannelMenuOpen(false);
    setSearchOpen(true);
  }

  function applyDatePresetSelection(preset: Exclude<DatePreset, "custom">) {
    const nextRange = applyDatePreset(preset);
    const nextFilters = {
      ...filters,
      datePreset: preset,
      from: nextRange.from,
      to: nextRange.to,
    } as ShellFilters;
    setFilters(nextFilters);
    setDateRangeUi({
      anchorMonth: nextRange.from ? nextRange.from.slice(0, 7) : currentMonthToken(),
      customFrom: nextRange.from,
      customTo: nextRange.to,
    });
    setDateRangeMenuOpen(false);
  }

  function selectCustomDateToken(token: string) {
    setDateRangeUi((current) => {
      const fromToken = String(current.customFrom || "").trim();
      const toToken = String(current.customTo || "").trim();
      if (!fromToken || toToken) {
        return {
          ...current,
          customFrom: token,
          customTo: "",
        };
      }
      if (token < fromToken) {
        return {
          ...current,
          customFrom: token,
          customTo: fromToken,
        };
      }
      return {
        ...current,
        customTo: token,
      };
    });
  }

  function applyCustomDateRangeSelection() {
    const from = String(dateRangeUi.customFrom || "").trim();
    const to = String(dateRangeUi.customTo || "").trim() || from;
    if (!from || !to || from > to) {
      return;
    }
    const nextFilters = {
      ...filters,
      datePreset: "custom",
      from,
      to,
    } as ShellFilters;
    setFilters(nextFilters);
    setDateRangeUi((current) => ({
      ...current,
      anchorMonth: from.slice(0, 7),
      customFrom: from,
      customTo: to,
    }));
    setDateRangeMenuOpen(false);
  }

  function selectMarketplace(nextMarketplace: string) {
    const nextFilters = {
      ...filters,
      marketplace: String(nextMarketplace || "").trim().toLowerCase(),
    } as ShellFilters;
    setFilters(nextFilters);
    setChannelMenuOpen(false);
  }

  function updateSearchQuery(value: string) {
    if (searchDebounceRef.current !== null) {
      clearTimeout(searchDebounceRef.current);
    }
    searchDebounceRef.current = setTimeout(() => {
      searchDebounceRef.current = null;
      const nextFilters = {
        ...filters,
        q: String(value || "").trim(),
      } as ShellFilters;
      setFilters(nextFilters);
    }, 300);
  }

  function clearSearchQuery() {
    if (searchDebounceRef.current !== null) {
      clearTimeout(searchDebounceRef.current);
      searchDebounceRef.current = null;
    }
    const nextFilters = {
      ...filters,
      q: "",
    } as ShellFilters;
    setFilters(nextFilters);
    window.requestAnimationFrame(() => {
      searchInputRef.current?.focus();
    });
  }

  return (
    <div className="page">
      <div className="full-back-row">
        <a className="bookkeeping-link" href={withDashboardBasePath("/bookings?subtab=transactions")}>Zurueck zum Cockpit</a>
      </div>

      <section className="controls-card" aria-hidden="true">
        {/* Controls moved to sidebar; kept for layout parity. */}
      </section>

      <div
        id="dateRangeMenu"
        ref={dateRangeMenuRef}
        className={classNames("control-menu", "date-range-menu", isDateRangeMenuOpen && "active")}
        aria-hidden={isDateRangeMenuOpen ? "false" : "true"}
      >
        <div className="date-menu-layout">
          <div className="date-menu-presets">
            {DATE_PRESETS.map((preset) => (
              <button
                key={preset}
                className={classNames("menu-item", filters.datePreset === preset && "active")}
                data-preset={preset}
                type="button"
                onClick={() => {
                  applyDatePresetSelection(preset);
                }}
              >
                {datePresetLabel(preset)}
              </button>
            ))}
          </div>
          <div className="date-menu-custom">
            <div className="date-menu-custom-title">Exakter Zeitraum</div>
            <div className="date-menu-custom-hint" id="dateRangePreviewText">{dateRangePreviewLabel(dateRangeUi)}</div>
            <div className="date-menu-nav">
              <button
                id="dateMonthPrevBtn"
                className="menu-item"
                type="button"
                onClick={() => {
                  setDateRangeUi((current) => ({
                    ...current,
                    anchorMonth: shiftMonthToken(current.anchorMonth, -1),
                  }));
                }}
              >
                &larr;
              </button>
              <button
                id="dateMonthNextBtn"
                className="menu-item"
                type="button"
                onClick={() => {
                  setDateRangeUi((current) => ({
                    ...current,
                    anchorMonth: shiftMonthToken(current.anchorMonth, 1),
                  }));
                }}
              >
                &rarr;
              </button>
            </div>
            <div className="date-calendars">
              <div className="date-calendar-pane">
                <div id="dateMonthLabelA" className="date-month-label">{firstMonthLabel}</div>
                <div className="date-weekdays">
                  <span>Mo</span><span>Di</span><span>Mi</span><span>Do</span><span>Fr</span><span>Sa</span><span>So</span>
                </div>
                <div id="dateCalendarA" className="date-days-grid">
                  {buildCalendarCells(firstMonthDate, dateRangeUi, selectCustomDateToken)}
                </div>
              </div>
              <div className="date-calendar-pane">
                <div id="dateMonthLabelB" className="date-month-label">{secondMonthLabel}</div>
                <div className="date-weekdays">
                  <span>Mo</span><span>Di</span><span>Mi</span><span>Do</span><span>Fr</span><span>Sa</span><span>So</span>
                </div>
                <div id="dateCalendarB" className="date-days-grid">
                  {buildCalendarCells(secondMonthDate, dateRangeUi, selectCustomDateToken)}
                </div>
              </div>
            </div>
            <button
              id="dateCustomApplyBtn"
              className="btn-inline"
              type="button"
              style={{ gridColumn: "1 / -1" }}
              disabled={!dateRangeUi.customFrom}
              onClick={() => {
                applyCustomDateRangeSelection();
              }}
            >
              Zeitraum anwenden
            </button>
          </div>
        </div>
      </div>
      <input id="fromDate" type="date" hidden value={filters.from} readOnly />
      <input id="toDate" type="date" hidden value={filters.to} readOnly />

      <div
        id="channelMenu"
        ref={channelMenuRef}
        className={classNames("control-menu", "channel-menu", isChannelMenuOpen && "active")}
        aria-hidden={isChannelMenuOpen ? "false" : "true"}
      >
        <button
          className={classNames("menu-item", !filters.marketplace && "active")}
          data-channel=""
          type="button"
          onClick={() => {
            selectMarketplace("");
          }}
        >
          Alle
        </button>
        <button
          className={classNames("menu-item", filters.marketplace === "shopify" && "active")}
          data-channel="shopify"
          type="button"
          onClick={() => {
            selectMarketplace("shopify");
          }}
        >
          Shopify
        </button>
        <button
          className={classNames("menu-item", filters.marketplace === "kaufland" && "active")}
          data-channel="kaufland"
          type="button"
          onClick={() => {
            selectMarketplace("kaufland");
          }}
        >
          Kaufland
        </button>
      </div>

      <div className={classNames("page-layout", sidebarCollapsed && "sidebar-collapsed")}>
        <nav className={classNames("sidebar", sidebarCollapsed && "collapsed")} id="mainSidebar">
          <div className="sidebar-inner">
            <div className="sidebar-header">
              <span className="sidebar-logo">E-Com</span>
              <button
                id="sidebarToggleBtn"
                className="sidebar-toggle-btn"
                type="button"
                aria-label={sidebarCollapsed ? "Sidebar ausklappen" : "Sidebar einklappen"}
                title={sidebarCollapsed ? "Ausklappen" : "Einklappen"}
                onClick={() => {
                  setSidebarCollapsed((current) => !current);
                }}
              >
                <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 18h18v-2H3v2zm0-5h18v-2H3v2zm0-7v2h18V6H3z" /></svg>
              </button>
            </div>

            <div className="sidebar-controls">
              <div className="sidebar-control sidebar-control-menu">
                <button
                  id="channelMenuBtn"
                  ref={channelMenuBtnRef}
                  className="sidebar-control-btn"
                  type="button"
                  aria-expanded={isChannelMenuOpen ? "true" : "false"}
                  aria-controls="channelMenu"
                  onClick={() => {
                    openChannelMenu();
                  }}
                >
                  {channelLabel(filters.marketplace)}
                </button>
              </div>
              <div className="sidebar-control sidebar-control-menu">
                <button
                  id="dateRangeBtn"
                  ref={dateRangeBtnRef}
                  className="sidebar-control-btn"
                  type="button"
                  aria-expanded={isDateRangeMenuOpen ? "true" : "false"}
                  aria-controls="dateRangeMenu"
                  onClick={() => {
                    openDateRangeMenu();
                  }}
                >
                  {dateRangeButtonLabel(filters)}
                </button>
              </div>
              <div className="sidebar-control">
                <button
                  id="searchOpenBtn"
                  className={classNames("sidebar-control-btn", Boolean(filters.q) && "active")}
                  type="button"
                  aria-expanded={isSearchOpen ? "true" : "false"}
                  aria-controls="searchModal"
                  aria-label="Suche oeffnen"
                  onClick={() => {
                    openSearch();
                  }}
                >
                  Suche
                </button>
              </div>
            </div>

            <div className="sidebar-nav" role="tablist" aria-label="Dashboard Bereiche">
              <button
                id="tabAnalyticsBtn"
                className={classNames("sidebar-nav-btn", route === "analytics" && "active")}
                type="button"
                role="tab"
                onClick={() => {
                  navigate("/analytics");
                }}
              >
                <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 13h8V3H3v10zm0 8h8v-6H3v6zm10 0h8V11h-8v10zm0-18v6h8V3h-8z" /></svg>
                <span>Analytics</span>
              </button>
              <button
                id="tabOrdersBtn"
                className={classNames("sidebar-nav-btn", route === "orders" && "active")}
                type="button"
                role="tab"
                onClick={() => {
                  navigate("/orders");
                }}
              >
                <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-7 14H7v-2h5v2zm3-4H7v-2h10v2zm0-4H7V7h10v2z" /></svg>
                <span>Orders</span>
              </button>
              <button
                id="tabCustomersBtn"
                className={classNames("sidebar-nav-btn", route === "customers" && "active")}
                type="button"
                role="tab"
                onClick={() => {
                  navigate("/customers");
                }}
              >
                <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M16 11c1.66 0 2.99-1.34 2.99-3S17.66 5 16 5c-1.66 0-3 1.34-3 3s1.34 3 3 3zm-8 0c1.66 0 2.99-1.34 2.99-3S9.66 5 8 5C6.34 5 5 6.34 5 8s1.34 3 3 3zm0 2c-2.33 0-7 1.17-7 3.5V19h14v-2.5c0-2.33-4.67-3.5-7-3.5zm8 0c-.29 0-.62.02-.97.05 1.16.84 1.97 1.97 1.97 3.45V19h6v-2.5c0-2.33-4.67-3.5-7-3.5z" /></svg>
                <span>Kunden</span>
              </button>
              <button
                id="tabInvoicesBtn"
                className={classNames("sidebar-nav-btn", route === "invoices" && "active")}
                type="button"
                role="tab"
                onClick={() => {
                  navigate("/invoices");
                }}
              >
                <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 2h9l5 5v15a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2zm8 2v4h4M8 12h8v-2H8v2zm0 4h8v-2H8v2zm0 4h5v-2H8v2z" /></svg>
                <span>Rechnungen</span>
              </button>
              <button
                id="tabBookingsBtn"
                className={classNames("sidebar-nav-btn", route === "bookings" && "active")}
                type="button"
                role="tab"
                onClick={() => {
                  navigate(`/bookings/full?subtab=${encodeURIComponent(bookingsSubtab)}`);
                }}
              >
                <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-5 14H7v-2h7v2zm3-4H7v-2h10v2zm0-4H7V7h10v2z" /></svg>
                <span>Buchungen</span>
              </button>
              <div className={classNames("sidebar-subnav", route === "bookings" && "visible")} id="bookingsSubnav">
                <button
                  className={classNames("sidebar-subnav-btn", bookingsSubtab === "transactions" && "active")}
                  type="button"
                  data-bookings-subtab="transactions"
                  onClick={() => {
                    setBookingsSubtab("transactions");
                  }}
                >
                  Transaktionen
                </button>
                <button
                  className={classNames("sidebar-subnav-btn", bookingsSubtab === "orders" && "active")}
                  type="button"
                  data-bookings-subtab="orders"
                  onClick={() => {
                    setBookingsSubtab("orders");
                  }}
                >
                  Bestellungen
                </button>
                <button
                  className={classNames("sidebar-subnav-btn", bookingsSubtab === "templates" && "active")}
                  type="button"
                  data-bookings-subtab="templates"
                  onClick={() => {
                    setBookingsSubtab("templates");
                  }}
                >
                  Templates
                </button>
                <button
                  className={classNames("sidebar-subnav-btn", bookingsSubtab === "accounts" && "active")}
                  type="button"
                  data-bookings-subtab="accounts"
                  onClick={() => {
                    setBookingsSubtab("accounts");
                  }}
                >
                  Konten
                </button>
                <button
                  className={classNames("sidebar-subnav-btn", bookingsSubtab === "documents" && "active")}
                  type="button"
                  data-bookings-subtab="documents"
                  onClick={() => {
                    setBookingsSubtab("documents");
                  }}
                >
                  Belege
                </button>
              </div>
              <button
                id="tabGoogleAdsBtn"
                className={classNames("sidebar-nav-btn", route === "google-ads" && "active")}
                type="button"
                role="tab"
                onClick={() => {
                  navigate("/google-ads");
                }}
              >
                <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-9.5 8.5c0 .83-.67 1.5-1.5 1.5H7v2H5.5V9H8c.83 0 1.5.67 1.5 1.5v1zm5 2c0 .83-.67 1.5-1.5 1.5h-2.5V9H13c.83 0 1.5.67 1.5 1.5v3zm4-3H17v1h1.5V11H17v2h-1.5V7h3v1.5zM9 9.5h1v-1H9v1zM4 6H2v14c0 1.1.9 2 2 2h14v-2H4V6zm10 5.5h1v-3h-1v3z" /></svg>
                <span>Google Ads</span>
              </button>
              <button
                id="tabEbayBtn"
                className={classNames("sidebar-nav-btn", route === "ebay" && "active")}
                type="button"
                role="tab"
                onClick={() => {
                  navigate("/ebay");
                }}
              >
                <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M7.5 12c0 .83-.67 1.5-1.5 1.5S4.5 12.83 4.5 12s.67-1.5 1.5-1.5 1.5.67 1.5 1.5zm12 0c0 .83-.67 1.5-1.5 1.5s-1.5-.67-1.5-1.5.67-1.5 1.5-1.5 1.5.67 1.5 1.5zM6 7.5h12v9H6v-9z" /></svg>
                <span>eBay</span>
              </button>
            </div>

            <div className="sidebar-footer">
              <button id="sidebarStatusBtn" className="sidebar-footer-btn" type="button" title="Status">
                <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z" /></svg>
                <span>Status</span>
              </button>
              <button id="sidebarSettingsBtn" className="sidebar-footer-btn" type="button" title="Einstellungen">
                <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M19.14 12.94c.04-.31.06-.63.06-.94 0-.31-.02-.63-.06-.94l2.03-1.58c.18-.14.23-.41.12-.61l-1.92-3.32c-.12-.22-.37-.29-.59-.22l-2.39.96c-.5-.38-1.03-.7-1.62-.94l-.36-2.54c-.04-.24-.24-.41-.48-.41h-3.84c-.24 0-.43.17-.47.41l-.36 2.54c-.59.24-1.13.57-1.62.94l-2.39-.96c-.22-.08-.47 0-.59.22L2.74 8.87c-.12.21-.08.47.12.61l2.03 1.58c-.04.31-.06.63-.06.94s.02.63.06.94l-2.03 1.58c-.18.14-.23.41-.12.61l1.92 3.32c.12.22.37.29.59.22l2.39-.96c.5.38 1.03.7 1.62.94l.36 2.54c.05.24.24.41.48.41h3.84c.24 0 .44-.17.47-.41l.36-2.54c.59-.24 1.13-.56 1.62-.94l2.39.96c.22.08.47 0 .59-.22l1.92-3.32c.12-.22.07-.47-.12-.61l-2.01-1.58zM12 15.6c-1.98 0-3.6-1.62-3.6-3.6s1.62-3.6 3.6-3.6 3.6 1.62 3.6 3.6-1.62 3.6-3.6 3.6z" /></svg>
                <span>Einstellungen</span>
              </button>
              <button id="sidebarSyncBtn" className="sidebar-footer-btn" type="button" title="Synchronisieren">
                <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 4V1L8 5l4 4V6c3.31 0 6 2.69 6 6 0 1.01-.25 1.97-.7 2.8l1.46 1.46C19.54 15.03 20 13.57 20 12c0-4.42-3.58-8-8-8zm0 14c-3.31 0-6-2.69-6-6 0-1.01.25-1.97.7-2.8L5.24 7.74C4.46 8.97 4 10.43 4 12c0 4.42 3.58 8 8 8v3l4-4-4-4v3z" /></svg>
                <span>Sync</span>
              </button>
            </div>
          </div>
        </nav>

        <div className="main-content-wrapper">
          <main className="main-content">{children}</main>
        </div>
      </div>

      <div
        id="searchModal"
        className={classNames("modal", isSearchOpen && "active")}
        role="dialog"
        aria-modal="true"
        aria-hidden={isSearchOpen ? "false" : "true"}
        onClick={(event) => {
          if (event.target === event.currentTarget) {
            setSearchOpen(false);
          }
        }}
      >
        <div className="modal-card search-modal-card">
          <div className="modal-head">
            <div className="modal-title">Suche</div>
            <button
              id="closeSearchModalBtn"
              className="btn-inline"
              type="button"
              onClick={() => {
                setSearchOpen(false);
              }}
            >
              Schliessen
            </button>
          </div>
          <div className="search-modal-body">
            <div className="control">
              <label htmlFor="searchInput">Detailsuche</label>
              <input
                id="searchInput"
                ref={searchInputRef}
                type="text"
                placeholder="Order, Kunde, Artikel, Referenz..."
                value={filters.q}
                onChange={(event) => {
                  updateSearchQuery(event.target.value);
                }}
                onKeyDown={(event) => {
                  if (event.key !== "Enter") {
                    return;
                  }
                  event.preventDefault();
                  requestRefresh();
                  setSearchOpen(false);
                }}
              />
            </div>
            <div className="search-modal-actions">
              <button
                id="clearSearchBtn"
                className="btn-inline ghost"
                type="button"
                style={{ display: filters.q ? undefined : "none" }}
                onClick={() => {
                  clearSearchQuery();
                }}
              >
                Suche leeren
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
