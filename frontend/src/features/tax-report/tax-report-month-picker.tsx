import { useEffect, useRef, useState } from "react";

import { MONTH_NAMES, monthLabel } from "./labels";

function cx(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(" ");
}

type TaxReportMonthPickerProps = {
  value: string;
  onChange: (token: string) => void;
};

/**
 * Monatswähler im selben Aufbau wie der Zeitraum-Picker der Sidebar:
 * gleicher Trigger (`sidebar-control-btn`), gleiches Popup (`control-menu`)
 * und dieselben Kalender-Klassen (`date-menu-*`, `date-days-grid`, `date-day`).
 */
export function TaxReportMonthPicker({ value, onChange }: TaxReportMonthPickerProps) {
  const [open, setOpen] = useState(false);
  const [year, setYear] = useState(() => Number(value.slice(0, 4)) || new Date().getFullYear());
  const wrapRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!open) {
      return;
    }
    const onPointerDown = (event: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(event.target as Node)) {
        setOpen(false);
      }
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("mousedown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [open]);

  return (
    <div className="control-menu-wrap" ref={wrapRef}>
      <button
        id="taxReportMonthBtn"
        className="sidebar-control-btn"
        type="button"
        aria-expanded={open ? "true" : "false"}
        aria-controls="taxReportMonthMenu"
        onClick={() => setOpen((previous) => !previous)}
      >
        {monthLabel(value)}
      </button>
      <div
        id="taxReportMonthMenu"
        className={cx("control-menu", open && "active")}
        aria-hidden={open ? "false" : "true"}
        style={{ minWidth: 340 }}
      >
        <div className="date-menu-custom-title">Berichtsmonat</div>
        <div className="date-menu-nav">
          <button
            className="menu-item"
            type="button"
            aria-label="Vorheriges Jahr"
            onClick={() => setYear((previous) => previous - 1)}
          >
            &larr;
          </button>
          <button
            className="menu-item"
            type="button"
            aria-label="Folgendes Jahr"
            onClick={() => setYear((previous) => previous + 1)}
          >
            &rarr;
          </button>
        </div>
        <div className="date-month-label">{year}</div>
        <div className="date-days-grid" style={{ gridTemplateColumns: "repeat(3, 1fr)" }}>
          {MONTH_NAMES.map((name, index) => {
            const token = `${year}-${String(index + 1).padStart(2, "0")}`;
            const selected = token === value;
            return (
              <button
                key={name}
                className={cx("date-day", selected && "range-edge")}
                type="button"
                data-month={token}
                onClick={() => {
                  onChange(token);
                  setOpen(false);
                }}
              >
                {name}
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}
