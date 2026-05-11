import { useCallback, useState } from "react";

import { useDashboardShellState } from "@/app/dashboard-shell-state";

import { BookingsPanel } from "./bookings-panel";
import { BookingsPage } from "./bookings-page";

type BookingsShellProps = {
  isActive: boolean;
};

export function BookingsShell({ isActive }: BookingsShellProps) {
  const { bookingsSubtab } = useDashboardShellState();
  const [panelElement, setPanelElement] = useState<HTMLElement | null>(null);

  const handlePanelRef = useCallback((element: HTMLDivElement | null) => {
    setPanelElement(element);
  }, []);

  return (
    <>
      <BookingsPanel panelRef={handlePanelRef} bookingsSubtab={bookingsSubtab} />
      {panelElement ? <BookingsPage panelElement={panelElement} isActive={isActive} /> : null}
    </>
  );
}
