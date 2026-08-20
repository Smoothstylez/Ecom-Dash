import { useEffect, useRef } from "react";

import { useDashboardRuntime } from "@/app/dashboard-runtime";

/**
 * Registers the given open/close state with the dashboard's shared details
 * modal (the same popup used for Order and Booking details) and returns the
 * DOM node to portal content into. Mirrors the registration pattern used by
 * OrderDetailRuntime/BookingsGlobalRuntime so the shared modal's built-in
 * "Schliessen" button and backdrop correctly close this flow too.
 */
export function useAmazonDetailModal(isOpen: boolean, title: string, onClose: () => void) {
  const { detailsModalApi, registerAmazonDetailsApi } = useDashboardRuntime();
  const isOpenRef = useRef(isOpen);
  const onCloseRef = useRef(onClose);
  isOpenRef.current = isOpen;
  onCloseRef.current = onClose;

  useEffect(() => {
    registerAmazonDetailsApi({
      isOpen: () => isOpenRef.current,
      close: () => onCloseRef.current(),
    });
    return () => registerAmazonDetailsApi(null);
  }, [registerAmazonDetailsApi]);

  useEffect(() => {
    if (isOpen) {
      detailsModalApi?.open(title);
    } else {
      detailsModalApi?.close();
    }
  }, [detailsModalApi, isOpen, title]);

  return typeof document !== "undefined" ? document.getElementById("detailsContent") : null;
}
