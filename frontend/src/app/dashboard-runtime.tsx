import { createContext, useCallback, useContext, useMemo, useRef, useState, type PropsWithChildren } from "react";

export type DetailsModalApi = {
  open: (title?: string) => void;
  close: () => void;
  isOpen: () => boolean;
  getTitle: () => string;
  setTitle: (title?: string) => void;
};

export type PreviewModalApi = {
  open: (url?: string, filename?: string, mimeType?: string) => void;
  close: () => void;
  isOpen: () => boolean;
};

export type OrderDetailsApi = {
  open: (marketplace: string, orderId: string, returnToTransactionId?: string) => void;
  openByExternalId: (marketplace: string, externalOrderId: string, returnToTransactionId?: string) => void;
  close: () => void;
  isOpen: () => boolean;
  hasPendingReturn: () => boolean;
  reopenPendingReturn: () => void;
};

export type BookingsDetailsApi = {
  openTransactionById: (transactionId?: string, options?: { returnToInvoiceId?: string }) => void;
  openMonthlyInvoiceById: (invoiceId?: string) => void;
  close: () => void;
  saveActive: (options?: { silent?: boolean }) => Promise<void>;
  deleteActive: () => Promise<void>;
  isOpen: () => boolean;
  getMode: () => "" | "booking-transaction" | "monthly-invoice";
};

export type BookingsUiState = {
  bookingClass: string;
  category: string;
  bookingType: string;
};

export type BookingsRefreshDetail = {
  bookingClass?: string;
  category?: string;
  bookingType?: string;
};

type DashboardRuntimeContextValue = {
  detailsModalApi: DetailsModalApi | null;
  registerDetailsModalApi: (api: DetailsModalApi | null) => void;
  previewModalApi: PreviewModalApi | null;
  registerPreviewModalApi: (api: PreviewModalApi | null) => void;
  orderDetailsApi: OrderDetailsApi | null;
  registerOrderDetailsApi: (api: OrderDetailsApi | null) => void;
  bookingsDetailsApi: BookingsDetailsApi | null;
  registerBookingsDetailsApi: (api: BookingsDetailsApi | null) => void;
  bookingsUiState: BookingsUiState;
  setBookingsUiState: (next: BookingsUiState) => void;
  bookingsRefreshRequestToken: number;
  bookingsRefreshDetail: BookingsRefreshDetail;
  requestBookingsRefresh: (detail?: BookingsRefreshDetail) => void;
};

const EMPTY_BOOKINGS_UI_STATE: BookingsUiState = {
  bookingClass: "",
  category: "",
  bookingType: "",
};

const DashboardRuntimeContext = createContext<DashboardRuntimeContextValue | null>(null);

export function DashboardRuntimeProvider({ children }: PropsWithChildren) {
  const [detailsModalApi, setDetailsModalApi] = useState<DetailsModalApi | null>(null);
  const [previewModalApi, setPreviewModalApi] = useState<PreviewModalApi | null>(null);
  const [orderDetailsApi, setOrderDetailsApi] = useState<OrderDetailsApi | null>(null);
  const [bookingsDetailsApi, setBookingsDetailsApi] = useState<BookingsDetailsApi | null>(null);
  const [bookingsUiState, setBookingsUiStateState] = useState<BookingsUiState>(EMPTY_BOOKINGS_UI_STATE);
  const [bookingsRefreshRequestToken, setBookingsRefreshRequestToken] = useState(0);
  const [bookingsRefreshDetail, setBookingsRefreshDetail] = useState<BookingsRefreshDetail>({});
  const bookingsUiStateRef = useRef(bookingsUiState);

  bookingsUiStateRef.current = bookingsUiState;

  const registerDetailsModalApi = useCallback((api: DetailsModalApi | null) => {
    setDetailsModalApi((current) => (current === api ? current : api));
  }, []);

  const registerPreviewModalApi = useCallback((api: PreviewModalApi | null) => {
    setPreviewModalApi((current) => (current === api ? current : api));
  }, []);

  const registerOrderDetailsApi = useCallback((api: OrderDetailsApi | null) => {
    setOrderDetailsApi((current) => (current === api ? current : api));
  }, []);

  const registerBookingsDetailsApi = useCallback((api: BookingsDetailsApi | null) => {
    setBookingsDetailsApi((current) => (current === api ? current : api));
  }, []);

  const setBookingsUiState = useCallback((next: BookingsUiState) => {
    setBookingsUiStateState((current) => {
      if (
        current.bookingClass === next.bookingClass
        && current.category === next.category
        && current.bookingType === next.bookingType
      ) {
        return current;
      }
      return next;
    });
  }, []);

  const requestBookingsRefresh = useCallback((detail?: BookingsRefreshDetail) => {
    const currentUiState = bookingsUiStateRef.current;
    setBookingsRefreshDetail({
      bookingClass: typeof detail?.bookingClass === "string" ? detail.bookingClass : currentUiState.bookingClass,
      category: typeof detail?.category === "string" ? detail.category : currentUiState.category,
      bookingType: typeof detail?.bookingType === "string" ? detail.bookingType : currentUiState.bookingType,
    });
    setBookingsRefreshRequestToken((current) => current + 1);
  }, []);

  const value = useMemo<DashboardRuntimeContextValue>(() => {
    return {
      detailsModalApi,
      registerDetailsModalApi,
      previewModalApi,
      registerPreviewModalApi,
      orderDetailsApi,
      registerOrderDetailsApi,
      bookingsDetailsApi,
      registerBookingsDetailsApi,
      bookingsUiState,
      setBookingsUiState,
      bookingsRefreshRequestToken,
      bookingsRefreshDetail,
      requestBookingsRefresh,
    };
  }, [bookingsDetailsApi, bookingsRefreshDetail, bookingsRefreshRequestToken, bookingsUiState, detailsModalApi, orderDetailsApi, previewModalApi, registerBookingsDetailsApi, registerDetailsModalApi, registerOrderDetailsApi, registerPreviewModalApi, requestBookingsRefresh, setBookingsUiState]);

  return (
    <DashboardRuntimeContext.Provider value={value}>
      {children}
    </DashboardRuntimeContext.Provider>
  );
}

export function useDashboardRuntime() {
  const context = useContext(DashboardRuntimeContext);
  if (!context) {
    throw new Error("useDashboardRuntime must be used inside DashboardRuntimeProvider.");
  }
  return context;
}
