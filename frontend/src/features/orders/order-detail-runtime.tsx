import { useCallback, useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";

import { useDashboardRuntime } from "@/app/dashboard-runtime";
import { buildDashboardApiUrl } from "@/shared/runtime/base-path";
import { fetchOrderDetail, type OrderDetail } from "./api";
import { OrderDetailContent } from "./order-detail-content";

type RuntimeState = {
  isOpen: boolean;
  loading: boolean;
  error: string;
  title: string;
  marketplace: string;
  orderId: string;
  detail: OrderDetail | null;
  returnToTransactionId: string;
  pendingReturnOrder: {
    marketplace: string;
    orderId: string;
  } | null;
};

function defaultState(): RuntimeState {
  return {
    isOpen: false,
    loading: false,
    error: "",
    title: "Order Details",
    marketplace: "",
    orderId: "",
    detail: null,
    returnToTransactionId: "",
    pendingReturnOrder: null,
  };
}

async function resolveOrderId(marketplace: string, externalOrderId: string) {
  const response = await fetch(buildDashboardApiUrl(`/api/orders?marketplace=${encodeURIComponent(marketplace)}&q=${encodeURIComponent(externalOrderId)}&limit=50`), {
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(`Order lookup failed: ${response.status}`);
  }

  const payload = await response.json().catch(() => ({}));
  const items = Array.isArray(payload?.items) ? payload.items : [];
  const match = items.find((item: Record<string, unknown>) => {
    return String(item.marketplace || "").trim().toLowerCase() === marketplace
      && String(item.external_order_id || item.order_id || "").trim() === externalOrderId;
  });

  const orderId = String(match?.order_id || "").trim();
  if (!orderId) {
    throw new Error("Order konnte nicht gefunden werden.");
  }
  return orderId;
}

export function OrderDetailRuntime() {
  const { bookingsDetailsApi, detailsModalApi, previewModalApi, registerOrderDetailsApi } = useDashboardRuntime();
  const [state, setState] = useState<RuntimeState>(() => defaultState());
  const stateRef = useRef(state);
  const requestIdRef = useRef(0);

  useEffect(() => {
    stateRef.current = state;
  }, [state]);

  const showLegacyModal = useCallback((title: string) => {
    const detailsModal = detailsModalApi;
    if (detailsModal) {
      detailsModal.open(title);
      return;
    }
    const modal = document.getElementById("detailsModal");
    const titleElement = document.getElementById("detailsTitle");
    if (titleElement instanceof HTMLElement) {
      titleElement.textContent = title;
    }
    if (modal instanceof HTMLElement) {
      modal.classList.add("active");
      modal.setAttribute("aria-hidden", "false");
    }
  }, [detailsModalApi]);

  const hideLegacyModal = useCallback(() => {
    const detailsModal = detailsModalApi;
    if (detailsModal) {
      detailsModal.close();
      return;
    }
    const modal = document.getElementById("detailsModal");
    if (modal instanceof HTMLElement) {
      modal.classList.remove("active");
      modal.setAttribute("aria-hidden", "true");
    }
  }, [detailsModalApi]);

  const loadByOrderId = useCallback(async (marketplace: string, orderId: string, returnToTransactionId = "") => {
    const normalizedMarketplace = String(marketplace || "").trim().toLowerCase();
    const normalizedOrderId = String(orderId || "").trim();
    if (!normalizedMarketplace || !normalizedOrderId) {
      return;
    }

    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;
    const title = `Details ${normalizedMarketplace.toUpperCase()} ${normalizedOrderId}`;

    showLegacyModal(title);
    setState({
      isOpen: true,
      loading: true,
      error: "",
      title,
      marketplace: normalizedMarketplace,
      orderId: normalizedOrderId,
      detail: null,
      returnToTransactionId: String(returnToTransactionId || "").trim(),
      pendingReturnOrder: null,
    });

    try {
      const detail = await fetchOrderDetail(normalizedMarketplace, normalizedOrderId);
      if (requestId !== requestIdRef.current) {
        return;
      }
      setState({
        isOpen: true,
        loading: false,
        error: "",
        title,
        marketplace: normalizedMarketplace,
        orderId: normalizedOrderId,
        detail,
        returnToTransactionId: String(returnToTransactionId || "").trim(),
        pendingReturnOrder: null,
      });
    } catch (error) {
      if (requestId !== requestIdRef.current) {
        return;
      }
      setState({
        isOpen: true,
        loading: false,
        error: error instanceof Error ? error.message : "Details konnten nicht geladen werden.",
        title,
        marketplace: normalizedMarketplace,
        orderId: normalizedOrderId,
        detail: null,
        returnToTransactionId: String(returnToTransactionId || "").trim(),
        pendingReturnOrder: null,
      });
    }
  }, [showLegacyModal]);

  const close = useCallback(() => {
    requestIdRef.current += 1;
    const returnToTransactionId = stateRef.current.returnToTransactionId;
    hideLegacyModal();
    setState(defaultState());
    if (returnToTransactionId && bookingsDetailsApi) {
      bookingsDetailsApi.openTransactionById(returnToTransactionId);
    }
  }, [bookingsDetailsApi, hideLegacyModal]);

  const open = useCallback(async (marketplace: string, orderId: string, returnToTransactionId = "") => {
    await loadByOrderId(marketplace, orderId, returnToTransactionId);
  }, [loadByOrderId]);

  const openByExternalId = useCallback(async (marketplace: string, externalOrderId: string, returnToTransactionId = "") => {
    const normalizedMarketplace = String(marketplace || "").trim().toLowerCase();
    const normalizedExternalOrderId = String(externalOrderId || "").trim();
    if (!normalizedMarketplace || !normalizedExternalOrderId) {
      return;
    }

    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;
    const title = `Details ${normalizedMarketplace.toUpperCase()} ${normalizedExternalOrderId}`;
    showLegacyModal(title);
    setState({
      isOpen: true,
      loading: true,
      error: "",
      title,
      marketplace: normalizedMarketplace,
      orderId: "",
      detail: null,
      returnToTransactionId: String(returnToTransactionId || "").trim(),
      pendingReturnOrder: null,
    });

    try {
      const orderId = await resolveOrderId(normalizedMarketplace, normalizedExternalOrderId);
      if (requestId !== requestIdRef.current) {
        return;
      }
      await loadByOrderId(normalizedMarketplace, orderId, returnToTransactionId);
    } catch (error) {
      if (requestId !== requestIdRef.current) {
        return;
      }
      setState({
        isOpen: true,
        loading: false,
        error: error instanceof Error ? error.message : "Order konnte nicht gefunden werden.",
        title,
        marketplace: normalizedMarketplace,
        orderId: "",
        detail: null,
        returnToTransactionId: String(returnToTransactionId || "").trim(),
        pendingReturnOrder: null,
      });
    }
  }, [loadByOrderId, showLegacyModal]);

  const reopenPendingReturn = useCallback(() => {
    const pending = stateRef.current.pendingReturnOrder;
    if (!pending) {
      return;
    }
    void loadByOrderId(pending.marketplace, pending.orderId);
  }, [loadByOrderId]);

  useEffect(() => {
    registerOrderDetailsApi({
      open,
      openByExternalId,
      close,
      isOpen: () => stateRef.current.isOpen,
      hasPendingReturn: () => Boolean(stateRef.current.pendingReturnOrder),
      reopenPendingReturn,
    });

    return () => {
      registerOrderDetailsApi(null);
    };
  }, [close, open, openByExternalId, registerOrderDetailsApi, reopenPendingReturn]);

  const contentElement = document.getElementById("detailsContent");
  if (!(contentElement instanceof HTMLElement) || !state.isOpen) {
    return null;
  }

  return createPortal(
    <div
      id="ordersDetailsContent"
      data-marketplace={state.marketplace}
      data-order-id={state.orderId}
      onClick={(event) => {
        const target = event.target;
        if (!(target instanceof HTMLElement)) {
          return;
        }

        if (target.dataset.action === "preview-document") {
          event.preventDefault();
          previewModalApi?.open(target.dataset.url, target.dataset.filename, target.dataset.mime);
          return;
        }

        const interactive = target.closest("input, select, button, a, label, textarea");
        if (interactive) {
          return;
        }

        const txRow = target.closest("tr[data-tx-id]");
        if (!(txRow instanceof HTMLElement) || !txRow.dataset.txId || !bookingsDetailsApi) {
          return;
        }

        const current = stateRef.current;
        if (!current.marketplace || !current.orderId) {
          return;
        }

        requestIdRef.current += 1;
        hideLegacyModal();
        setState({
          ...defaultState(),
          pendingReturnOrder: {
            marketplace: current.marketplace,
            orderId: current.orderId,
          },
        });
        bookingsDetailsApi.openTransactionById(txRow.dataset.txId);
      }}
    >
      <OrderDetailContent detail={state.detail} loading={state.loading} error={state.error} />
    </div>,
    contentElement,
  );
}
