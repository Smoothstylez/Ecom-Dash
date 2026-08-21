import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";

import { useDashboardRuntime } from "@/app/dashboard-runtime";

type PreviewKind = "image" | "pdf";

type PreviewState = {
  isOpen: boolean;
  url: string;
  filename: string;
  mimeType: string;
  kind: PreviewKind | "";
  zoom: number;
};

const PREVIEW_ZOOM_STEP = 0.25;
const PREVIEW_ZOOM_MIN = 0.25;
const PREVIEW_ZOOM_MAX = 5;

function defaultPreviewState(): PreviewState {
  return {
    isOpen: false,
    url: "",
    filename: "",
    mimeType: "",
    kind: "",
    zoom: 1,
  };
}

function clampPreviewZoom(value: number) {
  return Math.min(PREVIEW_ZOOM_MAX, Math.max(PREVIEW_ZOOM_MIN, value));
}

function inferMimeTypeFromFilename(filename?: string) {
  const name = String(filename || "").trim().toLowerCase();
  if (!name) {
    return "";
  }
  if (name.endsWith(".pdf")) {
    return "application/pdf";
  }
  if (name.endsWith(".png")) {
    return "image/png";
  }
  if (name.endsWith(".jpg") || name.endsWith(".jpeg")) {
    return "image/jpeg";
  }
  if (name.endsWith(".webp")) {
    return "image/webp";
  }
  if (name.endsWith(".gif")) {
    return "image/gif";
  }
  if (name.endsWith(".bmp")) {
    return "image/bmp";
  }
  return "";
}

function detectPreviewKind(mimeType?: string, filename?: string): PreviewKind | "" {
  const mime = String(mimeType || "").trim().toLowerCase();
  if (mime.startsWith("image/")) {
    return "image";
  }
  if (mime.includes("pdf")) {
    return "pdf";
  }

  const inferred = inferMimeTypeFromFilename(filename);
  if (inferred.startsWith("image/")) {
    return "image";
  }
  if (inferred.includes("pdf")) {
    return "pdf";
  }
  return "";
}

function appendInlineDisposition(url: string) {
  const [baseUrl, hash = ""] = String(url || "").split("#", 2);
  const joiner = baseUrl.includes("?") ? "&" : "?";
  return `${baseUrl}${joiner}disposition=inline${hash ? `#${hash}` : ""}`;
}

function setStatusBoxMessage(message: string, level: "info" | "ok" | "error" = "info") {
  const statusBox = document.getElementById("statusBox");
  if (!(statusBox instanceof HTMLElement)) {
    return;
  }
  const className = level === "error" ? "status-error" : (level === "ok" ? "status-ok" : "status-info");
  statusBox.className = `status ${className}`;
  statusBox.textContent = message;
}

export function DashboardSharedModals() {
  const {
    amazonDetailsApi,
    bookingsDetailsApi,
    orderDetailsApi,
    registerDetailsModalApi,
    registerPreviewModalApi,
  } = useDashboardRuntime();
  const [detailsModalState, setDetailsModalState] = useState({
    isOpen: false,
    title: "Order Details",
  });
  const [preview, setPreview] = useState<PreviewState>(() => defaultPreviewState());
  const detailsModalElementRef = useRef<HTMLDivElement | null>(null);
  const closeDetailsButtonRef = useRef<HTMLButtonElement | null>(null);
  const detailsModalRef = useRef(detailsModalState);
  const previewRef = useRef(preview);
  const previewBodyRef = useRef<HTMLDivElement | null>(null);

  detailsModalRef.current = detailsModalState;
  previewRef.current = preview;

  const setDetailsTitle = useCallback((title?: string) => {
    const nextTitle = String(title || "").trim() || "Order Details";
    setDetailsModalState((current) => {
      if (current.title === nextTitle) {
        return current;
      }
      return {
        ...current,
        title: nextTitle,
      };
    });
  }, []);

  const openDetailsModal = useCallback((title?: string) => {
    const nextTitle = String(title || "").trim() || detailsModalRef.current.title || "Order Details";
    setDetailsModalState({
      isOpen: true,
      title: nextTitle,
    });
  }, []);

  const closeDetailsModal = useCallback(() => {
    setDetailsModalState((current) => {
      if (!current.isOpen) {
        return current;
      }
      return {
        ...current,
        isOpen: false,
      };
    });
  }, []);

  const delegateDetailsClose = useCallback(() => {
    if (orderDetailsApi?.isOpen()) {
      orderDetailsApi.close();
      return;
    }

    if (bookingsDetailsApi?.isOpen()) {
      bookingsDetailsApi.close();
      return;
    }

    if (amazonDetailsApi?.isOpen()) {
      amazonDetailsApi.close();
      return;
    }

    closeDetailsModal();
  }, [amazonDetailsApi, bookingsDetailsApi, closeDetailsModal, orderDetailsApi]);

  const closePreview = useCallback(() => {
    setPreview(defaultPreviewState());
  }, []);

  const openPreview = useCallback((url?: string, filename?: string, mimeType?: string) => {
    const safeUrl = String(url || "").trim();
    if (!safeUrl) {
      setStatusBoxMessage("Preview-Link fehlt.", "error");
      return;
    }

    const kind = detectPreviewKind(mimeType, filename);
    if (!kind) {
      window.open(safeUrl, "_blank", "noopener,noreferrer");
      return;
    }

    const safeName = String(filename || "Beleg").trim() || "Beleg";
    const resolvedMimeType = String(mimeType || inferMimeTypeFromFilename(filename) || "-").trim() || "-";
    setPreview({
      isOpen: true,
      url: safeUrl,
      filename: safeName,
      mimeType: resolvedMimeType,
      kind,
      zoom: 1,
    });
  }, []);

  const adjustPreviewZoom = useCallback((delta: number) => {
    setPreview((current) => {
      if (!current.isOpen) {
        return current;
      }
      return {
        ...current,
        zoom: clampPreviewZoom(current.zoom + delta),
      };
    });
  }, []);

  const resetPreviewZoom = useCallback(() => {
    setPreview((current) => {
      if (!current.isOpen || current.zoom === 1) {
        return current;
      }
      return {
        ...current,
        zoom: 1,
      };
    });
  }, []);

  useEffect(() => {
    registerDetailsModalApi({
      open: openDetailsModal,
      close: closeDetailsModal,
      isOpen: () => detailsModalRef.current.isOpen,
      getTitle: () => detailsModalRef.current.title,
      setTitle: setDetailsTitle,
    });
    registerPreviewModalApi({
      open: openPreview,
      close: closePreview,
      isOpen: () => previewRef.current.isOpen,
    });

    return () => {
      registerDetailsModalApi(null);
      registerPreviewModalApi(null);
    };
  }, [closeDetailsModal, closePreview, openDetailsModal, openPreview, registerDetailsModalApi, registerPreviewModalApi, setDetailsTitle]);

  useLayoutEffect(() => {
    const previewBody = previewBodyRef.current;
    if (!(previewBody instanceof HTMLElement)) {
      return;
    }

    const handleWheel = (event: WheelEvent) => {
      const current = previewRef.current;
      if (!current.isOpen || current.kind !== "image") {
        return;
      }

      const image = previewBody.querySelector(".preview-image");
      if (!(image instanceof HTMLElement)) {
        return;
      }

      event.preventDefault();
      const delta = event.deltaY < 0 ? PREVIEW_ZOOM_STEP : -PREVIEW_ZOOM_STEP;
      setPreview((previewState) => {
        if (!previewState.isOpen || previewState.kind !== "image") {
          return previewState;
        }
        return {
          ...previewState,
          zoom: clampPreviewZoom(previewState.zoom + delta),
        };
      });
    };

    previewBody.addEventListener("wheel", handleWheel, { passive: false });
    return () => {
      previewBody.removeEventListener("wheel", handleWheel);
    };
  }, []);

  const previewTitle = preview.isOpen ? `Preview: ${preview.filename}` : "Beleg Preview";
  const previewMeta = preview.isOpen ? `${preview.filename} | ${preview.mimeType}` : "-";
  const previewImageUrl = preview.kind === "image" ? appendInlineDisposition(preview.url) : "";
  const previewPdfUrl = preview.kind === "pdf" ? `${appendInlineDisposition(preview.url)}#toolbar=1&view=FitH` : "";
  const previewImageStyle = preview.kind === "image"
    ? {
        transform: `scale(${preview.zoom})`,
        maxWidth: preview.zoom > 1 ? "none" : "100%",
        maxHeight: preview.zoom > 1 ? "none" : "66vh",
      }
    : undefined;

  return (
    <>
      <div
        id="detailsModal"
        ref={detailsModalElementRef}
        className={detailsModalState.isOpen ? "modal active" : "modal"}
        role="dialog"
        aria-modal="true"
        aria-hidden={detailsModalState.isOpen ? "false" : "true"}
        data-react-owned="true"
      >
        <div className="modal-card">
          <div className="modal-head">
            <div id="detailsTitle" className="modal-title">{detailsModalState.title}</div>
             <button
               id="closeModalBtn"
               ref={closeDetailsButtonRef}
               className="btn-inline"
               type="button"
               onClick={() => {
                 delegateDetailsClose();
               }}
             >
               Schliessen
             </button>
          </div>
          <div id="detailsContent" className="detail-content" />
        </div>
      </div>

      <div
        id="previewModal"
        className={preview.isOpen ? "modal active" : "modal"}
        role="dialog"
        aria-modal="true"
        aria-hidden={preview.isOpen ? "false" : "true"}
        data-react-owned="true"
        onClick={(event) => {
          if (event.target === event.currentTarget) {
            closePreview();
          }
        }}
      >
        <div className="modal-card preview-modal-card">
          <div className="modal-head">
            <div>
              <div id="previewTitle" className="modal-title">{previewTitle}</div>
              <div id="previewMeta" className="preview-meta">{previewMeta}</div>
            </div>
            <div className="preview-controls">
              <button
                id="previewZoomOut"
                className="btn-inline"
                type="button"
                title="Verkleinern"
                onClick={() => {
                  adjustPreviewZoom(-PREVIEW_ZOOM_STEP);
                }}
              >
                -
              </button>
              <span id="previewZoomLevel" className="preview-zoom-label">{Math.round(preview.zoom * 100)}%</span>
              <button
                id="previewZoomIn"
                className="btn-inline"
                type="button"
                title="Vergroessern"
                onClick={() => {
                  adjustPreviewZoom(PREVIEW_ZOOM_STEP);
                }}
              >
                +
              </button>
              <button
                id="previewZoomReset"
                className="btn-inline"
                type="button"
                title="Zuruecksetzen"
                onClick={() => {
                  resetPreviewZoom();
                }}
              >
                1:1
              </button>
              <button
                id="closePreviewBtn"
                className="btn-inline"
                type="button"
                onClick={() => {
                  closePreview();
                }}
              >
                Schliessen
              </button>
            </div>
          </div>
          <div id="previewBody" ref={previewBodyRef} className="preview-shell">
            {preview.isOpen && preview.kind === "image" ? (
              <img
                className="preview-image"
                src={previewImageUrl}
                alt={preview.filename}
                style={previewImageStyle}
              />
            ) : null}
            {preview.isOpen && preview.kind === "pdf" ? (
              <iframe className="preview-frame" src={previewPdfUrl} title={preview.filename} />
            ) : null}
            {!preview.isOpen ? "-" : null}
          </div>
        </div>
      </div>
    </>
  );
}
