export const ADMIN_TOKEN_STORAGE_KEY = "dash-combined.admin-token";

export function loadAdminToken() {
  if (typeof window === "undefined") {
    return "";
  }
  try {
    return String(window.localStorage.getItem(ADMIN_TOKEN_STORAGE_KEY) || "").trim();
  } catch (_error) {
    return "";
  }
}

export function persistAdminToken(token: string) {
  if (typeof window === "undefined") {
    return;
  }
  try {
    const normalized = String(token || "").trim();
    if (normalized) {
      window.localStorage.setItem(ADMIN_TOKEN_STORAGE_KEY, normalized);
    } else {
      window.localStorage.removeItem(ADMIN_TOKEN_STORAGE_KEY);
    }
  } catch (_error) {
    // Ignore localStorage failures.
  }
}

export function buildAdminHeaders(headers?: HeadersInit) {
  const nextHeaders = new Headers(headers || {});
  const token = loadAdminToken();
  if (token) {
    nextHeaders.set("X-Admin-Token", token);
  }
  return nextHeaders;
}

export function withAdminHeaders(init?: RequestInit): RequestInit {
  return {
    ...init,
    headers: buildAdminHeaders(init?.headers),
  };
}
