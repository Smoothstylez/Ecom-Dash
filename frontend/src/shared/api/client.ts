import { withAdminHeaders } from "@/shared/api/admin-auth";

export interface FetchOptions extends RequestInit {
  signal?: AbortSignal;
  timeoutMs?: number;
}

export async function fetchJson<T>(url: string, options?: FetchOptions): Promise<T> {
  const { signal, timeoutMs, ...init } = options ?? {};
  const controller = signal ? undefined : new AbortController();
  const effectiveSignal = signal ?? controller?.signal;

  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  if (timeoutMs && timeoutMs > 0) {
    timeoutId = setTimeout(() => controller?.abort(), timeoutMs);
  }

  try {
    const response = await fetch(url, withAdminHeaders({
      ...init,
      signal: effectiveSignal,
      headers: {
        Accept: "application/json",
        ...(init?.headers ?? {}),
      },
    }));

    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      const detail = payload && typeof payload === "object" ? payload.detail : "";
      const message = typeof detail === "string" && detail.trim()
        ? detail
        : `Request failed: ${response.status}`;
      throw new Error(message);
    }

    return (await response.json()) as T;
  } finally {
    if (timeoutId !== undefined) {
      clearTimeout(timeoutId);
    }
  }
}
