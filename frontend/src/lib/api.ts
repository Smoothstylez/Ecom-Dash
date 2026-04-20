export async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    headers: {
      Accept: "application/json",
      ...(init?.headers ?? {}),
    },
    ...init,
  });

  const payload = (await response.json().catch(() => ({}))) as {
    detail?: unknown;
    error?: unknown;
  };

  if (!response.ok) {
    const fallback = `HTTP ${response.status}`;
    const message =
      typeof payload.detail === "string"
        ? payload.detail
        : typeof payload.error === "string"
          ? payload.error
          : fallback;
    throw new Error(message);
  }

  return payload as T;
}

export function buildDashboardQuery(params: Record<string, string | undefined>) {
  const searchParams = new URLSearchParams();

  for (const [key, value] of Object.entries(params)) {
    if (value) {
      searchParams.set(key, value);
    }
  }

  return searchParams.toString();
}
