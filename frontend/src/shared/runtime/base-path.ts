function normalizeBasePath(value: unknown) {
  const text = String(value || "").trim();
  if (!text || text === "/") {
    return "";
  }

  const withLeadingSlash = text.startsWith("/") ? text : `/${text}`;
  return withLeadingSlash.endsWith("/")
    ? withLeadingSlash.slice(0, -1)
    : withLeadingSlash;
}

declare global {
  interface Window {
    __DASHBOARD_BASE_PATH__?: string;
  }
}

export function getDashboardBasePath() {
  if (typeof window === "undefined") {
    return "";
  }
  return normalizeBasePath(window.__DASHBOARD_BASE_PATH__);
}

export function stripDashboardBasePath(pathname: string) {
  const normalizedPathname = String(pathname || "").trim() || "/";
  const basePath = getDashboardBasePath();
  if (!basePath) {
    return normalizedPathname;
  }
  if (normalizedPathname === basePath) {
    return "/";
  }
  if (normalizedPathname.startsWith(`${basePath}/`)) {
    return normalizedPathname.slice(basePath.length) || "/";
  }
  return normalizedPathname;
}

export function withDashboardBasePath(path: string) {
  const normalizedPath = String(path || "").trim() || "/";
  const basePath = getDashboardBasePath();
  if (!basePath) {
    return normalizedPath;
  }
  if (normalizedPath === "/") {
    return basePath;
  }
  if (normalizedPath.startsWith(basePath + "/") || normalizedPath === basePath) {
    return normalizedPath;
  }
  return `${basePath}${normalizedPath.startsWith("/") ? normalizedPath : `/${normalizedPath}`}`;
}

export function buildDashboardApiUrl(path: string) {
  return withDashboardBasePath(path);
}
