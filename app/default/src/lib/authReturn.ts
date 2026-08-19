import { CONNECTION_RETURN_PATH_STORAGE_KEY } from "./constants";
import { appPath } from "./mount";

const DEFAULT_RETURN_PATH = appPath("/apps");
const LOGIN_CALLBACK_PATH = "/api/v1/auth/login/callback";
const AUTH_LOGIN_PATH = "/api/v1/auth/login";
const API_PATH = "/api";

function isBlockedPath(pathname: string): boolean {
  return (
    pathname === AUTH_LOGIN_PATH ||
    pathname.startsWith(`${AUTH_LOGIN_PATH}/`) ||
    pathname === LOGIN_CALLBACK_PATH ||
    pathname.startsWith(`${LOGIN_CALLBACK_PATH}/`) ||
    pathname === API_PATH ||
    pathname.startsWith(`${API_PATH}/`)
  );
}

export function sanitizeAuthReturnPath(raw: string | null | undefined): string {
  const value = raw?.trim();
  if (
    !value ||
    !value.startsWith("/") ||
    value.startsWith("//") ||
    value.includes("\\")
  ) {
    return DEFAULT_RETURN_PATH;
  }

  try {
    const url = new URL(value, "http://gestalt.local");
    if (
      url.origin !== "http://gestalt.local" ||
      !url.pathname.startsWith("/") ||
      isBlockedPath(url.pathname)
    ) {
      return DEFAULT_RETURN_PATH;
    }
    return `${url.pathname}${url.search}${url.hash}`;
  } catch {
    return DEFAULT_RETURN_PATH;
  }
}

export function currentAuthReturnPath(): string {
  return sanitizeAuthReturnPath(
    `${window.location.pathname}${window.location.search}${window.location.hash}`,
  );
}

export function serverLoginURL(returnPath?: string): string {
  const next = encodeURIComponent(
    sanitizeAuthReturnPath(returnPath ?? currentAuthReturnPath()),
  );
  return `${AUTH_LOGIN_PATH}?next=${next}`;
}

/**
 * Where to send the browser after an app OAuth round-trip.
 * The catalog currently lands on `/apps?connected=…`; this stored path is how
 * Setup (and any other connect surface) gets people back to the page they left.
 */
export function rememberConnectionReturnPath(path?: string): void {
  if (typeof window === "undefined") return;
  window.sessionStorage.setItem(
    CONNECTION_RETURN_PATH_STORAGE_KEY,
    sanitizeAuthReturnPath(path ?? currentAuthReturnPath()),
  );
}

export function consumeConnectionReturnPath(): string | null {
  if (typeof window === "undefined") return null;
  const raw = window.sessionStorage.getItem(CONNECTION_RETURN_PATH_STORAGE_KEY);
  window.sessionStorage.removeItem(CONNECTION_RETURN_PATH_STORAGE_KEY);
  if (!raw) return null;
  return sanitizeAuthReturnPath(raw);
}

export function clearConnectionReturnPath(): void {
  if (typeof window === "undefined") return;
  window.sessionStorage.removeItem(CONNECTION_RETURN_PATH_STORAGE_KEY);
}

/** Full in-app href to leave for, or null when we should stay on this page. */
export function connectionReturnRedirectHref(
  storedPath: string | null,
  currentPathname: string,
): string | null {
  if (!storedPath) return null;
  const url = new URL(sanitizeAuthReturnPath(storedPath), "http://gestalt.local");
  if (url.pathname === currentPathname) return null;
  return `${url.pathname}${url.search}${url.hash}`;
}
