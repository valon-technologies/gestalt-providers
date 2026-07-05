import { AUTH_RETURN_PATH_STORAGE_KEY, LOGIN_PATH } from "@/lib/constants";

const DEFAULT_RETURN_PATH = "/";
const LOGIN_CALLBACK_PATH = "/api/v1/auth/login/callback";
const API_PATH = "/api";

function isBlockedPath(pathname: string): boolean {
  return (
    pathname === LOGIN_PATH ||
    pathname.startsWith(`${LOGIN_PATH}/`) ||
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
  if (typeof window === "undefined") {
    return DEFAULT_RETURN_PATH;
  }
  return sanitizeAuthReturnPath(
    `${window.location.pathname}${window.location.search}${window.location.hash}`,
  );
}

export function authReturnPathFromLoginURL(): string {
  if (typeof window === "undefined") {
    return DEFAULT_RETURN_PATH;
  }
  return sanitizeAuthReturnPath(
    new URLSearchParams(window.location.search).get("next"),
  );
}

export function loginPathForReturnPath(returnPath: string): string {
  return `${LOGIN_PATH}?${new URLSearchParams({
    next: sanitizeAuthReturnPath(returnPath),
  })}`;
}

export function loginPathForCurrentLocation(): string {
  return loginPathForReturnPath(currentAuthReturnPath());
}

export function storeAuthReturnPath(returnPath: string): string {
  const sanitized = sanitizeAuthReturnPath(returnPath);
  if (typeof window !== "undefined") {
    window.sessionStorage.setItem(AUTH_RETURN_PATH_STORAGE_KEY, sanitized);
  }
  return sanitized;
}

export function storedAuthReturnPath(): string {
  if (typeof window === "undefined") {
    return DEFAULT_RETURN_PATH;
  }
  return sanitizeAuthReturnPath(
    window.sessionStorage.getItem(AUTH_RETURN_PATH_STORAGE_KEY),
  );
}

export function clearStoredAuthReturnPath(): void {
  if (typeof window === "undefined") {
    return;
  }
  window.sessionStorage.removeItem(AUTH_RETURN_PATH_STORAGE_KEY);
}
