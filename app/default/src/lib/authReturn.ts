const DEFAULT_RETURN_PATH = "/";
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
  if (typeof window === "undefined") {
    return DEFAULT_RETURN_PATH;
  }
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
