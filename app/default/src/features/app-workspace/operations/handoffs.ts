import { appOperationsPath } from "@/lib/appAdminPaths";

export const INVOKE_DOCS_PATH = "/docs/invoke" as const;
export const AUTHORIZATION_DOCS_PATH = "/docs/authorization" as const;

/** Absolute path + hash for sharing an operation deep link. */
export function operationDeepLinkPath(
  appName: string,
  operationId: string,
): string {
  return appOperationsPath(appName, operationId);
}

/** Absolute URL for the current origin (browser-only). */
export function operationDeepLinkHref(
  appName: string,
  operationId: string,
  origin: string = typeof window !== "undefined" ? window.location.origin : "",
): string {
  const path = operationDeepLinkPath(appName, operationId);
  return origin ? `${origin}${path}` : path;
}

/** Canonical CLI handoff for invoking one operation. */
export function operationInvokeCliCommand(
  appName: string,
  operationId: string,
): string {
  return `gestalt apps invoke ${appName} ${operationId}`;
}
