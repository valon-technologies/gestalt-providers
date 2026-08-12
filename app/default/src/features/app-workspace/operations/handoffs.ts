import { appOperationsPath } from "@/lib/appAdminPaths";

export const INVOKE_DOCS_PATH = "/docs/invoke" as const;
/** Grant app access docs (RBAC) — not Settings → API tokens. */
export const AUTHORIZATION_DOCS_PATH = "/docs/authorization" as const;
/** Docs subheading for granting app-level access (`Grant app access`). */
export const AUTHORIZATION_DOCS_GRANT_HASH = "authz-plugin-access" as const;
/** Docs subheading for creating and granting service accounts. */
export const AUTHORIZATION_DOCS_SERVICE_ACCOUNTS_HASH =
  "authz-service-accounts" as const;

/** Absolute path + hash for an operation deep link (inbound navigation / tests). */
export function operationDeepLinkPath(
  appName: string,
  operationId: string,
): string {
  return appOperationsPath(appName, operationId);
}

/** Canonical CLI handoff for invoking one operation. */
export function operationInvokeCliCommand(
  appName: string,
  operationId: string,
): string {
  return `gestalt apps invoke ${appName} ${operationId}`;
}
