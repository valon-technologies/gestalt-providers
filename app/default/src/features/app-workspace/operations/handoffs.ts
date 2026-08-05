import { appOperationsPath } from "@/lib/appAdminPaths";

export const INVOKE_DOCS_PATH = "/docs/invoke" as const;
export const AUTHORIZATION_DOCS_PATH = "/docs/authorization" as const;
/** Grant access section — app admin Members handoff. */
export const AUTHORIZATION_DOCS_GRANT_HASH = "authz-grant-app-access" as const;
/** Operation roles section — app admin Operations "Roles & access" handoff. */
export const AUTHORIZATION_DOCS_OPERATION_ROLES_HASH =
  "authz-operation-roles" as const;

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
