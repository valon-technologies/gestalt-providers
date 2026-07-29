/** DOM id for an app operation row — dots encoded for CSS/hash safety. */
export function appOperationElementId(operationId: string): string {
  return `app-operation-${operationId.replace(/\./g, "--")}`;
}

/** App operations route with optional deep-link hash. */
export function appOperationAdminHref(
  appName: string,
  operationId: string,
): string {
  return `/apps/${encodeURIComponent(appName)}/operations#${encodeURIComponent(operationId)}`;
}

export function appOperationsPath(appName: string, operationId?: string): string {
  const base = `/apps/${encodeURIComponent(appName)}/operations`;
  return operationId ? `${base}#${encodeURIComponent(operationId)}` : base;
}
