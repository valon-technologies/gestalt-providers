/** DOM id for an app operation row — dots encoded for CSS/hash safety. */
export function appOperationElementId(operationId: string): string {
  return `app-operation-${operationId.replace(/\./g, "--")}`;
}

/** App admin URL with Operations tab open on a specific method. */
export function appOperationAdminHref(
  appName: string,
  operationId: string,
): string {
  const params = new URLSearchParams({
    section: "operations",
    operation: operationId,
  });
  return `/apps/${encodeURIComponent(appName)}?${params.toString()}`;
}
