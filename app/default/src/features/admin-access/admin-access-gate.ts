import { HTTP_UNAUTHORIZED } from "@/lib/constants";
import { redirectToLogin, resolveAPIPath } from "@/lib/api";
import { setOnSessionCleared } from "@/lib/auth";

export const GESTALT_ADMIN_PROBE_PATH = "/admin/api/v1/app-registries";

let grantedGestaltAdminAccess = false;

/** True after a successful Admin probe in this session. */
export function hasGrantedGestaltAdminAccess(): boolean {
  return grantedGestaltAdminAccess;
}

/** Clear the in-session Admin admission cache (logout, 401, or tests). */
export function resetGestaltAdminAccessCache(): void {
  grantedGestaltAdminAccess = false;
}

setOnSessionCleared(resetGestaltAdminAccessCache);

/**
 * True when the caller can use Gestalt-admin APIs (`gestaltAdmin`).
 * 401/403 and network failures are treated as no access.
 */
export async function probeGestaltAdminAccess(options?: {
  redirectOnUnauthorized?: boolean;
}): Promise<boolean> {
  if (grantedGestaltAdminAccess) return true;
  try {
    const response = await fetch(resolveAPIPath(GESTALT_ADMIN_PROBE_PATH), {
      credentials: "include",
      cache: "no-store",
      headers: { Accept: "application/json" },
    });
    if (response.status === HTTP_UNAUTHORIZED) {
      if (options?.redirectOnUnauthorized) {
        redirectToLogin();
      }
      return false;
    }
    grantedGestaltAdminAccess = response.ok;
    return grantedGestaltAdminAccess;
  } catch {
    return false;
  }
}

export function canShowAdminNav(gestaltAdmin: boolean | undefined): boolean {
  return gestaltAdmin === true;
}

export async function canAccessAdminRoute(): Promise<boolean> {
  return probeGestaltAdminAccess({ redirectOnUnauthorized: true });
}
