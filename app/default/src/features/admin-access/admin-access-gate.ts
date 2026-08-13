import { canManageApp } from "@/lib/catalogFilters";
import { getIntegrations, type Integration } from "@/lib/api";
import { isLocalDevChrome } from "@/lib/local-dev-chrome";

/**
 * Admin nav and route: real app admins, plus local-dev / prod-remote chrome
 * while the surface is being built.
 */
export function canShowAdminNav(options: {
  localDevChrome: boolean;
  integrations?: Integration[] | null;
}): boolean {
  if (options.localDevChrome) return true;
  return (options.integrations ?? []).some(canManageApp);
}

export async function canAccessAdminRoute(): Promise<boolean> {
  if (isLocalDevChrome()) return true;
  try {
    const apps = await getIntegrations();
    return apps.some(canManageApp);
  } catch {
    return false;
  }
}
