import {
  ADMIN_METRICS_NAV_LABEL,
  APP_ACCESS_NAV_LABEL,
  APP_VERSIONS_NAV_LABEL,
  PLATFORM_ADMINS_NAV_LABEL,
} from "./admin-access-copy";

export const ADMIN_NAV = [
  {
    id: "who-can-use" as const,
    label: APP_ACCESS_NAV_LABEL,
    to: "/admin" as const,
  },
  {
    id: "platform-admins" as const,
    label: PLATFORM_ADMINS_NAV_LABEL,
    to: "/admin/platform-admins" as const,
  },
  {
    id: "versions" as const,
    label: APP_VERSIONS_NAV_LABEL,
    to: "/admin/versions" as const,
  },
  {
    id: "metrics" as const,
    label: ADMIN_METRICS_NAV_LABEL,
    to: "/admin/metrics" as const,
  },
] as const;

export type AdminNavId = (typeof ADMIN_NAV)[number]["id"];

export function adminNavIdForPathname(pathname: string): AdminNavId {
  const normalized =
    pathname.length > 1 && pathname.endsWith("/")
      ? pathname.slice(0, -1)
      : pathname;
  if (
    normalized === "/admin/platform-admins" ||
    normalized.startsWith("/admin/platform-admins/")
  ) {
    return "platform-admins";
  }
  if (
    normalized === "/admin/versions" ||
    normalized.startsWith("/admin/versions/")
  ) {
    return "versions";
  }
  if (
    normalized === "/admin/metrics" ||
    normalized.startsWith("/admin/metrics/")
  ) {
    return "metrics";
  }
  return "who-can-use";
}
