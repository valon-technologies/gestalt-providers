export type AppUserNavId = "overview" | "connection" | "operations";

export type AppAdminNavId =
  | "snapshots"
  | "history"
  | "workflows"
  | "members"
  | "agent-identities";

export type AppAdminSurface = "registry" | "workflows" | "authorization";

export const APP_USER_NAV = [
  {
    id: "overview" as const,
    label: "Overview",
    to: "/apps/$app" as const,
  },
  {
    id: "connection" as const,
    label: "Connection",
    to: "/apps/$app/connection" as const,
    when: "hasConnection" as const,
  },
  {
    id: "operations" as const,
    label: "Operations",
    to: "/apps/$app/operations" as const,
  },
] as const;

/** Admin surface required for a pathname under `/apps/:app/admin/*`. */
export function adminSurfaceForPathname(
  pathname: string,
  app: string,
): AppAdminSurface | null {
  const base = `/apps/${app}/admin`;
  if (!pathname.includes(base)) return null;
  if (pathname.includes(`${base}/snapshots`)) return "registry";
  if (pathname.includes(`${base}/history`)) return "registry";
  if (pathname.includes(`${base}/workflows`)) return "workflows";
  if (pathname.includes(`${base}/members`)) return "authorization";
  if (pathname.includes(`${base}/agent-identities`)) return "authorization";
  return "registry";
}

export const APP_ADMIN_NAV = [
  {
    id: "snapshots" as const,
    label: "Versions",
    to: "/apps/$app/admin/snapshots" as const,
    requires: "registry" as const satisfies AppAdminSurface,
  },
  {
    id: "history" as const,
    label: "Version history",
    to: "/apps/$app/admin/history" as const,
    requires: "registry" as const satisfies AppAdminSurface,
  },
  {
    id: "workflows" as const,
    label: "Workflows",
    to: "/apps/$app/admin/workflows" as const,
    requires: "workflows" as const satisfies AppAdminSurface,
  },
  {
    id: "members" as const,
    label: "Members",
    to: "/apps/$app/admin/members" as const,
    requires: "authorization" as const satisfies AppAdminSurface,
  },
  {
    id: "agent-identities" as const,
    label: "Agent identities",
    to: "/apps/$app/admin/agent-identities" as const,
    requires: "authorization" as const satisfies AppAdminSurface,
  },
] as const;
