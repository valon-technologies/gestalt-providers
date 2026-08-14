import { APP_METRICS_NAV_LABEL } from "./app-metrics-copy";
import { CONNECTION_SURFACE_NAV_LABEL } from "./connection-surface-copy";
import {
  SERVICE_ACCOUNTS_COPY,
  SERVICE_ACCOUNTS_LEGACY_PATH_SEGMENT,
  SERVICE_ACCOUNTS_ROUTE,
} from "./app-agent-identity-presentation";

export type AppUserNavId = "overview" | "connection" | "operations";

export type AppAdminNavId =
  | "versions"
  | "metrics"
  | "workflows"
  | "members"
  | "service-accounts";

export type AppAdminSurface = "registry" | "workflows" | "authorization";

export type WorkspaceNavId = AppUserNavId | AppAdminNavId;

export type WorkspaceLocation = {
  id: WorkspaceNavId;
  label: string;
  /** Nav catalog path template (still contains `$app`). */
  to: (typeof APP_USER_NAV)[number]["to"] | (typeof APP_ADMIN_NAV)[number]["to"];
  isOverview: boolean;
};

export const APP_USER_NAV = [
  {
    id: "overview" as const,
    label: "Overview",
    to: "/apps/$app" as const,
  },
  {
    id: "connection" as const,
    label: CONNECTION_SURFACE_NAV_LABEL,
    to: "/apps/$app/connection" as const,
    /** Shown only when {@link appShowsCredentialSurface} is true. */
    when: "hasCredentialSurface" as const,
  },
  {
    id: "operations" as const,
    label: "Operations",
    to: "/apps/$app/operations" as const,
  },
] as const;

/** Admin surface required for app workspace admin routes (including `/versions`). */
export function adminSurfaceForPathname(
  pathname: string,
  app: string,
): AppAdminSurface | null {
  const versionsPath = `/apps/${app}/versions`;
  if (pathname === versionsPath || pathname.startsWith(`${versionsPath}/`)) {
    return "registry";
  }
  const metricsPath = `/apps/${app}/metrics`;
  if (pathname === metricsPath || pathname.startsWith(`${metricsPath}/`)) {
    return "authorization";
  }

  const base = `/apps/${app}/admin`;
  if (!pathname.includes(base)) return null;
  if (pathname.includes(`${base}/snapshots`)) return "registry";
  if (pathname.includes(`${base}/history`)) return "registry";
  if (pathname.includes(`${base}/workflows`)) return "workflows";
  if (pathname.includes(`${base}/members`)) return "authorization";
  if (pathname.includes(`${base}/service-accounts`)) return "authorization";
  if (
    pathname.includes(`${base}/${SERVICE_ACCOUNTS_LEGACY_PATH_SEGMENT}`)
  ) {
    return "authorization";
  }
  return "registry";
}

/**
 * Fleet runtime status belongs on the Versions (registry inventory) surface only —
 * not Workflows, Members, or other admin chrome.
 */
export function isAppVersionsAdminPath(pathname: string, app: string): boolean {
  const versionsPath = `/apps/${app}/versions`;
  if (pathname === versionsPath || pathname.startsWith(`${versionsPath}/`)) {
    return true;
  }
  const base = `/apps/${app}/admin`;
  return (
    pathname.includes(`${base}/snapshots`) ||
    pathname.includes(`${base}/history`)
  );
}

export function isAppMetricsPath(pathname: string, app: string): boolean {
  const metricsPath = `/apps/${app}/metrics`;
  return pathname === metricsPath || pathname.startsWith(`${metricsPath}/`);
}

export function isAppAdminChromePath(pathname: string, app: string): boolean {
  return (
    pathname.includes(`/apps/${app}/admin`) ||
    isAppVersionsAdminPath(pathname, app) ||
    isAppMetricsPath(pathname, app)
  );
}

export const APP_ADMIN_NAV = [
  {
    id: "versions" as const,
    label: "Versions",
    to: "/apps/$app/versions" as const,
    requires: "registry" as const satisfies AppAdminSurface,
  },
  {
    id: "metrics" as const,
    label: APP_METRICS_NAV_LABEL,
    to: "/apps/$app/metrics" as const,
    requires: "authorization" as const satisfies AppAdminSurface,
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
    id: "service-accounts" as const,
    label: SERVICE_ACCOUNTS_COPY.navLabel,
    to: SERVICE_ACCOUNTS_ROUTE,
    requires: "authorization" as const satisfies AppAdminSurface,
  },
] as const;

function navPathForApp(to: string, app: string): string {
  return to.replace("$app", app);
}

/**
 * Resolve the workspace rail destination for a pathname.
 * Longest matching nav path wins so nested version routes stay under Versions.
 */
export function workspaceLocationForPathname(
  pathname: string,
  app: string,
): WorkspaceLocation {
  const overview = APP_USER_NAV[0];
  const overviewPath = navPathForApp(overview.to, app);
  const normalized =
    pathname.length > 1 && pathname.endsWith("/")
      ? pathname.slice(0, -1)
      : pathname;

  const candidates = [...APP_USER_NAV, ...APP_ADMIN_NAV]
    .filter((item) => item.id !== "overview")
    .map((item) => ({
      item,
      path: navPathForApp(item.to, app),
    }))
    .sort((left, right) => right.path.length - left.path.length);

  for (const { item, path } of candidates) {
    if (normalized === path || normalized.startsWith(`${path}/`)) {
      return {
        id: item.id,
        label: item.label,
        to: item.to,
        isOverview: false,
      };
    }
  }

  if (normalized === overviewPath || normalized.startsWith(`${overviewPath}/`)) {
    return {
      id: overview.id,
      label: overview.label,
      to: overview.to,
      isOverview: true,
    };
  }

  return {
    id: overview.id,
    label: overview.label,
    to: overview.to,
    isOverview: true,
  };
}

/** Browser/tab title segment before the product suffix (`· Gestalt`). */
export function workspaceDocumentTitle(
  appLabel: string,
  location: WorkspaceLocation,
  opts?: { pathname?: string; app?: string },
): string {
  if (location.isOverview) return appLabel;
  const page = workflowAdminPageLabel(opts?.pathname, opts?.app);
  if (page) return `${page} · ${location.label} · ${appLabel}`;
  return `${location.label} · ${appLabel}`;
}

export type WorkflowAdminBreadcrumbSegment = {
  label: string;
  /** When set, the segment is a link (not the current page). */
  link?:
    | {
        to: "/apps/$app/admin/workflows/runs/$runId";
        params: { app: string; runId: string };
      }
    | {
        to: "/apps/$app/admin/workflows/definitions";
        params: { app: string };
      }
    | {
        to: "/apps/$app/admin/workflows/definitions/$definitionId";
        params: { app: string; definitionId: string };
      };
};

function shortWorkflowCrumbId(id: string): string {
  if (id.length <= 28) return id;
  return `${id.slice(0, 12)}…${id.slice(-8)}`;
}

/**
 * Nested crumbs under Workflows (Apps → App → Workflows → …).
 * Returns null on the runs landing page (chrome stays "Workflows").
 */
export function workflowAdminBreadcrumbTrail(
  pathname: string | undefined,
  app: string | undefined,
): WorkflowAdminBreadcrumbSegment[] | null {
  if (!pathname || !app) return null;
  const base = `/apps/${app}/admin/workflows`;
  const normalized =
    pathname.length > 1 && pathname.endsWith("/")
      ? pathname.slice(0, -1)
      : pathname;
  if (normalized === base) return null;

  if (normalized.startsWith(`${base}/runs/`)) {
    const rest = normalized.slice(`${base}/runs/`.length);
    const parts = rest.split("/").filter(Boolean);
    const runId = parts[0];
    if (!runId) return [{ label: "Run" }];
    const stepsIdx = parts.indexOf("steps");
    const stepId = stepsIdx >= 0 ? parts[stepsIdx + 1] : undefined;
    if (stepId) {
      return [
        {
          label: shortWorkflowCrumbId(runId),
          link: {
            to: "/apps/$app/admin/workflows/runs/$runId",
            params: { app, runId },
          },
        },
        { label: shortWorkflowCrumbId(stepId) },
      ];
    }
    return [{ label: shortWorkflowCrumbId(runId) }];
  }

  if (normalized === `${base}/definitions`) {
    return [{ label: "Definitions" }];
  }
  if (normalized.startsWith(`${base}/definitions/`)) {
    const definitionId = normalized.slice(`${base}/definitions/`.length);
    if (!definitionId) return [{ label: "Definition" }];
    return [{ label: shortWorkflowCrumbId(definitionId) }];
  }
  return null;
}

/**
 * Nested label under Workflows for list/detail IA routes.
 * Returns null on the runs landing page (chrome stays "Workflows").
 */
export function workflowAdminPageLabel(
  pathname: string | undefined,
  app: string | undefined,
): string | null {
  const trail = workflowAdminBreadcrumbTrail(pathname, app);
  if (!trail || trail.length === 0) return null;
  return trail[trail.length - 1]?.label ?? null;
}
