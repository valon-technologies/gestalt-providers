import type { Integration } from "@/lib/api";
import {
  hasCredentialSurface,
  integrationNeedsReconnect,
  normalizeIntegrationStatus,
  type ConnectionContext,
  type NormalizedIntegrationStatus,
  type StatusTone,
} from "@/lib/integrationStatus";
import { getIntegrationLabel, matchesSearchQuery } from "@/lib/integrationSearch";

/**
 * Exclusive setup partitions for the Apps catalog.
 * Every integration maps to exactly one bucket — filters never overlap.
 */
export type ConnectionSetupBucket =
  | "needs_connection"
  | "ready"
  | "needs_attention";

/** Connection setup partitions for sort / attention — not UI filter tabs. */
export type ConnectionFilter =
  | "all"
  | "needs_connection"
  | "needs_attention"
  | "ready";

/**
 * Catalog presentation state — encodes install chrome and card navigation.
 * Distinct from `connectionSetupBucket`: mount-only apps have no credential
 * surface but still open their mounted UI from discovery.
 */
export type CatalogInstallState =
  | "not_connected"
  | "needs_attention"
  | "connected"
  | "mount_only";

/** Mounted product UI with zero connection rows — discovery, not Installed. */
export function isMountOnlyIntegration(integration: Integration): boolean {
  const hasMount = Boolean(integration.mountedPath?.trim());
  const connections = integration.connections ?? [];
  return hasMount && connections.length === 0;
}

export function catalogInstallState(
  integration: Integration,
  context: ConnectionContext = "current_user",
): CatalogInstallState {
  if (isMountOnlyIntegration(integration)) {
    return "mount_only";
  }
  const bucket = connectionSetupBucket(integration, context);
  if (bucket === "needs_connection") return "not_connected";
  if (bucket === "needs_attention") return "needs_attention";
  // Ready-to-use is not product-connected. Mode-none and shared MCP
  // passthrough stay in discovery until this identity has a chosen account.
  if (normalizeIntegrationStatus(integration, context).connected) {
    return "connected";
  }
  return "not_connected";
}

export type SurfaceFilter = "all" | "has_ui" | "no_ui" | "has_mcp";

export type AppSurfaces = {
  hasUi: boolean;
  hasMcp: boolean;
};

/**
 * User-facing labels for app surfaces — one term per surface across catalog
 * facets, cards, and Overview chrome. Never shorten `webApp` to "App".
 */
export const APP_SURFACE_LABELS = {
  webApp: "Web App",
  mcp: "Works with AI",
  api: "API",
} as const;

/** Surfaces from structured connection / mount fields only. */
export function getAppSurfaces(integration: Integration): AppSurfaces {
  const hasUi = Boolean(integration.mountedPath?.trim());
  const hasMcp = Boolean(
    integration.connections?.some(
      (connection) =>
        connection.mcpPassthrough ||
        (connection.authTypes || []).some(
          (auth) => auth.toLowerCase() === "mcp",
        ) ||
        /^mcp$/i.test(connection.name || "") ||
        /^mcp$/i.test(connection.displayName || ""),
    ),
  );
  return { hasUi, hasMcp };
}

export function badgeVariantFromTone(
  tone: StatusTone,
): "success" | "warning" | "destructive" | "muted" {
  switch (tone) {
    case "success":
      return "success";
    case "warning":
      return "warning";
    case "danger":
      return "destructive";
    case "neutral":
      return "muted";
  }
}

/** Map status tone onto Registry `Alert` variants (inline notice). */
export function alertVariantFromTone(
  tone: StatusTone,
): "success" | "warning" | "destructive" | "default" {
  switch (tone) {
    case "success":
      return "success";
    case "warning":
      return "warning";
    case "danger":
      return "destructive";
    case "neutral":
      return "default";
  }
}

/** Map status tone onto Registry `OutcomeStatusIndicator` statuses. */
function outcomeStatusFromTone(
  tone: StatusTone,
): "success" | "failure" | "warning" | "unknown" {
  switch (tone) {
    case "success":
      return "success";
    case "warning":
      return "warning";
    case "danger":
      return "failure";
    case "neutral":
      return "unknown";
  }
}

/**
 * Overview Your-access Status glyph — connection domain, not raw tone.
 * First-time connect (“Not connected” / missing credentials) is `pending`
 * (awaiting action), not `warning` (something broken).
 */
export function overviewConnectionOutcomeStatus(
  status: NormalizedIntegrationStatus,
): "success" | "failure" | "warning" | "pending" | "unknown" {
  if (integrationNeedsReconnect(status)) {
    return outcomeStatusFromTone(status.tone);
  }
  if (
    status.status === "needs_user_connection" ||
    status.credentialState === "missing"
  ) {
    return "pending";
  }
  return outcomeStatusFromTone(status.tone);
}

function needsFirstUserConnection(
  status: NormalizedIntegrationStatus,
): boolean {
  // Reconnect is "Needs fix", not first-time "To connect".
  if (status.connections.some((connection) => connection.canReconnect)) {
    return false;
  }
  if (integrationNeedsReconnect(status)) {
    return false;
  }
  // Unused alternative methods still advertise `connect`. That is Add account
  // on Connection — not app-level first-time connect — once any method works.
  if (status.connected) {
    return false;
  }
  return (
    status.status === "needs_user_connection" ||
    status.connections.some(
      (connection) =>
        connection.canConnect &&
        (connection.status === "needs_user_connection" ||
          connection.credentialState === "missing" ||
          connection.credentialState === "invalid"),
    )
  );
}

function needsAttentionBeyondConnect(
  status: NormalizedIntegrationStatus,
): boolean {
  return (
    status.status === "degraded" ||
    status.status === "needs_admin_configuration" ||
    status.status === "needs_instance_selection" ||
    status.status === "unavailable" ||
    integrationNeedsReconnect(status) ||
    status.tone === "danger" ||
    status.tone === "warning"
  );
}

/**
 * Canonical setup bucket. Filter matching and sort order both derive from this —
 * do not re-derive filter predicates from tone/status ad hoc.
 */
export function connectionSetupBucket(
  integration: Integration,
  context: ConnectionContext = "current_user",
): ConnectionSetupBucket {
  const status = normalizeIntegrationStatus(integration, context);
  if (needsFirstUserConnection(status)) {
    return "needs_connection";
  }
  if (needsAttentionBeyondConnect(status)) {
    return "needs_attention";
  }
  return "ready";
}

export function matchesConnectionFilter(
  integration: Integration,
  filter: ConnectionFilter,
  context: ConnectionContext = "current_user",
): boolean {
  if (filter === "all") return true;
  if (filter === "ready") {
    return catalogInstallState(integration, context) === "connected";
  }
  return connectionSetupBucket(integration, context) === filter;
}

export function matchesSurfaceFilter(
  integration: Integration,
  filter: SurfaceFilter,
): boolean {
  if (filter === "all") return true;
  const surfaces = getAppSurfaces(integration);
  if (filter === "has_ui") return surfaces.hasUi;
  if (filter === "no_ui") return !surfaces.hasUi;
  return surfaces.hasMcp;
}

const BUCKET_SORT_ORDER: Record<ConnectionSetupBucket, number> = {
  // Broken connections surface first; then first-time connect; then ready.
  needs_attention: 0,
  needs_connection: 1,
  ready: 2,
};

export function sortCatalogIntegrations(
  integrations: Integration[],
  context: ConnectionContext = "current_user",
): Integration[] {
  return [...integrations].sort((a, b) => {
    const bucketDiff =
      BUCKET_SORT_ORDER[connectionSetupBucket(a, context)] -
      BUCKET_SORT_ORDER[connectionSetupBucket(b, context)];
    if (bucketDiff !== 0) return bucketDiff;
    // Prefer apps with a product UI among ready rows.
    if (
      connectionSetupBucket(a, context) === "ready" &&
      connectionSetupBucket(b, context) === "ready"
    ) {
      const aUi = getAppSurfaces(a).hasUi ? 0 : 1;
      const bUi = getAppSurfaces(b).hasUi ? 0 : 1;
      if (aUi !== bUi) return aUi - bUi;
    }
    return getIntegrationLabel(a).localeCompare(getIntegrationLabel(b));
  });
}

/** How many apps need reconnect / config — drives the catalog attention callout. */
export function countNeedsAttention(
  integrations: Integration[],
  context: ConnectionContext = "current_user",
): number {
  return listNeedsAttention(integrations, context).length;
}

/** Apps in `needs_attention`, in catalog sort order (listed first in the grid). */
export function listNeedsAttention(
  integrations: Integration[],
  context: ConnectionContext = "current_user",
): Integration[] {
  return sortCatalogIntegrations(
    integrations.filter(
      (integration) =>
        connectionSetupBucket(integration, context) === "needs_attention",
    ),
    context,
  );
}

export function filterCatalogIntegrations(
  integrations: Integration[],
  options: {
    query: string;
    connection: ConnectionFilter;
    surface: SurfaceFilter;
    /** When true, keep only apps the user can administer. */
    admin?: boolean;
    context?: ConnectionContext;
  },
): Integration[] {
  const context = options.context ?? "current_user";
  const query = options.query.trim();
  const adminOnly = options.admin === true;
  const filtered = integrations.filter((integration) => {
    if (!matchesConnectionFilter(integration, options.connection, context)) {
      return false;
    }
    if (!matchesSurfaceFilter(integration, options.surface)) {
      return false;
    }
    if (adminOnly && !canManageApp(integration)) {
      return false;
    }
    if (!query) return true;
    const haystack = [
      integration.name,
      integration.displayName || "",
      integration.description || "",
    ].join(" ");
    return matchesSearchQuery(haystack, query);
  });
  return sortCatalogIntegrations(filtered, context);
}

/**
 * Catalog trailing + / Add is first-time connect chrome, not reconnect.
 * Dead logins already have Needs reconnect attention plus card navigation.
 */
export function catalogCardShowsConnectAction(
  installState: CatalogInstallState,
  connectLabel: "Connect" | "Reconnect" | null,
): boolean {
  if (installState === "needs_attention") return false;
  // Add only when there is a connect action, not for every uninstalled card.
  return installState === "mount_only" || connectLabel !== null;
}

export function primaryConnectLabel(
  integration: Integration,
  context: ConnectionContext = "current_user",
): "Connect" | "Reconnect" | null {
  const status = normalizeIntegrationStatus(integration, context);
  if (integrationNeedsReconnect(status)) return "Reconnect";
  const canReconnect = status.connections.some(
    (connection) => connection.canReconnect,
  );
  if (canReconnect) return "Reconnect";

  // Inferred "connect" actions can remain on already-connected rows; only
  // surface Connect when the integration still needs a user connection.
  if (needsFirstUserConnection(status)) return "Connect";
  return null;
}

/**
 * App-workspace Connection nav / page — SoT for credential-surface visibility.
 * Prefer this over `connected` or raw `connections.length`.
 */
export function appShowsCredentialSurface(
  integration: Integration,
  context: ConnectionContext = "current_user",
): boolean {
  return hasCredentialSurface(normalizeIntegrationStatus(integration, context));
}

/** User can administer this app (server omits managementPath otherwise). */
export function canManageApp(integration: Integration): boolean {
  return Boolean(integration.managementPath?.trim());
}

/** Server-authoritative admin route — never synthesize from `integration.name`. */
export function appManagementPath(integration: Integration): string | undefined {
  const path = integration.managementPath?.trim();
  return path || undefined;
}

export function appOpenPath(integration: Integration): string | undefined {
  const mountedPath = integration.mountedPath?.trim();
  return mountedPath || undefined;
}

export type AppDetailConnectionSearch = {
  action?: "disconnect";
};

/** User-facing connection route for credential management. */
export function appDetailConnectionPath(
  integration: Integration,
  options: AppDetailConnectionSearch = {},
): string {
  const base = `/apps/${encodeURIComponent(integration.name)}/connection`;
  if (options.action === "disconnect") {
    return `${base}?action=disconnect`;
  }
  return base;
}

/** Canonical app overview route. */
export function appDetailPath(integration: Integration): string {
  return `/apps/${encodeURIComponent(integration.name)}`;
}

/**
 * Whole-card activate target — always app detail (overview) for catalog tiles.
 * Product launch uses the explicit Open app control on the card; connect /
 * remove use the Add / More controls.
 */
export function catalogCardActivateTarget(
  _integration: Integration,
  _context: ConnectionContext = "current_user",
): "detail" {
  return "detail";
}

/** Route params for the whole-card stretch link — SoT for catalog tile navigation. */
export function catalogCardActivateRoute(integration: Integration): {
  to: "/apps/$app";
  params: { app: string };
} {
  switch (catalogCardActivateTarget(integration)) {
    case "detail":
      return {
        to: "/apps/$app",
        params: { app: integration.name },
      };
  }
}

/** Show Open app on the catalog card when the mounted UI is reachable. */
export function catalogShowOpenAppButton(
  integration: Integration,
  context: ConnectionContext = "current_user",
): boolean {
  if (!appOpenPath(integration)) return false;
  const status = normalizeIntegrationStatus(integration, context);
  return status.connected || primaryConnectLabel(integration, context) === null;
}

/** Catalog tile / listing badge — same owner as Overview (`summaryLabel`). */
export function catalogStatusBadgeLabel(
  integration: Integration,
  context: ConnectionContext = "current_user",
): string {
  return normalizeIntegrationStatus(integration, context).summaryLabel;
}
