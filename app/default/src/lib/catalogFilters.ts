import type { Integration } from "@/lib/api";
import {
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

/** Segmented catalog filters — attention is sort/warning, not a filter tab. */
export type ConnectionFilter = "all" | "needs_connection" | "ready";

export type SurfaceFilter = "all" | "has_ui" | "no_ui" | "has_mcp";

export type AppSurfaces = {
  hasUi: boolean;
  hasMcp: boolean;
};

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

function needsFirstUserConnection(
  status: NormalizedIntegrationStatus,
): boolean {
  // Reconnect is "Needs fix", not first-time "To connect".
  if (status.connections.some((connection) => connection.canReconnect)) {
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
    status.connections.some((connection) => connection.canReconnect) ||
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
  return integrations.filter(
    (integration) =>
      connectionSetupBucket(integration, context) === "needs_attention",
  ).length;
}

export function filterCatalogIntegrations(
  integrations: Integration[],
  options: {
    query: string;
    connection: ConnectionFilter;
    surface: SurfaceFilter;
    context?: ConnectionContext;
  },
): Integration[] {
  const context = options.context ?? "current_user";
  const query = options.query.trim().toLowerCase();
  const filtered = integrations.filter((integration) => {
    if (!matchesConnectionFilter(integration, options.connection, context)) {
      return false;
    }
    if (!matchesSurfaceFilter(integration, options.surface)) {
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

export function primaryConnectLabel(
  integration: Integration,
  context: ConnectionContext = "current_user",
): "Connect" | "Reconnect" | null {
  const status = normalizeIntegrationStatus(integration, context);
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
 * Whole-card activate target — listing (connect funnel) vs admin.
 * The tile has no separate face CTA; the card itself is the hit target.
 */
export function catalogCardActivateTarget(
  integration: Integration,
  context: ConnectionContext = "current_user",
): "listing" | "admin" {
  return connectionSetupBucket(integration, context) === "needs_connection"
    ? "listing"
    : "admin";
}
