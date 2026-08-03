import {
  normalizeIntegrationStatus,
  statusTone,
  type ConnectionContext,
  type NormalizedConnection,
  type NormalizedIntegrationStatus,
} from "@/lib/integrationStatus";
import type { Integration } from "@/lib/api";

/**
 * User-facing vocabulary for the app-workspace Connection surface.
 *
 * Domain spine: **Connection** (nav, page title, CTAs, empty states).
 * "Credentials" is reserved for technical/status copy from the status model
 * (e.g. "No credentials required") — never for page chrome.
 */
export const CONNECTION_SURFACE_TITLE = "Connection" as const;

export const CONNECTION_SURFACE_NAV_LABEL = "Connection" as const;

export type ConnectionSurfaceMode = "connect" | "manage" | "none";

export function connectionSurfaceMode(
  status: NormalizedIntegrationStatus,
): ConnectionSurfaceMode {
  if (status.credentialState === "not_required") {
    const hasManageable = status.connections.some((connection) => !connection.isNoAuth);
    if (!hasManageable) return "none";
  }

  const needsFirstConnect =
    status.status === "needs_user_connection" ||
    status.credentialState === "missing" ||
    status.connections.some(
      (connection) =>
        connection.canConnect &&
        !connection.connected &&
        (connection.status === "needs_user_connection" ||
          connection.credentialState === "missing" ||
          connection.credentialState === "invalid"),
    );

  if (needsFirstConnect && !status.connections.some((c) => c.connected)) {
    return "connect";
  }
  return "manage";
}

export type ConnectionSurfaceCopy = {
  title: typeof CONNECTION_SURFACE_TITLE;
  description: string;
  /** Trust / permission note under the header. No hollow privacy-policy CTA. */
  trustNote: string | null;
};

export function connectionSurfaceCopy(
  mode: ConnectionSurfaceMode,
): ConnectionSurfaceCopy {
  switch (mode) {
    case "none":
      return {
        title: CONNECTION_SURFACE_TITLE,
        description:
          "This app does not require a connection. Open it when you are ready to work.",
        trustNote: null,
      };
    case "connect":
      return {
        title: CONNECTION_SURFACE_TITLE,
        description:
          "Connect this app under your user. Disconnect later to revoke access.",
        trustNote:
          "Connecting lets this workspace use the app on your behalf.",
      };
    case "manage":
      return {
        title: CONNECTION_SURFACE_TITLE,
        description:
          "Manage this app’s connection under your user. Disconnect to revoke access, or add another connection.",
        trustNote:
          "This workspace can use the app on your behalf while connected.",
      };
  }
}

export function connectionSurfaceCopyForIntegration(
  integration: Integration,
  context: ConnectionContext = "current_user",
): ConnectionSurfaceCopy {
  const status = normalizeIntegrationStatus(integration, context);
  return connectionSurfaceCopy(connectionSurfaceMode(status));
}

/**
 * API often ships machine name `default`. Present a human label instead of
 * echoing the slug. Do not invent a primary/secondary hierarchy.
 */
export function humanizeConnectionName(
  name: string,
  fallback = "Connection",
): string {
  const trimmed = name.trim();
  if (!trimmed || /^default$/i.test(trimmed)) return fallback;
  return trimmed;
}

/** Fallback label when an instance/account slug is `default`. */
export const DEFAULT_ACCOUNT_LABEL = "Account" as const;

/** Overview blurb when the app has a credential/connection surface. */
export const CONNECTION_ACCESS_BLURB =
  "How you’re connected to this app under the signed-in user." as const;

/** Overview secondary CTA when already connected (no Connect/Reconnect). */
export const MANAGE_CONNECTION_LABEL = "Manage connection" as const;

/** Per-account primary action when multiple instances need an active choice. */
export const USE_ACCOUNT_LABEL = "Use this account" as const;

/**
 * Initials for account Avatar fallback from an instance display name.
 * Prefer two letters from words; otherwise the first two characters.
 */
export function accountInitials(label: string): string {
  const trimmed = label.trim();
  if (!trimmed) return "?";
  const parts = trimmed.split(/[\s._-]+/).filter(Boolean);
  if (parts.length >= 2) {
    const a = parts[0]?.[0];
    const b = parts[1]?.[0];
    if (a && b) return `${a}${b}`.toUpperCase();
  }
  return trimmed.slice(0, 2).toUpperCase();
}

/**
 * Overview attention notice — status recovery belongs in Alert + Connection
 * link, not a status Badge. First-time connect stays on the Connect CTA.
 */
export type OverviewConnectionAttention = {
  title: string;
  description: string;
  actionLabel: string;
};

export function overviewConnectionAttention(
  status: NormalizedIntegrationStatus,
): OverviewConnectionAttention | null {
  // Connect CTA owns first-time connect; do not duplicate as an Alert.
  if (status.status === "needs_user_connection") return null;
  if (status.tone !== "warning" && status.tone !== "danger") return null;

  switch (status.status) {
    case "needs_instance_selection":
      return {
        title: status.summaryLabel,
        description:
          "This app has more than one account available. Pick which one this workspace should use on Connection.",
        actionLabel: "Choose on Connection",
      };
    case "needs_admin_configuration":
      return {
        title: status.summaryLabel,
        description:
          "An admin needs to finish setup before you can use this app.",
        actionLabel: "View Connection",
      };
    case "degraded":
      return {
        title: status.summaryLabel,
        description:
          "This connection needs a fix before the app works reliably.",
        actionLabel: "Fix on Connection",
      };
    case "unavailable":
      return {
        title: status.summaryLabel,
        description: "This app is unavailable right now.",
        actionLabel: "View Connection",
      };
    default:
      if (status.connections.some((connection) => connection.canReconnect)) {
        return {
          title: status.summaryLabel,
          description: "Reconnect to restore access for this app.",
          actionLabel: "Reconnect on Connection",
        };
      }
      return {
        title: status.summaryLabel,
        description: "Review this app’s connection to continue.",
        actionLabel: "Open Connection",
      };
  }
}

/**
 * Connection-panel attention — recovery copy in Alert, never a status Badge.
 * Already on Connection, so no outbound link label.
 */
export type ConnectionPanelAttention = {
  title: string;
  description: string;
};

export function connectionPanelAttention(
  connection: NormalizedConnection,
): ConnectionPanelAttention | null {
  if (connection.status === "needs_user_connection") return null;
  const tone = statusTone(
    connection.status,
    connection.credentialState,
    connection.healthState,
  );
  if (tone !== "warning" && tone !== "danger") return null;

  switch (connection.status) {
    case "needs_instance_selection":
      return {
        title: connection.summaryLabel,
        description:
          "More than one account is connected. Choose which one this workspace should use.",
      };
    case "needs_admin_configuration":
      return {
        title: connection.summaryLabel,
        description:
          "An admin needs to finish setup before you can use this connection.",
      };
    case "degraded":
      return {
        title: connection.summaryLabel,
        description:
          "This connection needs a fix before the app works reliably.",
      };
    case "unavailable":
      return {
        title: connection.summaryLabel,
        description: "This connection is unavailable right now.",
      };
    default:
      if (connection.canReconnect) {
        return {
          title: connection.summaryLabel,
          description: "Reconnect to restore access for this connection.",
        };
      }
      return {
        title: connection.summaryLabel,
        description: "Review this connection to continue.",
      };
  }
}
