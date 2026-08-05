import {
  normalizeIntegrationStatus,
  statusTone,
  type ConnectionContext,
  type NormalizedConnection,
  type NormalizedIntegrationStatus,
} from "@/lib/integrationStatus";
import type { AccountIdentity, IdentityFact, Integration } from "@/lib/api";

/**
 * User-facing vocabulary for the app-workspace Connection surface.
 *
 * Domain model (one meaning each):
 * - **Connection** — the surface (nav / page) for managing linked accounts.
 * - **Account** — a linked provider identity (OAuth/API instance) in the list.
 * - **Account label** — the name the operator chooses before sign-in; shown on
 *   the account card so multiple sign-ins are distinguishable.
 * - **Account identity** — provider-recognized facts (email, workspace, …) for
 *   recognizing which provider account is linked; SCIM-style primary + others.
 * - **In use** — the preferred account this workspace acts through (one at a time).
 * - **Not in use** — linked but not the preferred account.
 * - **Available** — linked while the workspace still needs an active choice.
 *
 * "Credentials" stays technical/status-only (e.g. "No credentials required") —
 * never page chrome or catalog lead copy.
 */
export const CONNECTION_SURFACE_TITLE = "Connection" as const;

export const CONNECTION_SURFACE_NAV_LABEL = "Connection" as const;

/** Primary CTA to link another provider identity. */
export const ADD_ACCOUNT_LABEL = "Add account" as const;

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
          "This app does not require a linked account. Open it when you are ready to work.",
        trustNote: null,
      };
    case "connect":
      return {
        title: CONNECTION_SURFACE_TITLE,
        description:
          "Link an account so this workspace can use the app. Disconnect later to revoke access.",
        trustNote:
          "Linking lets this workspace use the app on your behalf.",
      };
    case "manage":
      return {
        title: CONNECTION_SURFACE_TITLE,
        description:
          "Accounts linked to this app. Only one is in use at a time — switch below, disconnect to revoke access, or add another account.",
        trustNote:
          "This workspace acts through the account marked In use.",
      };
  }
}

/**
 * Header copy owned by integration status — not mode alone.
 * Multi-account without a chosen account must not promise “while connected”.
 */
export function connectionSurfaceCopyForStatus(
  status: NormalizedIntegrationStatus,
): ConnectionSurfaceCopy {
  if (status.status === "needs_instance_selection") {
    return {
      title: CONNECTION_SURFACE_TITLE,
      description:
        "More than one account is linked. Choose which one this workspace should use, or disconnect any account you no longer want.",
      trustNote:
        "This app can act on your behalf here once you choose an account.",
    };
  }
  return connectionSurfaceCopy(connectionSurfaceMode(status));
}

export function connectionSurfaceCopyForIntegration(
  integration: Integration,
  context: ConnectionContext = "current_user",
): ConnectionSurfaceCopy {
  const status = normalizeIntegrationStatus(integration, context);
  return connectionSurfaceCopyForStatus(status);
}

/**
 * Copy for the pre-OAuth (or pre-token) account-label step.
 * Owns dialog promise + field guidance — do not hardcode these in the panel.
 */
export type AddAccountFormCopy = {
  title: string;
  /** Dialog-level path framing (under the title). */
  description: string;
  label: string;
  /** Field-local naming guidance (after the input). */
  fieldDescription: string;
  placeholder: string;
  continueLabel: string;
  cancelLabel: string;
};

export function addAccountFormCopy(args?: {
  connectionKeyLabel?: string | null;
}): AddAccountFormCopy {
  const key = args?.connectionKeyLabel?.trim();
  return {
    title: key ? `Add ${key} account` : "Add account",
    description:
      "You’ll authenticate with the provider on the next step.",
    label: "Account label",
    fieldDescription:
      "Pick a short name so you can tell this account apart in the list.",
    placeholder: "e.g. work, personal",
    continueLabel: "Continue",
    cancelLabel: "Cancel",
  };
}

/**
 * Per-account relationship to the workspace.
 * Only the preferred account is “In use”; others stay linked as “Not in use”.
 */
export function accountRelationshipLabel(args: {
  preferred?: boolean;
  needsInstanceSelection: boolean;
  connectionKeyLabel?: string | null;
}): string {
  if (args.preferred) return "In use";
  if (args.needsInstanceSelection) return "Available";
  if (args.connectionKeyLabel) return args.connectionKeyLabel;
  return "Not in use";
}

/**
 * Split SCIM-style identity facts into primary (emphasized) and additional lines.
 * Falls back to the first fact if no primary flag is set.
 */
export function accountIdentityLines(identity?: AccountIdentity | null): {
  primary: IdentityFact | null;
  additional: IdentityFact[];
} {
  const facts = (identity?.facts ?? []).filter(
    (fact) => fact.value?.trim() && fact.kind?.trim(),
  );
  if (facts.length === 0) {
    return { primary: null, additional: [] };
  }
  const primary = facts.find((fact) => fact.primary) ?? facts[0] ?? null;
  const additional = facts.filter((fact) => fact !== primary);
  return { primary, additional };
}

export function disconnectConfirmCopy(args: {
  displayName: string;
  accountLabel?: string | null;
  context?: ConnectionContext;
  removeApp?: boolean;
}): { heading: string; body: string } {
  const context = args.context ?? "current_user";
  if (args.removeApp) {
    return {
      heading: `Remove ${args.displayName}?`,
      body:
        context === "managed_subject"
          ? `This will remove this identity's access to ${args.displayName}. It can be reconnected later.`
          : `This will remove ${args.displayName} from this workspace. You can reconnect at any time.`,
    };
  }
  const account = args.accountLabel?.trim();
  if (account) {
    return {
      heading: `Disconnect ${account}?`,
      body:
        context === "managed_subject"
          ? `This disconnects ${account} from ${args.displayName} for this identity. It can be reconnected later.`
          : `This disconnects ${account} from ${args.displayName} in this workspace. You can sign in again anytime.`,
    };
  }
  return {
    heading: `Disconnect ${args.displayName}?`,
    body:
      context === "managed_subject"
        ? `This will remove this identity's connection to ${args.displayName}. It can be reconnected later.`
        : `This will remove your connection to ${args.displayName}. You can reconnect at any time.`,
  };
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
  "Manage linked accounts and which one this workspace uses." as const;

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
          "More than one account is available. Pick which one this workspace should use — until then this app is not connected.",
        actionLabel: "Choose an account",
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
          "More than one account is available. Pick which one this workspace should use — until then this app is not connected.",
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
