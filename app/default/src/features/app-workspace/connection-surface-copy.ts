import {
  ACCOUNT_NAME_FALLBACK,
  CONNECTION_SURFACE_TITLE,
  connectAppActionLabel,
  connectAppDialogTitle,
} from "@/lib/accountCopy";
import {
  connectionNeedsReconnect,
  integrationNeedsReconnect,
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
 * - **Connection** - the surface (nav / page) for connecting this app.
 * - **Connect / Disconnect** - the verbs for wiring or revoking a login.
 * - **Connected / Not connected** - app-level status.
 * - **Account** - a named provider identity (OAuth/API instance) in the list.
 * - **Account label** - the name the operator chooses before sign-in; shown on
 *   the account card so multiple sign-ins are distinguishable.
 * - **Account identity** - provider-recognized facts (email, workspace, …) for
 *   recognizing which provider account is connected; SCIM-style primary + others.
 * - **In use** - the preferred account this method acts through (one per method).
 * - **Not in use** - connected but not the preferred account for that method.
 * - **Available** - connected while the workspace still needs an active choice.
 *
 * Credential *absence* (`not_required`) is never Overview/catalog chrome:
 * silence or "Ready". Actionable auth states own Connect {app} / Connection copy.
 * Setup is the assistant journey, not this surface.
 */

/**
 * Happy-path credential state. Overview Status, catalog checkmark, and the
 * catalog "already connected" browse section. Pair with "Not connected".
 */
export { CONNECTION_SURFACE_TITLE };

/** Preferred-account badge. Scoped to a method when more than one method is in use. */
export const IN_USE_LABEL = "In use" as const;

export type ConnectionMethodKind =
  | "mcp"
  | "oauth"
  | "api_key"
  | "pat"
  | "manual"
  | "shared";

function methodSearchBlob(connection: NormalizedConnection): string {
  return `${connection.key} ${connection.label}`
    .toLowerCase()
    .replace(/[_-]+/g, " ");
}

/** How this connection signs in. Independent of whether another method is connected. */
export function connectionMethodKind(
  connection: NormalizedConnection,
): ConnectionMethodKind {
  if (connection.isMCPPassthrough) return "shared";
  const blob = methodSearchBlob(connection);
  if (/\bmcp\b/.test(blob)) return "mcp";
  if (/\bpat\b/.test(blob) || blob.includes("personal access token")) {
    return "pat";
  }
  if (blob.includes("api key") || blob.includes("apikey")) return "api_key";
  if (connection.authTypes.includes("oauth")) return "oauth";
  return "manual";
}

export function connectionMethodTitle(
  connection: NormalizedConnection,
): string {
  return humanizeConnectionName(connection.label);
}

/** Short name for badges and CTAs (OAuth, MCP, API key). */
export function connectionMethodShortName(
  connection: NormalizedConnection,
): string {
  return connectionMethodTitle(connection);
}

/**
 * Why this method exists. First-time connect only. Never credential-state chrome
 * like "User credentials missing".
 */
export function connectionMethodPurpose(
  connection: NormalizedConnection,
): string | null {
  if (connection.isMCPPassthrough) return "Uses a shared login";
  if (connection.isNoAuth || !connection.isSubjectOwned) return null;
  if (connection.instances.length > 0 || connection.connected) return null;
  switch (connectionMethodKind(connection)) {
    case "mcp":
      return "Sign in so assistants can use this app's hosted tools.";
    case "oauth":
      return "Sign in for API access from this workspace.";
    case "api_key":
      return "Enter an API key so this workspace can call the API.";
    case "pat":
      return "Your user token for API calls. Rotate it before it expires.";
    case "manual":
      return "Enter a token so this workspace can call the API.";
    case "shared":
      return "Uses a shared login";
  }
}

export function subjectOwnedMethods(
  connections: NormalizedConnection[],
): NormalizedConnection[] {
  return connections.filter(
    (connection) => connection.isSubjectOwned && !connection.isMCPPassthrough,
  );
}

export function connectionHasInUseAccount(
  connection: NormalizedConnection,
): boolean {
  if (connection.instances.some((instance) => instance.preferred)) return true;
  return connectionNeedsReconnect(connection) && connection.instances.length === 1;
}

/** Scope "In use for OAuth" only when two methods each have an active account. */
export function shouldScopeInUseBadge(
  connections: NormalizedConnection[],
): boolean {
  return connections.filter(connectionHasInUseAccount).length >= 2;
}

export function isInUseRelationship(label: string): boolean {
  return label === IN_USE_LABEL || label.startsWith(`${IN_USE_LABEL} for `);
}

function preferredInstanceForConnection(
  connection: NormalizedConnection,
): NormalizedConnection["instances"][number] | undefined {
  const preferred = connection.instances.find((instance) => instance.preferred);
  if (preferred) return preferred;
  if (
    connectionNeedsReconnect(connection) &&
    connection.instances.length === 1
  ) {
    return connection.instances[0];
  }
  return undefined;
}

function preferredAccountDisplay(connection: NormalizedConnection): string | null {
  const instance = preferredInstanceForConnection(connection);
  if (!instance) return null;
  const identity = accountIdentityLines(instance.identity).primary?.value?.trim();
  if (identity) return identity;
  return humanizeConnectionName(instance.name, DEFAULT_ACCOUNT_LABEL);
}

function joinEnglishList(items: string[]): string {
  if (items.length === 0) return "";
  if (items.length === 1) return items[0]!;
  if (items.length === 2) return `${items[0]} and ${items[1]}`;
  return `${items.slice(0, -1).join(", ")}, and ${items[items.length - 1]}`;
}

/** Which linked account REST vs MCP currently acts through. Null when nothing is in use. */
export function connectionEffectiveUseSummary(
  status: NormalizedIntegrationStatus,
): string | null {
  const parts: string[] = [];
  for (const connection of subjectOwnedMethods(status.connections)) {
    const account = preferredAccountDisplay(connection);
    if (!account) continue;
    const title = connectionMethodTitle(connection);
    if (connectionMethodKind(connection) === "mcp") {
      parts.push(`${title} "${account}"`);
    } else {
      parts.push(`${title} "${account}" for API access`);
    }
  }
  if (parts.length === 0) return null;
  return `This workspace uses ${joinEnglishList(parts)}.`;
}

export type ConnectionDialogCopy = {
  title: string;
  description: string;
};

/**
 * Dialog chrome: the job is connecting this app. Methods are
 * separate sign-ins, not one picker. Page title stays Connection.
 */
export function connectionDialogCopy(
  status: NormalizedIntegrationStatus,
  displayName: string,
): ConnectionDialogCopy {
  const title = connectAppDialogTitle(displayName);
  const summary = connectionEffectiveUseSummary(status);
  const methods = subjectOwnedMethods(status.connections);
  const mode = connectionSurfaceMode(status);

  if (mode === "none" || mode === "shared") {
    return {
      title,
      description: connectionSurfaceCopy(mode).description,
    };
  }

  if (integrationNeedsReconnect(status)) {
    return {
      title,
      description: [
        summary,
        "The account in use needs a new sign-in before this workspace can use this app.",
      ]
        .filter(Boolean)
        .join(" "),
    };
  }

  if (status.status === "needs_instance_selection") {
    return {
      title,
      description: [
        summary,
        "More than one account is connected. Choose which one this workspace should use.",
      ]
        .filter(Boolean)
        .join(" "),
    };
  }

  if (mode === "connect") {
    return {
      title,
      description:
        methods.length > 1
          ? "Pick how this workspace should sign in. Methods are separate. Each one can have its own account."
          : "Connect so this workspace can use the app.",
    };
  }

  if (methods.length > 1) {
    return {
      title,
      description: [
        summary,
        "Methods are separate. Each one can have its own account in use.",
      ]
        .filter(Boolean)
        .join(" "),
    };
  }

  return {
    title,
    description: summary ?? "Accounts connected to this app.",
  };
}

function isUnusedConnectableMethod(connection: NormalizedConnection): boolean {
  return (
    connection.isSubjectOwned &&
    !connection.isMCPPassthrough &&
    connection.instances.length === 0 &&
    !connectionNeedsReconnect(connection) &&
    (connection.canConnect || connection.connectable)
  );
}

/**
 * Linked methods stay visible. Unused connectable methods collapse once
 * something is already linked. Recovery (reconnect) keeps every method visible.
 */
export function partitionConnectionMethods(
  connections: NormalizedConnection[],
): {
  primary: NormalizedConnection[];
  other: NormalizedConnection[];
} {
  if (connections.some(connectionNeedsReconnect)) {
    return { primary: connections, other: [] };
  }
  const other = connections.filter(isUnusedConnectableMethod);
  const primary = connections.filter((connection) => !isUnusedConnectableMethod(connection));
  const hasLinked = connections.some(
    (connection) =>
      connection.instances.length > 0 || connection.isMCPPassthrough,
  );
  if (!hasLinked || primary.length === 0 || other.length === 0) {
    return { primary: connections, other: [] };
  }
  return { primary, other };
}

export type ConnectionSurfaceMode = "connect" | "manage" | "shared" | "none";

export function connectionSurfaceMode(
  status: NormalizedIntegrationStatus,
): ConnectionSurfaceMode {
  const hasPassthrough = status.connections.some(
    (connection) => connection.isMCPPassthrough,
  );
  const hasSubjectConnection = status.connections.some(
    (connection) => !connection.isNoAuth,
  );
  if (hasPassthrough && !hasSubjectConnection) {
    return "shared";
  }
  if (status.credentialState === "not_required") {
    if (!hasSubjectConnection) return "none";
  }

  const hasLinkedAccount = status.connections.some(
    (connection) =>
      connection.instances.length > 0 ||
      connection.canReconnect ||
      connection.canDisconnect,
  );
  if (hasLinkedAccount) return "manage";

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
          "Connect so this workspace can use the app. Disconnect later to revoke access.",
        trustNote:
          "Connecting lets this workspace use the app on your behalf.",
      };
    case "manage":
      return {
        title: CONNECTION_SURFACE_TITLE,
        description:
          "Accounts connected to this app. Only one is in use at a time. Switch below, disconnect to revoke access, or connect another account.",
        trustNote:
          "This workspace acts through the account marked In use.",
      };
    case "shared":
      return {
        title: CONNECTION_SURFACE_TITLE,
        description:
          "This app uses a shared login. There is nothing to connect for this identity.",
        trustNote: null,
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
  if (integrationNeedsReconnect(status)) {
    return {
      title: CONNECTION_SURFACE_TITLE,
      description:
        "The account in use needs a new sign-in before this workspace can use this app. Sign in again, switch to another account, or add one.",
      trustNote:
        "This workspace will act through the account in use once access is restored.",
    };
  }
  if (status.status === "needs_instance_selection") {
    return {
      title: CONNECTION_SURFACE_TITLE,
      description:
        "More than one account is connected. Choose which one this workspace should use, or disconnect any account you no longer want.",
      trustNote:
        "This app can act on your behalf here once you choose an account.",
    };
  }
  const copy = connectionSurfaceCopy(connectionSurfaceMode(status));
  if (
    connectionSurfaceMode(status) === "manage" &&
    subjectOwnedMethods(status.connections).length > 1
  ) {
    const summary = connectionEffectiveUseSummary(status);
    return {
      ...copy,
      description: [
        summary,
        "Each method can have its own active account. Switch below, disconnect to revoke access, or add another account.",
      ]
        .filter(Boolean)
        .join(" "),
      trustNote:
        "This workspace acts through the account marked In use for that method.",
    };
  }
  return copy;
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

export function addAccountFormCopy(args: {
  appDisplayName: string;
  connectionKeyLabel?: string | null;
}): AddAccountFormCopy {
  const key = args.connectionKeyLabel?.trim();
  return {
    title: key
      ? `Connect another ${key} account`
      : connectAppActionLabel(args.appDisplayName),
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
 * When two methods each have an active account, scope the badge to that method.
 */
export function accountRelationshipLabel(args: {
  preferred?: boolean;
  needsInstanceSelection: boolean;
  connectionKeyLabel?: string | null;
  /** Sole linked account on a connection that needs reconnect, even if preferred was stripped. */
  soleLinkedAccount?: boolean;
  /** Method short name when more than one method has an account in use. */
  methodScope?: string | null;
}): string {
  if (args.preferred || args.soleLinkedAccount) {
    const scope = args.methodScope?.trim();
    if (scope) return `${IN_USE_LABEL} for ${scope}`;
    return IN_USE_LABEL;
  }
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
          ? `This will remove this identity's access to ${args.displayName}. You can add it again later.`
          : `This will remove ${args.displayName} from this workspace. You can add it again at any time.`,
    };
  }
  const account = args.accountLabel?.trim();
  if (account) {
    return {
      heading: `Disconnect ${account}?`,
      body:
        context === "managed_subject"
          ? `This disconnects ${account} from ${args.displayName} for this identity. You can add it again later.`
          : `This disconnects ${account} from ${args.displayName} in this workspace. You can sign in again anytime.`,
    };
  }
  return {
    heading: `Disconnect ${args.displayName}?`,
    body:
      context === "managed_subject"
        ? `This will remove this identity's access to ${args.displayName}. You can add it again later.`
        : `This will remove your access to ${args.displayName}. You can add it again at any time.`,
  };
}

/**
 * API often ships machine name `default`. Present a human label instead of
 * echoing the slug. Do not invent a primary/secondary hierarchy.
 */
export function humanizeConnectionName(
  name: string,
  fallback: string = ACCOUNT_NAME_FALLBACK,
): string {
  const trimmed = name.trim();
  if (!trimmed || /^default$/i.test(trimmed)) return fallback;
  return trimmed;
}

/** Fallback label when an instance/account slug is `default`. */
export const DEFAULT_ACCOUNT_LABEL = ACCOUNT_NAME_FALLBACK;

/**
 * Instance-scoped disconnect always names the account. The generic
 * Account fallback is still an account, not the whole app.
 */
export function disconnectConfirmAccountLabel(args: {
  identityPrimary?: string | null;
  instanceName?: string | null;
}): string | null {
  const identity = args.identityPrimary?.trim();
  if (identity) return identity;
  const instance = args.instanceName?.trim();
  if (!instance) return null;
  return humanizeConnectionName(instance, DEFAULT_ACCOUNT_LABEL);
}

/** Overview blurb when the app has a credential/connection surface. */
export const CONNECTION_ACCESS_BLURB =
  "Manage connected accounts and which one this workspace uses." as const;

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
 * Overview attention notice. Status recovery belongs in Alert + Connection
 * link, not a status Badge. First-time connect stays on the Connect {app} CTA.
 */
export type OverviewConnectionAttention = {
  title: string;
  description: string;
  actionLabel: string;
};

export function overviewConnectionAttention(
  status: NormalizedIntegrationStatus,
): OverviewConnectionAttention | null {
  // Connect {app} CTA owns first-time connecting; do not duplicate as an Alert.
  const reconnecting = integrationNeedsReconnect(status);
  if (status.status === "needs_user_connection" && !reconnecting) return null;
  if (status.tone !== "warning" && status.tone !== "danger") return null;

  switch (status.status) {
    case "needs_instance_selection":
      return {
        title: status.summaryLabel,
        description:
          "More than one account is available. Pick which one this workspace should use. Until then this app has no account in use.",
        actionLabel: "Choose an account",
      };
    case "needs_admin_configuration":
      return {
        title: status.summaryLabel,
        description:
          "An admin needs to finish configuring this app before you can use it.",
        actionLabel: "View Connection",
      };
    case "degraded":
      return {
        title: status.summaryLabel,
        description:
          "This app needs a fix before it works reliably.",
        actionLabel: "Fix on Connection",
      };
    case "unavailable":
      return {
        title: status.summaryLabel,
        description: "This app is unavailable right now.",
        actionLabel: "View Connection",
      };
    default:
      if (reconnecting) {
        return {
          title: status.summaryLabel,
          description: "Sign in again to restore access for this app.",
          actionLabel: "Sign in again on Connection",
        };
      }
      return {
        title: status.summaryLabel,
        description: "Review this app’s accounts to continue.",
        actionLabel: "Open Connection",
      };
  }
}

/**
 * Connection-panel attention. Recovery copy in Alert, never a status Badge.
 * Already on Connection, so no outbound link label.
 */
export type ConnectionPanelAttention = {
  title: string;
  description: string;
};

export function connectionPanelAttention(
  connection: NormalizedConnection,
): ConnectionPanelAttention | null {
  if (
    connection.status === "needs_user_connection" &&
    !connectionNeedsReconnect(connection)
  ) {
    return null;
  }
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
          "More than one account is available. Pick which one this workspace should use. Until then this app has no account in use.",
      };
    case "needs_admin_configuration":
      return {
        title: connection.summaryLabel,
        description:
          "An admin needs to finish configuring this app before you can use it.",
      };
    case "degraded":
      return {
        title: connection.summaryLabel,
        description:
          "This app needs a fix before it works reliably.",
      };
    case "unavailable":
      return {
        title: connection.summaryLabel,
        description: "This app is unavailable right now.",
      };
    default:
      if (connectionNeedsReconnect(connection)) {
        return {
          title: connection.summaryLabel,
          description:
            "This app no longer accepts the saved sign-in for this account. Sign in again to restore access.",
        };
      }
      return {
        title: connection.summaryLabel,
        description: "Review this app’s accounts to continue.",
      };
  }
}
