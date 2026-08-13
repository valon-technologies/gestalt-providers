import type { AuthType, ConnectionParamDef, Integration } from "@/lib/api";
import {
  ADD_ACCOUNT_LABEL,
} from "@/features/app-workspace/connection-surface-copy";
import {
  normalizeIntegrationStatus,
  type ConnectionContext,
  type NormalizedConnection,
} from "@/lib/integrationStatus";

export type ConnectionAuthKind =
  | "connect"
  | "add_instance"
  | "reconnect"
  | "select_instance";

export type ConnectionAuthAction = {
  key: string;
  kind: ConnectionAuthKind;
  authType: AuthType;
  connectionKey: string;
  connection?: string;
  label: string;
  variant?: "default" | "secondary";
  requiresInstanceName: boolean;
};

export type ConnectFormView = "token" | "oauth_params" | "instance";

export type AuthActionStart =
  | { kind: "oauth"; instance?: string; connection?: string }
  | { kind: "form"; view: ConnectFormView };

/** How Connect from a catalog/setup tile should proceed. */
export type ConnectEntryPlan =
  | { kind: "oauth"; connection?: string }
  | { kind: "form"; view: ConnectFormView }
  | { kind: "chooser" };

export function hasConnectionParams(
  connectionParams: Record<string, ConnectionParamDef> | undefined,
): boolean {
  return Boolean(connectionParams && Object.keys(connectionParams).length > 0);
}

function normalizeActionKinds(connection: NormalizedConnection): ConnectionAuthKind[] {
  const kinds: ConnectionAuthKind[] = [];
  if (connection.canConnect) kinds.push("connect");
  if (connection.canAddInstance) kinds.push("add_instance");
  if (connection.canReconnect) kinds.push("reconnect");
  return kinds;
}

function buildAuthActionLabel(
  connection: NormalizedConnection,
  kind: ConnectionAuthKind,
  authType: AuthType,
  showConnectionNames: boolean,
): string {
  const dualAuth =
    connection.authTypes.includes("oauth") &&
    connection.authTypes.includes("manual");
  const name = connection.label;

  if (kind === "add_instance") {
    return showConnectionNames ? `Add ${name} account` : ADD_ACCOUNT_LABEL;
  }

  if (kind === "reconnect") {
    if (authType === "manual" && dualAuth) {
      return showConnectionNames
        ? `Reconnect ${name} with API token`
        : "Reconnect with API token";
    }
    return showConnectionNames ? `Reconnect ${name}` : "Reconnect";
  }

  if (kind === "select_instance") {
    return showConnectionNames ? `Select ${name} connection` : "Select connection";
  }

  if (authType === "manual") {
    if (dualAuth) {
      return showConnectionNames ? `Use API token for ${name}` : "Use API token";
    }
    return showConnectionNames ? `Connect with ${name}` : "Connect";
  }

  return showConnectionNames
    ? `Connect with ${name}`
    : dualAuth
      ? "Connect with OAuth"
      : "Connect";
}

export function buildAuthActions(
  connections: NormalizedConnection[],
): ConnectionAuthAction[] {
  const actionableConnections = connections.filter(
    (connection) =>
      connection.isSubjectOwned && normalizeActionKinds(connection).length > 0,
  );
  const showConnectionNames = actionableConnections.length > 1;
  const actions: ConnectionAuthAction[] = [];

  for (const connection of actionableConnections) {
    for (const kind of normalizeActionKinds(connection)) {
      for (const authType of connection.authTypes) {
        actions.push({
          key: `${connection.key}:${kind}:${authType}`,
          kind,
          authType,
          connectionKey: connection.key,
          connection: connection.connection,
          label: buildAuthActionLabel(
            connection,
            kind,
            authType,
            showConnectionNames,
          ),
          variant:
            authType === "manual" && connection.authTypes.includes("oauth")
              ? "secondary"
              : "default",
          requiresInstanceName: kind === "add_instance",
        });
      }
    }
  }

  return actions;
}

export function authActionStart(
  action: ConnectionAuthAction,
  connections: NormalizedConnection[],
): AuthActionStart {
  if (action.requiresInstanceName) {
    return { kind: "form", view: "instance" };
  }
  if (action.authType === "manual") {
    return { kind: "form", view: "token" };
  }
  const connection = connections.find(
    (item) => item.key === action.connectionKey,
  );
  if (hasConnectionParams(connection?.connectionParams)) {
    return { kind: "form", view: "oauth_params" };
  }
  return { kind: "oauth", connection: action.connection };
}

/**
 * Unique Connect with no extra fields starts OAuth immediately.
 * Unique Connect that needs a token, params, or account name opens that form.
 * Two or more actions keep the chooser dialog.
 */
export function connectEntryPlan(
  integration: Integration,
  context: ConnectionContext = "current_user",
): ConnectEntryPlan {
  const status = normalizeIntegrationStatus(integration, context);
  const actions = buildAuthActions(status.connections);
  if (actions.length !== 1) return { kind: "chooser" };
  const start = authActionStart(actions[0]!, status.connections);
  if (start.kind === "oauth") {
    return { kind: "oauth", connection: start.connection };
  }
  return { kind: "form", view: start.view };
}

export function seedPendingAuthAction(
  integration: Integration,
  context: ConnectionContext,
  view: ConnectFormView | string,
): ConnectionAuthAction | undefined {
  if (view !== "token" && view !== "oauth_params" && view !== "instance") {
    return undefined;
  }
  const actions = buildAuthActions(
    normalizeIntegrationStatus(integration, context).connections,
  );
  return actions[0];
}
