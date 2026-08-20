import type { AuthType, ConnectionParamDef, Integration } from "@/lib/api";
import {
  connectAppActionLabel,
  SIGN_IN_AGAIN_LABEL,
  SIGN_IN_WITH_OAUTH_LABEL,
} from "@/lib/accountCopy";
import { getIntegrationLabel } from "@/lib/integrationSearch";
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
  if (connection.canReconnect) kinds.push("reconnect");
  if (connection.canConnect) kinds.push("connect");
  if (connection.canAddInstance) kinds.push("add_instance");
  return kinds;
}

function buildAuthActionLabel(
  connection: NormalizedConnection,
  kind: ConnectionAuthKind,
  authType: AuthType,
  showConnectionNames: boolean,
  appLabel: string,
): string {
  const dualAuth =
    connection.authTypes.includes("oauth") &&
    connection.authTypes.includes("manual");
  const name = connection.label;
  const connectApp = connectAppActionLabel(appLabel);

  if (kind === "add_instance") {
    return showConnectionNames
      ? `Connect another ${name} account`
      : connectApp;
  }

  if (kind === "reconnect") {
    if (authType === "manual" && dualAuth) {
      return showConnectionNames
        ? `Sign in again to ${name} with API token`
        : "Sign in again with API token";
    }
    return showConnectionNames
      ? `Sign in again with ${name}`
      : SIGN_IN_AGAIN_LABEL;
  }

  if (kind === "select_instance") {
    return showConnectionNames ? `Choose ${name} account` : "Choose account";
  }

  if (authType === "manual") {
    if (dualAuth) {
      return showConnectionNames ? `Use API token for ${name}` : "Use API token";
    }
    return showConnectionNames ? `Connect ${name} account` : connectApp;
  }

  return showConnectionNames
    ? `Sign in with ${name}`
    : dualAuth
      ? SIGN_IN_WITH_OAUTH_LABEL
      : connectApp;
}

export function buildAuthActions(
  connections: NormalizedConnection[],
  appLabel: string,
): ConnectionAuthAction[] {
  const actionableConnections = connections.filter(
    (connection) =>
      connection.isSubjectOwned && normalizeActionKinds(connection).length > 0,
  );
  const showConnectionNames = actionableConnections.length > 1;
  const hasReconnect = connections.some((connection) => connection.canReconnect);
  const actions: ConnectionAuthAction[] = [];

  for (const connection of actionableConnections) {
    for (const kind of normalizeActionKinds(connection)) {
      for (const authType of connection.authTypes) {
        const demoteOtherMethods =
          hasReconnect && (kind === "connect" || kind === "add_instance");
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
            appLabel,
          ),
          variant:
            demoteOtherMethods
              ? "secondary"
              : authType === "manual" && connection.authTypes.includes("oauth")
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
  const actions = buildAuthActions(
    status.connections,
    getIntegrationLabel(integration),
  );
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
    getIntegrationLabel(integration),
  );
  return actions[0];
}
