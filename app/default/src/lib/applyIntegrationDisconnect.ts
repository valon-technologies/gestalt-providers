import type {
  ConnectionDefInfo,
  CredentialState,
  InstanceInfo,
  Integration,
  IntegrationStatus,
} from "./api";

export type IntegrationDisconnectSpec = {
  instance?: string;
  connection?: string;
};

/**
 * Apply a successful disconnect to the catalog entry.
 *
 * The integrations list is the canonical record of linked accounts. After
 * DELETE succeeds, the UI must reflect that mutation immediately — not wait
 * for a later GET to catch up.
 *
 * Mutate structural connection state only. Clear `actions` so
 * `normalizeIntegrationStatus` remains the single derivation path.
 */
export function applyDisconnectToIntegration(
  integration: Integration,
  spec: IntegrationDisconnectSpec,
): Integration {
  const connections = integration.connections;
  if (!connections?.length) {
    return integration;
  }

  return {
    ...integration,
    connections: connections.map((connection) =>
      connectionMatches(connection, spec.connection)
        ? applyDisconnectToConnection(connection, spec)
        : connection,
    ),
  };
}

export function applyDisconnectToIntegrations(
  integrations: Integration[],
  integrationName: string,
  spec: IntegrationDisconnectSpec,
): Integration[] {
  return integrations.map((integration) =>
    integration.name === integrationName
      ? applyDisconnectToIntegration(integration, spec)
      : integration,
  );
}

function connectionMatches(
  connection: ConnectionDefInfo,
  connectionName?: string,
): boolean {
  if (!connectionName) return true;
  return connection.name === connectionName;
}

function applyDisconnectToConnection(
  connection: ConnectionDefInfo,
  spec: IntegrationDisconnectSpec,
): ConnectionDefInfo {
  const remaining = remainingInstances(connection, spec);
  if (remaining.length === 0) {
    return emptiedConnection(connection);
  }

  const preferredInstance = preferredInstanceName(remaining, connection, spec);
  return {
    ...connection,
    instances: remaining,
    preferredInstance,
    actions: undefined,
  };
}

function remainingInstances(
  connection: ConnectionDefInfo,
  spec: IntegrationDisconnectSpec,
): InstanceInfo[] {
  const instances = connection.instances ?? [];
  if (!spec.instance) {
    return [];
  }
  return instances.filter(
    (instance) =>
      !instanceMatches(instance, connection.name, spec),
  );
}

function instanceMatches(
  instance: InstanceInfo,
  connectionName: string,
  spec: IntegrationDisconnectSpec,
): boolean {
  if (instance.name !== spec.instance) return false;
  if (!spec.connection) return true;
  return (instance.connection || connectionName) === spec.connection;
}

function preferredInstanceName(
  remaining: InstanceInfo[],
  connection: ConnectionDefInfo,
  spec: IntegrationDisconnectSpec,
): string | undefined {
  if (
    connection.preferredInstance &&
    connection.preferredInstance !== spec.instance
  ) {
    return connection.preferredInstance;
  }
  return remaining.find((instance) => instance.preferred)?.name;
}

function emptiedConnection(connection: ConnectionDefInfo): ConnectionDefInfo {
  return {
    ...connection,
    instances: [],
    connected: false,
    preferredInstance: undefined,
    credentialState: credentialStateAfterAccountsRemoved(
      connection.credentialState,
    ),
    status: statusAfterAccountsRemoved(connection.status),
    healthState:
      connection.healthState === "unhealthy"
        ? connection.healthState
        : "not_applicable",
    actions: undefined,
  };
}

function credentialStateAfterAccountsRemoved(
  state?: CredentialState,
): CredentialState {
  if (state === "not_required") return "not_required";
  return "missing";
}

function statusAfterAccountsRemoved(
  status?: IntegrationStatus,
): IntegrationStatus {
  if (status === "needs_admin_configuration" || status === "unavailable") {
    return status;
  }
  return "needs_user_connection";
}
