import type {
  AppCatalogConnection,
  AppCatalogEntry,
  AppConnectionOverlay,
  AppConnectionStatus,
  AppsDirectory,
  ConnectionDefInfo,
  Integration,
} from "@/lib/api";

export function mergeAppCatalogWithConnections(
  catalog: AppCatalogEntry[],
  overlay?: AppConnectionStatus[] | null,
): Integration[] {
  const byName = new Map((overlay ?? []).map((row) => [row.name, row]));
  return catalog.map((entry) => {
    const status = byName.get(entry.name);
    return {
      name: entry.name,
      displayName: entry.displayName,
      description: entry.description,
      iconUrl: entry.iconUrl,
      mountedPath: entry.mountedPath,
      managementPath: entry.managementPath,
      prompts: entry.prompts,
      status: status?.status,
      credentialState: status?.credentialState,
      healthState: status?.healthState,
      actions: status?.actions,
      connected: status?.connected,
      connections: mergeCatalogConnections(entry.connections, status?.connections),
    };
  });
}

export function integrationsFromDirectory(
  directory: AppsDirectory | undefined,
  overlay?: AppConnectionStatus[] | null,
): Integration[] | undefined {
  if (!directory) {
    return undefined;
  }
  if (directory.source === "composed") {
    return directory.integrations;
  }
  return mergeAppCatalogWithConnections(directory.entries, overlay);
}

function mergeCatalogConnections(
  schema: AppCatalogConnection[] | undefined,
  overlay: AppConnectionOverlay[] | undefined,
): ConnectionDefInfo[] | undefined {
  if (!schema && !overlay) {
    return undefined;
  }
  const statusByName = new Map((overlay ?? []).map((row) => [row.name, row]));
  if (!schema || schema.length === 0) {
    return overlay?.map((row) => ({
      name: row.name,
      status: row.status,
      credentialState: row.credentialState,
      healthState: row.healthState,
      actions: row.actions,
      credentialMode: row.credentialMode,
      ownerKind: row.ownerKind,
      instances: row.instances,
      preferredInstance: row.preferredInstance,
      connected: row.connected,
      mcpPassthrough: row.mcpPassthrough,
    }));
  }
  return schema.map((connection) => {
    const status = statusByName.get(connection.name);
    return {
      name: connection.name,
      displayName: connection.displayName,
      authTypes: connection.authTypes,
      connectionParams: connection.connectionParams,
      credentialFields: connection.credentialFields,
      mode: connection.mode,
      status: status?.status,
      credentialState: status?.credentialState,
      healthState: status?.healthState,
      actions: status?.actions,
      credentialMode: status?.credentialMode,
      ownerKind: status?.ownerKind,
      instances: status?.instances,
      preferredInstance: status?.preferredInstance,
      connected: status?.connected,
      mcpPassthrough: connection.mcpPassthrough ?? status?.mcpPassthrough,
    };
  });
}
