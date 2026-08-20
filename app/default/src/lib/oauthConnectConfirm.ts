import type { QueryClient } from "@tanstack/react-query";
import {
  getAppConnections,
  getAppsDirectory,
  type Integration,
} from "@/lib/api";
import { integrationsFromDirectory } from "@/lib/app-catalog";
import { appConnectedCopy } from "@/lib/accountCopy";
import { normalizeIntegrationStatus } from "@/lib/integrationStatus";
import { queryKeys } from "@/lib/query-keys";

export function isConnectedInCatalog(
  catalog: Integration[] | undefined,
  integrationName: string,
): boolean {
  const item = catalog?.find((integration) => integration.name === integrationName);
  return Boolean(item && normalizeIntegrationStatus(item).connected);
}

export function appIsConnectedCopy(label: string): string {
  return appConnectedCopy(label);
}

/**
 * After an OAuth popup closes, tell the operator only if the catalog
 * shows this app as connected. Popup close is not proof of connect.
 */
export function oauthConnectedToastMessage(
  connected: boolean,
  label: string,
): string | null {
  return connected ? appIsConnectedCopy(label) : null;
}

/** Reload directory plus overlay and report whether this app is connected now. */
export async function refetchIntegrationConnected(
  queryClient: QueryClient,
  integrationName: string,
): Promise<boolean> {
  await queryClient.invalidateQueries({ queryKey: queryKeys.integrations.root });
  const directory = await queryClient.fetchQuery({
    queryKey: queryKeys.integrations.directory(),
    queryFn: ({ signal }) => getAppsDirectory(signal),
  });
  const overlay =
    directory.source === "catalog"
      ? await queryClient.fetchQuery({
          queryKey: queryKeys.integrations.connections(),
          queryFn: ({ signal }) => getAppConnections(signal),
        })
      : undefined;
  return isConnectedInCatalog(
    integrationsFromDirectory(directory, overlay),
    integrationName,
  );
}
