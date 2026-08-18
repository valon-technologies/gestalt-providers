import type { QueryClient } from "@tanstack/react-query";
import { getIntegrations, type Integration } from "@/lib/api";
import { normalizeIntegrationStatus } from "@/lib/integrationStatus";
import { queryKeys } from "@/lib/query-keys";

export function isConnectedInCatalog(
  catalog: Integration[] | undefined,
  integrationName: string,
): boolean {
  const item = catalog?.find((integration) => integration.name === integrationName);
  return Boolean(item && normalizeIntegrationStatus(item).connected);
}

/** Reload the catalog and report whether this app is connected now. */
export async function refetchIntegrationConnected(
  queryClient: QueryClient,
  integrationName: string,
): Promise<boolean> {
  await queryClient.invalidateQueries({ queryKey: queryKeys.integrations.root });
  const catalog = await queryClient.fetchQuery({
    queryKey: queryKeys.integrations.list(),
    queryFn: getIntegrations,
  });
  return isConnectedInCatalog(catalog, integrationName);
}
