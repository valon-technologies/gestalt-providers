import { useMemo } from "react";
import {
  useQuery,
  useQueryClient,
  type QueryClient,
  type UseQueryOptions,
} from "@tanstack/react-query";
import {
  getAppConnections,
  getAppsDirectory,
  getIntegrationOperations,
  isAPIErrorStatus,
  isAPITimeoutError,
  type AppConnectionStatus,
  type AppsDirectory,
  type Integration,
  type IntegrationOperation,
} from "@/lib/api";
import {
  applyDisconnectToIntegrations,
  type IntegrationDisconnectSpec,
} from "@/lib/applyIntegrationDisconnect";
import { integrationsFromDirectory } from "@/lib/app-catalog";
import { queryKeys } from "@/lib/query-keys";

/** Query view for the apps directory: loading, ready, or unavailable (may keep cache). */
export type AppsCatalogQueryStatus =
  | { status: "loading"; integrations: Integration[] }
  | { status: "unavailable"; error: Error; integrations: Integration[] }
  | { status: "ready"; integrations: Integration[] };

export function appsCatalogQueryStatus(query: {
  isPending: boolean;
  error: Error | null;
  data: Integration[] | undefined;
}): AppsCatalogQueryStatus {
  const integrations = query.data ?? [];
  if (query.error) {
    return { status: "unavailable", error: query.error, integrations };
  }
  if (query.isPending) {
    return { status: "loading", integrations };
  }
  return { status: "ready", integrations };
}

export function shouldRetryAppsCatalogQuery(
  failureCount: number,
  error: Error,
): boolean {
  if (isAPITimeoutError(error) || isAPIErrorStatus(error, 503)) {
    return false;
  }
  return failureCount < 1;
}

/** Workspace pages need overlay status; the Apps catalog paints without it. */
export function workspaceIntegrationsPending(
  directoryPending: boolean,
  overlayPending: boolean,
): boolean {
  return directoryPending || overlayPending;
}

export function useAppsDirectoryQuery(
  options?: Omit<
    UseQueryOptions<AppsDirectory, Error>,
    "queryKey" | "queryFn"
  >,
) {
  return useQuery({
    ...options,
    queryKey: queryKeys.integrations.directory(),
    queryFn: ({ signal }) => getAppsDirectory(signal),
    retry: options?.retry ?? shouldRetryAppsCatalogQuery,
  });
}

export function useAppConnectionsQuery(
  options?: Omit<
    UseQueryOptions<AppConnectionStatus[], Error>,
    "queryKey" | "queryFn"
  > & { enabled?: boolean },
) {
  return useQuery({
    queryKey: queryKeys.integrations.connections(),
    queryFn: ({ signal }) => getAppConnections(signal),
    retry: options?.retry ?? shouldRetryAppsCatalogQuery,
    ...options,
  });
}

export function useIntegrationsQuery(
  options?: Omit<
    UseQueryOptions<AppsDirectory, Error>,
    "queryKey" | "queryFn"
  >,
) {
  const directoryQuery = useAppsDirectoryQuery(options);
  const overlayEnabled =
    (options?.enabled ?? true) && directoryQuery.data?.source === "catalog";
  const connectionsQuery = useAppConnectionsQuery({
    enabled: overlayEnabled,
  });
  const data = useMemo(
    () =>
      integrationsFromDirectory(directoryQuery.data, connectionsQuery.data),
    [directoryQuery.data, connectionsQuery.data],
  );

  return {
    ...directoryQuery,
    data,
    overlayPending: overlayEnabled && connectionsQuery.isPending,
    overlayError: overlayEnabled ? connectionsQuery.error : null,
    refetch: () => directoryQuery.refetch(),
  };
}

export function useIntegrationOperationsQuery(
  appName: string,
  options?: Omit<
    UseQueryOptions<IntegrationOperation[], Error>,
    "queryKey" | "queryFn"
  > & { enabled?: boolean },
) {
  return useQuery({
    queryKey: queryKeys.integrations.operations(appName),
    queryFn: () => getIntegrationOperations(appName),
    enabled: Boolean(appName) && (options?.enabled ?? true),
    ...options,
  });
}

export function useInvalidateIntegrations() {
  const queryClient = useQueryClient();
  return (): Promise<void> =>
    queryClient.invalidateQueries({ queryKey: queryKeys.integrations.root });
}

/** Write a confirmed disconnect into the catalog cache before the refetch. */
export async function commitIntegrationDisconnect(
  queryClient: QueryClient,
  integrationName: string,
  spec: IntegrationDisconnectSpec,
): Promise<void> {
  await queryClient.cancelQueries({ queryKey: queryKeys.integrations.list() });
  queryClient.setQueryData<Integration[]>(
    queryKeys.integrations.list(),
    (current) =>
      current
        ? applyDisconnectToIntegrations(current, integrationName, spec)
        : current,
  );
}
