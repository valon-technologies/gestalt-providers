import { useMemo } from "react";
import {
  useQuery,
  useQueryClient,
  type QueryClient,
  type UseQueryOptions,
} from "@tanstack/react-query";
import {
  getAppConnections,
  getAppAccess,
  getAppsDirectory,
  getIntegrationOperations,
  isAPIErrorStatus,
  isAPITimeoutError,
  type AppConnectionStatus,
  type AppAccessProfile,
  type AppsDirectory,
  type Integration,
  type IntegrationOperation,
} from "@/lib/api";
import {
  applyDisconnectToConnectionStatuses,
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

/**
 * Overlay owns product-connected status. Composed listings already include it.
 * Catalog-source rows are unknown until the overlay query succeeds.
 */
export function connectionOverlayKnown(
  overlayEnabled: boolean,
  overlayPending: boolean,
  overlayError: Error | null,
): boolean {
  if (!overlayEnabled) {
    return true;
  }
  return !overlayPending && overlayError == null;
}

export type WorkspaceConnectionView =
  | { status: "loading" }
  | { status: "overlay_unavailable"; error: Error }
  | { status: "ready" };

/** Gate for app workspace surfaces that show connection status. */
export function workspaceConnectionView(input: {
  directoryPending: boolean;
  overlayPending: boolean;
  overlayError: Error | null;
}): WorkspaceConnectionView {
  if (
    workspaceIntegrationsPending(input.directoryPending, input.overlayPending)
  ) {
    return { status: "loading" };
  }
  if (input.overlayError) {
    return { status: "overlay_unavailable", error: input.overlayError };
  }
  return { status: "ready" };
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
    overlayEnabled,
    overlayPending: overlayEnabled && connectionsQuery.isPending,
    overlayError: overlayEnabled ? connectionsQuery.error : null,
    overlayFetching: overlayEnabled && connectionsQuery.isFetching,
    refetchDirectory: () => directoryQuery.refetch(),
    refetchOverlay: () => connectionsQuery.refetch(),
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

export function useAppAccessQuery(
  appName: string,
  options?: Omit<
    UseQueryOptions<AppAccessProfile, Error>,
    "queryKey" | "queryFn"
  > & { enabled?: boolean },
) {
  return useQuery({
    queryKey: queryKeys.integrations.access(appName),
    queryFn: () => getAppAccess(appName),
    enabled: Boolean(appName) && (options?.enabled ?? true),
    ...options,
  });
}

export function useInvalidateIntegrations() {
  const queryClient = useQueryClient();
  return (): Promise<void> =>
    queryClient.invalidateQueries({ queryKey: queryKeys.integrations.root });
}

/** Write a confirmed disconnect into overlay (or composed listing) before refetch. */
export async function commitIntegrationDisconnect(
  queryClient: QueryClient,
  integrationName: string,
  spec: IntegrationDisconnectSpec,
): Promise<void> {
  await queryClient.cancelQueries({ queryKey: queryKeys.integrations.root });
  queryClient.setQueryData<AppConnectionStatus[]>(
    queryKeys.integrations.connections(),
    (current) =>
      current
        ? applyDisconnectToConnectionStatuses(current, integrationName, spec)
        : current,
  );
  queryClient.setQueryData<AppsDirectory>(
    queryKeys.integrations.directory(),
    (current) => {
      if (!current || current.source !== "composed") {
        return current;
      }
      return {
        ...current,
        integrations: applyDisconnectToIntegrations(
          current.integrations,
          integrationName,
          spec,
        ),
      };
    },
  );
}
