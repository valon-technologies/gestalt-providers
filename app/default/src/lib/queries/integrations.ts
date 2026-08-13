import { useQuery, useQueryClient, type UseQueryOptions } from "@tanstack/react-query";
import {
  getIntegrationOperations,
  getIntegrations,
  isAPIErrorStatus,
  isAPITimeoutError,
  type Integration,
  type IntegrationOperation,
} from "@/lib/api";
import { queryKeys } from "@/lib/query-keys";

/** Query view for GET /api/v1/apps: loading, ready, or unavailable (may keep cache). */
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

export function useIntegrationsQuery(
  options?: Omit<
    UseQueryOptions<Integration[], Error>,
    "queryKey" | "queryFn"
  >,
) {
  return useQuery({
    ...options,
    queryKey: queryKeys.integrations.list(),
    queryFn: ({ signal }) => getIntegrations(signal),
    retry: options?.retry ?? shouldRetryAppsCatalogQuery,
  });
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
  return () =>
    queryClient.invalidateQueries({ queryKey: queryKeys.integrations.root });
}
