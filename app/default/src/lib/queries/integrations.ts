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

function shouldRetryAppsCatalogQuery(failureCount: number, error: Error): boolean {
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
