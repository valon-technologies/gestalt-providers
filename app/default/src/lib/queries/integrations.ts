import { useQuery, useQueryClient, type UseQueryOptions } from "@tanstack/react-query";
import {
  getIntegrationOperations,
  getIntegrations,
  type Integration,
  type IntegrationOperation,
} from "@/lib/api";
import { queryKeys } from "@/lib/query-keys";

export function useIntegrationsQuery(
  options?: Omit<
    UseQueryOptions<Integration[], Error>,
    "queryKey" | "queryFn"
  >,
) {
  return useQuery({
    queryKey: queryKeys.integrations.list(),
    queryFn: getIntegrations,
    ...options,
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
