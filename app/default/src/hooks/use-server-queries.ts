import {
  useQuery,
  useQueryClient,
  type UseQueryOptions,
} from "@tanstack/react-query";
import {
  getIntegrationOperations,
  getIntegrations,
  type APIToken,
  type Integration,
  type IntegrationOperation,
} from "@/lib/api";
import { queryKeys } from "@/lib/queries/client";
import { fetchPersonalTokenList } from "@/lib/queries/tokens";

export function useIntegrationsQuery(
  options?: Omit<
    UseQueryOptions<Integration[], Error>,
    "queryKey" | "queryFn"
  >,
) {
  return useQuery({
    queryKey: queryKeys.integrations,
    queryFn: getIntegrations,
    ...options,
  });
}

export function useTokensQuery(
  options?: Omit<UseQueryOptions<APIToken[], Error>, "queryKey" | "queryFn">,
) {
  return useQuery({
    queryKey: queryKeys.tokens,
    queryFn: fetchPersonalTokenList,
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
    queryKey: queryKeys.integrationOperations(appName),
    queryFn: () => getIntegrationOperations(appName),
    enabled: Boolean(appName) && (options?.enabled ?? true),
    ...options,
  });
}

export function useInvalidateIntegrations() {
  const queryClient = useQueryClient();
  return () =>
    queryClient.invalidateQueries({ queryKey: queryKeys.integrations });
}

export function useInvalidateTokens() {
  const queryClient = useQueryClient();
  return () => queryClient.invalidateQueries({ queryKey: queryKeys.tokens });
}
