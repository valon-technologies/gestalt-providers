import { useQuery, useQueryClient } from "@tanstack/react-query";
import { getIntegrations } from "@/lib/api";
import { queryKeys } from "@/lib/query-keys";

export function useIntegrationsQuery() {
  return useQuery({
    queryKey: queryKeys.integrations.list(),
    queryFn: getIntegrations,
  });
}

export function useInvalidateIntegrations() {
  const queryClient = useQueryClient();
  return () =>
    queryClient.invalidateQueries({ queryKey: queryKeys.integrations.root });
}
