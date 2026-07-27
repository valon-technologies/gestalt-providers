import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { getTokens, revokeToken } from "@/lib/api";
import { queryKeys } from "@/lib/query-keys";

export function useTokensQuery() {
  return useQuery({
    queryKey: queryKeys.tokens.list(),
    queryFn: getTokens,
  });
}

export function useInvalidateTokens() {
  const queryClient = useQueryClient();
  return () =>
    queryClient.invalidateQueries({ queryKey: queryKeys.tokens.root });
}

export function useRevokeTokenMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => revokeToken(id),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: queryKeys.tokens.root }),
  });
}
