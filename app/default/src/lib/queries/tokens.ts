import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createToken, getTokens, revokeToken } from "@/lib/api";
import { queryKeys } from "@/lib/query-keys";

export function useTokensQuery() {
  return useQuery({
    queryKey: queryKeys.tokens.list(),
    queryFn: getTokens,
  });
}

export function useCreateTokenMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (input: { name: string; scopes: string; expiresIn?: number }) =>
      createToken(input.name, input.scopes, input.expiresIn),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: queryKeys.tokens.root }),
  });
}

export function useRevokeTokenMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => revokeToken(id),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: queryKeys.tokens.root }),
  });
}
