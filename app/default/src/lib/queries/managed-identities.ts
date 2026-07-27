import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { QueryClient } from "@tanstack/react-query";
import {
  createManagedIdentity,
  createManagedIdentityToken,
  deleteManagedIdentity,
  deleteManagedIdentityGrant,
  deleteManagedIdentityMember,
  getManagedIdentities,
  getManagedIdentity,
  getManagedIdentityGrants,
  getManagedIdentityIntegrations,
  getManagedIdentityMembers,
  getManagedIdentityTokens,
  putManagedIdentityGrant,
  putManagedIdentityMember,
  revokeManagedIdentityToken,
  updateManagedIdentity,
  type AccessPermission,
  type ManagedIdentityGrant,
} from "@/lib/api";
import { queryKeys } from "@/lib/query-keys";

export function useManagedIdentitiesQuery(enabled = true) {
  return useQuery({
    queryKey: queryKeys.managedIdentities.list(),
    queryFn: getManagedIdentities,
    enabled,
  });
}

export function useManagedIdentityQuery(identityId: string | null) {
  return useQuery({
    queryKey: queryKeys.managedIdentities.detail(identityId ?? ""),
    queryFn: () => getManagedIdentity(identityId!),
    enabled: !!identityId,
  });
}

export function useManagedIdentityMembersQuery(identityId: string | null) {
  return useQuery({
    queryKey: queryKeys.managedIdentities.members(identityId ?? ""),
    queryFn: () => getManagedIdentityMembers(identityId!),
    enabled: !!identityId,
  });
}

export function useManagedIdentityGrantsQuery(identityId: string | null) {
  return useQuery({
    queryKey: queryKeys.managedIdentities.grants(identityId ?? ""),
    queryFn: () => getManagedIdentityGrants(identityId!),
    enabled: !!identityId,
  });
}

export function useManagedIdentityTokensQuery(identityId: string | null) {
  return useQuery({
    queryKey: queryKeys.managedIdentities.tokens(identityId ?? ""),
    queryFn: () => getManagedIdentityTokens(identityId!),
    enabled: !!identityId,
  });
}

export function useManagedIdentityIntegrationsQuery(identityId: string | null) {
  return useQuery({
    queryKey: queryKeys.managedIdentities.integrations(identityId ?? ""),
    queryFn: () => getManagedIdentityIntegrations(identityId!),
    enabled: !!identityId,
    retry: false,
  });
}

function invalidateManagedIdentity(queryClient: QueryClient, id: string) {
  void queryClient.invalidateQueries({
    queryKey: queryKeys.managedIdentities.detail(id),
  });
  void queryClient.invalidateQueries({
    queryKey: queryKeys.managedIdentities.members(id),
  });
  void queryClient.invalidateQueries({
    queryKey: queryKeys.managedIdentities.grants(id),
  });
  void queryClient.invalidateQueries({
    queryKey: queryKeys.managedIdentities.tokens(id),
  });
  void queryClient.invalidateQueries({
    queryKey: queryKeys.managedIdentities.integrations(id),
  });
}

export function useCreateManagedIdentityMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (input: { id: string; displayName: string }) =>
      createManagedIdentity(input.id, input.displayName),
    onSuccess: () =>
      queryClient.invalidateQueries({
        queryKey: queryKeys.managedIdentities.root,
      }),
  });
}

export function useUpdateManagedIdentityMutation(identityId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (displayName: string) =>
      updateManagedIdentity(identityId, displayName),
    onSuccess: () => invalidateManagedIdentity(queryClient, identityId),
  });
}

export function useDeleteManagedIdentityMutation(identityId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: () => deleteManagedIdentity(identityId),
    onSuccess: () =>
      queryClient.invalidateQueries({
        queryKey: queryKeys.managedIdentities.root,
      }),
  });
}

export function usePutManagedIdentityMemberMutation(identityId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (input: { email: string; role: ManagedIdentityGrant["role"] }) =>
      putManagedIdentityMember(identityId, input.email, input.role),
    onSuccess: () => invalidateManagedIdentity(queryClient, identityId),
  });
}

export function useDeleteManagedIdentityMemberMutation(identityId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (memberSubjectId: string) =>
      deleteManagedIdentityMember(identityId, memberSubjectId),
    onSuccess: () => invalidateManagedIdentity(queryClient, identityId),
  });
}

export function usePutManagedIdentityGrantMutation(identityId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (input: {
      plugin: string;
      role: ManagedIdentityGrant["role"];
    }) => putManagedIdentityGrant(identityId, input.plugin, input.role),
    onSuccess: () => invalidateManagedIdentity(queryClient, identityId),
  });
}

export function useInvalidateManagedIdentity(identityId: string) {
  const queryClient = useQueryClient();
  return () => invalidateManagedIdentity(queryClient, identityId);
}

export function useDeleteManagedIdentityGrantMutation(identityId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (plugin: string) =>
      deleteManagedIdentityGrant(identityId, plugin),
    onSuccess: () => invalidateManagedIdentity(queryClient, identityId),
  });
}

export function useCreateManagedIdentityTokenMutation(identityId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (input: { name: string; permissions?: AccessPermission[] }) =>
      createManagedIdentityToken(identityId, input.name, input.permissions),
    onSuccess: () => invalidateManagedIdentity(queryClient, identityId),
  });
}

export function useRevokeManagedIdentityTokenMutation(identityId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (tokenId: string) =>
      revokeManagedIdentityToken(identityId, tokenId),
    onSuccess: () => invalidateManagedIdentity(queryClient, identityId),
  });
}
