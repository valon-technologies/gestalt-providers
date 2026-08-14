import { useMutation, useQueries, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  addAuthorizationRelationship,
  deleteAuthorizationRelationship,
  getAppAdminIdentities,
  getAppAuthorizationMembers,
  isAPIErrorStatus,
  listAuthorizationResourceTypes,
  type AuthorizationRelationshipTuple,
} from "@/lib/api";
import { queryKeys } from "@/lib/query-keys";

export function useAppAuthorizationMembersQuery(
  appName: string,
  options?: { enabled?: boolean },
) {
  return useQuery({
    queryKey: queryKeys.appAdmin.members(appName),
    queryFn: () => getAppAuthorizationMembers(appName),
    enabled: options?.enabled ?? true,
    retry: (failureCount, error) =>
      !isAPIErrorStatus(error, 403) && failureCount < 1,
  });
}

export function useAppAdminIdentitiesQuery(
  appName: string,
  options?: { enabled?: boolean },
) {
  return useQuery({
    queryKey: queryKeys.appAdmin.identities(appName),
    queryFn: () => getAppAdminIdentities(appName),
    enabled: options?.enabled ?? true,
    retry: (failureCount, error) =>
      !isAPIErrorStatus(error, 403) && failureCount < 1,
  });
}

export function useAuthorizationResourceTypesQuery(options?: {
  enabled?: boolean;
}) {
  return useQuery({
    queryKey: queryKeys.authorization.resourceTypes(),
    queryFn: listAuthorizationResourceTypes,
    enabled: options?.enabled ?? true,
    retry: (failureCount, error) =>
      !isAPIErrorStatus(error, 403) && failureCount < 1,
  });
}

export function useAddAppAccessMutation(appName: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (tuple: AuthorizationRelationshipTuple) =>
      addAuthorizationRelationship(tuple),
    onSettled: () =>
      queryClient.invalidateQueries({
        queryKey: queryKeys.appAdmin.members(appName),
      }),
  });
}

export function useDeleteAppAccessMutation(appName: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (tuple: AuthorizationRelationshipTuple) =>
      deleteAuthorizationRelationship(tuple),
    onSettled: () =>
      queryClient.invalidateQueries({
        queryKey: queryKeys.appAdmin.members(appName),
      }),
  });
}

export function useAppAuthorizationMembersQueries(appNames: string[]) {
  return useQueries({
    queries: appNames.map((appName) => ({
      queryKey: queryKeys.appAdmin.members(appName),
      queryFn: () => getAppAuthorizationMembers(appName),
      retry: (failureCount: number, error: Error) =>
        !isAPIErrorStatus(error, 403) && failureCount < 1,
    })),
  });
}
