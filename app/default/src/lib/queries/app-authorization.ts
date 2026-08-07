import { useQuery } from "@tanstack/react-query";
import {
  getAppAdminIdentities,
  getAppAuthorizationMembers,
  isAPIErrorStatus,
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
