import { useMutation, useQueries, useQuery, useQueryClient } from "@tanstack/react-query";
import { probeGestaltAdminAccess } from "@/features/admin-access/admin-access-gate";
import {
  addAuthorizationRelationship,
  deleteAuthorizationRelationship,
  getAdminMetricsText,
  getAdminPlatformAdmins,
  getAdminRegistryApp,
  getAppAdminMetrics,
  isAPIErrorStatus,
  listAdminRegistryApps,
  type AuthorizationRelationshipTuple,
} from "@/lib/api";
import { queryKeys } from "@/lib/query-keys";

export function useGestaltAdminQuery(options?: { enabled?: boolean }) {
  return useQuery({
    queryKey: queryKeys.admin.access(),
    queryFn: () => probeGestaltAdminAccess(),
    enabled: options?.enabled ?? true,
    staleTime: Infinity,
    refetchOnWindowFocus: false,
    retry: false,
  });
}

export function useAdminPlatformAdminsQuery(options?: { enabled?: boolean }) {
  return useQuery({
    queryKey: queryKeys.admin.platformAdmins(),
    queryFn: getAdminPlatformAdmins,
    enabled: options?.enabled ?? true,
    retry: (failureCount, error) =>
      !isAPIErrorStatus(error, 403) &&
      !isAPIErrorStatus(error, 503) &&
      failureCount < 1,
  });
}

export function useAddPlatformAdminMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (tuple: AuthorizationRelationshipTuple) =>
      addAuthorizationRelationship(tuple),
    onSettled: () =>
      queryClient.invalidateQueries({
        queryKey: queryKeys.admin.platformAdmins(),
      }),
  });
}

export function useDeletePlatformAdminMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (tuple: AuthorizationRelationshipTuple) =>
      deleteAuthorizationRelationship(tuple),
    onSettled: () =>
      queryClient.invalidateQueries({
        queryKey: queryKeys.admin.platformAdmins(),
      }),
  });
}

export function useAdminRegistryAppsQuery() {
  return useQuery({
    queryKey: queryKeys.admin.versions(),
    queryFn: listAdminRegistryApps,
    retry: (failureCount, error) =>
      !isAPIErrorStatus(error, 403) &&
      !isAPIErrorStatus(error, 503) &&
      failureCount < 1,
  });
}

export function useAdminRegistryAppQuery(
  app: string,
  options?: { refetchInterval?: number },
) {
  return useQuery({
    queryKey: queryKeys.admin.version(app),
    queryFn: () => getAdminRegistryApp(app),
    enabled: Boolean(app),
    refetchInterval: options?.refetchInterval,
    retry: (failureCount, error) =>
      !isAPIErrorStatus(error, 403) &&
      !isAPIErrorStatus(error, 404) &&
      !isAPIErrorStatus(error, 503) &&
      failureCount < 1,
  });
}

export function useAdminRegistryAppDetailsQueries(apps: string[]) {
  return useQueries({
    queries: apps.map((app) => ({
      queryKey: queryKeys.admin.version(app),
      queryFn: () => getAdminRegistryApp(app),
      enabled: Boolean(app),
      retry: (failureCount: number, error: Error) =>
        !isAPIErrorStatus(error, 403) &&
        !isAPIErrorStatus(error, 404) &&
        !isAPIErrorStatus(error, 503) &&
        failureCount < 1,
    })),
  });
}

export function useAdminMetricsQuery() {
  return useQuery({
    queryKey: queryKeys.admin.metrics(),
    queryFn: getAdminMetricsText,
    retry: (failureCount, error) =>
      !isAPIErrorStatus(error, 403) &&
      !isAPIErrorStatus(error, 503) &&
      failureCount < 1,
  });
}

export function useAppAdminMetricsQuery(app: string, options?: { enabled?: boolean }) {
  return useQuery({
    queryKey: queryKeys.appAdmin.metrics(app),
    queryFn: () => getAppAdminMetrics(app),
    enabled: Boolean(app) && (options?.enabled ?? true),
    retry: (failureCount, error) =>
      !isAPIErrorStatus(error, 403) &&
      !isAPIErrorStatus(error, 503) &&
      failureCount < 1,
  });
}
