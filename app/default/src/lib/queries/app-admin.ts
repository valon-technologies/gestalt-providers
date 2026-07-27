import {
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { useEffect, useRef } from "react";
import {
  APP_ADMIN_BOOTSTRAP_POLL_MS,
  APP_ADMIN_POLL_INTERVAL_MS,
  shouldPollAppAdminRegistry,
} from "@/features/registry/polling";
import {
  getAppAdminRegistry,
  getAppAdminRegistryHistory,
  isAPIErrorStatus,
  selectAppAdminRegistryVersion,
} from "@/lib/api";
import { queryKeys } from "@/lib/query-keys";

export function useAppAdminRegistryQuery(appName: string) {
  const bootstrapPollUntilRef = useRef(0);

  useEffect(() => {
    bootstrapPollUntilRef.current = Date.now() + APP_ADMIN_BOOTSTRAP_POLL_MS;
  }, [appName]);

  return useQuery({
    queryKey: queryKeys.appAdmin.registry(appName),
    queryFn: () => getAppAdminRegistry(appName),
    retry: (failureCount, error) =>
      !isAPIErrorStatus(error, 403) && failureCount < 1,
    refetchInterval: (query) => {
      const registry = query.state.data;
      if (!registry) return false;
      if (
        shouldPollAppAdminRegistry(registry, bootstrapPollUntilRef.current)
      ) {
        return APP_ADMIN_POLL_INTERVAL_MS;
      }
      return false;
    },
  });
}

export function useAppAdminRegistryHistoryQuery(app: string, enabled: boolean) {
  return useInfiniteQuery({
    queryKey: queryKeys.appAdmin.history(app),
    queryFn: ({ pageParam }) =>
      getAppAdminRegistryHistory(app, {
        limit: 50,
        cursor: pageParam,
      }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (page) => page.nextCursor,
    enabled,
  });
}

export function useDeployAppAdminVersionMutation(appName: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (version: string) =>
      selectAppAdminRegistryVersion(appName, version),
    onSettled: () =>
      queryClient.invalidateQueries({
        queryKey: queryKeys.appAdmin.registry(appName),
      }),
  });
}
