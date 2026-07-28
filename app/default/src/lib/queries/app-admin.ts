import {
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { useCallback, useEffect, useRef, useState } from "react";
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
  updateAppAdminRegistryAutoDeploy,
} from "@/lib/api";
import { queryKeys } from "@/lib/query-keys";

export function useAppAdminRegistryQuery(appName: string) {
  const bootstrapPollUntilRef = useRef(0);
  const [bootstrapPollEpoch, setBootstrapPollEpoch] = useState(0);

  useEffect(() => {
    bootstrapPollUntilRef.current = Date.now() + APP_ADMIN_BOOTSTRAP_POLL_MS;
    setBootstrapPollEpoch((epoch) => epoch + 1);
  }, [appName]);

  const query = useQuery({
    queryKey: queryKeys.appAdmin.registry(appName),
    queryFn: () => getAppAdminRegistry(appName),
    retry: (failureCount, error) =>
      !isAPIErrorStatus(error, 403) && failureCount < 1,
    refetchInterval: (query) => {
      void bootstrapPollEpoch;
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

  const checkForNewVersions = useCallback(() => {
    bootstrapPollUntilRef.current = Date.now() + APP_ADMIN_BOOTSTRAP_POLL_MS;
    setBootstrapPollEpoch((epoch) => epoch + 1);
    void query.refetch();
  }, [query]);

  return {
    ...query,
    checkForNewVersions,
  };
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

export function useUpdateAppAdminAutoDeployMutation(appName: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (enabled: boolean) =>
      updateAppAdminRegistryAutoDeploy(appName, enabled),
    onMutate: async (enabled) => {
      await queryClient.cancelQueries({
        queryKey: queryKeys.appAdmin.registry(appName),
      });
      const previous = queryClient.getQueryData<
        Awaited<ReturnType<typeof getAppAdminRegistry>>
      >(queryKeys.appAdmin.registry(appName));
      if (previous) {
        queryClient.setQueryData(queryKeys.appAdmin.registry(appName), {
          ...previous,
          autoDeploy: {
            ...previous.autoDeploy,
            enabled,
            lastError: enabled ? undefined : previous.autoDeploy?.lastError,
            pendingVersion: enabled ? previous.autoDeploy?.pendingVersion : undefined,
          },
        });
      }
      return { previous };
    },
    onError: (_error, _enabled, context) => {
      if (context?.previous) {
        queryClient.setQueryData(
          queryKeys.appAdmin.registry(appName),
          context.previous,
        );
      }
    },
    onSuccess: (response) => {
      queryClient.setQueryData(
        queryKeys.appAdmin.registry(appName),
        (current: Awaited<ReturnType<typeof getAppAdminRegistry>> | undefined) =>
          current
            ? {
                ...current,
                autoDeploy: response.autoDeploy,
              }
            : current,
      );
    },
    onSettled: () =>
      queryClient.invalidateQueries({
        queryKey: queryKeys.appAdmin.registry(appName),
      }),
  });
}
