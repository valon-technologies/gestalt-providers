import {
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import type { WorkflowRun } from "@/lib/api";
import { workflowRunMatchesApp } from "@/lib/workflowActivity";
import {
  cancelWorkflowRun,
  getWorkflowDefinition,
  getWorkflowRun,
  getWorkflowStepLogs,
  listWorkflowDefinitions,
  listWorkflowRuns,
} from "@/lib/workflowApi";
import { queryKeys } from "@/lib/query-keys";

export function useWorkflowDefinitionsQuery(appName: string) {
  return useQuery({
    queryKey: queryKeys.workflows.definitions(appName),
    queryFn: () => listWorkflowDefinitions({ targetApp: appName }),
  });
}

export function useWorkflowDefinitionQuery(
  appName: string,
  definitionId: string | null,
) {
  return useQuery({
    queryKey: queryKeys.workflows.definition(appName, definitionId ?? ""),
    queryFn: () =>
      getWorkflowDefinition(definitionId!, { targetApp: appName }),
    enabled: Boolean(definitionId),
  });
}

export function useWorkflowRunsQuery(appName: string) {
  return useInfiniteQuery({
    queryKey: queryKeys.workflows.list(appName),
    queryFn: ({ pageParam }) =>
      listWorkflowRuns({
        targetApp: appName,
        pageToken: pageParam,
      }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (page) => page.nextPageToken,
  });
}

export function useWorkflowRunQuery(
  appName: string,
  runId: string | null,
  listRun?: WorkflowRun,
) {
  return useQuery({
    queryKey: queryKeys.workflows.detail(appName, runId ?? ""),
    queryFn: async () => {
      const run = await getWorkflowRun(runId!, {
        run: listRun,
        targetApp: appName,
      });
      if (!workflowRunMatchesApp(run, appName)) {
        throw new Error("This workflow run is not available in this app.");
      }
      return run;
    },
    enabled: Boolean(runId),
    placeholderData: listRun,
  });
}

export function useWorkflowStepLogsQuery(
  appName: string,
  runId: string | null,
  jobId: string | null,
  stepId: string | null,
  listRun?: WorkflowRun,
) {
  return useQuery({
    queryKey: queryKeys.workflows.stepLogs(
      appName,
      runId ?? "",
      jobId ?? "",
      stepId ?? "",
    ),
    queryFn: () =>
      getWorkflowStepLogs(runId!, {
        jobId: jobId!,
        stepId: stepId!,
        run: listRun,
        targetApp: appName,
      }),
    enabled: Boolean(runId && jobId && stepId),
  });
}

export function useCancelWorkflowRunMutation(appName: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      id,
      reason,
      run,
    }: {
      id: string;
      reason?: string;
      run: WorkflowRun;
    }) => cancelWorkflowRun(id, reason, { run, targetApp: appName }),
    onSuccess: (canceled, { id }) => {
      queryClient.setQueryData(
        queryKeys.workflows.detail(appName, id),
        canceled,
      );
      void queryClient.invalidateQueries({
        queryKey: queryKeys.workflows.list(appName),
      });
    },
  });
}
