import {
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import type { WorkflowDefinition, WorkflowRun } from "@/lib/api";
import { workflowRunMatchesApp } from "@/lib/workflowActivity";
import {
  cancelWorkflowRun,
  deleteWorkflowDefinition,
  getWorkflowDefinition,
  getWorkflowRun,
  getWorkflowRunEvents,
  getWorkflowRunOutput,
  getWorkflowStepLogs,
  listWorkflowDefinitions,
  listWorkflowRuns,
  setWorkflowActivationPaused,
  setWorkflowDefinitionPaused,
  startWorkflowRun,
} from "@/lib/workflowApi";
import { queryKeys } from "@/lib/query-keys";
import { mergeWorkflowRunSummaries } from "./workflow-run-summaries";

function invalidateDefinitionQueries(
  queryClient: ReturnType<typeof useQueryClient>,
  appName: string,
  definitionId?: string,
) {
  void queryClient.invalidateQueries({
    queryKey: queryKeys.workflows.definitions(appName),
  });
  if (definitionId) {
    void queryClient.invalidateQueries({
      queryKey: queryKeys.workflows.definition(appName, definitionId),
    });
  }
}

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

export function useWorkflowRunsQuery(
  appName: string,
  opts?: {
    status?: string;
    definitionId?: string;
    pageSize?: number;
    enabled?: boolean;
  },
) {
  const queryClient = useQueryClient();
  const status = opts?.status?.trim() || undefined;
  const definitionId = opts?.definitionId?.trim() || undefined;
  const pageSize = opts?.pageSize;
  return useInfiniteQuery({
    queryKey: queryKeys.workflows.listPage(
      appName,
      status ?? "all",
      definitionId ?? "all",
      pageSize ?? "default",
    ),
    queryFn: async ({ pageParam }) => {
      const page = await listWorkflowRuns({
        targetApp: appName,
        pageToken: pageParam,
        status,
        definitionId,
        pageSize,
      });
      queryClient.setQueryData(
        queryKeys.workflows.runSummaries(appName),
        (prev: WorkflowRun[] | undefined) =>
          mergeWorkflowRunSummaries(prev ?? [], page.runs),
      );
      return page;
    },
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (page) => page.nextPageToken,
    enabled: opts?.enabled ?? true,
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

export function useWorkflowRunEventsQuery(
  appName: string,
  runId: string | null,
  listRun?: WorkflowRun,
) {
  return useQuery({
    queryKey: queryKeys.workflows.events(appName, runId ?? ""),
    queryFn: () =>
      getWorkflowRunEvents(runId!, {
        run: listRun,
        targetApp: appName,
      }),
    enabled: Boolean(runId),
  });
}

export function useWorkflowRunOutputQuery(
  appName: string,
  runId: string | null,
  listRun?: WorkflowRun,
  options?: { enabled?: boolean },
) {
  return useQuery({
    queryKey: queryKeys.workflows.output(appName, runId ?? ""),
    queryFn: () =>
      getWorkflowRunOutput(runId!, {
        run: listRun,
        targetApp: appName,
      }),
    enabled: (options?.enabled ?? true) && Boolean(runId),
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

export function useStartWorkflowRunMutation(appName: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      definitionId,
      definition,
    }: {
      definitionId: string;
      definition?: WorkflowDefinition;
    }) =>
      startWorkflowRun(definitionId, {
        targetApp: appName,
        provider: definition?.provider,
        expectedDefinitionGeneration: definition?.generation,
      }),
    onSuccess: (run) => {
      queryClient.setQueryData(
        queryKeys.workflows.detail(appName, run.id),
        run,
      );
      void queryClient.invalidateQueries({
        queryKey: queryKeys.workflows.list(appName),
      });
    },
  });
}

export function useSetWorkflowDefinitionPausedMutation(appName: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      definitionId,
      paused,
      provider,
    }: {
      definitionId: string;
      paused: boolean;
      provider?: string;
    }) =>
      setWorkflowDefinitionPaused(definitionId, {
        paused,
        provider,
        targetApp: appName,
      }),
    onSuccess: (definition) => {
      queryClient.setQueryData(
        queryKeys.workflows.definition(appName, definition.id),
        definition,
      );
      invalidateDefinitionQueries(queryClient, appName, definition.id);
    },
  });
}

export function useSetWorkflowActivationPausedMutation(appName: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      definitionId,
      activationId,
      paused,
      provider,
    }: {
      definitionId: string;
      activationId: string;
      paused: boolean;
      provider?: string;
    }) =>
      setWorkflowActivationPaused(definitionId, {
        activationId,
        paused,
        provider,
        targetApp: appName,
      }),
    onSuccess: (definition) => {
      queryClient.setQueryData(
        queryKeys.workflows.definition(appName, definition.id),
        definition,
      );
      invalidateDefinitionQueries(queryClient, appName, definition.id);
    },
  });
}

export function useDeleteWorkflowDefinitionMutation(appName: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      definitionId,
      provider,
    }: {
      definitionId: string;
      provider?: string;
    }) =>
      deleteWorkflowDefinition(definitionId, {
        provider,
        targetApp: appName,
      }),
    onSuccess: (_void, { definitionId }) => {
      invalidateDefinitionQueries(queryClient, appName, definitionId);
      void queryClient.invalidateQueries({
        queryKey: queryKeys.workflows.list(appName),
      });
    },
  });
}
