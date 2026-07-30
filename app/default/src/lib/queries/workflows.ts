import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { WorkflowRun } from "@/lib/api";
import { workflowRunMatchesApp } from "@/lib/workflowActivity";
import {
  cancelWorkflowRun,
  getWorkflowRun,
  listWorkflowRuns,
} from "@/lib/workflowApi";
import { queryKeys } from "@/lib/query-keys";

export function useWorkflowRunsQuery(appName: string) {
  return useQuery({
    queryKey: queryKeys.workflows.list(appName),
    queryFn: () => listWorkflowRuns({ targetApp: appName }),
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
        throw new Error("This workflow run does not belong to this app.");
      }
      return run;
    },
    enabled: Boolean(runId),
    placeholderData: listRun,
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
