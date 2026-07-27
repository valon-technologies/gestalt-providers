import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  cancelWorkflowRun,
  getWorkflowRun,
  getWorkflowRuns,
} from "@/lib/api";
import { queryKeys } from "@/lib/query-keys";

export function useWorkflowRunsQuery() {
  return useQuery({
    queryKey: queryKeys.workflows.list(),
    queryFn: getWorkflowRuns,
  });
}

export function useWorkflowRunQuery(runId: string | null) {
  return useQuery({
    queryKey: queryKeys.workflows.detail(runId ?? ""),
    queryFn: () => getWorkflowRun(runId!),
    enabled: !!runId,
  });
}

export function useCancelWorkflowRunMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ id, reason }: { id: string; reason?: string }) =>
      cancelWorkflowRun(id, reason),
    onSuccess: (_data, { id }) => {
      void queryClient.invalidateQueries({ queryKey: queryKeys.workflows.root });
      void queryClient.invalidateQueries({
        queryKey: queryKeys.workflows.detail(id),
      });
    },
  });
}
