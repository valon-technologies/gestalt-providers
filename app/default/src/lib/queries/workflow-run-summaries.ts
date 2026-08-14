import { useQuery, useQueryClient } from "@tanstack/react-query";
import type { WorkflowRun } from "@/lib/api";
import { queryKeys } from "@/lib/query-keys";

/**
 * Merge ListRuns pages into one app-scoped run index. Incoming rows replace
 * the same id so grouped and flat lists share identity for route resolution
 * and breadcrumbs.
 */
export function mergeWorkflowRunSummaries(
  existing: readonly WorkflowRun[],
  incoming: readonly WorkflowRun[],
): WorkflowRun[] {
  if (incoming.length === 0) return existing.slice();
  const byId = new Map<string, WorkflowRun>();
  for (const run of existing) {
    if (run.id) byId.set(run.id, run);
  }
  for (const run of incoming) {
    if (run.id) byId.set(run.id, run);
  }
  return [...byId.values()];
}

export function useWorkflowRunSummaries(appName: string): WorkflowRun[] {
  const queryClient = useQueryClient();
  const data = useQuery({
    queryKey: queryKeys.workflows.runSummaries(appName),
    queryFn: async () =>
      queryClient.getQueryData<WorkflowRun[]>(
        queryKeys.workflows.runSummaries(appName),
      ) ?? [],
    staleTime: Infinity,
    initialData: () =>
      queryClient.getQueryData<WorkflowRun[]>(
        queryKeys.workflows.runSummaries(appName),
      ) ?? [],
  }).data;
  return data ?? [];
}
