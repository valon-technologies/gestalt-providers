import type { WorkflowRun, WorkflowStatus } from "@/lib/api";
import { normalizeWorkflowStatus } from "@/lib/api";
import type { WorkflowRunStatusCounts } from "@/lib/workflowApi";

/** How the Runs list is arranged — layout only, not a filter. */
export type WorkflowRunsGroupBy = "none" | "definition";

export const WORKFLOW_RUNS_GROUP_BY_LABEL = "Group by definition";

/**
 * Worst-outcome rollup: failed → canceled → running → pending → succeeded
 * (skipped counts as ok alongside succeeded).
 */
export function rollupWorkflowRunGroupStatus(
  runs: readonly Pick<WorkflowRun, "status">[],
): WorkflowStatus {
  if (runs.length === 0) return "unknown";
  const normalized = runs.map((run) => normalizeWorkflowStatus(run.status));
  if (normalized.some((status) => status === "failed")) return "failed";
  if (normalized.some((status) => status === "canceled")) return "canceled";
  if (normalized.some((status) => status === "running")) return "running";
  if (normalized.some((status) => status === "pending")) return "pending";
  if (normalized.every((status) => status === "skipped")) return "skipped";
  if (
    normalized.every(
      (status) => status === "succeeded" || status === "skipped",
    )
  ) {
    return "succeeded";
  }
  return normalized[0] ?? "unknown";
}

/**
 * Same worst-outcome priority over a server histogram so a truncated first
 * page cannot hide failures the API already counted.
 */
export function rollupWorkflowStatusCounts(
  counts: WorkflowRunStatusCounts,
): WorkflowStatus {
  if (counts.failed > 0) return "failed";
  if (counts.canceled > 0) return "canceled";
  if (counts.running > 0) return "running";
  if (counts.pending > 0) return "pending";
  if (counts.succeeded > 0) return "succeeded";
  return "unknown";
}

/**
 * Group header health. Prefer the ListRuns histogram when the visible set is
 * the same corpus the histogram describes. Client-only filters cannot use it.
 * Truncated lists without a histogram stay unknown (do not paint succeeded
 * from page 1).
 */
export function rollupWorkflowRunGroupHeaderStatus(opts: {
  clientOnlyFilters: boolean;
  hasMore: boolean;
  loadedRuns: readonly Pick<WorkflowRun, "status">[];
  statusCounts?: WorkflowRunStatusCounts | null;
}): WorkflowStatus {
  if (opts.clientOnlyFilters) {
    if (opts.hasMore) return "unknown";
    return rollupWorkflowRunGroupStatus(opts.loadedRuns);
  }
  if (opts.statusCounts) return rollupWorkflowStatusCounts(opts.statusCounts);
  if (opts.hasMore) return "unknown";
  return rollupWorkflowRunGroupStatus(opts.loadedRuns);
}

export function parseWorkflowRunsGroupBy(
  raw: string | undefined,
): WorkflowRunsGroupBy {
  return raw?.trim() === "definition" ? "definition" : "none";
}

export function serializeWorkflowRunsGroupBy(
  groupBy: WorkflowRunsGroupBy,
): string | undefined {
  return groupBy === "definition" ? "definition" : undefined;
}

/**
 * Definition ids for grouped Runs: prefer inventory order, then append any
 * activity-only ids (definitions that produced runs but are not listed yet).
 */
export function mergeWorkflowDefinitionIds(
  inventoryIds: readonly string[],
  activityIds: readonly string[],
): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const raw of [...inventoryIds, ...activityIds]) {
    const id = raw.trim();
    if (!id || seen.has(id)) continue;
    seen.add(id);
    out.push(id);
  }
  return out;
}
