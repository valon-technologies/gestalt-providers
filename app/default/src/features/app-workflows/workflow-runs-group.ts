import type { WorkflowRun, WorkflowStatus } from "@/lib/api";
import { normalizeWorkflowStatus } from "@/lib/api";

/** How the Runs list is arranged — layout only, not a filter. */
export type WorkflowRunsGroupBy = "none" | "definition";

export const WORKFLOW_RUNS_GROUP_BY_LABEL = "Group by definition";

export type WorkflowRunDefinitionGroup = {
  /** Empty string when the run has no definitionId. */
  definitionId: string;
  /** Full definition id (or fallback copy) — do not mid-ellipsis shorten. */
  label: string;
  runs: WorkflowRun[];
};

/**
 * Bucket runs by definition, preserving first-seen order (matches newest-first
 * list order → groups ordered by most recent activity).
 */
export function groupWorkflowRunsByDefinition(
  runs: readonly WorkflowRun[],
): WorkflowRunDefinitionGroup[] {
  const order: string[] = [];
  const buckets = new Map<string, WorkflowRun[]>();

  for (const run of runs) {
    const definitionId = run.definitionId?.trim() || "";
    const existing = buckets.get(definitionId);
    if (existing) {
      existing.push(run);
      continue;
    }
    order.push(definitionId);
    buckets.set(definitionId, [run]);
  }

  return order.map((definitionId) => ({
    definitionId,
    label: definitionId || "Unknown definition",
    runs: buckets.get(definitionId) ?? [],
  }));
}

/**
 * Aggregate status for a definition group’s loaded runs. Worst outcome wins
 * (same priority as job/step rollup): failed → canceled → running → pending →
 * succeeded (skipped counts as ok alongside succeeded).
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
