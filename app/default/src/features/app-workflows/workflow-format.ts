import {
  workflowTargetApp,
  type WorkflowDefinitionActivation,
  type WorkflowRun,
  type WorkflowStepTarget,
  type WorkflowTarget,
} from "@/lib/api";
import { normalizeWorkflowStatus } from "@/lib/api";

export function formatDate(value?: string): string {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

export function targetLabel(target: WorkflowTarget): string {
  if (target.steps.length === 0) return "";
  if (target.steps.length === 1) return stepLabel(target.steps[0]);
  const app = workflowTargetApp(target);
  if (app.name && app.operation) {
    return `${app.name}.${app.operation} (+${target.steps.length - 1})`;
  }
  return `${target.steps.length} steps`;
}

export function stepLabel(step: WorkflowStepTarget): string {
  if (step.app?.name && step.app.operation) {
    return `${step.app.name}.${step.app.operation}`;
  }
  if (step.agent?.provider) {
    return `agent:${step.agent.provider}`;
  }
  return step.id || "step";
}

export function stepKind(step: WorkflowStepTarget): string {
  if (step.app) return "app";
  if (step.agent) return "agent";
  return "unknown";
}

export function runTriggerLabel(run: WorkflowRun): string {
  const kind = run.trigger?.kind?.trim();
  if (!kind) return "";
  if (run.trigger?.activationId) {
    return `${kind}:${run.trigger.activationId}`;
  }
  if (run.trigger?.event?.type) {
    return `${kind}:${run.trigger.event.type}`;
  }
  return kind;
}

/** Actor label from createdBy, or empty when the API omitted a creator. */
export function runActorLabel(run: WorkflowRun): string {
  const subject = run.createdBy?.subjectId?.trim();
  if (!subject) return "";
  return subject.replace(/^[^:]+:/, "");
}

/**
 * Compact list subtitle for how/who started a run. Omits invented
 * "unknown" / "system" fallbacks when trigger or createdBy are absent.
 */
export function runTriggerActorDescription(run: WorkflowRun): string {
  const trigger = runTriggerLabel(run);
  const actor = runActorLabel(run);
  if (trigger && actor) return `${trigger} by ${actor}`;
  if (trigger) return trigger;
  if (actor) return `Started by ${actor}`;
  return "";
}

type TemporalRunHandle = {
  kind?: string;
  run_workflow_id?: string;
  run_temporal_run_id?: string;
  owner_key?: string;
};

/** Decode a public Temporal run handle (`base64url` JSON), or null if not one. */
export function decodeTemporalRunHandle(id: string): TemporalRunHandle | null {
  const trimmed = id.trim();
  if (!trimmed) return null;
  try {
    const padded = trimmed.replace(/-/g, "+").replace(/_/g, "/");
    const padLen = (4 - (padded.length % 4)) % 4;
    const json = atob(padded + "=".repeat(padLen));
    const parsed = JSON.parse(json) as TemporalRunHandle;
    if (
      typeof parsed !== "object" ||
      parsed == null ||
      parsed.kind !== "temporal-run"
    ) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

/**
 * Short display id for lists. Temporal public ids are base64url JSON whose
 * prefix/suffix are shared (`{"kind":…` / `owner_key`), so naive slice
 * collision — prefer the unique Temporal run id when present.
 */
export function shortRunId(id: string): string {
  const handle = decodeTemporalRunHandle(id);
  const temporalRunId = handle?.run_temporal_run_id?.trim();
  if (temporalRunId) {
    const parts = temporalRunId.split("-").filter(Boolean);
    const last = parts[parts.length - 1];
    if (last && last.length >= 8) return last;
    if (temporalRunId.length <= 12) return temporalRunId;
    return `${temporalRunId.slice(0, 8)}…`;
  }
  const workflowId = handle?.run_workflow_id?.trim();
  if (workflowId) {
    if (workflowId.length <= 16) return workflowId;
    return `${workflowId.slice(0, 6)}…${workflowId.slice(-6)}`;
  }
  if (id.length <= 24) return id;
  return `${id.slice(0, 10)}…${id.slice(-8)}`;
}

/**
 * Primary list title. Flat lists use the target or definition so rows are
 * distinguishable. Grouped-by-definition lists already name the definition
 * on the section header, so the row is just the short run id.
 */
export function workflowRunListTitle(
  run: Pick<WorkflowRun, "id" | "definitionId" | "target">,
  options?: { groupedByDefinition?: boolean },
): string {
  if (options?.groupedByDefinition) return shortRunId(run.id);
  return targetLabel(run.target) || run.definitionId || shortRunId(run.id);
}

/** Route param for run detail links — short display id when the public id is a Temporal handle. */
export function workflowRunPathId(publicId: string): string {
  const trimmed = publicId.trim();
  if (!trimmed) return trimmed;
  return shortRunId(trimmed);
}

function workflowRunIdStorageKey(app: string, routeId: string): string {
  return `gestalt.workflowRunId:${app}:${routeId}`;
}

/** Remember public handle ↔ short route id so refresh/deep links in-session resolve. */
export function rememberWorkflowRunPublicId(app: string, publicId: string): void {
  const trimmed = publicId.trim();
  if (!app || !trimmed) return;
  const short = shortRunId(trimmed);
  try {
    sessionStorage.setItem(workflowRunIdStorageKey(app, short), trimmed);
    const temporalRunId = decodeTemporalRunHandle(trimmed)?.run_temporal_run_id?.trim();
    if (temporalRunId) {
      sessionStorage.setItem(workflowRunIdStorageKey(app, temporalRunId), trimmed);
    }
  } catch {
    // private mode / quota — resolution still works from in-memory list cache
  }
}

function lookupRememberedWorkflowRunPublicId(
  app: string,
  routeId: string,
): string | null {
  try {
    return sessionStorage.getItem(workflowRunIdStorageKey(app, routeId));
  } catch {
    return null;
  }
}

/**
 * Resolve a route `$runId` (short, temporal UUID, or full handle) to the public
 * API run id. GetRun still requires the full Temporal handle.
 */
export function resolveWorkflowRunPublicId(
  app: string,
  routeId: string,
  knownRuns: Iterable<{ id: string }> = [],
): string {
  const trimmed = routeId.trim();
  if (!trimmed) return trimmed;
  if (decodeTemporalRunHandle(trimmed)) {
    rememberWorkflowRunPublicId(app, trimmed);
    return trimmed;
  }
  for (const run of knownRuns) {
    if (!run?.id) continue;
    if (run.id === trimmed || shortRunId(run.id) === trimmed) {
      rememberWorkflowRunPublicId(app, run.id);
      return run.id;
    }
    const temporalRunId = decodeTemporalRunHandle(run.id)?.run_temporal_run_id?.trim();
    if (temporalRunId && temporalRunId === trimmed) {
      rememberWorkflowRunPublicId(app, run.id);
      return run.id;
    }
  }
  const remembered = lookupRememberedWorkflowRunPublicId(app, trimmed);
  if (remembered) return remembered;
  return trimmed;
}

export function shortDefinitionId(id: string): string {
  if (id.length <= 32) return id;
  return `${id.slice(0, 14)}…${id.slice(-10)}`;
}

export function capitalize(value: string): string {
  if (!value) return value;
  return value.charAt(0).toUpperCase() + value.slice(1);
}

export function prettyJSON(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

export function hasJSONValue(value: unknown): boolean {
  if (value == null) return false;
  if (typeof value === "object" && !Array.isArray(value)) {
    return Object.keys(value as object).length > 0;
  }
  return true;
}

export function workflowRunCounts(runs: WorkflowRun[]) {
  return {
    running: runs.filter(
      (run) => normalizeWorkflowStatus(run.status) === "running",
    ).length,
    succeeded: runs.filter(
      (run) => normalizeWorkflowStatus(run.status) === "succeeded",
    ).length,
    failed: runs.filter(
      (run) => normalizeWorkflowStatus(run.status) === "failed",
    ).length,
  };
}

export function runSearchTerms(run: WorkflowRun): string[] {
  const terms = [
    run.id,
    shortRunId(run.id),
    decodeTemporalRunHandle(run.id)?.run_temporal_run_id,
    run.provider,
    run.status,
    run.definitionId,
    run.statusMessage,
    runTriggerLabel(run),
    ...run.target.steps.flatMap((step) => [
      step.id,
      step.app?.name,
      step.app?.operation,
      step.agent?.provider,
      step.agent?.model,
    ]),
  ];
  return terms
    .filter((value): value is string => typeof value === "string" && !!value)
    .map((value) => value.toLowerCase());
}

export function activationTriggerLabel(
  activation: WorkflowDefinitionActivation,
): string {
  const kind =
    activation.trigger?.kind?.toLowerCase() ||
    activation.trigger?.case?.toLowerCase() ||
    "unknown";
  if (kind === "schedule" || kind === "cron") {
    const parts = [
      activation.trigger?.cron,
      activation.trigger?.timezone,
    ].filter(Boolean);
    return parts.length > 0 ? `schedule · ${parts.join(" ")}` : "schedule";
  }
  if (kind === "event") {
    const parts = [
      activation.trigger?.eventType,
      activation.trigger?.eventSource,
    ].filter(Boolean);
    return parts.length > 0 ? `event · ${parts.join(" / ")}` : "event";
  }
  return capitalize(kind);
}
