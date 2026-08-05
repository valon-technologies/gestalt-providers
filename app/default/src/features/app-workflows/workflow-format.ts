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
  const kind = run.trigger?.kind || "unknown";
  if (run.trigger?.activationId) {
    return `${kind}:${run.trigger.activationId}`;
  }
  if (run.trigger?.event?.type) {
    return `${kind}:${run.trigger.event.type}`;
  }
  return kind;
}

export function shortRunId(id: string): string {
  if (id.length <= 24) return id;
  return `${id.slice(0, 10)}…${id.slice(-8)}`;
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

export function filterRuns(
  runs: WorkflowRun[],
  query: string,
  status: string,
  definitionId?: string,
): WorkflowRun[] {
  const needle = query.trim().toLowerCase();
  const definitionFilter = definitionId?.trim();
  return runs.filter((run) => {
    if (
      definitionFilter &&
      (run.definitionId?.trim() || "") !== definitionFilter
    ) {
      return false;
    }
    if (
      status !== "all" &&
      normalizeWorkflowStatus(run.status) !== normalizeWorkflowStatus(status)
    ) {
      return false;
    }
    if (!needle) return true;
    return runSearchTerms(run).some((term) => term.includes(needle));
  });
}
