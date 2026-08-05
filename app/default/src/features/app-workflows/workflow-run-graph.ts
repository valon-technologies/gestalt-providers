import {
  normalizeWorkflowStatus,
  type WorkflowRun,
  type WorkflowStepExecution,
  type WorkflowStepTarget,
} from "@/lib/api";
import { stepLabel } from "./workflow-format";

/** Forward-compatible CI graph: sequential stages, optional parallel job groups. */
export type WorkflowJobStatus =
  | "pending"
  | "running"
  | "succeeded"
  | "failed"
  | "canceled"
  | "skipped"
  | "unknown";

export type WorkflowLogLine = {
  /** 1-based line number when provided by the backend. */
  number?: number;
  text: string;
  level?: "info" | "warning" | "error" | "debug";
};

/** One collapsible block in a GHA-style step log viewer. */
export type WorkflowLogGroup = {
  id: string;
  name: string;
  status?: WorkflowJobStatus | string;
  durationMs?: number | null;
  lines: WorkflowLogLine[];
  /** When true, start collapsed (GHA often collapses succeeded setup). */
  defaultCollapsed?: boolean;
};

export type WorkflowJobStep = {
  id: string;
  name: string;
  status?: WorkflowJobStatus | string;
  durationMs?: number | null;
  startedAt?: string;
  completedAt?: string;
  statusMessage?: string;
  skipReason?: string;
};

export type WorkflowJob = {
  id: string;
  name: string;
  status?: WorkflowJobStatus | string;
  durationMs?: number | null;
  startedAt?: string;
  completedAt?: string;
  steps: WorkflowJobStep[];
};

/**
 * A stage in the run graph.
 * - `sequential`: one job (typical today).
 * - `parallel`: several jobs shown as a grouped block (UI-ready; unused by
 *   current sequential backend execution).
 */
export type WorkflowJobStage = {
  id: string;
  kind: "sequential" | "parallel";
  jobs: WorkflowJob[];
};

export type WorkflowRunGraph = {
  stages: WorkflowJobStage[];
  /** Total run duration when timestamps allow. */
  durationMs: number | null;
};

export type WorkflowStepLogPayload = {
  runId: string;
  jobId: string;
  stepId: string;
  groups: WorkflowLogGroup[];
};

export function durationMsBetween(
  startedAt?: string,
  completedAt?: string,
): number | null {
  if (!startedAt || !completedAt) return null;
  const start = Date.parse(startedAt);
  const end = Date.parse(completedAt);
  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) {
    return null;
  }
  return end - start;
}

export function formatDuration(ms: number | null | undefined): string {
  if (ms == null || !Number.isFinite(ms) || ms < 0) return "—";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  const totalSeconds = Math.round(ms / 1000);
  if (totalSeconds < 60) return `${totalSeconds}s`;
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes < 60) {
    return seconds === 0 ? `${minutes}m` : `${minutes}m ${seconds}s`;
  }
  const hours = Math.floor(minutes / 60);
  const remMinutes = minutes % 60;
  return remMinutes === 0 ? `${hours}h` : `${hours}h ${remMinutes}m`;
}

export function formatRelativeTime(
  value?: string,
  nowMs: number = Date.now(),
): string {
  if (!value) return "—";
  const then = Date.parse(value);
  if (!Number.isFinite(then)) return value;
  const deltaSec = Math.round((nowMs - then) / 1000);
  const abs = Math.abs(deltaSec);
  const rtf = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
  if (abs < 60) return rtf.format(-deltaSec, "second");
  if (abs < 3600) return rtf.format(-Math.round(deltaSec / 60), "minute");
  if (abs < 86400) return rtf.format(-Math.round(deltaSec / 3600), "hour");
  if (abs < 86400 * 30) return rtf.format(-Math.round(deltaSec / 86400), "day");
  return rtf.format(-Math.round(deltaSec / (86400 * 30)), "month");
}

function normalizeStatus(status?: string): WorkflowJobStatus | string {
  return normalizeWorkflowStatus(status);
}

function rollupStatus(
  statuses: Array<WorkflowJobStatus | string | undefined>,
): WorkflowJobStatus | string {
  const normalized = statuses.map((s) => normalizeStatus(s));
  if (normalized.some((s) => s === "failed")) return "failed";
  if (normalized.some((s) => s === "canceled")) return "canceled";
  if (normalized.some((s) => s === "running")) return "running";
  if (normalized.some((s) => s === "pending")) return "pending";
  if (normalized.length > 0 && normalized.every((s) => s === "skipped")) {
    return "skipped";
  }
  if (normalized.every((s) => s === "succeeded" || s === "skipped")) {
    return "succeeded";
  }
  return normalized[0] || "unknown";
}

function stepNameFromTarget(
  stepId: string,
  targets: WorkflowStepTarget[],
  index: number,
): string {
  const target =
    targets.find((step) => step.id === stepId) || targets[index] || undefined;
  if (!target) return stepId || `Step ${index + 1}`;
  const label = stepLabel(target);
  return target.id ? `${target.id} · ${label}` : label;
}

function projectStep(
  execution: WorkflowStepExecution | undefined,
  target: WorkflowStepTarget | undefined,
  index: number,
): WorkflowJobStep {
  const id =
    execution?.stepId?.trim() ||
    target?.id?.trim() ||
    `step-${index + 1}`;
  const name = target
    ? stepNameFromTarget(id, [target], 0)
    : id;
  return {
    id,
    name,
    status: normalizeStatus(execution?.status),
    durationMs: durationMsBetween(execution?.startedAt, execution?.completedAt),
    startedAt: execution?.startedAt,
    completedAt: execution?.completedAt,
    statusMessage: execution?.statusMessage,
    skipReason: execution?.skipReason,
  };
}

/**
 * Prefer an explicit `stages` payload when the backend starts emitting jobs;
 * otherwise project today's ordered `steps` into one sequential job.
 */
export function projectWorkflowRunGraph(run: WorkflowRun): WorkflowRunGraph {
  const explicit = run.stages;
  if (explicit && explicit.length > 0) {
    return {
      stages: explicit.map((stage, stageIndex) => ({
        id: stage.id || `stage-${stageIndex + 1}`,
        kind: stage.kind === "parallel" ? "parallel" : "sequential",
        jobs: stage.jobs.map((job, jobIndex) => ({
          ...job,
          id: job.id || `job-${jobIndex + 1}`,
          status: normalizeStatus(job.status),
          durationMs:
            job.durationMs ??
            durationMsBetween(job.startedAt, job.completedAt),
          steps: job.steps.map((step, stepIndex) => ({
            ...step,
            id: step.id || `step-${stepIndex + 1}`,
            status: normalizeStatus(step.status),
            durationMs:
              step.durationMs ??
              durationMsBetween(step.startedAt, step.completedAt),
          })),
        })),
      })),
      durationMs: durationMsBetween(run.startedAt, run.completedAt),
    };
  }

  const targets = run.target?.steps ?? [];
  const executions = run.steps ?? [];
  const count = Math.max(targets.length, executions.length);
  const steps: WorkflowJobStep[] = [];
  for (let i = 0; i < count; i += 1) {
    steps.push(projectStep(executions[i], targets[i], i));
  }

  const jobId = "workflow";
  const jobName = run.definitionId?.trim() || "Workflow";
  const job: WorkflowJob = {
    id: jobId,
    name: jobName,
    status: normalizeStatus(run.status),
    durationMs: durationMsBetween(run.startedAt, run.completedAt),
    startedAt: run.startedAt,
    completedAt: run.completedAt,
    steps,
  };

  if (steps.length === 0) {
    job.steps = [
      {
        id: "pending",
        name: "Waiting for steps",
        status: normalizeStatus(run.status),
      },
    ];
  } else if (!run.status) {
    job.status = rollupStatus(steps.map((step) => step.status));
  }

  return {
    stages: [{ id: "main", kind: "sequential", jobs: [job] }],
    durationMs: job.durationMs ?? null,
  };
}

export function findJobInGraph(
  graph: WorkflowRunGraph,
  jobId: string,
): WorkflowJob | null {
  for (const stage of graph.stages) {
    const job = stage.jobs.find((item) => item.id === jobId);
    if (job) return job;
  }
  return null;
}

export function findStepInGraph(
  graph: WorkflowRunGraph,
  jobId: string,
  stepId: string,
): { job: WorkflowJob; step: WorkflowJobStep } | null {
  const job = findJobInGraph(graph, jobId);
  if (!job) return null;
  const step = job.steps.find((item) => item.id === stepId);
  if (!step) return null;
  return { job, step };
}

export function flattenJobs(graph: WorkflowRunGraph): WorkflowJob[] {
  return graph.stages.flatMap((stage) => stage.jobs);
}
