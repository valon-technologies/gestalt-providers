import { Link } from "@tanstack/react-router";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/cn";
import { listItemInteraction } from "@/lib/list-item-interaction";
import type {
  WorkflowJob,
  WorkflowJobStage,
  WorkflowJobStep,
  WorkflowRunGraph,
} from "./workflow-run-graph";
import { formatDuration } from "./workflow-run-graph";
import { WorkflowStatusIcon } from "./workflow-status-icon";

/**
 * GitHub Actions-inspired run graph.
 * Supports sequential stages and parallel job groups (UI-ready even when the
 * backend only emits a single sequential job today).
 */
export function WorkflowRunJobGraph({
  appName,
  runId,
  graph,
  definitionLabel,
  triggerLabel,
}: {
  appName: string;
  runId: string;
  graph: WorkflowRunGraph;
  definitionLabel?: string;
  triggerLabel?: string;
}) {
  return (
    <Card
      variant="outline"
      className="overflow-x-auto border-0 bg-card"
      data-testid="workflow-run-job-graph"
    >
      <CardHeader className="pb-3">
        <CardTitle className="font-mono text-sm font-medium">
          {definitionLabel || "workflow"}
        </CardTitle>
        {triggerLabel ? (
          <CardDescription>on: {triggerLabel}</CardDescription>
        ) : null}
      </CardHeader>
      <CardContent className="pt-0">
        <div className="flex min-w-max flex-col gap-4 lg:flex-row lg:items-stretch lg:gap-0">
          {graph.stages.map((stage, index) => (
            <div key={stage.id} className="flex items-stretch lg:contents">
              {index > 0 ? <StageConnector /> : null}
              <StageBlock appName={appName} runId={runId} stage={stage} />
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

function StageConnector() {
  return (
    <div
      className="flex items-center justify-center py-2 lg:w-10 lg:py-0"
      aria-hidden
    >
      <div className="h-8 w-px bg-border lg:h-px lg:w-full lg:self-center" />
    </div>
  );
}

function StageBlock({
  appName,
  runId,
  stage,
}: {
  appName: string;
  runId: string;
  stage: WorkflowJobStage;
}) {
  if (stage.kind === "parallel" && stage.jobs.length > 1) {
    return (
      <div
        className="w-full min-w-[16rem] max-w-md overflow-hidden rounded-lg bg-background"
        data-testid={`workflow-stage-${stage.id}`}
      >
        <ul className="divide-y divide-border/60">
          {stage.jobs.map((job) => (
            <li key={job.id}>
              <JobRow appName={appName} runId={runId} job={job} compact />
            </li>
          ))}
        </ul>
      </div>
    );
  }

  const job = stage.jobs[0];
  if (!job) return null;
  return (
    <div
      className="w-full min-w-[16rem] max-w-md"
      data-testid={`workflow-stage-${stage.id}`}
    >
      <JobCard appName={appName} runId={runId} job={job} />
    </div>
  );
}

function JobCard({
  appName,
  runId,
  job,
}: {
  appName: string;
  runId: string;
  job: WorkflowJob;
}) {
  return (
    <div className="overflow-hidden rounded-lg bg-background">
      <JobRow appName={appName} runId={runId} job={job} />
      {job.steps.length > 0 ? (
        <ul className="border-t border-border/60 px-1 py-1">
          {job.steps.map((step) => (
            <li key={step.id}>
              <StepRow
                appName={appName}
                runId={runId}
                jobId={job.id}
                step={step}
              />
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}

function JobRow({
  appName,
  runId,
  job,
  compact = false,
}: {
  appName: string;
  runId: string;
  job: WorkflowJob;
  compact?: boolean;
}) {
  const firstStep = job.steps[0];
  const to = firstStep
    ? {
        to: "/apps/$app/admin/workflows/runs/$runId/jobs/$jobId/steps/$stepId" as const,
        params: {
          app: appName,
          runId,
          jobId: job.id,
          stepId: firstStep.id,
        },
      }
    : {
        to: "/apps/$app/admin/workflows/runs/$runId" as const,
        params: { app: appName, runId },
      };

  return (
    <Link
      {...to}
      className={cn(
        "flex items-center gap-2 px-3 py-2 text-sm",
        listItemInteraction({ pointer: "css" }),
        compact && "rounded-md",
      )}
    >
      <WorkflowStatusIcon status={job.status} />
      <span className="min-w-0 flex-1 truncate font-medium text-foreground">
        {job.name}
      </span>
      <span className="shrink-0 text-xs text-muted-foreground tabular-nums">
        {formatDuration(job.durationMs)}
      </span>
    </Link>
  );
}

function StepRow({
  appName,
  runId,
  jobId,
  step,
}: {
  appName: string;
  runId: string;
  jobId: string;
  step: WorkflowJobStep;
}) {
  return (
    <Link
      to="/apps/$app/admin/workflows/runs/$runId/jobs/$jobId/steps/$stepId"
      params={{ app: appName, runId, jobId, stepId: step.id }}
      className={cn(
        "flex items-center gap-2 rounded-md px-2 py-1.5 text-sm",
        listItemInteraction({ pointer: "css" }),
      )}
      data-testid={`workflow-step-${step.id}`}
    >
      <WorkflowStatusIcon status={step.status} size="sm" />
      <span className="min-w-0 flex-1 truncate text-foreground/90">
        {step.name}
      </span>
      <span className="shrink-0 text-xs text-muted-foreground tabular-nums">
        {formatDuration(step.durationMs)}
      </span>
    </Link>
  );
}
