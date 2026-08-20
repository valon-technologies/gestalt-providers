import { Link } from "@tanstack/react-router";
import type {
  WorkflowRun,
  WorkflowStepExecution,
  WorkflowStepTarget,
  WorkflowTarget,
} from "@/lib/api";
import { CodeBlock } from "@/components/ui/code-block";
import {
  DescriptionDetails,
  DescriptionItem,
  DescriptionList,
  DescriptionTerm,
} from "@/components/ui/description-list";
import {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import { WorkflowStatusBadge } from "./workflow-status-badge";
import {
  formatDate,
  hasJSONValue,
  prettyJSON,
  runTriggerLabel,
  stepKind,
} from "./workflow-format";
import { formatDuration, formatRelativeTime } from "./workflow-run-graph";

function sameInstant(left?: string, right?: string): boolean {
  if (!left || !right) return false;
  const a = new Date(left).getTime();
  const b = new Date(right).getTime();
  return !Number.isNaN(a) && !Number.isNaN(b) && a === b;
}

export function WorkflowRunDetails({
  run,
  appName,
  outputOverride,
  durationMs,
}: {
  run: WorkflowRun;
  appName: string;
  /** Prefer dedicated GetRunOutput when inline output is empty. */
  outputOverride?: unknown;
  /** Wall-clock duration for the run (from the projected graph). */
  durationMs?: number | null;
}) {
  const output = outputOverride !== undefined ? outputOverride : run.output;
  const startedAt = run.startedAt || run.createdAt;
  const showCreated =
    Boolean(run.createdAt) &&
    Boolean(run.startedAt) &&
    !sameInstant(run.createdAt, run.startedAt);
  const trigger = runTriggerLabel(run);

  return (
    <div className="space-y-5">
      <DescriptionList
        variant="stacked"
        divided={false}
        className="grid gap-x-6 gap-y-1 sm:grid-cols-2"
      >
        <DescriptionItem>
          <DescriptionTerm>Status</DescriptionTerm>
          <DescriptionDetails>
            <WorkflowStatusBadge status={run.status} size="default" />
          </DescriptionDetails>
        </DescriptionItem>
        <DescriptionItem>
          <DescriptionTerm>Duration</DescriptionTerm>
          <DescriptionDetails>
            {formatDuration(durationMs ?? null)}
          </DescriptionDetails>
        </DescriptionItem>
        <DescriptionItem>
          <DescriptionTerm>Trigger</DescriptionTerm>
          <DescriptionDetails>{trigger || "—"}</DescriptionDetails>
        </DescriptionItem>
        <DescriptionItem>
          <DescriptionTerm>Started</DescriptionTerm>
          <DescriptionDetails>
            {formatDate(startedAt)}
            {startedAt ? (
              <span className="ml-2 text-muted-foreground">
                {formatRelativeTime(startedAt)}
              </span>
            ) : null}
          </DescriptionDetails>
        </DescriptionItem>
        {showCreated ? (
          <DescriptionItem>
            <DescriptionTerm>Created</DescriptionTerm>
            <DescriptionDetails>{formatDate(run.createdAt)}</DescriptionDetails>
          </DescriptionItem>
        ) : null}
        <DescriptionItem>
          <DescriptionTerm>Completed</DescriptionTerm>
          <DescriptionDetails>
            {formatDate(run.completedAt)}
          </DescriptionDetails>
        </DescriptionItem>
        <DescriptionItem>
          <DescriptionTerm>Provider</DescriptionTerm>
          <DescriptionDetails>{run.provider || "—"}</DescriptionDetails>
        </DescriptionItem>
        <DescriptionItem>
          <DescriptionTerm>Definition</DescriptionTerm>
          <DescriptionDetails>
            {run.definitionId ? (
              <Link
                to="/apps/$app/admin/workflows/definitions/$definitionId"
                params={{ app: appName, definitionId: run.definitionId }}
                className="break-all text-foreground underline underline-offset-2 hover:text-foreground/80"
              >
                {run.definitionId}
              </Link>
            ) : (
              "—"
            )}
          </DescriptionDetails>
        </DescriptionItem>
        <DescriptionItem>
          <DescriptionTerm>Run as</DescriptionTerm>
          <DescriptionDetails mono={Boolean(run.runAs)}>
            {run.runAs || "—"}
          </DescriptionDetails>
        </DescriptionItem>
        <DescriptionItem>
          <DescriptionTerm>Created by</DescriptionTerm>
          <DescriptionDetails>
            {run.createdBy?.subjectId || "—"}
          </DescriptionDetails>
        </DescriptionItem>
      </DescriptionList>
      <WorkflowTargetDetails target={run.target} />
      {run.steps && run.steps.length > 0 ? (
        <StepExecutions steps={run.steps} />
      ) : null}
      {hasJSONValue(run.input) ? (
        <JSONSection title="Input" value={run.input} />
      ) : null}
      {hasJSONValue(output) ? (
        <JSONSection title="Output" value={output} />
      ) : null}
    </div>
  );
}

export function WorkflowTargetDetails({ target }: { target: WorkflowTarget }) {
  if (target.steps.length === 0) {
    return (
      <div className="space-y-3">
        <SectionHeader>
          <SectionHeaderContent size="sm">
            <SectionHeaderTitle as="h3">Target</SectionHeaderTitle>
          </SectionHeaderContent>
        </SectionHeader>
        <p className="text-sm text-muted-foreground/70">No target steps.</p>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <SectionHeader>
        <SectionHeaderContent size="sm">
          <SectionHeaderTitle as="h3">Target steps</SectionHeaderTitle>
        </SectionHeaderContent>
      </SectionHeader>
      <ul className="space-y-3">
        {target.steps.map((step, index) => (
          <li
            key={step.id || `step-${index}`}
            className="rounded-md border border-border px-3 py-2"
          >
            <TargetStepDetails step={step} />
          </li>
        ))}
      </ul>
    </div>
  );
}

function TargetStepDetails({ step }: { step: WorkflowStepTarget }) {
  return (
    <div className="space-y-3">
      <DescriptionList termWidth="6.5rem">
        <DescriptionItem>
          <DescriptionTerm>Step</DescriptionTerm>
          <DescriptionDetails mono>{step.id || "—"}</DescriptionDetails>
        </DescriptionItem>
        <DescriptionItem>
          <DescriptionTerm>Kind</DescriptionTerm>
          <DescriptionDetails>{stepKind(step)}</DescriptionDetails>
        </DescriptionItem>
        {step.app ? (
          <DescriptionItem>
            <DescriptionTerm>App</DescriptionTerm>
            <DescriptionDetails mono>
              {`${step.app.name}.${step.app.operation}`}
            </DescriptionDetails>
          </DescriptionItem>
        ) : null}
        {step.app?.connection ? (
          <DescriptionItem>
            <DescriptionTerm>Sign-in method</DescriptionTerm>
            <DescriptionDetails>{step.app.connection}</DescriptionDetails>
          </DescriptionItem>
        ) : null}
        {step.agent ? (
          <DescriptionItem>
            <DescriptionTerm>Agent</DescriptionTerm>
            <DescriptionDetails>
              {`${step.agent.provider || "agent"} / ${step.agent.model || "model"}`}
            </DescriptionDetails>
          </DescriptionItem>
        ) : null}
        {step.timeoutSeconds != null ? (
          <DescriptionItem>
            <DescriptionTerm>Timeout</DescriptionTerm>
            <DescriptionDetails>{`${step.timeoutSeconds}s`}</DescriptionDetails>
          </DescriptionItem>
        ) : null}
      </DescriptionList>
      {hasJSONValue(step.inputs) ? (
        <JSONSection title="Inputs" value={step.inputs} compact />
      ) : null}
      {hasJSONValue(step.when) ? (
        <JSONSection title="When" value={step.when} compact />
      ) : null}
      {hasJSONValue(step.metadata) ? (
        <JSONSection title="Metadata" value={step.metadata} compact />
      ) : null}
    </div>
  );
}

/** Definition + config for one target step (recipe side of a run step). */
export function WorkflowStepDefinitionPanel({
  step,
}: {
  step: WorkflowStepTarget | null | undefined;
}) {
  if (!step) {
    return (
      <p className="text-sm text-muted-foreground/70">
        No step definition found on this run target.
      </p>
    );
  }
  return <TargetStepDetails step={step} />;
}

function StepExecutions({ steps }: { steps: WorkflowStepExecution[] }) {
  return (
    <div className="space-y-3">
      <SectionHeader>
        <SectionHeaderContent size="sm">
          <SectionHeaderTitle as="h3">Step executions</SectionHeaderTitle>
        </SectionHeaderContent>
      </SectionHeader>
      <ul className="space-y-3">
        {steps.map((step, index) => (
          <li
            key={step.stepId || `execution-${index}`}
            className="rounded-md border border-border px-3 py-3"
          >
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-sm font-medium text-foreground">
                {step.stepId || `Step ${index + 1}`}
              </span>
              <WorkflowStatusBadge status={step.status} />
            </div>
            {step.statusMessage ? (
              <p className="mt-1 text-xs text-muted-foreground">
                {step.statusMessage}
              </p>
            ) : null}
            {step.skipReason ? (
              <p className="mt-1 text-xs text-muted-foreground">
                Skipped: {step.skipReason}
              </p>
            ) : null}
            {hasJSONValue(step.input) ? (
              <JSONSection title="Input" value={step.input} compact />
            ) : null}
            {hasJSONValue(step.output) ? (
              <JSONSection title="Output" value={step.output} compact />
            ) : null}
          </li>
        ))}
      </ul>
    </div>
  );
}

function JSONSection({
  title,
  value,
  compact = false,
}: {
  title: string;
  value: unknown;
  compact?: boolean;
}) {
  return (
    <div className={compact ? "mt-3 space-y-2" : "space-y-3"}>
      <SectionHeader>
        <SectionHeaderContent size="sm">
          <SectionHeaderTitle as="h3">{title}</SectionHeaderTitle>
        </SectionHeaderContent>
      </SectionHeader>
      <CodeBlock
        chrome="inset"
        language="json"
        code={prettyJSON(value)}
        scrollable
        maxHeight={compact ? 192 : 320}
        copyLabel={`Copy ${title.toLowerCase()}`}
      />
    </div>
  );
}
