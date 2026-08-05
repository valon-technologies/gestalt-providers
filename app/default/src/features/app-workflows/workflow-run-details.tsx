import type { ReactNode } from "react";
import { Link } from "@tanstack/react-router";
import type {
  WorkflowRun,
  WorkflowStepExecution,
  WorkflowStepTarget,
  WorkflowTarget,
} from "@/lib/api";
import { WorkflowStatusBadge } from "./workflow-status-badge";
import {
  formatDate,
  hasJSONValue,
  prettyJSON,
  runTriggerLabel,
  stepKind,
} from "./workflow-format";

export function WorkflowRunDetails({
  run,
  appName,
}: {
  run: WorkflowRun;
  appName: string;
}) {
  return (
    <div className="space-y-5">
      <DetailGrid
        items={[
          ["Provider", run.provider],
          [
            "Definition",
            run.definitionId ? (
              <Link
                to="/apps/$app/admin/workflows/definitions/$definitionId"
                params={{ app: appName, definitionId: run.definitionId }}
                className="break-all text-foreground underline underline-offset-2 hover:text-foreground/80"
              >
                {run.definitionId}
              </Link>
            ) : (
              "—"
            ),
          ],
          ["Trigger", runTriggerLabel(run)],
          ["Created", formatDate(run.createdAt)],
          ["Started", formatDate(run.startedAt)],
          ["Completed", formatDate(run.completedAt)],
          ["Created by", run.createdBy?.subjectId || "—"],
        ]}
      />
      <WorkflowTargetDetails target={run.target} />
      {run.steps && run.steps.length > 0 ? (
        <StepExecutions steps={run.steps} />
      ) : null}
      {hasJSONValue(run.input) ? (
        <JSONSection title="Input" value={run.input} />
      ) : null}
      {hasJSONValue(run.output) ? (
        <JSONSection title="Output" value={run.output} />
      ) : null}
    </div>
  );
}

export function WorkflowTargetDetails({ target }: { target: WorkflowTarget }) {
  if (target.steps.length === 0) {
    return (
      <div>
        <SectionHeading>Target</SectionHeading>
        <p className="mt-2 text-sm text-muted-foreground/70">No target steps.</p>
      </div>
    );
  }

  return (
    <div>
      <SectionHeading>Target steps</SectionHeading>
      <ul className="mt-3 space-y-3">
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

function DetailGrid({
  items,
}: {
  items: Array<[string, ReactNode]>;
}) {
  return (
    <dl className="grid gap-3 sm:grid-cols-2">
      {items.map(([label, value]) => (
        <div key={label}>
          <dt className="text-xs font-medium text-muted-foreground">{label}</dt>
          <dd className="mt-1 break-all text-sm text-foreground">{value}</dd>
        </div>
      ))}
    </dl>
  );
}

function TargetStepDetails({ step }: { step: WorkflowStepTarget }) {
  return (
    <div className="space-y-1 text-sm">
      <DetailLine label="Step" value={step.id || "—"} />
      <DetailLine label="Kind" value={stepKind(step)} />
      {step.app ? (
        <DetailLine
          label="App"
          value={`${step.app.name}.${step.app.operation}`}
        />
      ) : null}
      {step.app?.connection ? (
        <DetailLine label="Connection" value={step.app.connection} />
      ) : null}
      {step.agent ? (
        <DetailLine
          label="Agent"
          value={`${step.agent.provider || "agent"} / ${step.agent.model || "model"}`}
        />
      ) : null}
      {step.timeoutSeconds != null ? (
        <DetailLine label="Timeout" value={`${step.timeoutSeconds}s`} />
      ) : null}
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
    <div>
      <SectionHeading>Step executions</SectionHeading>
      <ul className="mt-3 space-y-3">
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
    <div className={compact ? "mt-3" : undefined}>
      <SectionHeading>{title}</SectionHeading>
      <pre
        className={`mt-2 overflow-x-auto rounded-md border border-border bg-accent p-3 font-mono text-xs text-foreground ${
          compact ? "max-h-48" : "max-h-80"
        }`}
      >
        {prettyJSON(value)}
      </pre>
    </div>
  );
}

function SectionHeading({ children }: { children: ReactNode }) {
  return (
    <h4 className="text-xs font-medium tracking-wide text-muted-foreground">
      {children}
    </h4>
  );
}

function DetailLine({ label, value }: { label: string; value: string }) {
  return (
    <p className="text-sm text-foreground">
      <span className="text-muted-foreground">{label}: </span>
      {value}
    </p>
  );
}
