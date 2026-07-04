
import {
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import type {
  WorkflowRun,
  WorkflowStepExecution,
  WorkflowStepTarget,
  WorkflowTarget,
} from "@/lib/api";
import {
  cancelWorkflowRun,
  getWorkflowRun,
  getWorkflowRuns,
  workflowTargetApp,
} from "@/lib/api";
import AuthGuard from "@/components/AuthGuard";
import Container from "@/components/Container";
import Nav from "@/components/Nav";

const RUN_STATUSES = ["all", "pending", "running", "succeeded", "failed", "canceled"];

export default function WorkflowsPage() {
  const [runs, setRuns] = useState<WorkflowRun[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [runsError, setRunsError] = useState<string | null>(null);

  const [selectedRunID, setSelectedRunID] = useState<string | null>(null);
  const [selectedRun, setSelectedRun] = useState<WorkflowRun | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [canceling, setCanceling] = useState(false);

  const [runsQuery, setRunsQuery] = useState("");
  const [runStatus, setRunStatus] = useState("all");
  const deferredRunsQuery = useDeferredValue(runsQuery);
  const runsRef = useRef<WorkflowRun[]>([]);

  useEffect(() => {
    runsRef.current = runs;
  }, [runs]);

  useEffect(() => {
    let active = true;
    if (refreshNonce > 0) {
      setRefreshing(true);
    }

    getWorkflowRuns()
      .then((value) => {
        if (!active) return;
        setRuns(value);
        setRunsError(null);
      })
      .catch((err) => {
        if (!active) return;
        setRunsError(errorMessage(err, "Failed to load workflow runs"));
      })
      .finally(() => {
        if (!active) return;
        setLoading(false);
        setRefreshing(false);
      });

    return () => {
      active = false;
    };
  }, [refreshNonce]);

  const filteredRuns = useMemo(
    () => filterRuns(runs, deferredRunsQuery, runStatus),
    [runs, deferredRunsQuery, runStatus],
  );

  useEffect(() => {
    if (filteredRuns.length === 0) {
      setSelectedRunID(null);
      return;
    }
    if (!selectedRunID || !filteredRuns.some((run) => run.id === selectedRunID)) {
      setSelectedRunID(filteredRuns[0].id);
    }
  }, [filteredRuns, selectedRunID]);

  useEffect(() => {
    if (!selectedRunID) {
      setSelectedRun(null);
      setDetailError(null);
      return;
    }

    const existing = runsRef.current.find((run) => run.id === selectedRunID) ?? null;
    setSelectedRun(existing);
    setDetailLoading(true);
    setDetailError(null);
    setActionError(null);

    let active = true;
    getWorkflowRun(selectedRunID)
      .then((run) => {
        if (!active) return;
        setSelectedRun(run);
        setRuns((current) => upsertRun(current, run));
      })
      .catch((err) => {
        if (!active) return;
        setDetailError(errorMessage(err, "Failed to load workflow run"));
      })
      .finally(() => {
        if (!active) return;
        setDetailLoading(false);
      });

    return () => {
      active = false;
    };
  }, [selectedRunID]);

  async function handleCancelSelectedRun() {
    if (!selectedRun || canceling) return;
    setCanceling(true);
    setActionError(null);

    try {
      const canceled = await cancelWorkflowRun(
        selectedRun.id,
        "Canceled from Gestalt UI",
      );
      setSelectedRun(canceled);
      setRuns((current) => upsertRun(current, canceled));
    } catch (err) {
      setActionError(errorMessage(err, "Failed to cancel workflow run"));
    } finally {
      setCanceling(false);
    }
  }

  const counts = workflowRunCounts(runs);

  return (
    <AuthGuard>
      <div className="min-h-screen">
        <Nav />
        <Container as="main" className="py-10">
          <div className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
            <div>
              <span className="label-text">Workflows</span>
              <h1 className="mt-2 text-2xl font-heading text-primary">
                Workflows
              </h1>
              <p className="mt-2 max-w-2xl text-sm text-muted">
                Inspect durable workflow run history, step state, and captured
                inputs and outputs.
              </p>
            </div>
            <button
              type="button"
              onClick={() => setRefreshNonce((value) => value + 1)}
              disabled={refreshing}
              className="inline-flex items-center justify-center rounded-md border border-alpha px-4 py-2 text-sm font-medium text-primary transition-colors duration-150 hover:border-alpha-strong hover:bg-alpha-5 disabled:cursor-not-allowed disabled:opacity-60"
            >
              {refreshing ? "Refreshing..." : "Refresh"}
            </button>
          </div>

          <div className="mt-8 grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
            <SummaryCard label="Runs" value={String(runs.length)} tone="default" />
            <SummaryCard
              label="Running"
              value={String(counts.running)}
              tone="sky"
            />
            <SummaryCard
              label="Succeeded"
              value={String(counts.succeeded)}
              tone="grove"
            />
            <SummaryCard label="Failed" value={String(counts.failed)} tone="ember" />
          </div>

          <section className="mt-8 rounded-lg border border-alpha bg-base-100 p-4 dark:bg-surface">
            <div className="grid gap-4 md:grid-cols-[minmax(0,1fr)_12rem]">
              <label className="block">
                <span className="text-xs font-medium text-muted">Search runs</span>
                <input
                  value={runsQuery}
                  onChange={(event) => setRunsQuery(event.target.value)}
                  placeholder="Run ID, provider, app, step, definition, event"
                  className="mt-2 w-full rounded-md border border-alpha bg-background px-3 py-2 text-sm text-primary outline-hidden transition-colors duration-150 placeholder:text-faint focus:border-sky-500"
                />
              </label>
              <label className="block">
                <span className="text-xs font-medium text-muted">Status</span>
                <select
                  value={runStatus}
                  onChange={(event) => setRunStatus(event.target.value)}
                  className="mt-2 w-full rounded-md border border-alpha bg-background px-3 py-2 text-sm text-primary outline-hidden transition-colors duration-150 focus:border-sky-500"
                >
                  {RUN_STATUSES.map((status) => (
                    <option key={status} value={status}>
                      {capitalize(status)}
                    </option>
                  ))}
                </select>
              </label>
            </div>
          </section>

          {loading ? (
            <p className="mt-8 text-sm text-faint">Loading workflow runs...</p>
          ) : (
            <RunsPanel
              runs={filteredRuns}
              runsError={runsError}
              selectedRunID={selectedRunID}
              selectedRun={selectedRun}
              detailLoading={detailLoading}
              detailError={detailError}
              actionError={actionError}
              canceling={canceling}
              onSelectRun={setSelectedRunID}
              onCancelSelectedRun={handleCancelSelectedRun}
            />
          )}
        </Container>
      </div>
    </AuthGuard>
  );
}

function RunsPanel({
  runs,
  runsError,
  selectedRunID,
  selectedRun,
  detailLoading,
  detailError,
  actionError,
  canceling,
  onSelectRun,
  onCancelSelectedRun,
}: {
  runs: WorkflowRun[];
  runsError: string | null;
  selectedRunID: string | null;
  selectedRun: WorkflowRun | null;
  detailLoading: boolean;
  detailError: string | null;
  actionError: string | null;
  canceling: boolean;
  onSelectRun: (id: string) => void;
  onCancelSelectedRun: () => Promise<void>;
}) {
  const selectedRunCancelable = selectedRun?.status === "pending";

  return (
    <div className="mt-8 grid gap-6 lg:grid-cols-[minmax(0,1.05fr)_minmax(24rem,0.95fr)]">
      <section className="rounded-lg border border-alpha bg-base-100 dark:bg-surface">
        <div className="border-b border-alpha px-5 py-4">
          <h2 className="text-sm font-medium text-primary">Workflow Runs</h2>
          <p className="mt-1 text-xs text-faint">{runs.length} shown</p>
        </div>

        {runsError ? (
          <p className="px-5 py-8 text-sm text-ember-500">{runsError}</p>
        ) : (
          <div className="divide-y divide-alpha">
            {runs.length === 0 ? (
              <div className="px-5 py-8 text-sm text-faint">
                No workflow runs yet.
              </div>
            ) : (
              runs.map((run) => {
                const isActive = run.id === selectedRunID;
                return (
                  <button
                    key={run.id}
                    type="button"
                    onClick={() => onSelectRun(run.id)}
                    className={`flex w-full items-start justify-between gap-4 px-5 py-4 text-left transition-colors duration-150 ${
                      isActive ? "bg-alpha-5" : "hover:bg-alpha-5"
                    }`}
                  >
                    <div className="min-w-0">
                      <div className="flex min-w-0 flex-wrap items-center gap-2">
                        <span className="truncate text-sm font-medium text-primary">
                          {targetLabel(run.target)}
                        </span>
                        <span className={runStatusClassName(run.status)}>
                          {run.status || "unknown"}
                        </span>
                      </div>
                      <p className="mt-1 truncate text-xs text-faint">{run.id}</p>
                      <p className="mt-2 text-xs text-muted">
                        {runTriggerLabel(run)} · {run.provider}
                      </p>
                    </div>
                    <div className="shrink-0 text-right text-xs text-faint">
                      {formatDate(run.startedAt || run.completedAt || run.createdAt)}
                    </div>
                  </button>
                );
              })
            )}
          </div>
        )}
      </section>

      <section className="rounded-lg border border-alpha bg-base-100 p-5 dark:bg-surface">
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0">
            <h2 className="text-sm font-medium text-primary">Run Details</h2>
            <p className="mt-1 truncate text-xs text-faint">
              {selectedRun?.id || "Select a run"}
            </p>
          </div>
          {selectedRun?.status ? (
            <span className={runStatusClassName(selectedRun.status)}>
              {selectedRun.status}
            </span>
          ) : null}
        </div>

        {selectedRunCancelable ? (
          <div className="mt-4 flex items-center justify-between gap-3 border-y border-alpha py-3">
            <p className="text-sm text-muted">
              Canceling asks the workflow provider to stop this pending run.
            </p>
            <button
              type="button"
              onClick={() => void onCancelSelectedRun()}
              disabled={canceling}
              className="shrink-0 rounded-md bg-ember-500 px-3 py-2 text-sm font-medium text-white transition-opacity duration-150 hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
            >
              {canceling ? "Canceling..." : "Cancel run"}
            </button>
          </div>
        ) : null}

        {detailError && <p className="mt-4 text-sm text-ember-500">{detailError}</p>}
        {actionError && <p className="mt-4 text-sm text-ember-500">{actionError}</p>}

        {detailLoading && !selectedRun ? (
          <p className="mt-6 text-sm text-faint">Loading details...</p>
        ) : selectedRun ? (
          <RunDetails run={selectedRun} />
        ) : (
          <p className="mt-6 text-sm text-faint">
            Select a workflow run to inspect it.
          </p>
        )}
      </section>
    </div>
  );
}

function RunDetails({ run }: { run: WorkflowRun }) {
  return (
    <div className="mt-6 space-y-7">
      <DetailGrid
        items={[
          ["Provider", run.provider],
          ["Trigger", runTriggerLabel(run)],
          ["Definition", run.definitionId || "-"],
          ["Generation", run.definitionGeneration ? String(run.definitionGeneration) : "-"],
          ["Current step", run.currentStepId || "-"],
          ["Actor", run.createdBy?.subjectId || "-"],
          ["Created", formatDate(run.createdAt)],
          ["Started", formatDate(run.startedAt)],
          ["Completed", formatDate(run.completedAt)],
        ]}
      />

      <TargetDetails target={run.target} />

      <StepExecutions steps={run.steps ?? []} />

      <JSONSection title="Run Input" value={run.input} emptyText="No input captured." />
      <JSONSection
        title="Run Output"
        value={run.output}
        emptyText="No output captured."
      />

      <section>
        <SectionHeading>Status Message</SectionHeading>
        <p className="mt-3 text-sm text-primary">
          {run.statusMessage || "No status message"}
        </p>
      </section>
    </div>
  );
}

function DetailGrid({ items }: { items: Array<[string, string]> }) {
  return (
    <dl className="grid gap-x-5 gap-y-4 sm:grid-cols-2">
      {items.map(([label, value]) => (
        <div key={label} className="border-b border-alpha pb-3">
          <dt className="text-[11px] uppercase tracking-[0.18em] text-faint">
            {label}
          </dt>
          <dd className="mt-2 break-words text-sm text-primary">{value || "-"}</dd>
        </div>
      ))}
    </dl>
  );
}

function TargetDetails({ target }: { target: WorkflowTarget }) {
  if (target.steps.length === 0) {
    return (
      <section>
        <SectionHeading>Target</SectionHeading>
        <p className="mt-3 text-sm text-faint">No workflow target captured.</p>
      </section>
    );
  }

  return (
    <section>
      <SectionHeading>Target Steps</SectionHeading>
      <div className="mt-3 divide-y divide-alpha border-y border-alpha">
        {target.steps.map((step, index) => (
          <TargetStepDetails key={`${step.id || "step"}-${index}`} step={step} />
        ))}
      </div>
    </section>
  );
}

function TargetStepDetails({ step }: { step: WorkflowStepTarget }) {
  return (
    <div className="py-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <p className="text-sm font-medium text-primary">{stepLabel(step)}</p>
          <p className="mt-1 text-xs text-faint">{step.id || "unnamed step"}</p>
        </div>
        <span className="rounded-full bg-alpha-5 px-2 py-1 text-[11px] font-medium text-muted">
          {stepKind(step)}
        </span>
      </div>

      <div className="mt-4 grid gap-x-5 gap-y-3 sm:grid-cols-2">
        {step.app ? (
          <>
            <DetailLine label="App" value={step.app.name} />
            <DetailLine label="Operation" value={step.app.operation} />
            <DetailLine label="Connection" value={step.app.connection || "-"} />
            <DetailLine label="Instance" value={step.app.instance || "-"} />
            <DetailLine
              label="Credential mode"
              value={step.app.credentialMode || "-"}
            />
          </>
        ) : null}
        {step.agent ? (
          <>
            <DetailLine label="Agent provider" value={step.agent.provider || "-"} />
            <DetailLine label="Model" value={step.agent.model || "-"} />
            <DetailLine label="Session key" value={step.agent.sessionKey || "-"} />
          </>
        ) : null}
        <DetailLine
          label="Timeout"
          value={step.timeoutSeconds ? `${step.timeoutSeconds}s` : "-"}
        />
      </div>

      <div className="mt-4 grid gap-4">
        <JSONSection title="Step Inputs" value={step.inputs} emptyText="No inputs." />
        <JSONSection title="App Input" value={step.app?.input} emptyText="No app input." />
        <JSONSection
          title="Agent Prompt"
          value={step.agent?.prompt}
          emptyText="No agent prompt."
        />
        <JSONSection
          title="Agent Messages"
          value={step.agent?.messages}
          emptyText="No agent messages."
        />
        <JSONSection title="When" value={step.when} emptyText="No guard." />
        <JSONSection
          title="Metadata"
          value={step.metadata}
          emptyText="No metadata."
        />
      </div>
    </div>
  );
}

function StepExecutions({ steps }: { steps: WorkflowStepExecution[] }) {
  return (
    <section>
      <SectionHeading>Step Executions</SectionHeading>
      {steps.length === 0 ? (
        <p className="mt-3 text-sm text-faint">No step state captured.</p>
      ) : (
        <div className="mt-3 divide-y divide-alpha border-y border-alpha">
          {steps.map((step, index) => (
            <div key={`${step.stepId || "step"}-${index}`} className="py-4">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div>
                  <p className="text-sm font-medium text-primary">
                    {step.stepId || `Step ${index + 1}`}
                  </p>
                  <p className="mt-1 text-xs text-faint">
                    {step.startedAt ? formatDate(step.startedAt) : "Not started"}
                    {step.completedAt ? ` - ${formatDate(step.completedAt)}` : ""}
                  </p>
                </div>
                <span className={stepStatusClassName(step.status)}>
                  {step.status || "unknown"}
                </span>
              </div>

              <div className="mt-4 grid gap-x-5 gap-y-3 sm:grid-cols-2">
                <DetailLine
                  label="Skip reason"
                  value={step.skipReason || "-"}
                />
                <DetailLine
                  label="Status message"
                  value={step.statusMessage || "-"}
                />
                <DetailLine
                  label="Attempts"
                  value={String(step.attempts?.length ?? 0)}
                />
              </div>

              <div className="mt-4 grid gap-4">
                <JSONSection
                  title="Step Input"
                  value={step.input}
                  emptyText="No input."
                />
                <JSONSection
                  title="Step Output"
                  value={step.output}
                  emptyText="No output."
                />
              </div>

              {step.attempts && step.attempts.length > 0 ? (
                <div className="mt-4 space-y-3">
                  <p className="text-xs font-medium text-muted">Attempts</p>
                  {step.attempts.map((attempt, attemptIndex) => (
                    <div
                      key={`${attempt.id || "attempt"}-${attemptIndex}`}
                      className="border-l border-alpha pl-4"
                    >
                      <div className="flex flex-wrap items-center gap-2">
                        <p className="text-sm font-medium text-primary">
                          {attempt.id || `Attempt ${attemptIndex + 1}`}
                        </p>
                        <span className={stepStatusClassName(attempt.status)}>
                          {attempt.status || "unknown"}
                        </span>
                      </div>
                      <DetailGrid
                        items={[
                          ["Idempotency key", attempt.idempotencyKey || "-"],
                          ["Started", formatDate(attempt.startedAt)],
                          ["Completed", formatDate(attempt.completedAt)],
                          ["Status message", attempt.statusMessage || "-"],
                        ]}
                      />
                      <div className="mt-4 grid gap-4">
                        <JSONSection
                          title="Attempt Input"
                          value={attempt.input}
                          emptyText="No input."
                        />
                        <JSONSection
                          title="Attempt Output"
                          value={attempt.output}
                          emptyText="No output."
                        />
                      </div>
                    </div>
                  ))}
                </div>
              ) : null}
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

function JSONSection({
  title,
  value,
  emptyText,
}: {
  title: string;
  value: unknown;
  emptyText: string;
}) {
  return (
    <section>
      <p className="text-xs font-medium text-muted">{title}</p>
      {hasJSONValue(value) ? (
        <pre className="mt-2 max-h-64 overflow-auto rounded-md border border-alpha bg-background/70 p-3 text-xs text-primary dark:bg-background/20">
          {prettyJSON(value)}
        </pre>
      ) : (
        <p className="mt-2 text-xs text-faint">{emptyText}</p>
      )}
    </section>
  );
}

function SectionHeading({ children }: { children: ReactNode }) {
  return (
    <h3 className="text-xs font-medium uppercase tracking-[0.18em] text-faint">
      {children}
    </h3>
  );
}

function DetailLine({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <p className="text-[11px] uppercase tracking-[0.18em] text-faint">{label}</p>
      <p className="mt-1 break-words text-sm text-primary">{value || "-"}</p>
    </div>
  );
}

function SummaryCard({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone: "default" | "sky" | "grove" | "ember";
}) {
  const toneClassName = {
    default: "text-primary",
    sky: "text-sky-700 dark:text-sky-200",
    grove: "text-grove-700 dark:text-grove-200",
    ember: "text-ember-700 dark:text-ember-200",
  }[tone];

  return (
    <div className="rounded-lg border border-alpha bg-base-100 px-5 py-4 dark:bg-surface">
      <p className="text-[11px] uppercase tracking-[0.18em] text-faint">{label}</p>
      <p className={`mt-3 text-2xl font-heading font-bold ${toneClassName}`}>
        {value}
      </p>
    </div>
  );
}

function filterRuns(runs: WorkflowRun[], query: string, status: string): WorkflowRun[] {
  const trimmedQuery = query.trim().toLowerCase();
  return runs.filter((run) => {
    const matchesStatus = status === "all" || (run.status || "") === status;
    if (!matchesStatus) return false;
    if (!trimmedQuery) return true;

    return runSearchTerms(run).some((value) =>
      value.toLowerCase().includes(trimmedQuery),
    );
  });
}

function runSearchTerms(run: WorkflowRun): string[] {
  const terms = [
    run.id,
    run.provider,
    run.status,
    run.definitionId,
    run.currentStepId,
    run.trigger?.kind,
    run.trigger?.activationId,
    run.trigger?.event?.type,
    run.trigger?.event?.source,
    run.trigger?.event?.subject,
  ];

  for (const step of run.target.steps) {
    terms.push(step.id, step.app?.name, step.app?.operation, step.agent?.provider, step.agent?.model);
  }

  for (const step of run.steps ?? []) {
    terms.push(step.stepId, step.status, step.skipReason, step.statusMessage);
  }

  return terms.filter((value): value is string => Boolean(value));
}

function runTriggerLabel(run: WorkflowRun): string {
  const trigger = run.trigger;
  if (!trigger?.kind) return "unknown";
  if (trigger.kind === "schedule") {
    return trigger.activationId ? `schedule:${trigger.activationId}` : "schedule";
  }
  if (trigger.kind === "event") {
    if (trigger.activationId) return `event:${trigger.activationId}`;
    return trigger.event?.type ? `event:${trigger.event.type}` : "event";
  }
  return trigger.kind;
}

function workflowRunCounts(runs: WorkflowRun[]) {
  return runs.reduce(
    (counts, run) => {
      if (run.status === "running") counts.running += 1;
      if (run.status === "succeeded") counts.succeeded += 1;
      if (run.status === "failed") counts.failed += 1;
      return counts;
    },
    { running: 0, succeeded: 0, failed: 0 },
  );
}

function upsertRun(runs: WorkflowRun[], run: WorkflowRun): WorkflowRun[] {
  const index = runs.findIndex((item) => item.id === run.id);
  if (index < 0) return [run, ...runs];
  return runs.map((item) => (item.id === run.id ? run : item));
}

function formatDate(value?: string): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function targetLabel(target: WorkflowTarget): string {
  const app = workflowTargetApp(target);
  if (app.name && app.operation) {
    return `${app.name}.${app.operation}`;
  }
  const agentStep = target.steps.find((step) => step.agent);
  if (agentStep?.agent?.model) {
    return `agent.${agentStep.agent.model}`;
  }
  if (target.steps.length > 1) {
    return `${target.steps.length} steps`;
  }
  return app.name || app.operation || "unknown";
}

function stepLabel(step: WorkflowStepTarget): string {
  if (step.app?.name && step.app.operation) {
    return `${step.app.name}.${step.app.operation}`;
  }
  if (step.agent?.model) {
    return `agent.${step.agent.model}`;
  }
  if (step.agent?.provider) {
    return `agent.${step.agent.provider}`;
  }
  return step.id || "Workflow step";
}

function stepKind(step: WorkflowStepTarget): string {
  if (step.app) return "app";
  if (step.agent) return "agent";
  return "unknown";
}

function hasJSONValue(value: unknown): boolean {
  if (value === undefined) return false;
  if (value === null) return true;
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === "object") return Object.keys(value).length > 0;
  return true;
}

function prettyJSON(value: unknown): string {
  const rendered = JSON.stringify(value, null, 2);
  return rendered === undefined ? String(value) : rendered;
}

function runStatusClassName(status?: string): string {
  switch (status) {
    case "succeeded":
      return "rounded-full bg-grove-100 px-2 py-1 text-[11px] font-medium text-grove-700 dark:bg-grove-700/20 dark:text-grove-200";
    case "failed":
      return "rounded-full bg-ember-100 px-2 py-1 text-[11px] font-medium text-ember-700 dark:bg-ember-700/20 dark:text-ember-200";
    case "running":
      return "rounded-full bg-sky-100 px-2 py-1 text-[11px] font-medium text-sky-700 dark:bg-sky-700/20 dark:text-sky-200";
    case "pending":
      return "rounded-full bg-amber-100 px-2 py-1 text-[11px] font-medium text-amber-700 dark:bg-amber-700/20 dark:text-amber-200";
    case "canceled":
      return "rounded-full bg-alpha-10 px-2 py-1 text-[11px] font-medium text-muted";
    default:
      return "rounded-full bg-alpha-5 px-2 py-1 text-[11px] font-medium text-muted";
  }
}

function stepStatusClassName(status?: string): string {
  switch (status) {
    case "succeeded":
      return "rounded-full bg-grove-100 px-2 py-1 text-[11px] font-medium text-grove-700 dark:bg-grove-700/20 dark:text-grove-200";
    case "failed":
      return "rounded-full bg-ember-100 px-2 py-1 text-[11px] font-medium text-ember-700 dark:bg-ember-700/20 dark:text-ember-200";
    case "running":
      return "rounded-full bg-sky-100 px-2 py-1 text-[11px] font-medium text-sky-700 dark:bg-sky-700/20 dark:text-sky-200";
    case "pending":
      return "rounded-full bg-amber-100 px-2 py-1 text-[11px] font-medium text-amber-700 dark:bg-amber-700/20 dark:text-amber-200";
    case "skipped":
      return "rounded-full bg-alpha-10 px-2 py-1 text-[11px] font-medium text-muted";
    default:
      return "rounded-full bg-alpha-5 px-2 py-1 text-[11px] font-medium text-muted";
  }
}

function capitalize(value: string): string {
  return value.charAt(0).toUpperCase() + value.slice(1);
}

function errorMessage(err: unknown, fallback: string): string {
  if (err instanceof Error && err.message) return err.message;
  if (typeof err === "string" && err) return err;
  return fallback;
}
