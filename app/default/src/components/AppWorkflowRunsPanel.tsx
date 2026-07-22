import {
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import {
  cancelWorkflowRun,
  getWorkflowRun,
  getWorkflowRuns,
  workflowTargetApp,
  type WorkflowRun,
  type WorkflowStepExecution,
  type WorkflowStepTarget,
  type WorkflowTarget,
} from "@/lib/workflow";
import {
  collectAutomationSubjects,
  summarizeWorkflowDefinitionsFromRuns,
} from "@/lib/workflowActivity";
import { Badge } from "@/components/Badge";
import Button from "@/components/Button";
import { Link } from "@/components/Link";

const RUN_STATUSES = [
  "all",
  "pending",
  "running",
  "succeeded",
  "failed",
  "canceled",
] as const;

export default function AppWorkflowRunsPanel({ appName }: { appName: string }) {
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
  const [runStatus, setRunStatus] = useState<string>("all");
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

    getWorkflowRuns({ app: appName })
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
  }, [appName, refreshNonce]);

  const filteredRuns = useMemo(
    () => filterRuns(runs, deferredRunsQuery, runStatus),
    [runs, deferredRunsQuery, runStatus],
  );

  useEffect(() => {
    if (filteredRuns.length === 0) {
      setSelectedRunID(null);
      return;
    }
    if (
      !selectedRunID ||
      !filteredRuns.some((run) => run.id === selectedRunID)
    ) {
      setSelectedRunID(filteredRuns[0].id);
    }
  }, [filteredRuns, selectedRunID]);

  useEffect(() => {
    if (!selectedRunID) {
      setSelectedRun(null);
      setDetailError(null);
      return;
    }

    const existing =
      runsRef.current.find((run) => run.id === selectedRunID) ?? null;
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
  const definitions = useMemo(
    () => summarizeWorkflowDefinitionsFromRuns(runs),
    [runs],
  );
  const automationSubjects = useMemo(
    () => collectAutomationSubjects(runs),
    [runs],
  );
  const scheduleDefinitions = definitions.filter(
    (item) => item.scheduleCount > 0,
  );
  const eventDefinitions = definitions.filter((item) => item.eventCount > 0);

  return (
    <div className="space-y-8">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h2 className="text-lg font-heading text-foreground">Workflows</h2>
          <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
            Automation for this app: recent runs, schedule and event
            activations visible from run history, and the identities those
            runs execute as.
          </p>
        </div>
        <Button
          type="button"
          variant="secondary"
          onClick={() => setRefreshNonce((value) => value + 1)}
          disabled={refreshing}
        >
          {refreshing ? "Refreshing…" : "Refresh"}
        </Button>
      </div>

      <div
        className="rounded-lg border border-alpha bg-alpha-5 px-4 py-3 text-sm text-muted-foreground"
        data-testid="app-workflow-ownership-note"
      >
        Runs listed here target this app as a{" "}
        <span className="text-foreground">step app</span> (or ship under{" "}
        <code className="font-mono text-xs">app_{appName}_…</code> definition
        IDs). Workflows that only <em>publish</em> events handled elsewhere are
        not included — open the other app’s admin page for those.
      </div>

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <SummaryCard label="Runs" value={String(runs.length)} />
        <SummaryCard label="Running" value={String(counts.running)} />
        <SummaryCard label="Succeeded" value={String(counts.succeeded)} />
        <SummaryCard label="Failed" value={String(counts.failed)} />
      </div>

      <section className="space-y-3" aria-label="Definitions and schedules">
        <div>
          <h3 className="text-base font-heading text-foreground">
            Definitions &amp; schedules
          </h3>
          <p className="mt-1 text-sm text-muted-foreground">
            Cron jobs are schedule activations on a definition (cron, timezone,
            pause, runAs). Full definition APIs are not yet in this UI — below
            is what recent runs reveal.
          </p>
        </div>
        {loading ? (
          <p className="text-sm text-faint">Loading definitions…</p>
        ) : definitions.length === 0 ? (
          <p className="text-sm text-faint">
            No definitions observed in recent runs. Use{" "}
            <code className="font-mono text-xs">
              gestalt workflows runs list --app {appName}
            </code>{" "}
            or apply definitions from app config.
          </p>
        ) : (
          <ul className="divide-y divide-alpha rounded-lg border border-alpha">
            {definitions.map((item) => (
              <li
                key={item.definitionId}
                className="flex flex-col gap-2 px-4 py-3 sm:flex-row sm:items-start sm:justify-between"
              >
                <div className="min-w-0">
                  <p className="truncate font-mono text-sm text-foreground">
                    {item.definitionId}
                  </p>
                  <p className="mt-1 text-xs text-muted-foreground">
                    {item.runCount} run{item.runCount === 1 ? "" : "s"}
                    {item.lastCreatedAt
                      ? ` · last ${formatDate(item.lastCreatedAt)}`
                      : ""}
                    {item.lastStatus ? ` · ${item.lastStatus}` : ""}
                  </p>
                  {item.activationIds.length > 0 ? (
                    <p className="mt-1 text-xs text-muted-foreground">
                      Activations: {item.activationIds.join(", ")}
                    </p>
                  ) : null}
                </div>
                <div className="flex flex-wrap gap-1.5">
                  {item.scheduleCount > 0 ? (
                    <Badge variant="warning" size="sm">
                      Schedule ×{item.scheduleCount}
                    </Badge>
                  ) : null}
                  {item.eventCount > 0 ? (
                    <Badge variant="secondary" size="sm">
                      Event ×{item.eventCount}
                    </Badge>
                  ) : null}
                  {item.manualCount > 0 ? (
                    <Badge variant="muted" size="sm">
                      Manual ×{item.manualCount}
                    </Badge>
                  ) : null}
                </div>
              </li>
            ))}
          </ul>
        )}
        {scheduleDefinitions.length === 0 && !loading && runs.length > 0 ? (
          <p className="text-xs text-muted-foreground">
            No schedule-triggered runs in this window. Schedule cron / timezone
            / paused state will appear here once definition listing is wired.
          </p>
        ) : null}
      </section>

      <section className="space-y-3" aria-label="Event activations">
        <div>
          <h3 className="text-base font-heading text-foreground">
            Event activations
          </h3>
          <p className="mt-1 text-sm text-muted-foreground">
            Event-matched activations observed on recent runs (type / source /
            subject).
          </p>
        </div>
        {loading ? (
          <p className="text-sm text-faint">Loading events…</p>
        ) : eventDefinitions.length === 0 ? (
          <p className="text-sm text-faint">
            No event-triggered runs for this app in the current page.
          </p>
        ) : (
          <ul className="divide-y divide-alpha rounded-lg border border-alpha">
            {eventDefinitions.map((item) => (
              <li key={`event-${item.definitionId}`} className="px-4 py-3">
                <p className="font-mono text-sm text-foreground">
                  {item.definitionId}
                </p>
                <p className="mt-1 text-xs text-muted-foreground">
                  {item.eventTypes.length > 0
                    ? `Types: ${item.eventTypes.join(", ")}`
                    : "Event trigger"}
                  {item.eventSources.length > 0
                    ? ` · Sources: ${item.eventSources.join(", ")}`
                    : ""}
                </p>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className="space-y-3" aria-label="Automation identities">
        <div>
          <h3 className="text-base font-heading text-foreground">
            Automation identity
          </h3>
          <p className="mt-1 text-sm text-muted-foreground">
            Subjects observed as <code className="font-mono text-xs">createdBy</code>{" "}
            on recent runs (often the definition <code className="font-mono text-xs">runAs</code>).
          </p>
        </div>
        {loading ? (
          <p className="text-sm text-faint">Loading identities…</p>
        ) : automationSubjects.length === 0 ? (
          <p className="text-sm text-faint">
            No creator subjects on recent runs.
          </p>
        ) : (
          <ul className="divide-y divide-alpha rounded-lg border border-alpha">
            {automationSubjects.map((subject) => (
              <li
                key={subject}
                className="flex items-center justify-between gap-3 px-4 py-3"
              >
                <code className="truncate font-mono text-sm text-foreground">
                  {subject}
                </code>
                {subject.startsWith("service_account:") ? (
                  <Link
                    href={`/identities?id=${encodeURIComponent(subject)}`}
                    underlineVariant="always"
                  >
                    Open identity
                  </Link>
                ) : null}
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className="space-y-4" aria-label="Recent runs">
        <div>
          <h3 className="text-base font-heading text-foreground">Recent runs</h3>
          <p className="mt-1 text-sm text-muted-foreground">
            Status, trigger, definition, step plan, and I/O for runs that target
            this app.
          </p>
        </div>

      <div className="grid gap-4 md:grid-cols-[minmax(0,1fr)_12rem]">
        <label className="block">
          <span className="text-xs font-medium text-muted-foreground">Search runs</span>
          <input
            value={runsQuery}
            onChange={(event) => setRunsQuery(event.target.value)}
            placeholder="Run ID, step, definition, event"
            className="mt-2 w-full rounded-md border border-alpha bg-background px-3 py-2 text-sm text-foreground outline-hidden transition-colors duration-150 placeholder:text-faint focus:border-sky-500"
          />
        </label>
        <label className="block">
          <span className="text-xs font-medium text-muted-foreground">Status</span>
          <select
            value={runStatus}
            onChange={(event) => setRunStatus(event.target.value)}
            className="mt-2 w-full rounded-md border border-alpha bg-background px-3 py-2 text-sm text-foreground outline-hidden transition-colors duration-150 focus:border-sky-500"
          >
            {RUN_STATUSES.map((status) => (
              <option key={status} value={status}>
                {capitalize(status)}
              </option>
            ))}
          </select>
        </label>
      </div>

      <p className="text-xs text-muted-foreground">
        CLI:{" "}
        <code className="font-mono text-xs">
          gestalt workflows runs list --app {appName}
        </code>
        . See{" "}
        <Link href="/docs/workflows" underlineVariant="always">
          Workflow docs
        </Link>
        .
      </p>

      {loading ? (
        <p className="text-sm text-faint">Loading workflow runs…</p>
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
      </section>
    </div>
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
  onCancelSelectedRun: () => void;
}) {
  if (runsError) {
    return <p className="text-sm text-ember-500">{runsError}</p>;
  }

  if (runs.length === 0) {
    return (
      <p className="text-sm text-faint" data-testid="app-workflows-empty">
        No workflow runs for this app yet.
      </p>
    );
  }

  return (
    <div className="grid gap-4 lg:grid-cols-[minmax(0,18rem)_minmax(0,1fr)]">
      <div
        className="rounded-lg border border-alpha bg-base-white dark:bg-surface"
        data-testid="app-workflow-run-list"
      >
        <ul className="divide-y divide-alpha">
          {runs.map((run) => {
            const selected = run.id === selectedRunID;
            return (
              <li key={run.id}>
                <button
                  type="button"
                  onClick={() => onSelectRun(run.id)}
                  className={`flex w-full flex-col gap-1 px-4 py-3 text-left transition-colors duration-150 ${
                    selected
                      ? "bg-alpha-5"
                      : "hover:bg-alpha-5"
                  }`}
                >
                  <span className="truncate text-sm font-medium text-foreground">
                    {targetLabel(run.target) ||
                      run.definitionId ||
                      shortRunId(run.id)}
                  </span>
                  <span className="flex items-center gap-2 text-xs text-muted-foreground">
                    <StatusBadge status={run.status} />
                    <span>{formatDate(run.createdAt)}</span>
                  </span>
                </button>
              </li>
            );
          })}
        </ul>
      </div>

      <div className="rounded-lg border border-alpha bg-base-white p-5 dark:bg-surface">
        {detailError ? (
          <p className="text-sm text-ember-500">{detailError}</p>
        ) : !selectedRun ? (
          <p className="text-sm text-faint">Select a run to inspect details.</p>
        ) : (
          <div className="space-y-5">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
              <div className="min-w-0">
                <div className="flex flex-wrap items-center gap-2">
                  <StatusBadge status={selectedRun.status} />
                  <span className="truncate font-mono text-xs text-muted-foreground">
                    {shortRunId(selectedRun.id)}
                  </span>
                </div>
                <h3 className="mt-2 text-base font-heading text-foreground">
                  {targetLabel(selectedRun.target) ||
                    selectedRun.definitionId ||
                    "Workflow run"}
                </h3>
                {selectedRun.statusMessage ? (
                  <p className="mt-1 text-sm text-muted-foreground">
                    {selectedRun.statusMessage}
                  </p>
                ) : null}
              </div>
              {selectedRun.status === "pending" ? (
                <Button
                  type="button"
                  variant="secondary"
                  onClick={onCancelSelectedRun}
                  disabled={canceling}
                >
                  {canceling ? "Canceling…" : "Cancel run"}
                </Button>
              ) : null}
            </div>
            {actionError ? (
              <p className="text-sm text-ember-500">{actionError}</p>
            ) : null}
            {detailLoading ? (
              <p className="text-sm text-faint">Loading details…</p>
            ) : null}
            <RunDetails run={selectedRun} />
          </div>
        )}
      </div>
    </div>
  );
}

function RunDetails({ run }: { run: WorkflowRun }) {
  return (
    <div className="space-y-5">
      <DetailGrid
        items={[
          ["Provider", run.provider],
          ["Definition", run.definitionId || "—"],
          ["Trigger", runTriggerLabel(run)],
          ["Created", formatDate(run.createdAt)],
          ["Started", formatDate(run.startedAt)],
          ["Completed", formatDate(run.completedAt)],
          ["Created by", run.createdBy?.subjectId || "—"],
        ]}
      />
      <TargetDetails target={run.target} />
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

function DetailGrid({ items }: { items: Array<[string, string]> }) {
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

function TargetDetails({ target }: { target: WorkflowTarget }) {
  if (target.steps.length === 0) {
    return (
      <div>
        <SectionHeading>Target</SectionHeading>
        <p className="mt-2 text-sm text-faint">No target steps.</p>
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
            className="rounded-md border border-alpha px-3 py-2"
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
    <div className="space-y-1 text-sm">
      <DetailLine label="Step" value={step.id || "—"} />
      <DetailLine label="Kind" value={stepKind(step)} />
      {step.app ? (
        <DetailLine
          label="App"
          value={`${step.app.name}.${step.app.operation}`}
        />
      ) : null}
      {step.agent ? (
        <DetailLine
          label="Agent"
          value={`${step.agent.provider || "agent"} / ${step.agent.model || "model"}`}
        />
      ) : null}
    </div>
  );
}

function StepExecutions({ steps }: { steps: WorkflowStepExecution[] }) {
  return (
    <div>
      <SectionHeading>Step executions</SectionHeading>
      <ul className="mt-3 space-y-3">
        {steps.map((step, index) => (
          <li
            key={step.stepId || `execution-${index}`}
            className="rounded-md border border-alpha px-3 py-3"
          >
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-sm font-medium text-foreground">
                {step.stepId || `Step ${index + 1}`}
              </span>
              <StatusBadge status={step.status} />
            </div>
            {step.statusMessage ? (
              <p className="mt-1 text-xs text-muted-foreground">{step.statusMessage}</p>
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
        className={`mt-2 overflow-x-auto rounded-md border border-alpha bg-alpha-5 p-3 font-mono text-xs text-foreground ${
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
    <h4 className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
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

function SummaryCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-alpha bg-base-white px-4 py-3 dark:bg-surface">
      <p className="text-xs font-medium text-muted-foreground">{label}</p>
      <p className="mt-1 text-xl font-heading text-foreground">{value}</p>
    </div>
  );
}

function StatusBadge({ status }: { status?: string }) {
  const variant =
    status === "succeeded"
      ? "success"
      : status === "failed"
        ? "destructive"
        : status === "running" || status === "pending"
          ? "warning"
          : "muted";
  return (
    <Badge variant={variant} size="sm">
      {capitalize(status || "unknown")}
    </Badge>
  );
}

function filterRuns(
  runs: WorkflowRun[],
  query: string,
  status: string,
): WorkflowRun[] {
  const needle = query.trim().toLowerCase();
  return runs.filter((run) => {
    if (status !== "all" && (run.status || "").toLowerCase() !== status) {
      return false;
    }
    if (!needle) return true;
    return runSearchTerms(run).some((term) => term.includes(needle));
  });
}

function runSearchTerms(run: WorkflowRun): string[] {
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

function runTriggerLabel(run: WorkflowRun): string {
  const kind = run.trigger?.kind || "unknown";
  if (run.trigger?.activationId) {
    return `${kind}:${run.trigger.activationId}`;
  }
  if (run.trigger?.event?.type) {
    return `${kind}:${run.trigger.event.type}`;
  }
  return kind;
}

function workflowRunCounts(runs: WorkflowRun[]) {
  return {
    running: runs.filter((run) => run.status === "running").length,
    succeeded: runs.filter((run) => run.status === "succeeded").length,
    failed: runs.filter((run) => run.status === "failed").length,
  };
}

function upsertRun(runs: WorkflowRun[], run: WorkflowRun): WorkflowRun[] {
  const index = runs.findIndex((item) => item.id === run.id);
  if (index === -1) return [run, ...runs];
  const next = [...runs];
  next[index] = run;
  return next;
}

function formatDate(value?: string): string {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function targetLabel(target: WorkflowTarget): string {
  if (target.steps.length === 0) return "";
  if (target.steps.length === 1) return stepLabel(target.steps[0]);
  const app = workflowTargetApp(target);
  if (app.name && app.operation) {
    return `${app.name}.${app.operation} (+${target.steps.length - 1})`;
  }
  return `${target.steps.length} steps`;
}

function stepLabel(step: WorkflowStepTarget): string {
  if (step.app?.name && step.app.operation) {
    return `${step.app.name}.${step.app.operation}`;
  }
  if (step.agent?.provider) {
    return `agent:${step.agent.provider}`;
  }
  return step.id || "step";
}

function stepKind(step: WorkflowStepTarget): string {
  if (step.app) return "app";
  if (step.agent) return "agent";
  return "unknown";
}

function hasJSONValue(value: unknown): boolean {
  if (value == null) return false;
  if (typeof value === "object" && !Array.isArray(value)) {
    return Object.keys(value as object).length > 0;
  }
  return true;
}

function prettyJSON(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function shortRunId(id: string): string {
  if (id.length <= 24) return id;
  return `${id.slice(0, 10)}…${id.slice(-8)}`;
}

function capitalize(value: string): string {
  if (!value) return value;
  return value.charAt(0).toUpperCase() + value.slice(1);
}

function errorMessage(err: unknown, fallback: string): string {
  return err instanceof Error && err.message ? err.message : fallback;
}
