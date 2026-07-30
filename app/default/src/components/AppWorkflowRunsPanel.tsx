import {
  useDeferredValue,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import {
  workflowTargetApp,
  type WorkflowRun,
  type WorkflowStepExecution,
  type WorkflowStepTarget,
  type WorkflowTarget,
} from "@/lib/api";
import { cn } from "@/lib/cn";
import { listItemInteraction } from "@/lib/list-item-interaction";
import { Link } from "@tanstack/react-router";
import {
  useCancelWorkflowRunMutation,
  useWorkflowRunQuery,
  useWorkflowRunsQuery,
} from "@/lib/queries";
import {
  SectionHeader,
  SectionHeaderActions,
  SectionHeaderContent,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import {
  Alert,
  AlertDescription,
} from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Stat,
  StatGroup,
  StatLabel,
  StatValue,
} from "@/components/ui/stat";
import {
  collectAutomationSubjects,
  summarizeWorkflowDefinitionsFromRuns,
  workflowRunBadgeVariant,
} from "@/lib/workflowActivity";
import { Info } from "lucide-react";

const RUN_STATUSES = [
  "all",
  "pending",
  "running",
  "succeeded",
  "failed",
  "canceled",
] as const;

export default function AppWorkflowRunsPanel({ appName }: { appName: string }) {
  const runsQuery = useWorkflowRunsQuery(appName);
  const runs = runsQuery.data ?? [];
  const loading = runsQuery.isPending;
  const refreshing = runsQuery.isFetching && !runsQuery.isPending;
  const runsError = runsQuery.error
    ? errorMessage(runsQuery.error, "Failed to load workflow runs")
    : null;

  const [selectedRunID, setSelectedRunID] = useState<string | null>(null);
  const selectedListRun =
    runs.find((run) => run.id === selectedRunID) ?? undefined;
  const detailQuery = useWorkflowRunQuery(appName, selectedRunID, selectedListRun);
  const cancelMutation = useCancelWorkflowRunMutation(appName);

  const selectedRun = detailQuery.data ?? selectedListRun ?? null;
  const detailLoading = detailQuery.isFetching && !detailQuery.data;
  const detailError = detailQuery.error
    ? errorMessage(detailQuery.error, "Failed to load workflow run")
    : null;
  const actionError = cancelMutation.error
    ? errorMessage(cancelMutation.error, "Failed to cancel workflow run")
    : null;
  const canceling = cancelMutation.isPending;

  const [runsQueryText, setRunsQuery] = useState("");
  const [runStatus, setRunStatus] = useState<string>("all");
  const deferredRunsQuery = useDeferredValue(runsQueryText);

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

  function handleCancelSelectedRun() {
    if (!selectedRun || canceling) return;
    cancelMutation.mutate({
      id: selectedRun.id,
      reason: "Canceled from Gestalt UI",
      run: selectedRun,
    });
  }

  function refreshRuns() {
    void runsQuery.refetch();
    if (selectedRunID) {
      void detailQuery.refetch();
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
    <>
      <SectionHeader className="mb-6">
        <SectionHeaderContent>
          <SectionHeaderTitle className="font-heading text-2xl tracking-normal">
            Workflows
          </SectionHeaderTitle>
        </SectionHeaderContent>
        <SectionHeaderActions>
          <Button
            type="button"
            variant="outline"
            onClick={refreshRuns}
            disabled={refreshing}
          >
            {refreshing ? "Refreshing…" : "Refresh"}
          </Button>
        </SectionHeaderActions>
      </SectionHeader>

      <div className="space-y-8">
      <Alert variant="info" data-testid="app-workflow-ownership-note">
        <Info aria-hidden />
        <AlertDescription>
          Runs listed here target this app as a{" "}
          <span className="text-foreground">step app</span> (or ship under{" "}
          <code className="font-mono text-xs">app_{appName}_…</code> definition
          IDs). Workflows that only <em>publish</em> events handled elsewhere are
          not included — open the other app’s admin page for those.
        </AlertDescription>
      </Alert>

      <StatGroup className="w-full" data-testid="workflow-run-stats">
        <Stat variant="plain" className="w-max max-w-full shrink-0">
          <StatLabel>Runs</StatLabel>
          <StatValue>{runs.length}</StatValue>
        </Stat>
        <Stat variant="plain" className="w-max max-w-full shrink-0">
          <StatLabel>Running</StatLabel>
          <StatValue>{counts.running}</StatValue>
        </Stat>
        <Stat variant="plain" className="w-max max-w-full shrink-0">
          <StatLabel>Succeeded</StatLabel>
          <StatValue>{counts.succeeded}</StatValue>
        </Stat>
        <Stat variant="plain" className="w-max max-w-full shrink-0">
          <StatLabel>Failed</StatLabel>
          <StatValue>{counts.failed}</StatValue>
        </Stat>
      </StatGroup>

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
          <p className="text-sm text-muted-foreground/70">Loading definitions…</p>
        ) : definitions.length === 0 ? (
          <p className="text-sm text-muted-foreground/70">
            No definitions observed in recent runs. Use{" "}
            <code className="font-mono text-xs">
              gestalt workflows runs list --app {appName}
            </code>{" "}
            or apply definitions from app config.
          </p>
        ) : (
          <ul className="divide-y divide-border rounded-lg border border-border">
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
                    <Badge size="sm" variant="warning">
                      Schedule ×{item.scheduleCount}
                    </Badge>
                  ) : null}
                  {item.eventCount > 0 ? (
                    <Badge size="sm" variant="muted">
                      Event ×{item.eventCount}
                    </Badge>
                  ) : null}
                  {item.manualCount > 0 ? (
                    <Badge size="sm" variant="muted">
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
          <p className="text-sm text-muted-foreground/70">Loading events…</p>
        ) : eventDefinitions.length === 0 ? (
          <p className="text-sm text-muted-foreground/70">
            No event-triggered runs for this app in the current page.
          </p>
        ) : (
          <ul className="divide-y divide-border rounded-lg border border-border">
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
          <p className="text-sm text-muted-foreground/70">Loading identities…</p>
        ) : automationSubjects.length === 0 ? (
          <p className="text-sm text-muted-foreground/70">
            No creator subjects on recent runs.
          </p>
        ) : (
          <ul className="divide-y divide-border rounded-lg border border-border">
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
                    to="/identities"
                    search={{ id: subject }}
                    className="text-sm text-foreground underline underline-offset-2 hover:text-foreground/80"
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
        <div className="block">
          <Label htmlFor="workflow-runs-search" variant="field">
            Search runs
          </Label>
          <Input
            id="workflow-runs-search"
            value={runsQueryText}
            onChange={(event) => setRunsQuery(event.target.value)}
            placeholder="Run ID, step, definition, event"
            className="mt-2"
          />
        </div>
        <label className="block">
          <span className="text-xs font-medium text-muted-foreground">Status</span>
          <select
            value={runStatus}
            onChange={(event) => setRunStatus(event.target.value)}
            className="mt-2 w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground outline-hidden transition-colors duration-150 focus:border-info-foreground"
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
        <Link to="/docs/workflows" className="underline underline-offset-2">
          Workflow docs
        </Link>
        .
      </p>

      {loading ? (
        <p className="text-sm text-muted-foreground/70">Loading workflow runs…</p>
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
    </>
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
    return <p className="text-sm text-destructive">{runsError}</p>;
  }

  if (runs.length === 0) {
    return (
      <p className="text-sm text-muted-foreground/70" data-testid="app-workflows-empty">
        No workflow runs for this app yet.
      </p>
    );
  }

  return (
    <div className="grid gap-4 lg:grid-cols-[minmax(0,18rem)_minmax(0,1fr)]">
      <div
        className="rounded-lg border border-border bg-card"
        data-testid="app-workflow-run-list"
      >
        <ul className="divide-y divide-border">
          {runs.map((run) => {
            const selected = run.id === selectedRunID;
            return (
              <li key={run.id}>
                <button
                  type="button"
                  onClick={() => onSelectRun(run.id)}
                  data-selected={selected || undefined}
                  className={cn(
                    "flex w-full flex-col gap-1 px-4 py-3 text-left focus-ring-inset",
                    listItemInteraction({ pointer: "css" }),
                  )}
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

      <div className="rounded-lg border border-border bg-card p-5">
        {detailError ? (
          <p className="text-sm text-destructive">{detailError}</p>
        ) : !selectedRun ? (
          <p className="text-sm text-muted-foreground/70">Select a run to inspect details.</p>
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
                  variant="outline"
                  onClick={onCancelSelectedRun}
                  disabled={canceling}
                  className="shrink-0"
                >
                  {canceling ? "Canceling…" : "Cancel run"}
                </Button>
              ) : null}
            </div>
            {actionError ? (
              <p className="text-sm text-destructive">{actionError}</p>
            ) : null}
            {detailLoading ? (
              <p className="text-sm text-muted-foreground/70">Loading details…</p>
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
            className="rounded-md border border-border px-3 py-3"
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

function StatusBadge({ status }: { status?: string }) {
  return (
    <Badge size="sm" variant={workflowRunBadgeVariant(status)}>
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
