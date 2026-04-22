"use client";

import { useDeferredValue, useEffect, useEffectEvent, useState } from "react";
import type {
  Integration,
  IntegrationOperation,
  WorkflowEventTrigger,
  WorkflowEventTriggerUpsert,
  WorkflowRun,
  WorkflowSchedule,
  WorkflowScheduleUpsert,
  WorkflowTarget,
} from "@/lib/api";
import {
  cancelWorkflowRun,
  createWorkflowEventTrigger,
  createWorkflowSchedule,
  deleteWorkflowEventTrigger,
  deleteWorkflowSchedule,
  getIntegrationOperations,
  getIntegrations,
  getWorkflowEventTriggers,
  getWorkflowRun,
  getWorkflowRuns,
  getWorkflowSchedules,
  pauseWorkflowEventTrigger,
  pauseWorkflowSchedule,
  resumeWorkflowEventTrigger,
  resumeWorkflowSchedule,
  updateWorkflowEventTrigger,
  updateWorkflowSchedule,
} from "@/lib/api";
import AuthGuard from "@/components/AuthGuard";
import Nav from "@/components/Nav";

type WorkflowTab = "runs" | "schedules" | "triggers";
type ScheduleCadence = "hourly" | "daily" | "weekly" | "monthly";
type WorkflowFormMode = "create" | "edit" | null;
type TimezoneMode = "local" | "utc";

interface ScheduleFormState {
  plugin: string;
  operation: string;
  connection: string;
  instance: string;
  inputJSON: string;
  cadence: ScheduleCadence;
  hour: string;
  weekday: string;
  monthDay: string;
  timezoneMode: TimezoneMode;
  paused: boolean;
}

interface TriggerFormState {
  plugin: string;
  operation: string;
  connection: string;
  instance: string;
  inputJSON: string;
  type: string;
  source: string;
  subject: string;
  paused: boolean;
}

interface TargetEditorProps {
  integrations: Integration[];
  integrationsError: string | null;
  operations: IntegrationOperation[];
  operationsLoading: boolean;
  operationsError: string | null;
  plugin: string;
  operation: string;
  connection: string;
  instance: string;
  inputJSON: string;
  onPluginChange: (value: string) => void;
  onOperationChange: (value: string) => void;
  onConnectionChange: (value: string) => void;
  onInstanceChange: (value: string) => void;
  onInputJSONChange: (value: string) => void;
}

const SCHEDULE_CADENCE_OPTIONS: Array<{ value: ScheduleCadence; label: string }> = [
  { value: "hourly", label: "Hourly" },
  { value: "daily", label: "Daily" },
  { value: "weekly", label: "Weekly" },
  { value: "monthly", label: "Monthly" },
];

const WEEKDAY_OPTIONS = [
  { value: "0", label: "Sunday" },
  { value: "1", label: "Monday" },
  { value: "2", label: "Tuesday" },
  { value: "3", label: "Wednesday" },
  { value: "4", label: "Thursday" },
  { value: "5", label: "Friday" },
  { value: "6", label: "Saturday" },
];

const HOUR_OPTIONS = Array.from({ length: 24 }, (_, hour) => ({
  value: String(hour),
  label: formatHourLabel(hour),
}));

const MONTH_DAY_OPTIONS = Array.from({ length: 28 }, (_, index) => ({
  value: String(index + 1),
  label: ordinal(index + 1),
}));
const EMPTY_OPERATIONS: IntegrationOperation[] = [];

export default function WorkflowsPage() {
  const [activeTab, setActiveTab] = useState<WorkflowTab>("runs");

  const [runs, setRuns] = useState<WorkflowRun[]>([]);
  const [schedules, setSchedules] = useState<WorkflowSchedule[]>([]);
  const [triggers, setTriggers] = useState<WorkflowEventTrigger[]>([]);
  const [integrations, setIntegrations] = useState<Integration[]>([]);

  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshNonce, setRefreshNonce] = useState(0);

  const [runsError, setRunsError] = useState<string | null>(null);
  const [schedulesError, setSchedulesError] = useState<string | null>(null);
  const [triggersError, setTriggersError] = useState<string | null>(null);
  const [integrationsError, setIntegrationsError] = useState<string | null>(null);

  const [selectedRunID, setSelectedRunID] = useState<string | null>(null);
  const [selectedScheduleID, setSelectedScheduleID] = useState<string | null>(null);
  const [selectedTriggerID, setSelectedTriggerID] = useState<string | null>(null);

  const [selectedRun, setSelectedRun] = useState<WorkflowRun | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [canceling, setCanceling] = useState(false);

  const [runsQuery, setRunsQuery] = useState("");
  const [schedulesQuery, setSchedulesQuery] = useState("");
  const [triggersQuery, setTriggersQuery] = useState("");

  const [runStatus, setRunStatus] = useState("all");
  const [scheduleStatus, setScheduleStatus] = useState("all");
  const [triggerStatus, setTriggerStatus] = useState("all");

  const [browserTimezone, setBrowserTimezone] = useState("UTC");
  const [operationsByPlugin, setOperationsByPlugin] = useState<
    Record<string, IntegrationOperation[]>
  >({});
  const [operationsLoadingByPlugin, setOperationsLoadingByPlugin] = useState<
    Record<string, boolean>
  >({});
  const [operationErrorsByPlugin, setOperationErrorsByPlugin] = useState<
    Record<string, string | undefined>
  >({});

  const deferredRunsQuery = useDeferredValue(runsQuery);
  const deferredSchedulesQuery = useDeferredValue(schedulesQuery);
  const deferredTriggersQuery = useDeferredValue(triggersQuery);

  useEffect(() => {
    setBrowserTimezone(detectBrowserTimezone());
  }, []);

  useEffect(() => {
    let active = true;

    getIntegrations()
      .then((value) => {
        if (!active) return;
        setIntegrations(value);
        setIntegrationsError(null);
      })
      .catch((err) => {
        if (!active) return;
        setIntegrationsError(errorMessage(err, "Failed to load integrations"));
      });

    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    let active = true;
    const initialLoad = refreshNonce === 0;

    if (initialLoad) {
      setLoading(true);
    } else {
      setRefreshing(true);
    }

    Promise.allSettled([
      getWorkflowRuns(),
      getWorkflowSchedules(),
      getWorkflowEventTriggers(),
    ])
      .then(([runsResult, schedulesResult, triggersResult]) => {
        if (!active) return;

        if (runsResult.status === "fulfilled") {
          setRuns(runsResult.value);
          setRunsError(null);
        } else {
          setRunsError(errorMessage(runsResult.reason, "Failed to load workflow runs"));
        }

        if (schedulesResult.status === "fulfilled") {
          setSchedules(schedulesResult.value);
          setSchedulesError(null);
        } else {
          setSchedulesError(
            errorMessage(schedulesResult.reason, "Failed to load workflow schedules"),
          );
        }

        if (triggersResult.status === "fulfilled") {
          setTriggers(triggersResult.value);
          setTriggersError(null);
        } else {
          setTriggersError(errorMessage(triggersResult.reason, "Failed to load workflow triggers"));
        }
      })
      .finally(() => {
        if (!active) return;
        if (initialLoad) {
          setLoading(false);
        } else {
          setRefreshing(false);
        }
      });

    return () => {
      active = false;
    };
  }, [refreshNonce]);

  useEffect(() => {
    setSelectedRunID((current) =>
      current && runs.some((run) => run.id === current) ? current : runs[0]?.id ?? null,
    );
  }, [runs]);

  useEffect(() => {
    setSelectedScheduleID((current) =>
      current && schedules.some((schedule) => schedule.id === current)
        ? current
        : schedules[0]?.id ?? null,
    );
  }, [schedules]);

  useEffect(() => {
    setSelectedTriggerID((current) =>
      current && triggers.some((trigger) => trigger.id === current)
        ? current
        : triggers[0]?.id ?? null,
    );
  }, [triggers]);

  useEffect(() => {
    if (!selectedRunID) {
      setSelectedRun(null);
      setDetailError(null);
      setActionError(null);
      return;
    }

    const cached = runs.find((run) => run.id === selectedRunID) ?? null;
    if (cached) {
      setSelectedRun(cached);
    }
    setActionError(null);

    let active = true;
    setDetailLoading(true);
    setDetailError(null);

    getWorkflowRun(selectedRunID)
      .then((run) => {
        if (!active) return;
        setSelectedRun(run);
      })
      .catch((err) => {
        if (!active) return;
        setDetailError(errorMessage(err, "Failed to load workflow run"));
      })
      .finally(() => {
        if (active) {
          setDetailLoading(false);
        }
      });

    return () => {
      active = false;
    };
  }, [runs, selectedRunID]);

  async function ensureOperationsLoaded(pluginName: string): Promise<void> {
    const normalized = pluginName.trim();
    if (!normalized) return;
    if (operationsByPlugin[normalized] || operationsLoadingByPlugin[normalized]) {
      return;
    }

    setOperationsLoadingByPlugin((current) => ({ ...current, [normalized]: true }));
    setOperationErrorsByPlugin((current) => ({ ...current, [normalized]: undefined }));

    try {
      const operations = await getIntegrationOperations(normalized);
      setOperationsByPlugin((current) => ({
        ...current,
        [normalized]: sortOperations(operations),
      }));
    } catch (err) {
      setOperationErrorsByPlugin((current) => ({
        ...current,
        [normalized]: errorMessage(err, "Failed to load plugin operations"),
      }));
    } finally {
      setOperationsLoadingByPlugin((current) => ({ ...current, [normalized]: false }));
    }
  }

  const workflowIntegrations = integrations
    .filter((integration) => !integration.mountedPath)
    .slice()
    .sort((left, right) =>
      integrationLabel(left).localeCompare(integrationLabel(right), undefined, {
        sensitivity: "base",
      }),
    );

  const filteredRuns = filterRuns(runs, deferredRunsQuery, runStatus);
  const filteredSchedules = filterSchedules(schedules, deferredSchedulesQuery, scheduleStatus);
  const filteredTriggers = filterTriggers(triggers, deferredTriggersQuery, triggerStatus);

  const selectedSchedule = schedules.find((schedule) => schedule.id === selectedScheduleID) ?? null;
  const selectedTrigger = triggers.find((trigger) => trigger.id === selectedTriggerID) ?? null;
  const selectedRunCancelable = selectedRun?.status === "pending";

  const failedRuns = runs.filter((run) => run.status === "failed").length;

  function upsertSchedule(schedule: WorkflowSchedule) {
    setSchedules((current) => {
      const index = current.findIndex((item) => item.id === schedule.id);
      if (index === -1) {
        return [schedule, ...current];
      }
      return current.map((item) => (item.id === schedule.id ? schedule : item));
    });
    setSelectedScheduleID(schedule.id);
  }

  function removeSchedule(scheduleID: string) {
    setSchedules((current) => current.filter((item) => item.id !== scheduleID));
    setSelectedScheduleID((current) => (current === scheduleID ? null : current));
  }

  function upsertTrigger(trigger: WorkflowEventTrigger) {
    setTriggers((current) => {
      const index = current.findIndex((item) => item.id === trigger.id);
      if (index === -1) {
        return [trigger, ...current];
      }
      return current.map((item) => (item.id === trigger.id ? trigger : item));
    });
    setSelectedTriggerID(trigger.id);
  }

  function removeTrigger(triggerID: string) {
    setTriggers((current) => current.filter((item) => item.id !== triggerID));
    setSelectedTriggerID((current) => (current === triggerID ? null : current));
  }

  async function handleCancelSelectedRun() {
    if (!selectedRunID || !selectedRunCancelable) return;

    setCanceling(true);
    setActionError(null);
    try {
      const updated = await cancelWorkflowRun(selectedRunID, "Run canceled.");
      setSelectedRun(updated);
      setRuns((current) => current.map((run) => (run.id === updated.id ? updated : run)));
      setRefreshNonce((value) => value + 1);
    } catch (err) {
      setActionError(errorMessage(err, "Failed to cancel workflow run"));
    } finally {
      setCanceling(false);
    }
  }

  return (
    <AuthGuard>
      <div className="min-h-screen">
        <Nav />
        <main className="mx-auto max-w-5xl px-6 py-12">
          <div className="animate-fade-in-up flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
            <div>
              <span className="label-text">Automation</span>
              <h1 className="mt-2 text-2xl font-heading font-bold text-primary">Workflows</h1>
              <p className="mt-2 max-w-3xl text-sm text-muted">
                Inspect workflow schedules, triggers, and recent run activity across
                plugins and providers.
              </p>
            </div>
            <button
              type="button"
              onClick={() => setRefreshNonce((value) => value + 1)}
              className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
            >
              {refreshing ? "Refreshing..." : "Refresh"}
            </button>
          </div>

          {loading ? (
            <p className="mt-10 text-sm text-faint">Loading...</p>
          ) : (
            <div className="mt-10 space-y-6 animate-fade-in-up [animation-delay:60ms]">
              <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                <SummaryCard label="Schedules" value={String(schedules.length)} tone="default" />
                <SummaryCard label="Triggers" value={String(triggers.length)} tone="sky" />
                <SummaryCard label="Runs" value={String(runs.length)} tone="grove" />
                <SummaryCard label="Failed runs" value={String(failedRuns)} tone="ember" />
              </section>

              <section className="rounded-lg border border-alpha bg-base-100 p-4 dark:bg-surface">
                <div
                  role="tablist"
                  aria-label="Workflow surfaces"
                  className="flex flex-wrap gap-2 border-b border-alpha pb-4"
                >
                  <WorkflowTabButton
                    active={activeTab === "runs"}
                    label="Runs"
                    count={runs.length}
                    onClick={() => setActiveTab("runs")}
                  />
                  <WorkflowTabButton
                    active={activeTab === "schedules"}
                    label="Schedules"
                    count={schedules.length}
                    onClick={() => setActiveTab("schedules")}
                  />
                  <WorkflowTabButton
                    active={activeTab === "triggers"}
                    label="Triggers"
                    count={triggers.length}
                    onClick={() => setActiveTab("triggers")}
                  />
                </div>

                <div className="mt-4 flex flex-col gap-3 lg:flex-row">
                  {activeTab === "runs" ? (
                    <>
                      <input
                        value={runsQuery}
                        onChange={(event) => setRunsQuery(event.target.value)}
                        placeholder="Search by run, plugin, provider, or trigger"
                        className="min-w-0 flex-1 rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
                      />
                      <select
                        value={runStatus}
                        onChange={(event) => setRunStatus(event.target.value)}
                        className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
                      >
                        <option value="all">All statuses</option>
                        <option value="pending">Pending</option>
                        <option value="running">Running</option>
                        <option value="succeeded">Succeeded</option>
                        <option value="failed">Failed</option>
                        <option value="canceled">Canceled</option>
                      </select>
                    </>
                  ) : activeTab === "schedules" ? (
                    <>
                      <input
                        value={schedulesQuery}
                        onChange={(event) => setSchedulesQuery(event.target.value)}
                        placeholder="Search by schedule, plugin, operation, or cadence"
                        className="min-w-0 flex-1 rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
                      />
                      <select
                        value={scheduleStatus}
                        onChange={(event) => setScheduleStatus(event.target.value)}
                        className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
                      >
                        <option value="all">All schedules</option>
                        <option value="active">Active</option>
                        <option value="paused">Paused</option>
                      </select>
                    </>
                  ) : (
                    <>
                      <input
                        value={triggersQuery}
                        onChange={(event) => setTriggersQuery(event.target.value)}
                        placeholder="Search by trigger, event type, plugin, or provider"
                        className="min-w-0 flex-1 rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
                      />
                      <select
                        value={triggerStatus}
                        onChange={(event) => setTriggerStatus(event.target.value)}
                        className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
                      >
                        <option value="all">All triggers</option>
                        <option value="active">Active</option>
                        <option value="paused">Paused</option>
                      </select>
                    </>
                  )}
                </div>
              </section>

              {activeTab === "runs" ? (
                <RunsPanel
                  runs={filteredRuns}
                  runsError={runsError}
                  selectedRunID={selectedRunID}
                  selectedRun={selectedRun}
                  detailLoading={detailLoading}
                  detailError={detailError}
                  actionError={actionError}
                  canceling={canceling}
                  selectedRunCancelable={selectedRunCancelable}
                  onSelectRun={setSelectedRunID}
                  onCancelSelectedRun={handleCancelSelectedRun}
                />
              ) : activeTab === "schedules" ? (
                <SchedulesPanel
                  schedules={filteredSchedules}
                  schedulesError={schedulesError}
                  selectedScheduleID={selectedScheduleID}
                  selectedSchedule={selectedSchedule}
                  integrations={workflowIntegrations}
                  integrationsError={integrationsError}
                  browserTimezone={browserTimezone}
                  operationsByPlugin={operationsByPlugin}
                  operationsLoadingByPlugin={operationsLoadingByPlugin}
                  operationErrorsByPlugin={operationErrorsByPlugin}
                  ensureOperationsLoaded={ensureOperationsLoaded}
                  onSelectSchedule={setSelectedScheduleID}
                  onScheduleUpsert={upsertSchedule}
                  onScheduleDeleted={removeSchedule}
                />
              ) : (
                <TriggersPanel
                  triggers={filteredTriggers}
                  triggersError={triggersError}
                  selectedTriggerID={selectedTriggerID}
                  selectedTrigger={selectedTrigger}
                  integrations={workflowIntegrations}
                  integrationsError={integrationsError}
                  operationsByPlugin={operationsByPlugin}
                  operationsLoadingByPlugin={operationsLoadingByPlugin}
                  operationErrorsByPlugin={operationErrorsByPlugin}
                  ensureOperationsLoaded={ensureOperationsLoaded}
                  onSelectTrigger={setSelectedTriggerID}
                  onTriggerUpsert={upsertTrigger}
                  onTriggerDeleted={removeTrigger}
                />
              )}
            </div>
          )}
        </main>
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
  selectedRunCancelable,
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
  selectedRunCancelable: boolean;
  onSelectRun: (id: string) => void;
  onCancelSelectedRun: () => void | Promise<void>;
}) {
  return (
    <div className="grid gap-6 lg:grid-cols-[minmax(0,1.15fr)_minmax(20rem,0.85fr)]">
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
              <div className="px-5 py-8 text-sm text-faint">No workflow runs yet.</div>
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
                      <div className="flex items-center gap-2">
                        <span className="truncate text-sm font-medium text-primary">
                          {run.target.plugin}.{run.target.operation}
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
          <div>
            <h2 className="text-sm font-medium text-primary">Run Details</h2>
            <p className="mt-1 text-xs text-faint">{selectedRun?.id || "Select a run"}</p>
          </div>
          {selectedRun?.status ? (
            <span className={runStatusClassName(selectedRun.status)}>{selectedRun.status}</span>
          ) : null}
        </div>

        {selectedRunCancelable ? (
          <div className="mt-4 flex items-center justify-between gap-3 rounded-md border border-alpha bg-background/65 px-4 py-3 dark:bg-background/20">
            <p className="text-sm text-muted">
              Canceling a run asks the workflow provider to stop it as soon as possible.
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
          <div className="mt-6 space-y-6">
            <div className="grid gap-4 sm:grid-cols-2">
              <DetailItem label="Provider" value={selectedRun.provider} />
              <DetailItem label="Trigger" value={runTriggerLabel(selectedRun)} />
              <DetailItem label="Created" value={formatDate(selectedRun.createdAt)} />
              <DetailItem label="Started" value={formatDate(selectedRun.startedAt)} />
              <DetailItem label="Completed" value={formatDate(selectedRun.completedAt)} />
              <DetailItem
                label="Actor"
                value={
                  selectedRun.createdBy?.displayName || selectedRun.createdBy?.subjectId || "-"
                }
              />
            </div>

            <TargetDetails target={selectedRun.target} />

            <section>
              <h3 className="text-xs font-medium uppercase tracking-[0.18em] text-faint">
                Result
              </h3>
              <div className="mt-3 rounded-md border border-alpha bg-background/65 p-4 text-sm dark:bg-background/20">
                <p className="text-sm text-primary">
                  {selectedRun.statusMessage || "No status message"}
                </p>
                {selectedRun.resultBody ? (
                  <pre className="mt-3 overflow-x-auto text-xs text-primary">
                    {prettyResultBody(selectedRun.resultBody)}
                  </pre>
                ) : (
                  <p className="mt-3 text-xs text-faint">No result body captured.</p>
                )}
              </div>
            </section>
          </div>
        ) : (
          <p className="mt-6 text-sm text-faint">Select a workflow run to inspect it.</p>
        )}
      </section>
    </div>
  );
}

function SchedulesPanel({
  schedules,
  schedulesError,
  selectedScheduleID,
  selectedSchedule,
  integrations,
  integrationsError,
  browserTimezone,
  operationsByPlugin,
  operationsLoadingByPlugin,
  operationErrorsByPlugin,
  ensureOperationsLoaded,
  onSelectSchedule,
  onScheduleUpsert,
  onScheduleDeleted,
}: {
  schedules: WorkflowSchedule[];
  schedulesError: string | null;
  selectedScheduleID: string | null;
  selectedSchedule: WorkflowSchedule | null;
  integrations: Integration[];
  integrationsError: string | null;
  browserTimezone: string;
  operationsByPlugin: Record<string, IntegrationOperation[]>;
  operationsLoadingByPlugin: Record<string, boolean>;
  operationErrorsByPlugin: Record<string, string | undefined>;
  ensureOperationsLoaded: (pluginName: string) => Promise<void>;
  onSelectSchedule: (id: string | null) => void;
  onScheduleUpsert: (schedule: WorkflowSchedule) => void;
  onScheduleDeleted: (scheduleID: string) => void;
}) {
  const [formMode, setFormMode] = useState<WorkflowFormMode>(null);
  const [form, setForm] = useState<ScheduleFormState>(() =>
    defaultScheduleForm(browserTimezone),
  );
  const [formError, setFormError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [presetWarning, setPresetWarning] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [togglingPause, setTogglingPause] = useState(false);

  const ensureOperationsLoadedEvent = useEffectEvent((pluginName: string) => {
    void ensureOperationsLoaded(pluginName);
  });

  const operationOptions = form.plugin
    ? operationsByPlugin[form.plugin] ?? EMPTY_OPERATIONS
    : EMPTY_OPERATIONS;
  const operationsLoading = form.plugin ? Boolean(operationsLoadingByPlugin[form.plugin]) : false;
  const operationsError = form.plugin ? operationErrorsByPlugin[form.plugin] ?? null : null;

  useEffect(() => {
    if (!formMode) return;
    if (!form.plugin && integrations[0]) {
      setForm((current) => ({ ...current, plugin: integrations[0].name }));
    }
  }, [formMode, form.plugin, integrations]);

  useEffect(() => {
    if (!formMode || !form.plugin) return;
    ensureOperationsLoadedEvent(form.plugin);
  }, [formMode, form.plugin]);

  useEffect(() => {
    if (!formMode || !form.plugin || operationsLoading) return;
    if (operationOptions.length === 0) {
      if (form.operation) {
        setForm((current) => ({ ...current, operation: "" }));
      }
      return;
    }
    if (!operationOptions.some((operation) => operation.id === form.operation)) {
      setForm((current) => ({ ...current, operation: operationOptions[0].id }));
    }
  }, [formMode, form.plugin, form.operation, operationOptions, operationsLoading]);

  function beginCreate() {
    setForm(defaultScheduleForm(browserTimezone, integrations[0]?.name ?? ""));
    setPresetWarning(null);
    setFormError(null);
    setNotice(null);
    setFormMode("create");
  }

  function beginEdit() {
    if (!selectedSchedule) return;
    const next = scheduleFormFromSchedule(selectedSchedule, browserTimezone);
    setForm(next.form);
    setPresetWarning(next.warning);
    setFormError(null);
    setNotice(null);
    setFormMode("edit");
  }

  function cancelForm() {
    setFormMode(null);
    setFormError(null);
    setPresetWarning(null);
  }

  async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setFormError(null);
    setNotice(null);

    try {
      const body = scheduleFormToUpsert(
        form,
        browserTimezone,
        formMode === "edit" ? selectedSchedule?.provider : undefined,
      );

      setSubmitting(true);
      const saved =
        formMode === "edit" && selectedSchedule
          ? await updateWorkflowSchedule(selectedSchedule.id, body)
          : await createWorkflowSchedule(body);

      onScheduleUpsert(saved);
      setFormMode(null);
      setPresetWarning(null);
      setNotice(formMode === "edit" ? "Schedule updated." : "Schedule created.");
    } catch (err) {
      setFormError(errorMessage(err, "Failed to save workflow schedule"));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDelete() {
    if (!selectedSchedule) return;
    if (!window.confirm(`Delete schedule ${selectedSchedule.id}?`)) return;

    setDeleting(true);
    setFormError(null);
    setNotice(null);

    try {
      await deleteWorkflowSchedule(selectedSchedule.id);
      onScheduleDeleted(selectedSchedule.id);
      setFormMode(null);
      setPresetWarning(null);
      setNotice("Schedule deleted.");
    } catch (err) {
      setFormError(errorMessage(err, "Failed to delete workflow schedule"));
    } finally {
      setDeleting(false);
    }
  }

  async function handleTogglePause() {
    if (!selectedSchedule) return;

    setTogglingPause(true);
    setFormError(null);
    setNotice(null);

    try {
      const updated = selectedSchedule.paused
        ? await resumeWorkflowSchedule(selectedSchedule.id)
        : await pauseWorkflowSchedule(selectedSchedule.id);
      onScheduleUpsert(updated);
      setNotice(updated.paused ? "Schedule paused." : "Schedule resumed.");
    } catch (err) {
      setFormError(errorMessage(err, "Failed to update workflow schedule"));
    } finally {
      setTogglingPause(false);
    }
  }

  return (
    <div className="grid gap-6 lg:grid-cols-[minmax(0,1.1fr)_minmax(22rem,0.9fr)]">
      <section className="rounded-lg border border-alpha bg-base-100 dark:bg-surface">
        <div className="flex items-center justify-between gap-4 border-b border-alpha px-5 py-4">
          <div>
            <h2 className="text-sm font-medium text-primary">Workflow Schedules</h2>
            <p className="mt-1 text-xs text-faint">{schedules.length} shown</p>
          </div>
          <button
            type="button"
            onClick={beginCreate}
            className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
          >
            New schedule
          </button>
        </div>

        {schedulesError ? (
          <p className="px-5 py-8 text-sm text-ember-500">{schedulesError}</p>
        ) : (
          <div className="divide-y divide-alpha">
            {schedules.length === 0 ? (
              <div className="px-5 py-8 text-sm text-faint">No workflow schedules yet.</div>
            ) : (
              schedules.map((schedule) => {
                const isActive = schedule.id === selectedScheduleID;
                return (
                  <button
                    key={schedule.id}
                    type="button"
                    onClick={() => onSelectSchedule(schedule.id)}
                    className={`flex w-full items-start justify-between gap-4 px-5 py-4 text-left transition-colors duration-150 ${
                      isActive ? "bg-alpha-5" : "hover:bg-alpha-5"
                    }`}
                  >
                    <div className="min-w-0">
                      <div className="flex items-center gap-2">
                        <span className="truncate text-sm font-medium text-primary">
                          {schedule.target.plugin}.{schedule.target.operation}
                        </span>
                        <span className={pausedStateClassName(schedule.paused)}>
                          {schedule.paused ? "paused" : "active"}
                        </span>
                      </div>
                      <p className="mt-1 truncate text-xs text-faint">{schedule.id}</p>
                      <p className="mt-2 text-xs text-muted">
                        {scheduleCadenceLabel(schedule.cron)} · {schedule.provider}
                      </p>
                    </div>
                    <div className="shrink-0 text-right text-xs text-faint">
                      {formatDate(schedule.nextRunAt)}
                    </div>
                  </button>
                );
              })
            )}
          </div>
        )}
      </section>

      <section className="rounded-lg border border-alpha bg-base-100 p-5 dark:bg-surface">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h2 className="text-sm font-medium text-primary">
              {formMode === "create"
                ? "Create Schedule"
                : formMode === "edit"
                  ? "Edit Schedule"
                  : "Schedule Details"}
            </h2>
            <p className="mt-1 text-xs text-faint">
              {formMode
                ? formMode === "edit"
                  ? selectedSchedule?.id || "Selected schedule"
                  : "Use the existing schedule API"
                : selectedSchedule?.id || "Select a schedule"}
            </p>
          </div>

          {formMode ? (
            <button
              type="button"
              onClick={cancelForm}
              className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
            >
              Cancel
            </button>
          ) : selectedSchedule ? (
            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                onClick={beginEdit}
                className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
              >
                Edit
              </button>
              <button
                type="button"
                onClick={() => void handleTogglePause()}
                disabled={togglingPause}
                className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 disabled:cursor-not-allowed disabled:opacity-60 dark:bg-surface"
              >
                {togglingPause
                  ? selectedSchedule.paused
                    ? "Resuming..."
                    : "Pausing..."
                  : selectedSchedule.paused
                    ? "Resume"
                    : "Pause"}
              </button>
              <button
                type="button"
                onClick={() => void handleDelete()}
                disabled={deleting}
                className="rounded-md bg-ember-500 px-3 py-2 text-sm font-medium text-white transition-opacity duration-150 hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {deleting ? "Deleting..." : "Delete"}
              </button>
            </div>
          ) : null}
        </div>

        {notice ? <p className="mt-4 text-sm text-grove-700 dark:text-grove-200">{notice}</p> : null}
        {formError ? <p className="mt-4 text-sm text-ember-500">{formError}</p> : null}

        {formMode ? (
          <form className="mt-6 space-y-6" onSubmit={handleSubmit}>
            {presetWarning ? (
              <div className="rounded-md border border-alpha bg-background/65 px-4 py-3 text-sm text-muted dark:bg-background/20">
                {presetWarning}
              </div>
            ) : null}

            <div className="grid gap-4 sm:grid-cols-2">
              <label className="space-y-2 text-sm">
                <span className="text-muted">Cadence</span>
                <select
                  value={form.cadence}
                  onChange={(event) =>
                    setForm((current) => ({
                      ...current,
                      cadence: event.target.value as ScheduleCadence,
                    }))
                  }
                  className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
                >
                  {SCHEDULE_CADENCE_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>

              <label className="space-y-2 text-sm">
                <span className="text-muted">Timezone</span>
                <select
                  value={form.timezoneMode}
                  onChange={(event) =>
                    setForm((current) => ({
                      ...current,
                      timezoneMode: event.target.value as TimezoneMode,
                    }))
                  }
                  className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
                >
                  <option value="local">Current timezone ({browserTimezone})</option>
                  <option value="utc">UTC</option>
                </select>
              </label>

              {form.cadence !== "hourly" ? (
                <label className="space-y-2 text-sm">
                  <span className="text-muted">Time</span>
                  <select
                    value={form.hour}
                    onChange={(event) =>
                      setForm((current) => ({ ...current, hour: event.target.value }))
                    }
                    className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
                  >
                    {HOUR_OPTIONS.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </label>
              ) : null}

              {form.cadence === "weekly" ? (
                <label className="space-y-2 text-sm">
                  <span className="text-muted">Day</span>
                  <select
                    value={form.weekday}
                    onChange={(event) =>
                      setForm((current) => ({ ...current, weekday: event.target.value }))
                    }
                    className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
                  >
                    {WEEKDAY_OPTIONS.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </label>
              ) : null}

              {form.cadence === "monthly" ? (
                <label className="space-y-2 text-sm">
                  <span className="text-muted">Day of month</span>
                  <select
                    value={form.monthDay}
                    onChange={(event) =>
                      setForm((current) => ({ ...current, monthDay: event.target.value }))
                    }
                    className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
                  >
                    {MONTH_DAY_OPTIONS.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </label>
              ) : null}
            </div>

            <div className="rounded-md border border-alpha bg-background/65 px-4 py-3 text-sm dark:bg-background/20">
              <p className="text-[11px] uppercase tracking-[0.18em] text-faint">Cron preview</p>
              <p className="mt-2 font-mono text-primary">{cronFromScheduleForm(form)}</p>
            </div>

            <WorkflowTargetEditor
              integrations={integrations}
              integrationsError={integrationsError}
              operations={operationOptions}
              operationsLoading={operationsLoading}
              operationsError={operationsError}
              plugin={form.plugin}
              operation={form.operation}
              connection={form.connection}
              instance={form.instance}
              inputJSON={form.inputJSON}
              onPluginChange={(value) =>
                setForm((current) => ({
                  ...current,
                  plugin: value,
                  operation: "",
                }))
              }
              onOperationChange={(value) =>
                setForm((current) => ({ ...current, operation: value }))
              }
              onConnectionChange={(value) =>
                setForm((current) => ({ ...current, connection: value }))
              }
              onInstanceChange={(value) =>
                setForm((current) => ({ ...current, instance: value }))
              }
              onInputJSONChange={(value) =>
                setForm((current) => ({ ...current, inputJSON: value }))
              }
            />

            <label className="flex items-center gap-3 text-sm text-muted">
              <input
                type="checkbox"
                checked={form.paused}
                onChange={(event) =>
                  setForm((current) => ({ ...current, paused: event.target.checked }))
                }
                className="h-4 w-4 rounded border-alpha"
              />
              Create or save this schedule as paused
            </label>

            <div className="flex flex-wrap gap-3">
              <button
                type="submit"
                disabled={submitting}
                className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-background transition-opacity duration-150 hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {submitting
                  ? formMode === "edit"
                    ? "Saving..."
                    : "Creating..."
                  : formMode === "edit"
                    ? "Save schedule"
                    : "Create schedule"}
              </button>
              <button
                type="button"
                onClick={cancelForm}
                className="rounded-md border border-alpha bg-base-100 px-4 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
              >
                Cancel
              </button>
            </div>
          </form>
        ) : selectedSchedule ? (
          <div className="mt-6 space-y-6">
            <div className="grid gap-4 sm:grid-cols-2">
              <DetailItem label="Provider" value={selectedSchedule.provider} />
              <DetailItem label="Cadence" value={scheduleCadenceLabel(selectedSchedule.cron)} />
              <DetailItem
                label="Timezone"
                value={selectedSchedule.timezone || "Default timezone"}
              />
              <DetailItem label="Next run" value={formatDate(selectedSchedule.nextRunAt)} />
              <DetailItem label="Created" value={formatDate(selectedSchedule.createdAt)} />
              <DetailItem label="Updated" value={formatDate(selectedSchedule.updatedAt)} />
            </div>

            <div className="rounded-md border border-alpha bg-background/65 px-4 py-3 text-sm dark:bg-background/20">
              <p className="text-[11px] uppercase tracking-[0.18em] text-faint">Cron</p>
              <p className="mt-2 font-mono text-primary">{selectedSchedule.cron}</p>
            </div>

            <TargetDetails target={selectedSchedule.target} />
          </div>
        ) : (
          <div className="mt-6 space-y-4">
            <p className="text-sm text-faint">Select a workflow schedule to inspect it.</p>
            <button
              type="button"
              onClick={beginCreate}
              className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
            >
              Create schedule
            </button>
          </div>
        )}
      </section>
    </div>
  );
}

function TriggersPanel({
  triggers,
  triggersError,
  selectedTriggerID,
  selectedTrigger,
  integrations,
  integrationsError,
  operationsByPlugin,
  operationsLoadingByPlugin,
  operationErrorsByPlugin,
  ensureOperationsLoaded,
  onSelectTrigger,
  onTriggerUpsert,
  onTriggerDeleted,
}: {
  triggers: WorkflowEventTrigger[];
  triggersError: string | null;
  selectedTriggerID: string | null;
  selectedTrigger: WorkflowEventTrigger | null;
  integrations: Integration[];
  integrationsError: string | null;
  operationsByPlugin: Record<string, IntegrationOperation[]>;
  operationsLoadingByPlugin: Record<string, boolean>;
  operationErrorsByPlugin: Record<string, string | undefined>;
  ensureOperationsLoaded: (pluginName: string) => Promise<void>;
  onSelectTrigger: (id: string | null) => void;
  onTriggerUpsert: (trigger: WorkflowEventTrigger) => void;
  onTriggerDeleted: (triggerID: string) => void;
}) {
  const [formMode, setFormMode] = useState<WorkflowFormMode>(null);
  const [form, setForm] = useState<TriggerFormState>(() => defaultTriggerForm());
  const [formError, setFormError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [togglingPause, setTogglingPause] = useState(false);

  const ensureOperationsLoadedEvent = useEffectEvent((pluginName: string) => {
    void ensureOperationsLoaded(pluginName);
  });

  const operationOptions = form.plugin
    ? operationsByPlugin[form.plugin] ?? EMPTY_OPERATIONS
    : EMPTY_OPERATIONS;
  const operationsLoading = form.plugin ? Boolean(operationsLoadingByPlugin[form.plugin]) : false;
  const operationsError = form.plugin ? operationErrorsByPlugin[form.plugin] ?? null : null;

  useEffect(() => {
    if (!formMode) return;
    if (!form.plugin && integrations[0]) {
      setForm((current) => ({ ...current, plugin: integrations[0].name }));
    }
  }, [formMode, form.plugin, integrations]);

  useEffect(() => {
    if (!formMode || !form.plugin) return;
    ensureOperationsLoadedEvent(form.plugin);
  }, [formMode, form.plugin]);

  useEffect(() => {
    if (!formMode || !form.plugin || operationsLoading) return;
    if (operationOptions.length === 0) {
      if (form.operation) {
        setForm((current) => ({ ...current, operation: "" }));
      }
      return;
    }
    if (!operationOptions.some((operation) => operation.id === form.operation)) {
      setForm((current) => ({ ...current, operation: operationOptions[0].id }));
    }
  }, [formMode, form.plugin, form.operation, operationOptions, operationsLoading]);

  function beginCreate() {
    setForm(defaultTriggerForm(integrations[0]?.name ?? ""));
    setFormError(null);
    setNotice(null);
    setFormMode("create");
  }

  function beginEdit() {
    if (!selectedTrigger) return;
    setForm(triggerFormFromTrigger(selectedTrigger));
    setFormError(null);
    setNotice(null);
    setFormMode("edit");
  }

  function cancelForm() {
    setFormMode(null);
    setFormError(null);
  }

  async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setFormError(null);
    setNotice(null);

    try {
      const body = triggerFormToUpsert(
        form,
        formMode === "edit" ? selectedTrigger?.provider : undefined,
      );

      setSubmitting(true);
      const saved =
        formMode === "edit" && selectedTrigger
          ? await updateWorkflowEventTrigger(selectedTrigger.id, body)
          : await createWorkflowEventTrigger(body);

      onTriggerUpsert(saved);
      setFormMode(null);
      setNotice(formMode === "edit" ? "Trigger updated." : "Trigger created.");
    } catch (err) {
      setFormError(errorMessage(err, "Failed to save workflow trigger"));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDelete() {
    if (!selectedTrigger) return;
    if (!window.confirm(`Delete trigger ${selectedTrigger.id}?`)) return;

    setDeleting(true);
    setFormError(null);
    setNotice(null);

    try {
      await deleteWorkflowEventTrigger(selectedTrigger.id);
      onTriggerDeleted(selectedTrigger.id);
      setFormMode(null);
      setNotice("Trigger deleted.");
    } catch (err) {
      setFormError(errorMessage(err, "Failed to delete workflow trigger"));
    } finally {
      setDeleting(false);
    }
  }

  async function handleTogglePause() {
    if (!selectedTrigger) return;

    setTogglingPause(true);
    setFormError(null);
    setNotice(null);

    try {
      const updated = selectedTrigger.paused
        ? await resumeWorkflowEventTrigger(selectedTrigger.id)
        : await pauseWorkflowEventTrigger(selectedTrigger.id);
      onTriggerUpsert(updated);
      setNotice(updated.paused ? "Trigger paused." : "Trigger resumed.");
    } catch (err) {
      setFormError(errorMessage(err, "Failed to update workflow trigger"));
    } finally {
      setTogglingPause(false);
    }
  }

  return (
    <div className="grid gap-6 lg:grid-cols-[minmax(0,1.1fr)_minmax(22rem,0.9fr)]">
      <section className="rounded-lg border border-alpha bg-base-100 dark:bg-surface">
        <div className="flex items-center justify-between gap-4 border-b border-alpha px-5 py-4">
          <div>
            <h2 className="text-sm font-medium text-primary">Workflow Triggers</h2>
            <p className="mt-1 text-xs text-faint">{triggers.length} shown</p>
          </div>
          <button
            type="button"
            onClick={beginCreate}
            className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
          >
            New trigger
          </button>
        </div>

        {triggersError ? (
          <p className="px-5 py-8 text-sm text-ember-500">{triggersError}</p>
        ) : (
          <div className="divide-y divide-alpha">
            {triggers.length === 0 ? (
              <div className="px-5 py-8 text-sm text-faint">No workflow triggers yet.</div>
            ) : (
              triggers.map((trigger) => {
                const isActive = trigger.id === selectedTriggerID;
                return (
                  <button
                    key={trigger.id}
                    type="button"
                    onClick={() => onSelectTrigger(trigger.id)}
                    className={`flex w-full items-start justify-between gap-4 px-5 py-4 text-left transition-colors duration-150 ${
                      isActive ? "bg-alpha-5" : "hover:bg-alpha-5"
                    }`}
                  >
                    <div className="min-w-0">
                      <div className="flex items-center gap-2">
                        <span className="truncate text-sm font-medium text-primary">
                          {trigger.target.plugin}.{trigger.target.operation}
                        </span>
                        <span className={pausedStateClassName(trigger.paused)}>
                          {trigger.paused ? "paused" : "active"}
                        </span>
                      </div>
                      <p className="mt-1 truncate text-xs text-faint">{trigger.id}</p>
                      <p className="mt-2 text-xs text-muted">
                        {trigger.match.type} · {trigger.provider}
                      </p>
                    </div>
                    <div className="shrink-0 text-right text-xs text-faint">
                      {formatDate(trigger.updatedAt || trigger.createdAt)}
                    </div>
                  </button>
                );
              })
            )}
          </div>
        )}
      </section>

      <section className="rounded-lg border border-alpha bg-base-100 p-5 dark:bg-surface">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h2 className="text-sm font-medium text-primary">
              {formMode === "create"
                ? "Create Trigger"
                : formMode === "edit"
                  ? "Edit Trigger"
                  : "Trigger Details"}
            </h2>
            <p className="mt-1 text-xs text-faint">
              {formMode
                ? formMode === "edit"
                  ? selectedTrigger?.id || "Selected trigger"
                  : "Use the existing trigger API"
                : selectedTrigger?.id || "Select a trigger"}
            </p>
          </div>

          {formMode ? (
            <button
              type="button"
              onClick={cancelForm}
              className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
            >
              Cancel
            </button>
          ) : selectedTrigger ? (
            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                onClick={beginEdit}
                className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
              >
                Edit
              </button>
              <button
                type="button"
                onClick={() => void handleTogglePause()}
                disabled={togglingPause}
                className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 disabled:cursor-not-allowed disabled:opacity-60 dark:bg-surface"
              >
                {togglingPause
                  ? selectedTrigger.paused
                    ? "Resuming..."
                    : "Pausing..."
                  : selectedTrigger.paused
                    ? "Resume"
                    : "Pause"}
              </button>
              <button
                type="button"
                onClick={() => void handleDelete()}
                disabled={deleting}
                className="rounded-md bg-ember-500 px-3 py-2 text-sm font-medium text-white transition-opacity duration-150 hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {deleting ? "Deleting..." : "Delete"}
              </button>
            </div>
          ) : null}
        </div>

        {notice ? <p className="mt-4 text-sm text-grove-700 dark:text-grove-200">{notice}</p> : null}
        {formError ? <p className="mt-4 text-sm text-ember-500">{formError}</p> : null}

        {formMode ? (
          <form className="mt-6 space-y-6" onSubmit={handleSubmit}>
            <div className="grid gap-4 sm:grid-cols-2">
              <label className="space-y-2 text-sm sm:col-span-2">
                <span className="text-muted">Event type</span>
                <input
                  value={form.type}
                  onChange={(event) =>
                    setForm((current) => ({ ...current, type: event.target.value }))
                  }
                  placeholder="repo.push"
                  className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
                />
              </label>

              <label className="space-y-2 text-sm">
                <span className="text-muted">Source</span>
                <input
                  value={form.source}
                  onChange={(event) =>
                    setForm((current) => ({ ...current, source: event.target.value }))
                  }
                  placeholder="github"
                  className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
                />
              </label>

              <label className="space-y-2 text-sm">
                <span className="text-muted">Subject</span>
                <input
                  value={form.subject}
                  onChange={(event) =>
                    setForm((current) => ({ ...current, subject: event.target.value }))
                  }
                  placeholder="valon-technologies/gestalt"
                  className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
                />
              </label>
            </div>

            <WorkflowTargetEditor
              integrations={integrations}
              integrationsError={integrationsError}
              operations={operationOptions}
              operationsLoading={operationsLoading}
              operationsError={operationsError}
              plugin={form.plugin}
              operation={form.operation}
              connection={form.connection}
              instance={form.instance}
              inputJSON={form.inputJSON}
              onPluginChange={(value) =>
                setForm((current) => ({
                  ...current,
                  plugin: value,
                  operation: "",
                }))
              }
              onOperationChange={(value) =>
                setForm((current) => ({ ...current, operation: value }))
              }
              onConnectionChange={(value) =>
                setForm((current) => ({ ...current, connection: value }))
              }
              onInstanceChange={(value) =>
                setForm((current) => ({ ...current, instance: value }))
              }
              onInputJSONChange={(value) =>
                setForm((current) => ({ ...current, inputJSON: value }))
              }
            />

            <label className="flex items-center gap-3 text-sm text-muted">
              <input
                type="checkbox"
                checked={form.paused}
                onChange={(event) =>
                  setForm((current) => ({ ...current, paused: event.target.checked }))
                }
                className="h-4 w-4 rounded border-alpha"
              />
              Create or save this trigger as paused
            </label>

            <div className="flex flex-wrap gap-3">
              <button
                type="submit"
                disabled={submitting}
                className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-background transition-opacity duration-150 hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {submitting
                  ? formMode === "edit"
                    ? "Saving..."
                    : "Creating..."
                  : formMode === "edit"
                    ? "Save trigger"
                    : "Create trigger"}
              </button>
              <button
                type="button"
                onClick={cancelForm}
                className="rounded-md border border-alpha bg-base-100 px-4 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
              >
                Cancel
              </button>
            </div>
          </form>
        ) : selectedTrigger ? (
          <div className="mt-6 space-y-6">
            <div className="grid gap-4 sm:grid-cols-2">
              <DetailItem label="Provider" value={selectedTrigger.provider} />
              <DetailItem label="Event type" value={selectedTrigger.match.type} />
              <DetailItem label="Source" value={selectedTrigger.match.source || "-"} />
              <DetailItem label="Subject" value={selectedTrigger.match.subject || "-"} />
              <DetailItem label="Created" value={formatDate(selectedTrigger.createdAt)} />
              <DetailItem label="Updated" value={formatDate(selectedTrigger.updatedAt)} />
            </div>

            <section>
              <h3 className="text-xs font-medium uppercase tracking-[0.18em] text-faint">
                Match
              </h3>
              <div className="mt-3 rounded-md border border-alpha bg-background/65 p-4 text-sm dark:bg-background/20">
                <p className="font-medium text-primary">{selectedTrigger.match.type}</p>
                <p className="mt-2 text-xs text-muted">
                  Source: {selectedTrigger.match.source || "-"} · Subject:{" "}
                  {selectedTrigger.match.subject || "-"}
                </p>
              </div>
            </section>

            <TargetDetails target={selectedTrigger.target} />
          </div>
        ) : (
          <div className="mt-6 space-y-4">
            <p className="text-sm text-faint">Select a workflow trigger to inspect it.</p>
            <button
              type="button"
              onClick={beginCreate}
              className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
            >
              Create trigger
            </button>
          </div>
        )}
      </section>
    </div>
  );
}

function WorkflowTargetEditor({
  integrations,
  integrationsError,
  operations,
  operationsLoading,
  operationsError,
  plugin,
  operation,
  connection,
  instance,
  inputJSON,
  onPluginChange,
  onOperationChange,
  onConnectionChange,
  onInstanceChange,
  onInputJSONChange,
}: TargetEditorProps) {
  return (
    <section className="space-y-4 rounded-md border border-alpha bg-background/65 p-4 dark:bg-background/20">
      <div>
        <h3 className="text-xs font-medium uppercase tracking-[0.18em] text-faint">Target</h3>
        <p className="mt-2 text-sm text-muted">
          Choose the plugin operation this workflow should invoke.
        </p>
      </div>

      {integrationsError ? (
        <p className="text-sm text-ember-500">{integrationsError}</p>
      ) : null}

      <div className="grid gap-4 sm:grid-cols-2">
        <label className="space-y-2 text-sm">
          <span className="text-muted">Plugin</span>
          <select
            value={plugin}
            onChange={(event) => onPluginChange(event.target.value)}
            className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
          >
            <option value="">Select a plugin</option>
            {integrations.map((integration) => (
              <option key={integration.name} value={integration.name}>
                {integrationLabel(integration)}
              </option>
            ))}
          </select>
        </label>

        <label className="space-y-2 text-sm">
          <span className="text-muted">Operation</span>
          <select
            value={operation}
            onChange={(event) => onOperationChange(event.target.value)}
            disabled={!plugin || operationsLoading}
            className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong disabled:cursor-not-allowed disabled:opacity-60 dark:bg-surface"
          >
            <option value="">
              {!plugin
                ? "Select a plugin first"
                : operationsLoading
                  ? "Loading operations..."
                  : operations.length === 0
                    ? "No operations available"
                    : "Select an operation"}
            </option>
            {operations.map((item) => (
              <option key={item.id} value={item.id}>
                {operationLabel(item)}
              </option>
            ))}
          </select>
        </label>
      </div>

      {operationsError ? <p className="text-sm text-ember-500">{operationsError}</p> : null}

      <div className="grid gap-4 sm:grid-cols-2">
        <label className="space-y-2 text-sm">
          <span className="text-muted">Connection</span>
          <input
            value={connection}
            onChange={(event) => onConnectionChange(event.target.value)}
            placeholder="default"
            className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
          />
        </label>

        <label className="space-y-2 text-sm">
          <span className="text-muted">Instance</span>
          <input
            value={instance}
            onChange={(event) => onInstanceChange(event.target.value)}
            placeholder="Optional"
            className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
          />
        </label>
      </div>

      <label className="block space-y-2 text-sm">
        <span className="text-muted">Input JSON</span>
        <textarea
          value={inputJSON}
          onChange={(event) => onInputJSONChange(event.target.value)}
          rows={8}
          placeholder='{"channel":"C123","text":"Hello"}'
          className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 font-mono text-xs text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
        />
      </label>
    </section>
  );
}

function WorkflowTabButton({
  active,
  label,
  count,
  onClick,
}: {
  active: boolean;
  label: string;
  count: number;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      role="tab"
      aria-selected={active}
      aria-label={label}
      onClick={onClick}
      className={`rounded-md border px-3 py-2 text-sm transition-colors duration-150 ${
        active
          ? "border-alpha-strong bg-alpha-5 text-primary"
          : "border-alpha text-muted hover:bg-alpha-5 hover:text-primary"
      }`}
    >
      {label}
      <span className="ml-2 text-xs text-faint">{count}</span>
    </button>
  );
}

function TargetDetails({ target }: { target: WorkflowTarget }) {
  return (
    <section>
      <h3 className="text-xs font-medium uppercase tracking-[0.18em] text-faint">Target</h3>
      <div className="mt-3 rounded-md border border-alpha bg-background/65 p-4 text-sm dark:bg-background/20">
        <p className="font-medium text-primary">
          {target.plugin}.{target.operation}
        </p>
        <p className="mt-2 text-xs text-muted">
          Connection: {target.connection || "-"} · Instance: {target.instance || "-"}
        </p>
        {target.input && Object.keys(target.input).length > 0 ? (
          <pre className="mt-3 overflow-x-auto text-xs text-primary">
            {prettyJSON(target.input)}
          </pre>
        ) : (
          <p className="mt-3 text-xs text-faint">No target input configured.</p>
        )}
      </div>
    </section>
  );
}

function DetailItem({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-alpha bg-background/65 px-4 py-3 dark:bg-background/20">
      <p className="text-[11px] uppercase tracking-[0.18em] text-faint">{label}</p>
      <p className="mt-2 text-sm text-primary">{value || "-"}</p>
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
      <p className={`mt-3 text-2xl font-heading font-bold ${toneClassName}`}>{value}</p>
    </div>
  );
}

function filterRuns(runs: WorkflowRun[], query: string, status: string): WorkflowRun[] {
  const trimmedQuery = query.trim().toLowerCase();
  return runs.filter((run) => {
    const matchesStatus = status === "all" || (run.status || "") === status;
    if (!matchesStatus) return false;
    if (!trimmedQuery) return true;

    return [
      run.id,
      run.provider,
      run.target.plugin,
      run.target.operation,
      run.trigger?.scheduleId,
      run.trigger?.triggerId,
    ]
      .filter(Boolean)
      .some((value) => value!.toLowerCase().includes(trimmedQuery));
  });
}

function filterSchedules(
  schedules: WorkflowSchedule[],
  query: string,
  status: string,
): WorkflowSchedule[] {
  const trimmedQuery = query.trim().toLowerCase();
  return schedules.filter((schedule) => {
    const matchesStatus =
      status === "all" ||
      (status === "paused" && schedule.paused) ||
      (status === "active" && !schedule.paused);
    if (!matchesStatus) return false;
    if (!trimmedQuery) return true;

    return [
      schedule.id,
      schedule.provider,
      schedule.cron,
      schedule.timezone,
      schedule.target.plugin,
      schedule.target.operation,
      schedule.target.connection,
      schedule.target.instance,
      scheduleCadenceLabel(schedule.cron),
    ]
      .filter(Boolean)
      .some((value) => value!.toLowerCase().includes(trimmedQuery));
  });
}

function filterTriggers(
  triggers: WorkflowEventTrigger[],
  query: string,
  status: string,
): WorkflowEventTrigger[] {
  const trimmedQuery = query.trim().toLowerCase();
  return triggers.filter((trigger) => {
    const matchesStatus =
      status === "all" ||
      (status === "paused" && trigger.paused) ||
      (status === "active" && !trigger.paused);
    if (!matchesStatus) return false;
    if (!trimmedQuery) return true;

    return [
      trigger.id,
      trigger.provider,
      trigger.match.type,
      trigger.match.source,
      trigger.match.subject,
      trigger.target.plugin,
      trigger.target.operation,
      trigger.target.connection,
      trigger.target.instance,
    ]
      .filter(Boolean)
      .some((value) => value!.toLowerCase().includes(trimmedQuery));
  });
}

function runTriggerLabel(run: WorkflowRun): string {
  if (run.trigger?.kind === "schedule") {
    return run.trigger.scheduleId ? `schedule:${run.trigger.scheduleId}` : "schedule";
  }
  if (run.trigger?.kind === "event") {
    return run.trigger.triggerId ? `trigger:${run.trigger.triggerId}` : "trigger";
  }
  if (run.trigger?.kind === "manual") {
    return "manual";
  }
  return "unknown";
}

function formatDate(value?: string): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function prettyJSON(value: Record<string, unknown>): string {
  return JSON.stringify(value, null, 2);
}

function prettyResultBody(value: string): string {
  try {
    return JSON.stringify(JSON.parse(value), null, 2);
  } catch {
    return value;
  }
}

function runStatusClassName(status?: string): string {
  switch (status) {
    case "succeeded":
      return "rounded-full bg-grove-100 px-2 py-1 text-[11px] font-medium text-grove-700 dark:bg-grove-700/20 dark:text-grove-200";
    case "running":
      return "rounded-full bg-sky-100 px-2 py-1 text-[11px] font-medium text-sky-700 dark:bg-sky-700/20 dark:text-sky-200";
    case "failed":
      return "rounded-full bg-ember-100 px-2 py-1 text-[11px] font-medium text-ember-700 dark:bg-ember-700/20 dark:text-ember-200";
    case "canceled":
      return "rounded-full bg-slate-200 px-2 py-1 text-[11px] font-medium text-slate-700 dark:bg-slate-700/20 dark:text-slate-200";
    case "pending":
      return "rounded-full bg-amber-100 px-2 py-1 text-[11px] font-medium text-amber-700 dark:bg-amber-700/20 dark:text-amber-200";
    default:
      return "rounded-full bg-alpha-5 px-2 py-1 text-[11px] font-medium text-faint";
  }
}

function pausedStateClassName(paused?: boolean): string {
  if (paused) {
    return "rounded-full bg-amber-100 px-2 py-1 text-[11px] font-medium text-amber-700 dark:bg-amber-700/20 dark:text-amber-200";
  }
  return "rounded-full bg-grove-100 px-2 py-1 text-[11px] font-medium text-grove-700 dark:bg-grove-700/20 dark:text-grove-200";
}

function defaultScheduleForm(browserTimezone: string, plugin = ""): ScheduleFormState {
  const timezoneMode: TimezoneMode = browserTimezone === "UTC" ? "utc" : "local";
  return {
    plugin,
    operation: "",
    connection: "",
    instance: "",
    inputJSON: "",
    cadence: "hourly",
    hour: "9",
    weekday: "1",
    monthDay: "1",
    timezoneMode,
    paused: false,
  };
}

function defaultTriggerForm(plugin = ""): TriggerFormState {
  return {
    plugin,
    operation: "",
    connection: "",
    instance: "",
    inputJSON: "",
    type: "",
    source: "",
    subject: "",
    paused: false,
  };
}

function scheduleFormFromSchedule(
  schedule: WorkflowSchedule,
  browserTimezone: string,
): { form: ScheduleFormState; warning: string | null } {
  const preset = presetFromCron(schedule.cron);
  return {
    form: {
      plugin: schedule.target.plugin,
      operation: schedule.target.operation,
      connection: schedule.target.connection || "",
      instance: schedule.target.instance || "",
      inputJSON: schedule.target.input ? prettyJSON(schedule.target.input) : "",
      cadence: preset.cadence,
      hour: preset.hour,
      weekday: preset.weekday,
      monthDay: preset.monthDay,
      timezoneMode: normalizeTimezoneMode(schedule.timezone, browserTimezone),
      paused: schedule.paused,
    },
    warning: preset.supported
      ? null
      : "This schedule uses a custom cron expression. Editing it here will replace it with one of the preset cadence options.",
  };
}

function triggerFormFromTrigger(trigger: WorkflowEventTrigger): TriggerFormState {
  return {
    plugin: trigger.target.plugin,
    operation: trigger.target.operation,
    connection: trigger.target.connection || "",
    instance: trigger.target.instance || "",
    inputJSON: trigger.target.input ? prettyJSON(trigger.target.input) : "",
    type: trigger.match.type,
    source: trigger.match.source || "",
    subject: trigger.match.subject || "",
    paused: trigger.paused,
  };
}

function scheduleFormToUpsert(
  form: ScheduleFormState,
  browserTimezone: string,
  provider?: string,
): WorkflowScheduleUpsert {
  return {
    provider: provider || undefined,
    cron: cronFromScheduleForm(form),
    timezone: form.timezoneMode === "utc" ? "UTC" : browserTimezone,
    target: {
      plugin: form.plugin.trim(),
      operation: form.operation.trim(),
      connection: emptyToUndefined(form.connection),
      instance: emptyToUndefined(form.instance),
      input: parseInputJSONObject(form.inputJSON),
    },
    paused: form.paused,
  };
}

function triggerFormToUpsert(
  form: TriggerFormState,
  provider?: string,
): WorkflowEventTriggerUpsert {
  return {
    provider: provider || undefined,
    match: {
      type: form.type.trim(),
      source: emptyToUndefined(form.source),
      subject: emptyToUndefined(form.subject),
    },
    target: {
      plugin: form.plugin.trim(),
      operation: form.operation.trim(),
      connection: emptyToUndefined(form.connection),
      instance: emptyToUndefined(form.instance),
      input: parseInputJSONObject(form.inputJSON),
    },
    paused: form.paused,
  };
}

function cronFromScheduleForm(form: ScheduleFormState): string {
  switch (form.cadence) {
    case "hourly":
      return "0 * * * *";
    case "daily":
      return `0 ${normalizeNumeric(form.hour, 0, 23, 9)} * * *`;
    case "weekly":
      return `0 ${normalizeNumeric(form.hour, 0, 23, 9)} * * ${normalizeNumeric(form.weekday, 0, 6, 1)}`;
    case "monthly":
      return `0 ${normalizeNumeric(form.hour, 0, 23, 9)} ${normalizeNumeric(form.monthDay, 1, 28, 1)} * *`;
    default:
      return "0 * * * *";
  }
}

function presetFromCron(cron: string): {
  cadence: ScheduleCadence;
  hour: string;
  weekday: string;
  monthDay: string;
  supported: boolean;
} {
  const parts = cron.trim().split(/\s+/);
  if (parts.length !== 5) {
    return { cadence: "daily", hour: "9", weekday: "1", monthDay: "1", supported: false };
  }

  const [minute, hour, monthDay, month, weekday] = parts;
  if (minute !== "0") {
    return { cadence: "daily", hour: "9", weekday: "1", monthDay: "1", supported: false };
  }

  if (hour === "*" && monthDay === "*" && month === "*" && weekday === "*") {
    return { cadence: "hourly", hour: "9", weekday: "1", monthDay: "1", supported: true };
  }

  if (isCronNumber(hour, 0, 23) && monthDay === "*" && month === "*" && weekday === "*") {
    return { cadence: "daily", hour, weekday: "1", monthDay: "1", supported: true };
  }

  if (isCronNumber(hour, 0, 23) && monthDay === "*" && month === "*" && isCronNumber(weekday, 0, 6)) {
    return { cadence: "weekly", hour, weekday, monthDay: "1", supported: true };
  }

  if (isCronNumber(hour, 0, 23) && isCronNumber(monthDay, 1, 28) && month === "*" && weekday === "*") {
    return { cadence: "monthly", hour, weekday: "1", monthDay, supported: true };
  }

  return { cadence: "daily", hour: "9", weekday: "1", monthDay: "1", supported: false };
}

function scheduleCadenceLabel(cron: string): string {
  const preset = presetFromCron(cron);
  if (!preset.supported) return "Custom";
  switch (preset.cadence) {
    case "hourly":
      return "Hourly";
    case "daily":
      return "Daily";
    case "weekly":
      return "Weekly";
    case "monthly":
      return "Monthly";
    default:
      return "Custom";
  }
}

function normalizeTimezoneMode(timezone: string | undefined, browserTimezone: string): TimezoneMode {
  const normalized = (timezone || "").trim();
  if (!normalized) {
    return browserTimezone === "UTC" ? "utc" : "local";
  }
  return normalized.toUpperCase() === "UTC" ? "utc" : "local";
}

function parseInputJSONObject(value: string): Record<string, unknown> | undefined {
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  const parsed = JSON.parse(trimmed) as unknown;
  if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") {
    throw new Error("Input JSON must be an object.");
  }
  return parsed as Record<string, unknown>;
}

function detectBrowserTimezone(): string {
  try {
    const timezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
    return timezone || "UTC";
  } catch {
    return "UTC";
  }
}

function sortOperations(operations: IntegrationOperation[]): IntegrationOperation[] {
  return operations
    .filter((operation) => operation.visible !== false)
    .slice()
    .sort((left, right) =>
      operationLabel(left).localeCompare(operationLabel(right), undefined, {
        sensitivity: "base",
      }),
    );
}

function operationLabel(operation: IntegrationOperation): string {
  return operation.title?.trim() || operation.id;
}

function integrationLabel(integration: Integration): string {
  return integration.displayName?.trim() || integration.name;
}

function formatHourLabel(hour: number): string {
  const normalized = hour % 24;
  const suffix = normalized >= 12 ? "PM" : "AM";
  const twelveHour = normalized % 12 === 0 ? 12 : normalized % 12;
  return `${twelveHour}:00 ${suffix}`;
}

function ordinal(value: number): string {
  const suffix =
    value % 100 >= 11 && value % 100 <= 13
      ? "th"
      : value % 10 === 1
        ? "st"
        : value % 10 === 2
          ? "nd"
          : value % 10 === 3
            ? "rd"
            : "th";
  return `${value}${suffix}`;
}

function normalizeNumeric(
  value: string,
  min: number,
  max: number,
  fallback: number,
): string {
  const numeric = Number.parseInt(value, 10);
  if (Number.isNaN(numeric) || numeric < min || numeric > max) {
    return String(fallback);
  }
  return String(numeric);
}

function isCronNumber(value: string, min: number, max: number): boolean {
  const numeric = Number.parseInt(value, 10);
  return !Number.isNaN(numeric) && numeric >= min && numeric <= max;
}

function emptyToUndefined(value: string): string | undefined {
  const trimmed = value.trim();
  return trimmed || undefined;
}

function errorMessage(reason: unknown, fallback: string): string {
  if (reason instanceof Error) {
    return reason.message;
  }
  if (typeof reason === "string") {
    return reason;
  }
  return fallback;
}
