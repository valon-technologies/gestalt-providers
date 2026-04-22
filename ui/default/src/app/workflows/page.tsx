"use client";

import { useDeferredValue, useEffect, useState } from "react";
import type {
  WorkflowEventTrigger,
  WorkflowRun,
  WorkflowSchedule,
  WorkflowTarget,
} from "@/lib/api";
import {
  cancelWorkflowRun,
  getWorkflowEventTriggers,
  getWorkflowRun,
  getWorkflowRuns,
  getWorkflowSchedules,
} from "@/lib/api";
import AuthGuard from "@/components/AuthGuard";
import Nav from "@/components/Nav";

type WorkflowTab = "runs" | "schedules" | "triggers";

export default function WorkflowsPage() {
  const [activeTab, setActiveTab] = useState<WorkflowTab>("runs");

  const [runs, setRuns] = useState<WorkflowRun[]>([]);
  const [schedules, setSchedules] = useState<WorkflowSchedule[]>([]);
  const [triggers, setTriggers] = useState<WorkflowEventTrigger[]>([]);

  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshNonce, setRefreshNonce] = useState(0);

  const [runsError, setRunsError] = useState<string | null>(null);
  const [schedulesError, setSchedulesError] = useState<string | null>(null);
  const [triggersError, setTriggersError] = useState<string | null>(null);

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

  const deferredRunsQuery = useDeferredValue(runsQuery);
  const deferredSchedulesQuery = useDeferredValue(schedulesQuery);
  const deferredTriggersQuery = useDeferredValue(triggersQuery);

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
          setTriggersError(
            errorMessage(triggersResult.reason, "Failed to load workflow event triggers"),
          );
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

  const filteredRuns = filterRuns(runs, deferredRunsQuery, runStatus);
  const filteredSchedules = filterSchedules(schedules, deferredSchedulesQuery, scheduleStatus);
  const filteredTriggers = filterTriggers(triggers, deferredTriggersQuery, triggerStatus);

  const selectedSchedule = schedules.find((schedule) => schedule.id === selectedScheduleID) ?? null;
  const selectedTrigger = triggers.find((trigger) => trigger.id === selectedTriggerID) ?? null;
  const selectedRunCancelable = selectedRun?.status === "pending";

  const failedRuns = runs.filter((run) => run.status === "failed").length;

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
              <h1 className="mt-2 text-2xl font-heading font-bold text-primary">
                Workflows
              </h1>
              <p className="mt-2 max-w-3xl text-sm text-muted">
                Inspect workflow schedules, event triggers, and recent run activity across
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
                <SummaryCard
                  label="Event triggers"
                  value={String(triggers.length)}
                  tone="sky"
                />
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
                    label="Event Triggers"
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
                        placeholder="Search by schedule, plugin, operation, or cron"
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
                        <option value="all">All event triggers</option>
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
                  onSelectSchedule={setSelectedScheduleID}
                />
              ) : (
                <EventTriggersPanel
                  triggers={filteredTriggers}
                  triggersError={triggersError}
                  selectedTriggerID={selectedTriggerID}
                  selectedTrigger={selectedTrigger}
                  onSelectTrigger={setSelectedTriggerID}
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
                        <span className={runStatusClassName(run.status)}>{run.status || "unknown"}</span>
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
  onSelectSchedule,
}: {
  schedules: WorkflowSchedule[];
  schedulesError: string | null;
  selectedScheduleID: string | null;
  selectedSchedule: WorkflowSchedule | null;
  onSelectSchedule: (id: string) => void;
}) {
  return (
    <div className="grid gap-6 lg:grid-cols-[minmax(0,1.1fr)_minmax(20rem,0.9fr)]">
      <section className="rounded-lg border border-alpha bg-base-100 dark:bg-surface">
        <div className="border-b border-alpha px-5 py-4">
          <h2 className="text-sm font-medium text-primary">Workflow Schedules</h2>
          <p className="mt-1 text-xs text-faint">{schedules.length} shown</p>
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
                        {schedule.cron} · {schedule.provider}
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
        <div className="flex items-start justify-between gap-4">
          <div>
            <h2 className="text-sm font-medium text-primary">Schedule Details</h2>
            <p className="mt-1 text-xs text-faint">
              {selectedSchedule?.id || "Select a schedule"}
            </p>
          </div>
          {selectedSchedule ? (
            <span className={pausedStateClassName(selectedSchedule.paused)}>
              {selectedSchedule.paused ? "paused" : "active"}
            </span>
          ) : null}
        </div>

        {selectedSchedule ? (
          <div className="mt-6 space-y-6">
            <div className="grid gap-4 sm:grid-cols-2">
              <DetailItem label="Provider" value={selectedSchedule.provider} />
              <DetailItem label="Cron" value={selectedSchedule.cron} />
              <DetailItem
                label="Timezone"
                value={selectedSchedule.timezone || "Default timezone"}
              />
              <DetailItem label="Next run" value={formatDate(selectedSchedule.nextRunAt)} />
              <DetailItem label="Created" value={formatDate(selectedSchedule.createdAt)} />
              <DetailItem label="Updated" value={formatDate(selectedSchedule.updatedAt)} />
            </div>

            <TargetDetails target={selectedSchedule.target} />
          </div>
        ) : (
          <p className="mt-6 text-sm text-faint">Select a workflow schedule to inspect it.</p>
        )}
      </section>
    </div>
  );
}

function EventTriggersPanel({
  triggers,
  triggersError,
  selectedTriggerID,
  selectedTrigger,
  onSelectTrigger,
}: {
  triggers: WorkflowEventTrigger[];
  triggersError: string | null;
  selectedTriggerID: string | null;
  selectedTrigger: WorkflowEventTrigger | null;
  onSelectTrigger: (id: string) => void;
}) {
  return (
    <div className="grid gap-6 lg:grid-cols-[minmax(0,1.1fr)_minmax(20rem,0.9fr)]">
      <section className="rounded-lg border border-alpha bg-base-100 dark:bg-surface">
        <div className="border-b border-alpha px-5 py-4">
          <h2 className="text-sm font-medium text-primary">Workflow Event Triggers</h2>
          <p className="mt-1 text-xs text-faint">{triggers.length} shown</p>
        </div>

        {triggersError ? (
          <p className="px-5 py-8 text-sm text-ember-500">{triggersError}</p>
        ) : (
          <div className="divide-y divide-alpha">
            {triggers.length === 0 ? (
              <div className="px-5 py-8 text-sm text-faint">
                No workflow event triggers yet.
              </div>
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
        <div className="flex items-start justify-between gap-4">
          <div>
            <h2 className="text-sm font-medium text-primary">Event Trigger Details</h2>
            <p className="mt-1 text-xs text-faint">
              {selectedTrigger?.id || "Select an event trigger"}
            </p>
          </div>
          {selectedTrigger ? (
            <span className={pausedStateClassName(selectedTrigger.paused)}>
              {selectedTrigger.paused ? "paused" : "active"}
            </span>
          ) : null}
        </div>

        {selectedTrigger ? (
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
          <p className="mt-6 text-sm text-faint">
            Select a workflow event trigger to inspect it.
          </p>
        )}
      </section>
    </div>
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
    return run.trigger.triggerId ? `event:${run.trigger.triggerId}` : "event";
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

function errorMessage(reason: unknown, fallback: string): string {
  if (reason instanceof Error) {
    return reason.message;
  }
  if (typeof reason === "string") {
    return reason;
  }
  return fallback;
}
