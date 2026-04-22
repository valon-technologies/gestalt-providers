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
  createWorkflowEventTrigger,
  createWorkflowSchedule,
  deleteWorkflowEventTrigger,
  deleteWorkflowSchedule,
  getWorkflowEventTrigger,
  getWorkflowEventTriggers,
  getWorkflowRun,
  getWorkflowRuns,
  getWorkflowSchedule,
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
type ToggleFilter = "all" | "active" | "paused";

type TargetDraft = {
  plugin: string;
  operation: string;
  connection: string;
  instance: string;
  inputText: string;
};

type ScheduleDraft = {
  provider: string;
  cron: string;
  timezone: string;
  paused: boolean;
  target: TargetDraft;
};

type EventTriggerDraft = {
  provider: string;
  eventType: string;
  source: string;
  subject: string;
  paused: boolean;
  target: TargetDraft;
};

const RUN_TAB = "runs";
const SCHEDULE_TAB = "schedules";
const TRIGGER_TAB = "triggers";

export default function WorkflowsPage() {
  const [activeTab, setActiveTab] = useState<WorkflowTab>(RUN_TAB);

  const [runs, setRuns] = useState<WorkflowRun[]>([]);
  const [runsLoading, setRunsLoading] = useState(true);
  const [runsRefreshing, setRunsRefreshing] = useState(false);
  const [runsError, setRunsError] = useState<string | null>(null);
  const [selectedRunID, setSelectedRunID] = useState<string | null>(null);
  const [selectedRun, setSelectedRun] = useState<WorkflowRun | null>(null);
  const [runDetailLoading, setRunDetailLoading] = useState(false);
  const [runDetailError, setRunDetailError] = useState<string | null>(null);
  const [runActionError, setRunActionError] = useState<string | null>(null);
  const [cancelingRun, setCancelingRun] = useState(false);
  const [runQuery, setRunQuery] = useState("");
  const [runStatusFilter, setRunStatusFilter] = useState("all");
  const [runRefreshNonce, setRunRefreshNonce] = useState(0);
  const deferredRunQuery = useDeferredValue(runQuery);

  const [schedules, setSchedules] = useState<WorkflowSchedule[]>([]);
  const [schedulesLoading, setSchedulesLoading] = useState(true);
  const [schedulesRefreshing, setSchedulesRefreshing] = useState(false);
  const [schedulesError, setSchedulesError] = useState<string | null>(null);
  const [selectedScheduleID, setSelectedScheduleID] = useState<string | null>(null);
  const [selectedSchedule, setSelectedSchedule] = useState<WorkflowSchedule | null>(null);
  const [scheduleDetailLoading, setScheduleDetailLoading] = useState(false);
  const [scheduleDetailError, setScheduleDetailError] = useState<string | null>(null);
  const [scheduleActionError, setScheduleActionError] = useState<string | null>(null);
  const [scheduleFormError, setScheduleFormError] = useState<string | null>(null);
  const [scheduleSaving, setScheduleSaving] = useState(false);
  const [scheduleMutating, setScheduleMutating] = useState(false);
  const [creatingSchedule, setCreatingSchedule] = useState(false);
  const [scheduleQuery, setScheduleQuery] = useState("");
  const [scheduleStateFilter, setScheduleStateFilter] = useState<ToggleFilter>("all");
  const [scheduleRefreshNonce, setScheduleRefreshNonce] = useState(0);
  const [scheduleDraft, setScheduleDraft] = useState<ScheduleDraft>(emptyScheduleDraft());
  const deferredScheduleQuery = useDeferredValue(scheduleQuery);

  const [triggers, setTriggers] = useState<WorkflowEventTrigger[]>([]);
  const [triggersLoading, setTriggersLoading] = useState(true);
  const [triggersRefreshing, setTriggersRefreshing] = useState(false);
  const [triggersError, setTriggersError] = useState<string | null>(null);
  const [selectedTriggerID, setSelectedTriggerID] = useState<string | null>(null);
  const [selectedTrigger, setSelectedTrigger] = useState<WorkflowEventTrigger | null>(null);
  const [triggerDetailLoading, setTriggerDetailLoading] = useState(false);
  const [triggerDetailError, setTriggerDetailError] = useState<string | null>(null);
  const [triggerActionError, setTriggerActionError] = useState<string | null>(null);
  const [triggerFormError, setTriggerFormError] = useState<string | null>(null);
  const [triggerSaving, setTriggerSaving] = useState(false);
  const [triggerMutating, setTriggerMutating] = useState(false);
  const [creatingTrigger, setCreatingTrigger] = useState(false);
  const [triggerQuery, setTriggerQuery] = useState("");
  const [triggerStateFilter, setTriggerStateFilter] = useState<ToggleFilter>("all");
  const [triggerRefreshNonce, setTriggerRefreshNonce] = useState(0);
  const [triggerDraft, setTriggerDraft] = useState<EventTriggerDraft>(emptyEventTriggerDraft());
  const deferredTriggerQuery = useDeferredValue(triggerQuery);

  useEffect(() => {
    let active = true;
    const initialLoad = runRefreshNonce === 0;
    if (initialLoad) {
      setRunsLoading(true);
    } else {
      setRunsRefreshing(true);
    }
    getWorkflowRuns()
      .then((nextRuns) => {
        if (!active) return;
        setRuns(nextRuns);
        setRunsError(null);
        setSelectedRunID((current) =>
          current && nextRuns.some((run) => run.id === current)
            ? current
            : nextRuns[0]?.id ?? null,
        );
      })
      .catch((err) => {
        if (!active) return;
        setRunsError(errorMessage(err, "Failed to load workflow runs"));
      })
      .finally(() => {
        if (!active) return;
        if (initialLoad) {
          setRunsLoading(false);
        } else {
          setRunsRefreshing(false);
        }
      });

    return () => {
      active = false;
    };
  }, [runRefreshNonce]);

  useEffect(() => {
    if (!selectedRunID) {
      setSelectedRun(null);
      setRunDetailError(null);
      setRunActionError(null);
      return;
    }

    const cached = runs.find((run) => run.id === selectedRunID) ?? null;
    if (cached) {
      setSelectedRun(cached);
    }
    setRunActionError(null);

    let active = true;
    setRunDetailLoading(true);
    setRunDetailError(null);
    getWorkflowRun(selectedRunID)
      .then((run) => {
        if (!active) return;
        setSelectedRun(run);
      })
      .catch((err) => {
        if (!active) return;
        setRunDetailError(errorMessage(err, "Failed to load workflow run"));
      })
      .finally(() => {
        if (active) {
          setRunDetailLoading(false);
        }
      });

    return () => {
      active = false;
    };
  }, [runs, selectedRunID]);

  useEffect(() => {
    let active = true;
    const initialLoad = scheduleRefreshNonce === 0;
    if (initialLoad) {
      setSchedulesLoading(true);
    } else {
      setSchedulesRefreshing(true);
    }
    getWorkflowSchedules()
      .then((nextSchedules) => {
        if (!active) return;
        setSchedules(nextSchedules);
        setSchedulesError(null);
        if (!creatingSchedule) {
          setSelectedScheduleID((current) =>
            current && nextSchedules.some((schedule) => schedule.id === current)
              ? current
              : nextSchedules[0]?.id ?? null,
          );
        }
      })
      .catch((err) => {
        if (!active) return;
        setSchedulesError(errorMessage(err, "Failed to load workflow schedules"));
      })
      .finally(() => {
        if (!active) return;
        if (initialLoad) {
          setSchedulesLoading(false);
        } else {
          setSchedulesRefreshing(false);
        }
      });

    return () => {
      active = false;
    };
  }, [creatingSchedule, scheduleRefreshNonce]);

  useEffect(() => {
    if (creatingSchedule || !selectedScheduleID) {
      setSelectedSchedule(null);
      setScheduleDetailError(null);
      return;
    }

    const cached = schedules.find((schedule) => schedule.id === selectedScheduleID) ?? null;
    if (cached) {
      setSelectedSchedule(cached);
    }

    let active = true;
    setScheduleDetailLoading(true);
    setScheduleDetailError(null);
    getWorkflowSchedule(selectedScheduleID)
      .then((schedule) => {
        if (!active) return;
        setSelectedSchedule(schedule);
      })
      .catch((err) => {
        if (!active) return;
        setScheduleDetailError(errorMessage(err, "Failed to load workflow schedule"));
      })
      .finally(() => {
        if (active) {
          setScheduleDetailLoading(false);
        }
      });

    return () => {
      active = false;
    };
  }, [creatingSchedule, schedules, selectedScheduleID]);

  useEffect(() => {
    if (creatingSchedule) {
      setScheduleDraft(emptyScheduleDraft());
      setScheduleFormError(null);
      setScheduleActionError(null);
      return;
    }
    if (!selectedSchedule) return;
    setScheduleDraft(scheduleToDraft(selectedSchedule));
    setScheduleFormError(null);
    setScheduleActionError(null);
  }, [creatingSchedule, selectedSchedule]);

  useEffect(() => {
    let active = true;
    const initialLoad = triggerRefreshNonce === 0;
    if (initialLoad) {
      setTriggersLoading(true);
    } else {
      setTriggersRefreshing(true);
    }
    getWorkflowEventTriggers()
      .then((nextTriggers) => {
        if (!active) return;
        setTriggers(nextTriggers);
        setTriggersError(null);
        if (!creatingTrigger) {
          setSelectedTriggerID((current) =>
            current && nextTriggers.some((trigger) => trigger.id === current)
              ? current
              : nextTriggers[0]?.id ?? null,
          );
        }
      })
      .catch((err) => {
        if (!active) return;
        setTriggersError(errorMessage(err, "Failed to load workflow event triggers"));
      })
      .finally(() => {
        if (!active) return;
        if (initialLoad) {
          setTriggersLoading(false);
        } else {
          setTriggersRefreshing(false);
        }
      });

    return () => {
      active = false;
    };
  }, [creatingTrigger, triggerRefreshNonce]);

  useEffect(() => {
    if (creatingTrigger || !selectedTriggerID) {
      setSelectedTrigger(null);
      setTriggerDetailError(null);
      return;
    }

    const cached = triggers.find((trigger) => trigger.id === selectedTriggerID) ?? null;
    if (cached) {
      setSelectedTrigger(cached);
    }

    let active = true;
    setTriggerDetailLoading(true);
    setTriggerDetailError(null);
    getWorkflowEventTrigger(selectedTriggerID)
      .then((trigger) => {
        if (!active) return;
        setSelectedTrigger(trigger);
      })
      .catch((err) => {
        if (!active) return;
        setTriggerDetailError(errorMessage(err, "Failed to load workflow event trigger"));
      })
      .finally(() => {
        if (active) {
          setTriggerDetailLoading(false);
        }
      });

    return () => {
      active = false;
    };
  }, [creatingTrigger, selectedTriggerID, triggers]);

  useEffect(() => {
    if (creatingTrigger) {
      setTriggerDraft(emptyEventTriggerDraft());
      setTriggerFormError(null);
      setTriggerActionError(null);
      return;
    }
    if (!selectedTrigger) return;
    setTriggerDraft(triggerToDraft(selectedTrigger));
    setTriggerFormError(null);
    setTriggerActionError(null);
  }, [creatingTrigger, selectedTrigger]);

  const runSearch = deferredRunQuery.trim().toLowerCase();
  const filteredRuns = runs.filter((run) => {
    const matchesStatus = runStatusFilter === "all" || (run.status || "") === runStatusFilter;
    if (!matchesStatus) return false;
    if (!runSearch) return true;
    return [
      run.id,
      run.provider,
      run.target.plugin,
      run.target.operation,
      run.trigger?.scheduleId,
      run.trigger?.triggerId,
      run.status,
    ]
      .filter(Boolean)
      .some((value) => value!.toLowerCase().includes(runSearch));
  });

  const scheduleSearch = deferredScheduleQuery.trim().toLowerCase();
  const filteredSchedules = schedules.filter((schedule) => {
    const matchesState =
      scheduleStateFilter === "all" ||
      (scheduleStateFilter === "paused" ? schedule.paused : !schedule.paused);
    if (!matchesState) return false;
    if (!scheduleSearch) return true;
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
      .some((value) => value!.toLowerCase().includes(scheduleSearch));
  });

  const triggerSearch = deferredTriggerQuery.trim().toLowerCase();
  const filteredTriggers = triggers.filter((trigger) => {
    const matchesState =
      triggerStateFilter === "all" ||
      (triggerStateFilter === "paused" ? trigger.paused : !trigger.paused);
    if (!matchesState) return false;
    if (!triggerSearch) return true;
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
      .some((value) => value!.toLowerCase().includes(triggerSearch));
  });

  const inFlightRuns = runs.filter(
    (run) => run.status === "pending" || run.status === "running",
  ).length;
  const failedRuns = runs.filter((run) => run.status === "failed").length;

  const selectedRunCancelable = selectedRun?.status === "pending";

  async function handleCancelSelectedRun() {
    if (!selectedRunID || !selectedRunCancelable) return;
    setCancelingRun(true);
    setRunActionError(null);
    try {
      const updated = await cancelWorkflowRun(selectedRunID, "Run canceled.");
      setSelectedRun(updated);
      setRuns((current) => current.map((run) => (run.id === updated.id ? updated : run)));
      setRunRefreshNonce((value) => value + 1);
    } catch (err) {
      setRunActionError(errorMessage(err, "Failed to cancel workflow run"));
    } finally {
      setCancelingRun(false);
    }
  }

  async function handleSaveSchedule() {
    setScheduleFormError(null);
    setScheduleActionError(null);

    let body: ReturnType<typeof scheduleDraftToRequest>;
    try {
      body = scheduleDraftToRequest(scheduleDraft);
    } catch (err) {
      setScheduleFormError(errorMessage(err, "Invalid workflow schedule"));
      return;
    }

    setScheduleSaving(true);
    try {
      const saved = creatingSchedule
        ? await createWorkflowSchedule(body)
        : await updateWorkflowSchedule(selectedScheduleID ?? "", body);
      setCreatingSchedule(false);
      setSelectedSchedule(saved);
      setSelectedScheduleID(saved.id);
      setScheduleRefreshNonce((value) => value + 1);
      setActiveTab(SCHEDULE_TAB);
    } catch (err) {
      setScheduleActionError(errorMessage(err, "Failed to save workflow schedule"));
    } finally {
      setScheduleSaving(false);
    }
  }

  async function handleDeleteSchedule() {
    if (!selectedScheduleID) return;
    setScheduleMutating(true);
    setScheduleActionError(null);
    try {
      await deleteWorkflowSchedule(selectedScheduleID);
      setSelectedSchedule(null);
      setSelectedScheduleID(null);
      setScheduleRefreshNonce((value) => value + 1);
    } catch (err) {
      setScheduleActionError(errorMessage(err, "Failed to delete workflow schedule"));
    } finally {
      setScheduleMutating(false);
    }
  }

  async function handleToggleSchedulePause() {
    if (!selectedScheduleID || !selectedSchedule) return;
    setScheduleMutating(true);
    setScheduleActionError(null);
    try {
      const updated = selectedSchedule.paused
        ? await resumeWorkflowSchedule(selectedScheduleID)
        : await pauseWorkflowSchedule(selectedScheduleID);
      setSelectedSchedule(updated);
      setSchedules((current) =>
        current.map((schedule) => (schedule.id === updated.id ? updated : schedule)),
      );
      setScheduleRefreshNonce((value) => value + 1);
    } catch (err) {
      setScheduleActionError(errorMessage(err, "Failed to update workflow schedule"));
    } finally {
      setScheduleMutating(false);
    }
  }

  async function handleSaveTrigger() {
    setTriggerFormError(null);
    setTriggerActionError(null);

    let body: ReturnType<typeof triggerDraftToRequest>;
    try {
      body = triggerDraftToRequest(triggerDraft);
    } catch (err) {
      setTriggerFormError(errorMessage(err, "Invalid workflow event trigger"));
      return;
    }

    setTriggerSaving(true);
    try {
      const saved = creatingTrigger
        ? await createWorkflowEventTrigger(body)
        : await updateWorkflowEventTrigger(selectedTriggerID ?? "", body);
      setCreatingTrigger(false);
      setSelectedTrigger(saved);
      setSelectedTriggerID(saved.id);
      setTriggerRefreshNonce((value) => value + 1);
      setActiveTab(TRIGGER_TAB);
    } catch (err) {
      setTriggerActionError(errorMessage(err, "Failed to save workflow event trigger"));
    } finally {
      setTriggerSaving(false);
    }
  }

  async function handleDeleteTrigger() {
    if (!selectedTriggerID) return;
    setTriggerMutating(true);
    setTriggerActionError(null);
    try {
      await deleteWorkflowEventTrigger(selectedTriggerID);
      setSelectedTrigger(null);
      setSelectedTriggerID(null);
      setTriggerRefreshNonce((value) => value + 1);
    } catch (err) {
      setTriggerActionError(errorMessage(err, "Failed to delete workflow event trigger"));
    } finally {
      setTriggerMutating(false);
    }
  }

  async function handleToggleTriggerPause() {
    if (!selectedTriggerID || !selectedTrigger) return;
    setTriggerMutating(true);
    setTriggerActionError(null);
    try {
      const updated = selectedTrigger.paused
        ? await resumeWorkflowEventTrigger(selectedTriggerID)
        : await pauseWorkflowEventTrigger(selectedTriggerID);
      setSelectedTrigger(updated);
      setTriggers((current) =>
        current.map((trigger) => (trigger.id === updated.id ? updated : trigger)),
      );
      setTriggerRefreshNonce((value) => value + 1);
    } catch (err) {
      setTriggerActionError(errorMessage(err, "Failed to update workflow event trigger"));
    } finally {
      setTriggerMutating(false);
    }
  }

  function openSchedule(scheduleID?: string) {
    if (!scheduleID) return;
    setCreatingSchedule(false);
    setSelectedScheduleID(scheduleID);
    setActiveTab(SCHEDULE_TAB);
  }

  function openTrigger(triggerID?: string) {
    if (!triggerID) return;
    setCreatingTrigger(false);
    setSelectedTriggerID(triggerID);
    setActiveTab(TRIGGER_TAB);
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
              <p className="mt-2 text-sm text-muted">
                Manage schedules, event triggers, and recent runs across plugins and providers.
              </p>
            </div>
            <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
              <SummaryCard label="Schedules" value={String(schedules.length)} tone="default" />
              <SummaryCard
                label="Event triggers"
                value={String(triggers.length)}
                tone="sky"
              />
              <SummaryCard label="In flight" value={String(inFlightRuns)} tone="grove" />
              <SummaryCard label="Failed" value={String(failedRuns)} tone="ember" />
            </div>
          </div>

          <div className="mt-10 animate-fade-in-up [animation-delay:60ms]">
            <div className="flex flex-wrap gap-3 border-b border-alpha pb-4">
              <WorkflowTabButton
                label="Runs"
                count={runs.length}
                active={activeTab === RUN_TAB}
                onClick={() => setActiveTab(RUN_TAB)}
              />
              <WorkflowTabButton
                label="Schedules"
                count={schedules.length}
                active={activeTab === SCHEDULE_TAB}
                onClick={() => setActiveTab(SCHEDULE_TAB)}
              />
              <WorkflowTabButton
                label="Event Triggers"
                count={triggers.length}
                active={activeTab === TRIGGER_TAB}
                onClick={() => setActiveTab(TRIGGER_TAB)}
              />
            </div>

            {activeTab === RUN_TAB && (
              <div className="mt-8">
                {runsError && <p className="mb-4 text-sm text-ember-500">{runsError}</p>}

                {runsLoading ? (
                  <p className="text-sm text-faint">Loading...</p>
                ) : runs.length === 0 ? (
                  runsError ? null : (
                    <div className="rounded-lg border border-alpha bg-base-100 p-8 text-sm text-faint dark:bg-surface">
                      No workflow runs yet.
                    </div>
                  )
                ) : (
                  <div className="space-y-6">
                    <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                      <div>
                        <h2 className="text-sm font-medium text-primary">Recent Runs</h2>
                        <p className="mt-1 text-xs text-faint">
                          Inspect activity, errors, and provider responses.
                        </p>
                      </div>
                      <div className="flex flex-col gap-3 sm:flex-row">
                        <button
                          type="button"
                          onClick={() => setRunRefreshNonce((value) => value + 1)}
                          className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
                        >
                          {runsRefreshing ? "Refreshing..." : "Refresh"}
                        </button>
                        <input
                          value={runQuery}
                          onChange={(event) => setRunQuery(event.target.value)}
                          placeholder="Search by run, plugin, provider, or trigger"
                          className="min-w-72 rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
                        />
                        <select
                          value={runStatusFilter}
                          onChange={(event) => setRunStatusFilter(event.target.value)}
                          className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
                        >
                          <option value="all">All statuses</option>
                          <option value="pending">Pending</option>
                          <option value="running">Running</option>
                          <option value="succeeded">Succeeded</option>
                          <option value="failed">Failed</option>
                          <option value="canceled">Canceled</option>
                        </select>
                      </div>
                    </div>

                    <div className="grid gap-6 lg:grid-cols-[minmax(0,1.15fr)_minmax(20rem,0.85fr)]">
                      <section className="rounded-lg border border-alpha bg-base-100 dark:bg-surface">
                        <div className="border-b border-alpha px-5 py-4">
                          <h3 className="text-sm font-medium text-primary">Run List</h3>
                          <p className="mt-1 text-xs text-faint">
                            {filteredRuns.length} shown
                          </p>
                        </div>
                        <div className="divide-y divide-alpha">
                          {filteredRuns.length === 0 ? (
                            <div className="px-5 py-8 text-sm text-faint">
                              No runs match the current filters.
                            </div>
                          ) : (
                            filteredRuns.map((run) => {
                              const isActive = run.id === selectedRunID;
                              return (
                                <button
                                  key={run.id}
                                  type="button"
                                  onClick={() => setSelectedRunID(run.id)}
                                  className={`flex w-full items-start justify-between gap-4 px-5 py-4 text-left transition-colors duration-150 ${
                                    isActive ? "bg-alpha-5" : "hover:bg-alpha-5"
                                  }`}
                                >
                                  <div className="min-w-0">
                                    <div className="flex items-center gap-2">
                                      <span className="truncate text-sm font-medium text-primary">
                                        {run.target.plugin}.{run.target.operation}
                                      </span>
                                      <span className={statusClassName(run.status)}>
                                        {run.status || "unknown"}
                                      </span>
                                    </div>
                                    <p className="mt-1 truncate text-xs text-faint">{run.id}</p>
                                    <p className="mt-2 text-xs text-muted">
                                      {triggerLabel(run)} · {run.provider}
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
                      </section>

                      <section className="rounded-lg border border-alpha bg-base-100 p-5 dark:bg-surface">
                        <div className="flex items-start justify-between gap-4">
                          <div>
                            <h3 className="text-sm font-medium text-primary">Run Details</h3>
                            <p className="mt-1 text-xs text-faint">
                              {selectedRun?.id || "Select a run"}
                            </p>
                          </div>
                          {selectedRun?.status ? (
                            <span className={statusClassName(selectedRun.status)}>
                              {selectedRun.status}
                            </span>
                          ) : null}
                        </div>

                        {selectedRunCancelable ? (
                          <div className="mt-4 flex items-center justify-between gap-3 rounded-md border border-alpha bg-background/65 px-4 py-3 dark:bg-background/20">
                            <p className="text-sm text-muted">
                              Canceling a run asks the workflow provider to stop it as soon as possible.
                            </p>
                            <button
                              type="button"
                              onClick={() => void handleCancelSelectedRun()}
                              disabled={cancelingRun}
                              className="shrink-0 rounded-md bg-ember-500 px-3 py-2 text-sm font-medium text-white transition-opacity duration-150 hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
                            >
                              {cancelingRun ? "Canceling..." : "Cancel run"}
                            </button>
                          </div>
                        ) : null}

                        {runDetailError && (
                          <p className="mt-4 text-sm text-ember-500">{runDetailError}</p>
                        )}
                        {runActionError && (
                          <p className="mt-4 text-sm text-ember-500">{runActionError}</p>
                        )}

                        {runDetailLoading && !selectedRun ? (
                          <p className="mt-6 text-sm text-faint">Loading details...</p>
                        ) : selectedRun ? (
                          <div className="mt-6 space-y-6">
                            <div className="grid gap-4 sm:grid-cols-2">
                              <DetailItem label="Provider" value={selectedRun.provider} />
                              <DetailItem label="Trigger" value={triggerLabel(selectedRun)} />
                              <DetailItem label="Created" value={formatDate(selectedRun.createdAt)} />
                              <DetailItem label="Started" value={formatDate(selectedRun.startedAt)} />
                              <DetailItem label="Completed" value={formatDate(selectedRun.completedAt)} />
                              <DetailItem
                                label="Actor"
                                value={
                                  selectedRun.createdBy?.displayName ||
                                  selectedRun.createdBy?.subjectId ||
                                  "-"
                                }
                              />
                            </div>

                            {(selectedRun.trigger?.scheduleId || selectedRun.trigger?.triggerId) && (
                              <div className="flex flex-wrap gap-3">
                                {selectedRun.trigger?.scheduleId ? (
                                  <button
                                    type="button"
                                    onClick={() => openSchedule(selectedRun.trigger?.scheduleId)}
                                    className="rounded-md border border-alpha bg-background/65 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-background/20"
                                  >
                                    Open schedule
                                  </button>
                                ) : null}
                                {selectedRun.trigger?.triggerId ? (
                                  <button
                                    type="button"
                                    onClick={() => openTrigger(selectedRun.trigger?.triggerId)}
                                    className="rounded-md border border-alpha bg-background/65 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-background/20"
                                  >
                                    Open event trigger
                                  </button>
                                ) : null}
                              </div>
                            )}

                            <section>
                              <h4 className="text-xs font-medium uppercase tracking-[0.18em] text-faint">
                                Target
                              </h4>
                              <div className="mt-3 rounded-md border border-alpha bg-background/65 p-4 text-sm dark:bg-background/20">
                                <p className="font-medium text-primary">
                                  {selectedRun.target.plugin}.{selectedRun.target.operation}
                                </p>
                                <p className="mt-2 text-xs text-muted">
                                  Connection: {selectedRun.target.connection || "-"} · Instance:{" "}
                                  {selectedRun.target.instance || "-"}
                                </p>
                                {selectedRun.target.input ? (
                                  <pre className="mt-3 overflow-x-auto text-xs text-primary">
                                    {prettyJSON(selectedRun.target.input)}
                                  </pre>
                                ) : null}
                              </div>
                            </section>

                            <section>
                              <h4 className="text-xs font-medium uppercase tracking-[0.18em] text-faint">
                                Result
                              </h4>
                              <div className="mt-3 rounded-md border border-alpha bg-background/65 p-4 text-sm dark:bg-background/20">
                                <p className="text-sm text-primary">
                                  {selectedRun.statusMessage || "No status message"}
                                </p>
                                {selectedRun.resultBody ? (
                                  <pre className="mt-3 overflow-x-auto text-xs text-primary">
                                    {prettyResultBody(selectedRun.resultBody)}
                                  </pre>
                                ) : (
                                  <p className="mt-3 text-xs text-faint">
                                    No result body captured.
                                  </p>
                                )}
                              </div>
                            </section>
                          </div>
                        ) : (
                          <p className="mt-6 text-sm text-faint">
                            Select a workflow run to inspect it.
                          </p>
                        )}
                      </section>
                    </div>
                  </div>
                )}
              </div>
            )}

            {activeTab === SCHEDULE_TAB && (
              <div className="mt-8">
                {schedulesError && <p className="mb-4 text-sm text-ember-500">{schedulesError}</p>}

                {schedulesLoading ? (
                  <p className="text-sm text-faint">Loading...</p>
                ) : (
                  <div className="space-y-6">
                    <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                      <div>
                        <h2 className="text-sm font-medium text-primary">Schedules</h2>
                        <p className="mt-1 text-xs text-faint">
                          Create recurring workflows and adjust them without leaving the page.
                        </p>
                      </div>
                      <div className="flex flex-col gap-3 sm:flex-row">
                        <button
                          type="button"
                          onClick={() => setScheduleRefreshNonce((value) => value + 1)}
                          className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
                        >
                          {schedulesRefreshing ? "Refreshing..." : "Refresh"}
                        </button>
                        <button
                          type="button"
                          onClick={() => {
                            setCreatingSchedule(true);
                            setSelectedScheduleID(null);
                            setSelectedSchedule(null);
                            setActiveTab(SCHEDULE_TAB);
                          }}
                          className="rounded-md bg-primary px-3 py-2 text-sm font-medium text-background transition-opacity duration-150 hover:opacity-90"
                        >
                          New schedule
                        </button>
                        <input
                          value={scheduleQuery}
                          onChange={(event) => setScheduleQuery(event.target.value)}
                          placeholder="Search by plugin, cron, provider, or id"
                          className="min-w-72 rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
                        />
                        <select
                          value={scheduleStateFilter}
                          onChange={(event) =>
                            setScheduleStateFilter(event.target.value as ToggleFilter)
                          }
                          className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
                        >
                          <option value="all">All states</option>
                          <option value="active">Active</option>
                          <option value="paused">Paused</option>
                        </select>
                      </div>
                    </div>

                    <div className="grid gap-6 lg:grid-cols-[minmax(0,1.05fr)_minmax(22rem,0.95fr)]">
                      <section className="rounded-lg border border-alpha bg-base-100 dark:bg-surface">
                        <div className="border-b border-alpha px-5 py-4">
                          <h3 className="text-sm font-medium text-primary">Schedule List</h3>
                          <p className="mt-1 text-xs text-faint">
                            {filteredSchedules.length} shown
                          </p>
                        </div>
                        <div className="divide-y divide-alpha">
                          {filteredSchedules.length === 0 ? (
                            <div className="px-5 py-8 text-sm text-faint">
                              {schedules.length === 0
                                ? "No workflow schedules yet."
                                : "No schedules match the current filters."}
                            </div>
                          ) : (
                            filteredSchedules.map((schedule) => {
                              const isActive =
                                !creatingSchedule && schedule.id === selectedScheduleID;
                              return (
                                <button
                                  key={schedule.id}
                                  type="button"
                                  onClick={() => {
                                    setCreatingSchedule(false);
                                    setSelectedScheduleID(schedule.id);
                                  }}
                                  className={`flex w-full items-start justify-between gap-4 px-5 py-4 text-left transition-colors duration-150 ${
                                    isActive ? "bg-alpha-5" : "hover:bg-alpha-5"
                                  }`}
                                >
                                  <div className="min-w-0">
                                    <div className="flex items-center gap-2">
                                      <span className="truncate text-sm font-medium text-primary">
                                        {schedule.target.plugin}.{schedule.target.operation}
                                      </span>
                                      <span className={stateClassName(schedule.paused)}>
                                        {schedule.paused ? "paused" : "active"}
                                      </span>
                                    </div>
                                    <p className="mt-1 truncate text-xs text-faint">
                                      {schedule.id}
                                    </p>
                                    <p className="mt-2 text-xs text-muted">
                                      {schedule.cron}
                                      {schedule.timezone ? ` · ${schedule.timezone}` : ""}
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
                      </section>

                      <section className="rounded-lg border border-alpha bg-base-100 p-5 dark:bg-surface">
                        <div className="flex items-start justify-between gap-4">
                          <div>
                            <h3 className="text-sm font-medium text-primary">
                              {creatingSchedule ? "New Schedule" : "Schedule Details"}
                            </h3>
                            <p className="mt-1 text-xs text-faint">
                              {creatingSchedule
                                ? "Create a new recurring workflow."
                                : selectedSchedule?.id || "Select a schedule"}
                            </p>
                          </div>
                          {!creatingSchedule && selectedSchedule ? (
                            <span className={stateClassName(selectedSchedule.paused)}>
                              {selectedSchedule.paused ? "paused" : "active"}
                            </span>
                          ) : null}
                        </div>

                        {scheduleDetailError && (
                          <p className="mt-4 text-sm text-ember-500">{scheduleDetailError}</p>
                        )}
                        {scheduleActionError && (
                          <p className="mt-4 text-sm text-ember-500">{scheduleActionError}</p>
                        )}
                        {scheduleFormError && (
                          <p className="mt-4 text-sm text-ember-500">{scheduleFormError}</p>
                        )}

                        {!creatingSchedule && selectedSchedule ? (
                          <div className="mt-4 flex flex-wrap gap-3">
                            <button
                              type="button"
                              onClick={() => void handleToggleSchedulePause()}
                              disabled={scheduleMutating}
                              className="rounded-md border border-alpha bg-background/65 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 disabled:cursor-not-allowed disabled:opacity-60 dark:bg-background/20"
                            >
                              {scheduleMutating
                                ? "Working..."
                                : selectedSchedule.paused
                                  ? "Resume schedule"
                                  : "Pause schedule"}
                            </button>
                            <button
                              type="button"
                              onClick={() => void handleDeleteSchedule()}
                              disabled={scheduleMutating}
                              className="rounded-md bg-ember-500 px-3 py-2 text-sm font-medium text-white transition-opacity duration-150 hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
                            >
                              Delete schedule
                            </button>
                          </div>
                        ) : null}

                        {scheduleDetailLoading && !creatingSchedule && !selectedSchedule ? (
                          <p className="mt-6 text-sm text-faint">Loading details...</p>
                        ) : creatingSchedule || selectedSchedule ? (
                          <div className="mt-6 space-y-6">
                            {!creatingSchedule && selectedSchedule ? (
                              <div className="grid gap-4 sm:grid-cols-2">
                                <DetailItem label="Provider" value={selectedSchedule.provider} />
                                <DetailItem
                                  label="Next run"
                                  value={formatDate(selectedSchedule.nextRunAt)}
                                />
                                <DetailItem
                                  label="Created"
                                  value={formatDate(selectedSchedule.createdAt)}
                                />
                                <DetailItem
                                  label="Updated"
                                  value={formatDate(selectedSchedule.updatedAt)}
                                />
                              </div>
                            ) : null}

                            <section className="space-y-4">
                              <div className="grid gap-4 sm:grid-cols-2">
                                <EditorField
                                  label="Provider"
                                  value={scheduleDraft.provider}
                                  placeholder="Use default provider"
                                  onChange={(value) =>
                                    setScheduleDraft((current) => ({ ...current, provider: value }))
                                  }
                                />
                                <EditorField
                                  label="Cron"
                                  value={scheduleDraft.cron}
                                  placeholder="0 9 * * 1-5"
                                  onChange={(value) =>
                                    setScheduleDraft((current) => ({ ...current, cron: value }))
                                  }
                                />
                                <EditorField
                                  label="Timezone"
                                  value={scheduleDraft.timezone}
                                  placeholder="America/New_York"
                                  onChange={(value) =>
                                    setScheduleDraft((current) => ({ ...current, timezone: value }))
                                  }
                                />
                                <EditorToggle
                                  label="Paused"
                                  checked={scheduleDraft.paused}
                                  description="Create or save the schedule in a paused state."
                                  onChange={(checked) =>
                                    setScheduleDraft((current) => ({ ...current, paused: checked }))
                                  }
                                />
                              </div>

                              <div className="grid gap-4 sm:grid-cols-2">
                                <EditorField
                                  label="Target plugin"
                                  value={scheduleDraft.target.plugin}
                                  placeholder="github"
                                  onChange={(value) =>
                                    setScheduleDraft((current) => ({
                                      ...current,
                                      target: { ...current.target, plugin: value },
                                    }))
                                  }
                                />
                                <EditorField
                                  label="Target operation"
                                  value={scheduleDraft.target.operation}
                                  placeholder="issues.create"
                                  onChange={(value) =>
                                    setScheduleDraft((current) => ({
                                      ...current,
                                      target: { ...current.target, operation: value },
                                    }))
                                  }
                                />
                                <EditorField
                                  label="Connection"
                                  value={scheduleDraft.target.connection}
                                  placeholder="Optional connection name"
                                  onChange={(value) =>
                                    setScheduleDraft((current) => ({
                                      ...current,
                                      target: { ...current.target, connection: value },
                                    }))
                                  }
                                />
                                <EditorField
                                  label="Instance"
                                  value={scheduleDraft.target.instance}
                                  placeholder="Optional instance name"
                                  onChange={(value) =>
                                    setScheduleDraft((current) => ({
                                      ...current,
                                      target: { ...current.target, instance: value },
                                    }))
                                  }
                                />
                              </div>

                              <EditorTextArea
                                label="Input JSON"
                                value={scheduleDraft.target.inputText}
                                placeholder={'{"owner":"gestalt-bot"}'}
                                onChange={(value) =>
                                  setScheduleDraft((current) => ({
                                    ...current,
                                    target: { ...current.target, inputText: value },
                                  }))
                                }
                              />
                            </section>

                            <div className="flex flex-wrap gap-3">
                              <button
                                type="button"
                                onClick={() => void handleSaveSchedule()}
                                disabled={scheduleSaving}
                                className="rounded-md bg-primary px-3 py-2 text-sm font-medium text-background transition-opacity duration-150 hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
                              >
                                {scheduleSaving
                                  ? creatingSchedule
                                    ? "Creating..."
                                    : "Saving..."
                                  : creatingSchedule
                                    ? "Create schedule"
                                    : "Save schedule"}
                              </button>
                              {creatingSchedule ? (
                                <button
                                  type="button"
                                  onClick={() => {
                                    setCreatingSchedule(false);
                                    setSelectedScheduleID(schedules[0]?.id ?? null);
                                  }}
                                  className="rounded-md border border-alpha bg-background/65 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-background/20"
                                >
                                  Cancel
                                </button>
                              ) : null}
                            </div>
                          </div>
                        ) : (
                          <p className="mt-6 text-sm text-faint">
                            Select a schedule to edit it, or create a new one.
                          </p>
                        )}
                      </section>
                    </div>
                  </div>
                )}
              </div>
            )}

            {activeTab === TRIGGER_TAB && (
              <div className="mt-8">
                {triggersError && <p className="mb-4 text-sm text-ember-500">{triggersError}</p>}

                {triggersLoading ? (
                  <p className="text-sm text-faint">Loading...</p>
                ) : (
                  <div className="space-y-6">
                    <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                      <div>
                        <h2 className="text-sm font-medium text-primary">Event Triggers</h2>
                        <p className="mt-1 text-xs text-faint">
                          Route published workflow events into global operations.
                        </p>
                      </div>
                      <div className="flex flex-col gap-3 sm:flex-row">
                        <button
                          type="button"
                          onClick={() => setTriggerRefreshNonce((value) => value + 1)}
                          className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
                        >
                          {triggersRefreshing ? "Refreshing..." : "Refresh"}
                        </button>
                        <button
                          type="button"
                          onClick={() => {
                            setCreatingTrigger(true);
                            setSelectedTriggerID(null);
                            setSelectedTrigger(null);
                            setActiveTab(TRIGGER_TAB);
                          }}
                          className="rounded-md bg-primary px-3 py-2 text-sm font-medium text-background transition-opacity duration-150 hover:opacity-90"
                        >
                          New event trigger
                        </button>
                        <input
                          value={triggerQuery}
                          onChange={(event) => setTriggerQuery(event.target.value)}
                          placeholder="Search by event, plugin, provider, or id"
                          className="min-w-72 rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
                        />
                        <select
                          value={triggerStateFilter}
                          onChange={(event) =>
                            setTriggerStateFilter(event.target.value as ToggleFilter)
                          }
                          className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
                        >
                          <option value="all">All states</option>
                          <option value="active">Active</option>
                          <option value="paused">Paused</option>
                        </select>
                      </div>
                    </div>

                    <div className="grid gap-6 lg:grid-cols-[minmax(0,1.05fr)_minmax(22rem,0.95fr)]">
                      <section className="rounded-lg border border-alpha bg-base-100 dark:bg-surface">
                        <div className="border-b border-alpha px-5 py-4">
                          <h3 className="text-sm font-medium text-primary">Trigger List</h3>
                          <p className="mt-1 text-xs text-faint">
                            {filteredTriggers.length} shown
                          </p>
                        </div>
                        <div className="divide-y divide-alpha">
                          {filteredTriggers.length === 0 ? (
                            <div className="px-5 py-8 text-sm text-faint">
                              {triggers.length === 0
                                ? "No workflow event triggers yet."
                                : "No event triggers match the current filters."}
                            </div>
                          ) : (
                            filteredTriggers.map((trigger) => {
                              const isActive =
                                !creatingTrigger && trigger.id === selectedTriggerID;
                              return (
                                <button
                                  key={trigger.id}
                                  type="button"
                                  onClick={() => {
                                    setCreatingTrigger(false);
                                    setSelectedTriggerID(trigger.id);
                                  }}
                                  className={`flex w-full items-start justify-between gap-4 px-5 py-4 text-left transition-colors duration-150 ${
                                    isActive ? "bg-alpha-5" : "hover:bg-alpha-5"
                                  }`}
                                >
                                  <div className="min-w-0">
                                    <div className="flex items-center gap-2">
                                      <span className="truncate text-sm font-medium text-primary">
                                        {trigger.match.type}
                                      </span>
                                      <span className={stateClassName(trigger.paused)}>
                                        {trigger.paused ? "paused" : "active"}
                                      </span>
                                    </div>
                                    <p className="mt-1 truncate text-xs text-faint">{trigger.id}</p>
                                    <p className="mt-2 text-xs text-muted">
                                      {trigger.target.plugin}.{trigger.target.operation}
                                    </p>
                                  </div>
                                  <div className="shrink-0 text-right text-xs text-faint">
                                    {trigger.match.source || trigger.match.subject || trigger.provider}
                                  </div>
                                </button>
                              );
                            })
                          )}
                        </div>
                      </section>

                      <section className="rounded-lg border border-alpha bg-base-100 p-5 dark:bg-surface">
                        <div className="flex items-start justify-between gap-4">
                          <div>
                            <h3 className="text-sm font-medium text-primary">
                              {creatingTrigger ? "New Event Trigger" : "Event Trigger Details"}
                            </h3>
                            <p className="mt-1 text-xs text-faint">
                              {creatingTrigger
                                ? "Create a new event-driven workflow."
                                : selectedTrigger?.id || "Select an event trigger"}
                            </p>
                          </div>
                          {!creatingTrigger && selectedTrigger ? (
                            <span className={stateClassName(selectedTrigger.paused)}>
                              {selectedTrigger.paused ? "paused" : "active"}
                            </span>
                          ) : null}
                        </div>

                        {triggerDetailError && (
                          <p className="mt-4 text-sm text-ember-500">{triggerDetailError}</p>
                        )}
                        {triggerActionError && (
                          <p className="mt-4 text-sm text-ember-500">{triggerActionError}</p>
                        )}
                        {triggerFormError && (
                          <p className="mt-4 text-sm text-ember-500">{triggerFormError}</p>
                        )}

                        {!creatingTrigger && selectedTrigger ? (
                          <div className="mt-4 flex flex-wrap gap-3">
                            <button
                              type="button"
                              onClick={() => void handleToggleTriggerPause()}
                              disabled={triggerMutating}
                              className="rounded-md border border-alpha bg-background/65 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 disabled:cursor-not-allowed disabled:opacity-60 dark:bg-background/20"
                            >
                              {triggerMutating
                                ? "Working..."
                                : selectedTrigger.paused
                                  ? "Resume event trigger"
                                  : "Pause event trigger"}
                            </button>
                            <button
                              type="button"
                              onClick={() => void handleDeleteTrigger()}
                              disabled={triggerMutating}
                              className="rounded-md bg-ember-500 px-3 py-2 text-sm font-medium text-white transition-opacity duration-150 hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
                            >
                              Delete event trigger
                            </button>
                          </div>
                        ) : null}

                        {triggerDetailLoading && !creatingTrigger && !selectedTrigger ? (
                          <p className="mt-6 text-sm text-faint">Loading details...</p>
                        ) : creatingTrigger || selectedTrigger ? (
                          <div className="mt-6 space-y-6">
                            {!creatingTrigger && selectedTrigger ? (
                              <div className="grid gap-4 sm:grid-cols-2">
                                <DetailItem label="Provider" value={selectedTrigger.provider} />
                                <DetailItem
                                  label="Created"
                                  value={formatDate(selectedTrigger.createdAt)}
                                />
                                <DetailItem
                                  label="Updated"
                                  value={formatDate(selectedTrigger.updatedAt)}
                                />
                                <DetailItem
                                  label="Target"
                                  value={`${selectedTrigger.target.plugin}.${selectedTrigger.target.operation}`}
                                />
                              </div>
                            ) : null}

                            <section className="space-y-4">
                              <div className="grid gap-4 sm:grid-cols-2">
                                <EditorField
                                  label="Provider"
                                  value={triggerDraft.provider}
                                  placeholder="Use default provider"
                                  onChange={(value) =>
                                    setTriggerDraft((current) => ({ ...current, provider: value }))
                                  }
                                />
                                <EditorToggle
                                  label="Paused"
                                  checked={triggerDraft.paused}
                                  description="Create or save the trigger in a paused state."
                                  onChange={(checked) =>
                                    setTriggerDraft((current) => ({ ...current, paused: checked }))
                                  }
                                />
                                <EditorField
                                  label="Event type"
                                  value={triggerDraft.eventType}
                                  placeholder="task.updated"
                                  onChange={(value) =>
                                    setTriggerDraft((current) => ({ ...current, eventType: value }))
                                  }
                                />
                                <EditorField
                                  label="Source"
                                  value={triggerDraft.source}
                                  placeholder="Optional event source"
                                  onChange={(value) =>
                                    setTriggerDraft((current) => ({ ...current, source: value }))
                                  }
                                />
                                <EditorField
                                  label="Subject"
                                  value={triggerDraft.subject}
                                  placeholder="Optional event subject"
                                  onChange={(value) =>
                                    setTriggerDraft((current) => ({ ...current, subject: value }))
                                  }
                                />
                              </div>

                              <div className="grid gap-4 sm:grid-cols-2">
                                <EditorField
                                  label="Target plugin"
                                  value={triggerDraft.target.plugin}
                                  placeholder="github"
                                  onChange={(value) =>
                                    setTriggerDraft((current) => ({
                                      ...current,
                                      target: { ...current.target, plugin: value },
                                    }))
                                  }
                                />
                                <EditorField
                                  label="Target operation"
                                  value={triggerDraft.target.operation}
                                  placeholder="issues.create"
                                  onChange={(value) =>
                                    setTriggerDraft((current) => ({
                                      ...current,
                                      target: { ...current.target, operation: value },
                                    }))
                                  }
                                />
                                <EditorField
                                  label="Connection"
                                  value={triggerDraft.target.connection}
                                  placeholder="Optional connection name"
                                  onChange={(value) =>
                                    setTriggerDraft((current) => ({
                                      ...current,
                                      target: { ...current.target, connection: value },
                                    }))
                                  }
                                />
                                <EditorField
                                  label="Instance"
                                  value={triggerDraft.target.instance}
                                  placeholder="Optional instance name"
                                  onChange={(value) =>
                                    setTriggerDraft((current) => ({
                                      ...current,
                                      target: { ...current.target, instance: value },
                                    }))
                                  }
                                />
                              </div>

                              <EditorTextArea
                                label="Input JSON"
                                value={triggerDraft.target.inputText}
                                placeholder={'{"owner":"gestalt-bot"}'}
                                onChange={(value) =>
                                  setTriggerDraft((current) => ({
                                    ...current,
                                    target: { ...current.target, inputText: value },
                                  }))
                                }
                              />
                            </section>

                            <div className="flex flex-wrap gap-3">
                              <button
                                type="button"
                                onClick={() => void handleSaveTrigger()}
                                disabled={triggerSaving}
                                className="rounded-md bg-primary px-3 py-2 text-sm font-medium text-background transition-opacity duration-150 hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
                              >
                                {triggerSaving
                                  ? creatingTrigger
                                    ? "Creating..."
                                    : "Saving..."
                                  : creatingTrigger
                                    ? "Create event trigger"
                                    : "Save event trigger"}
                              </button>
                              {creatingTrigger ? (
                                <button
                                  type="button"
                                  onClick={() => {
                                    setCreatingTrigger(false);
                                    setSelectedTriggerID(triggers[0]?.id ?? null);
                                  }}
                                  className="rounded-md border border-alpha bg-background/65 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-background/20"
                                >
                                  Cancel
                                </button>
                              ) : null}
                            </div>
                          </div>
                        ) : (
                          <p className="mt-6 text-sm text-faint">
                            Select an event trigger to edit it, or create a new one.
                          </p>
                        )}
                      </section>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        </main>
      </div>
    </AuthGuard>
  );
}

function WorkflowTabButton({
  label,
  count,
  active,
  onClick,
}: {
  label: string;
  count: number;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`inline-flex items-center gap-3 rounded-full border px-4 py-2 text-sm transition-colors duration-150 ${
        active
          ? "border-alpha-strong bg-alpha-5 text-primary"
          : "border-alpha bg-base-100 text-muted hover:bg-alpha-5 hover:text-primary dark:bg-surface"
      }`}
    >
      <span>{label}</span>
      <span className="rounded-full bg-background/70 px-2 py-0.5 text-xs text-faint dark:bg-background/20">
        {count}
      </span>
    </button>
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

function EditorField({
  label,
  value,
  placeholder,
  onChange,
}: {
  label: string;
  value: string;
  placeholder: string;
  onChange: (value: string) => void;
}) {
  return (
    <label className="block">
      <span className="text-xs font-medium uppercase tracking-[0.18em] text-faint">
        {label}
      </span>
      <input
        value={value}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
        className="mt-2 w-full rounded-md border border-alpha bg-background/65 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-background/20"
      />
    </label>
  );
}

function EditorTextArea({
  label,
  value,
  placeholder,
  onChange,
}: {
  label: string;
  value: string;
  placeholder: string;
  onChange: (value: string) => void;
}) {
  return (
    <label className="block">
      <span className="text-xs font-medium uppercase tracking-[0.18em] text-faint">
        {label}
      </span>
      <textarea
        value={value}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
        rows={6}
        className="mt-2 w-full rounded-md border border-alpha bg-background/65 px-3 py-2 font-mono text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-background/20"
      />
    </label>
  );
}

function EditorToggle({
  label,
  checked,
  description,
  onChange,
}: {
  label: string;
  checked: boolean;
  description: string;
  onChange: (checked: boolean) => void;
}) {
  return (
    <label className="flex h-full items-start gap-3 rounded-md border border-alpha bg-background/65 px-4 py-3 dark:bg-background/20">
      <input
        type="checkbox"
        checked={checked}
        onChange={(event) => onChange(event.target.checked)}
        className="mt-0.5 h-4 w-4 rounded border-alpha"
      />
      <span>
        <span className="text-xs font-medium uppercase tracking-[0.18em] text-faint">
          {label}
        </span>
        <p className="mt-2 text-sm text-muted">{description}</p>
      </span>
    </label>
  );
}

function triggerLabel(run: WorkflowRun): string {
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

function stateClassName(paused: boolean): string {
  return paused
    ? "rounded-full bg-amber-100 px-2 py-1 text-[11px] font-medium text-amber-700 dark:bg-amber-700/20 dark:text-amber-200"
    : "rounded-full bg-grove-100 px-2 py-1 text-[11px] font-medium text-grove-700 dark:bg-grove-700/20 dark:text-grove-200";
}

function statusClassName(status?: string): string {
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

function emptyTargetDraft(): TargetDraft {
  return {
    plugin: "",
    operation: "",
    connection: "",
    instance: "",
    inputText: "",
  };
}

function emptyScheduleDraft(): ScheduleDraft {
  return {
    provider: "",
    cron: "",
    timezone: "",
    paused: false,
    target: emptyTargetDraft(),
  };
}

function emptyEventTriggerDraft(): EventTriggerDraft {
  return {
    provider: "",
    eventType: "",
    source: "",
    subject: "",
    paused: false,
    target: emptyTargetDraft(),
  };
}

function scheduleToDraft(schedule: WorkflowSchedule): ScheduleDraft {
  return {
    provider: schedule.provider || "",
    cron: schedule.cron || "",
    timezone: schedule.timezone || "",
    paused: !!schedule.paused,
    target: targetToDraft(schedule.target),
  };
}

function triggerToDraft(trigger: WorkflowEventTrigger): EventTriggerDraft {
  return {
    provider: trigger.provider || "",
    eventType: trigger.match.type || "",
    source: trigger.match.source || "",
    subject: trigger.match.subject || "",
    paused: !!trigger.paused,
    target: targetToDraft(trigger.target),
  };
}

function targetToDraft(target: WorkflowTarget): TargetDraft {
  return {
    plugin: target.plugin || "",
    operation: target.operation || "",
    connection: target.connection || "",
    instance: target.instance || "",
    inputText: target.input ? prettyJSON(target.input) : "",
  };
}

function scheduleDraftToRequest(draft: ScheduleDraft) {
  return {
    provider: normalizeOptionalField(draft.provider),
    cron: requireField(draft.cron, "Cron is required"),
    timezone: normalizeOptionalField(draft.timezone),
    paused: draft.paused,
    target: targetDraftToRequest(draft.target),
  };
}

function triggerDraftToRequest(draft: EventTriggerDraft) {
  return {
    provider: normalizeOptionalField(draft.provider),
    paused: draft.paused,
    match: {
      type: requireField(draft.eventType, "Event type is required"),
      source: normalizeOptionalField(draft.source),
      subject: normalizeOptionalField(draft.subject),
    },
    target: targetDraftToRequest(draft.target),
  };
}

function targetDraftToRequest(draft: TargetDraft): WorkflowTarget {
  return {
    plugin: requireField(draft.plugin, "Target plugin is required"),
    operation: requireField(draft.operation, "Target operation is required"),
    connection: normalizeOptionalField(draft.connection),
    instance: normalizeOptionalField(draft.instance),
    input: parseInputJSON(draft.inputText),
  };
}

function parseInputJSON(text: string): Record<string, unknown> | undefined {
  const trimmed = text.trim();
  if (!trimmed) {
    return undefined;
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    throw new Error("Input JSON must be valid JSON");
  }

  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Input JSON must be a JSON object");
  }

  return parsed as Record<string, unknown>;
}

function requireField(value: string, message: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(message);
  }
  return trimmed;
}

function normalizeOptionalField(value: string): string | undefined {
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
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

function errorMessage(reason: unknown, fallback: string): string {
  if (reason instanceof Error) {
    return reason.message;
  }
  if (typeof reason === "string") {
    return reason;
  }
  return fallback;
}
