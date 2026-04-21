"use client";

import { useDeferredValue, useEffect, useState } from "react";
import type { WorkflowRun } from "@/lib/api";
import { getWorkflowRun, getWorkflowRuns } from "@/lib/api";
import AuthGuard from "@/components/AuthGuard";
import Nav from "@/components/Nav";

export default function WorkflowsPage() {
  const [runs, setRuns] = useState<WorkflowRun[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedRunID, setSelectedRunID] = useState<string | null>(null);
  const [selectedRun, setSelectedRun] = useState<WorkflowRun | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("all");
  const deferredQuery = useDeferredValue(query);

  useEffect(() => {
    let active = true;
    getWorkflowRuns()
      .then((nextRuns) => {
        if (!active) return;
        setRuns(nextRuns);
        setError(null);
        setSelectedRunID((current) =>
          current && nextRuns.some((run) => run.id === current)
            ? current
            : nextRuns[0]?.id ?? null,
        );
      })
      .catch((err) => {
        if (!active) return;
        setError(err instanceof Error ? err.message : "Failed to load workflow runs");
      })
      .finally(() => {
        if (active) {
          setLoading(false);
        }
      });

    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (!selectedRunID) {
      setSelectedRun(null);
      setDetailError(null);
      return;
    }

    const cached = runs.find((run) => run.id === selectedRunID) ?? null;
    if (cached) {
      setSelectedRun(cached);
    }

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
        setDetailError(err instanceof Error ? err.message : "Failed to load workflow run");
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

  const trimmedQuery = deferredQuery.trim().toLowerCase();
  const filteredRuns = runs.filter((run) => {
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

  return (
    <AuthGuard>
      <div className="min-h-screen">
        <Nav />
        <main className="mx-auto max-w-6xl px-6 py-12">
          <div className="animate-fade-in-up flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
            <div>
              <span className="label-text">Automation</span>
              <h1 className="mt-2 text-2xl font-heading font-bold text-primary">
                Workflow Runs
              </h1>
              <p className="mt-2 text-sm text-muted">
                Inspect recent global workflow activity across plugins and providers.
              </p>
            </div>
            <div className="flex flex-col gap-3 sm:flex-row">
              <input
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder="Search by run, plugin, provider, or trigger"
                className="min-w-72 rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
              />
              <select
                value={status}
                onChange={(event) => setStatus(event.target.value)}
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

          {error && <p className="mt-6 text-sm text-ember-500">{error}</p>}

          {loading ? (
            <p className="mt-10 text-sm text-faint">Loading...</p>
          ) : !error && runs.length === 0 ? (
            <div className="mt-10 rounded-lg border border-alpha bg-base-100 p-8 text-sm text-faint dark:bg-surface">
              No workflow runs yet.
            </div>
          ) : !error ? (
            <div className="mt-10 grid gap-6 lg:grid-cols-[minmax(0,1.15fr)_minmax(20rem,0.85fr)] animate-fade-in-up [animation-delay:60ms]">
              <section className="rounded-lg border border-alpha bg-base-100 dark:bg-surface">
                <div className="border-b border-alpha px-5 py-4">
                  <h2 className="text-sm font-medium text-primary">
                    Recent Runs
                  </h2>
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
                            isActive
                              ? "bg-alpha-5"
                              : "hover:bg-alpha-5"
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
                            <p className="mt-1 truncate text-xs text-faint">
                              {run.id}
                            </p>
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
                    <h2 className="text-sm font-medium text-primary">
                      Run Details
                    </h2>
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

                {detailError && (
                  <p className="mt-4 text-sm text-ember-500">{detailError}</p>
                )}

                {detailLoading && !selectedRun ? (
                  <p className="mt-6 text-sm text-faint">Loading details...</p>
                ) : selectedRun ? (
                  <div className="mt-6 space-y-6">
                    <div className="grid gap-4 sm:grid-cols-2">
                      <DetailItem label="Provider" value={selectedRun.provider} />
                      <DetailItem
                        label="Trigger"
                        value={triggerLabel(selectedRun)}
                      />
                      <DetailItem
                        label="Created"
                        value={formatDate(selectedRun.createdAt)}
                      />
                      <DetailItem
                        label="Started"
                        value={formatDate(selectedRun.startedAt)}
                      />
                      <DetailItem
                        label="Completed"
                        value={formatDate(selectedRun.completedAt)}
                      />
                      <DetailItem
                        label="Actor"
                        value={
                          selectedRun.createdBy?.displayName ||
                          selectedRun.createdBy?.subjectId ||
                          "-"
                        }
                      />
                    </div>

                    <section>
                      <h3 className="text-xs font-medium uppercase tracking-[0.18em] text-faint">
                        Target
                      </h3>
                      <div className="mt-3 rounded-md border border-alpha bg-background/65 p-4 text-sm dark:bg-background/20">
                        <p className="font-medium text-primary">
                          {selectedRun.target.plugin}.{selectedRun.target.operation}
                        </p>
                        <p className="mt-2 text-xs text-muted">
                          Connection: {selectedRun.target.connection || "-"} · Instance:{" "}
                          {selectedRun.target.instance || "-"}
                        </p>
                        {selectedRun.target.input && (
                          <pre className="mt-3 overflow-x-auto text-xs text-primary">
                            {prettyJSON(selectedRun.target.input)}
                          </pre>
                        )}
                      </div>
                    </section>

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
          ) : null}
        </main>
      </div>
    </AuthGuard>
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

function triggerLabel(run: WorkflowRun): string {
  if (run.trigger?.kind === "schedule") {
    return run.trigger.scheduleId
      ? `schedule:${run.trigger.scheduleId}`
      : "schedule";
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
