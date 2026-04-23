"use client";

import { useDeferredValue, useEffect, useEffectEvent, useState } from "react";
import { useRouter } from "next/navigation";
import type {
  AgentRun,
  AgentRunCreate,
  AgentToolRef,
  Integration,
  IntegrationOperation,
} from "@/lib/api";
import {
  cancelAgentRun,
  createAgentRun,
  getAgentRun,
  getAgentRuns,
  getIntegrationOperations,
  getIntegrations,
  isAPIErrorStatus,
} from "@/lib/api";
import AuthGuard from "@/components/AuthGuard";
import Nav from "@/components/Nav";

type AgentRunFormMode = "create" | null;
type AgentToolMode = "none" | "explicit";

interface AgentToolForm {
  pluginName: string;
  operation: string;
  connection: string;
  instance: string;
  title: string;
  description: string;
}

interface AgentRunFormState {
  provider: string;
  model: string;
  systemPrompt: string;
  userPrompt: string;
  sessionRef: string;
  idempotencyKey: string;
  responseSchemaJSON: string;
  metadataJSON: string;
  providerOptionsJSON: string;
  toolMode: AgentToolMode;
  tools: AgentToolForm[];
}

const EMPTY_OPERATIONS: IntegrationOperation[] = [];

export default function AgentsPage() {
  const router = useRouter();
  const [runs, setRuns] = useState<AgentRun[]>([]);
  const [integrations, setIntegrations] = useState<Integration[]>([]);

  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshNonce, setRefreshNonce] = useState(0);

  const [runsError, setRunsError] = useState<string | null>(null);
  const [integrationsError, setIntegrationsError] = useState<string | null>(null);

  const [selectedRunID, setSelectedRunID] = useState<string | null>(null);
  const [selectedRun, setSelectedRun] = useState<AgentRun | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [canceling, setCanceling] = useState(false);

  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("all");
  const deferredQuery = useDeferredValue(query);

  const [formMode, setFormMode] = useState<AgentRunFormMode>(null);
  const [form, setForm] = useState<AgentRunFormState>(() => defaultAgentRunForm());
  const [formError, setFormError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const [operationsByPlugin, setOperationsByPlugin] = useState<
    Record<string, IntegrationOperation[]>
  >({});
  const [operationsLoadingByPlugin, setOperationsLoadingByPlugin] = useState<
    Record<string, boolean>
  >({});
  const [operationErrorsByPlugin, setOperationErrorsByPlugin] = useState<
    Record<string, string | undefined>
  >({});

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

    getAgentRuns()
      .then((value) => {
        if (!active) return;
        setRuns(value);
        setRunsError(null);
      })
      .catch((err) => {
        if (!active) return;
        if (isAPIErrorStatus(err, 412)) {
          router.replace("/");
          return;
        }
        setRunsError(errorMessage(err, "Failed to load agent runs"));
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
  }, [refreshNonce, router]);

  useEffect(() => {
    setSelectedRunID((current) =>
      current && runs.some((run) => run.id === current) ? current : runs[0]?.id ?? null,
    );
  }, [runs]);

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

    getAgentRun(selectedRunID)
      .then((run) => {
        if (!active) return;
        setSelectedRun(run);
      })
      .catch((err) => {
        if (!active) return;
        setDetailError(errorMessage(err, "Failed to load agent run"));
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

  const ensureOperationsLoadedEvent = useEffectEvent((pluginName: string) => {
    void ensureOperationsLoaded(pluginName);
  });

  useEffect(() => {
    if (!formMode || form.toolMode !== "explicit") return;
    form.tools.forEach((tool) => {
      if (tool.pluginName) {
        ensureOperationsLoadedEvent(tool.pluginName);
      }
    });
  }, [formMode, form.toolMode, form.tools]);

  const agentIntegrations = integrations
    .filter((integration) => !integration.mountedPath)
    .slice()
    .sort((left, right) =>
      integrationLabel(left).localeCompare(integrationLabel(right), undefined, {
        sensitivity: "base",
      }),
    );

  const filteredRuns = filterRuns(runs, deferredQuery, status);
  const selectedRunCancelable = isCancelableStatus(selectedRun?.status);

  function beginCreate() {
    setForm(defaultAgentRunForm());
    setFormError(null);
    setNotice(null);
    setActionError(null);
    setFormMode("create");
  }

  function cancelForm() {
    setFormMode(null);
    setFormError(null);
  }

  function upsertRun(run: AgentRun) {
    setRuns((current) => {
      const index = current.findIndex((item) => item.id === run.id);
      if (index === -1) {
        return [run, ...current];
      }
      return current.map((item) => (item.id === run.id ? run : item));
    });
    setSelectedRun(run);
    setSelectedRunID(run.id);
  }

  async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setFormError(null);
    setNotice(null);

    let body: AgentRunCreate;
    try {
      body = agentRunFormToCreate(form);
    } catch (err) {
      setFormError(errorMessage(err, "Invalid run request"));
      return;
    }

    setSubmitting(true);
    try {
      const created = await createAgentRun(body);
      upsertRun(created);
      setFormMode(null);
      setForm(defaultAgentRunForm());
      setNotice("Agent run started.");
    } catch (err) {
      setFormError(errorMessage(err, "Failed to start agent run"));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleCancelSelectedRun() {
    if (!selectedRunID || !selectedRunCancelable) return;

    setCanceling(true);
    setActionError(null);
    try {
      const updated = await cancelAgentRun(selectedRunID, "Run canceled.");
      upsertRun(updated);
      setRefreshNonce((value) => value + 1);
    } catch (err) {
      setActionError(errorMessage(err, "Failed to cancel agent run"));
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
              <span className="label-text">Orchestration</span>
              <h1 className="mt-2 text-2xl font-heading font-bold text-primary">Agents</h1>
              <p className="mt-2 max-w-3xl text-sm text-muted">
                Start agent runs, inspect model messages, and manage active execution
                across configured agent providers.
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                onClick={beginCreate}
                className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
              >
                New run
              </button>
              <button
                type="button"
                onClick={() => setRefreshNonce((value) => value + 1)}
                className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
              >
                {refreshing ? "Refreshing..." : "Refresh"}
              </button>
            </div>
          </div>

          {loading ? (
            <p className="mt-10 text-sm text-faint">Loading...</p>
          ) : (
            <div className="mt-10 space-y-6 animate-fade-in-up [animation-delay:60ms]">
              <section className="rounded-lg border border-alpha bg-base-100 p-4 dark:bg-surface">
                <div className="flex flex-col gap-3 lg:flex-row">
                  <input
                    value={query}
                    onChange={(event) => setQuery(event.target.value)}
                    placeholder="Search by run, provider, model, status, or message"
                    className="min-w-0 flex-1 rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
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
              </section>

              <div className="grid gap-6 lg:grid-cols-[minmax(0,1.1fr)_minmax(22rem,0.9fr)]">
                <section className="rounded-lg border border-alpha bg-base-100 dark:bg-surface">
                  <div className="flex items-center justify-between gap-4 border-b border-alpha px-5 py-4">
                    <div>
                      <h2 className="text-sm font-medium text-primary">Agent Runs</h2>
                      <p className="mt-1 text-xs text-faint">{filteredRuns.length} shown</p>
                    </div>
                    <button
                      type="button"
                      onClick={beginCreate}
                      className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
                    >
                      New run
                    </button>
                  </div>

                  {runsError ? (
                    <p className="px-5 py-8 text-sm text-ember-500">{runsError}</p>
                  ) : (
                    <div className="divide-y divide-alpha">
                      {filteredRuns.length === 0 ? (
                        <div className="px-5 py-8 text-sm text-faint">
                          No agent runs yet.
                        </div>
                      ) : (
                        filteredRuns.map((run) => {
                          const isActive = run.id === selectedRunID && !formMode;
                          return (
                            <button
                              key={run.id}
                              type="button"
                              onClick={() => {
                                setFormMode(null);
                                setSelectedRunID(run.id);
                              }}
                              className={`flex w-full items-start justify-between gap-4 px-5 py-4 text-left transition-colors duration-150 ${
                                isActive ? "bg-alpha-5" : "hover:bg-alpha-5"
                              }`}
                            >
                              <div className="min-w-0">
                                <div className="flex items-center gap-2">
                                  <span className="truncate text-sm font-medium text-primary">
                                    {agentRunLabel(run)}
                                  </span>
                                  <span className={runStatusClassName(run.status)}>
                                    {run.status || "unknown"}
                                  </span>
                                </div>
                                <p className="mt-1 truncate text-xs text-faint">{run.id}</p>
                                <p className="mt-2 truncate text-xs text-muted">
                                  {run.provider || "default"} / {run.model || "default model"}
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
                  <div className="flex flex-wrap items-start justify-between gap-4">
                    <div>
                      <h2 className="text-sm font-medium text-primary">
                        {formMode === "create" ? "Start Agent Run" : "Run Details"}
                      </h2>
                      <p className="mt-1 text-xs text-faint">
                        {formMode === "create"
                          ? "Create through the global agent API"
                          : selectedRun?.id || "Select a run"}
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
                    ) : selectedRun ? (
                      <div className="flex flex-wrap items-center gap-2">
                        {selectedRun.status ? (
                          <span className={runStatusClassName(selectedRun.status)}>
                            {selectedRun.status}
                          </span>
                        ) : null}
                        {selectedRunCancelable ? (
                          <button
                            type="button"
                            onClick={() => void handleCancelSelectedRun()}
                            disabled={canceling}
                            className="rounded-md bg-ember-500 px-3 py-2 text-sm font-medium text-white transition-opacity duration-150 hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
                          >
                            {canceling ? "Canceling..." : "Cancel run"}
                          </button>
                        ) : null}
                      </div>
                    ) : null}
                  </div>

                  {notice ? (
                    <p className="mt-4 text-sm text-grove-700 dark:text-grove-200">
                      {notice}
                    </p>
                  ) : null}
                  {actionError ? <p className="mt-4 text-sm text-ember-500">{actionError}</p> : null}
                  {detailError ? <p className="mt-4 text-sm text-ember-500">{detailError}</p> : null}

                  {formMode === "create" ? (
                    <AgentRunForm
                      form={form}
                      integrations={agentIntegrations}
                      integrationsError={integrationsError}
                      operationsByPlugin={operationsByPlugin}
                      operationsLoadingByPlugin={operationsLoadingByPlugin}
                      operationErrorsByPlugin={operationErrorsByPlugin}
                      formError={formError}
                      submitting={submitting}
                      setForm={setForm}
                      onSubmit={handleSubmit}
                      ensureOperationsLoaded={ensureOperationsLoaded}
                    />
                  ) : detailLoading && !selectedRun ? (
                    <p className="mt-6 text-sm text-faint">Loading details...</p>
                  ) : selectedRun ? (
                    <AgentRunDetails run={selectedRun} />
                  ) : (
                    <p className="mt-6 text-sm text-faint">Select an agent run to inspect it.</p>
                  )}
                </section>
              </div>
            </div>
          )}
        </main>
      </div>
    </AuthGuard>
  );
}

function AgentRunForm({
  form,
  integrations,
  integrationsError,
  operationsByPlugin,
  operationsLoadingByPlugin,
  operationErrorsByPlugin,
  formError,
  submitting,
  setForm,
  onSubmit,
  ensureOperationsLoaded,
}: {
  form: AgentRunFormState;
  integrations: Integration[];
  integrationsError: string | null;
  operationsByPlugin: Record<string, IntegrationOperation[]>;
  operationsLoadingByPlugin: Record<string, boolean>;
  operationErrorsByPlugin: Record<string, string | undefined>;
  formError: string | null;
  submitting: boolean;
  setForm: React.Dispatch<React.SetStateAction<AgentRunFormState>>;
  onSubmit: (event: React.FormEvent<HTMLFormElement>) => void | Promise<void>;
  ensureOperationsLoaded: (pluginName: string) => Promise<void>;
}) {
  function updateTool(index: number, patch: Partial<AgentToolForm>) {
    setForm((current) => ({
      ...current,
      tools: current.tools.map((tool, itemIndex) =>
        itemIndex === index ? { ...tool, ...patch } : tool,
      ),
    }));
  }

  function addTool() {
    setForm((current) => ({
      ...current,
      toolMode: "explicit",
      tools: [...current.tools, emptyAgentToolForm()],
    }));
  }

  function removeTool(index: number) {
    setForm((current) => {
      const tools = current.tools.filter((_, itemIndex) => itemIndex !== index);
      return {
        ...current,
        tools: tools.length === 0 ? [emptyAgentToolForm()] : tools,
      };
    });
  }

  return (
    <form className="mt-6 space-y-6" onSubmit={onSubmit}>
      {formError ? <p className="text-sm text-ember-500">{formError}</p> : null}

      <div className="grid gap-4 sm:grid-cols-2">
        <label className="space-y-2 text-sm">
          <span className="text-muted">Provider</span>
          <input
            value={form.provider}
            onChange={(event) =>
              setForm((current) => ({ ...current, provider: event.target.value }))
            }
            placeholder="default"
            className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
          />
        </label>

        <label className="space-y-2 text-sm">
          <span className="text-muted">Model</span>
          <input
            value={form.model}
            onChange={(event) =>
              setForm((current) => ({ ...current, model: event.target.value }))
            }
            placeholder="provider default"
            className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
          />
        </label>
      </div>

      <label className="block space-y-2 text-sm">
        <span className="text-muted">System message</span>
        <textarea
          value={form.systemPrompt}
          onChange={(event) =>
            setForm((current) => ({ ...current, systemPrompt: event.target.value }))
          }
          rows={3}
          className="w-full resize-y rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
        />
      </label>

      <label className="block space-y-2 text-sm">
        <span className="text-muted">User message</span>
        <textarea
          value={form.userPrompt}
          onChange={(event) =>
            setForm((current) => ({ ...current, userPrompt: event.target.value }))
          }
          rows={5}
          required
          className="w-full resize-y rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
        />
      </label>

      <div className="grid gap-4 sm:grid-cols-2">
        <label className="space-y-2 text-sm">
          <span className="text-muted">Session ref</span>
          <input
            value={form.sessionRef}
            onChange={(event) =>
              setForm((current) => ({ ...current, sessionRef: event.target.value }))
            }
            className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
          />
        </label>

        <label className="space-y-2 text-sm">
          <span className="text-muted">Idempotency key</span>
          <input
            value={form.idempotencyKey}
            onChange={(event) =>
              setForm((current) => ({ ...current, idempotencyKey: event.target.value }))
            }
            className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
          />
        </label>
      </div>

      <section className="rounded-md border border-alpha bg-background/65 p-4 dark:bg-background/20">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h3 className="text-sm font-medium text-primary">Tools</h3>
            <p className="mt-1 text-xs text-faint">
              Expose plugin operations to the agent provider.
            </p>
          </div>
          <select
            aria-label="Tools"
            value={form.toolMode}
            onChange={(event) =>
              setForm((current) => ({
                ...current,
                toolMode: event.target.value as AgentToolMode,
              }))
            }
            className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
          >
            <option value="none">No tools</option>
            <option value="explicit">Explicit tools</option>
          </select>
        </div>

        {integrationsError ? (
          <p className="mt-4 text-sm text-ember-500">{integrationsError}</p>
        ) : null}

        {form.toolMode === "explicit" ? (
          <div className="mt-4 space-y-4">
            {form.tools.map((tool, index) => {
              const operations = tool.pluginName
                ? operationsByPlugin[tool.pluginName] ?? EMPTY_OPERATIONS
                : EMPTY_OPERATIONS;
              const operationsLoading = tool.pluginName
                ? Boolean(operationsLoadingByPlugin[tool.pluginName])
                : false;
              const operationsError = tool.pluginName
                ? operationErrorsByPlugin[tool.pluginName] ?? null
                : null;
              return (
                <div
                  key={index}
                  className="rounded-md border border-alpha bg-base-100 p-4 dark:bg-surface"
                >
                  <div className="grid gap-3 sm:grid-cols-2">
                    <label className="space-y-2 text-sm">
                      <span className="text-muted">Plugin</span>
                      <select
                        value={tool.pluginName}
                        onChange={(event) => {
                          const pluginName = event.target.value;
                          updateTool(index, { pluginName, operation: "" });
                          void ensureOperationsLoaded(pluginName);
                        }}
                        className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
                      >
                        <option value="">Select plugin</option>
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
                        value={tool.operation}
                        onChange={(event) =>
                          updateTool(index, { operation: event.target.value })
                        }
                        disabled={!tool.pluginName || operationsLoading}
                        className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong disabled:cursor-not-allowed disabled:opacity-60 dark:bg-surface"
                      >
                        <option value="">
                          {operationsLoading ? "Loading operations..." : "Select operation"}
                        </option>
                        {operations.map((operation) => (
                          <option key={operation.id} value={operation.id}>
                            {operation.title || operation.id}
                          </option>
                        ))}
                      </select>
                    </label>
                  </div>

                  {operationsError ? (
                    <p className="mt-3 text-sm text-ember-500">{operationsError}</p>
                  ) : null}

                  <div className="mt-3 grid gap-3 sm:grid-cols-2">
                    <label className="space-y-2 text-sm">
                      <span className="text-muted">Connection</span>
                      <input
                        value={tool.connection}
                        onChange={(event) =>
                          updateTool(index, { connection: event.target.value })
                        }
                        className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
                      />
                    </label>
                    <label className="space-y-2 text-sm">
                      <span className="text-muted">Instance</span>
                      <input
                        value={tool.instance}
                        onChange={(event) =>
                          updateTool(index, { instance: event.target.value })
                        }
                        className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
                      />
                    </label>
                  </div>

                  <div className="mt-3 grid gap-3 sm:grid-cols-2">
                    <label className="space-y-2 text-sm">
                      <span className="text-muted">Tool title</span>
                      <input
                        value={tool.title}
                        onChange={(event) =>
                          updateTool(index, { title: event.target.value })
                        }
                        className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
                      />
                    </label>
                    <label className="space-y-2 text-sm">
                      <span className="text-muted">Tool description</span>
                      <input
                        value={tool.description}
                        onChange={(event) =>
                          updateTool(index, { description: event.target.value })
                        }
                        className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
                      />
                    </label>
                  </div>

                  <div className="mt-4 flex justify-end">
                    <button
                      type="button"
                      onClick={() => removeTool(index)}
                      className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
                    >
                      Remove tool
                    </button>
                  </div>
                </div>
              );
            })}

            <button
              type="button"
              onClick={addTool}
              className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
            >
              Add tool
            </button>
          </div>
        ) : null}
      </section>

      <div className="grid gap-4">
        <JsonTextarea
          label="Response schema JSON"
          value={form.responseSchemaJSON}
          onChange={(value) =>
            setForm((current) => ({ ...current, responseSchemaJSON: value }))
          }
        />
        <JsonTextarea
          label="Metadata JSON"
          value={form.metadataJSON}
          onChange={(value) => setForm((current) => ({ ...current, metadataJSON: value }))}
        />
        <JsonTextarea
          label="Provider options JSON"
          value={form.providerOptionsJSON}
          onChange={(value) =>
            setForm((current) => ({ ...current, providerOptionsJSON: value }))
          }
        />
      </div>

      <button
        type="submit"
        disabled={submitting}
        className="w-full rounded-md bg-primary px-4 py-2 text-sm font-medium text-background transition-opacity duration-150 hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
      >
        {submitting ? "Starting..." : "Start run"}
      </button>
    </form>
  );
}

function AgentRunDetails({ run }: { run: AgentRun }) {
  return (
    <div className="mt-6 space-y-6">
      <div className="grid gap-4 sm:grid-cols-2">
        <DetailItem label="Provider" value={run.provider || "-"} />
        <DetailItem label="Model" value={run.model || "-"} />
        <DetailItem label="Session" value={run.sessionRef || "-"} />
        <DetailItem label="Execution" value={run.executionRef || "-"} />
        <DetailItem label="Created" value={formatDate(run.createdAt)} />
        <DetailItem label="Started" value={formatDate(run.startedAt)} />
        <DetailItem label="Completed" value={formatDate(run.completedAt)} />
        <DetailItem
          label="Actor"
          value={run.createdBy?.displayName || run.createdBy?.subjectId || "-"}
        />
      </div>

      <section>
        <h3 className="text-xs font-medium uppercase tracking-[0.18em] text-faint">
          Messages
        </h3>
        <div className="mt-3 space-y-3">
          {run.messages?.length ? (
            run.messages.map((message, index) => (
              <div
                key={`${message.role}-${index}`}
                className="rounded-md border border-alpha bg-background/65 p-4 dark:bg-background/20"
              >
                <p className="text-xs font-medium uppercase tracking-[0.16em] text-faint">
                  {message.role || "message"}
                </p>
                <p className="mt-2 whitespace-pre-wrap break-words text-sm text-primary">
                  {message.text || "-"}
                </p>
              </div>
            ))
          ) : (
            <p className="rounded-md border border-alpha bg-background/65 p-4 text-sm text-faint dark:bg-background/20">
              No messages captured.
            </p>
          )}
        </div>
      </section>

      <section>
        <h3 className="text-xs font-medium uppercase tracking-[0.18em] text-faint">
          Output
        </h3>
        <div className="mt-3 rounded-md border border-alpha bg-background/65 p-4 dark:bg-background/20">
          {run.statusMessage ? (
            <p className="mb-3 text-sm text-muted">{run.statusMessage}</p>
          ) : null}
          {run.outputText ? (
            <pre className="whitespace-pre-wrap break-words text-sm text-primary">
              {run.outputText}
            </pre>
          ) : (
            <p className="text-sm text-faint">No output text captured.</p>
          )}
          {run.structuredOutput ? (
            <pre className="mt-4 overflow-x-auto rounded-md border border-alpha bg-base-100 p-3 text-xs text-primary dark:bg-surface">
              {JSON.stringify(run.structuredOutput, null, 2)}
            </pre>
          ) : null}
        </div>
      </section>
    </div>
  );
}

function JsonTextarea({
  label,
  value,
  onChange,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
}) {
  return (
    <label className="block space-y-2 text-sm">
      <span className="text-muted">{label}</span>
      <textarea
        value={value}
        onChange={(event) => onChange(event.target.value)}
        rows={3}
        spellCheck={false}
        className="w-full resize-y rounded-md border border-alpha bg-base-100 px-3 py-2 font-mono text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
      />
    </label>
  );
}

function DetailItem({ label, value }: { label: string; value?: string }) {
  return (
    <div>
      <p className="text-xs uppercase tracking-[0.16em] text-faint">{label}</p>
      <p className="mt-1 break-words text-sm text-primary">{value || "-"}</p>
    </div>
  );
}

function defaultAgentRunForm(): AgentRunFormState {
  return {
    provider: "",
    model: "",
    systemPrompt: "",
    userPrompt: "",
    sessionRef: "",
    idempotencyKey: "",
    responseSchemaJSON: "",
    metadataJSON: "",
    providerOptionsJSON: "",
    toolMode: "none",
    tools: [emptyAgentToolForm()],
  };
}

function emptyAgentToolForm(): AgentToolForm {
  return {
    pluginName: "",
    operation: "",
    connection: "",
    instance: "",
    title: "",
    description: "",
  };
}

function agentRunFormToCreate(form: AgentRunFormState): AgentRunCreate {
  const userText = form.userPrompt.trim();
  if (!userText) {
    throw new Error("User message is required");
  }

  const messages = [];
  const systemText = form.systemPrompt.trim();
  if (systemText) {
    messages.push({ role: "system", text: systemText });
  }
  messages.push({ role: "user", text: userText });

  const body: AgentRunCreate = { messages };
  assignTrimmed(body, "provider", form.provider);
  assignTrimmed(body, "model", form.model);
  assignTrimmed(body, "sessionRef", form.sessionRef);
  assignTrimmed(body, "idempotencyKey", form.idempotencyKey);

  const responseSchema = parseOptionalObject(form.responseSchemaJSON, "Response schema");
  const metadata = parseOptionalObject(form.metadataJSON, "Metadata");
  const providerOptions = parseOptionalObject(form.providerOptionsJSON, "Provider options");
  if (responseSchema) body.responseSchema = responseSchema;
  if (metadata) body.metadata = metadata;
  if (providerOptions) body.providerOptions = providerOptions;

  if (form.toolMode === "explicit") {
    const toolRefs = agentToolRefsFromForm(form.tools);
    if (toolRefs.length === 0) {
      throw new Error("At least one plugin operation is required for explicit tools");
    }
    body.toolSource = "explicit";
    body.toolRefs = toolRefs;
  }

  return body;
}

function agentToolRefsFromForm(tools: AgentToolForm[]): AgentToolRef[] {
  return tools
    .map((tool) => stripToolForm(tool))
    .filter((tool) => tool.pluginName || tool.operation)
    .map((tool) => {
      if (!tool.pluginName) {
        throw new Error("Tool plugin is required");
      }
      if (!tool.operation) {
        throw new Error("Tool operation is required");
      }
      const ref: AgentToolRef = {
        pluginName: tool.pluginName,
        operation: tool.operation,
      };
      if (tool.connection) ref.connection = tool.connection;
      if (tool.instance) ref.instance = tool.instance;
      if (tool.title) ref.title = tool.title;
      if (tool.description) ref.description = tool.description;
      return ref;
    });
}

function stripToolForm(tool: AgentToolForm): AgentToolForm {
  return {
    pluginName: tool.pluginName.trim(),
    operation: tool.operation.trim(),
    connection: tool.connection.trim(),
    instance: tool.instance.trim(),
    title: tool.title.trim(),
    description: tool.description.trim(),
  };
}

function parseOptionalObject(value: string, label: string): Record<string, unknown> | undefined {
  const trimmed = value.trim();
  if (!trimmed) return undefined;

  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    throw new Error(`${label} must be valid JSON`);
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`${label} must be a JSON object`);
  }
  return parsed as Record<string, unknown>;
}

function assignTrimmed(
  target: AgentRunCreate,
  key: "provider" | "model" | "sessionRef" | "idempotencyKey",
  value: string,
) {
  const trimmed = value.trim();
  if (trimmed) {
    target[key] = trimmed;
  }
}

function filterRuns(runs: AgentRun[], query: string, status: string): AgentRun[] {
  const normalizedQuery = query.trim().toLowerCase();
  return runs.filter((run) => {
    if (status !== "all" && run.status !== status) {
      return false;
    }
    if (!normalizedQuery) {
      return true;
    }
    const haystack = [
      run.id,
      run.provider,
      run.model,
      run.status,
      run.sessionRef,
      run.outputText,
      run.statusMessage,
      ...(run.messages?.map((message) => `${message.role} ${message.text}`) ?? []),
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
    return haystack.includes(normalizedQuery);
  });
}

function agentRunLabel(run: AgentRun): string {
  const lastUserMessage = [...(run.messages ?? [])]
    .reverse()
    .find((message) => message.role === "user" && message.text.trim());
  if (lastUserMessage) {
    return truncate(lastUserMessage.text.replace(/\s+/g, " "), 64);
  }
  if (run.outputText) {
    return truncate(run.outputText.replace(/\s+/g, " "), 64);
  }
  return run.model || run.id;
}

function runStatusClassName(status?: string): string {
  const base =
    "inline-flex shrink-0 rounded-full px-2 py-0.5 text-[11px] font-medium uppercase tracking-[0.14em]";
  switch (status) {
    case "succeeded":
      return `${base} bg-grove-500/10 text-grove-700 dark:text-grove-200`;
    case "failed":
      return `${base} bg-ember-500/10 text-ember-500`;
    case "canceled":
      return `${base} bg-alpha-5 text-muted`;
    case "pending":
    case "running":
      return `${base} bg-sky-500/10 text-sky-600 dark:text-sky-200`;
    default:
      return `${base} bg-alpha-5 text-faint`;
  }
}

function isCancelableStatus(status?: string): boolean {
  return status === "pending" || status === "running";
}

function sortOperations(operations: IntegrationOperation[]): IntegrationOperation[] {
  return operations
    .filter((operation) => operation.visible !== false)
    .slice()
    .sort((left, right) =>
      (left.title || left.id).localeCompare(right.title || right.id, undefined, {
        sensitivity: "base",
      }),
    );
}

function integrationLabel(integration: Integration): string {
  return integration.displayName || integration.name;
}

function formatDate(value?: string): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function truncate(value: string, maxLength: number): string {
  return value.length > maxLength ? `${value.slice(0, maxLength - 1)}...` : value;
}

function errorMessage(reason: unknown, fallback: string): string {
  if (reason instanceof Error && reason.message) {
    return reason.message;
  }
  if (typeof reason === "string" && reason) {
    return reason;
  }
  return fallback;
}
