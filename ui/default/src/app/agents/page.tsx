"use client";

import {
  useCallback,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { useRouter } from "next/navigation";
import type {
  AgentInteraction,
  AgentProvider,
  AgentSession,
  AgentToolRef,
  AgentTurn,
  AgentTurnCreate,
  AgentTurnEvent,
  AgentTurnEventStream,
  Integration,
  IntegrationOperation,
} from "@/lib/api";
import {
  cancelAgentTurn,
  createAgentSession,
  createAgentTurn,
  getAgentInteractions,
  getAgentProviders,
  getAgentSession,
  getAgentSessions,
  getAgentTurn,
  getAgentTurns,
  getAllAgentTurnEvents,
  getIntegrationOperations,
  getIntegrations,
  isAPIErrorStatus,
  openAgentTurnEventStream,
  resolveAgentInteraction,
} from "@/lib/api";
import {
  appendInteraction,
  appendTurnMessages,
  applyTurnEvent,
  createTranscriptState,
  finishTurnSnapshot,
  isTurnLive,
  type TranscriptItem,
  type TranscriptState,
} from "@/lib/agentTranscript";
import AuthGuard from "@/components/AuthGuard";
import Nav from "@/components/Nav";

type AgentToolMode = "none" | "selected";
type InteractionDrafts = Record<string, string>;

interface AgentToolForm {
  plugin: string;
  operation: string;
  connection: string;
  instance: string;
  title: string;
  description: string;
}

interface AgentComposerState {
  provider: string;
  model: string;
  clientRef: string;
  systemPrompt: string;
  userPrompt: string;
  idempotencyKey: string;
  responseSchemaJSON: string;
  metadataJSON: string;
  modelOptionsJSON: string;
  toolMode: AgentToolMode;
  tools: AgentToolForm[];
}

const EMPTY_OPERATIONS: IntegrationOperation[] = [];
const AGENT_BOOTSTRAP_TIMEOUT_MS = 15_000;

export default function AgentsPage() {
  const router = useRouter();
  const streamRef = useRef<AgentTurnEventStream | null>(null);
  const lastSeqRef = useRef<Record<string, number>>({});
  const blockedTurnRef = useRef<string | null>(null);

  const [providers, setProviders] = useState<AgentProvider[]>([]);
  const [sessions, setSessions] = useState<AgentSession[]>([]);
  const [selectedSessionID, setSelectedSessionID] = useState<string | null>(
    null,
  );
  const [querySelection, setQuerySelection] = useState<{
    session: string | null;
    turn: string | null;
  }>({ session: null, turn: null });
  const [selectedSession, setSelectedSession] = useState<AgentSession | null>(
    null,
  );
  const [turns, setTurns] = useState<AgentTurn[]>([]);
  const [selectedTurnID, setSelectedTurnID] = useState<string | null>(null);
  const [interactions, setInteractions] = useState<AgentInteraction[]>([]);
  const [transcript, setTranscript] = useState<TranscriptState>(() =>
    createTranscriptState(),
  );
  const [transcriptReady, setTranscriptReady] = useState(false);
  const [integrations, setIntegrations] = useState<Integration[]>([]);

  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [streamNonce, setStreamNonce] = useState(0);
  const [sessionsError, setSessionsError] = useState<string | null>(null);
  const [providersError, setProvidersError] = useState<string | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [integrationsError, setIntegrationsError] = useState<string | null>(
    null,
  );
  const [actionError, setActionError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const [query, setQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const deferredQuery = useDeferredValue(query);

  const [composer, setComposer] = useState<AgentComposerState>(() =>
    defaultComposer(),
  );
  const [composerError, setComposerError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [cancelingTurnID, setCancelingTurnID] = useState<string | null>(null);
  const [interactionDrafts, setInteractionDrafts] = useState<InteractionDrafts>(
    {},
  );
  const [resolvingInteractionID, setResolvingInteractionID] = useState<
    string | null
  >(null);

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
    function readQuerySelection() {
      const params = new URLSearchParams(window.location.search);
      setQuerySelection({
        session: params.get("session"),
        turn: params.get("turn"),
      });
    }
    readQuerySelection();
    window.addEventListener("popstate", readQuerySelection);
    return () => window.removeEventListener("popstate", readQuerySelection);
  }, []);

  useEffect(() => {
    let active = true;
    const initial = refreshNonce === 0;
    if (initial) setLoading(true);
    else setRefreshing(true);

    withLoadTimeout(getAgentProviders(), "Loading agent providers")
      .then((value) => {
        if (!active) return;
        setProviders(value);
        setProvidersError(null);
      })
      .catch((err) => {
        if (!active) return;
        setProvidersError(errorMessage(err, "Failed to load agent providers"));
      });

    withLoadTimeout(getIntegrations(), "Loading plugins")
      .then((value) => {
        if (!active) return;
        setIntegrations(value);
        setIntegrationsError(null);
      })
      .catch((err) => {
        if (!active) return;
        setIntegrationsError(errorMessage(err, "Failed to load plugins"));
      });

    withLoadTimeout(
      getAgentSessions({ view: "summary", limit: 100 }),
      "Loading agent sessions",
    )
      .then((value) => {
        if (!active) return;
        setSessions(value);
        setSessionsError(null);
      })
      .catch((err) => {
        if (!active) return;
        if (isAPIErrorStatus(err, 412)) {
          router.replace("/");
          return;
        }
        setSessionsError(errorMessage(err, "Failed to load agent sessions"));
      })
      .finally(() => {
        if (!active) return;
        setLoading(false);
        setRefreshing(false);
      });

    return () => {
      active = false;
    };
  }, [refreshNonce, router]);

  useEffect(() => {
    setSelectedSessionID((current) => {
      if (querySelection.session) return querySelection.session;
      if (current && sessions.some((session) => session.id === current)) {
        return current;
      }
      return sessions[0]?.id ?? null;
    });
  }, [querySelection.session, sessions]);

  const loadSelectedSession = useCallback(
    async (sessionID: string, requestedTurnID?: string | null) => {
      setDetailError(null);
      const [session, nextTurns] = await Promise.all([
        getAgentSession(sessionID),
        getTurnsIncludingTurn(sessionID, requestedTurnID),
      ]);
      setSelectedSession(session);
      setTurns(nextTurns);
      setSelectedTurnID((current) =>
        chooseSelectedTurnID(nextTurns, requestedTurnID ?? current),
      );
    },
    [],
  );

  useEffect(() => {
    if (!selectedSessionID) {
      setSelectedSession(null);
      setTurns([]);
      setSelectedTurnID(null);
      setInteractions([]);
      setTranscript(createTranscriptState());
      setTranscriptReady(true);
      return;
    }

    let active = true;
    setTranscriptReady(false);
    loadSelectedSession(selectedSessionID, querySelection.turn).catch((err) => {
      if (!active) return;
      setDetailError(errorMessage(err, "Failed to load agent session"));
      setTurns([]);
      setTranscriptReady(true);
    });
    return () => {
      active = false;
    };
  }, [loadSelectedSession, querySelection.turn, selectedSessionID, refreshNonce]);

  useEffect(() => {
    let active = true;

    async function replayTurns() {
      let state = createTranscriptState();
      const sortedTurns = sortTurnsAscending(turns);
      const eventResults = await Promise.all(
        sortedTurns.map((turn) => getAllAgentTurnEvents(turn.id, { limit: 100 })),
      );
      if (!active) return;
      for (let index = 0; index < sortedTurns.length; index += 1) {
        const turn = sortedTurns[index];
        const result = eventResults[index];
        state = appendTurnMessages(state, turn);
        for (const event of result.events) {
          state = applyTurnEvent(state, event);
        }
        state = finishTurnSnapshot(state, turn);
      }
      if (!active) return;
      lastSeqRef.current = state.lastSeqByTurnId;
      setTranscript(state);
      setTranscriptReady(true);
    }

    if (!selectedSessionID) {
      setTranscriptReady(true);
      return () => {
        active = false;
      };
    }

    setTranscriptReady(false);
    replayTurns().catch((err) => {
      if (!active) return;
      setDetailError(errorMessage(err, "Failed to load agent transcript"));
      setTranscript(createTranscriptState());
      setTranscriptReady(true);
    });
    return () => {
      active = false;
    };
  }, [selectedSessionID, turns]);

  const selectedTurn = useMemo(
    () => turns.find((turn) => turn.id === selectedTurnID) ?? null,
    [selectedTurnID, turns],
  );

  const loadInteractions = useCallback(async (turnID: string) => {
    const values = await getAgentInteractions(turnID);
    const pending = values.filter((interaction) => interaction.state === "pending");
    setInteractions(pending);
    setInteractionDrafts((current) => {
      const next = { ...current };
      for (const interaction of pending) {
        if (next[interaction.id] === undefined) {
          next[interaction.id] = interactionDefaultValue(interaction);
        }
      }
      return next;
    });
    setTranscript((current) => {
      let next = current;
      for (const interaction of pending) {
        next = appendInteraction(next, interaction);
      }
      return next;
    });
  }, []);

  useEffect(() => {
    if (!selectedTurn || selectedTurn.status !== "waiting_for_input") {
      setInteractions([]);
      return;
    }
    loadInteractions(selectedTurn.id).catch((err) => {
      setActionError(errorMessage(err, "Failed to load interactions"));
    });
  }, [loadInteractions, selectedTurn]);

  useEffect(() => {
    const previousStream = streamRef.current;
    streamRef.current = null;
    previousStream?.close();

    if (!selectedTurn || !transcriptReady || !isTurnLive(selectedTurn.status)) {
      return;
    }
    if (
      selectedTurn.status === "waiting_for_input" &&
      blockedTurnRef.current === selectedTurn.id
    ) {
      return;
    }

    const turnID = selectedTurn.id;
    let stream: AgentTurnEventStream;
    stream = openAgentTurnEventStream(turnID, {
      after: lastSeqRef.current[turnID] ?? 0,
      until: "blocked_or_terminal",
      onEvent(event) {
        setTranscript((current) => {
          const next = applyTurnEvent(current, event);
          lastSeqRef.current = next.lastSeqByTurnId;
          return next;
        });
      },
      onError(error) {
        if (error.message !== "Agent event stream closed") {
          setActionError(error.message);
        }
      },
      onClose() {
        if (streamRef.current !== stream) {
          return;
        }
        getTurnsIncludingTurn(selectedTurn.sessionId, turnID)
          .then((nextTurns) => {
            setTurns(nextTurns);
            const latest = nextTurns.find((turn) => turn.id === turnID);
            if (latest?.status === "waiting_for_input") {
              blockedTurnRef.current = turnID;
              return loadInteractions(turnID);
            }
            setInteractions([]);
            return undefined;
          })
          .catch((err) => {
            setActionError(errorMessage(err, "Failed to refresh agent turn"));
          });
      },
    });
    streamRef.current = stream;

    return () => {
      if (streamRef.current === stream) {
        streamRef.current = null;
      }
      stream.close();
    };
  }, [loadInteractions, selectedTurn, streamNonce, transcriptReady]);

  useEffect(() => {
    const params = new URLSearchParams();
    if (selectedSessionID) params.set("session", selectedSessionID);
    if (selectedTurnID) params.set("turn", selectedTurnID);
    const next = params.toString() ? `/agents?${params}` : "/agents";
    if (window.location.pathname + window.location.search !== next) {
      window.history.replaceState(null, "", next);
    }
  }, [selectedSessionID, selectedTurnID]);

  async function ensureOperationsLoaded(plugin: string): Promise<void> {
    const normalized = plugin.trim();
    if (!normalized) return;
    if (operationsByPlugin[normalized] || operationsLoadingByPlugin[normalized]) {
      return;
    }

    setOperationsLoadingByPlugin((current) => ({
      ...current,
      [normalized]: true,
    }));
    setOperationErrorsByPlugin((current) => ({
      ...current,
      [normalized]: undefined,
    }));

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
      setOperationsLoadingByPlugin((current) => ({
        ...current,
        [normalized]: false,
      }));
    }
  }

  const agentIntegrations = useMemo(
    () =>
      integrations
        .filter((integration) => !integration.mountedPath)
        .slice()
        .sort((left, right) =>
          integrationLabel(left).localeCompare(
            integrationLabel(right),
            undefined,
            { sensitivity: "base" },
          ),
        ),
    [integrations],
  );

  const filteredSessions = useMemo(
    () => filterSessions(sessions, deferredQuery, statusFilter),
    [deferredQuery, sessions, statusFilter],
  );

  const selectedProvider = providerForSessionOrComposer(
    providers,
    selectedSession,
    composer.provider,
  );
  const providerSupportsSelectedTools = providerSupportsMCPCatalog(
    selectedProvider,
    providers.length > 0,
  );

  function selectSession(sessionID: string | null, turnID?: string | null) {
    setActionError(null);
    setNotice(null);
    setSelectedSessionID(sessionID);
    setSelectedTurnID(turnID ?? null);
  }

  function selectTurn(turnID: string) {
    setSelectedTurnID(turnID);
    setActionError(null);
  }

  async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setComposerError(null);
    setActionError(null);
    setNotice(null);

    let turnBody: AgentTurnCreate;
    try {
      turnBody = composerToTurnCreate(composer, providerSupportsSelectedTools);
    } catch (err) {
      setComposerError(errorMessage(err, "Invalid turn request"));
      return;
    }

    setSubmitting(true);
    try {
      let session = selectedSession;
      if (!session) {
        session = await createAgentSession({
          provider: trimmedOrUndefined(composer.provider),
          model: trimmedOrUndefined(composer.model),
          clientRef: trimmedOrUndefined(composer.clientRef),
          metadata: parseOptionalObject(composer.metadataJSON, "Metadata"),
          modelOptions: parseOptionalObject(
            composer.modelOptionsJSON,
            "Model options",
          ),
          idempotencyKey: trimmedOrUndefined(composer.idempotencyKey),
        });
        setSessions((current) => [session as AgentSession, ...current]);
        setSelectedSession(session);
        setSelectedSessionID(session.id);
      }

      const created = await createAgentTurn(session.id, turnBody);
      setTurns((current) => [created, ...current]);
      setSelectedTurnID(created.id);
      setComposer((current) => ({ ...defaultComposer(), provider: current.provider }));
      setNotice("Agent turn started.");
      setStreamNonce((value) => value + 1);
    } catch (err) {
      setComposerError(errorMessage(err, "Failed to start agent turn"));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleCancelTurn(turn: AgentTurn) {
    if (!isTurnLive(turn.status)) return;
    setCancelingTurnID(turn.id);
    setActionError(null);
    try {
      const updated = await cancelAgentTurn(turn.id, "Turn canceled.");
      blockedTurnRef.current = null;
      setTurns((current) =>
        current.map((item) => (item.id === updated.id ? updated : item)),
      );
      setInteractions([]);
      setNotice("Agent turn canceled.");
    } catch (err) {
      setActionError(errorMessage(err, "Failed to cancel agent turn"));
    } finally {
      setCancelingTurnID(null);
    }
  }

  async function handleResolveInteraction(
    interaction: AgentInteraction,
    resolution: Record<string, unknown>,
  ) {
    if (!selectedTurn) return;
    setResolvingInteractionID(interaction.id);
    setActionError(null);
    try {
      await resolveAgentInteraction(selectedTurn.id, interaction.id, resolution);
      blockedTurnRef.current = null;
      setInteractions((current) =>
        current.filter((item) => item.id !== interaction.id),
      );
      const nextTurns = await getTurnsIncludingTurn(
        selectedTurn.sessionId,
        selectedTurn.id,
      );
      setTurns(nextTurns);
      setStreamNonce((value) => value + 1);
      setNotice("Interaction resolved.");
    } catch (err) {
      setActionError(errorMessage(err, "Failed to resolve interaction"));
    } finally {
      setResolvingInteractionID(null);
    }
  }

  return (
    <AuthGuard>
      <div className="min-h-screen">
        <Nav />
        <main className="mx-auto max-w-7xl px-6 py-10">
          <div className="flex flex-col gap-5 md:flex-row md:items-end md:justify-between">
            <div>
              <span className="label-text">Orchestration</span>
              <h1 className="mt-2 text-2xl font-heading font-bold text-primary">
                Agent Sessions
              </h1>
              <p className="mt-2 max-w-3xl text-sm text-muted">
                Inspect live turns, tool activity, public event frames, and
                pending agent interactions.
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                onClick={() => selectSession(null)}
                className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 dark:bg-surface"
              >
                New session
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
            <div className="mt-8 grid gap-5 lg:grid-cols-[18rem_minmax(0,1fr)_20rem]">
              <SessionSidebar
                sessions={filteredSessions}
                selectedSessionID={selectedSessionID}
                query={query}
                statusFilter={statusFilter}
                error={sessionsError}
                setQuery={setQuery}
                setStatusFilter={setStatusFilter}
                onSelect={selectSession}
              />

              <section className="min-w-0 rounded-lg border border-alpha bg-base-100 dark:bg-surface">
                <ConsoleHeader
                  session={selectedSession}
                  turn={selectedTurn}
                  turns={turns}
                  onSelectTurn={selectTurn}
                  onCancelTurn={handleCancelTurn}
                  cancelingTurnID={cancelingTurnID}
                />

                <div className="border-t border-alpha px-5 py-5">
                  {notice ? (
                    <p className="mb-4 text-sm text-grove-700 dark:text-grove-200">
                      {notice}
                    </p>
                  ) : null}
                  {actionError ? (
                    <p className="mb-4 text-sm text-ember-500">{actionError}</p>
                  ) : null}
                  {detailError ? (
                    <p className="mb-4 text-sm text-ember-500">{detailError}</p>
                  ) : null}

                  <TranscriptView
                    loading={!transcriptReady}
                    items={transcript.items}
                    emptyMessage={
                      selectedSession
                        ? "No transcript events captured yet."
                        : "Create a session or select one from the list."
                    }
                  />

                  <InteractionPanel
                    interactions={interactions}
                    drafts={interactionDrafts}
                    resolvingID={resolvingInteractionID}
                    setDrafts={setInteractionDrafts}
                    onResolve={handleResolveInteraction}
                  />

                  <AgentComposer
                    composer={composer}
                    selectedSession={selectedSession}
                    providers={providers}
                    providersError={providersError}
                    integrations={agentIntegrations}
                    integrationsError={integrationsError}
                    operationsByPlugin={operationsByPlugin}
                    operationsLoadingByPlugin={operationsLoadingByPlugin}
                    operationErrorsByPlugin={operationErrorsByPlugin}
                    formError={composerError}
                    submitting={submitting}
                    providerSupportsSelectedTools={providerSupportsSelectedTools}
                    setComposer={setComposer}
                    onSubmit={handleSubmit}
                    ensureOperationsLoaded={ensureOperationsLoaded}
                  />
                </div>
              </section>

              <EventInspector
                providers={providers}
                session={selectedSession}
                turn={selectedTurn}
                events={transcript.rawPublicEvents}
              />
            </div>
          )}
        </main>
      </div>
    </AuthGuard>
  );
}

function SessionSidebar({
  sessions,
  selectedSessionID,
  query,
  statusFilter,
  error,
  setQuery,
  setStatusFilter,
  onSelect,
}: {
  sessions: AgentSession[];
  selectedSessionID: string | null;
  query: string;
  statusFilter: string;
  error: string | null;
  setQuery: (value: string) => void;
  setStatusFilter: (value: string) => void;
  onSelect: (sessionID: string) => void;
}) {
  return (
    <aside className="rounded-lg border border-alpha bg-base-100 dark:bg-surface">
      <div className="border-b border-alpha p-4">
        <h2 className="text-sm font-medium text-primary">Sessions</h2>
        <div className="mt-3 space-y-2">
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search sessions"
            className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong dark:bg-surface"
          />
          <select
            value={statusFilter}
            onChange={(event) => setStatusFilter(event.target.value)}
            className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
          >
            <option value="all">All states</option>
            <option value="active">Active</option>
            <option value="archived">Archived</option>
          </select>
        </div>
      </div>

      {error ? (
        <p className="p-4 text-sm text-ember-500">{error}</p>
      ) : sessions.length === 0 ? (
        <p className="p-4 text-sm text-faint">No agent sessions yet.</p>
      ) : (
        <div className="divide-y divide-alpha">
          {sessions.map((session) => {
            const active = session.id === selectedSessionID;
            return (
              <button
                key={session.id}
                type="button"
                onClick={() => onSelect(session.id)}
                className={`flex w-full flex-col gap-1 px-4 py-3 text-left transition-colors duration-150 ${
                  active ? "bg-alpha-5" : "hover:bg-alpha-5"
                }`}
              >
                <span className="truncate text-sm font-medium text-primary">
                  {session.clientRef || session.id}
                </span>
                <span className="truncate text-xs text-muted">
                  {session.provider || "default"} /{" "}
                  {session.model || "default model"}
                </span>
                <span className="text-xs text-faint">
                  {formatDate(session.lastTurnAt || session.updatedAt)}
                </span>
              </button>
            );
          })}
        </div>
      )}
    </aside>
  );
}

function ConsoleHeader({
  session,
  turn,
  turns,
  onSelectTurn,
  onCancelTurn,
  cancelingTurnID,
}: {
  session: AgentSession | null;
  turn: AgentTurn | null;
  turns: AgentTurn[];
  onSelectTurn: (turnID: string) => void;
  onCancelTurn: (turn: AgentTurn) => void | Promise<void>;
  cancelingTurnID: string | null;
}) {
  return (
    <div className="px-5 py-4">
      <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
        <div className="min-w-0">
          <h2 className="truncate text-sm font-medium text-primary">
            {session?.clientRef || session?.id || "New agent session"}
          </h2>
          <p className="mt-1 text-xs text-faint">
            {session
              ? `${session.provider || "default"} / ${session.model || "default model"}`
              : "The first message will create a cloud session."}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          {turn ? <span className={statusClassName(turn.status)}>{turn.status}</span> : null}
          {turn && isTurnLive(turn.status) ? (
            <button
              type="button"
              onClick={() => void onCancelTurn(turn)}
              disabled={cancelingTurnID === turn.id}
              className="rounded-md bg-ember-500 px-3 py-2 text-sm font-medium text-white transition-opacity duration-150 hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
            >
              {cancelingTurnID === turn.id ? "Canceling..." : "Cancel turn"}
            </button>
          ) : null}
        </div>
      </div>

      {turns.length ? (
        <div className="mt-4 flex gap-2 overflow-x-auto pb-1">
          {turns.map((item) => (
            <button
              key={item.id}
              type="button"
              onClick={() => onSelectTurn(item.id)}
              className={`shrink-0 rounded-md border px-3 py-2 text-left text-xs transition-colors duration-150 ${
                item.id === turn?.id
                  ? "border-alpha-strong bg-alpha-5 text-primary"
                  : "border-alpha text-muted hover:bg-alpha-5"
              }`}
            >
              <span className="block max-w-40 truncate">{turnLabel(item)}</span>
              <span className="mt-1 block text-faint">{formatDate(item.createdAt)}</span>
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function TranscriptView({
  loading,
  items,
  emptyMessage,
}: {
  loading: boolean;
  items: TranscriptItem[];
  emptyMessage: string;
}) {
  if (loading) {
    return <p className="text-sm text-faint">Loading transcript...</p>;
  }
  if (items.length === 0) {
    return (
      <div className="rounded-md border border-alpha bg-background/65 p-5 text-sm text-faint dark:bg-background/20">
        {emptyMessage}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {items.map((item) => (
        <TranscriptBubble key={item.id} item={item} />
      ))}
    </div>
  );
}

function TranscriptBubble({ item }: { item: TranscriptItem }) {
  const alignClass =
    item.kind === "user"
      ? "justify-end"
      : item.kind === "system" || item.kind === "event"
        ? "justify-center"
        : "justify-start";
  const bubbleClass =
    item.kind === "user"
      ? "max-w-[min(42rem,86%)] border-primary bg-primary text-background"
      : item.kind === "assistant"
        ? "max-w-[min(42rem,86%)] border-alpha bg-base-100 text-primary dark:bg-surface"
        : item.kind === "error"
          ? "max-w-[min(42rem,86%)] border-ember-500/30 bg-ember-500/10 text-primary"
          : item.kind === "tool" || item.kind === "interaction"
            ? "max-w-[min(36rem,92%)] border-alpha bg-alpha-5 text-primary"
            : "max-w-[min(34rem,92%)] border-alpha bg-background/65 text-muted dark:bg-background/20";
  const labelClass =
    item.kind === "user" ? "text-background/70" : "text-faint";

  return (
    <div className={`flex ${alignClass}`}>
      <article className={`rounded-lg border px-4 py-3 ${bubbleClass}`}>
        <div className="flex flex-wrap items-center justify-between gap-2">
          <p className={`text-xs font-medium uppercase tracking-[0.14em] ${labelClass}`}>
            {item.title}
          </p>
          {item.streaming ? (
            <span className="rounded-full bg-sky-500/10 px-2 py-0.5 text-[11px] font-medium uppercase tracking-[0.14em] text-sky-600 dark:text-sky-200">
              Streaming
            </span>
          ) : null}
        </div>
        <pre className="mt-2 whitespace-pre-wrap break-words font-sans text-sm leading-6">
          {item.text}
        </pre>
      </article>
    </div>
  );
}

function InteractionPanel({
  interactions,
  drafts,
  resolvingID,
  setDrafts,
  onResolve,
}: {
  interactions: AgentInteraction[];
  drafts: InteractionDrafts;
  resolvingID: string | null;
  setDrafts: React.Dispatch<React.SetStateAction<InteractionDrafts>>;
  onResolve: (
    interaction: AgentInteraction,
    resolution: Record<string, unknown>,
  ) => void | Promise<void>;
}) {
  if (interactions.length === 0) return null;

  return (
    <section className="mt-5 rounded-md border border-alpha bg-alpha-5 p-4">
      <h3 className="text-sm font-medium text-primary">Waiting For Input</h3>
      <div className="mt-4 space-y-4">
        {interactions.map((interaction) => {
          const resolving = resolvingID === interaction.id;
          if (interaction.type === "approval") {
            return (
              <div key={interaction.id} className="space-y-3">
                <InteractionPrompt interaction={interaction} />
                <div className="flex flex-wrap gap-2">
                  <button
                    type="button"
                    disabled={resolving}
                    onClick={() =>
                      void onResolve(interaction, { approved: true })
                    }
                    className="rounded-md bg-primary px-3 py-2 text-sm font-medium text-background transition-opacity duration-150 hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    Approve
                  </button>
                  <button
                    type="button"
                    disabled={resolving}
                    onClick={() =>
                      void onResolve(interaction, { approved: false })
                    }
                    className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary transition-colors duration-150 hover:bg-alpha-5 disabled:cursor-not-allowed disabled:opacity-60 dark:bg-surface"
                  >
                    Reject
                  </button>
                </div>
              </div>
            );
          }

          if (interaction.type === "clarification" || interaction.type === "input") {
            const required = interaction.request?.required === true;
            const secret = interaction.request?.secret === true;
            return (
              <form
                key={interaction.id}
                className="space-y-3"
                onSubmit={(event) => {
                  event.preventDefault();
                  const response = drafts[interaction.id] || "";
                  if (required && !response.trim()) return;
                  void onResolve(interaction, { response });
                }}
              >
                <InteractionPrompt interaction={interaction} />
                <input
                  type={secret ? "password" : "text"}
                  aria-label={`Response for ${interaction.title || interaction.id}`}
                  value={drafts[interaction.id] || ""}
                  required={required}
                  onChange={(event) =>
                    setDrafts((current) => ({
                      ...current,
                      [interaction.id]: event.target.value,
                    }))
                  }
                  className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
                />
                <button
                  type="submit"
                  disabled={resolving}
                  className="rounded-md bg-primary px-3 py-2 text-sm font-medium text-background transition-opacity duration-150 hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  Submit
                </button>
              </form>
            );
          }

          return (
            <form
              key={interaction.id}
              className="space-y-3"
              onSubmit={(event) => {
                event.preventDefault();
                let resolution: Record<string, unknown>;
                try {
                  resolution = parseOptionalObject(
                    drafts[interaction.id] || "{}",
                    "Resolution",
                  ) ?? {};
                } catch {
                  return;
                }
                void onResolve(interaction, resolution);
              }}
            >
              <InteractionPrompt interaction={interaction} />
              <textarea
                aria-label={`Resolution for ${interaction.title || interaction.id}`}
                value={drafts[interaction.id] || "{}"}
                onChange={(event) =>
                  setDrafts((current) => ({
                    ...current,
                    [interaction.id]: event.target.value,
                  }))
                }
                rows={4}
                className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 font-mono text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
              />
              <button
                type="submit"
                disabled={resolving}
                className="rounded-md bg-primary px-3 py-2 text-sm font-medium text-background transition-opacity duration-150 hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
              >
                Resolve
              </button>
            </form>
          );
        })}
      </div>
    </section>
  );
}

function InteractionPrompt({ interaction }: { interaction: AgentInteraction }) {
  return (
    <div>
      <p className="text-sm font-medium text-primary">
        {interaction.title || interaction.type || "Interaction"}
      </p>
      {interaction.prompt ? (
        <p className="mt-1 whitespace-pre-wrap text-sm text-muted">
          {interaction.prompt}
        </p>
      ) : null}
    </div>
  );
}

function AgentComposer({
  composer,
  selectedSession,
  providers,
  providersError,
  integrations,
  integrationsError,
  operationsByPlugin,
  operationsLoadingByPlugin,
  operationErrorsByPlugin,
  formError,
  submitting,
  providerSupportsSelectedTools,
  setComposer,
  onSubmit,
  ensureOperationsLoaded,
}: {
  composer: AgentComposerState;
  selectedSession: AgentSession | null;
  providers: AgentProvider[];
  providersError: string | null;
  integrations: Integration[];
  integrationsError: string | null;
  operationsByPlugin: Record<string, IntegrationOperation[]>;
  operationsLoadingByPlugin: Record<string, boolean>;
  operationErrorsByPlugin: Record<string, string | undefined>;
  formError: string | null;
  submitting: boolean;
  providerSupportsSelectedTools: boolean;
  setComposer: React.Dispatch<React.SetStateAction<AgentComposerState>>;
  onSubmit: (event: React.FormEvent<HTMLFormElement>) => void | Promise<void>;
  ensureOperationsLoaded: (plugin: string) => Promise<void>;
}) {
  function updateTool(index: number, patch: Partial<AgentToolForm>) {
    setComposer((current) => ({
      ...current,
      tools: current.tools.map((tool, itemIndex) =>
        itemIndex === index ? { ...tool, ...patch } : tool,
      ),
    }));
  }

  function addTool() {
    setComposer((current) => ({
      ...current,
      toolMode: "selected",
      tools: [...current.tools, emptyToolForm()],
    }));
  }

  function removeTool(index: number) {
    setComposer((current) => {
      const tools = current.tools.filter((_, itemIndex) => itemIndex !== index);
      return {
        ...current,
        tools: tools.length ? tools : [emptyToolForm()],
      };
    });
  }

  return (
    <form className="mt-6 border-t border-alpha pt-5" onSubmit={onSubmit}>
      <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
        <div>
          <h3 className="text-sm font-medium text-primary">
            {selectedSession ? "Send Turn" : "Create Session"}
          </h3>
          <p className="mt-1 text-xs text-faint">
            {selectedSession
              ? "Messages are appended to the selected cloud session."
              : "The first message creates a cloud agent session."}
          </p>
        </div>
        <div className="grid min-w-0 gap-3 sm:grid-cols-2 md:w-[28rem]">
          <ProviderField
            providers={providers}
            value={composer.provider}
            disabled={Boolean(selectedSession)}
            onChange={(value) =>
              setComposer((current) => ({ ...current, provider: value }))
            }
          />
          <label className="space-y-2 text-sm">
            <span className="text-muted">
              {selectedSession ? "Model override" : "Model"}
            </span>
            <input
              value={composer.model}
              onChange={(event) =>
                setComposer((current) => ({
                  ...current,
                  model: event.target.value,
                }))
              }
              placeholder={selectedSession?.model || "provider default"}
              className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong disabled:cursor-not-allowed disabled:opacity-60 dark:bg-surface"
            />
          </label>
        </div>
      </div>

      {formError ? <p className="mt-4 text-sm text-ember-500">{formError}</p> : null}
      {providersError ? (
        <p className="mt-4 text-sm text-ember-500">{providersError}</p>
      ) : null}

      <div className="mt-5 space-y-4">
        {!selectedSession ? (
          <label className="block space-y-2 text-sm">
            <span className="text-muted">Client ref</span>
            <input
              value={composer.clientRef}
              onChange={(event) =>
                setComposer((current) => ({
                  ...current,
                  clientRef: event.target.value,
                }))
              }
              className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
            />
          </label>
        ) : null}

        <label className="block space-y-2 text-sm">
          <span className="text-muted">System message</span>
          <textarea
            value={composer.systemPrompt}
            onChange={(event) =>
              setComposer((current) => ({
                ...current,
                systemPrompt: event.target.value,
              }))
            }
            rows={2}
            className="w-full resize-y rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
          />
        </label>

        <label className="block space-y-2 text-sm">
          <span className="text-muted">User message</span>
          <textarea
            value={composer.userPrompt}
            onChange={(event) =>
              setComposer((current) => ({
                ...current,
                userPrompt: event.target.value,
              }))
            }
            rows={4}
            required
            className="w-full resize-y rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
          />
        </label>

        <section className="rounded-md border border-alpha bg-background/65 p-4 dark:bg-background/20">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h4 className="text-sm font-medium text-primary">Tools</h4>
              <p className="mt-1 text-xs text-faint">
                No tools sends an empty toolRefs list.
              </p>
            </div>
            <select
              aria-label="Tools"
              value={composer.toolMode}
              onChange={(event) =>
                setComposer((current) => ({
                  ...current,
                  toolMode: event.target.value as AgentToolMode,
                }))
              }
              className="rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
            >
              <option value="none">No tools</option>
              <option value="selected">Selected tools</option>
            </select>
          </div>

          {composer.toolMode === "selected" && !providerSupportsSelectedTools ? (
            <p className="mt-3 text-sm text-ember-500">
              The selected provider does not advertise mcp_catalog tools.
            </p>
          ) : null}
          {integrationsError ? (
            <p className="mt-3 text-sm text-ember-500">{integrationsError}</p>
          ) : null}

          {composer.toolMode === "selected" ? (
            <div className="mt-4 space-y-4">
              {composer.tools.map((tool, index) => {
                const operations = tool.plugin
                  ? (operationsByPlugin[tool.plugin] ?? EMPTY_OPERATIONS)
                  : EMPTY_OPERATIONS;
                const operationsLoading = tool.plugin
                  ? Boolean(operationsLoadingByPlugin[tool.plugin])
                  : false;
                const operationsError = tool.plugin
                  ? (operationErrorsByPlugin[tool.plugin] ?? null)
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
                          value={tool.plugin}
                          onChange={(event) => {
                            const plugin = event.target.value;
                            updateTool(index, { plugin, operation: "" });
                            void ensureOperationsLoaded(plugin);
                          }}
                          className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
                        >
                          <option value="">Select plugin</option>
                          {integrations.map((integration) => (
                            <option
                              key={integration.name}
                              value={integration.name}
                            >
                              {integrationLabel(integration)}
                            </option>
                          ))}
                        </select>
                      </label>

                      <label className="space-y-2 text-sm">
                        <span className="text-muted">Operation</span>
                        <select
                          value={tool.operation}
                          disabled={!tool.plugin || operationsLoading}
                          onChange={(event) =>
                            updateTool(index, { operation: event.target.value })
                          }
                          className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong disabled:cursor-not-allowed disabled:opacity-60 dark:bg-surface"
                        >
                          <option value="">
                            {operationsLoading
                              ? "Loading operations..."
                              : "Select operation"}
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
                      <p className="mt-3 text-sm text-ember-500">
                        {operationsError}
                      </p>
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

        <details className="rounded-md border border-alpha bg-base-100 p-4 dark:bg-surface">
          <summary className="cursor-pointer text-sm font-medium text-primary">
            Advanced request fields
          </summary>
          <div className="mt-4 grid gap-4">
            <JsonTextarea
              label="Response schema JSON"
              value={composer.responseSchemaJSON}
              onChange={(value) =>
                setComposer((current) => ({
                  ...current,
                  responseSchemaJSON: value,
                }))
              }
            />
            <JsonTextarea
              label="Metadata JSON"
              value={composer.metadataJSON}
              onChange={(value) =>
                setComposer((current) => ({ ...current, metadataJSON: value }))
              }
            />
            <JsonTextarea
              label="Model options JSON"
              value={composer.modelOptionsJSON}
              onChange={(value) =>
                setComposer((current) => ({
                  ...current,
                  modelOptionsJSON: value,
                }))
              }
            />
            <label className="space-y-2 text-sm">
              <span className="text-muted">Idempotency key</span>
              <input
                value={composer.idempotencyKey}
                onChange={(event) =>
                  setComposer((current) => ({
                    ...current,
                    idempotencyKey: event.target.value,
                  }))
                }
                className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
              />
            </label>
          </div>
        </details>

        <button
          type="submit"
          disabled={submitting}
          className="w-full rounded-md bg-primary px-4 py-2 text-sm font-medium text-background transition-opacity duration-150 hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {submitting ? "Starting..." : selectedSession ? "Send turn" : "Create session"}
        </button>
      </div>
    </form>
  );
}

function ProviderField({
  providers,
  value,
  disabled,
  onChange,
}: {
  providers: AgentProvider[];
  value: string;
  disabled: boolean;
  onChange: (value: string) => void;
}) {
  if (providers.length > 0) {
    return (
      <label className="space-y-2 text-sm">
        <span className="text-muted">Provider</span>
        <select
          value={value}
          disabled={disabled}
          onChange={(event) => onChange(event.target.value)}
          className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong disabled:cursor-not-allowed disabled:opacity-60 dark:bg-surface"
        >
          <option value="">Default provider</option>
          {providers.map((provider) => (
            <option key={provider.name} value={provider.name}>
              {provider.name}
              {provider.default ? " (default)" : ""}
            </option>
          ))}
        </select>
      </label>
    );
  }

  return (
    <label className="space-y-2 text-sm">
      <span className="text-muted">Provider</span>
      <input
        value={value}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
        placeholder="default"
        className="w-full rounded-md border border-alpha bg-base-100 px-3 py-2 text-sm text-primary outline-none transition-colors duration-150 placeholder:text-faint focus:border-alpha-strong disabled:cursor-not-allowed disabled:opacity-60 dark:bg-surface"
      />
    </label>
  );
}

function EventInspector({
  providers,
  session,
  turn,
  events,
}: {
  providers: AgentProvider[];
  session: AgentSession | null;
  turn: AgentTurn | null;
  events: TranscriptState["rawPublicEvents"];
}) {
  const activityEvents = events.filter(isActivityEvent);

  return (
    <aside className="rounded-lg border border-alpha bg-base-100 dark:bg-surface">
      <div className="border-b border-alpha p-4">
        <div className="flex items-start justify-between gap-3">
          <div>
            <h2 className="text-sm font-medium text-primary">Activity</h2>
            <p className="mt-1 text-xs text-faint">
              {activityEvents.length} public activity
            </p>
          </div>
        </div>
        <dl className="mt-4 space-y-3 text-xs">
          <InspectorRow label="Session" value={session?.id} />
          <InspectorRow label="Turn" value={turn?.id} />
          <InspectorRow label="Provider" value={session?.provider} />
          <InspectorRow
            label="Tool sources"
            value={providerSourceLabel(providers, session?.provider)}
          />
        </dl>
      </div>
      <div className="p-4">
        <h3 className="text-xs font-medium uppercase tracking-[0.16em] text-faint">
          Public Activity
        </h3>
        {activityEvents.length === 0 ? (
          <p className="mt-3 text-sm text-faint">No public activity.</p>
        ) : (
          <div className="mt-3 max-h-[40rem] space-y-3 overflow-y-auto pr-1">
            {activityEvents.map((event) => (
              <ActivityEvent
                key={`${event.turnId}-${event.seq}-${event.id}`}
                event={event}
              />
            ))}
          </div>
        )}
      </div>
    </aside>
  );
}

function ActivityEvent({ event }: { event: AgentTurnEvent }) {
  const title = eventTitle(event);
  const phase = eventPhase(event);
  const detail = eventDetail(event);
  const input = eventInput(event);

  return (
    <details className="rounded-md border border-alpha bg-background/65 p-3 dark:bg-background/20">
      <summary className="cursor-pointer list-none">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <p className="truncate text-sm font-medium text-primary">{title}</p>
            <p className="mt-1 text-xs text-faint">
              #{event.seq} {phase}
            </p>
          </div>
          <span className="shrink-0 rounded-full bg-alpha-5 px-2 py-0.5 text-[11px] uppercase tracking-[0.12em] text-faint">
            {event.display?.kind || event.source || "event"}
          </span>
        </div>
      </summary>
      {input ? (
        <pre className="mt-3 max-h-36 overflow-auto whitespace-pre-wrap break-words rounded-md border border-alpha bg-base-100 p-2 text-xs text-primary dark:bg-surface">
          {input}
        </pre>
      ) : null}
      {detail ? (
        <pre className="mt-3 max-h-36 overflow-auto whitespace-pre-wrap break-words rounded-md border border-alpha bg-base-100 p-2 text-xs text-primary dark:bg-surface">
          {detail}
        </pre>
      ) : null}
      <pre className="mt-3 max-h-52 overflow-auto whitespace-pre-wrap break-words rounded-md border border-alpha bg-base-100 p-2 text-xs text-muted dark:bg-surface">
        {JSON.stringify(event, null, 2)}
      </pre>
    </details>
  );
}

function eventTitle(event: AgentTurnEvent): string {
  return (
    event.display?.label ||
    event.display?.text ||
    stringDataField(event.data, [
      "toolName",
      "tool_name",
      "tool_id",
      "toolId",
      "name",
      "operation",
    ]) ||
    event.type
  );
}

function eventPhase(event: AgentTurnEvent): string {
  return (
    event.display?.action ||
    event.display?.phase ||
    stringDataField(event.data, ["status", "state", "phase"]) ||
    event.type
  );
}

function eventInput(event: AgentTurnEvent): string | null {
  return (
    eventValue(event.display?.input) ??
    eventValue(dataField(event.data, ["arguments", "input", "params"]))
  );
}

function isActivityEvent(event: AgentTurnEvent): boolean {
  if (event.display?.kind === "text" || event.display?.kind === "reasoning") {
    return false;
  }
  switch (event.type) {
    case "agent.message.delta":
    case "assistant.delta":
    case "assistant.message":
    case "assistant.completed":
      return false;
    default:
      return true;
  }
}

function InspectorRow({
  label,
  value,
}: {
  label: string;
  value?: string | null;
}) {
  return (
    <div>
      <dt className="uppercase tracking-[0.14em] text-faint">{label}</dt>
      <dd className="mt-1 break-words text-muted">{value || "-"}</dd>
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
        className="w-full resize-y rounded-md border border-alpha bg-base-100 px-3 py-2 font-mono text-sm text-primary outline-none transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
      />
    </label>
  );
}

function defaultComposer(): AgentComposerState {
  return {
    provider: "",
    model: "",
    clientRef: "",
    systemPrompt: "",
    userPrompt: "",
    idempotencyKey: "",
    responseSchemaJSON: "",
    metadataJSON: "",
    modelOptionsJSON: "",
    toolMode: "none",
    tools: [emptyToolForm()],
  };
}

function emptyToolForm(): AgentToolForm {
  return {
    plugin: "",
    operation: "",
    connection: "",
    instance: "",
    title: "",
    description: "",
  };
}

function composerToTurnCreate(
  composer: AgentComposerState,
  providerSupportsSelectedTools: boolean,
): AgentTurnCreate {
  const userText = composer.userPrompt.trim();
  if (!userText) {
    throw new Error("User message is required");
  }

  const messages = [];
  const systemText = composer.systemPrompt.trim();
  if (systemText) {
    messages.push({ role: "system", text: systemText });
  }
  messages.push({ role: "user", text: userText });

  const body: AgentTurnCreate = {
    messages,
    toolRefs: [],
  };
  const model = composer.model.trim();
  if (model) body.model = model;
  const idempotencyKey = composer.idempotencyKey.trim();
  if (idempotencyKey) body.idempotencyKey = idempotencyKey;

  const responseSchema = parseOptionalObject(
    composer.responseSchemaJSON,
    "Response schema",
  );
  const metadata = parseOptionalObject(composer.metadataJSON, "Metadata");
  const modelOptions = parseOptionalObject(
    composer.modelOptionsJSON,
    "Model options",
  );
  if (responseSchema) body.responseSchema = responseSchema;
  if (metadata) body.metadata = metadata;
  if (modelOptions) body.modelOptions = modelOptions;

  if (composer.toolMode === "selected") {
    if (!providerSupportsSelectedTools) {
      throw new Error("Selected provider does not support mcp_catalog tools");
    }
    const toolRefs = toolRefsFromForm(composer.tools);
    if (toolRefs.length === 0) {
      throw new Error("At least one plugin operation is required");
    }
    body.toolSource = "mcp_catalog";
    body.toolRefs = toolRefs;
  }

  return body;
}

function toolRefsFromForm(tools: AgentToolForm[]): AgentToolRef[] {
  return tools
    .map((tool) => ({
      plugin: tool.plugin.trim(),
      operation: tool.operation.trim(),
      connection: tool.connection.trim(),
      instance: tool.instance.trim(),
      title: tool.title.trim(),
      description: tool.description.trim(),
    }))
    .filter((tool) => tool.plugin || tool.operation)
    .map((tool) => {
      if (!tool.plugin) throw new Error("Tool plugin is required");
      if (!tool.operation) throw new Error("Tool operation is required");
      return stripEmpty({
        plugin: tool.plugin,
        operation: tool.operation,
        connection: tool.connection,
        instance: tool.instance,
        title: tool.title,
        description: tool.description,
      }) as AgentToolRef;
    });
}

function parseOptionalObject(
  value: string,
  label: string,
): Record<string, unknown> | undefined {
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

function stripEmpty(value: Record<string, string>): Record<string, string> {
  return Object.fromEntries(
    Object.entries(value).filter(([, item]) => item.trim() !== ""),
  );
}

function filterSessions(
  sessions: AgentSession[],
  query: string,
  status: string,
): AgentSession[] {
  const normalizedQuery = query.trim().toLowerCase();
  return sessions.filter((session) => {
    if (status !== "all" && session.state !== status) return false;
    if (!normalizedQuery) return true;
    return [
      session.id,
      session.clientRef,
      session.provider,
      session.model,
      session.state,
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase()
      .includes(normalizedQuery);
  });
}

function sortTurnsAscending(turns: AgentTurn[]): AgentTurn[] {
  return turns.slice().sort((left, right) => {
    const leftTime = Date.parse(left.createdAt || "");
    const rightTime = Date.parse(right.createdAt || "");
    return (
      (Number.isNaN(leftTime) ? 0 : leftTime) -
      (Number.isNaN(rightTime) ? 0 : rightTime)
    );
  });
}

async function getTurnsIncludingTurn(
  sessionID: string,
  pinnedTurnID?: string | null,
): Promise<AgentTurn[]> {
  const turns = await getAgentTurns(sessionID, { limit: 20 });
  if (!pinnedTurnID || turns.some((turn) => turn.id === pinnedTurnID)) {
    return turns;
  }

  try {
    const pinnedTurn = await getAgentTurn(pinnedTurnID);
    if (pinnedTurn.sessionId === sessionID) {
      return [pinnedTurn, ...turns];
    }
  } catch {
    return turns;
  }
  return turns;
}

function chooseSelectedTurnID(
  turns: AgentTurn[],
  requestedID?: string | null,
): string | null {
  if (requestedID && turns.some((turn) => turn.id === requestedID)) {
    return requestedID;
  }
  return (
    turns.find((turn) => isTurnLive(turn.status))?.id ??
    turns[0]?.id ??
    null
  );
}

function turnLabel(turn: AgentTurn): string {
  const userMessage = [...(turn.messages ?? [])]
    .reverse()
    .find((message) => message.role === "user" && message.text?.trim());
  if (userMessage?.text) return truncate(userMessage.text.replace(/\s+/g, " "), 48);
  if (turn.outputText) return truncate(turn.outputText.replace(/\s+/g, " "), 48);
  return turn.id;
}

function statusClassName(status?: string): string {
  const base =
    "inline-flex shrink-0 rounded-full px-2 py-0.5 text-[11px] font-medium uppercase tracking-[0.14em]";
  switch (status) {
    case "succeeded":
      return `${base} bg-grove-500/10 text-grove-700 dark:text-grove-200`;
    case "failed":
      return `${base} bg-ember-500/10 text-ember-500`;
    case "canceled":
      return `${base} bg-alpha-5 text-muted`;
    case "waiting_for_input":
      return `${base} bg-amber-500/10 text-amber-700 dark:text-amber-200`;
    case "pending":
    case "running":
      return `${base} bg-sky-500/10 text-sky-600 dark:text-sky-200`;
    default:
      return `${base} bg-alpha-5 text-faint`;
  }
}

function sortOperations(
  operations: IntegrationOperation[],
): IntegrationOperation[] {
  return operations
    .filter((operation) => operation.visible !== false)
    .slice()
    .sort((left, right) =>
      (left.title || left.id).localeCompare(
        right.title || right.id,
        undefined,
        { sensitivity: "base" },
      ),
    );
}

function providerForSessionOrComposer(
  providers: AgentProvider[],
  session: AgentSession | null,
  composerProvider: string,
): AgentProvider | undefined {
  const name = session?.provider || composerProvider;
  return providers.find((provider) => provider.name === name) ?? providers.find((provider) => provider.default);
}

function providerSupportsMCPCatalog(
  provider: AgentProvider | undefined,
  providersLoaded: boolean,
): boolean {
  if (!providersLoaded) return true;
  const sources = provider?.capabilities?.supportedToolSources ?? [];
  return sources.includes("mcp_catalog");
}

function providerSourceLabel(
  providers: AgentProvider[],
  providerName?: string,
): string {
  const provider =
    providers.find((item) => item.name === providerName) ??
    providers.find((item) => item.default);
  return provider?.capabilities?.supportedToolSources?.join(", ") || "-";
}

function eventDetail(event: AgentTurnEvent): string | null {
  return (
    eventValue(event.display?.error) ??
    eventValue(event.display?.output) ??
    eventValue(event.data?.error) ??
    eventValue(event.data?.output) ??
    eventValue(event.data?.result) ??
    eventValue(event.data?.content) ??
    eventValue(event.data?.note) ??
    eventValue(event.data?.text)
  );
}

function stringDataField(
  data: Record<string, unknown> | undefined,
  fields: string[],
): string | null {
  const value = dataField(data, fields);
  return typeof value === "string" && value.trim() ? value : null;
}

function dataField(
  data: Record<string, unknown> | undefined,
  fields: string[],
): unknown {
  if (!data) return undefined;
  for (const field of fields) {
    const value = data[field];
    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }
  return undefined;
}

function eventValue(value: unknown): string | null {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function interactionDefaultValue(interaction: AgentInteraction): string {
  const value = interaction.request?.default;
  if (typeof value === "string") return value;
  if (interaction.type === "approval") return "";
  if (value !== undefined) return JSON.stringify(value);
  return interaction.type === "clarification" || interaction.type === "input"
    ? ""
    : "{}";
}

function integrationLabel(integration: Integration): string {
  return integration.displayName || integration.name;
}

function trimmedOrUndefined(value: string): string | undefined {
  const trimmed = value.trim();
  return trimmed || undefined;
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
  return value.length > maxLength
    ? `${value.slice(0, maxLength - 1)}...`
    : value;
}

function errorMessage(reason: unknown, fallback: string): string {
  if (reason instanceof Error && reason.message) return reason.message;
  if (typeof reason === "string" && reason) return reason;
  return fallback;
}

function withLoadTimeout<T>(promise: Promise<T>, label: string): Promise<T> {
  return new Promise((resolve, reject) => {
    const timeout = window.setTimeout(() => {
      reject(
        new Error(
          `${label} timed out after ${Math.round(
            AGENT_BOOTSTRAP_TIMEOUT_MS / 1000,
          )} seconds.`,
        ),
      );
    }, AGENT_BOOTSTRAP_TIMEOUT_MS);

    promise.then(
      (value) => {
        window.clearTimeout(timeout);
        resolve(value);
      },
      (error: unknown) => {
        window.clearTimeout(timeout);
        reject(error);
      },
    );
  });
}
