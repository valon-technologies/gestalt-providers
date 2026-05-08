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
import { agentSessionHref } from "@/lib/agentLinks";
import {
  appendInteraction,
  appendOptimisticUserMessage,
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
        const fallback = value.find((p) => p.default) ?? value[0];
        if (fallback) {
          setComposer((current) =>
            current.provider ? current : { ...current, provider: fallback.name },
          );
        }
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
    const next = agentSessionHref(selectedSessionID, selectedTurnID);
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

    const userText = composer.userPrompt.trim();
    if (userText && selectedSession) {
      setTranscript((current) => appendOptimisticUserMessage(current, userText));
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

  const sessionShortID = selectedSession?.id
    ? selectedSession.id.slice(0, 8)
    : "—";
  const turnShortID = selectedTurn?.id ? selectedTurn.id.slice(0, 8) : "—";
  const turnLive = Boolean(selectedTurn && isTurnLive(selectedTurn.status));
  const footerStatus = turnLive
    ? liveActivityLabel(transcript.items)
    : selectedTurn?.status || (selectedSession ? "idle" : "no session");

  return (
    <AuthGuard>
      <div className="min-h-screen bg-background">
        <Nav />
        <main className="flex h-[calc(100vh-5rem)] flex-col overflow-hidden">
          <h1 className="sr-only">Agent Sessions</h1>
          <div className="flex items-center justify-between border-b border-alpha px-5 py-2.5">
            <div className="flex items-center gap-3 font-mono text-[11px] uppercase tracking-[0.22em] text-muted">
              <span className="tui-glyph text-grove-600 dark:text-grove-200">●</span>
              <span>agents</span>
              <span className="text-faint">/</span>
              <span className="text-primary">sessions</span>
              {selectedSession ? (
                <>
                  <span className="text-faint">/</span>
                  <span className="truncate text-primary">
                    {selectedSession.clientRef || sessionShortID}
                  </span>
                </>
              ) : null}
            </div>
            <div className="flex items-center gap-2 font-mono text-[11px]">
              <button
                type="button"
                onClick={() => selectSession(null)}
                className="rounded-sm border border-alpha bg-transparent px-2.5 py-1 uppercase tracking-[0.16em] text-muted transition-colors duration-150 hover:border-alpha-strong hover:text-primary"
              >
                + new
              </button>
              <button
                type="button"
                onClick={() => setRefreshNonce((value) => value + 1)}
                className="rounded-sm border border-alpha bg-transparent px-2.5 py-1 uppercase tracking-[0.16em] text-muted transition-colors duration-150 hover:border-alpha-strong hover:text-primary"
              >
                {refreshing ? "↻ refreshing" : "↻ refresh"}
              </button>
            </div>
          </div>

          {loading ? (
            <div className="flex flex-1 items-center justify-center font-mono text-xs uppercase tracking-[0.22em] text-faint">
              <span className="tui-glyph mr-2 text-sky-500">●</span>
              connecting to runtime…
            </div>
          ) : (
            <div className="flex min-h-0 flex-1 lg:grid lg:grid-cols-[18rem_minmax(0,1fr)_20rem]">
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

              <section className="flex min-w-0 flex-col overflow-hidden border-x border-alpha lg:border-x">
                <ConsoleHeader
                  session={selectedSession}
                  turn={selectedTurn}
                  turns={turns}
                  onSelectTurn={selectTurn}
                  onCancelTurn={handleCancelTurn}
                  cancelingTurnID={cancelingTurnID}
                />

                {notice || actionError || detailError ? (
                  <div className="border-t border-alpha px-5 py-2 font-mono text-[11px]">
                    {notice ? (
                      <p className="text-grove-700 dark:text-grove-200">
                        <span className="tui-glyph mr-2">●</span>
                        {notice}
                      </p>
                    ) : null}
                    {actionError ? (
                      <p className="text-ember-500">
                        <span className="tui-glyph mr-2">✗</span>
                        {actionError}
                      </p>
                    ) : null}
                    {detailError ? (
                      <p className="text-ember-500">
                        <span className="tui-glyph mr-2">✗</span>
                        {detailError}
                      </p>
                    ) : null}
                  </div>
                ) : null}

                <div className="min-h-0 flex-1 overflow-y-auto border-t border-alpha px-5 py-5">
                  <TranscriptView
                    loading={!transcriptReady}
                    items={transcript.items}
                    turnLive={Boolean(
                      selectedTurn && isTurnLive(selectedTurn.status),
                    )}
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
                </div>

                <div className="max-h-[45vh] overflow-y-auto border-t border-alpha bg-background/40 px-5 py-4 dark:bg-background/30">
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

          <div className="flex items-center justify-between border-t border-alpha px-5 py-2 font-mono text-[11px] text-muted">
            <div className="flex items-center gap-3">
              <span
                className={`tui-glyph ${footerStatusColor(footerStatus)}${turnLive ? " tui-pulse" : ""}`}
              >
                ●
              </span>
              <span
                className={`uppercase tracking-[0.18em]${turnLive ? " tui-pulse" : ""}`}
              >
                {turnLive ? footerStatus : `state ${footerStatus}`}
              </span>
              <span className="text-faint">│</span>
              <span>{selectedSession?.provider || "default"}</span>
              <span className="text-faint">·</span>
              <span>{selectedSession?.model || "—"}</span>
              <span className="text-faint">│</span>
              <span>session {sessionShortID}</span>
              <span className="text-faint">·</span>
              <span>turn {turnShortID}</span>
            </div>
            <div className="hidden items-center gap-3 text-faint md:flex">
              <span>↵ send</span>
              <span>·</span>
              <span>⌘K commands</span>
              <span>·</span>
              <span>esc cancel</span>
            </div>
          </div>
        </main>
      </div>
    </AuthGuard>
  );
}

function footerStatusColor(status?: string): string {
  const normalized = (status ?? "").toLowerCase();
  if (normalized === "succeeded") return "text-grove-600 dark:text-grove-200";
  if (normalized === "failed") return "text-ember-500";
  if (
    normalized === "running" ||
    normalized === "pending" ||
    normalized === "thinking" ||
    normalized === "writing" ||
    normalized.startsWith("running ")
  ) {
    return "text-sky-500";
  }
  if (normalized === "waiting_for_input") return "text-amber-500";
  if (normalized === "canceled") return "text-faint";
  return "text-faint";
}

function liveActivityLabel(items: TranscriptItem[]): string {
  for (let index = items.length - 1; index >= 0; index -= 1) {
    const item = items[index];
    if (item.kind === "tool") {
      const phase = item.event ? eventPhase(item.event).toLowerCase() : "";
      if (phase.includes("started") || phase.includes("running")) {
        const name = item.title || item.event?.display?.label || "tool";
        return `running ${name}`;
      }
    }
    if (item.kind === "assistant" && item.streaming) {
      return "writing";
    }
  }
  return "thinking";
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
    <aside className="flex min-h-[16rem] flex-col overflow-hidden bg-background/30 lg:min-h-0">
      <div className="border-b border-alpha px-4 py-3">
        <div className="tui-section-label flex items-center gap-2">
          <span className="tui-glyph text-faint">◇</span>
          <span>Sessions</span>
          <span className="ml-auto text-faint normal-case tracking-normal">
            {sessions.length}
          </span>
        </div>
        <div className="mt-3 space-y-1.5">
          <div className="flex items-center gap-2 border border-alpha bg-background/50 px-2 py-1.5 font-mono text-xs">
            <span className="tui-glyph text-faint">⌕</span>
            <input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="filter sessions…"
              className="w-full bg-transparent text-primary outline-none placeholder:text-faint"
            />
          </div>
          <select
            value={statusFilter}
            onChange={(event) => setStatusFilter(event.target.value)}
            className="w-full border border-alpha bg-background/50 px-2 py-1.5 font-mono text-xs uppercase tracking-[0.16em] text-muted outline-none transition-colors duration-150 focus:border-alpha-strong"
          >
            <option value="all">all states</option>
            <option value="active">active</option>
            <option value="archived">archived</option>
          </select>
        </div>
      </div>

      {error ? (
        <p className="px-4 py-3 font-mono text-xs text-ember-500">
          <span className="tui-glyph mr-2">✗</span>
          {error}
        </p>
      ) : sessions.length === 0 ? (
        <p className="px-4 py-3 font-mono text-xs text-faint">
          <span className="tui-glyph mr-2">·</span>
          No agent sessions yet.
        </p>
      ) : (
        <div className="min-h-0 flex-1 overflow-y-auto py-1">
          {sessions.map((session) => {
            const active = session.id === selectedSessionID;
            const dotClass = sessionDotClass(session);
            const shortID = session.id.slice(0, 8);
            return (
              <button
                key={session.id}
                type="button"
                onClick={() => onSelect(session.id)}
                className={`relative flex w-full items-start gap-2.5 px-4 py-2 text-left font-mono transition-colors duration-150 ${
                  active
                    ? "bg-alpha-10 text-primary before:absolute before:left-0 before:top-0 before:h-full before:w-[2px] before:bg-grove-500 dark:before:bg-grove-200"
                    : "text-muted hover:bg-alpha-5"
                }`}
              >
                <span className={`tui-glyph mt-1 text-[10px] ${dotClass}`}>●</span>
                <span className="flex min-w-0 flex-col">
                  <span className="truncate text-sm text-primary">
                    {session.clientRef || shortID}
                  </span>
                  <span className="truncate text-[11px] text-faint">
                    {session.provider || "default"} · {session.model || "—"}
                  </span>
                  <span className="text-[10px] uppercase tracking-[0.16em] text-faint">
                    {shortID} · {formatDate(session.lastTurnAt || session.updatedAt)}
                  </span>
                </span>
              </button>
            );
          })}
        </div>
      )}
    </aside>
  );
}

function sessionDotClass(session: AgentSession): string {
  const status = (session as { state?: string }).state;
  switch (status) {
    case "active":
    case "running":
      return "text-sky-500";
    case "archived":
      return "text-faint";
    default:
      return "text-grove-600 dark:text-grove-200";
  }
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
    <div className="px-5 py-3">
      <div className="flex flex-wrap items-center gap-3 font-mono text-xs">
        <span className="tui-section-label">session</span>
        <span className="tui-glyph text-faint">›</span>
        <h2 className="truncate text-sm font-normal text-primary">
          {session?.clientRef || session?.id?.slice(0, 8) || "new"}
        </h2>
        <span className="text-faint">·</span>
        <span className="text-muted">
          {session ? `${session.provider || "default"} / ${session.model || "—"}` : "first message creates a cloud session"}
        </span>
        <div className="ml-auto flex items-center gap-2">
          {turn ? <span className={statusClassName(turn.status)}>{turn.status}</span> : null}
          {turn && isTurnLive(turn.status) ? (
            <button
              type="button"
              aria-label="Cancel turn"
              onClick={() => void onCancelTurn(turn)}
              disabled={cancelingTurnID === turn.id}
              className="border border-ember-500 bg-transparent px-2.5 py-1 font-mono text-[11px] uppercase tracking-[0.16em] text-ember-500 transition-colors duration-150 hover:bg-ember-500 hover:text-white disabled:cursor-not-allowed disabled:opacity-60"
            >
              {cancelingTurnID === turn.id ? "✗ canceling" : "✗ cancel"}
            </button>
          ) : null}
        </div>
      </div>

      {turns.length ? (
        <div className="mt-3 flex gap-1.5 overflow-x-auto pb-1 font-mono text-[11px]">
          <span className="tui-section-label shrink-0 self-center pr-1">turns</span>
          {turns.map((item) => {
            const active = item.id === turn?.id;
            return (
              <button
                key={item.id}
                type="button"
                onClick={() => onSelectTurn(item.id)}
                className={`flex shrink-0 items-center gap-2 border px-2 py-1 text-left transition-colors duration-150 ${
                  active
                    ? "border-alpha-strong bg-alpha-10 text-primary"
                    : "border-alpha text-muted hover:border-alpha-strong hover:text-primary"
                }`}
              >
                <span
                  className={`tui-glyph text-[10px] ${turnDotColor(item.status)}${
                    isTurnLive(item.status) ? " tui-pulse" : ""
                  }`}
                >
                  ●
                </span>
                <span className="block max-w-40 truncate">{turnLabel(item)}</span>
                <span className="block text-faint">{formatDate(item.createdAt)}</span>
              </button>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}

function turnDotColor(status?: string): string {
  switch (status) {
    case "succeeded":
      return "text-grove-600 dark:text-grove-200";
    case "failed":
      return "text-ember-500";
    case "running":
    case "pending":
      return "text-sky-500";
    case "waiting_for_input":
      return "text-amber-500";
    case "canceled":
      return "text-faint";
    default:
      return "text-faint";
  }
}

function TranscriptView({
  loading,
  items,
  emptyMessage,
  turnLive,
}: {
  loading: boolean;
  items: TranscriptItem[];
  emptyMessage: string;
  turnLive: boolean;
}) {
  const bottomRef = useRef<HTMLDivElement | null>(null);
  const showThinking = turnLive && !hasInFlightActivity(items);
  const itemKey = useMemo(
    () =>
      items
        .map((item) => `${item.id}:${item.text.length}:${item.streaming}`)
        .join("|") + `|${showThinking ? "thinking" : ""}`,
    [items, showThinking],
  );

  useEffect(() => {
    if (!loading) {
      bottomRef.current?.scrollIntoView({ block: "end" });
    }
  }, [itemKey, loading]);

  if (loading) {
    return (
      <p className="font-mono text-xs text-faint">
        <span className="tui-glyph mr-2 text-sky-500">●</span>
        loading transcript…
      </p>
    );
  }
  if (items.length === 0 && !showThinking) {
    return (
      <div className="border border-dashed border-alpha bg-background/40 px-5 py-6 font-mono text-xs text-faint">
        <span className="tui-glyph mr-2">·</span>
        {emptyMessage}
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {items.map((item) => (
        <TranscriptBubble key={item.id} item={item} />
      ))}
      {showThinking ? <ThinkingRow /> : null}
      <div ref={bottomRef} />
    </div>
  );
}

function hasInFlightActivity(items: TranscriptItem[]): boolean {
  for (let index = items.length - 1; index >= 0; index -= 1) {
    const item = items[index];
    if (item.kind === "assistant" && item.streaming) return true;
    if (item.kind === "tool") {
      const phase = item.event ? eventPhase(item.event).toLowerCase() : "";
      if (phase.includes("started") || phase.includes("running")) return true;
    }
  }
  return false;
}

function ThinkingRow() {
  return (
    <article className="flex gap-3 px-1 py-1" aria-live="polite">
      <span className="tui-glyph mt-1 text-sky-500 tui-pulse">●</span>
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span className="tui-section-label">thinking</span>
        </div>
        <p className="mt-1 tui-thinking-dots font-mono text-sm text-muted">
          <span>●</span>
          <span>●</span>
          <span>●</span>
        </p>
      </div>
    </article>
  );
}

function TranscriptBubble({ item }: { item: TranscriptItem }) {
  if (item.kind === "tool") {
    return <ToolTranscriptCard item={item} />;
  }

  if (item.kind === "user") {
    return (
      <div className="tui-user-bar border-l-2 border-l-grove-500 px-3 py-2 dark:border-l-grove-200">
        <div className="flex items-center gap-2 text-[11px]">
          <span className="tui-user-bar-prompt text-grove-700 dark:text-grove-200">›</span>
          <span className="tui-section-label">{item.title}</span>
          {item.streaming ? (
            <span className="ml-auto tui-status-line text-sky-500">streaming</span>
          ) : null}
        </div>
        <pre className="mt-1.5 whitespace-pre-wrap break-words font-mono text-sm leading-6 text-primary">
          {item.text}
        </pre>
      </div>
    );
  }

  const glyph =
    item.kind === "assistant"
      ? "●"
      : item.kind === "error"
        ? "✗"
        : item.kind === "interaction"
          ? "◆"
          : "·";
  const glyphClass =
    item.kind === "assistant"
      ? "text-grove-600 dark:text-grove-200"
      : item.kind === "error"
        ? "text-ember-500"
        : item.kind === "interaction"
          ? "text-amber-500"
          : "text-faint";
  const textClass =
    item.kind === "error"
      ? "text-ember-500"
      : item.kind === "system" || item.kind === "event"
        ? "text-muted"
        : "text-primary";

  const isStreamingAssistant = item.kind === "assistant" && Boolean(item.streaming);

  return (
    <article className="flex gap-3 px-1 py-1">
      <span
        className={`tui-glyph mt-1 ${glyphClass}${isStreamingAssistant ? " tui-pulse" : ""}`}
      >
        {glyph}
      </span>
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span className="tui-section-label">{item.title}</span>
        </div>
        <pre className={`mt-1 whitespace-pre-wrap break-words font-sans text-sm leading-6 ${textClass}`}>
          {item.text}
          {isStreamingAssistant ? (
            <span className="tui-caret text-sky-500">▍</span>
          ) : null}
        </pre>
      </div>
    </article>
  );
}

function ToolTranscriptCard({ item }: { item: TranscriptItem }) {
  const event = item.event;
  const phase = event ? eventPhase(event) : item.text;
  const normalizedPhase = phase?.toLowerCase() ?? "";
  const isRunning =
    normalizedPhase.includes("started") || normalizedPhase.includes("running");
  const input = event ? eventInput(event) : null;
  const detail = event ? eventDetail(event) : item.text;
  const raw = event ? JSON.stringify(event, null, 2) : null;
  const showSummary = Boolean(item.text && item.text !== detail);
  const phaseClass = toolPhaseDotColor(phase);

  return (
    <article className="flex gap-3 px-1 py-1">
      <span
        className={`tui-glyph mt-1 ${phaseClass}${isRunning ? " tui-pulse" : ""}`}
      >
        ●
      </span>
      <div className="min-w-0 flex-1">
        <div className="flex flex-wrap items-center gap-2">
          <span className="font-mono text-sm text-primary">{item.title}</span>
          <span className="tui-status-line">
            {event?.seq ? `#${event.seq} ` : ""}
            {phase || "tool"}
          </span>
          <span className="ml-auto tui-status-line">
            {event?.display?.kind || event?.source || "tool"}
          </span>
        </div>

        {showSummary ? (
          <pre className="mt-1 whitespace-pre-wrap break-words font-sans text-sm leading-6 text-muted">
            {item.text}
          </pre>
        ) : null}

        <div className="mt-1.5 tui-tree">
          {input ? <TranscriptDetail glyph="├─" label="Input" value={input} /> : null}
          {isRunning ? (
            <p className="font-mono text-[11px] text-faint">
              <span className="tui-glyph mr-1">└─</span>running…
            </p>
          ) : (
            <>
              {detail ? (
                <TranscriptDetail
                  glyph={raw ? "├─" : "└─"}
                  label="Output"
                  value={detail}
                />
              ) : null}
              {raw ? (
                <TranscriptDetail glyph="└─" label="Event JSON" value={raw} muted />
              ) : null}
            </>
          )}
        </div>
      </div>
    </article>
  );
}

function toolPhaseDotColor(phase?: string | null): string {
  const normalized = phase?.toLowerCase() ?? "";
  if (normalized.includes("failed") || normalized.includes("error")) {
    return "text-ember-500";
  }
  if (normalized.includes("completed") || normalized.includes("succeeded")) {
    return "text-grove-600 dark:text-grove-200";
  }
  if (normalized.includes("started") || normalized.includes("running")) {
    return "text-sky-500";
  }
  return "text-faint";
}

function TranscriptDetail({
  glyph = "└─",
  label,
  value,
  muted,
}: {
  glyph?: string;
  label: string;
  value: string;
  muted?: boolean;
}) {
  return (
    <details className="block">
      <summary className="cursor-pointer list-none font-mono text-[11px] uppercase tracking-[0.14em] text-faint hover:text-muted">
        <span className="tui-glyph mr-1">{glyph}</span>
        {label}
      </summary>
      <pre
        className={`ml-4 mt-1 max-h-56 overflow-auto whitespace-pre-wrap break-words border-l border-alpha bg-background/40 p-2 font-mono text-[11px] leading-5 ${
          muted ? "text-muted" : "text-primary"
        }`}
      >
        {value}
      </pre>
    </details>
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
  formError,
  submitting,
  setComposer,
  onSubmit,
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
  function handleUserPromptKeyDown(
    event: React.KeyboardEvent<HTMLTextAreaElement>,
  ) {
    if (event.key !== "Enter" || (!event.metaKey && !event.ctrlKey)) {
      return;
    }
    event.preventDefault();
    if (!submitting) {
      event.currentTarget.form?.requestSubmit();
    }
  }

  const errorBlock =
    formError || providersError ? (
      <div className="space-y-1 font-mono text-[11px]">
        {formError ? <p className="text-ember-500">{formError}</p> : null}
        {providersError ? (
          <p className="text-ember-500">{providersError}</p>
        ) : null}
      </div>
    ) : null;

  if (selectedSession) {
    return (
      <form className="space-y-3" onSubmit={onSubmit}>
        {errorBlock}

        <div className="border border-alpha bg-background/50 p-3">
          <label className="block">
            <span className="sr-only">User message</span>
            <textarea
              aria-label="User message"
              aria-keyshortcuts="Meta+Enter Control+Enter"
              value={composer.userPrompt}
              onKeyDown={handleUserPromptKeyDown}
              onChange={(event) =>
                setComposer((current) => ({
                  ...current,
                  userPrompt: event.target.value,
                }))
              }
              rows={3}
              required
              placeholder="Message agent..."
              className="min-h-24 w-full resize-y border-0 bg-transparent p-0 font-mono text-sm leading-6 text-primary outline-none placeholder:text-faint"
            />
          </label>

          <div className="mt-3 flex flex-col gap-3 border-t border-alpha pt-3 sm:flex-row sm:items-center sm:justify-end">
            <button
              type="submit"
              disabled={submitting}
              className="shrink-0 border border-alpha bg-transparent px-3 py-1 font-mono text-[11px] uppercase tracking-[0.18em] text-primary transition-colors duration-150 hover:border-alpha-strong disabled:cursor-not-allowed disabled:opacity-60"
            >
              {submitting ? "↵ sending…" : "↵ send turn"}
            </button>
          </div>
        </div>
      </form>
    );
  }

  return (
    <form className="space-y-3" onSubmit={onSubmit}>
      <div className="flex flex-wrap items-center gap-3">
        <span className="tui-section-label">new session</span>
        <span className="tui-glyph text-faint">›</span>
        <ProviderField
          providers={providers}
          value={composer.provider}
          disabled={Boolean(selectedSession)}
          onChange={(value) =>
            setComposer((current) => ({ ...current, provider: value }))
          }
        />
      </div>

      {errorBlock}

      <div className="border border-alpha bg-background/50 p-3">
        <label className="block">
          <span className="sr-only">User message</span>
          <textarea
            aria-label="User message"
            aria-keyshortcuts="Meta+Enter Control+Enter"
            value={composer.userPrompt}
            onKeyDown={handleUserPromptKeyDown}
            onChange={(event) =>
              setComposer((current) => ({
                ...current,
                userPrompt: event.target.value,
              }))
            }
            rows={4}
            required
            placeholder="Message agent…"
            className="min-h-28 w-full resize-y border-0 bg-transparent p-0 font-mono text-sm leading-6 text-primary outline-none placeholder:text-faint"
          />
        </label>
        <div className="mt-3 flex items-center justify-between border-t border-alpha pt-3 font-mono text-[11px] text-faint">
          <span>The first message creates a cloud agent session.</span>
          <button
            type="submit"
            disabled={submitting}
            className="shrink-0 border border-alpha bg-transparent px-3 py-1 uppercase tracking-[0.18em] text-primary transition-colors duration-150 hover:border-alpha-strong disabled:cursor-not-allowed disabled:opacity-60"
          >
            {submitting ? "↵ creating…" : "↵ create session"}
          </button>
        </div>
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
  if (providers.length === 0) {
    return null;
  }
  return (
    <label className="flex items-center gap-2 font-mono text-xs">
      <span className="tui-section-label">Agent</span>
      <select
        value={value || providers.find((p) => p.default)?.name || providers[0]?.name || ""}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
        className="border border-alpha bg-background/50 px-2 py-1 text-xs uppercase tracking-[0.16em] text-primary outline-none transition-colors duration-150 focus:border-alpha-strong disabled:cursor-not-allowed disabled:opacity-60"
      >
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
  const sessionShort = session?.id?.slice(0, 8) ?? "—";
  const turnShort = turn?.id?.slice(0, 8) ?? "—";

  return (
    <aside className="flex min-h-[16rem] flex-col overflow-hidden bg-background/30 lg:min-h-0">
      <div className="border-b border-alpha px-4 py-3">
        <div className="tui-section-label flex items-center gap-2">
          <span className="tui-glyph text-faint">◇</span>
          <h2 className="font-normal tracking-[0.22em]">Activity</h2>
          <span className="ml-auto text-faint normal-case tracking-normal">
            {activityEvents.length}
          </span>
        </div>
        <dl className="mt-3 space-y-1 font-mono text-[11px]">
          <InspectorRow label="session" value={sessionShort} />
          <InspectorRow label="turn" value={turnShort} />
          <InspectorRow label="provider" value={session?.provider || "—"} />
          <InspectorRow label="model" value={session?.model || "—"} />
          <InspectorRow
            label="sources"
            value={providerSourceLabel(providers, session?.provider) || "—"}
          />
        </dl>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto px-4 py-3">
        <div className="tui-section-label mb-2 flex items-center gap-2">
          <span className="tui-glyph text-faint">◇</span>
          <h3 className="font-normal tracking-[0.22em]">Public Activity</h3>
        </div>
        {activityEvents.length === 0 ? (
          <p className="font-mono text-xs text-faint">
            <span className="tui-glyph mr-2">·</span>
            no public activity
          </p>
        ) : (
          <div className="space-y-1">
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
  const dotClass = toolPhaseDotColor(phase);

  return (
    <details className="group block border-l border-transparent pl-1 hover:border-alpha-strong">
      <summary className="cursor-pointer list-none py-1">
        <div className="flex items-start gap-2 font-mono text-[11px]">
          <span className={`tui-glyph mt-[2px] ${dotClass}`}>●</span>
          <div className="min-w-0 flex-1">
            <p className="truncate text-primary">{title}</p>
            <p className="tui-status-line">
              #{event.seq} {phase || event.display?.kind || event.source || "event"}
            </p>
          </div>
        </div>
      </summary>
      <div className="ml-4 mt-1 space-y-1 tui-tree">
        {input ? (
          <pre className="max-h-36 overflow-auto whitespace-pre-wrap break-words border-l border-alpha bg-background/40 p-2 text-[11px] text-primary">
            {input}
          </pre>
        ) : null}
        {detail ? (
          <pre className="max-h-36 overflow-auto whitespace-pre-wrap break-words border-l border-alpha bg-background/40 p-2 text-[11px] text-primary">
            {detail}
          </pre>
        ) : null}
        <pre className="max-h-52 overflow-auto whitespace-pre-wrap break-words border-l border-alpha bg-background/40 p-2 text-[11px] text-muted">
          {JSON.stringify(event, null, 2)}
        </pre>
      </div>
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
    <div className="flex items-baseline gap-2">
      <dt className="w-16 shrink-0 uppercase tracking-[0.16em] text-faint">{label}</dt>
      <dd className="min-w-0 flex-1 truncate text-primary">{value || "—"}</dd>
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
    "inline-flex shrink-0 items-center gap-1.5 border px-2 py-0.5 font-mono text-[11px] uppercase tracking-[0.16em]";
  switch (status) {
    case "succeeded":
      return `${base} border-grove-500/40 text-grove-700 dark:text-grove-200`;
    case "failed":
      return `${base} border-ember-500/50 text-ember-500`;
    case "canceled":
      return `${base} border-alpha text-muted`;
    case "waiting_for_input":
      return `${base} border-amber-500/40 text-amber-600 dark:text-amber-200`;
    case "pending":
    case "running":
      return `${base} border-sky-500/40 text-sky-600 dark:text-sky-200 tui-pulse`;
    default:
      return `${base} border-alpha text-faint`;
  }
}

function toolPhaseClassName(phase?: string | null): string {
  const base =
    "shrink-0 border px-2 py-0.5 font-mono text-[11px] uppercase tracking-[0.16em]";
  const normalized = phase?.toLowerCase() ?? "";
  if (normalized.includes("failed") || normalized.includes("error")) {
    return `${base} border-ember-500/50 text-ember-500`;
  }
  if (normalized.includes("completed") || normalized.includes("succeeded")) {
    return `${base} border-grove-500/40 text-grove-700 dark:text-grove-200`;
  }
  if (normalized.includes("started") || normalized.includes("running")) {
    return `${base} border-sky-500/40 text-sky-600 dark:text-sky-200`;
  }
  return `${base} border-alpha text-faint`;
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
