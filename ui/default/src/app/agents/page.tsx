"use client";

import {
  type ReactNode,
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
  AgentTurn,
  AgentTurnCreate,
  AgentTurnEvent,
  AgentTurnEventStream,
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
import Container from "@/components/Container";
import Nav from "@/components/Nav";
import {
  Conversation,
  ConversationContent,
  ConversationEmptyState,
  ConversationInitialScroll,
  ConversationScrollButton,
} from "@/components/ai-elements/conversation";
import { Message, MessageContent } from "@/components/ai-elements/message";
import { Response } from "@/components/ai-elements/response";
import {
  Reasoning,
  ReasoningContent,
  ReasoningTrigger,
} from "@/components/ai-elements/reasoning";
import {
  Tool,
  ToolContent,
  ToolHeader,
  ToolInput,
  ToolOutput,
  type ToolState,
} from "@/components/ai-elements/tool";
import {
  Confirmation,
  ConfirmationAction,
  ConfirmationActions,
  ConfirmationTitle,
} from "@/components/ai-elements/confirmation";
import {
  PromptInput,
  PromptInputSubmit,
  PromptInputTextarea,
  PromptInputToolbar,
  PromptInputTools,
} from "@/components/ai-elements/prompt-input";
import { Button, Shimmer } from "@/components/ai-elements/primitives";
import { MessageSquareIcon } from "lucide-react";

type InteractionDrafts = Record<string, string>;

interface AgentComposerState {
  provider: string;
  model: string;
  clientRef: string;
  systemPrompt: string;
  userPrompt: string;
  idempotencyKey: string;
  schemaJSON: string;
  metadataJSON: string;
  modelOptionsJSON: string;
}

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
    provider: string | null;
  }>({ session: null, turn: null, provider: null });
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
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [streamNonce, setStreamNonce] = useState(0);
  const [sessionsError, setSessionsError] = useState<string | null>(null);
  const [providersError, setProvidersError] = useState<string | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
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

  useEffect(() => {
    function readQuerySelection() {
      const params = new URLSearchParams(window.location.search);
      setQuerySelection({
        session: params.get("session"),
        turn: params.get("turn"),
        provider: params.get("provider"),
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

  // Session ids are provider-minted, so every session/turn-scoped request must
  // carry the provider the client already knows: the loaded session object,
  // the sessions list, or the deep-link query param.
  const selectedSessionProvider = useMemo(() => {
    if (!selectedSessionID) return null;
    if (selectedSession?.id === selectedSessionID && selectedSession.provider) {
      return selectedSession.provider;
    }
    const listed = sessions.find((session) => session.id === selectedSessionID);
    if (listed?.provider) return listed.provider;
    if (querySelection.session === selectedSessionID && querySelection.provider) {
      return querySelection.provider;
    }
    return null;
  }, [querySelection, selectedSession, selectedSessionID, sessions]);

  const loadSelectedSession = useCallback(
    async (
      sessionID: string,
      provider: string,
      requestedTurnID?: string | null,
    ) => {
      setDetailError(null);
      const [session, nextTurns] = await Promise.all([
        getAgentSession(sessionID, provider),
        getTurnsIncludingTurn(sessionID, provider, requestedTurnID),
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

    if (!selectedSessionProvider) {
      // Provider is unresolved while the sessions list is loading; if it stays
      // unknown afterwards the session cannot be fetched at all.
      if (!loading) {
        setDetailError(
          "Could not determine the agent provider for this session.",
        );
        setTurns([]);
        setTranscriptReady(true);
      }
      return;
    }

    let active = true;
    setTranscriptReady(false);
    loadSelectedSession(
      selectedSessionID,
      selectedSessionProvider,
      querySelection.turn,
    ).catch((err) => {
      if (!active) return;
      setDetailError(errorMessage(err, "Failed to load agent session"));
      setTurns([]);
      setTranscriptReady(true);
    });
    return () => {
      active = false;
    };
  }, [
    loadSelectedSession,
    loading,
    querySelection.turn,
    selectedSessionID,
    selectedSessionProvider,
    refreshNonce,
  ]);

  useEffect(() => {
    let active = true;

    async function replayTurns(provider: string) {
      let state = createTranscriptState();
      const sortedTurns = sortTurnsAscending(turns);
      const eventResults = await Promise.all(
        sortedTurns.map((turn) =>
          getAllAgentTurnEvents(turn.id, provider, { limit: 100 }),
        ),
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

    if (!selectedSessionID || !selectedSessionProvider) {
      setTranscriptReady(true);
      return () => {
        active = false;
      };
    }

    setTranscriptReady(false);
    replayTurns(selectedSessionProvider).catch((err) => {
      if (!active) return;
      setDetailError(errorMessage(err, "Failed to load agent transcript"));
      setTranscript(createTranscriptState());
      setTranscriptReady(true);
    });
    return () => {
      active = false;
    };
  }, [selectedSessionID, selectedSessionProvider, turns]);

  const selectedTurn = useMemo(
    () => turns.find((turn) => turn.id === selectedTurnID) ?? null,
    [selectedTurnID, turns],
  );

  const loadInteractions = useCallback(async (turnID: string, provider: string) => {
    const values = await getAgentInteractions(turnID, provider);
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
    if (
      !selectedTurn ||
      selectedTurn.status !== "waiting_for_input" ||
      !selectedSessionProvider
    ) {
      setInteractions([]);
      return;
    }
    loadInteractions(selectedTurn.id, selectedSessionProvider).catch((err) => {
      setActionError(errorMessage(err, "Failed to load interactions"));
    });
  }, [loadInteractions, selectedSessionProvider, selectedTurn]);

  useEffect(() => {
    const previousStream = streamRef.current;
    streamRef.current = null;
    previousStream?.close();

    const provider = selectedSessionProvider;
    if (
      !selectedTurn ||
      !provider ||
      !transcriptReady ||
      !isTurnLive(selectedTurn.status)
    ) {
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
    stream = openAgentTurnEventStream(turnID, provider, {
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
        getTurnsIncludingTurn(selectedTurn.sessionId, provider, turnID)
          .then((nextTurns) => {
            setTurns(nextTurns);
            const latest = nextTurns.find((turn) => turn.id === turnID);
            if (latest?.status === "waiting_for_input") {
              blockedTurnRef.current = turnID;
              return loadInteractions(turnID, provider);
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
  }, [
    loadInteractions,
    selectedSessionProvider,
    selectedTurn,
    streamNonce,
    transcriptReady,
  ]);

  useEffect(() => {
    const next = agentSessionHref({
      sessionID: selectedSessionID,
      turnID: selectedTurnID,
      provider: selectedSessionProvider,
    });
    if (window.location.pathname + window.location.search !== next) {
      window.history.replaceState(null, "", next);
    }
  }, [selectedSessionID, selectedSessionProvider, selectedTurnID]);

  const filteredSessions = useMemo(
    () => filterSessions(sessions, deferredQuery, statusFilter),
    [deferredQuery, sessions, statusFilter],
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
      turnBody = composerToTurnCreate(composer);
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

      const created = await createAgentTurn(session.id, session.provider, turnBody);
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
    if (!isTurnLive(turn.status) || !selectedSessionProvider) return;
    setCancelingTurnID(turn.id);
    setActionError(null);
    try {
      const updated = await cancelAgentTurn(
        turn.id,
        selectedSessionProvider,
        "Turn canceled.",
      );
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
    if (!selectedTurn || !selectedSessionProvider) return;
    setResolvingInteractionID(interaction.id);
    setActionError(null);
    try {
      await resolveAgentInteraction(
        selectedTurn.id,
        selectedSessionProvider,
        interaction.id,
        resolution,
      );
      blockedTurnRef.current = null;
      setInteractions((current) =>
        current.filter((item) => item.id !== interaction.id),
      );
      const nextTurns = await getTurnsIncludingTurn(
        selectedTurn.sessionId,
        selectedSessionProvider,
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
        <Container
          as="main"
          width="full"
          className="flex h-[calc(100vh-5rem)] flex-col overflow-hidden"
        >
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
                >
                  <InteractionPanel
                    interactions={interactions}
                    drafts={interactionDrafts}
                    resolvingID={resolvingInteractionID}
                    setDrafts={setInteractionDrafts}
                    onResolve={handleResolveInteraction}
                  />
                </TranscriptView>

                <div className="border-t border-alpha bg-background/40 px-5 py-4 dark:bg-background/30">
                  <div className="mx-auto w-full max-w-3xl">
                    <AgentComposer
                      composer={composer}
                      selectedSession={selectedSession}
                      providers={providers}
                      providersError={providersError}
                      formError={composerError}
                      submitting={submitting}
                      setComposer={setComposer}
                      onSubmit={handleSubmit}
                    />
                  </div>
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
        </Container>
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
              className="w-full bg-transparent text-primary outline-hidden placeholder:text-faint"
            />
          </div>
          <select
            value={statusFilter}
            onChange={(event) => setStatusFilter(event.target.value)}
            className="w-full border border-alpha bg-background/50 px-2 py-1.5 font-mono text-xs uppercase tracking-[0.16em] text-muted outline-hidden transition-colors duration-150 focus:border-alpha-strong"
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
  children,
}: {
  loading: boolean;
  items: TranscriptItem[];
  emptyMessage: string;
  turnLive: boolean;
  children?: ReactNode;
}) {
  const showThinking = turnLive && !hasInFlightActivity(items);

  return (
    <Conversation className="min-h-0 border-t border-alpha">
      <ConversationContent className="mx-auto w-full max-w-3xl px-5 py-5">
        {loading ? (
          <p className="font-mono text-xs text-faint">
            <span className="tui-glyph mr-2 text-sky-500">●</span>
            loading transcript…
          </p>
        ) : items.length === 0 && !showThinking ? (
          <ConversationEmptyState
            className="min-h-48"
            icon={<MessageSquareIcon className="size-6" />}
            title="No messages yet"
            description={emptyMessage}
          />
        ) : (
          items.map((item) => <TranscriptItemView key={item.id} item={item} />)
        )}
        {showThinking ? <ThinkingRow /> : null}
        {children}
      </ConversationContent>
      <ConversationInitialScroll ready={!loading} />
      <ConversationScrollButton />
    </Conversation>
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
    <div className="flex w-full items-center py-1 text-sm" aria-live="polite">
      <Shimmer>Thinking…</Shimmer>
    </div>
  );
}

function TranscriptItemView({ item }: { item: TranscriptItem }) {
  switch (item.kind) {
    case "tool":
      return <ToolTranscriptCard item={item} />;
    case "user":
      return (
        <Message from="user">
          <MessageContent>
            <p className="whitespace-pre-wrap break-words leading-6">
              {item.text}
            </p>
          </MessageContent>
        </Message>
      );
    case "assistant":
      return (
        <Message from="assistant">
          <MessageContent>
            <Response>{item.text}</Response>
          </MessageContent>
        </Message>
      );
    case "reasoning":
      return (
        <Reasoning className="w-full" isStreaming={Boolean(item.streaming)}>
          <ReasoningTrigger />
          <ReasoningContent>{item.text}</ReasoningContent>
        </Reasoning>
      );
    case "error":
      return (
        <div className="w-full rounded-md border border-danger/30 bg-danger/10 px-3 py-2 text-sm text-danger">
          <p className="font-medium">{item.title}</p>
          <p className="mt-1 whitespace-pre-wrap break-words">{item.text}</p>
        </div>
      );
    default:
      return <SystemRow item={item} />;
  }
}

// Low-emphasis single line for system/event/interaction markers, expandable
// when the item carries a payload (raw event JSON or multi-line text).
function SystemRow({ item }: { item: TranscriptItem }) {
  const block = item.event
    ? JSON.stringify(item.event, null, 2)
    : item.text.includes("\n")
      ? item.text
      : null;
  const inline = block === item.text ? null : item.text;

  if (!block) {
    return (
      <p className="flex w-full items-baseline gap-2 py-0.5 font-mono text-[11px] text-faint">
        <span className="shrink-0 uppercase tracking-[0.16em]">{item.title}</span>
        {inline ? <span className="truncate text-muted">{inline}</span> : null}
      </p>
    );
  }
  return (
    <details className="w-full py-0.5 font-mono text-[11px] text-faint">
      <summary className="flex cursor-pointer list-none items-baseline gap-2 hover:text-muted">
        <span className="shrink-0 uppercase tracking-[0.16em]">{item.title}</span>
        {inline ? (
          <span className="truncate text-muted">
            {truncate(inline.replace(/\s+/g, " "), 96)}
          </span>
        ) : null}
      </summary>
      <pre className="mt-1 max-h-48 overflow-auto whitespace-pre-wrap break-words rounded-md bg-alpha-5 p-2 text-[11px] leading-5 text-muted">
        {block}
      </pre>
    </details>
  );
}

function ToolTranscriptCard({ item }: { item: TranscriptItem }) {
  const event = item.event;
  const phase = event ? eventPhase(event) : item.text;
  const state = toolStateFromPhase(phase);
  const input = event ? eventInput(event) : null;
  const detail = event ? eventDetail(event) : item.text;
  const raw = event ? JSON.stringify(event, null, 2) : null;

  return (
    <article className="w-full">
      <Tool defaultOpen={state === "output-error"}>
        <ToolHeader title={item.title} state={state} />
        <ToolContent>
        {input ? <ToolInput input={input} /> : null}
        {state === "input-available" ? (
          <p className="font-mono text-[11px] text-faint">running…</p>
        ) : (
          <ToolOutput
            output={state === "output-error" ? undefined : detail || undefined}
            errorText={
              state === "output-error" ? detail || phase || "failed" : undefined
            }
          />
        )}
          {raw ? (
            <details className="font-mono text-[11px] text-faint">
              <summary className="cursor-pointer list-none uppercase tracking-[0.14em] hover:text-muted">
                Event JSON
              </summary>
              <pre className="mt-1 max-h-56 overflow-auto whitespace-pre-wrap break-words rounded-md bg-alpha-5 p-2 leading-5 text-muted">
                {raw}
              </pre>
            </details>
          ) : null}
        </ToolContent>
      </Tool>
    </article>
  );
}

function toolStateFromPhase(phase?: string | null): ToolState {
  const normalized = phase?.toLowerCase() ?? "";
  if (normalized.includes("failed") || normalized.includes("error")) {
    return "output-error";
  }
  if (normalized.includes("completed") || normalized.includes("succeeded")) {
    return "output-available";
  }
  if (normalized.includes("started") || normalized.includes("running")) {
    return "input-available";
  }
  return "input-streaming";
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
    <section className="w-full space-y-3" aria-label="Waiting for input">
      {interactions.map((interaction) => {
          const resolving = resolvingID === interaction.id;
          if (interaction.type === "approval") {
            return (
              <Confirmation
                key={interaction.id}
                approval={{ id: interaction.id }}
                state="approval-requested"
              >
                <ConfirmationTitle>
                  <span className="font-medium text-primary">
                    {interaction.title || "Approval required"}
                  </span>
                  {interaction.prompt ? (
                    <span className="mt-1 block whitespace-pre-wrap text-muted">
                      {interaction.prompt}
                    </span>
                  ) : null}
                </ConfirmationTitle>
                <ConfirmationActions>
                  <ConfirmationAction
                    variant="outline"
                    disabled={resolving}
                    onClick={() =>
                      void onResolve(interaction, { approved: false })
                    }
                  >
                    Reject
                  </ConfirmationAction>
                  <ConfirmationAction
                    disabled={resolving}
                    onClick={() =>
                      void onResolve(interaction, { approved: true })
                    }
                  >
                    Approve
                  </ConfirmationAction>
                </ConfirmationActions>
              </Confirmation>
            );
          }

          if (interaction.type === "clarification" || interaction.type === "input") {
            const required = interaction.request?.required === true;
            const secret = interaction.request?.secret === true;
            return (
              <form
                key={interaction.id}
                className="space-y-3 rounded-lg border border-alpha bg-surface px-4 py-3"
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
                  className="w-full rounded-md border border-alpha bg-background px-3 py-2 text-sm text-primary outline-hidden transition-colors duration-150 focus:border-alpha-strong"
                />
                <div className="flex justify-end">
                  <Button size="sm" type="submit" disabled={resolving}>
                    Submit
                  </Button>
                </div>
              </form>
            );
          }

          return (
            <form
              key={interaction.id}
              className="space-y-3 rounded-lg border border-alpha bg-surface px-4 py-3"
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
                className="w-full rounded-md border border-alpha bg-background px-3 py-2 font-mono text-sm text-primary outline-hidden transition-colors duration-150 focus:border-alpha-strong"
              />
              <div className="flex justify-end">
                <Button size="sm" type="submit" disabled={resolving}>
                  Resolve
                </Button>
              </div>
            </form>
          );
        })}
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
  formError: string | null;
  submitting: boolean;
  setComposer: React.Dispatch<React.SetStateAction<AgentComposerState>>;
  onSubmit: (event: React.FormEvent<HTMLFormElement>) => void | Promise<void>;
}) {
  const errorBlock =
    formError || providersError ? (
      <div className="space-y-1 font-mono text-[11px]">
        {formError ? <p className="text-danger">{formError}</p> : null}
        {providersError ? (
          <p className="text-danger">{providersError}</p>
        ) : null}
      </div>
    ) : null;

  return (
    <div className="space-y-2">
      {errorBlock}
      <PromptInput onSubmit={onSubmit}>
        <PromptInputTextarea
          aria-label="User message"
          value={composer.userPrompt}
          required
          disabled={submitting}
          placeholder="Message agent…"
          onChange={(event) =>
            setComposer((current) => ({
              ...current,
              userPrompt: event.target.value,
            }))
          }
        />
        <PromptInputToolbar>
          <PromptInputTools>
            {selectedSession ? (
              <span className="truncate px-2 font-mono text-[11px] text-faint">
                {selectedSession.provider || "default"} ·{" "}
                {selectedSession.model || "—"}
              </span>
            ) : (
              <>
                <ProviderField
                  providers={providers}
                  value={composer.provider}
                  disabled={false}
                  onChange={(value) =>
                    setComposer((current) => ({ ...current, provider: value }))
                  }
                />
                <span className="hidden truncate font-mono text-[11px] text-faint sm:inline">
                  The first message creates a cloud agent session.
                </span>
              </>
            )}
          </PromptInputTools>
          <PromptInputSubmit
            aria-label={selectedSession ? "Send turn" : "Create session"}
            status={submitting ? "submitted" : "ready"}
            disabled={submitting}
          />
        </PromptInputToolbar>
      </PromptInput>
    </div>
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
        className="border border-alpha bg-background/50 px-2 py-1 text-xs uppercase tracking-[0.16em] text-primary outline-hidden transition-colors duration-150 focus:border-alpha-strong disabled:cursor-not-allowed disabled:opacity-60"
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
        className="w-full resize-y rounded-md border border-alpha bg-base-100 px-3 py-2 font-mono text-sm text-primary outline-hidden transition-colors duration-150 focus:border-alpha-strong dark:bg-surface"
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
    schemaJSON: "",
    metadataJSON: "",
    modelOptionsJSON: "",
  };
}

function composerToTurnCreate(composer: AgentComposerState): AgentTurnCreate {
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

  const schema = parseOptionalObject(
    composer.schemaJSON,
    "Schema",
  );
  const metadata = parseOptionalObject(composer.metadataJSON, "Metadata");
  const modelOptions = parseOptionalObject(
    composer.modelOptionsJSON,
    "Model options",
  );
  body.output = schema
    ? { structured: { schema } }
    : { text: {} };
  if (metadata) body.metadata = metadata;
  if (modelOptions) body.modelOptions = modelOptions;

  return body;
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
  provider: string,
  pinnedTurnID?: string | null,
): Promise<AgentTurn[]> {
  const turns = await getAgentTurns(sessionID, provider, { limit: 20 });
  if (!pinnedTurnID || turns.some((turn) => turn.id === pinnedTurnID)) {
    return turns;
  }

  try {
    const pinnedTurn = await getAgentTurn(pinnedTurnID, provider);
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
  const output = turn.output?.text?.text ?? turn.output?.structured?.text;
  if (output) return truncate(output.replace(/\s+/g, " "), 48);
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
