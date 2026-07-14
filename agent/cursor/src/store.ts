import { randomUUID } from "node:crypto";

import type { JsonObject } from "@bufbuild/protobuf";

import {
  AgentExecutionStatus,
  AgentSessionState,
  AgentToolSourceMode,
  type AgentMessage,
  type ListedAgentTool,
  type AgentToolRef,
  type AgentSession,
  type AgentTurnEvent,
  type AgentTurn,
  type AgentTurnOutput,
} from "@valon-technologies/gestalt/services/agent";

export type PreparedWorkspace = {
  root: string;
  cwd: string;
};

export const TERMINAL_STATUSES = new Set<AgentExecutionStatus>([
  AgentExecutionStatus.SUCCEEDED,
  AgentExecutionStatus.FAILED,
  AgentExecutionStatus.CANCELED,
]);

export const SESSION_VISIBILITY_PRIVATE = "private";
export const SESSION_VISIBILITY_COMPANY = "company";

export type StoredSessionVisibility =
  | typeof SESSION_VISIBILITY_PRIVATE
  | typeof SESSION_VISIBILITY_COMPANY;

export class StoreConflictError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "StoreConflictError";
  }
}

export type StoredSession = {
  sessionId: string;
  idempotencyKey: string;
  providerName: string;
  model: string;
  clientRef: string;
  state: AgentSessionState;
  metadata: Record<string, unknown>;
  visibility: StoredSessionVisibility;
  preparedWorkspace: PreparedWorkspace | undefined;
  toolSource: AgentToolSourceMode;
  toolRefs: AgentToolRef[];
  listedTools: ListedAgentTool[];
  createdBySubjectId: string;
  createdAt: Date;
  updatedAt: Date;
  lastTurnAt: Date | undefined;
};

export type StoredTurn = {
  turnId: string;
  sessionId: string;
  idempotencyKey: string;
  providerName: string;
  model: string;
  status: AgentExecutionStatus;
  messages: AgentMessage[];
  output: AgentTurnOutput | undefined;
  statusMessage: string;
  createdBySubjectId: string;
  createdAt: Date;
  startedAt: Date | undefined;
  completedAt: Date | undefined;
  executionRef: string;
};

export type StoredTurnEvent = {
  eventId: string;
  turnId: string;
  seq: bigint;
  eventType: string;
  source: string;
  visibility: string;
  data: Record<string, unknown>;
  createdAt: Date;
};

export class InMemoryRunStore {
  private readonly sessions = new Map<string, StoredSession>();
  private readonly sessionIdempotency = new Map<string, string>();
  private readonly turns = new Map<string, StoredTurn>();
  private readonly turnIdempotency = new Map<string, string>();
  private readonly events = new Map<string, StoredTurnEvent[]>();

  close(): void {
    this.sessions.clear();
    this.sessionIdempotency.clear();
    this.turns.clear();
    this.turnIdempotency.clear();
    this.events.clear();
  }

  createSession(input: {
    idempotencyKey: string;
    providerName: string;
    model: string;
    clientRef: string;
    metadata: Record<string, unknown>;
    visibility: StoredSessionVisibility;
    preparedWorkspace: PreparedWorkspace | undefined;
    toolSource: AgentToolSourceMode;
    toolRefs: readonly AgentToolRef[];
    listedTools: readonly ListedAgentTool[];
    createdBySubjectId: string;
  }): { session: StoredSession; created: boolean } {
    const dedupKey = sessionDedupKey(input.createdBySubjectId, input.idempotencyKey);
    if (input.idempotencyKey.trim()) {
      const existingId = this.sessionIdempotency.get(dedupKey);
      if (existingId) {
        const idempotent = this.sessions.get(existingId);
        if (idempotent) {
          return { session: cloneSession(idempotent), created: false };
        }
      }
    }
    const sessionId = randomUUID();
    const now = new Date();
    const session: StoredSession = {
      sessionId,
      idempotencyKey: input.idempotencyKey,
      providerName: input.providerName,
      model: input.model,
      clientRef: input.clientRef,
      state: AgentSessionState.ACTIVE,
      metadata: cloneRecord(input.metadata),
      visibility: input.visibility,
      preparedWorkspace: cloneMaybe(input.preparedWorkspace),
      toolSource: input.toolSource,
      toolRefs: cloneToolRefs(input.toolRefs),
      listedTools: cloneListedTools(input.listedTools),
      createdBySubjectId: input.createdBySubjectId.trim(),
      createdAt: now,
      updatedAt: now,
      lastTurnAt: undefined,
    };
    this.sessions.set(sessionId, session);
    if (input.idempotencyKey.trim()) {
      this.sessionIdempotency.set(dedupKey, sessionId);
    }
    return { session: cloneSession(session), created: true };
  }

  getSession(sessionId: string): StoredSession | undefined {
    const session = this.sessions.get(sessionId.trim());
    return session ? cloneSession(session) : undefined;
  }

  getSessionByIdempotencyKey(
    createdBySubjectId: string,
    idempotencyKey: string,
  ): StoredSession | undefined {
    const sessionId = this.sessionIdempotency.get(
      sessionDedupKey(createdBySubjectId, idempotencyKey),
    );
    if (!sessionId) {
      return undefined;
    }
    const session = this.sessions.get(sessionId);
    return session ? cloneSession(session) : undefined;
  }

  listSessions(input: {
    sessionIds?: readonly string[];
    subjectId: string | undefined;
    state: AgentSessionState | undefined;
    limit: number | undefined;
  }): StoredSession[] {
    const requested = (input.sessionIds ?? []).map((value) => value.trim()).filter(Boolean);
    let sessions = requested.length
      ? requested.flatMap((id) => {
          const session = this.sessions.get(id);
          return session ? [session] : [];
        })
      : [...this.sessions.values()];
    if (input.subjectId) {
      sessions = sessions.filter(
        (session) =>
          session.createdBySubjectId === input.subjectId ||
          session.visibility === SESSION_VISIBILITY_COMPANY,
      );
    } else {
      sessions = [];
    }
    if (input.state) {
      sessions = sessions.filter((session) => session.state === input.state);
    }
    sessions = sessions.sort((left, right) => {
      const leftTime = left.lastTurnAt ?? left.updatedAt;
      const rightTime = right.lastTurnAt ?? right.updatedAt;
      return rightTime.getTime() - leftTime.getTime();
    });
    if (input.limit && input.limit > 0) {
      sessions = sessions.slice(0, input.limit);
    }
    return sessions.map(cloneSession);
  }

  updateSession(input: {
    sessionId: string;
    clientRef: string | undefined;
    state: AgentSessionState | undefined;
    metadata: Record<string, unknown> | undefined;
  }): StoredSession | undefined {
    const session = this.sessions.get(input.sessionId.trim());
    if (!session) {
      return undefined;
    }
    if (input.clientRef) {
      session.clientRef = input.clientRef;
    }
    if (input.state) {
      session.state = input.state;
    }
    if (input.metadata !== undefined) {
      session.metadata = cloneRecord(input.metadata);
    }
    session.updatedAt = new Date();
    return cloneSession(session);
  }

  beginTurn(input: {
    turnId: string;
    sessionId: string;
    idempotencyKey: string;
    providerName: string;
    model: string;
    messages: readonly AgentMessage[];
    createdBySubjectId: string;
    executionRef: string;
  }): { turn: StoredTurn; created: boolean } {
    const turnId = input.turnId.trim();
    const sessionId = input.sessionId.trim();
    if (!turnId) {
      throw new Error("turn_id is required");
    }
    if (!sessionId) {
      throw new Error("session_id is required");
    }
    const existing = this.turns.get(turnId);
    if (existing) {
      if (existing.sessionId !== sessionId) {
        throw new StoreConflictError(`turn_id ${JSON.stringify(turnId)} already exists for another session`);
      }
      return { turn: cloneTurn(existing), created: false };
    }
    const idempotencyKey = `${sessionId}\n${input.idempotencyKey}`;
    if (input.idempotencyKey && this.turnIdempotency.has(idempotencyKey)) {
      const existingId = this.turnIdempotency.get(idempotencyKey);
      const idempotent = existingId ? this.turns.get(existingId) : undefined;
      if (idempotent) {
        return { turn: cloneTurn(idempotent), created: false };
      }
    }
    const now = new Date();
    const turn: StoredTurn = {
      turnId,
      sessionId,
      idempotencyKey: input.idempotencyKey,
      providerName: input.providerName,
      model: input.model,
      status: AgentExecutionStatus.RUNNING,
      messages: cloneMessages(input.messages),
      output: undefined,
      statusMessage: "",
      createdBySubjectId: input.createdBySubjectId.trim(),
      createdAt: now,
      startedAt: now,
      completedAt: undefined,
      executionRef: input.executionRef,
    };
    this.turns.set(turnId, turn);
    if (input.idempotencyKey) {
      this.turnIdempotency.set(idempotencyKey, turnId);
    }
    const session = this.sessions.get(sessionId);
    if (session) {
      session.lastTurnAt = now;
      session.updatedAt = now;
    }
    this.appendEvent({
      turnId,
      eventType: "turn.started",
      source: input.providerName,
      data: { model: input.model },
    });
    return { turn: cloneTurn(turn), created: true };
  }

  getTurn(turnId: string): StoredTurn | undefined {
    const turn = this.turns.get(turnId.trim());
    return turn ? cloneTurn(turn) : undefined;
  }

  listTurns(input: {
    sessionId: string;
    turnIds: readonly string[] | undefined;
    subjectId: string | undefined;
    status: AgentExecutionStatus | undefined;
    limit: number | undefined;
  }): StoredTurn[] {
    const requested = (input.turnIds ?? []).map((value) => value.trim()).filter(Boolean);
    const sessionId = input.sessionId.trim();
    let turns = requested.length
      ? requested.flatMap((id) => {
          const turn = this.turns.get(id);
          return turn ? [turn] : [];
        })
      : [...this.turns.values()];
    if (sessionId) {
      turns = turns.filter((turn) => turn.sessionId === sessionId);
    } else if (!requested.length) {
      turns = [];
    }
    if (input.subjectId) {
      turns = turns.filter((turn) => turn.createdBySubjectId === input.subjectId);
    }
    if (input.status) {
      turns = turns.filter((turn) => turn.status === input.status);
    }
    turns = turns.sort((left, right) => right.createdAt.getTime() - left.createdAt.getTime());
    if (input.limit && input.limit > 0) {
      turns = turns.slice(0, input.limit);
    }
    return turns.map(cloneTurn);
  }

  completeTurn(turnId: string, output: AgentTurnOutput): StoredTurn | undefined {
    const turn = this.turns.get(turnId.trim());
    if (!turn || TERMINAL_STATUSES.has(turn.status)) {
      return turn ? cloneTurn(turn) : undefined;
    }
    turn.status = AgentExecutionStatus.SUCCEEDED;
    turn.output = cloneMaybe(output);
    turn.completedAt = new Date();
    this.appendEvent({
      turnId: turn.turnId,
      eventType: "assistant.message",
      source: turn.providerName,
      data: assistantMessageData(output),
    });
    this.appendEvent({
      turnId: turn.turnId,
      eventType: "turn.completed",
      source: turn.providerName,
      data: { status: "succeeded" },
    });
    return cloneTurn(turn);
  }

  failTurn(turnId: string, message: string): StoredTurn | undefined {
    const turn = this.turns.get(turnId.trim());
    if (!turn || TERMINAL_STATUSES.has(turn.status)) {
      return turn ? cloneTurn(turn) : undefined;
    }
    turn.status = AgentExecutionStatus.FAILED;
    turn.statusMessage = message;
    turn.completedAt = new Date();
    this.appendEvent({
      turnId: turn.turnId,
      eventType: "turn.failed",
      source: turn.providerName,
      data: { error: message },
    });
    return cloneTurn(turn);
  }

  cancelTurn(turnId: string, reason: string): StoredTurn | undefined {
    const turn = this.turns.get(turnId.trim());
    if (!turn) {
      return undefined;
    }
    if (!TERMINAL_STATUSES.has(turn.status)) {
      turn.status = AgentExecutionStatus.CANCELED;
      turn.statusMessage = reason;
      turn.completedAt = new Date();
      this.appendEvent({
        turnId: turn.turnId,
        eventType: "turn.canceled",
        source: turn.providerName,
        data: { reason },
      });
    }
    return cloneTurn(turn);
  }

  appendEvent(input: {
    turnId: string;
    eventType: string;
    source: string;
    data: Record<string, unknown>;
  }): StoredTurnEvent {
    const events = this.events.get(input.turnId) ?? [];
    this.events.set(input.turnId, events);
    const event: StoredTurnEvent = {
      eventId: `${input.turnId}:${events.length + 1}`,
      turnId: input.turnId,
      seq: BigInt(events.length + 1),
      eventType: input.eventType,
      source: input.source,
      visibility: "external",
      data: cloneRecord(input.data),
      createdAt: new Date(),
    };
    events.push(event);
    return cloneEvent(event);
  }

  listTurnEvents(input: { turnId: string; afterSeq?: bigint; limit?: number }): StoredTurnEvent[] {
    const afterSeq = input.afterSeq ?? 0n;
    let events = (this.events.get(input.turnId.trim()) ?? []).filter(
      (event) => event.seq > afterSeq,
    );
    if (input.limit && input.limit > 0) {
      events = events.slice(0, input.limit);
    }
    return events.map(cloneEvent);
  }
}

export function sessionToAgentSession(session: StoredSession, summaryOnly = false): AgentSession {
  const out: AgentSession = {
    id: session.sessionId,
    providerName: session.providerName,
    model: session.model,
    clientRef: session.clientRef,
    state: session.state,
    createdBySubjectId: session.createdBySubjectId,
    createdAt: new Date(session.createdAt),
    updatedAt: new Date(session.updatedAt),
  };
  if (!summaryOnly && Object.keys(session.metadata).length > 0) {
    out.metadata = session.metadata as JsonObject;
  }
  if (session.lastTurnAt) {
    out.lastTurnAt = new Date(session.lastTurnAt);
  }
  return out;
}

export function turnToAgentTurn(turn: StoredTurn, summaryOnly = false): AgentTurn {
  const out: AgentTurn = {
    id: turn.turnId,
    sessionId: turn.sessionId,
    providerName: turn.providerName,
    model: turn.model,
    status: turn.status,
    messages: summaryOnly ? [] : cloneMessages(turn.messages),
    output: summaryOnly
      ? { case: undefined }
      : (cloneMaybe(turn.output) ?? { case: undefined }),
    statusMessage: turn.statusMessage,
    createdBySubjectId: turn.createdBySubjectId,
    executionRef: turn.executionRef,
    createdAt: new Date(turn.createdAt),
  };
  if (turn.startedAt) {
    out.startedAt = new Date(turn.startedAt);
  }
  if (turn.completedAt) {
    out.completedAt = new Date(turn.completedAt);
  }
  return out;
}

export function turnEventToAgentTurnEvent(event: StoredTurnEvent): AgentTurnEvent {
  return {
    id: event.eventId,
    turnId: event.turnId,
    seq: event.seq,
    type: event.eventType,
    source: event.source,
    visibility: event.visibility,
    data: event.data as JsonObject,
    createdAt: new Date(event.createdAt),
  };
}

function sessionDedupKey(createdBySubjectId: string, idempotencyKey: string): string {
  return `${createdBySubjectId.trim()}${idempotencyKey.trim()}`;
}

export function cloneRecord(value: Record<string, unknown>): Record<string, unknown> {
  return structuredClone(value);
}

function cloneSession(session: StoredSession): StoredSession {
  const copy: StoredSession = {
    ...session,
    metadata: cloneRecord(session.metadata),
    preparedWorkspace: cloneMaybe(session.preparedWorkspace),
    toolRefs: cloneToolRefs(session.toolRefs),
    listedTools: cloneListedTools(session.listedTools),
    createdBySubjectId: session.createdBySubjectId,
    createdAt: new Date(session.createdAt),
    updatedAt: new Date(session.updatedAt),
  };
  if (session.lastTurnAt) {
    copy.lastTurnAt = new Date(session.lastTurnAt);
  }
  return copy;
}

function cloneTurn(turn: StoredTurn): StoredTurn {
  const copy: StoredTurn = {
    ...turn,
    messages: cloneMessages(turn.messages),
    output: cloneMaybe(turn.output),
    createdBySubjectId: turn.createdBySubjectId,
    createdAt: new Date(turn.createdAt),
  };
  if (turn.startedAt) {
    copy.startedAt = new Date(turn.startedAt);
  }
  if (turn.completedAt) {
    copy.completedAt = new Date(turn.completedAt);
  }
  return copy;
}

function cloneEvent(event: StoredTurnEvent): StoredTurnEvent {
  return {
    ...event,
    data: cloneRecord(event.data),
    createdAt: new Date(event.createdAt),
  };
}

function cloneMessages(messages: readonly AgentMessage[]): AgentMessage[] {
  return structuredClone([...messages]);
}

function cloneToolRefs(refs: readonly AgentToolRef[]): AgentToolRef[] {
  return structuredClone([...refs]);
}

function cloneListedTools(tools: readonly ListedAgentTool[]): ListedAgentTool[] {
  return structuredClone([...tools]);
}

function cloneMaybe<T>(value: T | undefined): T | undefined {
  return value === undefined ? undefined : structuredClone(value);
}

function assistantMessageData(output: AgentTurnOutput): Record<string, unknown> {
  if (output.case === "text") {
    return { text: output.value.text };
  }
  if (output.case === "structured") {
    const data: Record<string, unknown> = { text: output.value.text };
    if (output.value.value !== undefined) {
      data.value = cloneMaybe(output.value.value);
    }
    return data;
  }
  throw new Error("completed turn output must include text or structured");
}
