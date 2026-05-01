import type { Timestamp } from "@bufbuild/protobuf/wkt";
import {
  AgentExecutionStatus,
  AgentSessionState,
  type AgentActor,
  type AgentMessage,
  type AgentProviderOptions,
  type AgentSession,
  type AgentTurn,
  type AgentTurnEvent,
} from "@valon-technologies/gestalt";

type AgentSessionInit = Awaited<ReturnType<NonNullable<AgentProviderOptions["createSession"]>>>;
type AgentTurnInit = Awaited<ReturnType<NonNullable<AgentProviderOptions["createTurn"]>>>;
type AgentTurnEventInit = Awaited<ReturnType<NonNullable<AgentProviderOptions["listTurnEvents"]>>>[number];

export const TERMINAL_STATUSES = new Set<AgentExecutionStatus>([
  AgentExecutionStatus.SUCCEEDED,
  AgentExecutionStatus.FAILED,
  AgentExecutionStatus.CANCELED,
]);

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
  createdBy: AgentActor | undefined;
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
  outputText: string;
  statusMessage: string;
  createdBy: AgentActor | undefined;
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
    sessionId: string;
    idempotencyKey: string;
    providerName: string;
    model: string;
    clientRef: string;
    metadata: Record<string, unknown>;
    createdBy: AgentActor | undefined;
  }): { session: StoredSession; created: boolean } {
    const sessionId = input.sessionId.trim();
    if (!sessionId) {
      throw new Error("session_id is required");
    }
    const existing = this.sessions.get(sessionId);
    if (existing) {
      return { session: cloneSession(existing), created: false };
    }
    if (input.idempotencyKey) {
      const existingId = this.sessionIdempotency.get(input.idempotencyKey);
      if (existingId) {
        const idempotent = this.sessions.get(existingId);
        if (idempotent) {
          return { session: cloneSession(idempotent), created: false };
        }
      }
    }
    const now = new Date();
    const session: StoredSession = {
      sessionId,
      idempotencyKey: input.idempotencyKey,
      providerName: input.providerName,
      model: input.model,
      clientRef: input.clientRef,
      state: AgentSessionState.ACTIVE,
      metadata: cloneRecord(input.metadata),
      createdBy: cloneMaybe(input.createdBy),
      createdAt: now,
      updatedAt: now,
      lastTurnAt: undefined,
    };
    this.sessions.set(sessionId, session);
    if (input.idempotencyKey) {
      this.sessionIdempotency.set(input.idempotencyKey, sessionId);
    }
    return { session: cloneSession(session), created: true };
  }

  getSession(sessionId: string): StoredSession | undefined {
    const session = this.sessions.get(sessionId.trim());
    return session ? cloneSession(session) : undefined;
  }

  listSessions(input: {
    sessionIds?: string[];
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
      sessions = sessions.filter((session) => session.createdBy?.subjectId === input.subjectId);
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
    messages: AgentMessage[];
    createdBy: AgentActor | undefined;
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
      outputText: "",
      statusMessage: "",
      createdBy: cloneMaybe(input.createdBy),
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
    turnIds: string[] | undefined;
    subjectId: string | undefined;
    status: AgentExecutionStatus | undefined;
    limit: number | undefined;
  }): StoredTurn[] {
    const requested = (input.turnIds ?? []).map((value) => value.trim()).filter(Boolean);
    let turns = requested.length
      ? requested.flatMap((id) => {
          const turn = this.turns.get(id);
          return turn ? [turn] : [];
        })
      : [...this.turns.values()].filter((turn) => turn.sessionId === input.sessionId.trim());
    if (input.subjectId) {
      turns = turns.filter((turn) => turn.createdBy?.subjectId === input.subjectId);
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

  completeTurn(turnId: string, outputText: string): StoredTurn | undefined {
    const turn = this.turns.get(turnId.trim());
    if (!turn || TERMINAL_STATUSES.has(turn.status)) {
      return turn ? cloneTurn(turn) : undefined;
    }
    turn.status = AgentExecutionStatus.SUCCEEDED;
    turn.outputText = outputText;
    turn.completedAt = new Date();
    this.appendEvent({
      turnId: turn.turnId,
      eventType: "assistant.message",
      source: turn.providerName,
      data: { text: outputText },
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

export function sessionToProto(session: StoredSession, summaryOnly = false): AgentSessionInit {
  const out: AgentSessionInit = {
    id: session.sessionId,
    providerName: session.providerName,
    model: session.model,
    clientRef: session.clientRef,
    state: session.state,
    createdAt: timestamp(session.createdAt),
    updatedAt: timestamp(session.updatedAt),
  };
  if (!summaryOnly && Object.keys(session.metadata).length > 0) {
    out.metadata = session.metadata as AgentSession["metadata"];
  }
  if (session.createdBy) {
    out.createdBy = session.createdBy;
  }
  if (session.lastTurnAt) {
    out.lastTurnAt = timestamp(session.lastTurnAt);
  }
  return out;
}

export function turnToProto(turn: StoredTurn, summaryOnly = false): AgentTurnInit {
  const out: AgentTurnInit = {
    id: turn.turnId,
    sessionId: turn.sessionId,
    providerName: turn.providerName,
    model: turn.model,
    status: turn.status,
    outputText: summaryOnly ? "" : turn.outputText,
    statusMessage: turn.statusMessage,
    executionRef: turn.executionRef,
    createdAt: timestamp(turn.createdAt),
  };
  if (!summaryOnly) {
    out.messages = cloneMessages(turn.messages);
  }
  if (turn.createdBy) {
    out.createdBy = turn.createdBy;
  }
  if (turn.startedAt) {
    out.startedAt = timestamp(turn.startedAt);
  }
  if (turn.completedAt) {
    out.completedAt = timestamp(turn.completedAt);
  }
  return out;
}

export function turnEventToProto(event: StoredTurnEvent): AgentTurnEventInit {
  return {
    id: event.eventId,
    turnId: event.turnId,
    seq: event.seq,
    type: event.eventType,
    source: event.source,
    visibility: event.visibility,
    data: event.data as AgentTurnEvent["data"],
    createdAt: timestamp(event.createdAt),
  };
}

export function cloneRecord(value: Record<string, unknown>): Record<string, unknown> {
  return JSON.parse(JSON.stringify(value)) as Record<string, unknown>;
}

function cloneSession(session: StoredSession): StoredSession {
  const copy: StoredSession = {
    ...session,
    metadata: cloneRecord(session.metadata),
    createdBy: cloneMaybe(session.createdBy),
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
    createdBy: cloneMaybe(turn.createdBy),
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

function cloneMessages(messages: AgentMessage[]): AgentMessage[] {
  return JSON.parse(JSON.stringify(messages)) as AgentMessage[];
}

function cloneMaybe<T>(value: T | undefined): T | undefined {
  return value === undefined ? undefined : (JSON.parse(JSON.stringify(value)) as T);
}

function timestamp(date: Date): Timestamp {
  const milliseconds = date.getTime();
  return {
    seconds: BigInt(Math.floor(milliseconds / 1000)),
    nanos: (milliseconds % 1000) * 1_000_000,
  } as Timestamp;
}
