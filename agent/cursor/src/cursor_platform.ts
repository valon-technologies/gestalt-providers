import { randomUUID } from "node:crypto";

import type { AgentOptions } from "@cursor/sdk";

type CursorPlatformOptions = NonNullable<AgentOptions["platform"]>;
type ModelSelection = NonNullable<AgentOptions["model"]>;

type AgentRecord = {
  agentId: string;
  workspaceRef: string;
  status: "IDLE" | "RUNNING" | "ERROR" | "ARCHIVED";
  activeRunId?: string | null;
  latestCheckpointRef?: unknown;
  name?: string | null;
  metadata: Record<string, unknown>;
  createdAt: Date;
  updatedAt: Date;
};

type RunStatus = "QUEUED" | "CREATING" | "RUNNING" | "FINISHED" | "ERROR" | "EXPIRED" | "CANCELLED";

type RunRecord = {
  runId: string;
  agentId: string;
  turnNumber: number;
  status: RunStatus;
  model?: ModelSelection | null;
  startCheckpointRef?: unknown;
  latestCheckpointRef?: unknown;
  errorCode?: string | null;
  usageRef?: string | null;
  createdAt: Date;
  updatedAt: Date;
  startedAt?: Date | null | undefined;
  finishedAt?: Date | null | undefined;
  cancelledAt?: Date | null | undefined;
  expiredAt?: Date | null | undefined;
};

type ListOptions = {
  cursor?: string;
  limit?: number;
};

export function createCursorPlatformOptions(workspaceRef: string): CursorPlatformOptions {
  return {
    workspaceRef,
    store: new InMemoryAgentRunStore(workspaceRef),
    checkpointStore: new InMemoryCheckpointStore(),
    eventNotifier: new NoopRunEventNotifier(),
  } as CursorPlatformOptions;
}

class InMemoryAgentRunStore {
  private readonly agents = new Map<string, AgentRecord>();
  private readonly runs = new Map<string, RunRecord[]>();

  constructor(private readonly defaultWorkspaceRef: string) {}

  async createAgent(input: {
    agentId?: string;
    workspaceRef?: string;
    model?: ModelSelection;
    name?: string;
    metadata?: Record<string, unknown>;
  }): Promise<{ agent: AgentRecord; run: RunRecord }> {
    const now = new Date();
    const agentId = input.agentId || randomUUID();
    const run = this.createRunRecord(agentId, 1, input.model, now);
    const agent: AgentRecord = {
      agentId,
      workspaceRef: input.workspaceRef || this.defaultWorkspaceRef,
      status: "IDLE",
      activeRunId: run.runId,
      latestCheckpointRef: null,
      name: input.name ?? null,
      metadata: { ...(input.metadata ?? {}) },
      createdAt: now,
      updatedAt: now,
    };
    this.agents.set(agentId, agent);
    this.runs.set(agentId, [run]);
    return { agent: cloneAgent(agent), run: cloneRun(run) };
  }

  async createFollowUpRun(
    agentId: string,
    input: { model?: ModelSelection },
  ): Promise<RunRecord> {
    const agent = this.requireAgent(agentId);
    if (agent.status === "ARCHIVED") {
      throw new Error(`Cannot create follow-up run on archived agent ${agentId}. Unarchive it first.`);
    }
    const runs = this.runs.get(agentId) ?? [];
    const activeRun = agent.activeRunId
      ? runs.find((run) => run.runId === agent.activeRunId)
      : undefined;
    if (activeRun && !isTerminalRunStatus(activeRun.status)) {
      throw new Error(`Agent ${agentId} already has active run`);
    }
    const now = new Date();
    const run = this.createRunRecord(agentId, runs.length + 1, input.model, now);
    run.startCheckpointRef = agent.latestCheckpointRef ?? null;
    run.latestCheckpointRef = agent.latestCheckpointRef ?? null;
    runs.push(run);
    this.runs.set(agentId, runs);
    agent.activeRunId = run.runId;
    agent.status = "IDLE";
    agent.updatedAt = now;
    return cloneRun(run);
  }

  async listAgents(options?: ListOptions): Promise<{ items: AgentRecord[]; nextCursor?: string }> {
    const { items, nextCursor } = paginate(
      [...this.agents.values()].sort((left, right) => right.updatedAt.getTime() - left.updatedAt.getTime()),
      options,
    );
    return { items: items.map(cloneAgent), ...(nextCursor ? { nextCursor } : {}) };
  }

  async getAgent(agentId: string): Promise<AgentRecord | undefined> {
    const agent = this.agents.get(agentId);
    return agent ? cloneAgent(agent) : undefined;
  }

  async listRuns(
    agentId: string,
    options?: ListOptions,
  ): Promise<{ items: RunRecord[]; nextCursor?: string }> {
    const { items, nextCursor } = paginate(this.runs.get(agentId) ?? [], options);
    return { items: items.map(cloneRun), ...(nextCursor ? { nextCursor } : {}) };
  }

  async getRun(agentId: string, runId: string): Promise<RunRecord | undefined> {
    const run = (this.runs.get(agentId) ?? []).find((candidate) => candidate.runId === runId);
    return run ? cloneRun(run) : undefined;
  }

  async markRunStarting(agentId: string, runId: string): Promise<void> {
    const agent = this.requireAgent(agentId);
    const run = this.requireRun(agentId, runId);
    const now = new Date();
    if (isTerminalRunStatus(run.status)) {
      throw new Error(`Cannot start terminal run ${runId}`);
    }
    run.status = "RUNNING";
    run.startedAt ??= now;
    run.updatedAt = now;
    agent.status = "RUNNING";
    agent.activeRunId = runId;
    agent.updatedAt = now;
  }

  async patchCheckpoint(agentId: string, runId: string, checkpointRef: unknown): Promise<void> {
    const agent = this.requireAgent(agentId);
    const run = this.requireRun(agentId, runId);
    const now = new Date();
    run.latestCheckpointRef = checkpointRef;
    run.updatedAt = now;
    agent.latestCheckpointRef = checkpointRef;
    agent.updatedAt = now;
  }

  async markRunTerminal(
    agentId: string,
    runId: string,
    patch: { status: RunStatus; errorCode?: string; usageRef?: string },
  ): Promise<void> {
    const agent = this.requireAgent(agentId);
    const run = this.requireRun(agentId, runId);
    if (isTerminalRunStatus(run.status)) {
      return;
    }
    const now = new Date();
    run.status = patch.status;
    run.errorCode = patch.errorCode ?? run.errorCode ?? null;
    run.usageRef = patch.usageRef ?? run.usageRef ?? null;
    run.updatedAt = now;
    if (patch.status === "FINISHED") {
      run.finishedAt = now;
    } else if (patch.status === "CANCELLED") {
      run.cancelledAt = now;
    } else if (patch.status === "EXPIRED") {
      run.expiredAt = now;
    }
    agent.activeRunId = null;
    agent.status = patch.status === "ERROR" ? "ERROR" : "IDLE";
    agent.updatedAt = now;
  }

  async cancelRun(agentId: string, runId: string): Promise<void> {
    await this.markRunTerminal(agentId, runId, { status: "CANCELLED" });
  }

  async archiveAgent(agentId: string): Promise<void> {
    const agent = this.requireAgent(agentId);
    agent.status = "ARCHIVED";
    agent.updatedAt = new Date();
  }

  async unarchiveAgent(agentId: string): Promise<void> {
    const agent = this.requireAgent(agentId);
    agent.status = agent.activeRunId ? "RUNNING" : "IDLE";
    agent.updatedAt = new Date();
  }

  async deleteAgent(agentId: string): Promise<void> {
    this.agents.delete(agentId);
    this.runs.delete(agentId);
  }

  private createRunRecord(
    agentId: string,
    turnNumber: number,
    model: ModelSelection | undefined,
    now: Date,
  ): RunRecord {
    return {
      runId: randomUUID(),
      agentId,
      turnNumber,
      status: "QUEUED",
      model: model ?? null,
      startCheckpointRef: null,
      latestCheckpointRef: null,
      errorCode: null,
      usageRef: null,
      createdAt: now,
      updatedAt: now,
      startedAt: null,
      finishedAt: null,
      cancelledAt: null,
      expiredAt: null,
    };
  }

  private requireAgent(agentId: string): AgentRecord {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent ${agentId} not found`);
    }
    return agent;
  }

  private requireRun(agentId: string, runId: string): RunRecord {
    const run = (this.runs.get(agentId) ?? []).find((candidate) => candidate.runId === runId);
    if (!run) {
      throw new Error(`Run ${runId} not found`);
    }
    return run;
  }
}

class InMemoryCheckpointStore {
  private readonly checkpoints = new Map<string, unknown>();
  private readonly blobs = new Map<string, InMemoryBlobStore>();

  async loadLatest(agentId: string): Promise<unknown> {
    return this.checkpoints.get(agentId) ?? null;
  }

  async saveCheckpoint(agentId: string, checkpoint: unknown): Promise<Record<string, unknown>> {
    const ref = { blobId: randomUUID(), storeKind: "gestalt-memory" };
    this.checkpoints.set(agentId, checkpoint);
    return ref;
  }

  async getBlobStore(agentId: string): Promise<InMemoryBlobStore> {
    const existing = this.blobs.get(agentId);
    if (existing) {
      return existing;
    }
    const store = new InMemoryBlobStore();
    this.blobs.set(agentId, store);
    return store;
  }

  async getFullConversation(_agentId: string): Promise<{ turns: unknown[] }> {
    return { turns: [] };
  }

  async deleteAgent(agentId: string): Promise<void> {
    this.checkpoints.delete(agentId);
    this.blobs.delete(agentId);
  }
}

class InMemoryBlobStore {
  private readonly blobs = new Map<string, unknown>();

  async getBlob(_namespace: unknown, key: Uint8Array): Promise<unknown> {
    return this.blobs.get(blobKey(key));
  }

  async setBlob(_namespace: unknown, key: Uint8Array, value: unknown): Promise<void> {
    this.blobs.set(blobKey(key), value);
  }

  async setBlobLocallyOnly(namespace: unknown, key: Uint8Array, value: unknown): Promise<void> {
    await this.setBlob(namespace, key, value);
  }

  async flush(_namespace: unknown): Promise<void> {}

  async dispose(): Promise<void> {
    this.blobs.clear();
  }
}

class NoopRunEventNotifier {
  async publishRunEventAppended(_event: unknown): Promise<void> {}

  subscribeRunEvents(): AsyncIterable<never> & { ready: Promise<void> } {
    return {
      ready: Promise.resolve(),
      async *[Symbol.asyncIterator]() {},
    };
  }
}

function isTerminalRunStatus(status: RunStatus): boolean {
  return status === "FINISHED" || status === "ERROR" || status === "EXPIRED" || status === "CANCELLED";
}

function paginate<T>(items: T[], options?: ListOptions): { items: T[]; nextCursor?: string } {
  const limit = options?.limit && options.limit > 0 ? options.limit : 50;
  const offset = options?.cursor ? Number.parseInt(options.cursor, 10) : 0;
  const start = Number.isFinite(offset) && offset > 0 ? offset : 0;
  const page = items.slice(start, start + limit);
  const next = start + limit < items.length ? String(start + limit) : undefined;
  return { items: page, ...(next ? { nextCursor: next } : {}) };
}

function cloneAgent(agent: AgentRecord): AgentRecord {
  return {
    ...agent,
    metadata: { ...agent.metadata },
    createdAt: new Date(agent.createdAt),
    updatedAt: new Date(agent.updatedAt),
  };
}

function cloneRun(run: RunRecord): RunRecord {
  return {
    ...run,
    createdAt: new Date(run.createdAt),
    updatedAt: new Date(run.updatedAt),
    startedAt: cloneMaybeDate(run.startedAt),
    finishedAt: cloneMaybeDate(run.finishedAt),
    cancelledAt: cloneMaybeDate(run.cancelledAt),
    expiredAt: cloneMaybeDate(run.expiredAt),
  };
}

function cloneMaybeDate(value: Date | null | undefined): Date | null | undefined {
  return value ? new Date(value) : value;
}

function blobKey(value: Uint8Array): string {
  return Buffer.from(value).toString("hex");
}
