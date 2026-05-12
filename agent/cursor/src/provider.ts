import { Code, ConnectError } from "@connectrpc/connect";
import {
  AgentExecutionStatus,
  AgentProvider as SDKAgentProvider,
  AgentSessionState,
  AgentToolSourceMode,
  slugName,
  type AgentInteraction,
  type AgentProviderCapabilities,
  type AgentProviderOptions,
  type AgentSession,
  type AgentTurn,
  type AgentTurnEvent,
  type CancelAgentProviderTurnRequest,
  type CreateAgentProviderSessionRequest,
  type CreateAgentProviderTurnRequest,
  type GetAgentProviderCapabilitiesRequest,
  type GetAgentProviderInteractionRequest,
  type GetAgentProviderSessionRequest,
  type GetAgentProviderTurnRequest,
  type ListAgentProviderInteractionsRequest,
  type ListAgentProviderSessionsRequest,
  type ListAgentProviderTurnEventsRequest,
  type ListAgentProviderTurnsRequest,
  type ResolveAgentProviderInteractionRequest,
  type UpdateAgentProviderSessionRequest,
} from "@valon-technologies/gestalt";

import {
  parseCursorAgentConfig,
  resolveModel,
  type CursorAgentConfig,
} from "./config.ts";
import { CursorExecutionCanceled, CursorExecutionError } from "./errors.ts";
import { CursorSDKRunner } from "./runner.ts";
import {
  prependSessionStartContext,
  runSessionStartHooks,
  validateSessionStartUserMetadata,
} from "./session_start.ts";
import {
  InMemoryRunStore,
  StoreConflictError,
  type PreparedWorkspace,
  sessionToAgentSession,
  turnEventToAgentTurnEvent,
  turnToAgentTurn,
} from "./store.ts";

export type CursorAgentProviderDependencies = {
  store?: InMemoryRunStore;
  runnerFactory?: (config: CursorAgentConfig) => CursorSDKRunner;
};

type AgentSessionInit = Awaited<
  ReturnType<NonNullable<AgentProviderOptions["createSession"]>>
>;
type AgentSessionListInit = Awaited<
  ReturnType<NonNullable<AgentProviderOptions["listSessions"]>>
>;
type AgentTurnInit = Awaited<
  ReturnType<NonNullable<AgentProviderOptions["createTurn"]>>
>;
type AgentTurnListInit = Awaited<
  ReturnType<NonNullable<AgentProviderOptions["listTurns"]>>
>;
type AgentTurnEventListInit = Awaited<
  ReturnType<NonNullable<AgentProviderOptions["listTurnEvents"]>>
>;
type AgentInteractionInit = Awaited<
  ReturnType<NonNullable<AgentProviderOptions["getInteraction"]>>
>;
type AgentInteractionListInit = Awaited<
  ReturnType<NonNullable<AgentProviderOptions["listInteractions"]>>
>;
type AgentCapabilitiesInit = Awaited<
  ReturnType<NonNullable<AgentProviderOptions["getCapabilities"]>>
>;

export class CursorAgentProvider extends SDKAgentProvider {
  private config?: CursorAgentConfig;
  private runner?: CursorSDKRunner;
  private readonly store: InMemoryRunStore;
  private readonly runnerFactory:
    | ((config: CursorAgentConfig) => CursorSDKRunner)
    | undefined;
  private sessionStartLock: Promise<void> = Promise.resolve();

  constructor(dependencies: CursorAgentProviderDependencies = {}) {
    super({
      displayName: "Cursor Agent SDK",
      description: "Runs the Cursor Agent SDK with Gestalt MCP catalog tools.",
      version: "0.0.1-alpha.1",
    });
    this.store = dependencies.store ?? new InMemoryRunStore();
    this.runnerFactory = dependencies.runnerFactory;
  }

  override async configureProvider(
    name: string,
    rawConfig: Record<string, unknown>,
  ): Promise<void> {
    const config = parseCursorAgentConfig(rawConfig);
    if (name.trim()) {
      this.name = slugName(name);
    }
    await this.runner?.close();
    this.store.close();
    this.config = config;
    this.runner = this.runnerFactory
      ? this.runnerFactory(this.config)
      : new CursorSDKRunner(this.config);
  }

  override async warnings(): Promise<string[]> {
    const config = this.config;
    if (!config) {
      return [];
    }
    if (!config.cursorApiKey && !process.env.CURSOR_API_KEY) {
      return [
        "set config.cursorApiKey or CURSOR_API_KEY before running live Cursor turns",
      ];
    }
    return [];
  }

  override async closeProvider(): Promise<void> {
    await this.runner?.close();
    this.store.close();
  }

  async createSession(
    request: CreateAgentProviderSessionRequest,
  ): Promise<AgentSessionInit> {
    const { config } = this.requireRuntime();
    const model = modelFor(config, request.model);
    try {
      const sessionStart = (request as { sessionStart?: unknown }).sessionStart;
      let metadata = objectOrEmpty(request.metadata);
      validateSessionStartUserMetadata(metadata);
      const preparedWorkspace = preparedWorkspaceFromRequest(request);
      if (hasSessionStartHooks(sessionStart)) {
        return await this.withSessionStartLock(async () => {
          const existing = this.existingSessionForCreate(
            request.sessionId,
            request.idempotencyKey,
          );
          if (existing) {
            return sessionToAgentSession(existing);
          }
          metadata = await runSessionStartHooks(sessionStart, metadata);
          const { session } = this.store.createSession({
            sessionId: request.sessionId,
            idempotencyKey: request.idempotencyKey,
            providerName: this.name,
            model,
            clientRef: request.clientRef,
            metadata,
            preparedWorkspace,
            createdBy: request.createdBy,
          });
          return sessionToAgentSession(session);
        });
      }
      const { session } = this.store.createSession({
        sessionId: request.sessionId,
        idempotencyKey: request.idempotencyKey,
        providerName: this.name,
        model,
        clientRef: request.clientRef,
        metadata,
        preparedWorkspace,
        createdBy: request.createdBy,
      });
      return sessionToAgentSession(session);
    } catch (error) {
      if (errorMessage(error).startsWith("sessionStart hook")) {
        throw new ConnectError(errorMessage(error), Code.FailedPrecondition);
      }
      throw invalidArgument(errorMessage(error));
    }
  }

  async getSession(
    request: GetAgentProviderSessionRequest,
  ): Promise<AgentSessionInit> {
    this.requireRuntime();
    const session = this.store.getSession(request.sessionId);
    if (!session) {
      throw notFound(
        `agent session ${JSON.stringify(request.sessionId)} was not found`,
      );
    }
    return sessionToAgentSession(session);
  }

  async listSessions(
    request: ListAgentProviderSessionsRequest,
  ): Promise<AgentSessionListInit> {
    this.requireRuntime();
    if (request.limit < 0) {
      throw invalidArgument("limit must be non-negative");
    }
    return this.store
      .listSessions({
        sessionIds: request.sessionIds,
        subjectId: request.subject?.subjectId,
        state: request.state,
        limit: request.limit,
      })
      .map((session) => sessionToAgentSession(session, request.summaryOnly));
  }

  async updateSession(
    request: UpdateAgentProviderSessionRequest,
  ): Promise<AgentSessionInit> {
    this.requireRuntime();
    const metadata =
      request.metadata === undefined ? undefined : objectOrEmpty(request.metadata);
    try {
      validateSessionStartUserMetadata(metadata);
    } catch (error) {
      throw invalidArgument(errorMessage(error));
    }
    const session = this.store.updateSession({
      sessionId: request.sessionId,
      clientRef: request.clientRef,
      state: request.state || undefined,
      metadata,
    });
    if (!session) {
      throw notFound(
        `agent session ${JSON.stringify(request.sessionId)} was not found`,
      );
    }
    return sessionToAgentSession(session);
  }

  async createTurn(
    request: CreateAgentProviderTurnRequest,
  ): Promise<AgentTurnInit> {
    const { config, runner } = this.requireRuntime();
    validateCreateTurnRequest(request);
    const session = this.store.getSession(request.sessionId);
    if (!session) {
      throw notFound(
        `agent session ${JSON.stringify(request.sessionId)} was not found`,
      );
    }
    if (request.messages.length === 0) {
      throw invalidArgument("messages must contain at least one entry");
    }

    const model = modelFor(config, request.model || session.model);
    let turn;
    let created = false;
    try {
      const result = this.store.beginTurn({
        turnId: request.turnId,
        sessionId: request.sessionId,
        idempotencyKey: request.idempotencyKey,
        providerName: this.name,
        model,
        messages: prependSessionStartContext(request.messages, session.metadata),
        createdBy: request.createdBy,
        executionRef: request.executionRef,
      });
      turn = result.turn;
      created = result.created;
    } catch (error) {
      if (error instanceof StoreConflictError) {
        throw new ConnectError(error.message, Code.AlreadyExists);
      }
      throw invalidArgument(errorMessage(error));
    }

    if (created) {
      void this.completeTurn({
        runner,
        turnId: turn.turnId,
        sessionId: turn.sessionId,
        model,
        messages: turn.messages,
        runGrant: request.runGrant.trim(),
        cwd: session.preparedWorkspace?.cwd ?? "",
      });
    }
    return turnToAgentTurn(turn);
  }

  async getTurn(request: GetAgentProviderTurnRequest): Promise<AgentTurnInit> {
    this.requireRuntime();
    const turn = this.store.getTurn(request.turnId);
    if (!turn) {
      throw notFound(
        `agent turn ${JSON.stringify(request.turnId)} was not found`,
      );
    }
    return turnToAgentTurn(turn);
  }

  async listTurns(
    request: ListAgentProviderTurnsRequest,
  ): Promise<AgentTurnListInit> {
    this.requireRuntime();
    if (request.limit < 0) {
      throw invalidArgument("limit must be non-negative");
    }
    return this.store
      .listTurns({
        sessionId: request.sessionId,
        turnIds: request.turnIds,
        subjectId: request.subject?.subjectId,
        status: request.status,
        limit: request.limit,
      })
      .map((turn) => turnToAgentTurn(turn, request.summaryOnly));
  }

  async cancelTurn(
    request: CancelAgentProviderTurnRequest,
  ): Promise<AgentTurnInit> {
    const { runner } = this.requireRuntime();
    const turn = this.store.cancelTurn(request.turnId, request.reason);
    if (!turn) {
      throw notFound(
        `agent turn ${JSON.stringify(request.turnId)} was not found`,
      );
    }
    if (turn.status === AgentExecutionStatus.CANCELED) {
      void runner.cancelTurn(turn.turnId);
    }
    return turnToAgentTurn(turn);
  }

  async listTurnEvents(
    request: ListAgentProviderTurnEventsRequest,
  ): Promise<AgentTurnEventListInit> {
    this.requireRuntime();
    return this.store
      .listTurnEvents({
        turnId: request.turnId,
        afterSeq: request.afterSeq,
        limit: request.limit,
      })
      .map(turnEventToAgentTurnEvent);
  }

  async getInteraction(
    request: GetAgentProviderInteractionRequest,
  ): Promise<AgentInteractionInit> {
    this.requireRuntime();
    throw notFound(
      `agent interaction ${JSON.stringify(request.interactionId)} was not found`,
    );
  }

  async listInteractions(
    _request: ListAgentProviderInteractionsRequest,
  ): Promise<AgentInteractionListInit> {
    this.requireRuntime();
    return [];
  }

  async resolveInteraction(
    request: ResolveAgentProviderInteractionRequest,
  ): Promise<AgentInteractionInit> {
    this.requireRuntime();
    throw notFound(
      `agent interaction ${JSON.stringify(request.interactionId)} was not found`,
    );
  }

  async getCapabilities(
    _request?: GetAgentProviderCapabilitiesRequest,
  ): Promise<AgentCapabilitiesInit> {
    this.requireRuntime();
    return {
      streamingText: false,
      toolCalls: true,
      parallelToolCalls: false,
      structuredOutput: false,
      interactions: false,
      resumableTurns: false,
      reasoningSummaries: false,
      supportsSessionStart: true,
      supportsPreparedWorkspace: true,
      boundedListHydration: true,
      supportedToolSources: [AgentToolSourceMode.MCP_CATALOG],
    } as AgentCapabilitiesInit;
  }

  private requireRuntime(): {
    config: CursorAgentConfig;
    runner: CursorSDKRunner;
  } {
    if (!this.config || !this.runner) {
      throw new ConnectError(
        "agent provider has not been configured",
        Code.FailedPrecondition,
      );
    }
    return { config: this.config, runner: this.runner };
  }

  private async completeTurn(input: {
    runner: CursorSDKRunner;
    turnId: string;
    sessionId: string;
    model: string;
    messages: CreateAgentProviderTurnRequest["messages"];
    runGrant: string;
    cwd: string;
  }): Promise<void> {
    try {
      const output = await input.runner.runTurn({
        sessionId: input.sessionId,
        turnId: input.turnId,
        model: input.model,
        messages: input.messages,
        runGrant: input.runGrant,
        cwd: input.cwd,
        onEvent: (eventType, data) => {
          this.store.appendEvent({
            turnId: input.turnId,
            eventType,
            source: this.name,
            data,
          });
        },
      });
      this.store.completeTurn(input.turnId, output);
    } catch (error) {
      if (error instanceof CursorExecutionCanceled) {
        this.store.cancelTurn(input.turnId, error.message);
        return;
      }
      if (error instanceof CursorExecutionError) {
        this.store.failTurn(input.turnId, error.message);
        return;
      }
      this.store.failTurn(input.turnId, errorMessage(error));
    }
  }

  private existingSessionForCreate(
    sessionId: string,
    idempotencyKey: string,
  ): ReturnType<InMemoryRunStore["getSession"]> {
    return (
      this.store.getSession(sessionId) ??
      (idempotencyKey
        ? this.store.getSessionByIdempotencyKey(idempotencyKey)
        : undefined)
    );
  }

  private async withSessionStartLock<T>(callback: () => Promise<T>): Promise<T> {
    let release: () => void = () => {};
    const previous = this.sessionStartLock;
    this.sessionStartLock = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await callback();
    } finally {
      release();
    }
  }
}

export function createCursorAgentProvider(
  dependencies: CursorAgentProviderDependencies = {},
): CursorAgentProvider {
  return new CursorAgentProvider(dependencies);
}

export const provider = createCursorAgentProvider();

function validateCreateTurnRequest(
  request: CreateAgentProviderTurnRequest,
): void {
  if (request.toolSource !== AgentToolSourceMode.MCP_CATALOG) {
    throw invalidArgument("agent/cursor requires toolSource mcp_catalog");
  }
  if (!request.runGrant.trim()) {
    throw invalidArgument("run_grant is required");
  }
  if (request.tools.length > 0) {
    throw invalidArgument(
      "resolved tools are not supported; use tool_refs with mcp_catalog",
    );
  }
  if (hasObjectData(request.responseSchema)) {
    throw invalidArgument("response_schema is not supported by agent/cursor");
  }
  if (hasObjectData(request.modelOptions)) {
    throw invalidArgument("model_options are not supported by agent/cursor");
  }
  validateToolRefs(request.toolRefs);
}

function validateToolRefs(
  toolRefs: CreateAgentProviderTurnRequest["toolRefs"],
): void {
  if (toolRefs.length === 0) {
    throw invalidArgument("tool_refs are required for mcp_catalog turns");
  }
  toolRefs.forEach((ref, index) => {
    const plugin = (ref.plugin ?? "").trim();
    const system = (ref.system ?? "").trim();
    const operation = (ref.operation ?? "").trim();
    const connection = (ref.connection ?? "").trim();
    const instance = (ref.instance ?? "").trim();
    const label = `tool_refs[${index + 1}]`;
    if (!operation) {
      throw invalidArgument(`${label}.operation is required`);
    }
    if ([plugin, system, operation, connection, instance].includes("*")) {
      throw invalidArgument("wildcard tool_refs are not supported");
    }
    if (Boolean(plugin) === Boolean(system)) {
      throw invalidArgument(
        `${label} must set exactly one of plugin or system`,
      );
    }
    if (system && system !== "workflow") {
      throw invalidArgument(
        `${label}.system ${JSON.stringify(system)} is not supported`,
      );
    }
  });
}

function modelFor(config: CursorAgentConfig, requested: string): string {
  try {
    return resolveModel(config, requested.trim());
  } catch (error) {
    throw invalidArgument(errorMessage(error));
  }
}

function objectOrEmpty(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  return value as Record<string, unknown>;
}

function preparedWorkspaceFromRequest(
  request: CreateAgentProviderSessionRequest,
): PreparedWorkspace | undefined {
  const workspace = (request as { preparedWorkspace?: unknown }).preparedWorkspace;
  if (!workspace || typeof workspace !== "object" || Array.isArray(workspace)) {
    return undefined;
  }
  const root = String((workspace as { root?: unknown }).root ?? "").trim();
  const cwd = String((workspace as { cwd?: unknown }).cwd ?? "").trim();
  if (!root && !cwd) {
    return undefined;
  }
  if (!root || !cwd) {
    throw invalidArgument("preparedWorkspace root and cwd are required");
  }
  return { root, cwd };
}

function hasObjectData(value: unknown): boolean {
  return Object.keys(objectOrEmpty(value)).length > 0;
}

function hasSessionStartHooks(value: unknown): boolean {
  if (!value || typeof value !== "object") {
    return false;
  }
  const hooks = (value as { hooks?: unknown }).hooks;
  return Array.isArray(hooks) && hooks.length > 0;
}

function invalidArgument(message: string): ConnectError {
  return new ConnectError(message, Code.InvalidArgument);
}

function notFound(message: string): ConnectError {
  return new ConnectError(message, Code.NotFound);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
