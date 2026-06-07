import { Code, ConnectError } from "@connectrpc/connect";
import {
  AgentExecutionStatus,
  AgentProvider as SDKAgentProvider,
  AgentSessionState,
  AgentToolSourceMode,
  slugName,
  type AgentInteraction,
  type AgentMessage,
  type AgentProviderCapabilities,
  type AgentSession,
  type AgentSessionStartConfig,
  type AgentToolRef,
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
import { CursorSDKRunner, validateSchema } from "./runner.ts";
import {
  prependSessionStartContext,
  runSessionStartHooks,
  validateSessionStartUserMetadata,
} from "./session_start.ts";
import {
  InMemoryRunStore,
  SESSION_VISIBILITY_COMPANY,
  SESSION_VISIBILITY_PRIVATE,
  StoreConflictError,
  type PreparedWorkspace,
  type StoredSession,
  type StoredSessionVisibility,
  sessionToAgentSession,
  turnEventToAgentTurnEvent,
  turnToAgentTurn,
} from "./store.ts";

export type CursorAgentProviderDependencies = {
  store?: InMemoryRunStore;
  runnerFactory?: (config: CursorAgentConfig) => CursorSDKRunner;
};

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
  ): Promise<AgentSession> {
    const { config } = this.requireRuntime();
    const model = modelFor(config, request.model);
    try {
      let metadata = objectOrEmpty(request.metadata);
      validateSessionStartUserMetadata(metadata);
      const preparedWorkspace = preparedWorkspaceFromRequest(request);
      const { toolSource, toolRefs } = sessionToolScopeFromConfig(request.tools);
      if (hasSessionStartHooks(request.sessionStart)) {
        return await this.withSessionStartLock(async () => {
          const existing = this.existingSessionForCreate(
            request.sessionId,
            request.idempotencyKey,
          );
          if (existing) {
            this.requireReadableSession(existing, request.subject);
            return sessionToAgentSession(existing);
          }
          metadata = await runSessionStartHooks(request.sessionStart, metadata);
          const { session } = this.store.createSession({
            sessionId: request.sessionId,
            idempotencyKey: request.idempotencyKey,
            providerName: this.name,
            model,
            clientRef: request.clientRef,
            metadata,
            visibility: sessionVisibilityForCreateMetadata(
              metadata,
              (request.createdBySubjectId ?? "").trim(),
            ),
            preparedWorkspace,
            toolSource,
            toolRefs,
            createdBySubjectId: (request.createdBySubjectId ?? "").trim(),
          });
          return sessionToAgentSession(session);
        });
      }
      const { session, created } = this.store.createSession({
        sessionId: request.sessionId,
        idempotencyKey: request.idempotencyKey,
        providerName: this.name,
        model,
        clientRef: request.clientRef,
        metadata,
        visibility: sessionVisibilityForCreateMetadata(
          metadata,
          (request.createdBySubjectId ?? "").trim(),
        ),
        preparedWorkspace,
        toolSource,
        toolRefs,
        createdBySubjectId: (request.createdBySubjectId ?? "").trim(),
      });
      if (!created) {
        this.requireReadableSession(session, request.subject);
      }
      return sessionToAgentSession(session);
    } catch (error) {
      if (error instanceof ConnectError) {
        throw error;
      }
      if (errorMessage(error).startsWith("sessionStart hook")) {
        throw new ConnectError(errorMessage(error), Code.FailedPrecondition);
      }
      throw invalidArgument(errorMessage(error));
    }
  }

  async getSession(
    request: GetAgentProviderSessionRequest,
  ): Promise<AgentSession> {
    this.requireRuntime();
    const session = this.store.getSession(request.sessionId);
    if (!session || !canReadSession(session, request.subject)) {
      throw notFound(
        `agent session ${JSON.stringify(request.sessionId)} was not found`,
      );
    }
    return sessionToAgentSession(session);
  }

  async listSessions(
    request: ListAgentProviderSessionsRequest,
  ): Promise<AgentSession[]> {
    this.requireRuntime();
    if (request.limit < 0) {
      throw invalidArgument("limit must be non-negative");
    }
    return this.store
      .listSessions({
        sessionIds: request.sessionIds,
        subjectId: subjectIdFrom(request.subject),
        state: request.state,
        limit: request.limit,
      })
      .map((session) => sessionToAgentSession(session, request.summaryOnly));
  }

  async updateSession(
    request: UpdateAgentProviderSessionRequest,
  ): Promise<AgentSession> {
    this.requireRuntime();
    const metadata =
      request.metadata === undefined ? undefined : objectOrEmpty(request.metadata);
    try {
      validateSessionStartUserMetadata(metadata);
    } catch (error) {
      throw invalidArgument(errorMessage(error));
    }
    const existing = this.store.getSession(request.sessionId);
    if (!existing) {
      throw notFound(
        `agent session ${JSON.stringify(request.sessionId)} was not found`,
      );
    }
    requireOwnedSession(existing, request.subject);
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
  ): Promise<AgentTurn> {
    const { config, runner } = this.requireRuntime();
    const session = this.store.getSession(request.sessionId);
    if (!session) {
      throw notFound(
        `agent session ${JSON.stringify(request.sessionId)} was not found`,
      );
    }
    requireOwnedSession(session, request.subject);
    const schema = validateCreateTurnRequest(request, session);
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
        createdBySubjectId: (request.createdBySubjectId ?? "").trim(),
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
      const requestContext = request.context;
      if (requestContext === undefined) {
        throw invalidArgument("request context is required");
      }
      void this.completeTurn({
        runner,
        turnId: turn.turnId,
        sessionId: turn.sessionId,
        model,
        messages: turn.messages,
        requestContext,
        cwd: session.preparedWorkspace?.cwd ?? "",
        schema,
      });
    }
    return turnToAgentTurn(turn);
  }

  async getTurn(request: GetAgentProviderTurnRequest): Promise<AgentTurn> {
    this.requireRuntime();
    const turn = this.store.getTurn(request.turnId);
    if (!turn) {
      throw notFound(
        `agent turn ${JSON.stringify(request.turnId)} was not found`,
      );
    }
    const session = this.store.getSession(turn.sessionId);
    if (!session || !canReadSession(session, request.subject)) {
      throw notFound(
        `agent turn ${JSON.stringify(request.turnId)} was not found`,
      );
    }
    return turnToAgentTurn(turn);
  }

  async listTurns(
    request: ListAgentProviderTurnsRequest,
  ): Promise<AgentTurn[]> {
    this.requireRuntime();
    if (request.limit < 0) {
      throw invalidArgument("limit must be non-negative");
    }
    const sessionId = String(request.sessionId ?? "").trim();
    if (sessionId) {
      const session = this.store.getSession(sessionId);
      if (!session || !canReadSession(session, request.subject)) {
        return [];
      }
    } else if (request.turnIds.length === 0) {
      return [];
    }
    const turns = this.store.listTurns({
      sessionId,
      turnIds: request.turnIds,
      subjectId: undefined,
      status: request.status,
      limit: request.turnIds.length > 0 ? undefined : request.limit,
    });
    const readable = turns.filter((turn) => {
      const session = this.store.getSession(turn.sessionId);
      return Boolean(session && canReadSession(session, request.subject));
    });
    const limited =
      request.turnIds.length > 0 && request.limit > 0
        ? readable.slice(0, request.limit)
        : readable;
    return limited.map((turn) => turnToAgentTurn(turn, request.summaryOnly));
  }

  async cancelTurn(
    request: CancelAgentProviderTurnRequest,
  ): Promise<AgentTurn> {
    const { runner } = this.requireRuntime();
    const existing = this.store.getTurn(request.turnId);
    if (!existing) {
      throw notFound(
        `agent turn ${JSON.stringify(request.turnId)} was not found`,
      );
    }
    const session = this.store.getSession(existing.sessionId);
    if (!session) {
      throw notFound(
        `agent turn ${JSON.stringify(request.turnId)} was not found`,
      );
    }
    requireOwnedSession(session, request.subject);
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
  ): Promise<AgentTurnEvent[]> {
    this.requireRuntime();
    const turn = this.store.getTurn(request.turnId);
    const session = turn ? this.store.getSession(turn.sessionId) : undefined;
    if (!session || !canReadSession(session, request.subject)) {
      return [];
    }
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
  ): Promise<AgentInteraction> {
    this.requireRuntime();
    throw notFound(
      `agent interaction ${JSON.stringify(request.interactionId)} was not found`,
    );
  }

  async listInteractions(
    request: ListAgentProviderInteractionsRequest,
  ): Promise<AgentInteraction[]> {
    this.requireRuntime();
    const turnId = String(request.turnId ?? "").trim();
    if (turnId) {
      const turn = this.store.getTurn(turnId);
      const session = turn ? this.store.getSession(turn.sessionId) : undefined;
      if (!session || !canReadSession(session, request.subject)) {
        return [];
      }
    }
    return [];
  }

  async resolveInteraction(
    request: ResolveAgentProviderInteractionRequest,
  ): Promise<AgentInteraction> {
    this.requireRuntime();
    throw notFound(
      `agent interaction ${JSON.stringify(request.interactionId)} was not found`,
    );
  }

  async getCapabilities(
    _request?: GetAgentProviderCapabilitiesRequest,
  ): Promise<AgentProviderCapabilities> {
    this.requireRuntime();
    return {
      streamingText: false,
      toolCalls: true,
      parallelToolCalls: false,
      interactions: false,
      resumableTurns: false,
      reasoningSummaries: false,
      supportsSessionStart: true,
      supportsPreparedWorkspace: true,
      boundedListHydration: true,
      supportedToolSources: [AgentToolSourceMode.CATALOG],
    };
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
    messages: AgentMessage[];
    requestContext: NonNullable<CreateAgentProviderTurnRequest["context"]>;
    cwd: string;
    schema?: Record<string, unknown> | undefined;
  }): Promise<void> {
    try {
      const output = await input.runner.runTurn({
        sessionId: input.sessionId,
        turnId: input.turnId,
        model: input.model,
        messages: input.messages,
        requestContext: input.requestContext,
        cwd: input.cwd,
        schema: input.schema,
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

  private requireReadableSession(
    session: StoredSession,
    subject: ReadSubject | undefined,
  ): void {
    if (!canReadSession(session, subject)) {
      throw notFound(
        `agent session ${JSON.stringify(session.sessionId)} was not found`,
      );
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
  session: StoredSession,
): Record<string, unknown> | undefined {
  if (session.toolSource !== AgentToolSourceMode.CATALOG) {
    throw invalidArgument("agent/cursor requires session tools.catalog");
  }
  if (request.context === undefined) {
    throw invalidArgument("request context is required");
  }
  if (request.tools.length > 0) {
    throw invalidArgument(
      "resolved tools are not supported; use tool_refs with catalog",
    );
  }
  const schema = schemaFromOutput(request.output);
  if (schema) {
    try {
      validateSchema(schema);
    } catch (error) {
      throw invalidArgument(errorMessage(error));
    }
  }
  if (hasObjectData(request.modelOptions)) {
    throw invalidArgument("model_options are not supported by agent/cursor");
  }
  validateToolRefs(session.toolRefs);
  return schema;
}

function sessionToolScopeFromConfig(
  tools: CreateAgentProviderSessionRequest["tools"] | undefined,
): {
  toolSource: AgentToolSourceMode;
  toolRefs: AgentToolRef[];
} {
  if (!tools || tools.none !== undefined) {
    return { toolSource: AgentToolSourceMode.NONE, toolRefs: [] };
  }
  if (tools.catalog !== undefined) {
    if ((tools.catalog.tools ?? []).length > 0) {
      throw invalidArgument(
        "resolved tools are not supported; use tools.catalog.refs",
      );
    }
    const refs = [...(tools.catalog.refs ?? [])];
    validateToolRefs(refs);
    return { toolSource: AgentToolSourceMode.CATALOG, toolRefs: refs };
  }
  return { toolSource: AgentToolSourceMode.NONE, toolRefs: [] };
}

function schemaFromOutput(
  output: CreateAgentProviderTurnRequest["output"] | undefined,
): Record<string, unknown> | undefined {
  if (!output) {
    throw invalidArgument("output is required");
  }
  const textSet = output.text !== undefined;
  const structuredSet = output.structured !== undefined;
  if (textSet === structuredSet) {
    throw invalidArgument("exactly one of output.text or output.structured is required");
  }
  if (output.structured) {
    return { ...output.structured.schema };
  }
  return undefined;
}

function validateToolRefs(
  toolRefs: readonly AgentToolRef[],
): void {
  if (toolRefs.length === 0) {
    throw invalidArgument("tools.catalog.refs are required");
  }
  toolRefs.forEach((ref, index) => {
    const app = (ref.app ?? "").trim();
    const system = (ref.system ?? "").trim();
    const operation = (ref.operation ?? "").trim();
    const connection = (ref.connection ?? "").trim();
    const instance = (ref.instance ?? "").trim();
    const label = `tool_refs[${index + 1}]`;
    if (!operation) {
      throw invalidArgument(`${label}.operation is required`);
    }
    if ([app, system, operation, connection, instance].includes("*")) {
      throw invalidArgument("wildcard tool_refs are not supported");
    }
    if (Boolean(app) === Boolean(system)) {
      throw invalidArgument(
        `${label} must set exactly one of app or system`,
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

function sessionVisibilityForCreateMetadata(
  metadata: Record<string, unknown>,
  createdBySubjectId: string,
): StoredSessionVisibility {
  return hasSlackSessionMetadata(metadata) &&
    isManagedSubjectId(createdBySubjectId)
    ? SESSION_VISIBILITY_COMPANY
    : SESSION_VISIBILITY_PRIVATE;
}

function hasSlackSessionMetadata(metadata: Record<string, unknown>): boolean {
  const slack = metadata.slack;
  return (
    isRecord(slack) &&
    nonEmptyString(slack.team_id) &&
    nonEmptyString(slack.channel_id) &&
    nonEmptyString(slack.root_message_ts) &&
    nonEmptyString(slack.session_ref)
  );
}

function isManagedSubjectId(subjectId: string): boolean {
  return subjectId.trim().startsWith("service_account:");
}

function nonEmptyString(value: unknown): boolean {
  return typeof value === "string" && value.trim().length > 0;
}

function canReadSession(
  session: StoredSession,
  subject: ReadSubject | undefined,
): boolean {
  const subjectId = subjectIdFrom(subject);
  if (!subjectId) {
    return false;
  }
  return (
    session.createdBySubjectId === subjectId ||
    session.visibility === SESSION_VISIBILITY_COMPANY
  );
}

function requireOwnedSession(
  session: StoredSession,
  subject: ReadSubject | undefined,
): void {
  const subjectId = subjectIdFrom(subject);
  if (!subjectId || session.createdBySubjectId !== subjectId) {
    throw permissionDenied(
      `agent session ${JSON.stringify(session.sessionId)} is owned by another subject`,
    );
  }
}

type ReadSubject = {
  id?: string | undefined;
};
type ReadActor = {
  subjectId?: string | undefined;
  subjectKind?: string | undefined;
};

function subjectIdFrom(subject: ReadSubject | undefined): string {
  return String(subject?.id ?? "").trim();
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function preparedWorkspaceFromRequest(
  request: CreateAgentProviderSessionRequest,
): PreparedWorkspace | undefined {
  if (!request.preparedWorkspace) {
    return undefined;
  }
  const root = (request.preparedWorkspace.root ?? "").trim();
  const cwd = (request.preparedWorkspace.cwd ?? "").trim();
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

function hasSessionStartHooks(value: AgentSessionStartConfig | undefined): boolean {
  return (value?.hooks?.length ?? 0) > 0;
}

function invalidArgument(message: string): ConnectError {
  return new ConnectError(message, Code.InvalidArgument);
}

function notFound(message: string): ConnectError {
  return new ConnectError(message, Code.NotFound);
}

function permissionDenied(message: string): ConnectError {
  return new ConnectError(message, Code.PermissionDenied);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
