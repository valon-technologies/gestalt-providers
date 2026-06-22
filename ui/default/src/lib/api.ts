import { clearSession } from "./auth";
import { HTTP_UNAUTHORIZED, LOGIN_PATH } from "./constants";
import { loginPathForCurrentLocation } from "./authReturn";

export interface ConnectionParamDef {
  required?: boolean;
  description?: string;
  default?: string;
}

export interface CredentialFieldDef {
  name: string;
  label?: string;
  description?: string;
}

export interface InstanceInfo {
  name: string;
  connection?: string;
}

export type AuthType = "oauth" | "manual";
export type IntegrationStatus =
  | "ready"
  | "degraded"
  | "needs_user_connection"
  | "needs_instance_selection"
  | "needs_admin_configuration"
  | "unavailable"
  | "unknown";
export type CredentialState =
  | "not_required"
  | "connected"
  | "configured"
  | "missing"
  | "invalid"
  | "unknown";
export type HealthState =
  | "healthy"
  | "unhealthy"
  | "not_checked"
  | "not_applicable"
  | "unknown";
export type IntegrationAction =
  | "connect"
  | "disconnect"
  | "add_instance"
  | "select_instance"
  | "reconnect"
  | "admin_configure";
export type ConnectionMode = "none" | "subject";
export type CredentialMode = "none" | "subject";
export type OwnerKind =
  | "none"
  | "current_user"
  | "service_account"
  | "unknown";

export interface ConnectionDefInfo {
  name: string;
  displayName?: string;
  authTypes?: AuthType[];
  connectionParams?: Record<string, ConnectionParamDef>;
  credentialFields?: CredentialFieldDef[];
  status?: IntegrationStatus;
  credentialState?: CredentialState;
  healthState?: HealthState;
  actions?: IntegrationAction[];
  mode?: ConnectionMode;
  credentialMode?: CredentialMode;
  ownerKind?: OwnerKind;
  instances?: InstanceInfo[];
  mcpPassthrough?: boolean;
}

export interface Integration {
  name: string;
  displayName?: string;
  description?: string;
  iconSvg?: string;
  mountedPath?: string;
  connections?: ConnectionDefInfo[];
  status?: IntegrationStatus;
  credentialState?: CredentialState;
  healthState?: HealthState;
  actions?: IntegrationAction[];
}

export interface IntegrationOperation {
  id: string;
  title?: string;
  description?: string;
  readOnly?: boolean;
  visible?: boolean;
  tags?: string[];
}

export interface AccessPermission {
  plugin: string;
  operations?: string[];
  actions?: string[];
}

export interface APIToken {
  id: string;
  name?: string;
  scopes?: string[];
  permissions?: AccessPermission[];
  createdAt: string;
  expiresAt?: string;
}

export interface CreateTokenResponse {
  id: string;
  name?: string;
  token: string;
  permissions?: AccessPermission[];
  expiresAt?: string;
}

export interface WorkflowAppTarget {
  name: string;
  operation: string;
  connection?: string;
  instance?: string;
  credentialMode?: string;
  input?: unknown;
}

export interface WorkflowTextTarget {
  template?: string;
}

export interface WorkflowMessageTarget {
  role?: string;
  text?: WorkflowTextTarget;
  metadata?: Record<string, unknown>;
}

export interface WorkflowAgentTarget {
  provider?: string;
  model?: string;
  sessionKey?: string;
  prompt?: WorkflowTextTarget;
  messages?: WorkflowMessageTarget[];
  tools?: AgentToolRef[];
  output?: AgentOutput;
  modelOptions?: Record<string, unknown>;
}

export interface WorkflowStepTarget {
  id?: string;
  inputs?: Record<string, unknown>;
  app?: WorkflowAppTarget;
  agent?: WorkflowAgentTarget;
  metadata?: Record<string, unknown>;
  timeoutSeconds?: number;
  when?: Record<string, unknown>;
}

export interface WorkflowTarget {
  steps: WorkflowStepTarget[];
}

export interface WorkflowEvent {
  id?: string;
  source?: string;
  specVersion?: string;
  type?: string;
  subject?: string;
  time?: string;
  dataContentType?: string;
  data?: Record<string, unknown>;
  extensions?: Record<string, unknown>;
}

export interface WorkflowRunTrigger {
  kind?: string;
  activationId?: string;
  scheduledFor?: string;
  event?: WorkflowEvent;
}

export interface WorkflowActor {
  subjectId?: string;
}

export interface WorkflowStepAttempt {
  id?: string;
  status?: string;
  idempotencyKey?: string;
  input?: unknown;
  output?: unknown;
  statusMessage?: string;
  startedAt?: string;
  completedAt?: string;
}

export interface WorkflowStepExecution {
  stepId?: string;
  status?: string;
  attempts?: WorkflowStepAttempt[];
  input?: unknown;
  output?: unknown;
  statusMessage?: string;
  skipReason?: string;
  startedAt?: string;
  completedAt?: string;
}

export interface WorkflowRun {
  id: string;
  provider: string;
  status?: string;
  target: WorkflowTarget;
  trigger?: WorkflowRunTrigger;
  createdBy?: WorkflowActor;
  createdAt?: string;
  startedAt?: string;
  completedAt?: string;
  statusMessage?: string;
  output?: unknown;
  definitionId?: string;
  definitionGeneration?: number;
  input?: Record<string, unknown>;
  currentStepId?: string;
  steps?: WorkflowStepExecution[];
}

interface WorkflowRunListResponse {
  runs: WorkflowRunWire[];
  nextPageToken?: string;
}

type WorkflowRunWire = Omit<WorkflowRun, "target" | "steps"> & {
  target?: unknown;
  steps?: unknown;
};

function normalizeWorkflowRun(run: WorkflowRunWire): WorkflowRun {
  return {
    ...run,
    target: normalizeWorkflowTarget(run.target),
    steps: normalizeWorkflowStepExecutions(run.steps),
  };
}

function normalizeWorkflowTarget(target: unknown): WorkflowTarget {
  if (!isRecord(target)) {
    return { steps: [] };
  }

  const rawSteps = target.steps;
  if (!Array.isArray(rawSteps)) {
    return { steps: [] };
  }

  return {
    steps: rawSteps.flatMap((rawStep) => {
      if (!isRecord(rawStep)) {
        return [];
      }
      const rawApp = rawStep.app;
      const rawAgent = rawStep.agent;
      return [
        {
          id: optionalString(rawStep.id),
          inputs: optionalRecord(rawStep.inputs),
          app: isRecord(rawApp)
            ? {
                name: stringValue(rawApp.name),
                operation: stringValue(rawApp.operation),
                connection: optionalString(rawApp.connection),
                instance: optionalString(rawApp.instance),
                credentialMode: optionalString(rawApp.credentialMode),
                input: rawApp.input,
              }
            : undefined,
          agent: normalizeWorkflowAgentTarget(rawAgent),
          metadata: optionalRecord(rawStep.metadata),
          timeoutSeconds:
            typeof rawStep.timeoutSeconds === "number"
              ? rawStep.timeoutSeconds
              : undefined,
          when: optionalRecord(rawStep.when),
        },
      ];
    }),
  };
}

function normalizeWorkflowAgentTarget(
  value: unknown,
): WorkflowAgentTarget | undefined {
  if (!isRecord(value)) return undefined;
  return {
    provider: optionalString(value.provider),
    model: optionalString(value.model),
    sessionKey: optionalString(value.sessionKey),
    prompt: normalizeWorkflowTextTarget(value.prompt),
    messages: Array.isArray(value.messages)
      ? value.messages.flatMap((message) => {
          if (!isRecord(message)) return [];
          return [
            {
              role: optionalString(message.role),
              text: normalizeWorkflowTextTarget(message.text),
              metadata: optionalRecord(message.metadata),
            },
          ];
        })
      : undefined,
    tools: Array.isArray(value.tools)
      ? value.tools.flatMap((tool) => (isRecord(tool) ? [tool as AgentToolRef] : []))
      : undefined,
    output: isRecord(value.output) ? (value.output as AgentOutput) : undefined,
    modelOptions: optionalRecord(value.modelOptions),
  };
}

function normalizeWorkflowTextTarget(
  value: unknown,
): WorkflowTextTarget | undefined {
  if (!isRecord(value)) return undefined;
  return {
    template: optionalString(value.template),
  };
}

function normalizeWorkflowStepExecutions(
  value: unknown,
): WorkflowStepExecution[] {
  if (!Array.isArray(value)) return [];
  return value.flatMap((rawStep) => {
    if (!isRecord(rawStep)) return [];
    return [
      {
        stepId: optionalString(rawStep.stepId),
        status: optionalString(rawStep.status),
        attempts: normalizeWorkflowStepAttempts(rawStep.attempts),
        input: rawStep.input,
        output: rawStep.output,
        statusMessage: optionalString(rawStep.statusMessage),
        skipReason: optionalString(rawStep.skipReason),
        startedAt: optionalString(rawStep.startedAt),
        completedAt: optionalString(rawStep.completedAt),
      },
    ];
  });
}

function normalizeWorkflowStepAttempts(value: unknown): WorkflowStepAttempt[] {
  if (!Array.isArray(value)) return [];
  return value.flatMap((rawAttempt) => {
    if (!isRecord(rawAttempt)) return [];
    return [
      {
        id: optionalString(rawAttempt.id),
        status: optionalString(rawAttempt.status),
        idempotencyKey: optionalString(rawAttempt.idempotencyKey),
        input: rawAttempt.input,
        output: rawAttempt.output,
        statusMessage: optionalString(rawAttempt.statusMessage),
        startedAt: optionalString(rawAttempt.startedAt),
        completedAt: optionalString(rawAttempt.completedAt),
      },
    ];
  });
}

export function workflowTargetApp(target: WorkflowTarget): WorkflowAppTarget {
  return (
    target.steps.find((step) => step.app)?.app ?? {
      name: "",
      operation: "",
    }
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringValue(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value ? value : undefined;
}

function optionalRecord(value: unknown): Record<string, unknown> | undefined {
  return isRecord(value) ? value : undefined;
}

export type AgentExecutionStatus =
  | "pending"
  | "running"
  | "waiting_for_input"
  | "succeeded"
  | "failed"
  | "canceled"
  | string;

export type AgentSessionState = "active" | "archived" | string;

export interface AgentMessagePart {
  type?: string;
  text?: string;
  json?: Record<string, unknown>;
  toolCall?: Record<string, unknown>;
  toolResult?: Record<string, unknown>;
  imageRef?: Record<string, unknown>;
}

export interface AgentMessage {
  role: string;
  text?: string;
  parts?: AgentMessagePart[];
  metadata?: Record<string, unknown>;
}

export interface AgentActor {
  subjectId?: string;
  subjectKind?: string;
  displayName?: string;
  authSource?: string;
}

export interface AgentToolRef {
  system?: string;
  plugin?: string;
  operation?: string;
  connection?: string;
  instance?: string;
  title?: string;
  description?: string;
}

export interface AgentRun {
  id: string;
  sessionId?: string;
  provider: string;
  model?: string;
  status?: AgentExecutionStatus;
  messages?: AgentMessage[];
  output?: AgentTurnOutput;
  statusMessage?: string;
  sessionRef?: string;
  createdBy?: AgentActor;
  createdAt?: string;
  startedAt?: string;
  completedAt?: string;
  executionRef?: string;
}

export interface AgentRunCreate {
  provider?: string;
  model?: string;
  messages: AgentMessage[];
  toolRefs?: AgentToolRef[];
  toolSource?: "catalog" | "explicit" | "inherit_invokes";
  output?: AgentOutput;
  sessionRef?: string;
  metadata?: Record<string, unknown>;
  modelOptions?: Record<string, unknown>;
  idempotencyKey?: string;
}

export interface AgentSession {
  id: string;
  provider: string;
  model?: string;
  clientRef?: string;
  state?: AgentSessionState;
  metadata?: Record<string, unknown>;
  createdBy?: AgentActor;
  createdAt?: string;
  updatedAt?: string;
  lastTurnAt?: string;
}

export type AgentTurn = Omit<AgentRun, "sessionRef"> & {
  sessionId: string;
};

type AgentTurnWire = AgentTurn;

export type AgentOutput =
  | { text: Record<string, never>; structured?: never }
  | { text?: never; structured: { schema: Record<string, unknown> } };

export type AgentTurnOutput =
  | { text: { text?: string }; structured?: never }
  | { text?: never; structured: { text?: string; value?: Record<string, unknown> } };

export interface AgentProviderCapabilities {
  streamingText?: boolean;
  toolCalls?: boolean;
  parallelToolCalls?: boolean;
  interactions?: boolean;
  resumableTurns?: boolean;
  reasoningSummaries?: boolean;
  boundedListHydration?: boolean;
  supportedToolSources?: string[];
}

export interface AgentProvider {
  name: string;
  default?: boolean;
  capabilities?: AgentProviderCapabilities;
}

export interface AgentProviderList {
  providers: AgentProvider[];
}

export interface AgentSessionCreate {
  provider?: string;
  model?: string;
  clientRef?: string;
  tools?: AgentSessionTools;
  metadata?: Record<string, unknown>;
  modelOptions?: Record<string, unknown>;
  idempotencyKey?: string;
}

export type AgentSessionTools =
  | { none: Record<string, never>; catalog?: never }
  | { none?: never; catalog: { refs?: AgentToolRef[] } };

export interface AgentSessionUpdate {
  clientRef?: string;
  state?: AgentSessionState;
  metadata?: Record<string, unknown>;
}

export interface AgentTurnCreate {
  model?: string;
  messages: AgentMessage[];
  output?: AgentOutput;
  metadata?: Record<string, unknown>;
  modelOptions?: Record<string, unknown>;
  idempotencyKey?: string;
}

export interface AgentTurnDisplay {
  kind?: string;
  phase?: string;
  text?: string;
  label?: string;
  ref?: string;
  parentRef?: string;
  input?: unknown;
  output?: unknown;
  error?: unknown;
  action?: string;
  format?: string;
  language?: string;
}

export interface AgentTurnEvent {
  id: string;
  turnId: string;
  seq: number;
  type: string;
  source?: string;
  visibility?: "public" | "private" | string;
  data?: Record<string, unknown>;
  createdAt?: string;
  display?: AgentTurnDisplay;
}

export type AgentInteractionType =
  | "approval"
  | "clarification"
  | "input"
  | string;

export type AgentInteractionState = "pending" | "resolved" | "canceled" | string;

export interface AgentInteraction {
  id: string;
  turnId: string;
  type: AgentInteractionType;
  state: AgentInteractionState;
  title?: string;
  prompt?: string;
  request?: Record<string, unknown>;
  resolution?: Record<string, unknown>;
  createdAt?: string;
  resolvedAt?: string;
}

export interface AgentInteractionResolve {
  resolution: Record<string, unknown>;
}

export interface AgentTurnEventStream {
  close: () => void;
}

export interface AgentTurnEventStreamOptions {
  after?: number;
  limit?: number;
  until?: "terminal" | "blocked_or_terminal";
  onEvent?: (event: AgentTurnEvent) => void;
  onError?: (error: Error, event?: unknown) => void;
  onClose?: () => void;
}

function normalizeAgentRun(
  turn: AgentTurnWire,
  session?: AgentSession,
): AgentRun {
  return {
    ...turn,
    sessionRef: session?.clientRef || turn.sessionId,
  };
}

function compareAgentRunsDesc(left: AgentRun, right: AgentRun): number {
  const leftTime = Date.parse(left.createdAt || "");
  const rightTime = Date.parse(right.createdAt || "");
  const leftValue = Number.isNaN(leftTime) ? 0 : leftTime;
  const rightValue = Number.isNaN(rightTime) ? 0 : rightTime;
  return rightValue - leftValue || right.id.localeCompare(left.id);
}

function idempotencyKeyPart(prefix: string, key?: string): string | undefined {
  return key ? `${prefix}:${key}` : undefined;
}

function agentToolRefsToRequest(
  toolRefs?: AgentToolRef[],
): AgentToolRef[] | undefined {
  return toolRefs?.map((tool) => ({
    system: tool.system,
    plugin: tool.plugin,
    operation: tool.operation,
    connection: tool.connection,
    instance: tool.instance,
    title: tool.title,
    description: tool.description,
  }));
}

function agentToolsToRequest(
  value?: AgentRunCreate["toolSource"],
  refs?: AgentToolRef[],
): AgentSessionTools | undefined {
  switch (value) {
    case undefined:
      return undefined;
    case "catalog":
    case "explicit":
      return { catalog: { refs: agentToolRefsToRequest(refs) } };
    case "inherit_invokes":
      throw new Error("inherit_invokes is not supported by the agent API");
    default:
      return undefined;
  }
}

export interface ManagedIdentity {
  id: string;
  subjectId: string;
  kind: "service_account";
  displayName: string;
  description?: string;
  credentialSubjectId: string;
  createdBySubjectId?: string;
  createdAt: string;
  updatedAt: string;
  deletedAt?: string;
}

export interface ManagedIdentityMember {
  subjectId: string;
  email?: string;
  role: "viewer" | "editor" | "admin";
}

export interface ManagedIdentityGrant {
  plugin: string;
  role: "viewer" | "editor" | "admin";
  source: "static" | "dynamic" | string;
  mutable: boolean;
}

export interface ConnectIntegrationResult {
  status: string;
  integration?: string;
  selectionUrl?: string;
  pendingToken?: string;
  candidates?: { id: string; name?: string }[];
}

export class APIError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
    this.name = "APIError";
  }
}

export function isAPIErrorStatus(error: unknown, status: number): boolean {
  return error instanceof APIError && error.status === status;
}

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";
export const PENDING_CONNECTION_PATH = "/api/v1/auth/pending-connection";

export function resolveAPIPath(path: string): string {
  if (/^[a-zA-Z][a-zA-Z\d+.-]*:/.test(path)) {
    return path;
  }
  return `${API_BASE}${path}`;
}

export async function fetchAPI<T>(
  path: string,
  options?: RequestInit,
): Promise<T> {
  const res = await fetch(resolveAPIPath(path), {
    ...options,
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
  });

  if (res.status === HTTP_UNAUTHORIZED) {
    clearSession();
    if (window.location.pathname !== LOGIN_PATH) {
      window.location.href = loginPathForCurrentLocation();
    }
    throw new APIError(HTTP_UNAUTHORIZED, "Session expired");
  }

  if (!res.ok) {
    const body = await res.text();
    let message: string;
    try {
      const parsed = JSON.parse(body);
      message = parsed.error || body;
    } catch {
      message = body;
    }
    throw new APIError(res.status, message);
  }

  const contentType = res.headers.get("content-type") || "";
  if (!/\bapplication\/([a-z\d.+-]*\+)?json\b/i.test(contentType)) {
    throw new APIError(
      res.status,
      `Expected JSON response from ${path}, received ${contentType || "unknown content type"}`,
    );
  }

  return res.json() as Promise<T>;
}

export interface AuthInfo {
  provider: string;
  displayName: string;
  loginSupported: boolean;
  features?: {
    agent?: boolean;
  };
}

export interface AuthSession {
  subjectId: string;
  email?: string;
  displayName?: string;
}

export async function getAuthInfo(): Promise<AuthInfo> {
  return fetchAPI("/api/v1/auth/info");
}

export async function getAuthSession(): Promise<AuthSession> {
  return fetchAPI("/api/v1/auth/session");
}

export async function startLogin(state: string): Promise<{ url: string }> {
  return fetchAPI("/api/v1/auth/login", {
    method: "POST",
    body: JSON.stringify({ state }),
  });
}

export async function loginCallback(
  code: string,
  state?: string,
): Promise<{ status?: string }> {
  const params = new URLSearchParams({ code });
  if (state) params.set("state", state);
  return fetchAPI(`/api/v1/auth/login/callback?${params}`);
}

export async function logout(): Promise<void> {
  await fetchAPI("/api/v1/auth/logout", { method: "POST" });
}

export async function getIntegrations(): Promise<Integration[]> {
  return fetchAPI<Integration[]>("/api/v1/apps");
}

export async function getIntegrationOperations(
  integration: string,
): Promise<IntegrationOperation[]> {
  return fetchAPI<IntegrationOperation[]>(
    `/api/v1/apps/${encodeURIComponent(integration)}/operations`,
  );
}

export async function startIntegrationOAuth(
  integration: string,
  scopes?: string[],
  connectionParams?: Record<string, string>,
  instance?: string,
  connection?: string,
  returnPath?: string,
): Promise<{ url: string; state: string }> {
  return fetchAPI("/api/v1/auth/start-oauth", {
    method: "POST",
    body: JSON.stringify({
      integration,
      instance,
      connection,
      scopes: scopes || [],
      connectionParams,
      returnPath,
    }),
  });
}

export async function connectManualIntegration(
  integration: string,
  credential: string | Record<string, string>,
  connectionParams?: Record<string, string>,
  instance?: string,
  connection?: string,
  returnPath?: string,
): Promise<ConnectIntegrationResult> {
  const body: Record<string, unknown> = {
    integration,
    instance,
    connection,
    connectionParams,
    returnPath,
  };
  if (typeof credential === "string") {
    body.credential = credential;
  } else {
    body.credentials = credential;
  }
  return fetchAPI("/api/v1/auth/connect-manual", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function disconnectIntegration(
  name: string,
  instance?: string,
  connection?: string,
): Promise<void> {
  const query = new URLSearchParams();
  if (instance) query.set("_instance", instance);
  if (connection) query.set("_connection", connection);
  const params = query.toString();
  await fetchAPI(
    `/api/v1/apps/${encodeURIComponent(name)}${params ? `?${params}` : ""}`,
    {
      method: "DELETE",
    },
  );
}

export async function getTokens(): Promise<APIToken[]> {
  return fetchAPI("/api/v1/tokens");
}

export async function getWorkflowRuns(): Promise<WorkflowRun[]> {
  const response = await fetchAPI<WorkflowRunListResponse>(
    "/api/v1/workflow/runs",
  );
  return response.runs.map(normalizeWorkflowRun);
}

export async function getWorkflowRun(id: string): Promise<WorkflowRun> {
  const run = await fetchAPI<WorkflowRunWire>(
    `/api/v1/workflow/runs/${encodeURIComponent(id)}`,
  );
  return normalizeWorkflowRun(run);
}

export async function cancelWorkflowRun(
  id: string,
  reason?: string,
): Promise<WorkflowRun> {
  const run = await fetchAPI<WorkflowRunWire>(
    `/api/v1/workflow/runs/${encodeURIComponent(id)}/cancel`,
    {
      method: "POST",
      body: JSON.stringify(reason ? { reason } : {}),
    },
  );
  return normalizeWorkflowRun(run);
}

export async function getAgentProviders(): Promise<AgentProvider[]> {
  const response = await fetchAPI<AgentProviderList | AgentProvider[]>(
    "/api/v1/agent/providers",
  );
  return Array.isArray(response) ? response : (response.providers ?? []);
}

export async function getAgentSessions(opts?: {
  provider?: string;
  state?: string;
  view?: "full" | "summary";
  limit?: number;
}): Promise<AgentSession[]> {
  const query = new URLSearchParams();
  if (opts?.provider) query.set("provider", opts.provider);
  if (opts?.state && opts.state !== "all") query.set("state", opts.state);
  if (opts?.view) query.set("view", opts.view);
  if (opts?.limit) query.set("limit", String(opts.limit));
  const params = query.toString();
  return fetchAPI<AgentSession[]>(
    `/api/v1/agent/sessions${params ? `?${params}` : ""}`,
  );
}

export async function getAgentSession(
  id: string,
  provider: string,
): Promise<AgentSession> {
  return fetchAPI<AgentSession>(
    `/api/v1/agent/sessions/${encodeURIComponent(id)}?${new URLSearchParams({ provider })}`,
  );
}

export async function createAgentSession(
  body: AgentSessionCreate,
): Promise<AgentSession> {
  return fetchAPI<AgentSession>("/api/v1/agent/sessions", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function updateAgentSession(
  id: string,
  provider: string,
  body: AgentSessionUpdate,
): Promise<AgentSession> {
  return fetchAPI<AgentSession>(
    `/api/v1/agent/sessions/${encodeURIComponent(id)}?${new URLSearchParams({ provider })}`,
    {
      method: "PATCH",
      body: JSON.stringify(body),
    },
  );
}

export async function getAgentTurns(
  sessionID: string,
  provider: string,
  opts?: {
    status?: string;
    limit?: number;
    view?: "full" | "summary";
  },
): Promise<AgentTurn[]> {
  const query = new URLSearchParams({ provider });
  if (opts?.status && opts.status !== "all") query.set("status", opts.status);
  if (opts?.limit) query.set("limit", String(opts.limit));
  if (opts?.view) query.set("view", opts.view);
  return fetchAPI<AgentTurn[]>(
    `/api/v1/agent/sessions/${encodeURIComponent(sessionID)}/turns?${query}`,
  );
}

export async function getAgentTurn(
  id: string,
  provider: string,
): Promise<AgentTurn> {
  return fetchAPI<AgentTurn>(
    `/api/v1/agent/turns/${encodeURIComponent(id)}?${new URLSearchParams({ provider })}`,
  );
}

export async function createAgentTurn(
  sessionID: string,
  provider: string,
  body: AgentTurnCreate,
): Promise<AgentTurn> {
  const output = body.output ?? { text: {} };
  return fetchAPI<AgentTurn>(
    `/api/v1/agent/sessions/${encodeURIComponent(sessionID)}/turns?${new URLSearchParams({ provider })}`,
    {
      method: "POST",
      body: JSON.stringify({
        model: body.model,
        messages: body.messages,
        output,
        metadata: body.metadata,
        modelOptions: body.modelOptions,
        idempotencyKey: body.idempotencyKey,
      }),
    },
  );
}

export async function cancelAgentTurn(
  id: string,
  provider: string,
  reason?: string,
): Promise<AgentTurn> {
  return fetchAPI<AgentTurn>(
    `/api/v1/agent/turns/${encodeURIComponent(id)}/cancel?${new URLSearchParams({ provider })}`,
    {
      method: "POST",
      body: JSON.stringify(reason ? { reason } : {}),
    },
  );
}

export async function getAgentTurnEvents(
  turnID: string,
  provider: string,
  opts?: { after?: number; limit?: number },
): Promise<AgentTurnEvent[]> {
  const query = new URLSearchParams({ provider });
  if (typeof opts?.after === "number") query.set("after", String(opts.after));
  if (typeof opts?.limit === "number") query.set("limit", String(opts.limit));
  return fetchAPI<AgentTurnEvent[]>(
    `/api/v1/agent/turns/${encodeURIComponent(turnID)}/events?${query}`,
  );
}

export async function getAllAgentTurnEvents(
  turnID: string,
  provider: string,
  opts?: { after?: number; limit?: number },
): Promise<{ events: AgentTurnEvent[]; lastSeq: number }> {
  const limit = opts?.limit ?? 100;
  let after = opts?.after ?? 0;
  const events: AgentTurnEvent[] = [];

  for (;;) {
    const page = await getAgentTurnEvents(turnID, provider, { after, limit });
    if (page.length === 0) {
      break;
    }

    let maxSeq = after;
    for (const event of page) {
      if (typeof event.seq === "number") {
        maxSeq = Math.max(maxSeq, event.seq);
      }
      events.push(event);
    }

    if (page.length < limit || maxSeq <= after) {
      after = maxSeq;
      break;
    }
    after = maxSeq;
  }

  return { events, lastSeq: after };
}

export async function getAgentInteractions(
  turnID: string,
  provider: string,
): Promise<AgentInteraction[]> {
  return fetchAPI<AgentInteraction[]>(
    `/api/v1/agent/turns/${encodeURIComponent(turnID)}/interactions?${new URLSearchParams({ provider })}`,
  );
}

export async function resolveAgentInteraction(
  turnID: string,
  provider: string,
  interactionID: string,
  resolution: Record<string, unknown>,
): Promise<AgentInteraction> {
  return fetchAPI<AgentInteraction>(
    `/api/v1/agent/turns/${encodeURIComponent(
      turnID,
    )}/interactions/${encodeURIComponent(interactionID)}/resolve?${new URLSearchParams({ provider })}`,
    {
      method: "POST",
      body: JSON.stringify({ resolution } satisfies AgentInteractionResolve),
    },
  );
}

export function openAgentTurnEventStream(
  turnID: string,
  provider: string,
  opts: AgentTurnEventStreamOptions,
): AgentTurnEventStream {
  const query = new URLSearchParams({
    provider,
    after: String(opts.after ?? 0),
    limit: String(opts.limit ?? 100),
    until: opts.until ?? "blocked_or_terminal",
  });
  const source = new EventSource(
    resolveAPIPath(
      `/api/v1/agent/turns/${encodeURIComponent(turnID)}/events/stream?${query}`,
    ),
    { withCredentials: true },
  );
  let closed = false;

  function close() {
    if (closed) return;
    closed = true;
    source.close();
    opts.onClose?.();
  }

  function parseEvent(data: string, eventName: string): AgentTurnEvent | null {
    const trimmed = data.trim();
    if (!trimmed) return null;
    try {
      const parsed = JSON.parse(trimmed) as AgentTurnEvent;
      if (eventName === "error") {
        const message =
          typeof parsed?.data?.error === "string"
            ? parsed.data.error
            : "Agent event stream error";
        opts.onError?.(new Error(message), parsed);
        return null;
      }
      return parsed;
    } catch (err) {
      opts.onError?.(
        err instanceof Error ? err : new Error("Invalid agent event frame"),
      );
      return null;
    }
  }

  source.onmessage = (event) => {
    const parsed = parseEvent(event.data, "message");
    if (!parsed) return;
    opts.onEvent?.(parsed);
    if (
      parsed.type === "turn.completed" ||
      parsed.type === "turn.failed" ||
      parsed.type === "turn.canceled"
    ) {
      close();
    }
  };

  source.addEventListener("error", (event) => {
    if (event instanceof MessageEvent && typeof event.data === "string") {
      parseEvent(event.data, "error");
    } else {
      opts.onError?.(new Error("Agent event stream closed"));
    }
    close();
  });

  return { close };
}

export async function getAgentRuns(opts?: {
  provider?: string;
  status?: string;
}): Promise<AgentRun[]> {
  const sessions = await getAgentSessions({
    provider: opts?.provider,
    view: "summary",
    limit: 50,
  });

  const turnLists = await Promise.all(
    sessions.map(async (session) => {
      const turns = await getAgentTurns(session.id, session.provider, {
        status: opts?.status,
        limit: 20,
      });
      return turns.map((turn) => normalizeAgentRun(turn, session));
    }),
  );

  return turnLists.flat().sort(compareAgentRunsDesc);
}

export async function getAgentRun(
  id: string,
  provider: string,
): Promise<AgentRun> {
  const turn = await getAgentTurn(id, provider);
  return normalizeAgentRun(turn);
}

export async function createAgentRun(body: AgentRunCreate): Promise<AgentRun> {
  const tools = agentToolsToRequest(body.toolSource, body.toolRefs);

  const session = await fetchAPI<AgentSession>("/api/v1/agent/sessions", {
    method: "POST",
    body: JSON.stringify({
      provider: body.provider,
      model: body.model,
      clientRef: body.sessionRef,
      tools,
      metadata: body.metadata,
      idempotencyKey: idempotencyKeyPart("session", body.idempotencyKey),
    }),
  });

  const turn = await fetchAPI<AgentTurnWire>(
    `/api/v1/agent/sessions/${encodeURIComponent(session.id)}/turns?${new URLSearchParams(
      { provider: session.provider },
    )}`,
    {
      method: "POST",
      body: JSON.stringify({
        model: body.model,
        messages: body.messages,
        output: body.output ?? { text: {} },
        metadata: body.metadata,
        modelOptions: body.modelOptions,
        idempotencyKey: idempotencyKeyPart("turn", body.idempotencyKey),
      }),
    },
  );
  return normalizeAgentRun(turn, session);
}

export async function cancelAgentRun(
  id: string,
  provider: string,
  reason?: string,
): Promise<AgentRun> {
  const turn = await cancelAgentTurn(id, provider, reason);
  return normalizeAgentRun(turn);
}

export async function createToken(
  name: string,
  scopes: string,
  expiresInSeconds?: number,
): Promise<CreateTokenResponse> {
  const body: { name: string; scopes: string; expiresInSeconds?: number } = {
    name,
    scopes,
  };
  if (expiresInSeconds !== undefined) {
    body.expiresInSeconds = expiresInSeconds;
  }
  return fetchAPI("/api/v1/tokens", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function revokeToken(id: string): Promise<void> {
  await fetchAPI(`/api/v1/tokens/${id}`, { method: "DELETE" });
}

const MANAGED_SUBJECTS_PATH = "/api/v1/authorization/subjects";

function managedSubjectPath(id: string): string {
  return `${MANAGED_SUBJECTS_PATH}/${encodeURIComponent(id)}`;
}

function unwrapManagedIdentityGrant(
  response: ManagedIdentityGrant | { grant?: ManagedIdentityGrant },
): ManagedIdentityGrant {
  if ("grant" in response && response.grant) {
    return response.grant;
  }
  return response as ManagedIdentityGrant;
}

export async function getManagedIdentities(): Promise<ManagedIdentity[]> {
  return fetchAPI(MANAGED_SUBJECTS_PATH);
}

export async function createManagedIdentity(
  id: string,
  displayName: string,
  description?: string,
): Promise<ManagedIdentity> {
  return fetchAPI(MANAGED_SUBJECTS_PATH, {
    method: "POST",
    body: JSON.stringify({ id, displayName, description }),
  });
}

export async function getManagedIdentity(id: string): Promise<ManagedIdentity> {
  return fetchAPI(managedSubjectPath(id));
}

export async function getManagedIdentityIntegrations(
  id: string,
): Promise<Integration[]> {
  return fetchAPI<Integration[]>(`${managedSubjectPath(id)}/apps`);
}

export async function startManagedIdentityIntegrationOAuth(
  id: string,
  integration: string,
  scopes?: string[],
  connectionParams?: Record<string, string>,
  instance?: string,
  connection?: string,
  returnPath?: string,
): Promise<{ url: string; state: string }> {
  return fetchAPI(`${managedSubjectPath(id)}/auth/start-oauth`, {
    method: "POST",
    body: JSON.stringify({
      integration,
      instance,
      connection,
      scopes: scopes || [],
      connectionParams,
      returnPath,
    }),
  });
}

export async function connectManagedIdentityManualIntegration(
  id: string,
  integration: string,
  credential: string | Record<string, string>,
  connectionParams?: Record<string, string>,
  instance?: string,
  connection?: string,
  returnPath?: string,
): Promise<ConnectIntegrationResult> {
  const body: Record<string, unknown> = {
    integration,
    instance,
    connection,
    connectionParams,
    returnPath,
  };
  if (typeof credential === "string") {
    body.credential = credential;
  } else {
    body.credentials = credential;
  }
  return fetchAPI(`${managedSubjectPath(id)}/auth/connect-manual`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function disconnectManagedIdentityIntegration(
  id: string,
  name: string,
  instance?: string,
  connection?: string,
): Promise<void> {
  const query = new URLSearchParams();
  if (instance) query.set("_instance", instance);
  if (connection) query.set("_connection", connection);
  const params = query.toString();
  await fetchAPI(
    `${managedSubjectPath(id)}/apps/${encodeURIComponent(name)}${params ? `?${params}` : ""}`,
    {
      method: "DELETE",
    },
  );
}

export async function updateManagedIdentity(
  id: string,
  displayName: string,
): Promise<ManagedIdentity> {
  return fetchAPI(managedSubjectPath(id), {
    method: "PATCH",
    body: JSON.stringify({ displayName }),
  });
}

export async function deleteManagedIdentity(id: string): Promise<void> {
  await fetchAPI(managedSubjectPath(id), {
    method: "DELETE",
  });
}

export async function getManagedIdentityMembers(
  id: string,
): Promise<ManagedIdentityMember[]> {
  return fetchAPI(`${managedSubjectPath(id)}/members`);
}

export async function putManagedIdentityMember(
  id: string,
  email: string,
  role: ManagedIdentityMember["role"],
): Promise<ManagedIdentityMember> {
  return fetchAPI(`${managedSubjectPath(id)}/members`, {
    method: "PUT",
    body: JSON.stringify({ email, role }),
  });
}

export async function deleteManagedIdentityMember(
  id: string,
  memberSubjectID: string,
): Promise<void> {
  await fetchAPI(
    `${managedSubjectPath(id)}/members/${encodeURIComponent(memberSubjectID)}`,
    { method: "DELETE" },
  );
}

export async function getManagedIdentityGrants(
  id: string,
): Promise<ManagedIdentityGrant[]> {
  return fetchAPI(`${managedSubjectPath(id)}/grants`);
}

export async function putManagedIdentityGrant(
  id: string,
  plugin: string,
  role: ManagedIdentityGrant["role"],
): Promise<ManagedIdentityGrant> {
  const response = await fetchAPI<
    ManagedIdentityGrant | { grant?: ManagedIdentityGrant }
  >(`${managedSubjectPath(id)}/grants/${encodeURIComponent(plugin)}`, {
    method: "PUT",
    body: JSON.stringify({ role }),
  });
  return unwrapManagedIdentityGrant(response);
}

export async function deleteManagedIdentityGrant(
  id: string,
  plugin: string,
): Promise<void> {
  await fetchAPI(
    `${managedSubjectPath(id)}/grants/${encodeURIComponent(plugin)}`,
    { method: "DELETE" },
  );
}

export async function getManagedIdentityTokens(
  id: string,
): Promise<APIToken[]> {
  return fetchAPI(`${managedSubjectPath(id)}/tokens`);
}

export async function createManagedIdentityToken(
  id: string,
  name: string,
  permissions?: AccessPermission[],
): Promise<CreateTokenResponse> {
  const body: { name: string; permissions?: AccessPermission[] } = { name };
  if (permissions !== undefined) {
    body.permissions = permissions;
  }
  return fetchAPI(`${managedSubjectPath(id)}/tokens`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function revokeManagedIdentityToken(
  id: string,
  tokenId: string,
): Promise<void> {
  await fetchAPI(
    `${managedSubjectPath(id)}/tokens/${encodeURIComponent(tokenId)}`,
    { method: "DELETE" },
  );
}
