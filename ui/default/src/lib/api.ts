import { clearSession } from "./auth";
import { HTTP_UNAUTHORIZED, LOGIN_PATH } from "./constants";

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
export type ConnectionMode = "none" | "user" | "platform";
export type CredentialMode = "none" | "subject" | "platform";
export type OwnerKind =
  | "none"
  | "current_user"
  | "managed_identity"
  | "service_account"
  | "platform"
  | "unknown";

export interface ConnectionDefInfo {
  name: string;
  displayName?: string;
  authTypes?: AuthType[];
  credentialFields?: CredentialFieldDef[];
  status?: IntegrationStatus;
  credentialState?: CredentialState;
  healthState?: HealthState;
  actions?: IntegrationAction[];
  mode?: ConnectionMode;
  credentialMode?: CredentialMode;
  ownerKind?: OwnerKind;
  disconnectable?: boolean;
  connected?: boolean;
  connectable?: boolean;
  instances?: InstanceInfo[];
  mcpPassthrough?: boolean;
}

export interface Integration {
  name: string;
  displayName?: string;
  description?: string;
  iconSvg?: string;
  mountedPath?: string;
  connected?: boolean;
  instances?: InstanceInfo[];
  authTypes?: AuthType[];
  connectionParams?: Record<string, ConnectionParamDef>;
  connections?: ConnectionDefInfo[];
  credentialFields?: CredentialFieldDef[];
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
  name: string;
  scopes?: string;
  permissions?: AccessPermission[];
  createdAt: string;
  expiresAt?: string;
}

export interface CreateTokenResponse {
  id: string;
  name: string;
  token: string;
  permissions?: AccessPermission[];
  expiresAt?: string;
}

export interface WorkflowPluginTarget {
  name: string;
  operation: string;
  connection?: string;
  instance?: string;
  input?: Record<string, unknown>;
}

export interface WorkflowTarget {
  plugin: WorkflowPluginTarget;
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
  scheduleId?: string;
  scheduledFor?: string;
  triggerId?: string;
  event?: WorkflowEvent;
}

export interface WorkflowActor {
  subjectId?: string;
  subjectKind?: string;
  displayName?: string;
  authSource?: string;
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
  resultBody?: string;
}

export interface WorkflowSchedule {
  id: string;
  provider: string;
  cron: string;
  timezone?: string;
  target: WorkflowTarget;
  paused: boolean;
  createdAt?: string;
  updatedAt?: string;
  nextRunAt?: string;
}

export interface WorkflowEventTriggerMatch {
  type: string;
  source?: string;
  subject?: string;
}

export interface WorkflowEventTrigger {
  id: string;
  provider: string;
  match: WorkflowEventTriggerMatch;
  target: WorkflowTarget;
  paused: boolean;
  createdAt?: string;
  updatedAt?: string;
}

export interface WorkflowScheduleUpsert {
  provider?: string;
  cron: string;
  timezone?: string;
  target: WorkflowTarget;
  paused?: boolean;
}

export interface WorkflowEventTriggerUpsert {
  provider?: string;
  match: WorkflowEventTriggerMatch;
  target: WorkflowTarget;
  paused?: boolean;
}

type WorkflowRunWire = Omit<WorkflowRun, "target"> & { target?: unknown };
type WorkflowScheduleWire = Omit<WorkflowSchedule, "target"> & { target?: unknown };
type WorkflowEventTriggerWire = Omit<WorkflowEventTrigger, "target"> & { target?: unknown };

function normalizeWorkflowRun(run: WorkflowRunWire): WorkflowRun {
  return {
    ...run,
    target: normalizeWorkflowTarget(run.target),
  };
}

function normalizeWorkflowSchedule(schedule: WorkflowScheduleWire): WorkflowSchedule {
  return {
    ...schedule,
    target: normalizeWorkflowTarget(schedule.target),
  };
}

function normalizeWorkflowEventTrigger(
  trigger: WorkflowEventTriggerWire,
): WorkflowEventTrigger {
  return {
    ...trigger,
    target: normalizeWorkflowTarget(trigger.target),
  };
}

function normalizeWorkflowTarget(target: unknown): WorkflowTarget {
  if (!isRecord(target)) {
    return { plugin: { name: "", operation: "" } };
  }

  const rawPlugin = target.plugin;
  if (isRecord(rawPlugin)) {
    return {
      plugin: {
        name: stringValue(rawPlugin.name),
        operation: stringValue(rawPlugin.operation),
        connection: optionalString(rawPlugin.connection),
        instance: optionalString(rawPlugin.instance),
        input: optionalRecord(rawPlugin.input),
      },
    };
  }

  return {
    plugin: {
      name: stringValue(rawPlugin),
      operation: stringValue(target.operation),
      connection: optionalString(target.connection),
      instance: optionalString(target.instance),
      input: optionalRecord(target.input),
    },
  };
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

export interface AgentMessage {
  role: string;
  text: string;
}

export interface AgentActor {
  subjectId?: string;
  subjectKind?: string;
  displayName?: string;
  authSource?: string;
}

export interface AgentToolRef {
  pluginName: string;
  operation: string;
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
  status?: string;
  messages?: AgentMessage[];
  outputText?: string;
  structuredOutput?: Record<string, unknown>;
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
  toolSource?: "explicit" | "inherit_invokes";
  responseSchema?: Record<string, unknown>;
  sessionRef?: string;
  metadata?: Record<string, unknown>;
  providerOptions?: Record<string, unknown>;
  idempotencyKey?: string;
}

interface AgentSession {
  id: string;
  provider: string;
  model?: string;
  clientRef?: string;
  state?: string;
  createdAt?: string;
  updatedAt?: string;
  lastTurnAt?: string;
}

type AgentTurnWire = Omit<AgentRun, "sessionRef"> & {
  sessionId: string;
};

function normalizeAgentRun(turn: AgentTurnWire, session?: AgentSession): AgentRun {
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

function agentToolRefsToRequest(toolRefs?: AgentToolRef[]) {
  return toolRefs?.map((tool) => ({
    plugin: tool.pluginName,
    operation: tool.operation,
    connection: tool.connection,
    instance: tool.instance,
    title: tool.title,
    description: tool.description,
  }));
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
      window.location.href = LOGIN_PATH;
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

export async function getAuthInfo(): Promise<AuthInfo> {
  return fetchAPI("/api/v1/auth/info");
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
): Promise<{ email: string; displayName: string }> {
  const params = new URLSearchParams({ code });
  if (state) params.set("state", state);
  return fetchAPI(`/api/v1/auth/login/callback?${params}`);
}

export async function logout(): Promise<void> {
  await fetchAPI("/api/v1/auth/logout", { method: "POST" });
}

export async function getIntegrations(): Promise<Integration[]> {
  return fetchAPI<Integration[]>("/api/v1/integrations");
}

export async function getIntegrationOperations(
  integration: string,
): Promise<IntegrationOperation[]> {
  return fetchAPI<IntegrationOperation[]>(
    `/api/v1/integrations/${encodeURIComponent(integration)}/operations`,
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
    `/api/v1/integrations/${encodeURIComponent(name)}${params ? `?${params}` : ""}`,
    {
      method: "DELETE",
    },
  );
}

export async function getTokens(): Promise<APIToken[]> {
  return fetchAPI("/api/v1/tokens");
}

export async function getWorkflowRuns(): Promise<WorkflowRun[]> {
  const runs = await fetchAPI<WorkflowRunWire[]>("/api/v1/workflow/runs");
  return runs.map(normalizeWorkflowRun);
}

export async function getWorkflowRun(id: string): Promise<WorkflowRun> {
  const run = await fetchAPI<WorkflowRunWire>(
    `/api/v1/workflow/runs/${encodeURIComponent(id)}`,
  );
  return normalizeWorkflowRun(run);
}

export async function getWorkflowSchedules(): Promise<WorkflowSchedule[]> {
  const schedules = await fetchAPI<WorkflowScheduleWire[]>("/api/v1/workflow/schedules");
  return schedules.map(normalizeWorkflowSchedule);
}

export async function getWorkflowSchedule(id: string): Promise<WorkflowSchedule> {
  const schedule = await fetchAPI<WorkflowScheduleWire>(
    `/api/v1/workflow/schedules/${encodeURIComponent(id)}`,
  );
  return normalizeWorkflowSchedule(schedule);
}

export async function createWorkflowSchedule(
  body: WorkflowScheduleUpsert,
): Promise<WorkflowSchedule> {
  const schedule = await fetchAPI<WorkflowScheduleWire>("/api/v1/workflow/schedules", {
    method: "POST",
    body: JSON.stringify(body),
  });
  return normalizeWorkflowSchedule(schedule);
}

export async function updateWorkflowSchedule(
  id: string,
  body: WorkflowScheduleUpsert,
): Promise<WorkflowSchedule> {
  const schedule = await fetchAPI<WorkflowScheduleWire>(
    `/api/v1/workflow/schedules/${encodeURIComponent(id)}`,
    {
      method: "PUT",
      body: JSON.stringify(body),
    },
  );
  return normalizeWorkflowSchedule(schedule);
}

export async function deleteWorkflowSchedule(id: string): Promise<void> {
  await fetchAPI(`/api/v1/workflow/schedules/${encodeURIComponent(id)}`, {
    method: "DELETE",
  });
}

export async function pauseWorkflowSchedule(id: string): Promise<WorkflowSchedule> {
  const schedule = await fetchAPI<WorkflowScheduleWire>(
    `/api/v1/workflow/schedules/${encodeURIComponent(id)}/pause`,
    {
      method: "POST",
    },
  );
  return normalizeWorkflowSchedule(schedule);
}

export async function resumeWorkflowSchedule(id: string): Promise<WorkflowSchedule> {
  const schedule = await fetchAPI<WorkflowScheduleWire>(
    `/api/v1/workflow/schedules/${encodeURIComponent(id)}/resume`,
    {
      method: "POST",
    },
  );
  return normalizeWorkflowSchedule(schedule);
}

export async function getWorkflowEventTriggers(): Promise<WorkflowEventTrigger[]> {
  const triggers = await fetchAPI<WorkflowEventTriggerWire[]>(
    "/api/v1/workflow/event-triggers",
  );
  return triggers.map(normalizeWorkflowEventTrigger);
}

export async function getWorkflowEventTrigger(
  id: string,
): Promise<WorkflowEventTrigger> {
  const trigger = await fetchAPI<WorkflowEventTriggerWire>(
    `/api/v1/workflow/event-triggers/${encodeURIComponent(id)}`,
  );
  return normalizeWorkflowEventTrigger(trigger);
}

export async function createWorkflowEventTrigger(
  body: WorkflowEventTriggerUpsert,
): Promise<WorkflowEventTrigger> {
  const trigger = await fetchAPI<WorkflowEventTriggerWire>(
    "/api/v1/workflow/event-triggers",
    {
      method: "POST",
      body: JSON.stringify(body),
    },
  );
  return normalizeWorkflowEventTrigger(trigger);
}

export async function updateWorkflowEventTrigger(
  id: string,
  body: WorkflowEventTriggerUpsert,
): Promise<WorkflowEventTrigger> {
  const trigger = await fetchAPI<WorkflowEventTriggerWire>(
    `/api/v1/workflow/event-triggers/${encodeURIComponent(id)}`,
    {
      method: "PUT",
      body: JSON.stringify(body),
    },
  );
  return normalizeWorkflowEventTrigger(trigger);
}

export async function deleteWorkflowEventTrigger(id: string): Promise<void> {
  await fetchAPI(`/api/v1/workflow/event-triggers/${encodeURIComponent(id)}`, {
    method: "DELETE",
  });
}

export async function pauseWorkflowEventTrigger(
  id: string,
): Promise<WorkflowEventTrigger> {
  const trigger = await fetchAPI<WorkflowEventTriggerWire>(
    `/api/v1/workflow/event-triggers/${encodeURIComponent(id)}/pause`,
    {
      method: "POST",
    },
  );
  return normalizeWorkflowEventTrigger(trigger);
}

export async function resumeWorkflowEventTrigger(
  id: string,
): Promise<WorkflowEventTrigger> {
  const trigger = await fetchAPI<WorkflowEventTriggerWire>(
    `/api/v1/workflow/event-triggers/${encodeURIComponent(id)}/resume`,
    {
      method: "POST",
    },
  );
  return normalizeWorkflowEventTrigger(trigger);
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

export async function getAgentRuns(opts?: {
  provider?: string;
  status?: string;
}): Promise<AgentRun[]> {
  const sessionQuery = new URLSearchParams({
    view: "summary",
    limit: "50",
  });
  if (opts?.provider) sessionQuery.set("provider", opts.provider);
  const sessions = await fetchAPI<AgentSession[]>(
    `/api/v1/agent/sessions?${sessionQuery}`,
  );

  const turnLists = await Promise.all(
    sessions.map(async (session) => {
      const turnQuery = new URLSearchParams({
        limit: "20",
      });
      if (opts?.status && opts.status !== "all") {
        turnQuery.set("status", opts.status);
      }
      const turns = await fetchAPI<AgentTurnWire[]>(
        `/api/v1/agent/sessions/${encodeURIComponent(session.id)}/turns?${turnQuery}`,
      );
      return turns.map((turn) => normalizeAgentRun(turn, session));
    }),
  );

  return turnLists.flat().sort(compareAgentRunsDesc);
}

export async function getAgentRun(id: string): Promise<AgentRun> {
  const turn = await fetchAPI<AgentTurnWire>(
    `/api/v1/agent/turns/${encodeURIComponent(id)}`,
  );
  return normalizeAgentRun(turn);
}

export async function createAgentRun(body: AgentRunCreate): Promise<AgentRun> {
  const session = await fetchAPI<AgentSession>("/api/v1/agent/sessions", {
    method: "POST",
    body: JSON.stringify({
      provider: body.provider,
      model: body.model,
      clientRef: body.sessionRef,
      metadata: body.metadata,
      providerOptions: body.providerOptions,
      idempotencyKey: idempotencyKeyPart("session", body.idempotencyKey),
    }),
  });

  const turn = await fetchAPI<AgentTurnWire>(
    `/api/v1/agent/sessions/${encodeURIComponent(session.id)}/turns`,
    {
      method: "POST",
      body: JSON.stringify({
        model: body.model,
        messages: body.messages,
        toolRefs: agentToolRefsToRequest(body.toolRefs),
        toolSource: body.toolSource,
        responseSchema: body.responseSchema,
        metadata: body.metadata,
        providerOptions: body.providerOptions,
        idempotencyKey: idempotencyKeyPart("turn", body.idempotencyKey),
      }),
    },
  );
  return normalizeAgentRun(turn, session);
}

export async function cancelAgentRun(
  id: string,
  reason?: string,
): Promise<AgentRun> {
  const turn = await fetchAPI<AgentTurnWire>(
    `/api/v1/agent/turns/${encodeURIComponent(id)}/cancel`,
    {
      method: "POST",
      body: JSON.stringify(reason ? { reason } : {}),
    },
  );
  return normalizeAgentRun(turn);
}

export async function createToken(name: string): Promise<CreateTokenResponse> {
  return fetchAPI("/api/v1/tokens", {
    method: "POST",
    body: JSON.stringify({ name }),
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

export async function getManagedIdentityIntegrations(id: string): Promise<Integration[]> {
  return fetchAPI<Integration[]>(`${managedSubjectPath(id)}/integrations`);
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
    `${managedSubjectPath(id)}/integrations/${encodeURIComponent(name)}${params ? `?${params}` : ""}`,
    {
      method: "DELETE",
    },
  );
}

export async function updateManagedIdentity(id: string, displayName: string): Promise<ManagedIdentity> {
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

export async function getManagedIdentityMembers(id: string): Promise<ManagedIdentityMember[]> {
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

export async function deleteManagedIdentityMember(id: string, memberSubjectID: string): Promise<void> {
  await fetchAPI(
    `${managedSubjectPath(id)}/members/${encodeURIComponent(memberSubjectID)}`,
    { method: "DELETE" },
  );
}

export async function getManagedIdentityGrants(id: string): Promise<ManagedIdentityGrant[]> {
  return fetchAPI(`${managedSubjectPath(id)}/grants`);
}

export async function putManagedIdentityGrant(
  id: string,
  plugin: string,
  role: ManagedIdentityGrant["role"],
): Promise<ManagedIdentityGrant> {
  const response = await fetchAPI<ManagedIdentityGrant | { grant?: ManagedIdentityGrant }>(
    `${managedSubjectPath(id)}/grants/${encodeURIComponent(plugin)}`,
    {
      method: "PUT",
      body: JSON.stringify({ role }),
    },
  );
  return unwrapManagedIdentityGrant(response);
}

export async function deleteManagedIdentityGrant(id: string, plugin: string): Promise<void> {
  await fetchAPI(
    `${managedSubjectPath(id)}/grants/${encodeURIComponent(plugin)}`,
    { method: "DELETE" },
  );
}

export async function getManagedIdentityTokens(id: string): Promise<APIToken[]> {
  return fetchAPI(`${managedSubjectPath(id)}/tokens`);
}

export async function createManagedIdentityToken(
  id: string,
  name: string,
  permissions: AccessPermission[],
): Promise<CreateTokenResponse> {
  return fetchAPI(`${managedSubjectPath(id)}/tokens`, {
    method: "POST",
    body: JSON.stringify({ name, permissions }),
  });
}

export async function revokeManagedIdentityToken(id: string, tokenId: string): Promise<void> {
  await fetchAPI(
    `${managedSubjectPath(id)}/tokens/${encodeURIComponent(tokenId)}`,
    { method: "DELETE" },
  );
}
