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

export interface ConnectionDefInfo {
  name: string;
  displayName?: string;
  authTypes: ("oauth" | "manual")[];
  credentialFields?: CredentialFieldDef[];
}

export interface Integration {
  name: string;
  displayName?: string;
  description?: string;
  iconSvg?: string;
  mountedPath?: string;
  connected?: boolean;
  instances?: InstanceInfo[];
  authTypes?: ("oauth" | "manual")[];
  connectionParams?: Record<string, ConnectionParamDef>;
  connections?: ConnectionDefInfo[];
  credentialFields?: CredentialFieldDef[];
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

export interface WorkflowTarget {
  plugin: string;
  operation: string;
  connection?: string;
  instance?: string;
  input?: Record<string, unknown>;
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

export interface ManagedIdentity {
  id: string;
  displayName: string;
  role: "viewer" | "editor" | "admin";
  createdAt: string;
  updatedAt: string;
}

export interface ManagedIdentityMember {
  userId?: string;
  email: string;
  role: "viewer" | "editor" | "admin";
  createdAt: string;
  updatedAt: string;
}

export interface ManagedIdentityGrant {
  plugin: string;
  operations?: string[];
  createdAt: string;
  updatedAt: string;
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
  if (instance) query.set("instance", instance);
  if (connection) query.set("connection", connection);
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
  return fetchAPI("/api/v1/workflow/runs");
}

export async function getWorkflowRun(id: string): Promise<WorkflowRun> {
  return fetchAPI(`/api/v1/workflow/runs/${encodeURIComponent(id)}`);
}

export async function getWorkflowSchedules(): Promise<WorkflowSchedule[]> {
  return fetchAPI("/api/v1/workflow/schedules");
}

export async function getWorkflowSchedule(id: string): Promise<WorkflowSchedule> {
  return fetchAPI(`/api/v1/workflow/schedules/${encodeURIComponent(id)}`);
}

export async function createWorkflowSchedule(
  body: WorkflowScheduleUpsert,
): Promise<WorkflowSchedule> {
  return fetchAPI("/api/v1/workflow/schedules", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function updateWorkflowSchedule(
  id: string,
  body: WorkflowScheduleUpsert,
): Promise<WorkflowSchedule> {
  return fetchAPI(`/api/v1/workflow/schedules/${encodeURIComponent(id)}`, {
    method: "PUT",
    body: JSON.stringify(body),
  });
}

export async function deleteWorkflowSchedule(id: string): Promise<void> {
  await fetchAPI(`/api/v1/workflow/schedules/${encodeURIComponent(id)}`, {
    method: "DELETE",
  });
}

export async function pauseWorkflowSchedule(id: string): Promise<WorkflowSchedule> {
  return fetchAPI(`/api/v1/workflow/schedules/${encodeURIComponent(id)}/pause`, {
    method: "POST",
  });
}

export async function resumeWorkflowSchedule(id: string): Promise<WorkflowSchedule> {
  return fetchAPI(`/api/v1/workflow/schedules/${encodeURIComponent(id)}/resume`, {
    method: "POST",
  });
}

export async function getWorkflowEventTriggers(): Promise<WorkflowEventTrigger[]> {
  return fetchAPI("/api/v1/workflow/event-triggers");
}

export async function getWorkflowEventTrigger(
  id: string,
): Promise<WorkflowEventTrigger> {
  return fetchAPI(`/api/v1/workflow/event-triggers/${encodeURIComponent(id)}`);
}

export async function createWorkflowEventTrigger(
  body: WorkflowEventTriggerUpsert,
): Promise<WorkflowEventTrigger> {
  return fetchAPI("/api/v1/workflow/event-triggers", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function updateWorkflowEventTrigger(
  id: string,
  body: WorkflowEventTriggerUpsert,
): Promise<WorkflowEventTrigger> {
  return fetchAPI(`/api/v1/workflow/event-triggers/${encodeURIComponent(id)}`, {
    method: "PUT",
    body: JSON.stringify(body),
  });
}

export async function deleteWorkflowEventTrigger(id: string): Promise<void> {
  await fetchAPI(`/api/v1/workflow/event-triggers/${encodeURIComponent(id)}`, {
    method: "DELETE",
  });
}

export async function pauseWorkflowEventTrigger(
  id: string,
): Promise<WorkflowEventTrigger> {
  return fetchAPI(`/api/v1/workflow/event-triggers/${encodeURIComponent(id)}/pause`, {
    method: "POST",
  });
}

export async function resumeWorkflowEventTrigger(
  id: string,
): Promise<WorkflowEventTrigger> {
  return fetchAPI(`/api/v1/workflow/event-triggers/${encodeURIComponent(id)}/resume`, {
    method: "POST",
  });
}

export async function cancelWorkflowRun(
  id: string,
  reason?: string,
): Promise<WorkflowRun> {
  return fetchAPI(`/api/v1/workflow/runs/${encodeURIComponent(id)}/cancel`, {
    method: "POST",
    body: JSON.stringify(reason ? { reason } : {}),
  });
}

export async function getAgentRuns(opts?: {
  provider?: string;
  status?: string;
}): Promise<AgentRun[]> {
  const query = new URLSearchParams();
  if (opts?.provider) query.set("provider", opts.provider);
  if (opts?.status && opts.status !== "all") query.set("status", opts.status);
  const params = query.toString();
  return fetchAPI(`/api/v1/agent/runs${params ? `?${params}` : ""}`);
}

export async function getAgentRun(id: string): Promise<AgentRun> {
  return fetchAPI(`/api/v1/agent/runs/${encodeURIComponent(id)}`);
}

export async function createAgentRun(body: AgentRunCreate): Promise<AgentRun> {
  return fetchAPI("/api/v1/agent/runs", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function cancelAgentRun(
  id: string,
  reason?: string,
): Promise<AgentRun> {
  return fetchAPI(`/api/v1/agent/runs/${encodeURIComponent(id)}/cancel`, {
    method: "POST",
    body: JSON.stringify(reason ? { reason } : {}),
  });
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

export async function getManagedIdentities(): Promise<ManagedIdentity[]> {
  return fetchAPI("/api/v1/identities");
}

export async function createManagedIdentity(displayName: string): Promise<ManagedIdentity> {
  return fetchAPI("/api/v1/identities", {
    method: "POST",
    body: JSON.stringify({ displayName }),
  });
}

export async function getManagedIdentity(id: string): Promise<ManagedIdentity> {
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}`);
}

export async function updateManagedIdentity(id: string, displayName: string): Promise<ManagedIdentity> {
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}`, {
    method: "PATCH",
    body: JSON.stringify({ displayName }),
  });
}

export async function deleteManagedIdentity(id: string): Promise<void> {
  await fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}`, {
    method: "DELETE",
  });
}

export async function getManagedIdentityMembers(id: string): Promise<ManagedIdentityMember[]> {
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}/members`);
}

export async function putManagedIdentityMember(
  id: string,
  email: string,
  role: ManagedIdentityMember["role"],
): Promise<ManagedIdentityMember> {
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}/members`, {
    method: "PUT",
    body: JSON.stringify({ email, role }),
  });
}

export async function deleteManagedIdentityMember(id: string, email: string): Promise<void> {
  await fetchAPI(
    `/api/v1/identities/${encodeURIComponent(id)}/members/${encodeURIComponent(email)}`,
    { method: "DELETE" },
  );
}

export async function getManagedIdentityGrants(id: string): Promise<ManagedIdentityGrant[]> {
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}/grants`);
}

export async function putManagedIdentityGrant(
  id: string,
  plugin: string,
  operations?: string[],
): Promise<ManagedIdentityGrant> {
  return fetchAPI(
    `/api/v1/identities/${encodeURIComponent(id)}/grants/${encodeURIComponent(plugin)}`,
    {
      method: "PUT",
      body: JSON.stringify({ operations }),
    },
  );
}

export async function deleteManagedIdentityGrant(id: string, plugin: string): Promise<void> {
  await fetchAPI(
    `/api/v1/identities/${encodeURIComponent(id)}/grants/${encodeURIComponent(plugin)}`,
    { method: "DELETE" },
  );
}

export async function getManagedIdentityTokens(id: string): Promise<APIToken[]> {
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}/tokens`);
}

export async function createManagedIdentityToken(
  id: string,
  name: string,
  permissions: AccessPermission[],
): Promise<CreateTokenResponse> {
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}/tokens`, {
    method: "POST",
    body: JSON.stringify({ name, permissions }),
  });
}

export async function revokeManagedIdentityToken(id: string, tokenId: string): Promise<void> {
  await fetchAPI(
    `/api/v1/identities/${encodeURIComponent(id)}/tokens/${encodeURIComponent(tokenId)}`,
    { method: "DELETE" },
  );
}

export async function getManagedIdentityIntegrations(id: string): Promise<Integration[]> {
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}/integrations`);
}

export async function startManagedIdentityOAuth(
  id: string,
  integration: string,
  scopes?: string[],
  connectionParams?: Record<string, string>,
  instance?: string,
  connection?: string,
  returnPath?: string,
): Promise<{ url: string; state: string }> {
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}/auth/start-oauth`, {
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

export async function connectManagedIdentityManual(
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
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}/auth/connect-manual`, {
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
  if (instance) query.set("instance", instance);
  if (connection) query.set("connection", connection);
  const params = query.toString();
  await fetchAPI(
    `/api/v1/identities/${encodeURIComponent(id)}/integrations/${encodeURIComponent(name)}${params ? `?${params}` : ""}`,
    { method: "DELETE" },
  );
}
