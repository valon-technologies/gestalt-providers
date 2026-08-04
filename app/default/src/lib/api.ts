import { clearSession } from "./auth";
import { HTTP_UNAUTHORIZED } from "./constants";
import { serverLoginURL } from "./authReturn";

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
  /** True when this instance is the subject's preferred account for the connection. */
  preferred?: boolean;
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
  preferredInstance?: string;
  /**
   * True only when a chosen account exists (valid preferred, or a single valid
   * instance). Stored credentials without a chosen account leave this false.
   */
  connected?: boolean;
  mcpPassthrough?: boolean;
}

export interface Integration {
  name: string;
  displayName?: string;
  description?: string;
  iconSvg?: string;
  mountedPath?: string;
  managementPath?: string;
  connections?: ConnectionDefInfo[];
  status?: IntegrationStatus;
  credentialState?: CredentialState;
  healthState?: HealthState;
  actions?: IntegrationAction[];
  /** Curated prompts projected from the root app's deployment config. */
  prompts?: IntegrationPrompt[];
}

export interface IntegrationPrompt {
  id: string;
  text: string;
}

export interface AppAdminPublicationPullRequest {
  number: number;
  url: string;
  title?: string;
}

export interface AppAdminPublicationCommit {
  sha: string;
  url: string;
}

export interface AppAdminPublication {
  workflowRunUrl?: string;
  triggerPullRequest?: AppAdminPublicationPullRequest;
  triggerCommit?: AppAdminPublicationCommit;
}

export interface AppAdminPublishedVersion {
  version: string;
  publishedAt: string;
  publishStartedAt?: string;
  publishDurationSeconds?: number;
  platforms?: string[];
  sourceRef?: string;
  sourceUrl?: string;
  publication?: AppAdminPublication;
}

export interface AppAdminPendingVersion {
  version: string;
  startedAt: string;
  updatedAt: string;
  phase: string;
  publishingForSeconds?: number;
  sourceRef?: string;
  sourceUrl?: string;
  publication?: AppAdminPublication;
}

export interface AppAdminFailedVersion {
  version: string;
  startedAt: string;
  failedAt: string;
  reason: string;
  publishDurationSeconds?: number;
  sourceRef?: string;
  sourceUrl?: string;
  publication?: AppAdminPublication;
}

export interface AppAdminAutoDeploy {
  enabled: boolean;
  pendingVersion?: string;
  lastError?: string;
}

export interface AppAdminFleetReplica {
  instanceId: string;
  startedAt?: string;
  heartbeatAt: string;
  appState: string;
  runningVersion?: string;
  observedDesiredVersion?: string;
  lastError?: string;
  class: string;
}

export interface AppAdminFleetState {
  state: string;
  sourceVersion?: string;
  desiredVersion?: string;
  minimumHealthyInstances: number;
  liveInstances: number;
  runningDesiredVersion: number;
  mismatched: number;
  errors: number;
  heartbeatTtlSeconds: number;
  evaluatedAt: string;
  replicas?: AppAdminFleetReplica[];
}

export interface AppAdminRecovery {
  recoveredAt: string;
  sourceVersion: string;
  liveInstances: number;
  minimumHealthyInstances: number;
}

export interface AppAdminRegistryResponse {
  app: string;
  registry: string;
  desiredVersion?: string;
  knownVersions: Array<{
    version: string;
    installedAt?: string;
    installedBy?: string;
  }>;
  publishedVersions: AppAdminPublishedVersion[];
  pendingVersions?: AppAdminPendingVersion[];
  failedVersions?: AppAdminFailedVersion[];
  rollout?: {
    version: string;
    state: string;
    targetSourceVersion?: string;
  };
  fleetState?: AppAdminFleetState;
  recovery?: AppAdminRecovery;
  autoDeploy: AppAdminAutoDeploy;
  selectionDisabled: boolean;
  disabledReason?: string;
}

export interface AppAdminRegistryAutoDeployResponse {
  app: string;
  autoDeploy: AppAdminAutoDeploy;
}

export interface AppAdminRegistryVersionResponse {
  app: string;
  registry: string;
  fromVersion?: string;
  desiredVersion: string;
  rollout: {
    version: string;
    state: string;
    targetSourceVersion?: string;
  };
}

export interface AppAdminRegistryRevision {
  id: string;
  version: string;
  previousVersion?: string;
  deployedAt: string;
  deployedBy?: string;
  sourceRef?: string;
  sourceUrl?: string;
  publication?: AppAdminPublication;
  deploymentState?: string;
  deployableUntil?: string;
  current?: boolean;
  rolloutState?: string;
  rolloutForSeconds?: number;
  rolloutDurationSeconds?: number;
  rolloutCompletedAt?: string;
  rolloutFailedAt?: string;
  recovery?: AppAdminRecovery;
}

export interface AppAdminRegistryHistoryResponse {
  app: string;
  revisions: AppAdminRegistryRevision[];
  fleetState?: AppAdminFleetState;
  nextCursor?: string;
}

export interface IntegrationOperation {
  id: string;
  title?: string;
  description?: string;
  readOnly?: boolean;
  visible?: boolean;
  tags?: string[];
  method?: string;
  path?: string;
  allowedRoles?: string[];
  transport?: string;
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
  lastUsedAt?: string;
}

export interface CreateTokenResponse {
  id: string;
  name?: string;
  token: string;
  permissions?: AccessPermission[];
  expiresAt?: string;
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

export type AgentOutput =
  | { text: Record<string, never>; structured?: never }
  | { text?: never; structured: { schema: Record<string, unknown> } };

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
  /** Server-declared step-target app for this run (Workflow API `target_app`). */
  targetApp?: string;
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

type WorkflowRunWire = Omit<WorkflowRun, "target" | "steps"> & {
  target?: unknown;
  steps?: unknown;
};

export function normalizeWorkflowRun(run: WorkflowRunWire): WorkflowRun {
  const wire = run as WorkflowRunWire & {
    targetApp?: string;
    target_app?: string;
  };
  return {
    ...run,
    targetApp: optionalString(wire.targetApp) ?? optionalString(wire.target_app),
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

export function redirectToLogin(returnPath?: string): void {
  if (typeof window === "undefined") {
    return;
  }
  clearSession();
  if (!window.location.pathname.startsWith("/api/v1/auth/login")) {
    window.location.assign(serverLoginURL(returnPath));
  }
}

export const PENDING_CONNECTION_PATH = "/api/v1/auth/pending-connection";

/**
 * Resolve a request URL at the browser/host boundary.
 *
 * The browser owns only same-origin navigation: production gestaltd serves
 * `/api/*` itself, while Vite development proxies that same path to the
 * configured backend. The upstream target is therefore host configuration,
 * never a public client environment variable. Absolute URLs are preserved
 * for server-provided OAuth and connection-selection redirects.
 */
export function resolveAPIPath(path: string): string {
  if (/^[a-zA-Z][a-zA-Z\d+.-]*:/.test(path)) {
    return path;
  }
  if (!path.startsWith("/")) {
    throw new Error(
      `API path must be absolute (got ${JSON.stringify(path)})`,
    );
  }
  return path;
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
    redirectToLogin();
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
    /** Default `providers.workflow.*` name for this deployment. */
    workflowDefaultProvider?: string;
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

/**
 * App authorization member row from the admin control plane.
 * Same shape as `/admin/` Authorization → app members.
 */
export interface AppAuthorizationMember {
  email?: string;
  role?: string;
  source?: "static" | "dynamic" | string;
  mutable?: boolean;
  effective?: boolean;
  shadowedBy?: string;
  selectorKind?: string;
  selectorValue?: string;
  subjectId?: string;
}

/**
 * List humans (and selectors) with access to an app.
 * Requires admin authorization for the app; callers should handle 403.
 */
export async function getAppAuthorizationMembers(
  appName: string,
): Promise<AppAuthorizationMember[]> {
  const response = await fetchAPI<
    AppAuthorizationMember[] | { members?: AppAuthorizationMember[] }
  >(
    `/admin/api/v1/authorization/apps/${encodeURIComponent(appName)}/members`,
  );
  if (Array.isArray(response)) return response;
  return response.members ?? [];
}

export async function logout(): Promise<void> {
  await fetchAPI("/api/v1/auth/logout", { method: "POST" });
}

export async function getIntegrations(): Promise<Integration[]> {
  return fetchAPI<Integration[]>("/api/v1/apps");
}

export async function getAppAdminRegistry(
  app: string,
): Promise<AppAdminRegistryResponse> {
  return fetchAPI<AppAdminRegistryResponse>(
    `/api/v1/apps/${encodeURIComponent(app)}/admin/registry`,
  );
}

export async function selectAppAdminRegistryVersion(
  app: string,
  version: string,
): Promise<AppAdminRegistryVersionResponse> {
  return fetchAPI<AppAdminRegistryVersionResponse>(
    `/api/v1/apps/${encodeURIComponent(app)}/admin/registry/version`,
    {
      method: "POST",
      body: JSON.stringify({ version }),
    },
  );
}

export async function updateAppAdminRegistryAutoDeploy(
  app: string,
  enabled: boolean,
): Promise<AppAdminRegistryAutoDeployResponse> {
  return fetchAPI<AppAdminRegistryAutoDeployResponse>(
    `/api/v1/apps/${encodeURIComponent(app)}/admin/registry/auto-deploy`,
    {
      method: "PUT",
      body: JSON.stringify({ enabled }),
    },
  );
}

export async function getAppAdminRegistryHistory(
  app: string,
  options?: { limit?: number; cursor?: string },
): Promise<AppAdminRegistryHistoryResponse> {
  const params = new URLSearchParams();
  if (options?.limit) params.set("limit", String(options.limit));
  if (options?.cursor) params.set("cursor", options.cursor);
  const query = params.toString();
  return fetchAPI<AppAdminRegistryHistoryResponse>(
    `/api/v1/apps/${encodeURIComponent(app)}/admin/registry/history${query ? `?${query}` : ""}`,
  );
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

export type SelectPreferredInstanceResult = {
  status: string;
  integration?: string;
  connection?: string;
  instance?: string;
};

/** Set the active account for an app connection (server-owned preferred instance). */
export async function selectPreferredInstance(
  name: string,
  instance: string,
  connection?: string,
): Promise<SelectPreferredInstanceResult> {
  const body: Record<string, string> = { instance };
  if (connection) body.connection = connection;
  return fetchAPI(`/api/v1/apps/${encodeURIComponent(name)}/preferred-instance`, {
    method: "PUT",
    body: JSON.stringify(body),
  });
}

import {
  listPersonalAPITokens,
  revokePersonalAPIToken,
} from "./personalGrants";

export async function getTokens(): Promise<APIToken[]> {
  return listPersonalAPITokens(fetchAPI);
}

export async function createToken(
  name: string,
  scopes: string,
  expiresIn?: number,
): Promise<CreateTokenResponse> {
  const body: Record<string, unknown> = { name, scopes };
  if (expiresIn !== undefined) {
    body.expiresIn = expiresIn;
  }
  return fetchAPI("/api/v1/tokens", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function revokeToken(id: string): Promise<void> {
  return revokePersonalAPIToken(fetchAPI, id);
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
