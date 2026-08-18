import { isAdminMetricsScrapeText } from "./admin-metrics-response";
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

export interface IdentityFact {
  kind: string;
  value: string;
  /** SCIM-style: at most one fact should be primary. */
  primary?: boolean;
}

export interface AccountIdentity {
  facts: IdentityFact[];
}

export interface InstanceInfo {
  name: string;
  connection?: string;
  /** True when this instance is the subject's preferred account for the connection. */
  preferred?: boolean;
  /** Provider-recognized account facts for Connection UI (email, workspace, …). */
  identity?: AccountIdentity;
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
  /**
   * Browseable source tree for this app (http(s) URL). Distinct from
   * per-version `sourceUrl`, which points at a published commit.
   */
  sourceTreeUrl?: string;
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
  /** Retention lifecycle: desired | redeployable | locked | available | expired. */
  deploymentState?: string;
  deployableUntil?: string;
  current?: boolean;
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
  observedAt?: string;
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

export interface APITokenScope {
  scope: string;
  /** Optional resource constraints from identity grants. */
  resources?: string[];
}

export interface APIToken {
  id: string;
  name?: string;
  scopes?: string[];
  /** Structured scopes when the grant API returns resource bindings. */
  scopeDetails?: APITokenScope[];
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

/**
 * Forward-compatible job graph (GitHub Actions-style). Today the backend runs
 * steps sequentially; the UI accepts optional `stages` so parallel job groups
 * can render when the platform starts emitting them.
 */
export interface WorkflowRunJobStep {
  id: string;
  name: string;
  status?: string;
  durationMs?: number | null;
  startedAt?: string;
  completedAt?: string;
  statusMessage?: string;
  skipReason?: string;
}

export interface WorkflowRunJob {
  id: string;
  name: string;
  status?: string;
  durationMs?: number | null;
  startedAt?: string;
  completedAt?: string;
  steps: WorkflowRunJobStep[];
}

export interface WorkflowRunStage {
  id: string;
  kind: "sequential" | "parallel";
  jobs: WorkflowRunJob[];
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
  /** Subject the run executes as (Workflow API `run_as`). */
  runAs?: string;
  steps?: WorkflowStepExecution[];
  /** Optional job graph; when absent the UI projects from `steps`. */
  stages?: WorkflowRunStage[];
}

/** Workflow recipe (config), distinct from a WorkflowRun execution. */
export interface WorkflowDefinitionActivation {
  id: string;
  paused?: boolean;
  input?: Record<string, unknown>;
  trigger?: {
    kind?: string;
    case?: string;
    cron?: string;
    timezone?: string;
    eventType?: string;
    eventSource?: string;
    eventSubject?: string;
  };
}

export interface WorkflowDefinition {
  id: string;
  provider: string;
  generation?: number;
  paused?: boolean;
  runAs?: string;
  createdAt?: string;
  updatedAt?: string;
  target: WorkflowTarget;
  activations: WorkflowDefinitionActivation[];
}

type WorkflowRunWire = Omit<WorkflowRun, "target" | "steps" | "stages"> & {
  target?: unknown;
  steps?: unknown;
  stages?: unknown;
};

/** Canonical UI/domain statuses for workflow runs, jobs, and steps. */
export type WorkflowStatus =
  | "pending"
  | "running"
  | "succeeded"
  | "failed"
  | "canceled"
  | "skipped"
  | "unknown";

const WORKFLOW_STATUS_VALUES = new Set<string>([
  "pending",
  "running",
  "succeeded",
  "failed",
  "canceled",
  "skipped",
  "unknown",
]);

/**
 * Collapse wire statuses (short names or proto enums like
 * `WORKFLOW_RUN_STATUS_SUCCEEDED` / `WORKFLOW_STEP_STATUS_FAILED`) into the
 * canonical domain vocabulary UI and filters expect.
 */
export function normalizeWorkflowStatus(status?: string | null): WorkflowStatus {
  let value = (status || "").trim().toLowerCase();
  if (!value) return "unknown";
  value = value
    .replace(/^workflow_run_status_/, "")
    .replace(/^workflow_step_status_/, "")
    .replace(/^workflow_job_status_/, "");
  if (value === "cancelled") value = "canceled";
  if (WORKFLOW_STATUS_VALUES.has(value)) {
    return value as WorkflowStatus;
  }
  return "unknown";
}

export function normalizeWorkflowRun(run: WorkflowRunWire): WorkflowRun {
  const wire = run as WorkflowRunWire & {
    targetApp?: string;
    target_app?: string;
    runAs?: string;
    run_as?: string;
    definitionId?: string;
    definition_id?: string;
  };
  return {
    ...run,
    status: normalizeWorkflowStatus(optionalString(run.status)),
    targetApp: optionalString(wire.targetApp) ?? optionalString(wire.target_app),
    runAs: optionalString(wire.runAs) ?? optionalString(wire.run_as),
    definitionId:
      optionalString(wire.definitionId) ?? optionalString(wire.definition_id),
    target: normalizeWorkflowTarget(run.target),
    steps: normalizeWorkflowStepExecutions(run.steps),
    stages: normalizeWorkflowRunStages(run.stages),
  };
}

type WorkflowDefinitionWire = Record<string, unknown>;

export function normalizeWorkflowDefinition(
  raw: WorkflowDefinitionWire,
): WorkflowDefinition {
  const id = optionalString(raw.id) ?? "";
  const provider = optionalString(raw.provider) ?? "";
  const generationRaw = raw.generation;
  const generation =
    typeof generationRaw === "number"
      ? generationRaw
      : typeof generationRaw === "string"
        ? Number(generationRaw)
        : undefined;
  const activationsRaw = Array.isArray(raw.activations) ? raw.activations : [];
  return {
    id,
    provider,
    generation: Number.isFinite(generation) ? generation : undefined,
    paused: Boolean(raw.paused),
    runAs: optionalString(raw.runAs) ?? optionalString(raw.run_as),
    createdAt: optionalString(raw.createdAt) ?? optionalString(raw.created_at),
    updatedAt: optionalString(raw.updatedAt) ?? optionalString(raw.updated_at),
    target: normalizeWorkflowTarget(raw.target),
    activations: activationsRaw.map(normalizeWorkflowActivation),
  };
}

function normalizeWorkflowActivation(
  raw: unknown,
): WorkflowDefinitionActivation {
  if (!isRecord(raw)) return { id: "" };
  const triggerRaw = isRecord(raw.trigger) ? raw.trigger : null;
  const caseName =
    optionalString(triggerRaw?.case) ??
    optionalString(triggerRaw?.kind) ??
    undefined;
  const value = isRecord(triggerRaw?.value) ? triggerRaw.value : triggerRaw;
  const match = isRecord(value?.match) ? value.match : null;
  return {
    id: optionalString(raw.id) ?? "",
    paused: Boolean(raw.paused),
    input: optionalRecord(raw.input),
    trigger: {
      kind: caseName,
      case: caseName,
      cron: optionalString(value?.cron),
      timezone: optionalString(value?.timezone),
      eventType: optionalString(match?.type) ?? optionalString(value?.type),
      eventSource:
        optionalString(match?.source) ?? optionalString(value?.source),
      eventSubject:
        optionalString(match?.subject) ?? optionalString(value?.subject),
    },
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

function normalizeWorkflowRunStages(
  value: unknown,
): WorkflowRunStage[] | undefined {
  if (!Array.isArray(value) || value.length === 0) return undefined;
  const stages = value.flatMap((rawStage, stageIndex) => {
    if (!isRecord(rawStage)) return [];
    const jobsRaw = Array.isArray(rawStage.jobs) ? rawStage.jobs : [];
    const jobs = jobsRaw.flatMap((rawJob, jobIndex) => {
      if (!isRecord(rawJob)) return [];
      const stepsRaw = Array.isArray(rawJob.steps) ? rawJob.steps : [];
      const steps = stepsRaw.flatMap((rawStep, stepIndex) => {
        if (!isRecord(rawStep)) return [];
        const durationRaw = rawStep.durationMs ?? rawStep.duration_ms;
        const durationMs =
          typeof durationRaw === "number"
            ? durationRaw
            : typeof durationRaw === "string"
              ? Number(durationRaw)
              : undefined;
        return [
          {
            id:
              optionalString(rawStep.id) ||
              `step-${stageIndex + 1}-${jobIndex + 1}-${stepIndex + 1}`,
            name:
              optionalString(rawStep.name) ||
              optionalString(rawStep.id) ||
              `Step ${stepIndex + 1}`,
            status: normalizeWorkflowStatus(optionalString(rawStep.status)),
            durationMs: Number.isFinite(durationMs) ? durationMs : undefined,
            startedAt:
              optionalString(rawStep.startedAt) ??
              optionalString(rawStep.started_at),
            completedAt:
              optionalString(rawStep.completedAt) ??
              optionalString(rawStep.completed_at),
            statusMessage:
              optionalString(rawStep.statusMessage) ??
              optionalString(rawStep.status_message),
            skipReason:
              optionalString(rawStep.skipReason) ??
              optionalString(rawStep.skip_reason),
          },
        ];
      });
      const jobDurationRaw = rawJob.durationMs ?? rawJob.duration_ms;
      const jobDurationMs =
        typeof jobDurationRaw === "number"
          ? jobDurationRaw
          : typeof jobDurationRaw === "string"
            ? Number(jobDurationRaw)
            : undefined;
      return [
        {
          id:
            optionalString(rawJob.id) ||
            `job-${stageIndex + 1}-${jobIndex + 1}`,
          name:
            optionalString(rawJob.name) ||
            optionalString(rawJob.id) ||
            `Job ${jobIndex + 1}`,
          status: normalizeWorkflowStatus(optionalString(rawJob.status)),
          durationMs: Number.isFinite(jobDurationMs) ? jobDurationMs : undefined,
          startedAt:
            optionalString(rawJob.startedAt) ??
            optionalString(rawJob.started_at),
          completedAt:
            optionalString(rawJob.completedAt) ??
            optionalString(rawJob.completed_at),
          steps,
        },
      ];
    });
    const kindRaw = optionalString(rawStage.kind)?.toLowerCase();
    return [
      {
        id: optionalString(rawStage.id) || `stage-${stageIndex + 1}`,
        kind: kindRaw === "parallel" ? ("parallel" as const) : ("sequential" as const),
        jobs,
      },
    ];
  });
  return stages.length > 0 ? stages : undefined;
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
        status: normalizeWorkflowStatus(optionalString(rawStep.status)),
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
        status: normalizeWorkflowStatus(optionalString(rawAttempt.status)),
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

/** The request was aborted because it exceeded this client's timeout. */
export class APITimeoutError extends Error {
  constructor(message = "Request timed out") {
    super(message);
    this.name = "APITimeoutError";
  }
}

export function isAPIErrorStatus(error: unknown, status: number): boolean {
  return error instanceof APIError && error.status === status;
}

export function isAPITimeoutError(error: unknown): boolean {
  return error instanceof APITimeoutError;
}

export type FetchAPIOptions = RequestInit & {
  /** Abort the request if it has not settled within this many milliseconds. */
  timeoutMs?: number;
};

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

function mergeAbortSignals(
  ...signals: Array<AbortSignal | null | undefined>
): AbortSignal | undefined {
  const active = signals.filter((signal): signal is AbortSignal => signal != null);
  if (active.length === 0) return undefined;
  if (active.length === 1) return active[0];
  return AbortSignal.any(active);
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
  options?: FetchAPIOptions,
): Promise<T> {
  const { timeoutMs, signal: callerSignal, headers, ...requestInit } =
    options ?? {};
  const timeoutSignal =
    timeoutMs != null && timeoutMs > 0
      ? AbortSignal.timeout(timeoutMs)
      : undefined;
  const signal = mergeAbortSignals(callerSignal, timeoutSignal);

  let res: Response;
  try {
    res = await fetch(resolveAPIPath(path), {
      ...requestInit,
      signal,
      credentials: "include",
      headers: {
        "Content-Type": "application/json",
        ...headers,
      },
    });
  } catch (error) {
    if (isAbortError(error)) {
      if (callerSignal?.aborted) throw error;
      if (timeoutSignal?.aborted) throw new APITimeoutError();
    }
    throw error;
  }

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
 * App authorization member row from GET /api/v1/apps/{app}/admin/members.
 * Humans are subject_id; groups are subject_set (e.g. group:eng#member).
 * Service-account grants live on AppAdminIdentity.
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

export interface AdminPlatformAdminsResponse {
  resource: AuthorizationResource;
  role: string;
  members: AppAuthorizationMember[];
}

export interface AppAdminOperationMetric {
  operation: string;
  requests: number;
  errors: number;
  durationSecondsSum: number;
  durationSecondsCount: number;
}

export interface AppAdminMetricsResponse {
  app: string;
  available: boolean;
  requests: number;
  errors: number;
  durationSecondsSum: number;
  durationSecondsCount: number;
  operations: AppAdminOperationMetric[];
}

export interface AdminFleetState {
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
}

export interface AdminFleetReplica {
  instanceId: string;
  sourceVersion: string;
  currentSource: boolean;
  sourceStatus: "current" | "superseded" | "unavailable";
  fresh: boolean;
  startedAt?: string;
  heartbeatAt: string;
  heartbeatAgeSeconds: number;
  appObservation: {
    state: "running" | "starting" | "not_running" | "error" | "unknown";
    desiredVersion?: string;
    runningVersion?: string;
    observedAt?: string;
    lastError?: string;
  };
}

export interface AdminRegistryAppSummary {
  app: string;
  registry: string;
  desiredVersion?: string;
  rollout?: {
    version: string;
    state: string;
    targetSourceVersion?: string;
    createdAt: string;
    enrollmentEndsAt: string;
    deadline: string;
    completedAt?: string;
    failedAt?: string;
  };
  cohort?: {
    acknowledged: number;
    materialized: number;
    restarted: number;
    failed: number;
  };
  fleetState: AdminFleetState;
}

export interface AdminRegistryAppDetail extends AdminRegistryAppSummary {
  knownVersions: Array<{
    version: string;
    installedAt?: string;
    installedBy?: string;
  }>;
  latestPublished?: {
    version: string;
    publishedAt: string;
  };
  freshReplicas: AdminFleetReplica[];
  staleReplicas: AdminFleetReplica[];
}

export interface AuthorizationResource {
  type: string;
  id: string;
}

export interface AuthorizationSubject {
  type: string;
  id: string;
}

export interface AuthorizationRelationshipTarget {
  subject?: AuthorizationSubject;
  subjectSet?: {
    resource: AuthorizationResource;
    relation: string;
  };
}

export interface AuthorizationRelationshipTuple {
  resource: AuthorizationResource;
  relation: string;
  target: AuthorizationRelationshipTarget;
}

export interface AuthorizationResourceType {
  name: string;
  defaultRole?: string;
}

/**
 * Agent identity grant on an app (service_account subject).
 * Sourced from the same authorization relationships as members.
 */
export interface AppAdminIdentity {
  subjectId: string;
  displayName: string;
  role: string;
  source?: "static" | "dynamic" | string;
  mutable?: boolean;
  effective?: boolean;
  shadowedBy?: string;
}

/**
 * List humans and groups with access to an app.
 * Requires app admin; callers should handle 403.
 */
export async function getAppAuthorizationMembers(
  appName: string,
): Promise<AppAuthorizationMember[]> {
  const response = await fetchAPI<
    AppAuthorizationMember[] | { members?: AppAuthorizationMember[] }
  >(`/api/v1/apps/${encodeURIComponent(appName)}/admin/members`);
  if (Array.isArray(response)) return response;
  return response.members ?? [];
}

/**
 * List service-account identities with a grant on this app.
 * Requires admin authorization for the app; callers should handle 403.
 */
export async function getAppAdminIdentities(
  appName: string,
): Promise<AppAdminIdentity[]> {
  const response = await fetchAPI<
    AppAdminIdentity[] | { identities?: AppAdminIdentity[] }
  >(`/api/v1/apps/${encodeURIComponent(appName)}/admin/identities`);
  if (Array.isArray(response)) return response;
  return response.identities ?? [];
}

export async function listAuthorizationResourceTypes(): Promise<
  AuthorizationResourceType[]
> {
  const types: AuthorizationResourceType[] = [];
  let pageToken = "";
  for (;;) {
    const query = new URLSearchParams({ pageSize: "100" });
    if (pageToken) query.set("pageToken", pageToken);
    const response = await fetchAPI<{
      resourceTypes?: Array<{ name?: string; defaultRole?: string; default_role?: string }>;
      resource_types?: Array<{ name?: string; defaultRole?: string; default_role?: string }>;
      nextPageToken?: string;
      next_page_token?: string;
    }>(`/api/v2/authorization/models/active/resource-types?${query}`);
    const raw = response.resourceTypes ?? response.resource_types ?? [];
    for (const item of raw) {
      if (!item.name) continue;
      types.push({
        name: item.name,
        defaultRole: item.defaultRole ?? item.default_role,
      });
    }
    pageToken =
      response.nextPageToken?.trim() ||
      response.next_page_token?.trim() ||
      "";
    if (!pageToken) return types;
  }
}

export async function addAuthorizationRelationship(
  tuple: AuthorizationRelationshipTuple,
): Promise<void> {
  await fetchAPI("/api/v2/authorization/relationships", {
    method: "POST",
    body: JSON.stringify({
      relationship: { tuple },
    }),
  });
}

export async function deleteAuthorizationRelationship(
  tuple: AuthorizationRelationshipTuple,
): Promise<void> {
  await fetchAPI("/api/v2/authorization/relationships:delete", {
    method: "POST",
    body: JSON.stringify({ relationshipTuple: tuple }),
  });
}

export async function logout(): Promise<void> {
  await fetchAPI("/api/v1/auth/logout", { method: "POST" });
}

/**
 * Client abort for GET /api/v1/apps so the UI can leave loading and show
 * retry instead of waiting for an upstream gateway failure.
 */
export const APPS_CATALOG_TIMEOUT_MS = 12_000;

export async function getIntegrations(
  signal?: AbortSignal,
): Promise<Integration[]> {
  return fetchAPI<Integration[]>("/api/v1/apps", {
    timeoutMs: APPS_CATALOG_TIMEOUT_MS,
    signal,
  });
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

export async function getAppAdminMetrics(
  app: string,
): Promise<AppAdminMetricsResponse> {
  const response = await fetchAPI<AppAdminMetricsResponse>(
    `/api/v1/apps/${encodeURIComponent(app)}/admin/metrics`,
  );
  return {
    ...response,
    operations: response.operations ?? [],
  };
}

export async function getAdminPlatformAdmins(): Promise<AdminPlatformAdminsResponse> {
  const response = await fetchAPI<
    AdminPlatformAdminsResponse & { members?: AppAuthorizationMember[] }
  >("/admin/api/v1/platform-admins");
  return {
    resource: response.resource,
    role: response.role,
    members: response.members ?? [],
  };
}

export async function listAdminRegistryApps(): Promise<AdminRegistryAppSummary[]> {
  return fetchAPI<AdminRegistryAppSummary[]>("/admin/api/v1/registry-apps");
}

export async function getAdminRegistryApp(
  app: string,
): Promise<AdminRegistryAppDetail> {
  const detail = await fetchAPI<AdminRegistryAppDetail>(
    `/admin/api/v1/registry-apps/${encodeURIComponent(app)}`,
  );
  return {
    ...detail,
    knownVersions: detail.knownVersions ?? [],
    freshReplicas: detail.freshReplicas ?? [],
    staleReplicas: detail.staleReplicas ?? [],
  };
}

export async function getAdminMetricsText(): Promise<string> {
  const res = await fetch(resolveAPIPath("/admin/api/v1/metrics"), {
    credentials: "include",
    cache: "no-store",
    headers: { Accept: "text/plain" },
  });
  if (res.status === HTTP_UNAUTHORIZED) {
    redirectToLogin();
    throw new APIError(HTTP_UNAUTHORIZED, "Session expired");
  }
  if (!res.ok) {
    const body = await res.text();
    let message =
      res.status === 503
        ? "Prometheus metrics are unavailable."
        : "Couldn't load metrics.";
    try {
      const parsed = JSON.parse(body) as { error?: unknown };
      if (typeof parsed.error === "string" && parsed.error.trim()) {
        message = parsed.error;
      }
    } catch {
      /* keep fallback */
    }
    throw new APIError(res.status, message);
  }
  const body = await res.text();
  const contentType = res.headers.get("content-type") || "";
  if (!isAdminMetricsScrapeText(contentType, body)) {
    throw new APIError(503, "Metrics are unavailable on this server.");
  }
  return body;
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
