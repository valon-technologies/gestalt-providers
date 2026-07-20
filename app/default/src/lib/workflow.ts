import { fetchAPI } from "./api";

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
  tools?: unknown[];
  output?: unknown;
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

function normalizeWorkflowTextTarget(
  value: unknown,
): WorkflowTextTarget | undefined {
  if (!isRecord(value)) return undefined;
  return {
    template: optionalString(value.template),
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
    tools: Array.isArray(value.tools) ? value.tools : undefined,
    output: value.output,
    modelOptions: optionalRecord(value.modelOptions),
  };
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

function normalizeWorkflowRun(run: WorkflowRunWire): WorkflowRun {
  return {
    ...run,
    target: normalizeWorkflowTarget(run.target),
    steps: normalizeWorkflowStepExecutions(run.steps),
  };
}

export function workflowTargetApp(target: WorkflowTarget): WorkflowAppTarget {
  return (
    target.steps.find((step) => step.app)?.app ?? {
      name: "",
      operation: "",
    }
  );
}

/** Apps referenced by steps on a workflow target. */
export function workflowTargetAppNames(target: WorkflowTarget): string[] {
  const names = new Set<string>();
  for (const step of target.steps) {
    const name = step.app?.name?.trim();
    if (name) names.add(name);
  }
  return [...names];
}

/**
 * Best-effort ownership signal when list responses omit hydrated targets.
 * Definition IDs are conventionally `app_<appName>_…`.
 */
export function workflowRunDefinitionApp(run: WorkflowRun): string | null {
  const definitionId = run.definitionId?.trim();
  if (!definitionId?.startsWith("app_")) return null;
  const rest = definitionId.slice("app_".length);
  if (!rest) return null;
  const underscore = rest.indexOf("_");
  return underscore === -1 ? rest : rest.slice(0, underscore);
}

export function workflowRunMatchesApp(run: WorkflowRun, appName: string): boolean {
  const needle = appName.trim();
  if (!needle) return false;
  if (workflowTargetAppNames(run.target).includes(needle)) return true;
  return workflowRunDefinitionApp(run) === needle;
}

export async function getWorkflowRuns(opts?: {
  /** Step-target app filter (`?app=` → TargetApp). */
  app?: string;
  /** @deprecated Prefer `app` — kept as alias for older call sites. */
  targetApp?: string;
}): Promise<WorkflowRun[]> {
  const appName = (opts?.app ?? opts?.targetApp)?.trim();
  const query = new URLSearchParams();
  if (appName) {
    // gestaltd: GET /api/v1/workflow/runs?app=<name> → TargetApp
    query.set("app", appName);
  }
  const params = query.toString();
  const response = await fetchAPI<WorkflowRunListResponse>(
    `/api/v1/workflow/runs${params ? `?${params}` : ""}`,
  );
  const runs = response.runs.map(normalizeWorkflowRun);
  if (!appName) return runs;
  // List responses often omit hydrated targets; keep admin scoped via
  // definitionId / step-app when the server page is incomplete.
  return runs.filter((run) => workflowRunMatchesApp(run, appName));
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
