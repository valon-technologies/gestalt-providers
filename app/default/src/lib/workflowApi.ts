import {
  fetchAPI,
  normalizeWorkflowDefinition,
  normalizeWorkflowRun,
  type WorkflowDefinition,
  type WorkflowRun,
} from "@/lib/api";
import {
  rememberWorkflowProvider,
  resolveWorkflowProvider,
} from "@/lib/workflowProvider";
import { workflowDefinitionMatchesApp } from "@/lib/workflowActivity";

/** Canonical workflow platform REST surface (gestalt.provider.v1.Workflow). */
const WORKFLOW_RUNS_PATH = "/api/v2/workflow/runs";
const WORKFLOW_DEFINITIONS_PATH = "/api/v2/workflow/definitions";

type WorkflowRunWire = Parameters<typeof normalizeWorkflowRun>[0];

/** protojson encodes int64 as a JSON string; numbers still appear in mocks. */
type CountWire = number | string;

interface WorkflowRunListResponse {
  runs: WorkflowRunWire[];
  nextPageToken?: string;
  next_page_token?: string;
  totalCount?: CountWire;
  total_count?: CountWire;
  statusCounts?: {
    pending?: CountWire;
    running?: CountWire;
    succeeded?: CountWire;
    failed?: CountWire;
    canceled?: CountWire;
  };
  status_counts?: {
    pending?: CountWire;
    running?: CountWire;
    succeeded?: CountWire;
    failed?: CountWire;
    canceled?: CountWire;
  };
}

interface WorkflowDefinitionListResponse {
  definitions?: unknown[];
}

export type ListWorkflowRunsOptions = {
  /** Workflow platform provider (`provider` in the Workflow API). */
  provider?: string;
  /** Server-side step-target filter (`target_app` in the Workflow API). */
  targetApp?: string;
  /** Server-side definition filter (`definition_id` / `definitionId`). */
  definitionId?: string;
  status?: string;
  pageSize?: number;
  pageToken?: string;
};

export type WorkflowRunRequestOptions = {
  provider?: string;
  /** When set, uses `run.provider` before the deployment default. */
  run?: Pick<WorkflowRun, "provider" | "targetApp">;
  /** Server-side step-target filter (`target_app` in the Workflow API). */
  targetApp?: string;
};

function workflowRunPath(runId: string): string {
  return `${WORKFLOW_RUNS_PATH}/${encodeURIComponent(runId)}`;
}

async function resolveProvider(
  opts?: WorkflowRunRequestOptions,
): Promise<string> {
  const fromRun = opts?.run?.provider?.trim();
  if (fromRun) {
    rememberWorkflowProvider(fromRun);
    return fromRun;
  }
  return resolveWorkflowProvider(opts?.provider);
}

function appendProviderQuery(
  query: URLSearchParams,
  provider: string,
): void {
  query.set("provider", provider);
}

function resolveTargetApp(opts?: ListWorkflowRunsOptions | WorkflowRunRequestOptions): string | undefined {
  const explicit = "targetApp" in (opts ?? {}) ? opts?.targetApp?.trim() : undefined;
  if (explicit) return explicit;
  const fromRun =
    opts && "run" in opts ? opts.run?.targetApp?.trim() : undefined;
  return fromRun || undefined;
}

function appendTargetAppQuery(
  query: URLSearchParams,
  targetApp: string | undefined,
): void {
  if (targetApp) {
    query.set("targetApp", targetApp);
  }
}

/**
 * ListRuns HTTP status query expects protobuf enum names
 * (`WORKFLOW_RUN_STATUS_FAILED`), not product tokens (`failed`).
 */
const WORKFLOW_RUN_STATUS_QUERY: Record<string, string> = {
  pending: "WORKFLOW_RUN_STATUS_PENDING",
  running: "WORKFLOW_RUN_STATUS_RUNNING",
  succeeded: "WORKFLOW_RUN_STATUS_SUCCEEDED",
  failed: "WORKFLOW_RUN_STATUS_FAILED",
  canceled: "WORKFLOW_RUN_STATUS_CANCELED",
};

export function toWorkflowRunStatusQueryParam(status: string): string {
  const trimmed = status.trim();
  if (!trimmed) return "";
  if (trimmed.startsWith("WORKFLOW_RUN_STATUS_")) return trimmed;
  const key = trimmed.toLowerCase();
  return WORKFLOW_RUN_STATUS_QUERY[key] ?? trimmed;
}

/** Builds ListRuns query string (exported for contract tests). */
export function listWorkflowRunsQuery(
  provider: string,
  opts?: ListWorkflowRunsOptions,
): string {
  const query = new URLSearchParams();
  appendProviderQuery(query, provider);
  appendTargetAppQuery(query, resolveTargetApp(opts));
  const status = toWorkflowRunStatusQueryParam(opts?.status ?? "");
  if (status) {
    query.set("status", status);
  }
  const definitionId = opts?.definitionId?.trim();
  if (definitionId) {
    query.set("definitionId", definitionId);
  }
  if (opts?.pageSize !== undefined) {
    query.set("pageSize", String(opts.pageSize));
  }
  const pageToken = opts?.pageToken?.trim();
  if (pageToken) {
    query.set("pageToken", pageToken);
  }
  return query.toString();
}

function runDetailQuery(
  provider: string,
  targetApp?: string,
): string {
  const query = new URLSearchParams();
  appendProviderQuery(query, provider);
  appendTargetAppQuery(query, targetApp);
  return query.toString();
}

function normalizeRuns(runs: WorkflowRunWire[]): WorkflowRun[] {
  return runs.map((run) => {
    const normalized = normalizeWorkflowRun(run);
    rememberWorkflowProvider(normalized.provider);
    return normalized;
  });
}

export type WorkflowRunStatusCounts = {
  pending: number;
  running: number;
  succeeded: number;
  failed: number;
  canceled: number;
};

export type WorkflowRunPage = {
  runs: WorkflowRun[];
  nextPageToken?: string;
  /** Visibility total for this list filter. Absent when the API omits it. */
  totalCount?: number;
  /** Status histogram for provider/target_app (status filter cleared). */
  statusCounts?: WorkflowRunStatusCounts;
};

export type WorkflowRunListAggregates = {
  totalCount?: number;
  statusCounts?: WorkflowRunStatusCounts;
};

/**
 * protojson int64 → JSON string; local mocks may send numbers. Matches
 * definition `generation` coercion in api.ts.
 */
export function parseOptionalCount(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return undefined;
}

export function normalizeStatusCounts(
  raw: WorkflowRunListResponse["statusCounts"],
): WorkflowRunStatusCounts | undefined {
  if (!raw || typeof raw !== "object") return undefined;
  return {
    pending: parseOptionalCount(raw.pending) ?? 0,
    running: parseOptionalCount(raw.running) ?? 0,
    succeeded: parseOptionalCount(raw.succeeded) ?? 0,
    failed: parseOptionalCount(raw.failed) ?? 0,
    canceled: parseOptionalCount(raw.canceled) ?? 0,
  };
}

/** First page that carries aggregates (gestaltd echoes them on later pages). */
export function pickWorkflowRunListAggregates(
  pages: readonly WorkflowRunListAggregates[],
): WorkflowRunListAggregates {
  for (const page of pages) {
    if (page.totalCount != null || page.statusCounts != null) {
      return {
        totalCount: page.totalCount,
        statusCounts: page.statusCounts,
      };
    }
  }
  return {};
}

export function normalizeWorkflowRunListResponse(
  response: WorkflowRunListResponse,
): Omit<WorkflowRunPage, "runs"> & { runs: WorkflowRunWire[] } {
  const nextPageToken =
    (response.nextPageToken ?? response.next_page_token)?.trim() || undefined;
  return {
    runs: response.runs,
    nextPageToken,
    totalCount: parseOptionalCount(response.totalCount ?? response.total_count),
    statusCounts: normalizeStatusCounts(
      response.statusCounts ?? response.status_counts,
    ),
  };
}

export async function listWorkflowRuns(
  opts?: ListWorkflowRunsOptions,
): Promise<WorkflowRunPage> {
  const provider = await resolveWorkflowProvider(opts?.provider);
  const params = listWorkflowRunsQuery(provider, opts);
  const response = await fetchAPI<WorkflowRunListResponse>(
    `${WORKFLOW_RUNS_PATH}?${params}`,
  );
  const normalized = normalizeWorkflowRunListResponse(response);
  // `targetApp` is a server-side ownership filter. List summaries may omit the
  // hydrated target, so re-filtering here can silently discard valid results.
  return {
    runs: normalizeRuns(normalized.runs),
    nextPageToken: normalized.nextPageToken,
    totalCount: normalized.totalCount,
    statusCounts: normalized.statusCounts,
  };
}

export async function getWorkflowRun(
  runId: string,
  opts?: WorkflowRunRequestOptions,
): Promise<WorkflowRun> {
  const provider = await resolveProvider(opts);
  const params = runDetailQuery(provider, resolveTargetApp(opts));
  const run = await fetchAPI<WorkflowRunWire>(
    `${workflowRunPath(runId)}?${params}`,
  );
  const normalized = normalizeWorkflowRun(run);
  rememberWorkflowProvider(normalized.provider);
  return normalized;
}

export type WorkflowStepLogsOptions = WorkflowRunRequestOptions & {
  jobId: string;
  stepId: string;
};

export type WorkflowStepLogsResponse = {
  runId: string;
  jobId: string;
  stepId: string;
  groups: Array<{
    id: string;
    name: string;
    status?: string;
    durationMs?: number | null;
    defaultCollapsed?: boolean;
    lines: Array<{
      number?: number;
      text: string;
      level?: "info" | "warning" | "error" | "debug";
    }>;
  }>;
};

export async function getWorkflowStepLogs(
  runId: string,
  opts: WorkflowStepLogsOptions,
): Promise<WorkflowStepLogsResponse> {
  const provider = await resolveProvider(opts);
  const params = runDetailQuery(provider, resolveTargetApp(opts));
  const path = `${workflowRunPath(runId)}/jobs/${encodeURIComponent(opts.jobId)}/steps/${encodeURIComponent(opts.stepId)}/logs`;
  return fetchAPI<WorkflowStepLogsResponse>(`${path}?${params}`);
}

export async function cancelWorkflowRun(
  runId: string,
  reason?: string,
  opts?: WorkflowRunRequestOptions,
): Promise<WorkflowRun> {
  const provider = await resolveProvider(opts);
  const params = runDetailQuery(provider, resolveTargetApp(opts));
  const run = await fetchAPI<WorkflowRunWire>(
    `${workflowRunPath(runId)}:cancel?${params}`,
    {
      method: "POST",
      body: JSON.stringify(reason ? { reason } : {}),
    },
  );
  const normalized = normalizeWorkflowRun(run);
  rememberWorkflowProvider(normalized.provider);
  return normalized;
}

export type ListWorkflowDefinitionsOptions = {
  provider?: string;
  /** Client-side ownership filter (definitions API is provider-scoped). */
  targetApp?: string;
};

export type WorkflowDefinitionRequestOptions = {
  provider?: string;
  targetApp?: string;
};

function workflowDefinitionPath(definitionId: string): string {
  return `${WORKFLOW_DEFINITIONS_PATH}/${encodeURIComponent(definitionId)}`;
}

function listDefinitionsQuery(provider: string): string {
  const query = new URLSearchParams();
  appendProviderQuery(query, provider);
  return query.toString();
}

function definitionDetailQuery(provider: string): string {
  return listDefinitionsQuery(provider);
}

export async function listWorkflowDefinitions(
  opts?: ListWorkflowDefinitionsOptions,
): Promise<WorkflowDefinition[]> {
  const provider = await resolveWorkflowProvider(opts?.provider);
  const params = listDefinitionsQuery(provider);
  const response = await fetchAPI<WorkflowDefinitionListResponse>(
    `${WORKFLOW_DEFINITIONS_PATH}?${params}`,
  );
  const definitions = (response.definitions ?? []).map((item) => {
    const normalized = normalizeWorkflowDefinition(
      item as Record<string, unknown>,
    );
    rememberWorkflowProvider(normalized.provider);
    return normalized;
  });
  const targetApp = opts?.targetApp?.trim();
  if (!targetApp) return definitions;
  return definitions.filter((definition) =>
    workflowDefinitionMatchesApp(definition, targetApp),
  );
}

export async function getWorkflowDefinition(
  definitionId: string,
  opts?: WorkflowDefinitionRequestOptions,
): Promise<WorkflowDefinition> {
  const provider = await resolveWorkflowProvider(opts?.provider);
  const params = definitionDetailQuery(provider);
  const response = await fetchAPI<Record<string, unknown>>(
    `${workflowDefinitionPath(definitionId)}?${params}`,
  );
  const normalized = normalizeWorkflowDefinition(response);
  rememberWorkflowProvider(normalized.provider);
  const targetApp = opts?.targetApp?.trim();
  if (
    targetApp &&
    !workflowDefinitionMatchesApp(normalized, targetApp)
  ) {
    throw new Error("This workflow definition is not available in this app.");
  }
  return normalized;
}

function assertDefinitionForApp(
  definition: WorkflowDefinition,
  targetApp?: string,
): WorkflowDefinition {
  const app = targetApp?.trim();
  if (app && !workflowDefinitionMatchesApp(definition, app)) {
    throw new Error("This workflow definition is not available in this app.");
  }
  return definition;
}

export type SetWorkflowDefinitionPausedOptions =
  WorkflowDefinitionRequestOptions & {
    paused: boolean;
  };

export async function setWorkflowDefinitionPaused(
  definitionId: string,
  opts: SetWorkflowDefinitionPausedOptions,
): Promise<WorkflowDefinition> {
  const provider = await resolveWorkflowProvider(opts.provider);
  const params = definitionDetailQuery(provider);
  const response = await fetchAPI<Record<string, unknown>>(
    `${workflowDefinitionPath(definitionId)}:setPaused?${params}`,
    {
      method: "POST",
      body: JSON.stringify({ paused: opts.paused }),
    },
  );
  const normalized = normalizeWorkflowDefinition(response);
  rememberWorkflowProvider(normalized.provider);
  return assertDefinitionForApp(normalized, opts.targetApp);
}

export type SetWorkflowActivationPausedOptions =
  WorkflowDefinitionRequestOptions & {
    activationId: string;
    paused: boolean;
  };

export async function setWorkflowActivationPaused(
  definitionId: string,
  opts: SetWorkflowActivationPausedOptions,
): Promise<WorkflowDefinition> {
  const provider = await resolveWorkflowProvider(opts.provider);
  const params = definitionDetailQuery(provider);
  const activationPath = `${workflowDefinitionPath(definitionId)}/activations/${encodeURIComponent(opts.activationId)}:setPaused`;
  const response = await fetchAPI<Record<string, unknown>>(
    `${activationPath}?${params}`,
    {
      method: "POST",
      body: JSON.stringify({ paused: opts.paused }),
    },
  );
  const normalized = normalizeWorkflowDefinition(response);
  rememberWorkflowProvider(normalized.provider);
  return assertDefinitionForApp(normalized, opts.targetApp);
}

export async function deleteWorkflowDefinition(
  definitionId: string,
  opts?: WorkflowDefinitionRequestOptions,
): Promise<void> {
  const provider = await resolveWorkflowProvider(opts?.provider);
  const params = definitionDetailQuery(provider);
  await fetchAPI(`${workflowDefinitionPath(definitionId)}?${params}`, {
    method: "DELETE",
  });
}

export type StartWorkflowRunOptions = WorkflowDefinitionRequestOptions & {
  input?: Record<string, unknown>;
  idempotencyKey?: string;
  workflowKey?: string;
  expectedDefinitionGeneration?: number;
};

export async function startWorkflowRun(
  definitionId: string,
  opts?: StartWorkflowRunOptions,
): Promise<WorkflowRun> {
  const provider = await resolveWorkflowProvider(opts?.provider);
  const params = definitionDetailQuery(provider);
  const body: Record<string, unknown> = { provider };
  if (opts?.input) body.input = opts.input;
  if (opts?.idempotencyKey) body.idempotencyKey = opts.idempotencyKey;
  if (opts?.workflowKey) body.workflowKey = opts.workflowKey;
  if (opts?.expectedDefinitionGeneration !== undefined) {
    body.expectedDefinitionGeneration = opts.expectedDefinitionGeneration;
  }
  const run = await fetchAPI<WorkflowRunWire>(
    `${workflowDefinitionPath(definitionId)}/runs?${params}`,
    {
      method: "POST",
      body: JSON.stringify(body),
    },
  );
  const normalized = normalizeWorkflowRun(run);
  rememberWorkflowProvider(normalized.provider);
  return normalized;
}

export type WorkflowRunEvent = {
  id?: string;
  runId?: string;
  stepId?: string;
  type?: string;
  data?: Record<string, unknown>;
  createdAt?: string;
};

export async function getWorkflowRunEvents(
  runId: string,
  opts?: WorkflowRunRequestOptions,
): Promise<WorkflowRunEvent[]> {
  const provider = await resolveProvider(opts);
  const params = runDetailQuery(provider, resolveTargetApp(opts));
  const response = await fetchAPI<
    WorkflowRunEvent[] | { events?: WorkflowRunEvent[] }
  >(`${workflowRunPath(runId)}/events?${params}`);
  if (Array.isArray(response)) return response;
  return response.events ?? [];
}

export async function getWorkflowRunOutput(
  runId: string,
  opts?: WorkflowRunRequestOptions,
): Promise<unknown> {
  const provider = await resolveProvider(opts);
  const params = runDetailQuery(provider, resolveTargetApp(opts));
  const response = await fetchAPI<unknown | { output?: unknown }>(
    `${workflowRunPath(runId)}/output?${params}`,
  );
  if (
    response &&
    typeof response === "object" &&
    !Array.isArray(response) &&
    "output" in response
  ) {
    return (response as { output?: unknown }).output;
  }
  return response;
}
