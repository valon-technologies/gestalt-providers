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

interface WorkflowRunListResponse {
  runs: WorkflowRunWire[];
  nextPageToken?: string;
}

interface WorkflowDefinitionListResponse {
  definitions?: unknown[];
}

export type ListWorkflowRunsOptions = {
  /** Workflow platform provider (`provider` in the Workflow API). */
  provider?: string;
  /** Server-side step-target filter (`target_app` in the Workflow API). */
  targetApp?: string;
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

function listWorkflowRunsQuery(
  provider: string,
  opts?: ListWorkflowRunsOptions,
): string {
  const query = new URLSearchParams();
  appendProviderQuery(query, provider);
  appendTargetAppQuery(query, resolveTargetApp(opts));
  const status = opts?.status?.trim();
  if (status) {
    query.set("status", status);
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

export type WorkflowRunPage = {
  runs: WorkflowRun[];
  nextPageToken?: string;
};

export async function listWorkflowRuns(
  opts?: ListWorkflowRunsOptions,
): Promise<WorkflowRunPage> {
  const provider = await resolveWorkflowProvider(opts?.provider);
  const params = listWorkflowRunsQuery(provider, opts);
  const response = await fetchAPI<WorkflowRunListResponse>(
    `${WORKFLOW_RUNS_PATH}?${params}`,
  );
  const runs = normalizeRuns(response.runs);
  // `targetApp` is a server-side ownership filter. List summaries may omit the
  // hydrated target, so re-filtering here can silently discard valid results.
  const nextPageToken = response.nextPageToken?.trim() || undefined;
  return { runs, nextPageToken };
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
