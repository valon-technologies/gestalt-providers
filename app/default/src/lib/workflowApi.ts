import { fetchAPI, normalizeWorkflowRun, type WorkflowRun } from "@/lib/api";
import {
  rememberWorkflowProvider,
  resolveWorkflowProvider,
} from "@/lib/workflowProvider";

/** Canonical workflow platform REST surface (gestalt.provider.v1.Workflow). */
const WORKFLOW_RUNS_PATH = "/api/v2/workflow/runs";

type WorkflowRunWire = Parameters<typeof normalizeWorkflowRun>[0];

interface WorkflowRunListResponse {
  runs: WorkflowRunWire[];
  nextPageToken?: string;
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
  run?: Pick<WorkflowRun, "provider">;
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

function listWorkflowRunsQuery(
  provider: string,
  opts?: ListWorkflowRunsOptions,
): string {
  const query = new URLSearchParams();
  appendProviderQuery(query, provider);
  const targetApp = opts?.targetApp?.trim();
  if (targetApp) {
    query.set("targetApp", targetApp);
  }
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

function runDetailQuery(provider: string): string {
  const query = new URLSearchParams();
  appendProviderQuery(query, provider);
  return query.toString();
}

function normalizeRuns(runs: WorkflowRunWire[]): WorkflowRun[] {
  return runs.map((run) => {
    const normalized = normalizeWorkflowRun(run);
    rememberWorkflowProvider(normalized.provider);
    return normalized;
  });
}

export async function listWorkflowRuns(
  opts?: ListWorkflowRunsOptions,
): Promise<WorkflowRun[]> {
  const provider = await resolveWorkflowProvider(opts?.provider);
  const params = listWorkflowRunsQuery(provider, opts);
  const response = await fetchAPI<WorkflowRunListResponse>(
    `${WORKFLOW_RUNS_PATH}?${params}`,
  );
  return normalizeRuns(response.runs);
}

export async function getWorkflowRun(
  runId: string,
  opts?: WorkflowRunRequestOptions,
): Promise<WorkflowRun> {
  const provider = await resolveProvider(opts);
  const params = runDetailQuery(provider);
  const run = await fetchAPI<WorkflowRunWire>(
    `${workflowRunPath(runId)}?${params}`,
  );
  const normalized = normalizeWorkflowRun(run);
  rememberWorkflowProvider(normalized.provider);
  return normalized;
}

export async function cancelWorkflowRun(
  runId: string,
  reason?: string,
  opts?: WorkflowRunRequestOptions,
): Promise<WorkflowRun> {
  const provider = await resolveProvider(opts);
  const params = runDetailQuery(provider);
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
