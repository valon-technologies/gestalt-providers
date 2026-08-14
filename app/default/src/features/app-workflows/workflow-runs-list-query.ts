import {
  normalizeWorkflowStatus,
  type WorkflowRun,
  type WorkflowStatus,
} from "@/lib/api";
import { runSearchTerms } from "@/features/app-workflows/workflow-format";
import {
  parseWorkflowRunsGroupBy,
  serializeWorkflowRunsGroupBy,
  type WorkflowRunsGroupBy,
} from "@/features/app-workflows/workflow-runs-group";

/** Statuses the Workflows list can filter on (excludes unknown/skipped noise). */
export const WORKFLOW_RUNS_LIST_STATUSES = [
  "pending",
  "running",
  "succeeded",
  "failed",
  "canceled",
] as const satisfies readonly WorkflowStatus[];

export type WorkflowRunsListStatus =
  (typeof WORKFLOW_RUNS_LIST_STATUSES)[number];

/**
 * Canonical Workflows list filter + layout model. URL search is the source of
 * truth; the panel only presents and navigates.
 */
export type WorkflowRunsListQuery = {
  q: string;
  /** Empty means all statuses. Multiple values are OR within the status axis. */
  statuses: WorkflowRunsListStatus[];
  definitionId?: string;
  /** List layout — not a filter. Default flat (`none`). */
  groupBy: WorkflowRunsGroupBy;
};

export type WorkflowRunsSearchParams = {
  q?: string;
  status?: string;
  definition?: string;
  /** Present only when grouping: `group=definition`. */
  group?: string;
};

const LIST_STATUS_SET = new Set<string>(WORKFLOW_RUNS_LIST_STATUSES);

export function isWorkflowRunsListStatus(
  value: string,
): value is WorkflowRunsListStatus {
  return LIST_STATUS_SET.has(value);
}

export function emptyWorkflowRunsListQuery(): WorkflowRunsListQuery {
  return { q: "", statuses: [], groupBy: "none" };
}

export function parseWorkflowRunsSearch(
  search: Record<string, unknown>,
): WorkflowRunsSearchParams {
  const q = typeof search.q === "string" ? search.q.trim() : "";
  const status =
    typeof search.status === "string" ? search.status.trim() : "";
  const definition =
    typeof search.definition === "string" ? search.definition.trim() : "";
  const group = typeof search.group === "string" ? search.group.trim() : "";
  return {
    ...(q ? { q } : {}),
    ...(status ? { status } : {}),
    ...(definition ? { definition } : {}),
    ...(group ? { group } : {}),
  };
}

export function workflowRunsListQueryFromSearch(
  search: WorkflowRunsSearchParams,
): WorkflowRunsListQuery {
  const statuses = parseStatusParam(search.status);
  const definitionId = search.definition?.trim() || undefined;
  return {
    q: search.q?.trim() || "",
    statuses,
    groupBy: parseWorkflowRunsGroupBy(search.group),
    ...(definitionId ? { definitionId } : {}),
  };
}

export function workflowRunsSearchFromQuery(
  query: WorkflowRunsListQuery,
): WorkflowRunsSearchParams {
  const q = query.q.trim();
  const status = serializeStatusParam(query.statuses);
  const definition = query.definitionId?.trim() || "";
  const group = serializeWorkflowRunsGroupBy(query.groupBy);
  return {
    ...(q ? { q } : {}),
    ...(status ? { status } : {}),
    ...(definition ? { definition } : {}),
    ...(group ? { group } : {}),
  };
}

export function workflowRunsListQueryIsActive(
  query: WorkflowRunsListQuery,
): boolean {
  return (
    query.q.trim().length > 0 ||
    query.statuses.length > 0 ||
    Boolean(query.definitionId?.trim())
  );
}

/**
 * List API accepts a single status. When exactly one chip is selected, push it
 * server-side so pagination stays honest. Multi-status stays loaded-only until
 * the API supports OR.
 */
export function serverListStatus(
  query: WorkflowRunsListQuery,
): WorkflowRunsListStatus | undefined {
  if (query.statuses.length !== 1) return undefined;
  return query.statuses[0];
}

export type WorkflowRunsFilterScope = "none" | "server" | "loaded-only";

/** How status filtering is enforced for the current query. */
export function workflowRunsStatusFilterScope(
  query: WorkflowRunsListQuery,
): WorkflowRunsFilterScope {
  if (query.statuses.length === 0) return "none";
  if (query.statuses.length === 1) return "server";
  return "loaded-only";
}

/**
 * True when the UI applies filters the ListRuns aggregates cannot see
 * (search, multi-status). Definition is server-backed via `definitionId`.
 * Layout `groupBy` does not qualify — it changes how lists are fetched/shown.
 */
export function workflowRunsListQueryUsesClientOnlyFilters(
  query: WorkflowRunsListQuery,
): boolean {
  if (query.q.trim()) return true;
  return workflowRunsStatusFilterScope(query) === "loaded-only";
}

/**
 * Header / Load-more cardinality from ListRuns `total_count`.
 * Omit it when the visible rows are a client-only subset, or the header
 * would count the unfiltered corpus.
 */
export function workflowVisibleRunTotalCount(
  query: WorkflowRunsListQuery,
  serverTotalCount?: number | null,
): number | undefined {
  if (workflowRunsListQueryUsesClientOnlyFilters(query)) return undefined;
  return serverTotalCount ?? undefined;
}

/**
 * Whether the list should offer Load more.
 *
 * Visibility list pages can return a leftover nextPageToken with an empty
 * (or already-complete) result. Trust the token only when the server total
 * says we have not loaded every run, or — if the total is unknown — when
 * this page actually returned runs.
 */
export function workflowListHasMorePages(opts: {
  hasNextPage: boolean;
  loadedCount: number;
  totalCount?: number | null;
}): boolean {
  if (!opts.hasNextPage) return false;
  if (opts.totalCount != null) return opts.loadedCount < opts.totalCount;
  return opts.loadedCount > 0;
}

/** Group header count — server total when known, never a page size posing as complete. */
export function workflowDefinitionRunCountLabel(opts: {
  loading: boolean;
  loadedCount: number;
  totalCount?: number | null;
  hasMore: boolean;
}): string {
  if (opts.loading) return "…";
  const noun = (count: number) => (count === 1 ? "run" : "runs");
  if (opts.totalCount != null) {
    return `${opts.totalCount} ${noun(opts.totalCount)}`;
  }
  if (opts.hasMore) {
    if (opts.loadedCount <= 0) return "…";
    return `${opts.loadedCount}+ ${noun(opts.loadedCount)}`;
  }
  return `${opts.loadedCount} ${noun(opts.loadedCount)}`;
}

/**
 * Client-side filters after ListRuns: search and multi-status.
 * Definition is a display invariant for grouped sections (a group never shows
 * another definition's rows). Paging and `total_count` stay on the ListRuns
 * request; this filter does not fetch extra pages to count.
 */
export function applyWorkflowRunsListQuery(
  runs: WorkflowRun[],
  query: WorkflowRunsListQuery,
): WorkflowRun[] {
  const needle = query.q.trim().toLowerCase();
  const statusFilter = query.statuses;
  const definitionFilter = query.definitionId?.trim();

  return runs.filter((run) => {
    if (
      definitionFilter &&
      (run.definitionId?.trim() || "") !== definitionFilter
    ) {
      return false;
    }
    if (statusFilter.length > 0) {
      const status = normalizeWorkflowStatus(run.status);
      if (!statusFilter.includes(status as WorkflowRunsListStatus)) {
        return false;
      }
    }
    if (!needle) return true;
    return runSearchTerms(run).some((term) => term.includes(needle));
  });
}

function parseStatusParam(raw?: string): WorkflowRunsListStatus[] {
  if (!raw?.trim()) return [];
  const seen = new Set<WorkflowRunsListStatus>();
  for (const part of raw.split(",")) {
    const normalized = normalizeWorkflowStatus(part.trim());
    if (isWorkflowRunsListStatus(normalized) && !seen.has(normalized)) {
      seen.add(normalized);
    }
  }
  return WORKFLOW_RUNS_LIST_STATUSES.filter((status) => seen.has(status));
}

function serializeStatusParam(statuses: readonly WorkflowRunsListStatus[]): string {
  const ordered = WORKFLOW_RUNS_LIST_STATUSES.filter((status) =>
    statuses.includes(status),
  );
  return ordered.join(",");
}
