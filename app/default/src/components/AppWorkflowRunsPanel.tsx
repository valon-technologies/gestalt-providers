import { useCallback, useMemo } from "react";
import { ChevronDownIcon, Lightbulb } from "lucide-react";
import type { WorkflowRun } from "@/lib/api";
import {
  Link as RouterLink,
  useNavigate,
  useSearch,
} from "@tanstack/react-router";
import {
  useWorkflowDefinitionsQuery,
  useWorkflowRunsQuery,
} from "@/lib/queries";
import { Link } from "@/components/ui/link";
import { CopyableCode } from "@/components/ui/copyable-code";
import { cardVariants } from "@/components/ui/card";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import {
  PageHeader,
  PageHeaderActions,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import ErrorNotice from "@/components/ErrorNotice";
import { userFacingError } from "@/lib/user-facing-error";
import { WorkflowProviderConfigurationError } from "@/lib/workflowProvider";
import { WorkflowRefreshedAt } from "@/features/app-workflows/workflow-refreshed-at";
import {
  WorkflowGhaRunsList,
  WorkflowGroupedDefinitionRunsList,
} from "@/features/app-workflows/workflow-gha-runs-list";
import {
  WorkflowRunsFilters,
  useWorkflowDefinitionFilterOptions,
} from "@/features/app-workflows/workflow-runs-filters";
import { mergeWorkflowDefinitionIds } from "@/features/app-workflows/workflow-runs-group";
import {
  applyWorkflowRunsListQuery,
  emptyWorkflowRunsListQuery,
  serverListStatus,
  workflowListHasMorePages,
  workflowRunsListQueryFromSearch,
  workflowRunsListQueryIsActive,
  workflowRunsSearchFromQuery,
  type WorkflowRunsListQuery,
} from "@/features/app-workflows/workflow-runs-list-query";
import { useQueryClient } from "@tanstack/react-query";
import { queryKeys } from "@/lib/query-keys";
import { pickWorkflowRunListAggregates } from "@/lib/workflowApi";

export default function AppWorkflowRunsPanel({ appName }: { appName: string }) {
  const navigate = useNavigate({ from: "/apps/$app/admin/workflows" });
  const search = useSearch({ from: "/apps/$app/admin/workflows" });
  const queryClient = useQueryClient();
  const listQuery = useMemo(
    () => workflowRunsListQueryFromSearch(search),
    [search],
  );
  const serverStatus = serverListStatus(listQuery);
  const definitionFilter = listQuery.definitionId?.trim() || undefined;
  const groupedByDefinition = listQuery.groupBy === "definition";

  const replaceListQuery = useCallback(
    (next: WorkflowRunsListQuery) => {
      void navigate({
        to: "/apps/$app/admin/workflows",
        params: { app: appName },
        search: workflowRunsSearchFromQuery(next),
        replace: true,
      });
    },
    [appName, navigate],
  );

  const clearListQuery = useCallback(() => {
    // Preserve layout (`groupBy`); clear is for filters only.
    replaceListQuery({
      ...emptyWorkflowRunsListQuery(),
      groupBy: listQuery.groupBy,
    });
  }, [listQuery.groupBy, replaceListQuery]);

  // Flat list source. Grouped sections own their ListRuns; this query only
  // seeds activity-only definition ids and must not block those sections.
  const runsQuery = useWorkflowRunsQuery(appName, {
    status: serverStatus,
    definitionId: definitionFilter,
  });
  const definitionsQuery = useWorkflowDefinitionsQuery(appName);
  const runs = useMemo(
    () => runsQuery.data?.pages.flatMap((page) => page.runs) ?? [],
    [runsQuery.data],
  );
  const refreshing =
    (runsQuery.isFetching && !runsQuery.isPending) ||
    (definitionsQuery.isFetching && !definitionsQuery.isPending);
  const loadingMore = runsQuery.isFetchingNextPage;
  const runAggregates = useMemo(
    () => pickWorkflowRunListAggregates(runsQuery.data?.pages ?? []),
    [runsQuery.data?.pages],
  );
  const hasMoreRuns = workflowListHasMorePages({
    hasNextPage: Boolean(runsQuery.hasNextPage),
    loadedCount: runs.length,
    totalCount: runAggregates.totalCount,
  });
  const runsError = runsQuery.error
    ? userFacingError(
        runsQuery.error,
        "Couldn't load workflow runs. Try again.",
      )
    : null;
  const definitionsError = definitionsQuery.error
    ? userFacingError(
        definitionsQuery.error,
        "Couldn't load workflow definitions. Try again.",
      )
    : null;
  const catalogError = runsError ?? definitionsError;
  const activityUnavailable = Boolean(runsError && !runsQuery.isPending);
  const activityRetryable = !(
    (runsQuery.error ?? definitionsQuery.error) instanceof
    WorkflowProviderConfigurationError
  );

  const filteredRuns = useMemo(
    () => applyWorkflowRunsListQuery(runs, listQuery),
    [runs, listQuery],
  );
  const hasFilters = workflowRunsListQueryIsActive(listQuery);

  const apiDefinitionIds = useMemo(
    () => (definitionsQuery.data ?? []).map((definition) => definition.id),
    [definitionsQuery.data],
  );
  const runDefinitionIds = useMemo(
    () =>
      runs
        .map((run) => run.definitionId?.trim() || "")
        .filter(Boolean),
    [runs],
  );
  const definitionOptions = useWorkflowDefinitionFilterOptions(
    apiDefinitionIds,
    runDefinitionIds,
  );
  const groupedDefinitionIds = useMemo(() => {
    if (definitionFilter) return [definitionFilter];
    return mergeWorkflowDefinitionIds(apiDefinitionIds, runDefinitionIds);
  }, [apiDefinitionIds, definitionFilter, runDefinitionIds]);

  function refreshRuns() {
    void queryClient.invalidateQueries({
      queryKey: queryKeys.workflows.list(appName),
    });
    void definitionsQuery.refetch();
  }

  return (
    <>
      <PageHeader className="mb-6">
        <PageHeaderContent size="md">
          <PageHeaderTitle>Runs</PageHeaderTitle>
          <PageHeaderDescription>
            Showing runs from all workflows that target this app. Open a run to
            inspect jobs, steps, and logs.
          </PageHeaderDescription>
        </PageHeaderContent>
        <PageHeaderActions className="flex-col items-start gap-2 sm:flex-row sm:items-center sm:gap-4">
          <WorkflowRefreshedAt
            dataUpdatedAt={
              runsQuery.isFetched ? runsQuery.dataUpdatedAt : null
            }
            refreshing={refreshing}
          />
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={refreshRuns}
            disabled={refreshing}
          >
            {refreshing ? "Refreshing…" : "Refresh"}
          </Button>
        </PageHeaderActions>
      </PageHeader>

      <div className="space-y-8">
        {catalogError ? (
          <ErrorNotice
            message={catalogError}
            onRetry={activityRetryable ? refreshRuns : undefined}
            retrying={refreshing}
          />
        ) : null}

        <section className="space-y-3" aria-label="All workflow runs">
          {/* Optional CLI tip — outline Card + content width, not a full-bleed Alert wash. */}
          <Collapsible
            className={cardVariants({ variant: "outline" })}
            data-animate-size=""
            data-testid="workflow-runs-cli-help"
          >
            <CollapsibleTrigger className="rounded-t-lg px-4 py-3 data-[state=closed]:rounded-lg">
              <span className="inline-flex min-w-0 items-center gap-2">
                <Lightbulb
                  className="size-4 shrink-0 text-muted-foreground"
                  aria-hidden
                />
                List runs from your terminal
              </span>
              <ChevronDownIcon className="size-4 shrink-0 text-muted-foreground transition-transform duration-overshoot ease-out-back motion-reduce:transition-none" aria-hidden />
            </CollapsibleTrigger>
            <CollapsibleContent className="space-y-2 rounded-b-lg border-t border-border px-4 py-3">
              <p className="text-sm text-foreground text-pretty">
                List this app&apos;s workflow runs with the Gestalt CLI.
              </p>
              <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
                <CopyableCode
                  value={`gestalt workflows runs list --app ${appName}`}
                  className="max-w-full text-xs [&_code]:text-xs"
                  tooltip="Copy command"
                />
                <Link asChild className="text-xs">
                  <RouterLink to="/docs/workflows">
                    Workflow CLI reference
                  </RouterLink>
                </Link>
              </div>
            </CollapsibleContent>
          </Collapsible>

          <WorkflowRunsFilters
            query={listQuery}
            definitionOptions={definitionOptions}
            disabled={!groupedByDefinition && activityUnavailable}
            onChange={replaceListQuery}
            onClear={clearListQuery}
          />

          {groupedByDefinition ? (
            groupedDefinitionIds.length > 0 ? (
              <WorkflowGroupedDefinitionRunsList
                appName={appName}
                definitionIds={groupedDefinitionIds}
                activityDefinitionIds={runDefinitionIds}
                status={serverStatus}
                listQuery={listQuery}
                onClearFilters={clearListQuery}
              />
            ) : definitionsQuery.isPending || runsQuery.isPending ? (
              <p className="text-sm text-muted-foreground/70">
                Loading workflow runs…
              </p>
            ) : catalogError ? null : (
              <EmptyRunsState appName={appName} />
            )
          ) : activityUnavailable ? (
            <p className="text-sm text-muted-foreground">
              Workflow activity is unavailable until the run list loads.
            </p>
          ) : runsQuery.isPending ? (
            <p className="text-sm text-muted-foreground/70">
              Loading workflow runs…
            </p>
          ) : (
            <RunsList
              runs={filteredRuns}
              hasFilters={hasFilters}
              appName={appName}
              highlightQuery={listQuery.q}
              onClearFilters={clearListQuery}
              hasMoreRuns={hasMoreRuns}
              loadingMore={loadingMore}
              onLoadMore={() => {
                void runsQuery.fetchNextPage();
              }}
            />
          )}
        </section>
      </div>
    </>
  );
}

function EmptyRunsState({ appName }: { appName: string }) {
  return (
    <div className="space-y-3" data-testid="app-workflows-empty">
      <p className="text-sm text-muted-foreground/70">
        No workflow runs for this app yet.
      </p>
      <ul className="list-disc space-y-1 pl-5 text-sm text-muted-foreground">
        <li className="space-y-1.5">
          <span>List workflow runs for this app from your terminal.</span>
          <div>
            <CopyableCode
              value={`gestalt workflows runs list --app ${appName}`}
              className="max-w-full text-xs [&_code]:text-xs"
              tooltip="Copy command"
            />
          </div>
        </li>
        <li>
          <Link asChild>
            <RouterLink to="/docs/workflows">
              Workflow CLI reference
            </RouterLink>
          </Link>{" "}
          for triggers and run inspection
        </li>
        <li>
          <Link asChild>
            <RouterLink to="/apps/$app/operations" params={{ app: appName }}>
              View the operations
            </RouterLink>
          </Link>{" "}
          this app can run
        </li>
      </ul>
    </div>
  );
}

function RunsList({
  runs,
  hasFilters,
  appName,
  highlightQuery,
  onClearFilters,
  hasMoreRuns,
  loadingMore,
  onLoadMore,
}: {
  runs: WorkflowRun[];
  hasFilters: boolean;
  appName: string;
  highlightQuery: string;
  onClearFilters: () => void;
  hasMoreRuns: boolean;
  loadingMore: boolean;
  onLoadMore: () => void;
}) {
  if (runs.length === 0) {
    if (hasFilters) {
      return (
        <div
          className="flex flex-col items-start gap-3"
          data-testid="app-workflows-filtered-empty"
        >
          <p className="text-sm text-muted-foreground">
            {hasMoreRuns
              ? "No matching runs loaded so far. Load more runs, or clear filters."
              : "No workflow runs match the current filters."}
          </p>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={onClearFilters}
          >
            Clear filters
          </Button>
          {hasMoreRuns ? (
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="w-full"
              onClick={onLoadMore}
              disabled={loadingMore}
              data-testid="app-workflows-load-more"
            >
              {loadingMore ? "Loading…" : "Load more runs"}
            </Button>
          ) : null}
        </div>
      );
    }
    return <EmptyRunsState appName={appName} />;
  }

  return (
    <div className="space-y-3">
      <WorkflowGhaRunsList
        runs={runs}
        appName={appName}
        highlightQuery={highlightQuery}
      />
      {hasMoreRuns ? (
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="w-full"
          onClick={onLoadMore}
          disabled={loadingMore}
          data-testid="app-workflows-load-more"
        >
          {loadingMore ? "Loading…" : "Load more runs"}
        </Button>
      ) : null}
    </div>
  );
}
