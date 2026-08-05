import { useDeferredValue, useMemo, useState } from "react";
import type { WorkflowRun } from "@/lib/api";
import { Link as RouterLink, useRouterState } from "@tanstack/react-router";
import { useWorkflowRunsQuery } from "@/lib/queries";
import { Link } from "@/components/ui/link";
import {
  PageHeader,
  PageHeaderActions,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderDescription,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Field,
  FieldContent,
  FieldLabel,
} from "@/components/ui/field";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import ErrorNotice from "@/components/ErrorNotice";
import {
  Stat,
  StatGroup,
  StatLabel,
  StatValue,
} from "@/components/ui/stat";
import { userFacingError } from "@/lib/user-facing-error";
import { WorkflowProviderConfigurationError } from "@/lib/workflowProvider";
import { WorkflowRefreshedAt } from "@/features/app-workflows/workflow-refreshed-at";
import { WorkflowGhaRunsList } from "@/features/app-workflows/workflow-gha-runs-list";
import {
  capitalize,
  filterRuns,
  workflowRunCounts,
} from "@/features/app-workflows/workflow-format";

const RUN_STATUSES = [
  "all",
  "pending",
  "running",
  "succeeded",
  "failed",
  "canceled",
] as const;

export default function AppWorkflowRunsPanel({ appName }: { appName: string }) {
  const definitionFilter = useRouterState({
    select: (state) => {
      const value = new URLSearchParams(state.location.searchStr).get(
        "definition",
      );
      return value?.trim() || undefined;
    },
  });

  const runsQuery = useWorkflowRunsQuery(appName);
  const runs = useMemo(
    () => runsQuery.data?.pages.flatMap((page) => page.runs) ?? [],
    [runsQuery.data],
  );
  const loading = runsQuery.isPending;
  const refreshing = runsQuery.isFetching && !runsQuery.isPending;
  const loadingMore = runsQuery.isFetchingNextPage;
  const hasMoreRuns = Boolean(runsQuery.hasNextPage);
  const runsError = runsQuery.error
    ? userFacingError(runsQuery.error, "Unable to load workflow activity. Try again.")
    : null;
  const activityUnavailable = Boolean(runsError && !loading);
  const activityRetryable = !(
    runsQuery.error instanceof WorkflowProviderConfigurationError
  );

  const [runsQueryText, setRunsQuery] = useState("");
  const [runStatus, setRunStatus] = useState<string>("all");
  const deferredRunsQuery = useDeferredValue(runsQueryText);

  const filteredRuns = useMemo(
    () =>
      filterRuns(runs, deferredRunsQuery, runStatus, definitionFilter),
    [runs, deferredRunsQuery, runStatus, definitionFilter],
  );

  function refreshRuns() {
    void runsQuery.refetch();
  }

  const counts = workflowRunCounts(runs);
  const hasFilters = Boolean(
    deferredRunsQuery.trim() || runStatus !== "all" || definitionFilter,
  );

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
        {runsError ? (
          <ErrorNotice
            message={runsError}
            onRetry={activityRetryable ? refreshRuns : undefined}
            retrying={refreshing}
          />
        ) : null}

        {!activityUnavailable ? (
          <StatGroup
            className="w-full max-w-3xl"
            data-testid="workflow-run-stats"
          >
            <Stat variant="plain" className="w-max max-w-full shrink-0">
              <StatLabel>Runs</StatLabel>
              <StatValue>{loading ? "—" : runs.length}</StatValue>
            </Stat>
            <Stat variant="plain" className="w-max max-w-full shrink-0">
              <StatLabel>Running</StatLabel>
              <StatValue>{loading ? "—" : counts.running}</StatValue>
            </Stat>
            <Stat variant="plain" className="w-max max-w-full shrink-0">
              <StatLabel>Succeeded</StatLabel>
              <StatValue>{loading ? "—" : counts.succeeded}</StatValue>
            </Stat>
            <Stat variant="plain" className="w-max max-w-full shrink-0">
              <StatLabel>Failed</StatLabel>
              <StatValue>{loading ? "—" : counts.failed}</StatValue>
            </Stat>
          </StatGroup>
        ) : null}

        <section className="space-y-4" aria-label="All workflow runs">
          <SectionHeader>
            <SectionHeaderContent size="sm">
              <SectionHeaderTitle as="h3">Workflow runs</SectionHeaderTitle>
              <SectionHeaderDescription>
                Filter by status or search, then open a run for the Actions-style
                job graph and step logs.
              </SectionHeaderDescription>
            </SectionHeaderContent>
          </SectionHeader>

          {definitionFilter ? (
            <div className="flex flex-wrap items-center gap-2 rounded-md border border-border bg-muted/40 px-3 py-2 text-sm text-muted-foreground">
              <span>
                Filtered to definition{" "}
                <code className="font-mono text-xs text-foreground">
                  {definitionFilter}
                </code>
              </span>
              <Button asChild variant="ghost" size="sm">
                <RouterLink
                  to="/apps/$app/admin/workflows"
                  params={{ app: appName }}
                  search={{}}
                >
                  Clear
                </RouterLink>
              </Button>
            </div>
          ) : null}

          <div className="grid gap-4 md:grid-cols-[minmax(0,1fr)_12rem]">
            <Field>
              <FieldLabel htmlFor="workflow-runs-search">Search runs</FieldLabel>
              <FieldContent>
                <Input
                  id="workflow-runs-search"
                  value={runsQueryText}
                  onChange={(event) => setRunsQuery(event.target.value)}
                  placeholder="Run ID, step, definition, event"
                  disabled={activityUnavailable}
                />
              </FieldContent>
            </Field>
            <Field>
              <FieldLabel htmlFor="workflow-runs-status">Status</FieldLabel>
              <FieldContent>
                <Select
                  value={runStatus}
                  onValueChange={setRunStatus}
                  disabled={activityUnavailable}
                >
                  <SelectTrigger id="workflow-runs-status" className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {RUN_STATUSES.map((status) => (
                      <SelectItem key={status} value={status}>
                        {capitalize(status)}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </FieldContent>
            </Field>
          </div>

          <p className="text-xs text-muted-foreground">
            CLI:{" "}
            <code className="font-mono text-xs">
              gestalt workflows runs list --app {appName}
            </code>
            .{" "}
            <Link asChild>
              <RouterLink to="/docs/workflows">View workflow docs</RouterLink>
            </Link>
            .
          </p>

          {activityUnavailable ? (
            <p className="text-sm text-muted-foreground">
              Workflow activity is unavailable until the run list loads.
            </p>
          ) : loading ? (
            <p className="text-sm text-muted-foreground/70">
              Loading workflow runs…
            </p>
          ) : (
            <RunsList
              runs={filteredRuns}
              totalRuns={runs.length}
              hasFilters={hasFilters}
              appName={appName}
              definitionFilter={definitionFilter}
              onClearFilters={() => {
                setRunsQuery("");
                setRunStatus("all");
              }}
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

function RunsList({
  runs,
  totalRuns,
  hasFilters,
  appName,
  definitionFilter,
  onClearFilters,
  hasMoreRuns,
  loadingMore,
  onLoadMore,
}: {
  runs: WorkflowRun[];
  totalRuns: number;
  hasFilters: boolean;
  appName: string;
  definitionFilter?: string;
  onClearFilters: () => void;
  hasMoreRuns: boolean;
  loadingMore: boolean;
  onLoadMore: () => void;
}) {
  if (runs.length === 0) {
    if (totalRuns > 0 && hasFilters) {
      return (
        <div
          className="flex flex-col items-start gap-3"
          data-testid="app-workflows-filtered-empty"
        >
          <p className="text-sm text-muted-foreground">
            No workflow runs match the current filters.
          </p>
          <div className="flex flex-wrap gap-2">
            <Button type="button" variant="outline" size="sm" onClick={onClearFilters}>
              Clear filters
            </Button>
            {definitionFilter ? (
              <Button asChild variant="outline" size="sm">
                <RouterLink
                  to="/apps/$app/admin/workflows"
                  params={{ app: appName }}
                  search={{}}
                >
                  Clear definition filter
                </RouterLink>
              </Button>
            ) : null}
          </div>
        </div>
      );
    }
    return (
      <div className="space-y-3" data-testid="app-workflows-empty">
        <p className="text-sm text-muted-foreground/70">
          No workflow runs for this app yet.
        </p>
        <ul className="list-disc space-y-1 pl-5 text-sm text-muted-foreground">
          <li>
            List runs from the CLI:{" "}
            <code className="font-mono text-xs">
              gestalt workflows runs list --app {appName}
            </code>
          </li>
          <li>
            <Link asChild>
              <RouterLink to="/docs/workflows">View workflow docs</RouterLink>
            </Link>{" "}
            for triggers and run inspection
          </li>
          <li>
            <Link asChild>
              <RouterLink to="/apps/$app/operations" params={{ app: appName }}>
                View operations
              </RouterLink>
            </Link>{" "}
            this app can run
          </li>
        </ul>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <WorkflowGhaRunsList runs={runs} appName={appName} />
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
