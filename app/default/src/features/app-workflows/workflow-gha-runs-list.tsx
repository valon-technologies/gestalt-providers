import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "@tanstack/react-router";
import { Calendar, ChevronRight, Timer } from "lucide-react";
import type { WorkflowRun } from "@/lib/api";
import { cn } from "@/lib/cn";
import { listItemInteraction } from "@/lib/list-item-interaction";
import { isInteractiveTarget, rowLinkClickIntent } from "@/lib/row-link";
import { useWorkflowRunsQuery } from "@/lib/queries";
import { pickWorkflowRunListAggregates } from "@/lib/workflowApi";
import { userFacingError } from "@/lib/user-facing-error";
import ErrorNotice from "@/components/ErrorNotice";
import { Card } from "@/components/ui/card";
import { CopyableCode } from "@/components/ui/copyable-code";
import { Button } from "@/components/ui/button";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import {
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemGroup,
  ItemMedia,
  ItemSeparator,
  ItemTitle,
} from "@/components/ui/item";
import {
  SectionHeader,
  SectionHeaderActions,
  SectionHeaderContent,
  SectionHeaderIcon,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import {
  SearchHighlight,
  SearchHighlightProvider,
} from "@/components/ui/search-highlight";
import {
  formatDuration,
  formatRelativeTime,
  projectWorkflowRunGraph,
} from "./workflow-run-graph";
import { WorkflowStatusIcon } from "./workflow-status-icon";
import {
  runTriggerActorDescription,
  shortRunId,
  workflowRunPathId,
  rememberWorkflowRunPublicId,
  workflowRunListTitle,
} from "./workflow-format";
import { rollupWorkflowRunGroupHeaderStatus, shouldExpandGroupForVisibleMatches } from "./workflow-runs-group";
import { useWorkflowDefinitionGroupOpen } from "./workflow-runs-group-disclosure";
import { useStickyStuck } from "./workflow-runs-sticky-stuck";
import {
  applyWorkflowRunsListQuery,
  workflowDefinitionRunCountLabel,
  workflowListHasMorePages,
  workflowRunsListQueryIsActive,
  workflowRunsListQueryUsesClientOnlyFilters,
  workflowVisibleRunTotalCount,
  type WorkflowRunsListQuery,
} from "./workflow-runs-list-query";

/** Flush navigable rows — Card radius on first/last Item (application-lists.md). */
const navigableRowClassName = cn(
  "relative rounded-none px-4 first:rounded-t-xl last:rounded-b-xl",
  // Item base paints `[a]:hover:bg-accent/50`; kill it so list wash owns hover.
  "hover:bg-transparent [a]:hover:bg-transparent",
  listItemInteraction({ pointer: "css" }),
);

/** Nested run rows inside a definition section card. */
const nestedRunRowClassName = cn(
  "relative isolate z-0 rounded-none px-4 first:rounded-t-xl last:rounded-b-xl",
  "hover:bg-transparent [a]:hover:bg-transparent",
  listItemInteraction({ pointer: "css" }),
);

/** Grouped list page size — small enough that deep definitions need Load more. */
const GROUPED_RUNS_PAGE_SIZE = 20;

export function WorkflowGhaRunsList({
  runs,
  appName,
  highlightQuery = "",
}: {
  runs: WorkflowRun[];
  appName: string;
  highlightQuery?: string;
}) {
  return (
    <SearchHighlightProvider query={highlightQuery}>
      <FlatRunsList runs={runs} appName={appName} />
    </SearchHighlightProvider>
  );
}

/**
 * Grouped Runs: one infinite ListRuns query per definition. The header count
 * is `total_count` from that first page (visibility cardinality). Load more
 * advances that definition’s page token only — never a walk of every page
 * to derive a total.
 */
export function WorkflowGroupedDefinitionRunsList({
  appName,
  definitionIds,
  activityDefinitionIds,
  status,
  listQuery,
  onClearFilters,
}: {
  appName: string;
  definitionIds: readonly string[];
  activityDefinitionIds: readonly string[];
  status?: string;
  listQuery: WorkflowRunsListQuery;
  onClearFilters: () => void;
}) {
  const filtersActive = workflowRunsListQueryIsActive(listQuery);
  const activityIds = useMemo(
    () => new Set(activityDefinitionIds.map((id) => id.trim()).filter(Boolean)),
    [activityDefinitionIds],
  );
  const [omittedIds, setOmittedIds] = useState<Set<string>>(() => new Set());
  const definitionKey = definitionIds.join("\0");
  const filterKey = `${listQuery.q}\0${listQuery.statuses.join(",")}\0${listQuery.definitionId ?? ""}`;

  useEffect(() => {
    setOmittedIds(new Set());
  }, [definitionKey, filterKey]);

  const reportOmitted = useCallback((id: string, omitted: boolean) => {
    setOmittedIds((prev) => {
      const has = prev.has(id);
      if (omitted === has) return prev;
      const next = new Set(prev);
      if (omitted) next.add(id);
      else next.delete(id);
      return next;
    });
  }, []);

  const allOmitted =
    filtersActive &&
    definitionIds.length > 0 &&
    definitionIds.every((id) => omittedIds.has(id));

  return (
    <SearchHighlightProvider query={listQuery.q}>
      {allOmitted ? (
        <div
          className="flex flex-col items-start gap-3"
          data-testid="app-workflows-filtered-empty"
        >
          <p className="text-sm text-muted-foreground">
            No workflow runs match the current filters.
          </p>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={onClearFilters}
          >
            Clear filters
          </Button>
        </div>
      ) : null}
      <div data-testid="app-workflow-run-list-grouped">
        {definitionIds.map((definitionId) => (
          <WorkflowDefinitionRunsSection
            key={definitionId}
            appName={appName}
            definitionId={definitionId}
            defaultOpen={
              activityIds.has(definitionId) ||
              listQuery.definitionId === definitionId
            }
            status={status}
            listQuery={listQuery}
            filtersActive={filtersActive}
            onClearFilters={onClearFilters}
            onOmittedChange={reportOmitted}
          />
        ))}
      </div>
    </SearchHighlightProvider>
  );
}

function FlatRunsList({
  runs,
  appName,
}: {
  runs: WorkflowRun[];
  appName: string;
}) {
  return (
    <Card
      variant="outline"
      className="overflow-hidden"
      data-testid="app-workflow-run-list"
    >
      <ItemGroup>
        {runs.map((run, index) => (
          <Fragment key={run.id}>
            {index > 0 ? <ItemSeparator /> : null}
            <WorkflowGhaRunRow
              run={run}
              appName={appName}
              className={navigableRowClassName}
            />
          </Fragment>
        ))}
      </ItemGroup>
    </Card>
  );
}

function WorkflowDefinitionRunsSection({
  appName,
  definitionId,
  defaultOpen,
  status,
  listQuery,
  filtersActive,
  onClearFilters,
  onOmittedChange,
}: {
  appName: string;
  definitionId: string;
  defaultOpen: boolean;
  status?: string;
  listQuery: WorkflowRunsListQuery;
  filtersActive: boolean;
  onClearFilters: () => void;
  onOmittedChange: (id: string, omitted: boolean) => void;
}) {
  const [groupOpen, setGroupOpen] = useWorkflowDefinitionGroupOpen(
    appName,
    definitionId,
    defaultOpen,
  );
  const runsQuery = useWorkflowRunsQuery(appName, {
    status,
    definitionId,
    pageSize: GROUPED_RUNS_PAGE_SIZE,
    enabled: groupOpen || filtersActive,
  });
  const runs = useMemo(
    () => runsQuery.data?.pages.flatMap((page) => page.runs) ?? [],
    [runsQuery.data],
  );
  const filteredRuns = useMemo(
    () =>
      applyWorkflowRunsListQuery(runs, {
        ...listQuery,
        // Display invariant: this section never shows another definition's
        // rows. Header cardinality uses ListRuns `totalCount` only when the
        // visible set is that same corpus (see workflowVisibleRunTotalCount).
        definitionId,
      }),
    [runs, listQuery, definitionId],
  );
  const aggregates = useMemo(
    () => pickWorkflowRunListAggregates(runsQuery.data?.pages ?? []),
    [runsQuery.data?.pages],
  );
  const loading = runsQuery.isLoading;
  const loadingMore = runsQuery.isFetchingNextPage;
  const hasMoreRuns = workflowListHasMorePages({
    hasNextPage: Boolean(runsQuery.hasNextPage),
    loadedCount: runs.length,
    totalCount: aggregates.totalCount,
  });
  const runCountLabel =
    !runsQuery.isFetched && !groupOpen
      ? ""
      : workflowDefinitionRunCountLabel({
          loading: groupOpen && loading,
          loadedCount: filteredRuns.length,
          totalCount: workflowVisibleRunTotalCount(
            listQuery,
            aggregates.totalCount,
          ),
          hasMore: hasMoreRuns,
        });
  const toggleLabel = groupOpen
    ? `Hide runs for ${definitionId}`
    : `Show runs for ${definitionId}`;
  const groupStatus = rollupWorkflowRunGroupHeaderStatus({
    clientOnlyFilters: workflowRunsListQueryUsesClientOnlyFilters({
      ...listQuery,
      definitionId,
    }),
    hasMore: hasMoreRuns,
    loadedRuns: filteredRuns,
    statusCounts: aggregates.statusCounts,
  });
  const headingId = `workflow-run-group-${definitionId}`;
  const headerRef = useRef<HTMLDivElement>(null);
  const headerStuck = useStickyStuck(headerRef, groupOpen);
  const runsError = runsQuery.error
    ? userFacingError(
        runsQuery.error,
        "Couldn't load runs for this definition. Try again.",
      )
    : null;
  const filteredEmpty = filteredRuns.length === 0;
  const hasListFilters = workflowRunsListQueryIsActive(listQuery);
  const omit =
    filtersActive &&
    !loading &&
    !runsError &&
    filteredEmpty &&
    !hasMoreRuns;

  useEffect(() => {
    onOmittedChange(definitionId, omit);
  }, [definitionId, omit, onOmittedChange]);

  useEffect(() => {
    if (
      shouldExpandGroupForVisibleMatches({
        filtersActive,
        matchingRunCount: filteredRuns.length,
      }) &&
      !groupOpen
    ) {
      setGroupOpen(true);
    }
  }, [filtersActive, filteredRuns.length, groupOpen, setGroupOpen]);

  if (omit) return null;

  return (
    <section
      aria-labelledby={headingId}
      data-testid={`app-workflow-run-group-${definitionId}`}
    >
      <Collapsible
        open={groupOpen}
        onOpenChange={setGroupOpen}
        className="group/definition-group"
      >
        {/* Sticky flush under app chrome so a long group keeps its definition. */}
        <div
          ref={headerRef}
          className="sticky top-[calc(var(--page-layout-mobile-nav-top)+var(--page-layout-mobile-nav-height))] z-20 isolate flex items-center gap-1 border-b border-transparent bg-background pt-6 pb-4 data-[stuck=true]:border-border lg:top-[var(--app-sticky-chrome-height)]"
          data-stuck={headerStuck ? "true" : undefined}
          data-testid={`app-workflow-run-group-header-${definitionId}`}
        >
          <Button
            asChild
            variant="ghost"
            size="icon-sm"
            className="size-control-sm shrink-0"
          >
            <CollapsibleTrigger
              type="button"
              aria-label={toggleLabel}
              className="size-control-sm w-control-sm max-w-control-sm justify-center gap-0 p-0 [&[data-state=open]>svg]:rotate-90"
            >
              <ChevronRight className="size-4 text-muted-foreground transition-transform duration-hover-out ease-out-quart motion-reduce:transition-none" />
            </CollapsibleTrigger>
          </Button>

          <SectionHeader className="min-w-0 flex-1 gap-x-1.5">
            <SectionHeaderIcon>
              <WorkflowStatusIcon status={groupStatus} />
            </SectionHeaderIcon>
            <SectionHeaderContent
              size="sm"
              className="min-w-0 flex-1 items-start text-left"
            >
              <SectionHeaderTitle
                as="h3"
                id={headingId}
                title={definitionId}
                className="max-w-full text-left text-sm font-medium whitespace-normal break-all"
              >
                <Link
                  to="/apps/$app/admin/workflows/definitions/$definitionId"
                  params={{ app: appName, definitionId }}
                  className="text-inherit hover:underline"
                >
                  <SearchHighlight text={definitionId} variant="vivid" />
                </Link>
              </SectionHeaderTitle>
            </SectionHeaderContent>
            <SectionHeaderActions>
              <span className="text-xs font-normal text-muted-foreground">
                {runCountLabel}
              </span>
            </SectionHeaderActions>
          </SectionHeader>
        </div>

        <CollapsibleContent drawerClassName="relative z-0">
          {/* pb-6 above the rule matches the next header's pt-6. */}
          <div className="space-y-2 border-b border-border pb-6 pl-[calc(var(--size-control-sm)+0.25rem)]">
            {loading ? (
              <p className="text-sm text-muted-foreground/70">
                Loading runs…
              </p>
            ) : runsError ? (
              <ErrorNotice
                message={runsError}
                onRetry={() => {
                  void runsQuery.refetch();
                }}
                retrying={runsQuery.isFetching && !runsQuery.isPending}
              />
            ) : filteredEmpty ? (
              <div className="flex flex-col items-start gap-3">
                <p className="text-sm text-muted-foreground">
                  {hasMoreRuns
                    ? "No matching runs loaded so far. Load more runs, or clear filters."
                    : hasListFilters
                      ? "No runs for this definition match the current filters."
                      : "No runs for this definition yet."}
                </p>
                {hasListFilters ? (
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={onClearFilters}
                  >
                    Clear filters
                  </Button>
                ) : null}
              </div>
            ) : (
              <Card variant="outline" className="overflow-hidden">
                <ItemGroup aria-label={`Runs for ${definitionId}`}>
                  {filteredRuns.map((run, index) => (
                    <Fragment key={run.id}>
                      {index > 0 ? <ItemSeparator /> : null}
                      <WorkflowGhaRunRow
                        run={run}
                        appName={appName}
                        className={nestedRunRowClassName}
                        groupedByDefinition
                      />
                    </Fragment>
                  ))}
                </ItemGroup>
              </Card>
            )}
            {hasMoreRuns ? (
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="w-full"
                onClick={() => {
                  void runsQuery.fetchNextPage();
                }}
                disabled={loadingMore}
                data-testid={`app-workflow-run-group-load-more-${definitionId}`}
              >
                {loadingMore ? "Loading…" : "Load more runs"}
              </Button>
            ) : null}
          </div>
        </CollapsibleContent>
      </Collapsible>
    </section>
  );
}

function WorkflowGhaRunRow({
  run,
  appName,
  className,
  groupedByDefinition = false,
}: {
  run: WorkflowRun;
  appName: string;
  className?: string;
  groupedByDefinition?: boolean;
}) {
  const graph = projectWorkflowRunGraph(run);
  const runIdLabel = shortRunId(run.id);
  const title = workflowRunListTitle(run, { groupedByDefinition });
  const showDefinitionTitle = !groupedByDefinition && title !== runIdLabel;
  const triggerActor = runTriggerActorDescription(run);
  const when = run.completedAt || run.startedAt || run.createdAt;
  useEffect(() => {
    rememberWorkflowRunPublicId(appName, run.id);
  }, [appName, run.id]);

  return (
    <Item
      role="listitem"
      size="sm"
      className={cn(className, triggerActor ? "items-start" : "items-center")}
      data-testid={`app-workflow-run-${run.id}`}
    >
      <ItemMedia className={cn("h-5", triggerActor ? "self-start" : "self-center")}>
        <WorkflowStatusIcon status={run.status} title={run.status} />
      </ItemMedia>
      <ItemContent className="min-w-0 gap-1">
        <ItemTitle className="max-w-full min-w-0">
          <Link
            to="/apps/$app/admin/workflows/runs/$runId"
            params={{ app: appName, runId: workflowRunPathId(run.id) }}
            data-row-link=""
            aria-label={
              showDefinitionTitle ? `${title}, run ${runIdLabel}` : runIdLabel
            }
            className={cn(
              "min-w-0 truncate font-medium text-foreground no-underline",
              "after:absolute after:inset-0 after:z-[1] after:rounded-[inherit] after:content-['']",
              "focus-visible:outline-none focus-visible:after:outline-3",
              "focus-visible:after:outline-offset-2 focus-visible:after:outline-ring",
            )}
            onClick={(event) => {
              const intent = rowLinkClickIntent({
                button: event.button,
                metaKey: event.metaKey,
                ctrlKey: event.ctrlKey,
                shiftKey: event.shiftKey,
                altKey: event.altKey,
                targetIsInteractive: isInteractiveTarget(
                  event.target,
                  event.currentTarget,
                ),
              });
              if (intent === "suppress") {
                event.preventDefault();
              }
            }}
          >
            {showDefinitionTitle ? (
              <SearchHighlight text={title} variant="vivid" />
            ) : (
              <span className="sr-only">{runIdLabel}</span>
            )}
          </Link>
            <span className="relative z-[2] shrink-0" data-no-row-click>
            <CopyableCode
              value={runIdLabel}
              tooltip="Copy run ID"
              className="max-w-[10rem] text-xs [&_code]:text-xs"
            >
              <SearchHighlight text={runIdLabel} variant="vivid" />
            </CopyableCode>
          </span>
        </ItemTitle>
        {triggerActor ? (
          <ItemDescription>
            <SearchHighlight text={triggerActor} variant="vivid" />
          </ItemDescription>
        ) : null}
      </ItemContent>
      <ItemActions className="h-5 items-center text-xs text-muted-foreground">
        <span className="inline-flex items-center gap-1">
          <Calendar className="size-3.5" aria-hidden />
          {formatRelativeTime(when)}
        </span>
        <span className="inline-flex items-center gap-1">
          <Timer className="size-3.5" aria-hidden />
          {formatDuration(graph.durationMs)}
        </span>
      </ItemActions>
    </Item>
  );
}
