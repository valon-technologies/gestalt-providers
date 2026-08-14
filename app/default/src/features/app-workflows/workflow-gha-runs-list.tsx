import { Fragment, useEffect, useMemo, useRef } from "react";
import { Link } from "@tanstack/react-router";
import { Calendar, ChevronRight, Timer } from "lucide-react";
import type { WorkflowRun } from "@/lib/api";
import { cn } from "@/lib/cn";
import { listItemInteraction } from "@/lib/list-item-interaction";
import { isInteractiveTarget, rowLinkClickIntent } from "@/lib/row-link";
import { useWorkflowRunsQuery } from "@/lib/queries";
import { pickWorkflowRunListAggregates } from "@/lib/workflowApi";
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
import {
  rollupWorkflowRunGroupStatus,
  type WorkflowRunsGroupBy,
} from "./workflow-runs-group";
import { useWorkflowDefinitionGroupOpen } from "./workflow-runs-group-disclosure";
import { useStickyStuck } from "./workflow-runs-sticky-stuck";
import {
  applyWorkflowRunsListQuery,
  workflowDefinitionRunCountLabel,
  workflowListHasMorePages,
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
  groupBy = "none",
  highlightQuery = "",
}: {
  runs: WorkflowRun[];
  appName: string;
  groupBy?: WorkflowRunsGroupBy;
  highlightQuery?: string;
}) {
  // Flat list only — grouped mode uses WorkflowGroupedDefinitionRunsList so
  // each definition owns its ListRuns cursor.
  if (groupBy === "definition") {
    return null;
  }
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
  status,
  listQuery,
}: {
  appName: string;
  definitionIds: readonly string[];
  status?: string;
  listQuery: WorkflowRunsListQuery;
}) {
  return (
    <SearchHighlightProvider query={listQuery.q}>
      <div data-testid="app-workflow-run-list-grouped">
        {definitionIds.map((definitionId) => (
          <WorkflowDefinitionRunsSection
            key={definitionId}
            appName={appName}
            definitionId={definitionId}
            status={status}
            listQuery={listQuery}
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
  status,
  listQuery,
}: {
  appName: string;
  definitionId: string;
  status?: string;
  listQuery: WorkflowRunsListQuery;
}) {
  const [groupOpen, setGroupOpen] = useWorkflowDefinitionGroupOpen(
    appName,
    definitionId,
  );
  const runsQuery = useWorkflowRunsQuery(appName, {
    status,
    definitionId,
    pageSize: GROUPED_RUNS_PAGE_SIZE,
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
        // rows. Cardinality still comes from ListRuns `totalCount`.
        definitionId,
      }),
    [runs, listQuery, definitionId],
  );
  const aggregates = useMemo(
    () => pickWorkflowRunListAggregates(runsQuery.data?.pages ?? []),
    [runsQuery.data?.pages],
  );
  const loading = runsQuery.isPending;
  const loadingMore = runsQuery.isFetchingNextPage;
  const hasMoreRuns = workflowListHasMorePages({
    hasNextPage: Boolean(runsQuery.hasNextPage),
    loadedCount: runs.length,
    totalCount: aggregates.totalCount,
  });
  const runCountLabel = workflowDefinitionRunCountLabel({
    loading,
    loadedCount: filteredRuns.length,
    totalCount: aggregates.totalCount,
    hasMore: hasMoreRuns,
  });
  const toggleLabel = `Toggle runs for ${definitionId}`;
  const groupStatus = rollupWorkflowRunGroupStatus(filteredRuns);
  const headingId = `workflow-run-group-${definitionId}`;
  const headerRef = useRef<HTMLDivElement>(null);
  const headerStuck = useStickyStuck(headerRef, groupOpen);

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
          className={cn(
            "sticky top-[calc(var(--page-layout-mobile-nav-top)+var(--page-layout-mobile-nav-height))] z-20 isolate flex items-center gap-1 border-b border-transparent bg-background py-3 lg:top-[var(--app-sticky-chrome-height)]",
            headerStuck && "border-border",
          )}
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
              <ChevronRight className="size-4 text-muted-foreground transition-transform duration-hover-out ease-out-quart" />
            </CollapsibleTrigger>
          </Button>

          <SectionHeader className="min-w-0 flex-1 gap-x-1.5">
            <SectionHeaderIcon>
              <WorkflowStatusIcon status={groupStatus} title={groupStatus} />
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
                <SearchHighlight text={definitionId} variant="vivid" />
              </SectionHeaderTitle>
            </SectionHeaderContent>
            <SectionHeaderActions>
              <span className="text-xs font-normal text-muted-foreground">
                {runCountLabel}
              </span>
            </SectionHeaderActions>
          </SectionHeader>
        </div>

        <CollapsibleContent
          drawerClassName="relative z-0"
          className="pt-2 pb-8"
        >
          <div className="space-y-2 border-b border-border pb-3 pl-[calc(var(--size-control-sm)+0.25rem)]">
            {loading ? (
              <p className="text-sm text-muted-foreground/70">
                Loading runs…
              </p>
            ) : filteredRuns.length === 0 ? (
              hasMoreRuns ? null : (
                <p className="text-sm text-muted-foreground">
                  No runs for this definition
                  {listQuery.q.trim() || listQuery.statuses.length > 0
                    ? " match the current filters"
                    : " yet"}
                  .
                </p>
              )
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
