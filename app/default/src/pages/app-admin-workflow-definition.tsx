import { useMemo } from "react";
import { Link, useParams } from "@tanstack/react-router";
import type { WorkflowDefinition } from "@/lib/api";
import {
  useWorkflowDefinitionQuery,
  useWorkflowDefinitionsQuery,
  useWorkflowRunsQuery,
} from "@/lib/queries";
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
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";
import ErrorNotice from "@/components/ErrorNotice";
import { WorkflowTargetDetails } from "@/features/app-workflows/workflow-run-details";
import { WorkflowStatusBadge } from "@/features/app-workflows/workflow-status-badge";
import { WorkflowRefreshedAt } from "@/features/app-workflows/workflow-refreshed-at";
import { WorkflowSiblingPagination } from "@/features/app-workflows/workflow-sibling-pagination";
import {
  activationTriggerLabel,
  formatDate,
  runTriggerLabel,
  shortRunId,
  targetLabel,
} from "@/features/app-workflows/workflow-format";
import {
  summarizeWorkflowDefinitionsFromRuns,
} from "@/lib/workflowActivity";
import { userFacingError } from "@/lib/user-facing-error";
import { cn } from "@/lib/cn";
import { listItemInteraction } from "@/lib/list-item-interaction";
import { Info } from "lucide-react";
export default function AppAdminWorkflowDefinitionPage() {
  const { app, definitionId } = useParams({
    from: "/apps/$app/admin/workflows/definitions/$definitionId",
  });

  const definitionQuery = useWorkflowDefinitionQuery(app, definitionId);
  const definitionsQuery = useWorkflowDefinitionsQuery(app);
  const runsQuery = useWorkflowRunsQuery(app);
  const runs = useMemo(
    () => runsQuery.data?.pages.flatMap((page) => page.runs) ?? [],
    [runsQuery.data],
  );

  const observedDefinitions = useMemo(
    () => summarizeWorkflowDefinitionsFromRuns(runs),
    [runs],
  );

  const observedSummary = useMemo(
    () =>
      observedDefinitions.find((item) => item.definitionId === definitionId),
    [observedDefinitions, definitionId],
  );

  const definition: WorkflowDefinition | null = definitionQuery.data ?? null;
  const usingObservedFallback =
    Boolean(definitionQuery.error) ||
    (!definitionQuery.isPending && !definition);
  const showObservedNote = usingObservedFallback && Boolean(observedSummary);

  const apiDefinitions = definitionsQuery.data ?? [];
  const useObservedList =
    !definitionsQuery.isPending &&
    apiDefinitions.length === 0 &&
    (definitionsQuery.isSuccess || definitionsQuery.isError);

  const definitionIds = useMemo(() => {
    if (useObservedList) {
      return observedDefinitions.map((item) => item.definitionId);
    }
    return apiDefinitions.map((item) => item.id);
  }, [apiDefinitions, observedDefinitions, useObservedList]);

  const recentRuns = useMemo(
    () =>
      runs
        .filter((run) => run.definitionId === definitionId)
        .slice(0, 10),
    [runs, definitionId],
  );

  const definitionError =
    definitionQuery.error && !observedSummary
      ? userFacingError(
          definitionQuery.error,
          "Unable to load this workflow definition. Try again.",
        )
      : null;

  function refresh() {
    void definitionQuery.refetch();
    void definitionsQuery.refetch();
    void runsQuery.refetch();
  }

  const refreshing =
    (definitionQuery.isFetching && !definitionQuery.isPending) ||
    (definitionsQuery.isFetching && !definitionsQuery.isPending) ||
    (runsQuery.isFetching && !runsQuery.isPending);

  const refreshedAt = (() => {
    const times: number[] = [];
    if (definitionQuery.isFetched) times.push(definitionQuery.dataUpdatedAt);
    if (definitionsQuery.isFetched) times.push(definitionsQuery.dataUpdatedAt);
    if (runsQuery.isFetched) times.push(runsQuery.dataUpdatedAt);
    return times.length > 0 ? Math.max(...times) : null;
  })();

  return (
    <section aria-label="Workflow definition">
      <PageHeader className="mb-6">
        <PageHeaderContent size="md">
          <WorkflowSiblingPagination
            itemLabel="Definition"
            ids={definitionIds}
            currentId={definitionId}
            linkForId={(id) => ({
              to: "/apps/$app/admin/workflows/definitions/$definitionId",
              params: { app, definitionId: id },
            })}
          />
          <PageHeaderTitle className="font-mono text-base sm:text-lg">
            {definitionId}
          </PageHeaderTitle>
          {definition ? (
            <PageHeaderDescription>
              Generation {definition.generation ?? "—"}
              {definition.runAs ? ` · run as ${definition.runAs}` : ""}
            </PageHeaderDescription>
          ) : observedSummary ? (
            <PageHeaderDescription>
              Observed from {observedSummary.runCount} recent run
              {observedSummary.runCount === 1 ? "" : "s"}
            </PageHeaderDescription>
          ) : null}
        </PageHeaderContent>
        <PageHeaderActions className="flex-col items-start gap-2 sm:flex-row sm:items-center sm:gap-4">
          <WorkflowRefreshedAt
            dataUpdatedAt={refreshedAt}
            refreshing={refreshing}
          />
          <Button asChild variant="outline" size="sm">
            <Link
              to="/apps/$app/admin/workflows"
              params={{ app }}
              search={{ definition: definitionId }}
            >
              View runs
            </Link>
          </Button>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={refresh}
            disabled={refreshing}
          >
            {refreshing ? "Refreshing…" : "Refresh"}
          </Button>
        </PageHeaderActions>
      </PageHeader>

      {definitionError ? (
        <ErrorNotice
          message={definitionError}
          onRetry={refresh}
          retrying={refreshing}
        />
      ) : null}

      {showObservedNote ? (
        <Alert variant="info" className="mb-6">
          <Info aria-hidden />
          <AlertDescription>
            Definition details are inferred from recent runs. Full inventory may
            be incomplete.
          </AlertDescription>
        </Alert>
      ) : null}

      {definitionQuery.isPending && !observedSummary ? (
        <p className="text-sm text-muted-foreground/70">
          Loading workflow definition…
        </p>
      ) : null}

      {definition ? (
        <div className="space-y-8">
          <section className="space-y-4">
            <SectionHeader>
              <SectionHeaderContent size="sm">
                <SectionHeaderTitle as="h3">Configuration</SectionHeaderTitle>
                <SectionHeaderDescription>
                  Provider, pause state, and activations for this definition.
                </SectionHeaderDescription>
              </SectionHeaderContent>
            </SectionHeader>

            <dl className="grid gap-3 sm:grid-cols-2">
              <div>
                <dt className="text-xs font-medium text-muted-foreground">
                  Provider
                </dt>
                <dd className="mt-1 text-sm text-foreground">
                  {definition.provider}
                </dd>
              </div>
              <div>
                <dt className="text-xs font-medium text-muted-foreground">
                  Generation
                </dt>
                <dd className="mt-1 text-sm text-foreground">
                  {definition.generation ?? "—"}
                </dd>
              </div>
              <div>
                <dt className="text-xs font-medium text-muted-foreground">
                  Run as
                </dt>
                <dd className="mt-1 text-sm text-foreground">
                  {definition.runAs || "—"}
                </dd>
              </div>
              <div>
                <dt className="text-xs font-medium text-muted-foreground">
                  Updated
                </dt>
                <dd className="mt-1 text-sm text-foreground">
                  {formatDate(definition.updatedAt)}
                </dd>
              </div>
            </dl>

            <div className="flex flex-wrap gap-2">
              {definition.paused ? (
                <Badge size="sm" variant="warning">
                  Paused
                </Badge>
              ) : (
                <Badge size="sm" variant="success">
                  Active
                </Badge>
              )}
            </div>

            {definition.activations.length > 0 ? (
              <div>
                <h4 className="text-xs font-medium tracking-wide text-muted-foreground">
                  Activations
                </h4>
                <ul className="mt-3 space-y-2">
                  {definition.activations.map((activation) => (
                    <li
                      key={activation.id}
                      className="rounded-md border border-border px-3 py-2 text-sm"
                    >
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="font-mono text-foreground">
                          {activation.id || "—"}
                        </span>
                        {activation.paused ? (
                          <Badge size="sm" variant="warning">
                            Paused
                          </Badge>
                        ) : null}
                      </div>
                      <p className="mt-1 text-xs text-muted-foreground">
                        {activationTriggerLabel(activation)}
                      </p>
                    </li>
                  ))}
                </ul>
              </div>
            ) : (
              <p className="text-sm text-muted-foreground/70">
                No activations on this definition.
              </p>
            )}
          </section>

          <section className="space-y-3">
            <SectionHeader>
              <SectionHeaderContent size="sm">
                <SectionHeaderTitle as="h3">Target steps</SectionHeaderTitle>
              </SectionHeaderContent>
            </SectionHeader>
            <WorkflowTargetDetails target={definition.target} />
          </section>
        </div>
      ) : null}

      <section className="mt-8 space-y-3" aria-label="Recent runs">
        <SectionHeader>
          <SectionHeaderContent size="sm">
            <SectionHeaderTitle as="h3">Recent runs</SectionHeaderTitle>
            <SectionHeaderDescription>
              Runs for this definition from the loaded run history.
            </SectionHeaderDescription>
          </SectionHeaderContent>
        </SectionHeader>

        {recentRuns.length === 0 ? (
          <p className="text-sm text-muted-foreground/70">
            No runs for this definition in recent results.
          </p>
        ) : (
          <ul className="divide-y divide-border rounded-lg border border-border">
            {recentRuns.map((run) => (
              <li key={run.id}>
                <Link
                  to="/apps/$app/admin/workflows/runs/$runId"
                  params={{ app, runId: run.id }}
                  className={cn(
                    "flex flex-col gap-1 px-4 py-3 focus-ring-inset sm:flex-row sm:items-center sm:justify-between",
                    listItemInteraction({ pointer: "css" }),
                  )}
                >
                  <span className="truncate text-sm font-medium text-foreground">
                    {targetLabel(run.target) || shortRunId(run.id)}
                  </span>
                  <span className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                    <WorkflowStatusBadge status={run.status} />
                    <span>{formatDate(run.createdAt)}</span>
                    <span>{runTriggerLabel(run)}</span>
                  </span>
                </Link>
              </li>
            ))}
          </ul>
        )}
      </section>
    </section>
  );
}
