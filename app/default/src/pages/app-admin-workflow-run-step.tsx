import { useMemo } from "react";
import { Link, useParams } from "@tanstack/react-router";
import {
  useWorkflowDefinitionQuery,
  useWorkflowRunQuery,
  useWorkflowStepLogsQuery,
} from "@/lib/queries";
import {
  PageHeader,
  PageHeaderActions,
  PageHeaderContent,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderDescription,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { CodeBlock } from "@/components/ui/code-block";
import { cn } from "@/lib/cn";
import { listItemInteraction } from "@/lib/list-item-interaction";
import ErrorNotice from "@/components/ErrorNotice";
import { WorkflowStepDefinitionPanel } from "@/features/app-workflows/workflow-run-details";
import { WorkflowStepLogViewer } from "@/features/app-workflows/workflow-step-log-viewer";
import { WorkflowStatusIcon } from "@/features/app-workflows/workflow-status-icon";
import { WorkflowStatusBadge } from "@/features/app-workflows/workflow-status-badge";
import { WorkflowRefreshedAt } from "@/features/app-workflows/workflow-refreshed-at";
import { useResolvedWorkflowRunRoute } from "@/features/app-workflows/use-resolved-workflow-run-route";
import {
  findStepInGraph,
  flattenJobs,
  formatDuration,
  formatRelativeTime,
  projectWorkflowRunGraph,
} from "@/features/app-workflows/workflow-run-graph";
import { hasJSONValue, prettyJSON } from "@/features/app-workflows/workflow-format";
import { userFacingError } from "@/lib/user-facing-error";

export default function AppAdminWorkflowRunStepPage() {
  const { app, runId: routeRunId, jobId, stepId } = useParams({
    from: "/apps/$app/admin/workflows/runs/$runId/jobs/$jobId/steps/$stepId",
  });
  const { publicRunId, listRun, pathRunId } = useResolvedWorkflowRunRoute(
    app,
    routeRunId,
  );

  const detailQuery = useWorkflowRunQuery(app, publicRunId, listRun);
  const run = detailQuery.data ?? listRun ?? null;
  const definitionId = run?.definitionId?.trim() || null;
  const definitionQuery = useWorkflowDefinitionQuery(app, definitionId);

  const graph = useMemo(
    () => (run ? projectWorkflowRunGraph(run) : null),
    [run],
  );
  const located = graph ? findStepInGraph(graph, jobId, stepId) : null;
  const jobs = graph ? flattenJobs(graph) : [];
  // Synthetic single-job / single-step runs have nothing to jump between — skip the tree.
  const showJobsNav =
    jobs.length > 1 || jobs.some((job) => job.steps.length > 1);

  const stepTarget = useMemo(() => {
    const fromRun = run?.target.steps.find((step) => step.id === stepId);
    if (fromRun) return fromRun;
    return definitionQuery.data?.target.steps.find((step) => step.id === stepId);
  }, [run, definitionQuery.data, stepId]);

  const stepExecution = useMemo(
    () => run?.steps?.find((step) => step.stepId === stepId),
    [run, stepId],
  );

  const logsQuery = useWorkflowStepLogsQuery(
    app,
    publicRunId,
    jobId,
    stepId,
    listRun,
  );

  const refreshing =
    (detailQuery.isFetching && !detailQuery.isPending) ||
    (logsQuery.isFetching && !logsQuery.isPending) ||
    (definitionQuery.isFetching && !definitionQuery.isPending);

  const refreshedAt = useMemo(() => {
    const times = [
      detailQuery.isFetched ? detailQuery.dataUpdatedAt : 0,
      logsQuery.isFetched ? logsQuery.dataUpdatedAt : 0,
      definitionQuery.isFetched ? definitionQuery.dataUpdatedAt : 0,
    ];
    const latest = Math.max(...times);
    return latest > 0 ? latest : null;
  }, [
    definitionQuery.dataUpdatedAt,
    definitionQuery.isFetched,
    detailQuery.dataUpdatedAt,
    detailQuery.isFetched,
    logsQuery.dataUpdatedAt,
    logsQuery.isFetched,
  ]);

  const detailError = detailQuery.error
    ? userFacingError(detailQuery.error, "Unable to load this workflow run.")
    : null;
  const logsError = logsQuery.error
    ? userFacingError(logsQuery.error, "Unable to load step logs.")
    : null;

  return (
    <section aria-label="Workflow step detail">
      <PageHeader className="mb-6">
        <PageHeaderContent size="md">
          <PageHeaderTitle className="flex flex-wrap items-center gap-2">
            {located ? (
              <WorkflowStatusIcon status={located.step.status} size="lg" />
            ) : null}
            <span>{stepId}</span>
          </PageHeaderTitle>
        </PageHeaderContent>
        <PageHeaderActions className="flex-col items-start gap-2 sm:flex-row sm:items-center sm:gap-4">
          <WorkflowRefreshedAt
            dataUpdatedAt={refreshedAt}
            refreshing={refreshing}
          />
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => {
              void detailQuery.refetch();
              void logsQuery.refetch();
              void definitionQuery.refetch();
            }}
            disabled={refreshing || detailQuery.isFetching}
          >
            {refreshing ? "Refreshing…" : "Refresh"}
          </Button>
        </PageHeaderActions>
      </PageHeader>

      {detailError ? (
        <ErrorNotice
          message={detailError}
          onRetry={() => void detailQuery.refetch()}
          retrying={detailQuery.isFetching}
        />
      ) : null}

      <div
        className={cn(
          "grid gap-6",
          showJobsNav && "lg:grid-cols-[16rem_minmax(0,1fr)]",
        )}
      >
        {showJobsNav ? (
          <aside aria-label="Jobs in this run">
            <Card variant="outline" className="border-0 bg-card">
              <p className="px-3 py-2 text-xs font-medium tracking-wide text-muted-foreground">
                Jobs
              </p>
              <ul className="px-1 pb-1">
                {jobs.map((job) => {
                  const active = job.id === jobId;
                  const hrefStep = job.steps[0]?.id || stepId;
                  return (
                    <li key={job.id}>
                      <Link
                        to="/apps/$app/admin/workflows/runs/$runId/jobs/$jobId/steps/$stepId"
                        params={{
                          app,
                          runId: pathRunId,
                          jobId: job.id,
                          stepId: hrefStep,
                        }}
                        className={cn(
                          "flex items-center gap-2 rounded-md px-2 py-1.5 text-sm",
                          listItemInteraction({ pointer: "css" }),
                        )}
                        aria-current={active ? "page" : undefined}
                        data-selected={active ? "" : undefined}
                      >
                        <WorkflowStatusIcon status={job.status} size="sm" />
                        <span className="min-w-0 flex-1 truncate">
                          {job.name}
                        </span>
                      </Link>
                      {active ? (
                        <ul className="mb-1 ml-4 border-l border-border/60 pl-2">
                          {job.steps.map((step) => {
                            const stepActive = step.id === stepId;
                            return (
                              <li key={step.id}>
                                <Link
                                  to="/apps/$app/admin/workflows/runs/$runId/jobs/$jobId/steps/$stepId"
                                  params={{
                                    app,
                                    runId: pathRunId,
                                    jobId: job.id,
                                    stepId: step.id,
                                  }}
                                  className={cn(
                                    "flex items-center gap-2 rounded-md px-2 py-1 text-xs",
                                    listItemInteraction({ pointer: "css" }),
                                  )}
                                  data-selected={stepActive ? "" : undefined}
                                  aria-current={
                                    stepActive ? "page" : undefined
                                  }
                                >
                                  <WorkflowStatusIcon
                                    status={step.status}
                                    size="sm"
                                  />
                                  <span className="min-w-0 flex-1 truncate">
                                    {step.name}
                                  </span>
                                </Link>
                              </li>
                            );
                          })}
                        </ul>
                      ) : null}
                    </li>
                  );
                })}
              </ul>
            </Card>
          </aside>
        ) : null}

        <div className="min-w-0 space-y-8">
          {located ? (
            <p className="text-sm text-muted-foreground">
              {located.step.status || "unknown"}
              {located.step.completedAt || located.step.startedAt
                ? ` ${formatRelativeTime(located.step.completedAt || located.step.startedAt)}`
                : ""}
              {" in "}
              {formatDuration(located.step.durationMs)}
              {located.step.statusMessage
                ? ` · ${located.step.statusMessage}`
                : null}
              {located.step.skipReason
                ? ` · skipped: ${located.step.skipReason}`
                : null}
            </p>
          ) : null}

          <section className="space-y-3" aria-label="Step definition">
            <SectionHeader>
              <SectionHeaderContent size="sm">
                <SectionHeaderTitle as="h3">Definition</SectionHeaderTitle>
                <SectionHeaderDescription>
                  Recipe configuration for this step
                  {definitionId ? (
                    <>
                      {" "}
                      from{" "}
                      <Link
                        to="/apps/$app/admin/workflows/definitions/$definitionId"
                        params={{ app, definitionId }}
                        className="font-mono underline underline-offset-2"
                      >
                        {definitionId}
                      </Link>
                    </>
                  ) : null}
                  .
                </SectionHeaderDescription>
              </SectionHeaderContent>
            </SectionHeader>
            <WorkflowStepDefinitionPanel step={stepTarget} />
          </section>

          {stepExecution &&
          (hasJSONValue(stepExecution.input) ||
            hasJSONValue(stepExecution.output)) ? (
            <section className="space-y-3" aria-label="Step execution">
              <SectionHeader>
                <SectionHeaderContent size="sm">
                  <div className="flex flex-wrap items-center gap-2">
                    <SectionHeaderTitle as="h3">Execution</SectionHeaderTitle>
                    <WorkflowStatusBadge
                      status={stepExecution.status}
                      size="default"
                    />
                  </div>
                  <SectionHeaderDescription>
                    Materialized input and output from this run.
                  </SectionHeaderDescription>
                </SectionHeaderContent>
              </SectionHeader>
              <Card variant="outline" className="border-0 bg-card">
                <CardContent className="space-y-5 p-4">
                  {hasJSONValue(stepExecution.input) ? (
                    <div className="space-y-2">
                      <SectionHeader>
                        <SectionHeaderContent size="sm">
                          <SectionHeaderTitle as="h3">Input</SectionHeaderTitle>
                        </SectionHeaderContent>
                      </SectionHeader>
                      <CodeBlock
                        chrome="inset"
                        language="json"
                        code={prettyJSON(stepExecution.input)}
                        scrollable
                        maxHeight={192}
                        copyLabel="Copy input"
                      />
                    </div>
                  ) : null}
                  {hasJSONValue(stepExecution.output) ? (
                    <div className="space-y-2">
                      <SectionHeader>
                        <SectionHeaderContent size="sm">
                          <SectionHeaderTitle as="h3">
                            Output
                          </SectionHeaderTitle>
                        </SectionHeaderContent>
                      </SectionHeader>
                      <CodeBlock
                        chrome="inset"
                        language="json"
                        code={prettyJSON(stepExecution.output)}
                        scrollable
                        maxHeight={192}
                        copyLabel="Copy output"
                      />
                    </div>
                  ) : null}
                </CardContent>
              </Card>
            </section>
          ) : null}

          <section className="space-y-3" aria-label="Step logs">
            <SectionHeader>
              <SectionHeaderContent size="sm">
                <SectionHeaderTitle as="h3">Logs</SectionHeaderTitle>
                <SectionHeaderDescription>
                  Collapsible stdout-style log groups for this step.
                </SectionHeaderDescription>
              </SectionHeaderContent>
            </SectionHeader>

            {logsError ? (
              <ErrorNotice
                message={logsError}
                onRetry={() => void logsQuery.refetch()}
                retrying={logsQuery.isFetching}
              />
            ) : null}

            {logsQuery.isPending ? (
              <p className="text-sm text-muted-foreground/70">
                Loading step logs…
              </p>
            ) : (
              <WorkflowStepLogViewer groups={logsQuery.data?.groups ?? []} />
            )}
          </section>
        </div>
      </div>
    </section>
  );
}
