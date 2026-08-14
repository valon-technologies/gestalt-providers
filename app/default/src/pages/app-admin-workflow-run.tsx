import { useMemo, useState } from "react";
import { useParams } from "@tanstack/react-router";
import {
  useCancelWorkflowRunMutation,
  useWorkflowRunEventsQuery,
  useWorkflowRunOutputQuery,
  useWorkflowRunQuery,
} from "@/lib/queries";
import { Button } from "@/components/ui/button";
import {
  PageHeader,
  PageHeaderActions,
  PageHeaderContent,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import ErrorNotice from "@/components/ErrorNotice";
import { CodeBlock } from "@/components/ui/code-block";
import { CopyableCode } from "@/components/ui/copyable-code";
import { WorkflowRunDetails } from "@/features/app-workflows/workflow-run-details";
import { WorkflowRunJobGraph } from "@/features/app-workflows/workflow-run-job-graph";
import { WorkflowStatusIcon } from "@/features/app-workflows/workflow-status-icon";
import { WorkflowRefreshedAt } from "@/features/app-workflows/workflow-refreshed-at";
import { useResolvedWorkflowRunRoute } from "@/features/app-workflows/use-resolved-workflow-run-route";
import { projectWorkflowRunGraph } from "@/features/app-workflows/workflow-run-graph";
import {
  formatDate,
  hasJSONValue,
  prettyJSON,
  shortRunId,
  targetLabel,
} from "@/features/app-workflows/workflow-format";
import { userFacingError } from "@/lib/user-facing-error";

const CANCEL_REASON = "Canceled manually";

export default function AppAdminWorkflowRunPage() {
  const { app, runId: routeRunId } = useParams({
    from: "/apps/$app/admin/workflows/runs/$runId",
  });
  const { publicRunId, listRun, pathRunId } = useResolvedWorkflowRunRoute(
    app,
    routeRunId,
  );

  const detailQuery = useWorkflowRunQuery(app, publicRunId, listRun);
  const eventsQuery = useWorkflowRunEventsQuery(app, publicRunId, listRun);
  const cancelMutation = useCancelWorkflowRunMutation(app);
  const run = detailQuery.data ?? listRun ?? null;
  const fetchDedicatedOutput = Boolean(run) && !hasJSONValue(run?.output);
  const outputQuery = useWorkflowRunOutputQuery(app, publicRunId, listRun, {
    enabled: fetchDedicatedOutput,
  });
  const graph = useMemo(
    () => (run ? projectWorkflowRunGraph(run) : null),
    [run],
  );
  const events = eventsQuery.data ?? [];
  const dedicatedOutput = outputQuery.data;
  const displayOutput = hasJSONValue(run?.output)
    ? run?.output
    : dedicatedOutput;

  const detailLoading = detailQuery.isFetching && !detailQuery.data;
  const detailError = detailQuery.error
    ? userFacingError(
        detailQuery.error,
        "Unable to load this workflow run. Try again.",
      )
    : null;
  const actionError = cancelMutation.error
    ? userFacingError(
        cancelMutation.error,
        "Unable to cancel this workflow run. Try again.",
        { action: "cancel" },
      )
    : null;
  const canceling = cancelMutation.isPending;
  const [cancelDialogOpen, setCancelDialogOpen] = useState(false);
  const refreshing =
    (detailQuery.isFetching && !detailQuery.isPending) ||
    (eventsQuery.isFetching && !eventsQuery.isPending) ||
    (outputQuery.isFetching && !outputQuery.isPending);

  const pageTitle =
    (run ? targetLabel(run.target) : "") ||
    run?.definitionId ||
    shortRunId(publicRunId ?? routeRunId);

  function handleCancelRun() {
    if (!run || canceling) return;
    cancelMutation.mutate({
      id: run.id,
      reason: CANCEL_REASON,
      run,
    });
  }

  function refresh() {
    void detailQuery.refetch();
    void eventsQuery.refetch();
    if (fetchDedicatedOutput) {
      void outputQuery.refetch();
    }
  }

  return (
    <section aria-label="Workflow run">
      <PageHeader className="mb-6">
        <PageHeaderContent size="md">
          <PageHeaderTitle className="flex flex-wrap items-center gap-2">
            {run ? <WorkflowStatusIcon status={run.status} size="lg" /> : null}
            <span>{pageTitle}</span>
          </PageHeaderTitle>
        </PageHeaderContent>
        <PageHeaderActions className="flex-col items-start gap-2 sm:flex-row sm:items-center sm:gap-4">
          <WorkflowRefreshedAt
            dataUpdatedAt={
              detailQuery.isFetched ? detailQuery.dataUpdatedAt : null
            }
            refreshing={refreshing}
          />
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={refresh}
            disabled={refreshing || detailQuery.isFetching}
          >
            {detailQuery.isFetching ? "Refreshing…" : "Refresh"}
          </Button>
          {run?.status === "pending" ? (
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => setCancelDialogOpen(true)}
              disabled={canceling}
            >
              {canceling ? "Canceling…" : "Cancel run"}
            </Button>
          ) : null}
        </PageHeaderActions>
      </PageHeader>

      <div className="mb-6 max-w-full">
        <CopyableCode
          value={shortRunId(run?.id ?? publicRunId ?? routeRunId)}
          tooltip="Copy run ID"
          className="max-w-full text-xs [&_code]:text-xs"
        >
          {shortRunId(run?.id ?? publicRunId ?? routeRunId)}
        </CopyableCode>
      </div>

      {!run && detailLoading ? (
        <p className="text-sm text-muted-foreground/70">Loading workflow run…</p>
      ) : null}

      {detailError ? (
        <ErrorNotice
          message={detailError}
          onRetry={refresh}
          retrying={detailQuery.isFetching}
        />
      ) : null}

      {actionError ? (
        <p className="mb-4 text-sm text-destructive">{actionError}</p>
      ) : null}

      {run && graph ? (
        <div className="space-y-6">
          {run.status === "running" ? (
            <p className="text-sm text-muted-foreground">
              Running runs can&apos;t be canceled from this page. Refresh to
              check for an updated status.
            </p>
          ) : null}

          {detailLoading ? (
            <p className="text-sm text-muted-foreground/70">Loading details…</p>
          ) : null}

          <WorkflowRunJobGraph
            appName={app}
            runId={pathRunId}
            graph={graph}
          />

          {run.statusMessage ? (
            <p className="text-sm text-muted-foreground">{run.statusMessage}</p>
          ) : null}

          <WorkflowRunDetails
            run={run}
            appName={app}
            outputOverride={displayOutput}
            durationMs={graph.durationMs}
          />

          <section className="space-y-3" aria-label="Run events">
            <SectionHeader>
              <SectionHeaderContent size="sm">
                <SectionHeaderTitle as="h3">Events</SectionHeaderTitle>
              </SectionHeaderContent>
            </SectionHeader>
            {eventsQuery.isPending ? (
              <p className="text-sm text-muted-foreground/70">
                Loading run events…
              </p>
            ) : eventsQuery.error ? (
              <ErrorNotice
                message={userFacingError(
                  eventsQuery.error,
                  "Couldn't load run events. Try again.",
                )}
                onRetry={() => {
                  void eventsQuery.refetch();
                }}
                retrying={eventsQuery.isFetching && !eventsQuery.isPending}
              />
            ) : events.length === 0 ? (
              <p className="text-sm text-muted-foreground/70">
                No events recorded for this run yet.
              </p>
            ) : (
              <ul className="divide-y divide-border rounded-lg border border-border">
                {events.map((event, index) => (
                  <li
                    key={event.id || `${event.type ?? "event"}-${index}`}
                    className="px-4 py-3"
                  >
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <p className="text-sm font-medium text-foreground">
                        {event.type || "event"}
                      </p>
                      <p className="text-xs text-muted-foreground">
                        {formatDate(event.createdAt)}
                      </p>
                    </div>
                    {event.stepId ? (
                      <p className="mt-1 font-mono text-xs text-muted-foreground">
                        step {event.stepId}
                      </p>
                    ) : null}
                    {hasJSONValue(event.data) ? (
                      <div className="mt-2">
                        <CodeBlock
                          chrome="inset"
                          language="json"
                          code={prettyJSON(event.data)}
                          scrollable
                          maxHeight={192}
                          copyLabel="Copy event data"
                        />
                      </div>
                    ) : null}
                  </li>
                ))}
              </ul>
            )}
          </section>
        </div>
      ) : null}

      <AlertDialog open={cancelDialogOpen} onOpenChange={setCancelDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Cancel this workflow run?</AlertDialogTitle>
            <AlertDialogDescription>
              The run will stop if it has not already completed. This action
              cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={canceling}>Keep run</AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              onClick={handleCancelRun}
              disabled={canceling}
            >
              {canceling ? "Canceling…" : "Cancel run"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </section>
  );
}
