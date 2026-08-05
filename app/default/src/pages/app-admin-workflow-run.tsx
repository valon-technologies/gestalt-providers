import { useMemo, useState } from "react";
import { useParams } from "@tanstack/react-router";
import { Calendar, Timer } from "lucide-react";
import {
  useCancelWorkflowRunMutation,
  useWorkflowRunQuery,
  useWorkflowRunsQuery,
} from "@/lib/queries";
import { Button } from "@/components/ui/button";
import {
  PageHeader,
  PageHeaderActions,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
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
import { WorkflowRunDetails } from "@/features/app-workflows/workflow-run-details";
import { WorkflowRunJobGraph } from "@/features/app-workflows/workflow-run-job-graph";
import { WorkflowStatusBadge } from "@/features/app-workflows/workflow-status-badge";
import { WorkflowStatusIcon } from "@/features/app-workflows/workflow-status-icon";
import { WorkflowRefreshedAt } from "@/features/app-workflows/workflow-refreshed-at";
import {
  formatDuration,
  formatRelativeTime,
  projectWorkflowRunGraph,
} from "@/features/app-workflows/workflow-run-graph";
import {
  runTriggerLabel,
  shortRunId,
  targetLabel,
} from "@/features/app-workflows/workflow-format";
import { userFacingError } from "@/lib/user-facing-error";

const CANCEL_REASON = "Canceled manually";

export default function AppAdminWorkflowRunPage() {
  const { app, runId } = useParams({
    from: "/apps/$app/admin/workflows/runs/$runId",
  });

  const runsQuery = useWorkflowRunsQuery(app);
  const listRun = useMemo(
    () =>
      runsQuery.data?.pages
        .flatMap((page) => page.runs)
        .find((run) => run.id === runId),
    [runsQuery.data, runId],
  );

  const detailQuery = useWorkflowRunQuery(app, runId, listRun);
  const cancelMutation = useCancelWorkflowRunMutation(app);
  const run = detailQuery.data ?? listRun ?? null;
  const graph = useMemo(
    () => (run ? projectWorkflowRunGraph(run) : null),
    [run],
  );

  const detailLoading = detailQuery.isFetching && !detailQuery.data;
  const detailError = detailQuery.error
    ? userFacingError(detailQuery.error, "Unable to load this workflow run. Try again.")
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
  const refreshing = detailQuery.isFetching && !detailQuery.isPending;

  const pageTitle =
    (run ? targetLabel(run.target) : "") ||
    run?.definitionId ||
    shortRunId(runId);

  function handleCancelRun() {
    if (!run || canceling) return;
    cancelMutation.mutate({
      id: run.id,
      reason: CANCEL_REASON,
      run,
    });
  }

  return (
    <section aria-label="Workflow run">
      <PageHeader className="mb-6">
        <PageHeaderContent size="md">
          <PageHeaderTitle className="flex flex-wrap items-center gap-2">
            {run ? <WorkflowStatusIcon status={run.status} size="lg" /> : null}
            <span>{pageTitle}</span>
          </PageHeaderTitle>
          {run ? (
            <PageHeaderDescription>
              {runTriggerLabel(run)}
              {run.createdBy?.subjectId
                ? ` · ${run.createdBy.subjectId.replace(/^[^:]+:/, "")}`
                : ""}
            </PageHeaderDescription>
          ) : null}
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
            onClick={() => {
              void detailQuery.refetch();
            }}
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

      {!run && detailLoading ? (
        <p className="text-sm text-muted-foreground/70">Loading workflow run…</p>
      ) : null}

      {detailError ? (
        <ErrorNotice
          message={detailError}
          onRetry={() => {
            void detailQuery.refetch();
          }}
          retrying={detailQuery.isFetching}
        />
      ) : null}

      {actionError ? (
        <p className="mb-4 text-sm text-destructive">{actionError}</p>
      ) : null}

      {run && graph ? (
        <div className="space-y-6">
          <dl className="grid gap-4 rounded-lg border border-border bg-card px-4 py-3 sm:grid-cols-4">
            <div>
              <dt className="text-xs text-muted-foreground">Triggered via</dt>
              <dd className="mt-1 text-sm text-foreground">
                {runTriggerLabel(run)}{" "}
                <span className="text-muted-foreground">
                  {formatRelativeTime(run.createdAt)}
                </span>
              </dd>
            </div>
            <div>
              <dt className="text-xs text-muted-foreground">Status</dt>
              <dd className="mt-1">
                <WorkflowStatusBadge status={run.status} />
              </dd>
            </div>
            <div>
              <dt className="text-xs text-muted-foreground">Total duration</dt>
              <dd className="mt-1 inline-flex items-center gap-1 text-sm text-foreground">
                <Timer className="size-3.5 text-muted-foreground" aria-hidden />
                {formatDuration(graph.durationMs)}
              </dd>
            </div>
            <div>
              <dt className="text-xs text-muted-foreground">Started</dt>
              <dd className="mt-1 inline-flex items-center gap-1 text-sm text-foreground">
                <Calendar className="size-3.5 text-muted-foreground" aria-hidden />
                {formatRelativeTime(run.startedAt || run.createdAt)}
              </dd>
            </div>
          </dl>

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
            runId={run.id}
            graph={graph}
            definitionLabel={run.definitionId || "workflow"}
            triggerLabel={runTriggerLabel(run)}
          />

          {run.statusMessage ? (
            <p className="text-sm text-muted-foreground">{run.statusMessage}</p>
          ) : null}

          <WorkflowRunDetails run={run} appName={app} />
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
