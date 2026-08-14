import { useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import type { WorkflowDefinition } from "@/lib/api";
import {
  useDeleteWorkflowDefinitionMutation,
  useSetWorkflowActivationPausedMutation,
  useSetWorkflowDefinitionPausedMutation,
  useStartWorkflowRunMutation,
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
  workflowRunPathId,
} from "@/features/app-workflows/workflow-format";
import {
  summarizeWorkflowDefinitionsFromRuns,
} from "@/lib/workflowActivity";
import { userFacingError } from "@/lib/user-facing-error";
import { cn } from "@/lib/cn";
import { listItemInteraction } from "@/lib/list-item-interaction";
import { Info } from "lucide-react";

export default function AppAdminWorkflowDefinitionPage() {
  const navigate = useNavigate();
  const { app, definitionId } = useParams({
    from: "/apps/$app/admin/workflows/definitions/$definitionId",
  });

  const definitionQuery = useWorkflowDefinitionQuery(app, definitionId);
  const definitionsQuery = useWorkflowDefinitionsQuery(app);
  const runsQuery = useWorkflowRunsQuery(app);
  const startRunMutation = useStartWorkflowRunMutation(app);
  const setPausedMutation = useSetWorkflowDefinitionPausedMutation(app);
  const setActivationPausedMutation =
    useSetWorkflowActivationPausedMutation(app);
  const deleteMutation = useDeleteWorkflowDefinitionMutation(app);

  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);

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
  const controlsEnabled = Boolean(definition) && !usingObservedFallback;

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

  const actionError = startRunMutation.error
    ? userFacingError(
        startRunMutation.error,
        "Unable to start a run for this definition. Try again.",
      )
    : setPausedMutation.error
      ? userFacingError(
          setPausedMutation.error,
          "Unable to update pause state. Try again.",
        )
      : setActivationPausedMutation.error
        ? userFacingError(
            setActivationPausedMutation.error,
            "Unable to update this activation. Try again.",
          )
        : deleteMutation.error
          ? userFacingError(
              deleteMutation.error,
              "Unable to delete this definition. Try again.",
            )
          : null;

  const mutating =
    startRunMutation.isPending ||
    setPausedMutation.isPending ||
    setActivationPausedMutation.isPending ||
    deleteMutation.isPending;

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

  async function handleStartRun() {
    if (!definition || mutating) return;
    const run = await startRunMutation.mutateAsync({
      definitionId: definition.id,
      definition,
    });
    await navigate({
      to: "/apps/$app/admin/workflows/runs/$runId",
      params: { app, runId: workflowRunPathId(run.id) },
    });
  }

  function handleToggleDefinitionPaused() {
    if (!definition || mutating) return;
    setPausedMutation.mutate({
      definitionId: definition.id,
      paused: !definition.paused,
      provider: definition.provider,
    });
  }

  function handleToggleActivationPaused(activationId: string, paused: boolean) {
    if (!definition || mutating || !activationId) return;
    setActivationPausedMutation.mutate({
      definitionId: definition.id,
      activationId,
      paused: !paused,
      provider: definition.provider,
    });
  }

  async function handleDeleteDefinition() {
    if (!definition || mutating) return;
    await deleteMutation.mutateAsync({
      definitionId: definition.id,
      provider: definition.provider,
    });
    setDeleteDialogOpen(false);
    await navigate({
      to: "/apps/$app/admin/workflows",
      params: { app },
    });
  }

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
          {controlsEnabled ? (
            <>
              <Button
                type="button"
                size="sm"
                onClick={() => void handleStartRun()}
                disabled={mutating}
              >
                {startRunMutation.isPending ? "Starting…" : "Run now"}
              </Button>
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={handleToggleDefinitionPaused}
                disabled={mutating}
              >
                {setPausedMutation.isPending
                  ? "Updating…"
                  : definition?.paused
                    ? "Resume"
                    : "Pause"}
              </Button>
              <Button
                type="button"
                variant="danger"
                size="sm"
                onClick={() => setDeleteDialogOpen(true)}
                disabled={mutating}
              >
                Delete
              </Button>
            </>
          ) : null}
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

      {actionError ? (
        <ErrorNotice message={actionError} className="mb-6" />
      ) : null}

      {showObservedNote ? (
        <Alert variant="info" className="mb-6">
          <Info aria-hidden />
          <AlertDescription>
            Definition details come from recent runs. Some fields may be
            missing until the full definition loads.
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
                      className="flex flex-col gap-2 rounded-md border border-border px-3 py-2 text-sm sm:flex-row sm:items-center sm:justify-between"
                    >
                      <div className="min-w-0">
                        <div className="flex flex-wrap items-center gap-2">
                          <span className="font-mono text-foreground">
                            {activation.id || "—"}
                          </span>
                          {activation.paused ? (
                            <Badge size="sm" variant="warning">
                              Paused
                            </Badge>
                          ) : (
                            <Badge size="sm" variant="success">
                              Active
                            </Badge>
                          )}
                        </div>
                        <p className="mt-1 text-xs text-muted-foreground">
                          {activationTriggerLabel(activation)}
                        </p>
                      </div>
                      {controlsEnabled && activation.id ? (
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          disabled={mutating}
                          onClick={() =>
                            handleToggleActivationPaused(
                              activation.id,
                              Boolean(activation.paused),
                            )
                          }
                        >
                          {activation.paused ? "Resume" : "Pause"}
                        </Button>
                      ) : null}
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
                  params={{ app, runId: workflowRunPathId(run.id) }}
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
                    {runTriggerLabel(run) ? (
                      <span>{runTriggerLabel(run)}</span>
                    ) : null}
                  </span>
                </Link>
              </li>
            ))}
          </ul>
        )}
      </section>

      <AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete workflow definition</AlertDialogTitle>
            <AlertDialogDescription>
              This removes the definition and its activations. Existing run
              history is kept, but you will not be able to start new runs from
              this definition.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={deleteMutation.isPending}>
              Keep definition
            </AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              disabled={deleteMutation.isPending}
              onClick={(event) => {
                event.preventDefault();
                void handleDeleteDefinition();
              }}
            >
              {deleteMutation.isPending ? "Deleting…" : "Delete definition"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </section>
  );
}
