import { useMemo } from "react";
import { Link, useParams } from "@tanstack/react-router";
import type { WorkflowDefinition } from "@/lib/api";
import {
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
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";
import ErrorNotice from "@/components/ErrorNotice";
import { WorkflowStatusBadge } from "@/features/app-workflows/workflow-status-badge";
import { WorkflowRefreshedAt } from "@/features/app-workflows/workflow-refreshed-at";
import { formatDate } from "@/features/app-workflows/workflow-format";
import {
  summarizeWorkflowDefinitionsFromRuns,
  type WorkflowDefinitionActivity,
} from "@/lib/workflowActivity";
import { userFacingError } from "@/lib/user-facing-error";
import { cn } from "@/lib/cn";
import { listItemInteraction } from "@/lib/list-item-interaction";
import { Info } from "lucide-react";

export default function AppAdminWorkflowDefinitionsPage() {
  const { app } = useParams({
    from: "/apps/$app/admin/workflows/definitions",
  });

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

  const apiDefinitions = definitionsQuery.data ?? [];
  const useObservedFallback =
    !definitionsQuery.isPending &&
    apiDefinitions.length === 0 &&
    (definitionsQuery.isSuccess || definitionsQuery.isError);
  const showingObserved = useObservedFallback && observedDefinitions.length > 0;

  const definitionsError = definitionsQuery.error
    ? userFacingError(
        definitionsQuery.error,
        "Unable to load workflow definitions. Try again.",
      )
    : null;

  function refresh() {
    void definitionsQuery.refetch();
    void runsQuery.refetch();
  }

  const refreshing =
    (definitionsQuery.isFetching && !definitionsQuery.isPending) ||
    (runsQuery.isFetching && !runsQuery.isPending);

  const refreshedAt = (() => {
    const times: number[] = [];
    if (definitionsQuery.isFetched) times.push(definitionsQuery.dataUpdatedAt);
    if (runsQuery.isFetched) times.push(runsQuery.dataUpdatedAt);
    return times.length > 0 ? Math.max(...times) : null;
  })();

  return (
    <section aria-label="Workflow definitions">
      <PageHeader className="mb-6">
        <PageHeaderContent size="md">
          <PageHeaderTitle>Definitions</PageHeaderTitle>
          <PageHeaderDescription>
            Workflow definitions configured for this app, including triggers and
            run-as identity.
          </PageHeaderDescription>
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
            onClick={refresh}
            disabled={refreshing}
          >
            {refreshing ? "Refreshing…" : "Refresh"}
          </Button>
        </PageHeaderActions>
      </PageHeader>

      {definitionsError && !showingObserved ? (
        <ErrorNotice
          message={definitionsError}
          onRetry={refresh}
          retrying={refreshing}
        />
      ) : null}

      {showingObserved ? (
        <Alert variant="info" className="mb-6">
          <Info aria-hidden />
          <AlertDescription>
            Showing definitions observed in recent runs. Inventory may be
            incomplete.
          </AlertDescription>
        </Alert>
      ) : null}

      {definitionsQuery.isPending && !showingObserved ? (
        <p className="text-sm text-muted-foreground/70">
          Loading workflow definitions…
        </p>
      ) : null}

      {!definitionsQuery.isPending || showingObserved ? (
        <DefinitionsList
          appName={app}
          apiDefinitions={useObservedFallback ? [] : apiDefinitions}
          observedDefinitions={useObservedFallback ? observedDefinitions : []}
        />
      ) : null}
    </section>
  );
}

function DefinitionsList({
  appName,
  apiDefinitions,
  observedDefinitions,
}: {
  appName: string;
  apiDefinitions: WorkflowDefinition[];
  observedDefinitions: WorkflowDefinitionActivity[];
}) {
  if (apiDefinitions.length === 0 && observedDefinitions.length === 0) {
    return (
      <p className="text-sm text-muted-foreground/70">
        No workflow definitions found for this app yet.
      </p>
    );
  }

  return (
    <ul
      className="divide-y divide-border rounded-lg border border-border"
      data-testid="app-workflow-definitions-list"
    >
      {apiDefinitions.map((definition) => (
        <li key={definition.id}>
          <ApiDefinitionRow appName={appName} definition={definition} />
        </li>
      ))}
      {observedDefinitions.map((item) => (
        <li key={`observed-${item.definitionId}`}>
          <ObservedDefinitionRow appName={appName} item={item} />
        </li>
      ))}
    </ul>
  );
}

function ApiDefinitionRow({
  appName,
  definition,
}: {
  appName: string;
  definition: WorkflowDefinition;
}) {
  return (
    <div className="flex flex-col gap-3 px-4 py-3 sm:flex-row sm:items-start sm:justify-between">
      <div className="min-w-0 flex-1">
        <Link
          to="/apps/$app/admin/workflows/definitions/$definitionId"
          params={{ app: appName, definitionId: definition.id }}
          className={cn(
            "block truncate font-mono text-sm text-foreground underline-offset-2 hover:underline focus-ring-inset",
            listItemInteraction({ pointer: "css" }),
          )}
        >
          {definition.id}
        </Link>
        <p className="mt-1 text-xs text-muted-foreground">
          {definition.activations.length} activation
          {definition.activations.length === 1 ? "" : "s"}
          {definition.runAs ? ` · run as ${definition.runAs}` : ""}
          {definition.updatedAt
            ? ` · updated ${formatDate(definition.updatedAt)}`
            : ""}
        </p>
      </div>
      <div className="flex flex-wrap items-center gap-2">
        {definition.paused ? (
          <Badge size="sm" variant="warning">
            Paused
          </Badge>
        ) : null}
        <Button asChild variant="outline" size="sm">
          <Link
            to="/apps/$app/admin/workflows"
            params={{ app: appName }}
            search={{ definition: definition.id }}
          >
            View runs
          </Link>
        </Button>
      </div>
    </div>
  );
}

function ObservedDefinitionRow({
  appName,
  item,
}: {
  appName: string;
  item: WorkflowDefinitionActivity;
}) {
  return (
    <div className="flex flex-col gap-3 px-4 py-3 sm:flex-row sm:items-start sm:justify-between">
      <div className="min-w-0 flex-1">
        <Link
          to="/apps/$app/admin/workflows/definitions/$definitionId"
          params={{ app: appName, definitionId: item.definitionId }}
          className={cn(
            "block truncate font-mono text-sm text-foreground underline-offset-2 hover:underline focus-ring-inset",
            listItemInteraction({ pointer: "css" }),
          )}
        >
          {item.definitionId}
        </Link>
        <p className="mt-1 text-xs text-muted-foreground">
          {item.runCount} run{item.runCount === 1 ? "" : "s"}
          {item.lastCreatedAt
            ? ` · last ${formatDate(item.lastCreatedAt)}`
            : ""}
          {item.lastStatus ? (
            <>
              {" "}
              · Last run:{" "}
              <WorkflowStatusBadge status={item.lastStatus} />
            </>
          ) : null}
        </p>
      </div>
      <Button asChild variant="outline" size="sm">
        <Link
          to="/apps/$app/admin/workflows"
          params={{ app: appName }}
          search={{ definition: item.definitionId }}
        >
          View runs
        </Link>
      </Button>
    </div>
  );
}
