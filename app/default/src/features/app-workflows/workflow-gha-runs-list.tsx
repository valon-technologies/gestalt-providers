import { Fragment } from "react";
import { Link } from "@tanstack/react-router";
import { Calendar, Timer } from "lucide-react";
import type { WorkflowRun } from "@/lib/api";
import { cn } from "@/lib/cn";
import { listItemInteraction } from "@/lib/list-item-interaction";
import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
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
  formatDuration,
  formatRelativeTime,
  projectWorkflowRunGraph,
} from "./workflow-run-graph";
import { WorkflowStatusIcon } from "./workflow-status-icon";
import {
  runTriggerLabel,
  shortDefinitionId,
  shortRunId,
  targetLabel,
} from "./workflow-format";

/** Flush navigable rows — Card radius on first/last Item (application-lists.md). */
const navigableRowClassName = cn(
  "rounded-none px-4 first:rounded-t-xl last:rounded-b-xl",
  listItemInteraction({ pointer: "css" }),
);

export function WorkflowGhaRunsList({
  runs,
  appName,
}: {
  runs: WorkflowRun[];
  appName: string;
}) {
  return (
    <Card variant="outline" data-testid="app-workflow-run-list">
      <ItemGroup>
        {runs.map((run, index) => (
          <Fragment key={run.id}>
            {index > 0 ? <ItemSeparator /> : null}
            <WorkflowGhaRunRow run={run} appName={appName} />
          </Fragment>
        ))}
      </ItemGroup>
    </Card>
  );
}

function WorkflowGhaRunRow({
  run,
  appName,
}: {
  run: WorkflowRun;
  appName: string;
}) {
  const graph = projectWorkflowRunGraph(run);
  const title =
    targetLabel(run.target) || run.definitionId || shortRunId(run.id);
  const actor = run.createdBy?.subjectId?.replace(/^[^:]+:/, "") || "system";
  const when = run.completedAt || run.startedAt || run.createdAt;

  return (
    <Item
      role="listitem"
      size="sm"
      className={navigableRowClassName}
      asChild
    >
      <Link
        to="/apps/$app/admin/workflows/runs/$runId"
        params={{ app: appName, runId: run.id }}
        data-testid={`app-workflow-run-${run.id}`}
      >
        <ItemMedia>
          <WorkflowStatusIcon status={run.status} title={run.status} />
        </ItemMedia>
        <ItemContent className="min-w-0 gap-1">
          <ItemTitle className="max-w-full truncate">{title}</ItemTitle>
          <ItemDescription className="flex flex-wrap items-center gap-2">
            <span className="font-mono text-xs">{shortRunId(run.id)}</span>
            <span>
              {runTriggerLabel(run)} by {actor}
            </span>
            {run.definitionId ? (
              <Badge
                variant="secondary"
                className="max-w-[14rem] truncate font-mono"
              >
                {shortDefinitionId(run.definitionId)}
              </Badge>
            ) : null}
          </ItemDescription>
        </ItemContent>
        <ItemActions className="text-xs text-muted-foreground">
          <span className="inline-flex items-center gap-1">
            <Calendar className="size-3.5" aria-hidden />
            {formatRelativeTime(when)}
          </span>
          <span className="inline-flex items-center gap-1">
            <Timer className="size-3.5" aria-hidden />
            {formatDuration(graph.durationMs)}
          </span>
        </ItemActions>
      </Link>
    </Item>
  );
}
