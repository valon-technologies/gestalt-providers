import * as React from "react";
import { Link } from "@tanstack/react-router";
import { hotkeysCoreFeature, syncDataLoaderFeature } from "@headless-tree/core";
import { useTree } from "@headless-tree/react";

import {
  Tree,
  TreeItem,
  TreeItemLabel,
  TREE_INDENT_BY_SIZE,
} from "@/components/ui/tree";
import {
  formatDuration,
  indexWorkflowRunGraphTree,
  type WorkflowRunGraph,
  type WorkflowRunGraphTreeItem,
} from "./workflow-run-graph";
import { WorkflowStatusIcon } from "./workflow-status-icon";

const ROOT_ID = "__workflow-run-graph-root__";

export function WorkflowRunJobGraph({
  appName,
  runId,
  graph,
}: {
  appName: string;
  runId: string;
  graph: WorkflowRunGraph;
}) {
  const { rootIds, items } = React.useMemo(
    () => indexWorkflowRunGraphTree(graph),
    [graph],
  );

  const loaderItems = React.useMemo(
    (): Record<string, WorkflowRunGraphTreeItem> => ({
      [ROOT_ID]: {
        id: ROOT_ID,
        kind: "job",
        name: "",
        jobId: "",
        children: rootIds,
      },
      ...items,
    }),
    [items, rootIds],
  );

  const initialExpandedItems = React.useMemo(
    () => [...rootIds],
    // Headless Tree reads expandedItems only from initialState on mount.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [],
  );

  const tree = useTree<WorkflowRunGraphTreeItem>({
    initialState: { expandedItems: initialExpandedItems },
    indent: TREE_INDENT_BY_SIZE.default,
    rootItemId: ROOT_ID,
    getItemName: (item) => item.getItemData()?.name ?? "",
    isItemFolder: (item) => item.getItemData()?.kind === "job",
    dataLoader: {
      getItem: (itemId) => loaderItems[itemId]!,
      getChildren: (itemId) => loaderItems[itemId]?.children ?? [],
    },
    features: [syncDataLoaderFeature, hotkeysCoreFeature],
  });

  React.useLayoutEffect(() => {
    tree.rebuildTree();
  }, [loaderItems, tree]);

  return (
    <Tree
      indent={TREE_INDENT_BY_SIZE.default}
      expandActivation="toggle"
      showIndentGuides
      tree={tree}
      toggleIconType="plus-minus"
      data-testid="workflow-run-job-graph"
      aria-label="Run graph"
    >
      {tree
        .getItems()
        .filter((item) => item.getId() !== ROOT_ID)
        .map((item) => {
          const data = loaderItems[item.getId()];
          if (!data) return null;
          return (
            <TreeItem key={item.getId()} item={item}>
              <GraphTreeItemLabel
                data={data}
                appName={appName}
                runId={runId}
              />
            </TreeItem>
          );
        })}
    </Tree>
  );
}

function GraphTreeItemLabel({
  data,
  appName,
  runId,
}: {
  data: WorkflowRunGraphTreeItem;
  appName: string;
  runId: string;
}) {
  const body = (
    <>
      <WorkflowStatusIcon
        status={data.status}
        size={data.kind === "job" ? "md" : "sm"}
      />
      <span
        className={
          data.kind === "job"
            ? "min-w-0 flex-1 truncate font-medium text-foreground"
            : "min-w-0 flex-1 truncate text-foreground/90"
        }
      >
        {data.name}
      </span>
      <span className="shrink-0 text-xs text-muted-foreground tabular-nums">
        {formatDuration(data.durationMs)}
      </span>
    </>
  );

  if (data.kind === "step" && data.stepId) {
    return (
      <TreeItemLabel>
        <Link
          to="/apps/$app/admin/workflows/runs/$runId/jobs/$jobId/steps/$stepId"
          params={{
            app: appName,
            runId,
            jobId: data.jobId,
            stepId: data.stepId,
          }}
          className="flex min-w-0 flex-1 items-center gap-1.5"
          data-testid={`workflow-step-${data.stepId}`}
          data-row-link=""
        >
          {body}
        </Link>
      </TreeItemLabel>
    );
  }

  return <TreeItemLabel>{body}</TreeItemLabel>;
}
