import { useMemo, useState } from "react";
import {
  createColumnHelper,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type SortingState,
} from "@tanstack/react-table";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  DataTableColumnHeader,
  DataTableSearchShell,
  DataTableView,
} from "@/components/ui/data-table";
import { SearchHighlight } from "@/components/ui/search-highlight";
import { isActiveRegistryRollout } from "@/features/registry/format";
import {
  isRolloutDeployingAction,
  selectedVersionRowAffordance,
} from "@/features/registry/rollout-stepper";
import {
  PublicationPullRequestLabel,
  REGISTRY_TABLE_LINK_CLASS,
} from "@/features/registry/publication-pull-request-label";
import { RegistryCode } from "@/features/registry/registry-code";
import {
  buildAppAdminSnapshotRows,
  snapshotFailedReason,
  snapshotLastUpdatedAt,
  snapshotLastUpdatedLabel,
  snapshotStatusTimer,
} from "@/features/registry/snapshot-rows";
import type {
  AppAdminPublication,
  AppAdminRegistryResponse,
  AppAdminSnapshotRow,
  RegistryAppSummary,
} from "@/features/registry/types";
import { useLiveNow } from "@/hooks/use-live-now";
import { textContainsAllSearchTokens } from "@/lib/search-highlight";
import { cn } from "@/lib/cn";
import { Loader2 } from "lucide-react";

const PUBLISH_DURATION_CLASS =
  "inline-block min-w-[4.75rem] tabular-nums text-muted-foreground";

const columnHelper = createColumnHelper<AppAdminSnapshotRow>();

function shortenSnapshotVersion(version: string): string {
  const trimmed = version.trim();
  if (trimmed.length <= 24) return trimmed;
  return `${trimmed.slice(0, 20)}…`;
}

function rowPublication(row: AppAdminSnapshotRow): AppAdminPublication | undefined {
  if (row.kind === "published") return row.published.publication;
  if (row.kind === "pending") return row.pending.publication;
  return row.failed.publication;
}

function snapshotRowSearchText(
  row: AppAdminSnapshotRow,
  registry: SnapshotTableMeta["registry"],
): string {
  const publication = rowPublication(row);
  const pullRequest = publication?.triggerPullRequest;
  const status = snapshotStatus({
    row,
    desiredVersion: registry.desiredVersion,
    rollout: registry.rollout,
    autoDeployPendingVersion: registry.autoDeploy?.pendingVersion,
  });
  const lastUpdated = snapshotLastUpdatedLabel(row);
  return [
    row.version,
    status.label,
    pullRequest?.number ? `PR #${pullRequest.number}` : null,
    pullRequest?.title,
    publication?.workflowRunUrl ? "workflow" : null,
    snapshotFailedReason(row),
    snapshotStatusTimer(row),
    lastUpdated?.relative,
    lastUpdated?.absolute,
  ]
    .filter(Boolean)
    .join(" ");
}

function snapshotStatus({
  row,
  desiredVersion,
  rollout,
  autoDeployPendingVersion,
}: {
  row: AppAdminSnapshotRow;
  desiredVersion?: string;
  rollout?: RegistryAppSummary["rollout"];
  autoDeployPendingVersion?: string;
}): { label: string; variant: "success" | "warning" | "info" | "destructive" } {
  if (row.kind === "pending") {
    return { label: "Publishing", variant: "warning" };
  }
  if (row.kind === "failed") {
    return { label: "Failed", variant: "destructive" };
  }
  if (
    autoDeployPendingVersion === row.version &&
    !isRolloutDeployingAction(rollout, row.version)
  ) {
    return { label: "Queued for deploy", variant: "warning" };
  }
  const rolloutTargetActive =
    rollout &&
    rollout.version === row.version &&
    isActiveRegistryRollout(rollout.state);
  if (row.version === desiredVersion && !rolloutTargetActive) {
    return { label: "Deployed", variant: "success" };
  }
  if (rollout && rollout.version === row.version && isActiveRegistryRollout(rollout.state)) {
    return { label: "Rolling out", variant: "warning" };
  }
  return { label: "Available", variant: "info" };
}

function PendingPublishDuration({ statusTimer }: { statusTimer: string }) {
  const duration = statusTimer.replace(/^for\s+/, "");
  return (
    <span className="text-muted-foreground">
      for <span className={PUBLISH_DURATION_CLASS}>{duration}</span>
    </span>
  );
}

function selectedRowClassName(
  rollout: RegistryAppSummary["rollout"],
  rowVersion: string,
): string {
  const affordance = selectedVersionRowAffordance(rollout, rowVersion);
  return cn(
    affordance === "pulsing" && "bg-primary/10 motion-safe:animate-pulse",
    affordance === "success" && "bg-success/10",
    affordance === "error" && "bg-destructive/10",
  );
}

type SnapshotTableMeta = {
  registry: Pick<
    AppAdminRegistryResponse,
    | "publishedVersions"
    | "pendingVersions"
    | "failedVersions"
    | "desiredVersion"
    | "rollout"
    | "selectionDisabled"
    | "autoDeploy"
  >;
  controlsDisabled: boolean;
  deployingVersion: string | null;
  onDeployVersion: (version: string) => void;
};

type SnapshotTableRuntimeMeta = {
  liveNow: number;
};

function SnapshotPullRequestCell({
  row,
  registry,
}: {
  row: AppAdminSnapshotRow;
  registry: SnapshotTableMeta["registry"];
}) {
  const publication = rowPublication(row);
  const pullRequest = publication?.triggerPullRequest;
  const rowAffordance =
    row.kind !== "pending"
      ? selectedVersionRowAffordance(registry.rollout, row.version)
      : undefined;

  return (
    <div className="relative">
      {rowAffordance ? (
        <span
          className="pointer-events-none absolute -left-7 top-1/2 flex w-7 -translate-y-1/2 items-center justify-center text-primary leading-none"
          aria-hidden="true"
          data-testid="snapshot-row-arrow"
        >
          →
        </span>
      ) : null}
      {pullRequest?.number ? (
        <PublicationPullRequestLabel pullRequest={pullRequest} />
      ) : publication?.workflowRunUrl ? (
        <a
          href={publication.workflowRunUrl}
          target="_blank"
          rel="noreferrer"
          className={REGISTRY_TABLE_LINK_CLASS}
        >
          <SearchHighlight text="workflow" />
        </a>
      ) : (
        <span className="text-muted-foreground">—</span>
      )}
    </div>
  );
}

function SnapshotStatusCell({
  row,
  registry,
  liveNow,
}: {
  row: AppAdminSnapshotRow;
  registry: SnapshotTableMeta["registry"];
  liveNow?: number;
}) {
  const status = snapshotStatus({
    row,
    desiredVersion: registry.desiredVersion,
    rollout: registry.rollout,
    autoDeployPendingVersion: registry.autoDeploy?.pendingVersion,
  });
  const statusTimer = snapshotStatusTimer(
    row,
    row.kind === "pending" ? liveNow : undefined,
  );
  const failedReason = snapshotFailedReason(row);

  return (
    <div className="flex flex-col gap-1">
      <div className="flex flex-wrap items-center gap-2">
        <Badge variant={status.variant} data-testid="snapshot-status" className="relative">
          {row.kind === "pending" ? (
            <Loader2
              className="absolute top-1/2 right-full mr-1.5 size-3.5 -translate-y-1/2 animate-spin text-warning-foreground"
              aria-hidden="true"
              data-testid="snapshot-status-spinner"
            />
          ) : null}
          <SearchHighlight text={status.label} />
        </Badge>
        {row.kind === "pending" && statusTimer ? (
          <PendingPublishDuration statusTimer={statusTimer} />
        ) : null}
      </div>
      {statusTimer && row.kind !== "pending" ? (
        <div className="text-xs text-muted-foreground">
          <SearchHighlight text={statusTimer} />
        </div>
      ) : null}
      {failedReason ? (
        <div className="text-xs text-muted-foreground">
          <SearchHighlight text={failedReason} />
        </div>
      ) : null}
    </div>
  );
}

function SnapshotLastUpdateCell({
  row,
  liveNow,
}: {
  row: AppAdminSnapshotRow;
  liveNow?: number;
}) {
  const lastUpdated = snapshotLastUpdatedLabel(row, {
    now: row.kind === "pending" ? liveNow : undefined,
    minRelativeUnit: row.kind === "pending" ? "minute" : undefined,
  });

  if (!lastUpdated) {
    return <span className="text-muted-foreground">—</span>;
  }

  return (
    <time
      className="text-muted-foreground"
      dateTime={snapshotLastUpdatedAt(row) ?? undefined}
      title={lastUpdated.absolute}
      data-testid="snapshot-last-updated-at"
    >
      <SearchHighlight text={lastUpdated.relative} />
    </time>
  );
}

function SnapshotActionCell({
  row,
  registry,
  controlsDisabled,
  deployingVersion,
  onDeployVersion,
}: {
  row: AppAdminSnapshotRow;
  registry: SnapshotTableMeta["registry"];
  controlsDisabled: boolean;
  deployingVersion: string | null;
  onDeployVersion: (version: string) => void;
}) {
  const isDeployable = row.kind === "published";
  const isDeploying = deployingVersion === row.version;
  const autoDeployEnabled = registry.autoDeploy?.enabled ?? false;
  const deployDisabled =
    !isDeployable ||
    controlsDisabled ||
    autoDeployEnabled ||
    isDeploying ||
    row.version === registry.desiredVersion;
  const showRolloutDeploying = isRolloutDeployingAction(registry.rollout, row.version);

  if (showRolloutDeploying) {
    return (
      <Button
        type="button"
        size="sm"
        data-testid={`deploy-version-${row.version}`}
        disabled
      >
        Deploying...
      </Button>
    );
  }

  if (isDeployable && row.version === registry.desiredVersion) {
    return <span className="text-muted-foreground">—</span>;
  }

  if (isDeployable) {
    return (
      <Button
        type="button"
        size="sm"
        data-testid={`deploy-version-${row.version}`}
        disabled={deployDisabled}
        onClick={() => onDeployVersion(row.version)}
      >
        {isDeploying ? "Deploying..." : "Deploy"}
      </Button>
    );
  }

  return <span className="text-muted-foreground">—</span>;
}

export function AppAdminSnapshotsTable({
  registry,
  controlsDisabled,
  deployingVersion,
  onDeployVersion,
}: SnapshotTableMeta) {
  const rows = useMemo(() => buildAppAdminSnapshotRows(registry), [registry]);
  const hasPendingRows = rows.some((row) => row.kind === "pending");
  const liveNow = useLiveNow({ enabled: hasPendingRows });
  const [sorting, setSorting] = useState<SortingState>([
    { id: "sortAt", desc: true },
  ]);
  const [search, setSearch] = useState("");
  const filteredRows = useMemo(() => {
    const query = search.trim();
    if (!query) return rows;
    return rows.filter((row) =>
      textContainsAllSearchTokens(snapshotRowSearchText(row, registry), query),
    );
  }, [rows, registry, search]);

  const columns = useMemo(
    () => [
      columnHelper.accessor(
        (row) => rowPublication(row)?.triggerPullRequest?.number ?? 0,
        {
          id: "pullRequest",
          header: ({ column }) => (
            <DataTableColumnHeader column={column} title="Pull request" />
          ),
          cell: ({ row }) => (
            <SnapshotPullRequestCell row={row.original} registry={registry} />
          ),
        },
      ),
      columnHelper.accessor((row) => row.version, {
        id: "snapshot",
        header: ({ column }) => (
          <DataTableColumnHeader column={column} title="Snapshot" />
        ),
        cell: ({ row }) => (
          <RegistryCode title={row.original.version}>
            <SearchHighlight text={shortenSnapshotVersion(row.original.version)} />
          </RegistryCode>
        ),
      }),
      columnHelper.accessor(
        (row) =>
          snapshotStatus({
            row,
            desiredVersion: registry.desiredVersion,
            rollout: registry.rollout,
            autoDeployPendingVersion: registry.autoDeploy?.pendingVersion,
          }).label,
        {
          id: "status",
          header: ({ column }) => (
            <DataTableColumnHeader column={column} title="Status" />
          ),
          cell: ({ row, table }) => (
            <SnapshotStatusCell
              row={row.original}
              registry={registry}
              liveNow={
                row.original.kind === "pending"
                  ? (table.options.meta as SnapshotTableRuntimeMeta).liveNow
                  : undefined
              }
            />
          ),
          enableSorting: false,
        },
      ),
      columnHelper.accessor((row) => row.sortAt, {
        id: "sortAt",
        header: ({ column }) => (
          <DataTableColumnHeader column={column} title="Last update" />
        ),
        cell: ({ row, table }) => (
          <SnapshotLastUpdateCell
            row={row.original}
            liveNow={
              row.original.kind === "pending"
                ? (table.options.meta as SnapshotTableRuntimeMeta).liveNow
                : undefined
            }
          />
        ),
        sortingFn: "datetime",
      }),
      columnHelper.display({
        id: "action",
        header: "Action",
        meta: { align: "end" },
        enableSorting: false,
        cell: ({ row }) => (
          <SnapshotActionCell
            row={row.original}
            registry={registry}
            controlsDisabled={controlsDisabled}
            deployingVersion={deployingVersion}
            onDeployVersion={onDeployVersion}
          />
        ),
      }),
    ],
    [registry, controlsDisabled, deployingVersion, onDeployVersion],
  );

  const table = useReactTable({
    data: filteredRows,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getRowId: (row) => `${row.kind}:${row.version}`,
    autoResetPageIndex: false,
    meta: { liveNow },
  });

  if (rows.length === 0) {
    return (
      <p className="text-sm text-muted-foreground">No published versions are available.</p>
    );
  }

  return (
    <DataTableSearchShell
      search={search}
      onSearchChange={setSearch}
      searchPlaceholder="Search snapshots"
    >
      <DataTableView
        table={table}
        testId="snapshots-table"
        emptyMessage={search.trim() ? "No results." : "No results."}
        getRowProps={(row) => {
          const affordance = selectedVersionRowAffordance(
            registry.rollout,
            row.original.version,
          );
          return {
            "data-testid": `snapshot-row-${row.original.kind}`,
            ...(affordance ? { "data-selected-version-row": "true" } : {}),
            className: selectedRowClassName(registry.rollout, row.original.version),
          };
        }}
      />
    </DataTableSearchShell>
  );
}
