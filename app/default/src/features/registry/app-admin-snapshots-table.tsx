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
import { CopyableCode } from "@/components/ui/copyable-code";
import {
  DataTableColumnHeader,
  DataTableSearchShell,
  DataTableView,
} from "@/components/ui/data-table";
import { SearchHighlight } from "@/components/ui/search-highlight";
import {
  TableStatusIndicator,
} from "@/components/ui/table-status-indicator";
import { isActiveRegistryRollout } from "@/features/registry/format";
import { RolloutPhaseStepper } from "@/features/registry/rollout-phase-stepper";
import {
  isRolloutDeployingAction,
  rolloutProgressSubline,
  selectedVersionRowAffordance,
  shouldShowRowRolloutStepper,
} from "@/features/registry/rollout-stepper";
import {
  PublicationPullRequestLabel,
  REGISTRY_TABLE_LINK_CLASS,
} from "@/features/registry/publication-pull-request-label";
import {
  resolveSnapshotRowStatus,
  snapshotRowStatusPresentation,
} from "@/features/registry/snapshot-row-status";
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

/** Registry DataTable row actions use outline — bordered + page background, not secondary (secondary === neutral-hover in theme). */
const TABLE_ROW_ACTION_BUTTON_VARIANT = "outline" as const;

/** Gutter width = px-3 + size-5 indicator + px-3 (12px + 20px + 12px). */
const SNAPSHOT_SEVERITY_GUTTER_CLASS = "w-11 px-3";

const columnHelper = createColumnHelper<AppAdminSnapshotRow>();

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
  const statusId = resolveSnapshotRowStatus({
    row,
    desiredVersion: registry.desiredVersion,
    rollout: registry.rollout,
    autoDeployPendingVersion: registry.autoDeploy?.pendingVersion,
  });
  const status = snapshotRowStatusPresentation(statusId);
  const rolloutPhase =
    registry.rollout && registry.rollout.version === row.version
      ? rolloutProgressSubline(registry.rollout)
      : null;
  const lastUpdated = snapshotLastUpdatedLabel(row);
  return [
    row.version,
    status.label,
    rolloutPhase,
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

function snapshotRowStatusInput(
  row: AppAdminSnapshotRow,
  registry: SnapshotTableMeta["registry"],
) {
  return {
    row,
    desiredVersion: registry.desiredVersion,
    rollout: registry.rollout,
    autoDeployPendingVersion: registry.autoDeploy?.pendingVersion,
  };
}

function snapshotStatusSortKey(
  row: AppAdminSnapshotRow,
  registry: SnapshotTableMeta["registry"],
): number {
  const statusId = resolveSnapshotRowStatus(snapshotRowStatusInput(row, registry));
  return snapshotRowStatusPresentation(statusId).sortOrder;
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

function SnapshotSeverityCell({
  row,
  registry,
}: {
  row: AppAdminSnapshotRow;
  registry: SnapshotTableMeta["registry"];
}) {
  const statusId = resolveSnapshotRowStatus(snapshotRowStatusInput(row, registry));
  const status = snapshotRowStatusPresentation(statusId);

  return (
    <div className="flex h-5 items-center justify-center">
      <TableStatusIndicator
        variant={status.indicatorVariant}
        iconOnly
        label={status.label}
      />
    </div>
  );
}

function SnapshotPullRequestCell({
  row,
  registry,
}: {
  row: AppAdminSnapshotRow;
  registry: SnapshotTableMeta["registry"];
}) {
  const publication = rowPublication(row);
  const pullRequest = publication?.triggerPullRequest;
  const rollout = registry.rollout;
  const showRolloutStepper =
    row.kind === "published" &&
    rollout &&
    shouldShowRowRolloutStepper(rollout, row.version);

  return (
    <div className="flex min-w-0 flex-col gap-2">
      <div className="flex min-w-0 flex-col gap-2">
        <div className="flex min-h-5 items-center">
          {pullRequest?.number ? (
            <PublicationPullRequestLabel
              pullRequest={pullRequest}
              titleClassName="font-medium text-foreground"
            />
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
        <CopyableCode
          value={row.version}
          className="w-fit max-w-full text-xs [&_code]:text-xs"
          tooltip={`Copy ${row.version}`}
        >
          <SearchHighlight text={row.version} />
        </CopyableCode>
      </div>
      {showRolloutStepper ? (
        <RolloutPhaseStepper rollout={rollout} size="mini" className="self-start" />
      ) : null}
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
  const rollout = registry.rollout;
  const statusId = resolveSnapshotRowStatus(snapshotRowStatusInput(row, registry));
  const status = snapshotRowStatusPresentation(statusId);
  const statusTimer = snapshotStatusTimer(
    row,
    row.kind === "pending" ? liveNow : undefined,
  );
  const failedReason = snapshotFailedReason(row);
  const showRolloutStepper =
    row.kind === "published" &&
    rollout &&
    shouldShowRowRolloutStepper(rollout, row.version);
  const rolloutSubline =
    rollout && showRolloutStepper && isActiveRegistryRollout(rollout.state)
      ? rolloutProgressSubline(rollout, liveNow)
      : null;

  return (
    <div className="flex flex-col gap-1">
      <div className="flex flex-wrap items-center gap-2">
        <Badge
          variant={status.badgeVariant}
          data-testid="snapshot-status"
          className="relative"
        >
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
      {rolloutSubline ? (
        <div className="text-xs text-muted-foreground">
          <SearchHighlight text={rolloutSubline} />
        </div>
      ) : statusTimer && row.kind !== "pending" ? (
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
      className="text-xs text-muted-foreground"
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
      <span className="inline-block align-baseline">
        <Button
          type="button"
          variant={TABLE_ROW_ACTION_BUTTON_VARIANT}
          size="sm"
          data-testid={`deploy-version-${row.version}`}
          disabled
        >
          Deploying...
        </Button>
      </span>
    );
  }

  if (isDeployable && row.version === registry.desiredVersion) {
    return <span className="text-muted-foreground">—</span>;
  }

  if (isDeployable) {
    return (
      <span className="inline-block align-baseline">
        <Button
          type="button"
          variant={TABLE_ROW_ACTION_BUTTON_VARIANT}
          size="sm"
          data-testid={`deploy-version-${row.version}`}
          disabled={deployDisabled}
          onClick={() => onDeployVersion(row.version)}
        >
          {isDeploying ? "Deploying..." : "Deploy"}
        </Button>
      </span>
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
  const hasActiveRollout = Boolean(
    registry.rollout && isActiveRegistryRollout(registry.rollout.state),
  );
  const liveNow = useLiveNow({ enabled: hasPendingRows || hasActiveRollout });
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
      columnHelper.display({
        id: "severity",
        header: () => <span className="sr-only">Status</span>,
        meta: {
          headerClassName: SNAPSHOT_SEVERITY_GUTTER_CLASS,
          className: cn(SNAPSHOT_SEVERITY_GUTTER_CLASS, "align-top"),
        },
        enableSorting: false,
        cell: ({ row }) => (
          <SnapshotSeverityCell row={row.original} registry={registry} />
        ),
      }),
      columnHelper.accessor(
        (row) => rowPublication(row)?.triggerPullRequest?.number ?? 0,
        {
          id: "pullRequest",
          meta: { className: "align-top pl-0", headerClassName: "pl-0" },
          header: ({ column }) => (
            <DataTableColumnHeader column={column} title="Pull request" />
          ),
          cell: ({ row }) => (
            <SnapshotPullRequestCell row={row.original} registry={registry} />
          ),
        },
      ),
      columnHelper.accessor(
        (row) => snapshotStatusSortKey(row, registry),
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
                row.original.kind === "pending" ||
                (registry.rollout &&
                  row.original.version === registry.rollout.version &&
                  isActiveRegistryRollout(registry.rollout.state))
                  ? (table.options.meta as SnapshotTableRuntimeMeta).liveNow
                  : undefined
              }
            />
          ),
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
