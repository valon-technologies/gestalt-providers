import { memo, useMemo, useRef, useState, type ReactNode } from "react";
import {
  createColumnHelper,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type SortingState,
} from "@tanstack/react-table";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { CopyableCode } from "@/components/ui/copyable-code";
import {
  DataTableColumnHeader,
  DataTableRegistryCell,
  DataTableRegistryPrimaryLine,
  DataTableRegistrySecondaryLine,
  DataTableSearchShell,
  DataTableView,
} from "@/components/ui/data-table";
import { SearchHighlight } from "@/components/ui/search-highlight";
import {
  TableStatusIndicator,
} from "@/components/ui/table-status-indicator";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  rolloutKey,
  snapshotRegistryPollEqual,
} from "@/features/registry/stable-snapshot-registry";
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
  isFailedRolloutRetryRow,
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
import {
  SnapshotRowLiveReplicas,
} from "@/features/registry/snapshot-live-replicas";
import {
  fleetReplicasLivenessKey,
  fleetReplicasPollKey,
  partitionFleetReplicasForSnapshotTable,
  replicaClassLabel,
  shortInstanceId,
  type FleetReplicasForSnapshotTable,
} from "@/features/registry/fleet-replicas";
import type {
  AppAdminFleetReplica,
  AppAdminPublication,
  AppAdminRegistryResponse,
  AppAdminRegistryRevision,
  AppAdminSnapshotRow,
} from "@/features/registry/types";
import { useLiveNow } from "@/hooks/use-live-now";
import { textContainsAllSearchTokens } from "@/lib/search-highlight";
import { Loader2 } from "lucide-react";

const PUBLISH_DURATION_CLASS =
  "inline-block min-w-[4.75rem] tabular-nums text-muted-foreground";

/** Registry DataTable row actions use outline — bordered + page background, not secondary (secondary === neutral-hover in theme). */
const TABLE_ROW_ACTION_BUTTON_VARIANT = "outline" as const;

const columnHelper = createColumnHelper<AppAdminSnapshotRow>();

function rowPublication(row: AppAdminSnapshotRow): AppAdminPublication | undefined {
  if (row.kind === "published") return row.published.publication;
  if (row.kind === "pending") return row.pending.publication;
  return row.failed.publication;
}

function snapshotRowSearchText(
  row: AppAdminSnapshotRow,
  registry: SnapshotTableMeta["registry"],
  deployedByByVersion: Map<string, string>,
  replicasForRow: AppAdminFleetReplica[] | undefined,
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
  const isDeployedVersion = statusId === "current";
  const rolloutPhase =
    registry.rollout && registry.rollout.version === row.version
      ? rolloutProgressSubline(registry.rollout)
      : null;
  const lastUpdated = snapshotLastUpdatedLabel(row);
  const replicaSearch = (replicasForRow ?? []).flatMap((replica) => [
    shortInstanceId(replica.instanceId),
    replica.instanceId,
    replicaClassLabel(replica.class),
    replica.lastError,
  ]);
  return [
    row.version,
    status.label,
    isDeployedVersion ? status.label : null,
    rolloutPhase,
    pullRequest?.number ? `PR #${pullRequest.number}` : null,
    pullRequest?.title,
    publication?.workflowRunUrl ? "View workflow run" : null,
    snapshotFailedReason(row),
    snapshotStatusTimer(row),
    deployedByByVersion.get(row.version),
    lastUpdated?.relative,
    lastUpdated?.absolute,
    ...replicaSearch,
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

function deployedByDisplayLabel(actor?: string): string | null {
  const trimmed = actor?.trim();
  if (!trimmed) return null;
  return trimmed.replace(/^user:/i, "");
}

function deployedByInitials(actor: string): string {
  const emailLocal = actor.includes("@") ? actor.split("@")[0] : actor;
  const parts = emailLocal
    .replace(/[._-]+/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  if (parts.length >= 2) {
    return (parts[0][0] + parts[1][0]).toUpperCase();
  }
  return emailLocal.slice(0, 2).toUpperCase() || "?";
}

const SnapshotDeployedByCell = memo(function SnapshotDeployedByCell({
  actor,
}: {
  actor?: string;
}) {
  const displayLabel = deployedByDisplayLabel(actor);

  if (!displayLabel) {
    return (
      <DataTableRegistryPrimaryLine>
        <span className="text-muted-foreground">—</span>
      </DataTableRegistryPrimaryLine>
    );
  }

  if (displayLabel.toLowerCase().startsWith("system:")) {
    return (
      <DataTableRegistryPrimaryLine>
        <span data-no-row-click>
          <Badge
            variant="muted"
            data-testid="auto-deployed-badge"
          >
            Automatically deployed
          </Badge>
        </span>
      </DataTableRegistryPrimaryLine>
    );
  }

  return (
    <DataTableRegistryPrimaryLine>
      <Tooltip>
        <TooltipTrigger asChild>
          <span
            className="inline-flex size-8 shrink-0 overflow-hidden rounded-full"
            data-testid="deployed-by-avatar"
            role="img"
            aria-label={`Deployed by ${displayLabel}`}
          >
            <Avatar
              size="lg"
              variant="solid"
              aria-hidden="true"
              className="size-full max-h-full max-w-full text-xs leading-8"
            >
              <AvatarFallback>{deployedByInitials(displayLabel)}</AvatarFallback>
            </Avatar>
          </span>
        </TooltipTrigger>
        <TooltipContent side="top">{displayLabel}</TooltipContent>
      </Tooltip>
    </DataTableRegistryPrimaryLine>
  );
});

function deployedByForVersions(
  revisions: AppAdminRegistryRevision[],
): Map<string, string> {
  const deployedByByVersion = new Map<string, string>();
  for (const revision of revisions) {
    if (!deployedByByVersion.has(revision.version)) {
      deployedByByVersion.set(revision.version, revision.deployedBy ?? "");
    }
  }
  return deployedByByVersion;
}

/** Content key so history page-array identity churn does not remount the table. */
function historyRevisionsPollKey(
  revisions: AppAdminRegistryRevision[],
): string {
  if (!revisions.length) return "";
  return revisions
    .map((revision) => `${revision.version}\0${revision.deployedBy ?? ""}`)
    .join("\n");
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
    | "fleetState"
  >;
  historyRevisions: AppAdminRegistryRevision[];
  controlsDisabled: boolean;
  offerManualDeploy: boolean;
  emptyTitle: string;
  emptyHint: string;
  deployingVersion: string | null;
  onDeployVersion: (version: string) => void;
  toolbarTrailing?: ReactNode;
};

type SnapshotTableRuntimeMeta = {
  registry: SnapshotTableMeta["registry"];
  replicaPartition: FleetReplicasForSnapshotTable;
  deployedByByVersion: Map<string, string>;
};

function readSnapshotTableMeta(
  meta: unknown,
): SnapshotTableRuntimeMeta {
  return meta as SnapshotTableRuntimeMeta;
}

function snapshotRowKey(row: AppAdminSnapshotRow): string {
  return `${row.kind}:${row.version}`;
}

function snapshotActionRegistryInputs(registry: SnapshotTableMeta["registry"]) {
  return {
    desiredVersion: registry.desiredVersion,
    selectionDisabled: registry.selectionDisabled,
    autoDeployEnabled: registry.autoDeploy?.enabled ?? false,
    autoDeployPendingVersion: registry.autoDeploy?.pendingVersion,
    rolloutKey: rolloutKey(registry.rollout),
  };
}

type SnapshotActionCellProps = {
  row: AppAdminSnapshotRow;
  registry: SnapshotTableMeta["registry"];
  controlsDisabled: boolean;
  offerManualDeploy: boolean;
  deployingVersion: string | null;
  onDeployVersion: (version: string) => void;
};

function snapshotActionCellPropsAreEqual(
  prev: SnapshotActionCellProps,
  next: SnapshotActionCellProps,
): boolean {
  if (snapshotRowKey(prev.row) !== snapshotRowKey(next.row)) return false;
  if (prev.controlsDisabled !== next.controlsDisabled) return false;
  if (prev.offerManualDeploy !== next.offerManualDeploy) return false;
  if (prev.deployingVersion !== next.deployingVersion) return false;
  if (prev.onDeployVersion !== next.onDeployVersion) return false;

  const prevInputs = snapshotActionRegistryInputs(prev.registry);
  const nextInputs = snapshotActionRegistryInputs(next.registry);
  return (
    prevInputs.desiredVersion === nextInputs.desiredVersion &&
    prevInputs.selectionDisabled === nextInputs.selectionDisabled &&
    prevInputs.autoDeployEnabled === nextInputs.autoDeployEnabled &&
    prevInputs.autoDeployPendingVersion === nextInputs.autoDeployPendingVersion &&
    prevInputs.rolloutKey === nextInputs.rolloutKey
  );
}

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
    <DataTableRegistryPrimaryLine className="justify-center">
      <TableStatusIndicator
        variant={status.indicatorVariant}
        iconOnly
        label={status.label}
      />
    </DataTableRegistryPrimaryLine>
  );
}

const SnapshotPullRequestCell = memo(function SnapshotPullRequestCell({
  row,
  registry,
  liveReplicas,
}: {
  row: AppAdminSnapshotRow;
  registry: SnapshotTableMeta["registry"];
  liveReplicas: AppAdminFleetReplica[];
}) {
  const publication = rowPublication(row);
  const pullRequest = publication?.triggerPullRequest;
  const rollout = registry.rollout;
  const showRolloutStepper =
    row.kind === "published" &&
    rollout &&
    shouldShowRowRolloutStepper(rollout, row.version);

  return (
    <DataTableRegistryCell className="gap-2">
      <DataTableRegistryPrimaryLine>
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
            <SearchHighlight text="View workflow run" />
          </a>
        ) : (
          <span className="text-muted-foreground">—</span>
        )}
      </DataTableRegistryPrimaryLine>
      <CopyableCode
        value={row.version}
        className="w-fit max-w-full text-xs [&_code]:text-xs"
        tooltip={`Copy ${row.version}`}
      >
        <SearchHighlight text={row.version} />
      </CopyableCode>
      {showRolloutStepper ? (
        <RolloutPhaseStepper rollout={rollout} size="mini" className="self-start" />
      ) : null}
      <SnapshotRowLiveReplicas
        replicas={liveReplicas}
        heartbeatTtlSeconds={registry.fleetState?.heartbeatTtlSeconds}
      />
    </DataTableRegistryCell>
  );
}, (prev, next) => {
  if (snapshotRowKey(prev.row) !== snapshotRowKey(next.row)) return false;
  if (
    prev.registry.fleetState?.heartbeatTtlSeconds !==
    next.registry.fleetState?.heartbeatTtlSeconds
  ) {
    return false;
  }
  if (fleetReplicasPollKey(prev.liveReplicas) !== fleetReplicasPollKey(next.liveReplicas)) {
    return false;
  }
  // Presentation-stable polls still patch heartbeatAt — re-render for freshness
  // without treating liveness as chip remount identity.
  if (
    fleetReplicasLivenessKey(prev.liveReplicas) !==
    fleetReplicasLivenessKey(next.liveReplicas)
  ) {
    return false;
  }
  const version = prev.row.version;
  const prevShowsStepper =
    prev.row.kind === "published" &&
    prev.registry.rollout &&
    shouldShowRowRolloutStepper(prev.registry.rollout, version);
  const nextShowsStepper =
    next.row.kind === "published" &&
    next.registry.rollout &&
    shouldShowRowRolloutStepper(next.registry.rollout, version);
  if (!prevShowsStepper && !nextShowsStepper) {
    return true;
  }
  return prev.registry.rollout === next.registry.rollout;
});

function snapshotStatusCellPropsAreEqual(
  prev: { row: AppAdminSnapshotRow; registry: SnapshotTableMeta["registry"] },
  next: { row: AppAdminSnapshotRow; registry: SnapshotTableMeta["registry"] },
): boolean {
  if (snapshotRowKey(prev.row) !== snapshotRowKey(next.row)) return false;
  return snapshotRegistryPollEqual(prev.registry, next.registry);
}

const SnapshotStatusCell = memo(function SnapshotStatusCell({
  row,
  registry,
}: {
  row: AppAdminSnapshotRow;
  registry: SnapshotTableMeta["registry"];
}) {
  const rollout = registry.rollout;
  const needsLiveNow =
    row.kind === "pending" ||
    (row.kind === "published" &&
      rollout &&
      shouldShowRowRolloutStepper(rollout, row.version) &&
      isActiveRegistryRollout(rollout.state));
  const liveNow = useLiveNow({ enabled: Boolean(needsLiveNow) });
  const statusId = resolveSnapshotRowStatus(snapshotRowStatusInput(row, registry));
  const status = snapshotRowStatusPresentation(statusId);
  const isDeployedVersion = statusId === "current";
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
  const isReadyToDeploy = statusId === "ready_to_deploy";
  const showTimerBelowBadge =
    Boolean(statusTimer) &&
    row.kind !== "pending" &&
    !isReadyToDeploy &&
    !isDeployedVersion;

  if (isReadyToDeploy) {
    if (!statusTimer) {
      return (
        <DataTableRegistryPrimaryLine>
          <span className="text-muted-foreground">—</span>
        </DataTableRegistryPrimaryLine>
      );
    }

    return (
      <DataTableRegistryPrimaryLine className="min-h-0 text-xs leading-4 text-muted-foreground">
        <SearchHighlight text={statusTimer} />
      </DataTableRegistryPrimaryLine>
    );
  }

  return (
    <DataTableRegistryCell className={showTimerBelowBadge ? "gap-2" : undefined}>
      <DataTableRegistryPrimaryLine
        className={row.kind === "pending" && statusTimer ? "flex-wrap gap-2" : undefined}
      >
        {isDeployedVersion ? (
          <Badge variant="info" data-testid="deployed-version-badge">
            {status.label}
          </Badge>
        ) : (
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
        )}
        {row.kind === "pending" && statusTimer ? (
          <PendingPublishDuration statusTimer={statusTimer} />
        ) : null}
      </DataTableRegistryPrimaryLine>
      {rolloutSubline ? (
        <DataTableRegistrySecondaryLine>
          <SearchHighlight text={rolloutSubline} />
        </DataTableRegistrySecondaryLine>
      ) : null}
      {showTimerBelowBadge && statusTimer ? (
        <DataTableRegistrySecondaryLine>
          <SearchHighlight text={statusTimer} />
        </DataTableRegistrySecondaryLine>
      ) : null}
      {failedReason ? (
        <DataTableRegistrySecondaryLine>
          <SearchHighlight text={failedReason} />
        </DataTableRegistrySecondaryLine>
      ) : null}
    </DataTableRegistryCell>
  );
}, snapshotStatusCellPropsAreEqual);

const SnapshotLastUpdateCell = memo(function SnapshotLastUpdateCell({
  row,
}: {
  row: AppAdminSnapshotRow;
}) {
  const liveNow = useLiveNow({ enabled: row.kind === "pending" });
  const lastUpdated = snapshotLastUpdatedLabel(row, {
    now: row.kind === "pending" ? liveNow : undefined,
    minRelativeUnit: row.kind === "pending" ? "minute" : undefined,
  });

  if (!lastUpdated) {
    return (
      <DataTableRegistryPrimaryLine className="text-xs leading-5">
        <span className="text-muted-foreground">—</span>
      </DataTableRegistryPrimaryLine>
    );
  }

  return (
    <DataTableRegistryPrimaryLine className="text-xs leading-5">
      <time
        className="text-muted-foreground"
        dateTime={snapshotLastUpdatedAt(row) ?? undefined}
        title={lastUpdated.absolute}
        data-testid="snapshot-last-updated-at"
      >
        <SearchHighlight text={lastUpdated.relative} />
      </time>
    </DataTableRegistryPrimaryLine>
  );
}, (prev, next) => snapshotRowKey(prev.row) === snapshotRowKey(next.row));

const SnapshotActionCell = memo(function SnapshotActionCell({
  row,
  registry,
  controlsDisabled,
  offerManualDeploy,
  deployingVersion,
  onDeployVersion,
}: SnapshotActionCellProps) {
  const isDeployable = row.kind === "published";
  const isDeploying = deployingVersion === row.version;
  const failedRolloutRetry = isFailedRolloutRetryRow(row, registry.rollout);
  const canOfferDeploy = offerManualDeploy || failedRolloutRetry;
  const deployDisabled =
    !isDeployable ||
    (controlsDisabled && !failedRolloutRetry) ||
    (row.version === registry.desiredVersion && !failedRolloutRetry);
  const showRolloutDeploying = isRolloutDeployingAction(registry.rollout, row.version);

  if (showRolloutDeploying) {
    return (
      <DataTableRegistryPrimaryLine className="justify-end">
        <Button
          type="button"
          variant={TABLE_ROW_ACTION_BUTTON_VARIANT}
          size="sm"
          data-testid={`deploy-version-${row.version}`}
          loading
        >
          Deploying...
        </Button>
      </DataTableRegistryPrimaryLine>
    );
  }

  if (isDeployable && row.version === registry.desiredVersion && !failedRolloutRetry) {
    return (
      <DataTableRegistryPrimaryLine className="justify-end">
        <span className="text-muted-foreground">—</span>
      </DataTableRegistryPrimaryLine>
    );
  }

  if (isDeployable && !canOfferDeploy) {
    return (
      <DataTableRegistryPrimaryLine className="justify-end">
        <span className="text-muted-foreground">—</span>
      </DataTableRegistryPrimaryLine>
    );
  }

  if (isDeployable) {
    const actionLabel = failedRolloutRetry
      ? isDeploying
        ? "Retrying..."
        : "Retry deploy"
      : isDeploying
        ? "Deploying..."
        : "Deploy";

    return (
      <DataTableRegistryPrimaryLine className="justify-end">
        <Button
          type="button"
          variant={TABLE_ROW_ACTION_BUTTON_VARIANT}
          size="sm"
          data-testid={`deploy-version-${row.version}`}
          loading={isDeploying}
          disabled={deployDisabled}
          onClick={() => onDeployVersion(row.version)}
        >
          {actionLabel}
        </Button>
      </DataTableRegistryPrimaryLine>
    );
  }

  return (
    <DataTableRegistryPrimaryLine className="justify-end">
      <span className="text-muted-foreground">—</span>
    </DataTableRegistryPrimaryLine>
  );
}, snapshotActionCellPropsAreEqual);

export const AppAdminSnapshotsTable = memo(function AppAdminSnapshotsTable({
  registry,
  historyRevisions,
  controlsDisabled,
  offerManualDeploy,
  emptyTitle,
  emptyHint,
  deployingVersion,
  onDeployVersion,
  toolbarTrailing,
}: SnapshotTableMeta) {
  const registryRef = useRef(registry);
  registryRef.current = registry;

  // Version lists are structurally shared across heartbeat polls — key off
  // those refs so row identity (and TanStack cells) stay stable.
  const rows = useMemo(
    () => buildAppAdminSnapshotRows(registry),
    [
      registry.publishedVersions,
      registry.pendingVersions,
      registry.failedVersions,
    ],
  );
  const replicaPartition = useMemo(
    () =>
      partitionFleetReplicasForSnapshotTable(
        registry.fleetState?.replicas,
        rows.map((row) => row.version),
      ),
    [registry.fleetState?.replicas, rows],
  );
  const historyPollKey = historyRevisionsPollKey(historyRevisions);
  const deployedByByVersion = useMemo(
    () => deployedByForVersions(historyRevisions),
    // Content key: history page-array identity must not churn this Map.
    [historyPollKey, historyRevisions],
  );
  // Mutable meta bag is the single live lookup for cells/accessors — column
  // defs stay identity-stable while partition/deployedBy Maps refresh.
  const tableMetaRef = useRef<SnapshotTableRuntimeMeta | null>(null);
  if (!tableMetaRef.current) {
    tableMetaRef.current = {
      registry,
      replicaPartition,
      deployedByByVersion,
    };
  }
  tableMetaRef.current.registry = registry;
  tableMetaRef.current.replicaPartition = replicaPartition;
  tableMetaRef.current.deployedByByVersion = deployedByByVersion;
  const showActionColumn =
    offerManualDeploy ||
    rows.some(
      (row) =>
        isFailedRolloutRetryRow(row, registry.rollout) ||
        isRolloutDeployingAction(registry.rollout, row.version),
    );
  const [sorting, setSorting] = useState<SortingState>([
    { id: "sortAt", desc: true },
  ]);
  const [search, setSearch] = useState("");
  const filteredRows = useMemo(() => {
    const query = search.trim();
    if (!query) return rows;
    return rows.filter((row) =>
      textContainsAllSearchTokens(
        snapshotRowSearchText(
          row,
          registry,
          deployedByByVersion,
          replicaPartition.byVersion.get(row.version),
        ),
        query,
      ),
    );
  }, [deployedByByVersion, replicaPartition.byVersion, rows, registry, search]);

  const columns = useMemo(
    () => [
      columnHelper.display({
        id: "severity",
        header: () => <span className="sr-only">Status</span>,
        meta: {
          severityGutter: true,
        },
        enableSorting: false,
        cell: ({ row, table }) => (
          <SnapshotSeverityCell
            row={row.original}
            registry={readSnapshotTableMeta(table.options.meta).registry}
          />
        ),
      }),
      columnHelper.accessor(
        (row) => rowPublication(row)?.triggerPullRequest?.number ?? 0,
        {
          id: "pullRequest",
          meta: { className: "pl-0", headerClassName: "pl-0" },
          header: ({ column }) => (
            <DataTableColumnHeader column={column} title="Pull request" />
          ),
          cell: ({ row, table }) => {
            const meta = readSnapshotTableMeta(table.options.meta);
            return (
              <SnapshotPullRequestCell
                row={row.original}
                registry={meta.registry}
                liveReplicas={
                  meta.replicaPartition.byVersion.get(row.original.version) ??
                  []
                }
              />
            );
          },
        },
      ),
      columnHelper.accessor(
        (row) => snapshotStatusSortKey(row, registryRef.current),
        {
          id: "status",
          header: ({ column }) => (
            <DataTableColumnHeader column={column} title="Status" />
          ),
          cell: ({ row, table }) => (
            <SnapshotStatusCell
              row={row.original}
              registry={readSnapshotTableMeta(table.options.meta).registry}
            />
          ),
        },
      ),
      columnHelper.accessor((row) => row.sortAt, {
        id: "sortAt",
        meta: { hideBelow: "md" },
        header: ({ column }) => (
          <DataTableColumnHeader column={column} title="Last update" />
        ),
        cell: ({ row }) => <SnapshotLastUpdateCell row={row.original} />,
        sortingFn: "datetime",
      }),
      columnHelper.accessor(
        (row) =>
          tableMetaRef.current?.deployedByByVersion.get(row.version) ?? "",
        {
          id: "deployedBy",
          meta: { hideBelow: "md" },
          header: ({ column }) => (
            <DataTableColumnHeader column={column} title="Deployed by" />
          ),
          cell: ({ row, table }) => (
            <SnapshotDeployedByCell
              actor={readSnapshotTableMeta(
                table.options.meta,
              ).deployedByByVersion.get(row.original.version)}
            />
          ),
        },
      ),
      ...(showActionColumn
        ? [
            columnHelper.display({
              id: "action",
              header: "Action",
              meta: { align: "end" },
              enableSorting: false,
              cell: ({ row, table }) => (
                <SnapshotActionCell
                  row={row.original}
                  registry={readSnapshotTableMeta(table.options.meta).registry}
                  controlsDisabled={controlsDisabled}
                  offerManualDeploy={offerManualDeploy}
                  deployingVersion={deployingVersion}
                  onDeployVersion={onDeployVersion}
                />
              ),
            }),
          ]
        : []),
    ],
    [
      controlsDisabled,
      deployingVersion,
      offerManualDeploy,
      onDeployVersion,
      showActionColumn,
    ],
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
    meta: tableMetaRef.current,
  });

  if (rows.length === 0) {
    return (
      <div className="space-y-1" data-testid="snapshots-empty">
        <p className="text-sm text-foreground">{emptyTitle}</p>
        <p className="text-sm text-muted-foreground">{emptyHint}</p>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <DataTableSearchShell
        search={search}
        onSearchChange={setSearch}
        searchPlaceholder="Search versions"
        trailing={toolbarTrailing}
      >
        <DataTableView
          table={table}
          testId="snapshots-table"
          emptyMessage={search.trim() ? "No matching versions." : "No results."}
          getRowProps={(row) => {
            const affordance = selectedVersionRowAffordance(
              registry.rollout,
              row.original.version,
            );
            return {
              "data-testid": `snapshot-row-${row.original.kind}`,
              ...(affordance ? { "data-selected-version-row": "true" } : {}),
            };
          }}
        />
      </DataTableSearchShell>
      {replicaPartition.orphans.length > 0 ? (
        <div
          className="space-y-1.5"
          data-testid="fleet-orphan-replicas"
        >
          <p className="text-sm text-muted-foreground">
            Replicas not on a listed version
          </p>
          <SnapshotRowLiveReplicas
            replicas={replicaPartition.orphans}
            className="mt-0"
            hoverScope="orphan"
            heartbeatTtlSeconds={registry.fleetState?.heartbeatTtlSeconds}
          />
        </div>
      ) : null}
    </div>
  );
}, snapshotTablePropsAreEqual);

function snapshotTablePropsAreEqual(
  prev: SnapshotTableMeta,
  next: SnapshotTableMeta,
): boolean {
  if (prev.controlsDisabled !== next.controlsDisabled) return false;
  if (prev.offerManualDeploy !== next.offerManualDeploy) return false;
  if (prev.emptyTitle !== next.emptyTitle) return false;
  if (prev.emptyHint !== next.emptyHint) return false;
  if (prev.deployingVersion !== next.deployingVersion) return false;
  if (prev.onDeployVersion !== next.onDeployVersion) return false;
  if (
    historyRevisionsPollKey(prev.historyRevisions) !==
    historyRevisionsPollKey(next.historyRevisions)
  ) {
    return false;
  }
  // Poll-equal omits fleet so reconcile can keep stable version refs while
  // refreshing replicas. Compare presentation keys (not heartbeat ticks) so
  // controlled HoverCards are not remounted every fleet poll.
  if (
    fleetReplicasPollKey(prev.registry.fleetState?.replicas) !==
    fleetReplicasPollKey(next.registry.fleetState?.replicas)
  ) {
    return false;
  }
  return snapshotRegistryPollEqual(prev.registry, next.registry);
}
