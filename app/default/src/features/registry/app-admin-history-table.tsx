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
import { formatRegistryTime, formatRegistryTimeAgo } from "@/features/registry/format";
import {
  PublicationPullRequestLabel,
  REGISTRY_TABLE_LINK_CLASS,
} from "@/features/registry/publication-pull-request-label";
import { RegistryCode } from "@/features/registry/registry-code";
import {
  decorateRevisionRollout,
  revisionHasActiveRollout,
  revisionRecoveryDurationLabel,
  revisionRolloutStatusLabel,
  revisionRolloutStatusTimer,
  revisionRolloutStatusVariant,
} from "@/features/registry/revision-history-rows";
import type { AppAdminRegistryRevision, RegistryRollout } from "@/features/registry/types";
import { useLiveNow } from "@/hooks/use-live-now";
import { textContainsAllSearchTokens } from "@/lib/search-highlight";
import { Loader2 } from "lucide-react";

const ROLLOUT_DURATION_CLASS =
  "inline-block min-w-[4.75rem] tabular-nums text-muted-foreground";

const columnHelper = createColumnHelper<AppAdminRegistryRevision>();

function shortenVersion(version: string): string {
  const trimmed = version.trim();
  if (trimmed.length <= 24) return trimmed;
  return `${trimmed.slice(0, 20)}…`;
}

function transitionLabel(revision: AppAdminRegistryRevision): string {
  if (!revision.previousVersion) {
    return `First deployment → ${shortenVersion(revision.version)}`;
  }
  return `${shortenVersion(revision.previousVersion)} → ${shortenVersion(revision.version)}`;
}

function fullTransitionLabel(revision: AppAdminRegistryRevision): string {
  if (!revision.previousVersion) {
    return `First deployment → ${revision.version}`;
  }
  return `${revision.previousVersion} → ${revision.version}`;
}

function deployedAtLabel(
  deployedAt?: string,
  now: number | Date = Date.now(),
): { relative: string; absolute: string; dateTime: string } | null {
  if (!deployedAt) return null;
  const relative = formatRegistryTimeAgo(deployedAt, now) || formatRegistryTime(deployedAt);
  const absolute = formatRegistryTime(deployedAt);
  if (!relative || relative === "—") return null;
  return { relative, absolute, dateTime: deployedAt };
}

function deployedByLabel(actor?: string): string {
  const trimmed = actor?.trim();
  return trimmed || "—";
}

function revisionRowSearchText(
  revision: AppAdminRegistryRevision,
  rollout: RegistryRollout | undefined,
  currentRevisionId: string | undefined,
  liveNow: number,
): string {
  const decoratedRevision = decorateRevisionRollout(revision, rollout, currentRevisionId);
  const deployedAt = deployedAtLabel(revision.deployedAt, liveNow);
  const pullRequest = revision.publication?.triggerPullRequest;
  return [
    revision.version,
    revision.previousVersion,
    transitionLabel(revision),
    fullTransitionLabel(revision),
    deployedAt?.relative,
    deployedAt?.absolute,
    revision.deployedBy,
    revision.sourceRef,
    pullRequest?.number ? `PR #${pullRequest.number}` : null,
    pullRequest?.title,
    revision.publication?.workflowRunUrl ? "workflow" : null,
    revisionRolloutStatusLabel(decoratedRevision.rolloutState),
    revisionRolloutStatusTimer(decoratedRevision, liveNow),
    revision.recovery ? "Recovered after failed rollout" : null,
    revisionRecoveryDurationLabel(revision),
    revision.recovery?.recoveredAt
      ? formatRegistryTimeAgo(revision.recovery.recoveredAt, liveNow)
      : null,
    revision.recovery?.recoveredAt
      ? formatRegistryTime(revision.recovery.recoveredAt)
      : null,
    revision.recovery?.sourceVersion,
  ]
    .filter(Boolean)
    .join(" ");
}

function RevisionRolloutDuration({ statusTimer }: { statusTimer: string }) {
  if (statusTimer.startsWith("for ")) {
    const duration = statusTimer.replace(/^for\s+/, "");
    return (
      <span className="text-muted-foreground">
        for <span className={ROLLOUT_DURATION_CLASS}>{duration}</span>
      </span>
    );
  }
  return <span className="text-muted-foreground">{statusTimer}</span>;
}

function RevisionDeployedAtCell({
  revision,
  liveNow,
}: {
  revision: AppAdminRegistryRevision;
  liveNow: number;
}) {
  const deployedAt = deployedAtLabel(revision.deployedAt, liveNow);

  if (!deployedAt) {
    return <span className="text-muted-foreground">—</span>;
  }

  return (
    <time
      className="text-muted-foreground"
      dateTime={deployedAt.dateTime}
      title={deployedAt.absolute}
      data-testid="revision-deployed-at"
    >
      <SearchHighlight text={deployedAt.relative} />
    </time>
  );
}

function RevisionTransitionCell({ revision }: { revision: AppAdminRegistryRevision }) {
  const pullRequest = revision.publication?.triggerPullRequest;

  return (
    <div className="flex flex-col gap-1">
      <RegistryCode title={fullTransitionLabel(revision)}>
        <SearchHighlight text={transitionLabel(revision)} />
      </RegistryCode>
      {revision.sourceUrl ? (
        <a
          href={revision.sourceUrl}
          target="_blank"
          rel="noreferrer"
          className={REGISTRY_TABLE_LINK_CLASS}
        >
          <SearchHighlight text={revision.sourceRef?.slice(0, 12) ?? "source"} />
        </a>
      ) : null}
      {pullRequest?.number ? (
        <PublicationPullRequestLabel
          pullRequest={pullRequest}
          titleClassName="text-xs text-muted-foreground"
        />
      ) : revision.publication?.workflowRunUrl ? (
        <a
          href={revision.publication.workflowRunUrl}
          target="_blank"
          rel="noreferrer"
          className={REGISTRY_TABLE_LINK_CLASS}
        >
          <SearchHighlight text="workflow" />
        </a>
      ) : null}
    </div>
  );
}

function RevisionStatusCell({
  revision,
  rollout,
  currentRevisionId,
  liveNow,
}: {
  revision: AppAdminRegistryRevision;
  rollout?: RegistryRollout;
  currentRevisionId?: string;
  liveNow: number;
}) {
  const decoratedRevision = decorateRevisionRollout(revision, rollout, currentRevisionId);
  const statusLabel = revisionRolloutStatusLabel(decoratedRevision.rolloutState);
  const statusVariant = revisionRolloutStatusVariant(decoratedRevision.rolloutState);
  const statusTimer = revisionRolloutStatusTimer(decoratedRevision, liveNow);
  const isActiveRollout =
    decoratedRevision.rolloutState === "enrolling" ||
    decoratedRevision.rolloutState === "restarting";
  const recovery =
    decoratedRevision.rolloutState === "failed"
      ? decoratedRevision.recovery
      : undefined;
  const recoveryDuration = revisionRecoveryDurationLabel(decoratedRevision);
  const recoveredAgo = recovery?.recoveredAt
    ? formatRegistryTimeAgo(recovery.recoveredAt, liveNow)
    : "";

  if (!statusLabel || !statusVariant) {
    return <span className="text-muted-foreground">—</span>;
  }

  return (
    <div className="flex flex-col gap-1">
      <div className="flex flex-wrap items-center gap-2">
        <Badge variant={statusVariant} data-testid="revision-rollout-status">
          {isActiveRollout ? (
            <Loader2
              className="mr-1 size-3.5 animate-spin"
              aria-hidden="true"
              data-testid="revision-rollout-status-spinner"
            />
          ) : null}
          <SearchHighlight text={statusLabel} />
        </Badge>
        {statusTimer ? <RevisionRolloutDuration statusTimer={statusTimer} /> : null}
      </div>
      {recovery ? (
        <div
          className="flex flex-wrap items-center gap-2 pt-1"
          data-testid="revision-recovery"
        >
          <Badge variant="success">
            <SearchHighlight text="Recovered after failed rollout" />
          </Badge>
          <time
            className="text-xs text-muted-foreground"
            dateTime={recovery.recoveredAt}
            title={formatRegistryTime(recovery.recoveredAt)}
            data-testid="revision-recovered-at"
          >
            <SearchHighlight
              text={recoveredAgo || formatRegistryTime(recovery.recoveredAt)}
            />
          </time>
          {recoveryDuration ? (
            <span className="text-xs text-muted-foreground">
              <SearchHighlight text={`(${recoveryDuration})`} />
            </span>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

export function AppAdminHistoryTable({
  revisions,
  rollout,
  loading,
  loadingMore,
  error,
  onLoadMore,
  hasMore,
}: {
  revisions: AppAdminRegistryRevision[];
  rollout?: RegistryRollout;
  loading: boolean;
  loadingMore: boolean;
  error: string | null;
  onLoadMore: () => void;
  hasMore: boolean;
}) {
  const liveNow = useLiveNow({
    enabled:
      revisionHasActiveRollout(revisions, rollout) ||
      revisions.some((revision) => revision.recovery !== undefined),
  });
  const currentRevisionId = revisions[0]?.id;
  const [sorting, setSorting] = useState<SortingState>([
    { id: "deployedAt", desc: true },
  ]);
  const [search, setSearch] = useState("");
  const filteredRevisions = useMemo(() => {
    const query = search.trim();
    if (!query) return revisions;
    return revisions.filter((revision) =>
      textContainsAllSearchTokens(
        revisionRowSearchText(revision, rollout, currentRevisionId, liveNow),
        query,
      ),
    );
  }, [currentRevisionId, liveNow, revisions, rollout, search]);

  const columns = useMemo(
    () => [
      columnHelper.accessor((revision) => revision.deployedAt ?? "", {
        id: "deployedAt",
        header: ({ column }) => (
          <DataTableColumnHeader column={column} title="Deployed at" />
        ),
        cell: ({ row }) => (
          <RevisionDeployedAtCell revision={row.original} liveNow={liveNow} />
        ),
        sortingFn: "datetime",
      }),
      columnHelper.accessor((revision) => revision.version, {
        id: "transition",
        header: ({ column }) => (
          <DataTableColumnHeader column={column} title="Transition" />
        ),
        cell: ({ row }) => <RevisionTransitionCell revision={row.original} />,
      }),
      columnHelper.accessor(
        (revision) =>
          revisionRolloutStatusLabel(
            decorateRevisionRollout(revision, rollout, currentRevisionId).rolloutState,
          ) ?? "",
        {
          id: "status",
          header: ({ column }) => (
            <DataTableColumnHeader column={column} title="Status" />
          ),
          cell: ({ row }) => (
            <RevisionStatusCell
              revision={row.original}
              rollout={rollout}
              currentRevisionId={currentRevisionId}
              liveNow={liveNow}
            />
          ),
        },
      ),
      columnHelper.accessor((revision) => revision.deployedBy?.trim() ?? "", {
        id: "deployedBy",
        header: ({ column }) => (
          <DataTableColumnHeader column={column} title="Deployed by" />
        ),
        cell: ({ row }) => (
          <span className="text-muted-foreground">
            <SearchHighlight text={deployedByLabel(row.original.deployedBy)} />
          </span>
        ),
      }),
    ],
    [currentRevisionId, liveNow, rollout],
  );

  const table = useReactTable({
    data: filteredRevisions,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getRowId: (revision) => revision.id,
    autoResetPageIndex: false,
  });

  if (loading) {
    return <p className="text-sm text-muted-foreground">Loading revision history…</p>;
  }

  if (error) {
    return <p className="text-sm text-destructive">{error}</p>;
  }

  if (revisions.length === 0) {
    return (
      <p className="text-sm text-muted-foreground" data-testid="revision-history-empty">
        No deployments yet
      </p>
    );
  }

  return (
    <div className="space-y-4">
      <DataTableSearchShell
        search={search}
        onSearchChange={setSearch}
        searchPlaceholder="Search revision history"
      >
        <DataTableView
          table={table}
          testId="revision-history-table"
          emptyMessage={search.trim() ? "No results." : "No results."}
          getRowProps={() => ({
            "data-testid": "revision-history-row",
          })}
        />
      </DataTableSearchShell>

      {hasMore ? (
        <Button
          type="button"
          variant="secondary"
          onClick={onLoadMore}
          disabled={loadingMore}
          data-testid="revision-history-load-more"
        >
          {loadingMore ? (
            <>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              Loading…
            </>
          ) : (
            "Load older revisions"
          )}
        </Button>
      ) : null}
    </div>
  );
}
