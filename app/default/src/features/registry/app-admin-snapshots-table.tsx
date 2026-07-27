import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { isActiveRegistryRollout } from "@/features/registry/format";
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
import { Loader2 } from "lucide-react";
import { useLiveNow } from "@/hooks/use-live-now";

const PUBLISH_DURATION_CLASS =
  "inline-block min-w-[4.75rem] tabular-nums text-muted-foreground";

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

function snapshotStatus({
  row,
  desiredVersion,
  rollout,
}: {
  row: AppAdminSnapshotRow;
  desiredVersion?: string;
  rollout?: RegistryAppSummary["rollout"];
}): { label: string; variant: "success" | "warning" | "secondary" | "destructive" } {
  if (row.kind === "pending") {
    return { label: "Publishing", variant: "warning" };
  }
  if (row.kind === "failed") {
    return { label: "Failed", variant: "destructive" };
  }
  if (row.version === desiredVersion) {
    return { label: "Deployed", variant: "success" };
  }
  if (rollout && rollout.version === row.version && isActiveRegistryRollout(rollout.state)) {
    return { label: "Rolling out", variant: "warning" };
  }
  return { label: "Available", variant: "secondary" };
}

function PendingPublishDuration({ statusTimer }: { statusTimer: string }) {
  const duration = statusTimer.replace(/^for\s+/, "");
  return (
    <span className="text-muted-foreground">
      for <span className={PUBLISH_DURATION_CLASS}>{duration}</span>
    </span>
  );
}

function AppAdminPendingSnapshotTableRow({
  row,
  registry,
}: {
  row: Extract<AppAdminSnapshotRow, { kind: "pending" }>;
  registry: Pick<
    AppAdminRegistryResponse,
    "desiredVersion" | "rollout" | "selectionDisabled"
  >;
}) {
  const liveNow = useLiveNow({ enabled: true });
  const publication = rowPublication(row);
  const pullRequest = publication?.triggerPullRequest;
  const status = snapshotStatus({
    row,
    desiredVersion: registry.desiredVersion,
    rollout: registry.rollout,
  });
  const statusTimer = snapshotStatusTimer(row, liveNow);
  const lastUpdated = snapshotLastUpdatedLabel(row, {
    now: liveNow,
    minRelativeUnit: "minute",
  });
  const failedReason = snapshotFailedReason(row);

  return (
    <tr data-testid="snapshot-row-pending">
      <td className="px-4 py-3 align-top">
        {pullRequest?.number ? (
          <PublicationPullRequestLabel pullRequest={pullRequest} />
        ) : publication?.workflowRunUrl ? (
          <a
            href={publication.workflowRunUrl}
            target="_blank"
            rel="noreferrer"
            className={REGISTRY_TABLE_LINK_CLASS}
          >
            workflow
          </a>
        ) : (
          <span className="text-muted-foreground">—</span>
        )}
      </td>
      <td className="px-4 py-3 align-top">
        <RegistryCode title={row.version}>{shortenSnapshotVersion(row.version)}</RegistryCode>
      </td>
      <td className="px-4 py-3 align-top">
        <div className="flex flex-col gap-1">
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant={status.variant} data-testid="snapshot-status" className="relative">
              <Loader2
                className="absolute top-1/2 right-full mr-1.5 size-3.5 -translate-y-1/2 animate-spin text-warning-foreground"
                aria-hidden="true"
                data-testid="snapshot-status-spinner"
              />
              {status.label}
            </Badge>
            {statusTimer ? <PendingPublishDuration statusTimer={statusTimer} /> : null}
          </div>
          {failedReason ? (
            <div className="text-xs text-muted-foreground">{failedReason}</div>
          ) : null}
        </div>
      </td>
      <td className="px-4 py-3 align-top text-muted-foreground">
        {lastUpdated ? (
          <time
            dateTime={snapshotLastUpdatedAt(row) ?? undefined}
            title={lastUpdated.absolute}
            data-testid="snapshot-last-updated-at"
          >
            {lastUpdated.relative}
          </time>
        ) : (
          <span>—</span>
        )}
      </td>
      <td className="px-4 py-3 align-top text-right">
        <span className="text-muted-foreground">—</span>
      </td>
    </tr>
  );
}

function AppAdminSnapshotTableRow({
  row,
  registry,
  controlsDisabled,
  deployingVersion,
  onDeployVersion,
}: {
  row: AppAdminSnapshotRow;
  registry: Pick<
    AppAdminRegistryResponse,
    | "publishedVersions"
    | "pendingVersions"
    | "failedVersions"
    | "desiredVersion"
    | "rollout"
    | "selectionDisabled"
  >;
  controlsDisabled: boolean;
  deployingVersion: string | null;
  onDeployVersion: (version: string) => void;
}) {
  if (row.kind === "pending") {
    return (
      <AppAdminPendingSnapshotTableRow row={row} registry={registry} />
    );
  }

  const publication = rowPublication(row);
  const pullRequest = publication?.triggerPullRequest;
  const status = snapshotStatus({
    row,
    desiredVersion: registry.desiredVersion,
    rollout: registry.rollout,
  });
  const isDeployable = row.kind === "published";
  const isDeploying = deployingVersion === row.version;
  const deployDisabled =
    !isDeployable ||
    controlsDisabled ||
    isDeploying ||
    row.version === registry.desiredVersion;
  const statusTimer = snapshotStatusTimer(row);
  const lastUpdated = snapshotLastUpdatedLabel(row);
  const failedReason = snapshotFailedReason(row);

  return (
    <tr data-testid={`snapshot-row-${row.kind}`}>
      <td className="px-4 py-3 align-top">
        {pullRequest?.number ? (
          <PublicationPullRequestLabel pullRequest={pullRequest} />
        ) : publication?.workflowRunUrl ? (
          <a
            href={publication.workflowRunUrl}
            target="_blank"
            rel="noreferrer"
            className={REGISTRY_TABLE_LINK_CLASS}
          >
            workflow
          </a>
        ) : (
          <span className="text-muted-foreground">—</span>
        )}
      </td>
      <td className="px-4 py-3 align-top">
        <RegistryCode title={row.version}>{shortenSnapshotVersion(row.version)}</RegistryCode>
      </td>
      <td className="px-4 py-3 align-top">
        <div className="flex flex-col gap-1">
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant={status.variant} data-testid="snapshot-status" className="relative">
              {status.label}
            </Badge>
          </div>
          {statusTimer ? (
            <div className="text-xs text-muted-foreground">{statusTimer}</div>
          ) : null}
          {failedReason ? (
            <div className="text-xs text-muted-foreground">{failedReason}</div>
          ) : null}
        </div>
      </td>
      <td className="px-4 py-3 align-top text-muted-foreground">
        {lastUpdated ? (
          <time
            dateTime={snapshotLastUpdatedAt(row) ?? undefined}
            title={lastUpdated.absolute}
            data-testid="snapshot-last-updated-at"
          >
            {lastUpdated.relative}
          </time>
        ) : (
          <span>—</span>
        )}
      </td>
      <td className="px-4 py-3 align-top text-right">
        {isDeployable ? (
          <Button
            type="button"
            size="sm"
            data-testid={`deploy-version-${row.version}`}
            disabled={deployDisabled}
            onClick={() => onDeployVersion(row.version)}
          >
            {isDeploying ? "Deploying..." : "Deploy"}
          </Button>
        ) : (
          <span className="text-muted-foreground">—</span>
        )}
      </td>
    </tr>
  );
}

export function AppAdminSnapshotsTable({
  registry,
  controlsDisabled,
  deployingVersion,
  onDeployVersion,
}: {
  registry: Pick<
    AppAdminRegistryResponse,
    | "publishedVersions"
    | "pendingVersions"
    | "failedVersions"
    | "desiredVersion"
    | "rollout"
    | "selectionDisabled"
  >;
  controlsDisabled: boolean;
  deployingVersion: string | null;
  onDeployVersion: (version: string) => void;
}) {
  const rows = buildAppAdminSnapshotRows(registry);

  if (rows.length === 0) {
    return (
      <p className="text-sm text-muted-foreground">No published versions are available.</p>
    );
  }

  return (
    <div className="overflow-x-auto rounded-xl border border-border">
      <table
        className="min-w-full table-fixed divide-y divide-border text-sm"
        data-testid="snapshots-table"
      >
        <colgroup>
          <col className="w-[28%]" />
          <col className="w-[22%]" />
          <col className="w-[26%]" />
          <col className="w-[14%]" />
          <col className="w-[10%]" />
        </colgroup>
        <thead className="bg-foreground/[0.03] text-left text-xs font-medium uppercase tracking-wide text-muted-foreground">
          <tr>
            <th className="px-4 py-3 font-medium">Pull request</th>
            <th className="px-4 py-3 font-medium">Snapshot</th>
            <th className="px-4 py-3 font-medium">Status</th>
            <th className="px-4 py-3 font-medium">Last update</th>
            <th className="px-4 py-3 font-medium text-right">Action</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border bg-card text-card-foreground">
          {rows.map((row) => (
            <AppAdminSnapshotTableRow
              key={`${row.kind}:${row.version}`}
              row={row}
              registry={registry}
              controlsDisabled={controlsDisabled}
              deployingVersion={deployingVersion}
              onDeployVersion={onDeployVersion}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}
