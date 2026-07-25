import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { isActiveRegistryRollout } from "@/features/registry/format";
import { RegistryCode } from "@/features/registry/registry-code";
import {
  buildAppAdminSnapshotRows,
  formatPublicationLabel,
  snapshotFailedReason,
  snapshotStatusTimer,
  snapshotStatusTimestamp,
} from "@/features/registry/snapshot-rows";
import type {
  AppAdminPublication,
  AppAdminPublicationPullRequest,
  AppAdminRegistryResponse,
  AppAdminSnapshotRow,
  RegistryAppSummary,
} from "@/features/registry/types";
import { Loader2 } from "lucide-react";

function shortenSnapshotVersion(version: string): string {
  const trimmed = version.trim();
  if (trimmed.length <= 24) return trimmed;
  return `${trimmed.slice(0, 20)}…`;
}

function pullRequestLabel(pullRequest?: AppAdminPublicationPullRequest): string {
  if (!pullRequest?.number) return "—";
  return formatPublicationLabel({ triggerPullRequest: pullRequest }) ?? `PR #${pullRequest.number}`;
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
    <div className="overflow-x-auto rounded-xl border border-alpha">
      <table className="min-w-full divide-y divide-alpha text-sm" data-testid="snapshots-table">
        <thead className="bg-foreground/[0.03] text-left text-xs font-medium uppercase tracking-wide text-muted-foreground">
          <tr>
            <th className="px-4 py-3 font-medium">Pull request</th>
            <th className="px-4 py-3 font-medium">Snapshot</th>
            <th className="px-4 py-3 font-medium">Status</th>
            <th className="px-4 py-3 font-medium text-right">Action</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-alpha bg-base-white dark:bg-surface">
          {rows.map((row) => {
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
            const statusTimestamp = snapshotStatusTimestamp(row);
            const failedReason = snapshotFailedReason(row);

            return (
              <tr
                key={`${row.kind}:${row.version}`}
                data-testid={`snapshot-row-${row.kind}`}
              >
                <td className="px-4 py-3 align-top">
                  {pullRequest?.url ? (
                    <a
                      href={pullRequest.url}
                      target="_blank"
                      rel="noreferrer"
                      className="font-medium text-gold-700 underline decoration-gold-300 underline-offset-2 hover:text-gold-800 dark:text-gold-300"
                    >
                      {pullRequestLabel(pullRequest)}
                    </a>
                  ) : publication?.workflowRunUrl ? (
                    <a
                      href={publication.workflowRunUrl}
                      target="_blank"
                      rel="noreferrer"
                      className="font-medium text-gold-700 underline decoration-gold-300 underline-offset-2 hover:text-gold-800 dark:text-gold-300"
                    >
                      workflow
                    </a>
                  ) : (
                    <span className="text-muted-foreground">—</span>
                  )}
                </td>
                <td className="px-4 py-3 align-top">
                  <RegistryCode title={row.version}>
                    {shortenSnapshotVersion(row.version)}
                  </RegistryCode>
                </td>
                <td className="px-4 py-3 align-top">
                  <div className="flex flex-col gap-1">
                    <div className="flex flex-wrap items-center gap-2">
                      {row.kind === "pending" ? (
                        <Loader2
                          className="size-3.5 shrink-0 animate-spin text-warning"
                          aria-hidden="true"
                          data-testid="snapshot-status-spinner"
                        />
                      ) : null}
                      <Badge variant={status.variant} data-testid="snapshot-status">
                        {status.label}
                      </Badge>
                      {row.kind === "pending" && statusTimer ? (
                        <span className="text-muted-foreground">{statusTimer}</span>
                      ) : null}
                    </div>
                    {row.kind !== "pending" && statusTimer ? (
                      <div className="text-xs text-muted-foreground">{statusTimer}</div>
                    ) : null}
                    {statusTimestamp ? (
                      <div className="text-xs text-muted-foreground">{statusTimestamp}</div>
                    ) : null}
                    {failedReason ? (
                      <div className="text-xs text-muted-foreground">{failedReason}</div>
                    ) : null}
                  </div>
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
          })}
        </tbody>
      </table>
    </div>
  );
}
