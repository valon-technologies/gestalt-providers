import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  formatRegistryTimeAgo,
  isActiveRegistryRollout,
  sortPublishedVersionsNewestFirst,
} from "@/features/registry/format";
import { RegistryCode } from "@/features/registry/registry-code";
import type {
  AppAdminPublishedVersion,
  RegistryAppSummary,
} from "@/features/registry/types";

function shortenSnapshotVersion(version: string): string {
  const trimmed = version.trim();
  if (trimmed.length <= 24) return trimmed;
  return `${trimmed.slice(0, 20)}…`;
}

function pullRequestLabel(
  pullRequest?: { number: number; title?: string | null },
): string {
  if (!pullRequest?.number) return "—";
  if (pullRequest.title?.trim()) {
    return `PR #${pullRequest.number} · ${pullRequest.title.trim()}`;
  }
  return `PR #${pullRequest.number}`;
}

function snapshotStatus({
  version,
  desiredVersion,
  rollout,
}: {
  version: string;
  desiredVersion?: string;
  rollout?: RegistryAppSummary["rollout"];
}): { label: string; variant: "success" | "warning" | "secondary" } {
  if (version === desiredVersion) {
    return { label: "Deployed", variant: "success" };
  }
  if (
    rollout &&
    rollout.version === version &&
    isActiveRegistryRollout(rollout.state)
  ) {
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
  registry: RegistryAppSummary & {
    publishedVersions: AppAdminPublishedVersion[];
    desiredVersion?: string;
    selectionDisabled: boolean;
  };
  controlsDisabled: boolean;
  deployingVersion: string | null;
  onDeployVersion: (version: string) => void;
}) {
  const publishedVersions = sortPublishedVersionsNewestFirst(registry.publishedVersions);

  if (publishedVersions.length === 0) {
    return <p className="text-sm text-muted-foreground">No published versions are available.</p>;
  }

  return (
    <div className="overflow-x-auto rounded-xl border border-alpha">
      <table className="min-w-full divide-y divide-alpha text-sm" data-testid="snapshots-table">
        <thead className="bg-foreground/[0.03] text-left text-xs uppercase tracking-wide text-muted-foreground">
          <tr>
            <th className="px-4 py-3 font-medium">Pull request</th>
            <th className="px-4 py-3 font-medium">Snapshot</th>
            <th className="px-4 py-3 font-medium">Published</th>
            <th className="px-4 py-3 font-medium">Status</th>
            <th className="px-4 py-3 font-medium text-right">Action</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-alpha bg-base-white dark:bg-surface">
          {publishedVersions.map((version) => {
            const pullRequest = version.publication?.triggerPullRequest;
            const status = snapshotStatus({
              version: version.version,
              desiredVersion: registry.desiredVersion,
              rollout: registry.rollout,
            });
            const isDeploying = deployingVersion === version.version;
            const deployDisabled =
              controlsDisabled ||
              isDeploying ||
              version.version === registry.desiredVersion;

            return (
              <tr key={version.version} data-testid="snapshot-row-published">
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
                  ) : (
                    <span className="text-muted-foreground">—</span>
                  )}
                </td>
                <td className="px-4 py-3 align-top">
                  <RegistryCode title={version.version}>
                    {shortenSnapshotVersion(version.version)}
                  </RegistryCode>
                </td>
                <td className="px-4 py-3 align-top text-muted-foreground">
                  {formatRegistryTimeAgo(version.publishedAt) || "—"}
                </td>
                <td className="px-4 py-3 align-top">
                  <Badge variant={status.variant} data-testid="snapshot-status">
                    {status.label}
                  </Badge>
                </td>
                <td className="px-4 py-3 align-top text-right">
                  <Button
                    type="button"
                    size="sm"
                    data-testid={`deploy-version-${version.version}`}
                    disabled={deployDisabled}
                    onClick={() => onDeployVersion(version.version)}
                  >
                    {isDeploying ? "Deploying..." : "Deploy"}
                  </Button>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
