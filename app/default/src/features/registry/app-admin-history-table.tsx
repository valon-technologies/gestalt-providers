import { Button } from "@/components/ui/button";
import { formatPublicationLabel } from "@/features/registry/snapshot-rows";
import { formatRegistryTime } from "@/features/registry/format";
import { RegistryCode } from "@/features/registry/registry-code";
import type { AppAdminRegistryRevision } from "@/features/registry/types";
import { Loader2 } from "lucide-react";

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

function availabilityLabel(revision: AppAdminRegistryRevision): string {
  switch (revision.deploymentState) {
    case "desired":
      return "Current";
    case "redeployable":
      return "Redeployable";
    case "locked":
      return "Locked";
    default:
      return revision.deploymentState || "—";
  }
}

function deployedByLabel(actor?: string): string {
  const trimmed = actor?.trim();
  if (!trimmed) return "—";
  if (trimmed.startsWith("user:")) {
    return trimmed.slice("user:".length);
  }
  return trimmed;
}

export function AppAdminHistoryTable({
  revisions,
  loading,
  loadingMore,
  error,
  onLoadMore,
  hasMore,
}: {
  revisions: AppAdminRegistryRevision[];
  loading: boolean;
  loadingMore: boolean;
  error: string | null;
  onLoadMore: () => void;
  hasMore: boolean;
}) {
  if (loading) {
    return <p className="text-sm text-muted">Loading revision history…</p>;
  }

  if (error) {
    return <p className="text-sm text-ember-500">{error}</p>;
  }

  if (revisions.length === 0) {
    return (
      <p className="text-sm text-muted" data-testid="revision-history-empty">
        No deployments yet
      </p>
    );
  }

  return (
    <div className="space-y-4">
      <div className="overflow-x-auto">
        <table className="min-w-full text-left text-sm" data-testid="revision-history-table">
          <thead className="border-b border-alpha text-faint">
            <tr>
              <th className="px-3 py-2 font-medium">Deployed at</th>
              <th className="px-3 py-2 font-medium">Transition</th>
              <th className="px-3 py-2 font-medium">Availability</th>
              <th className="px-3 py-2 font-medium">Deployed by</th>
            </tr>
          </thead>
          <tbody>
            {revisions.map((revision) => (
              <tr
                key={revision.id}
                className="border-b border-alpha/60 align-top"
                data-testid="revision-history-row"
              >
                <td className="px-3 py-3 whitespace-nowrap text-muted">
                  {formatRegistryTime(revision.deployedAt)}
                </td>
                <td className="px-3 py-3">
                  <div className="space-y-1">
                    <p className="text-primary">
                      <RegistryCode>{transitionLabel(revision)}</RegistryCode>
                    </p>
                    {revision.sourceUrl ? (
                      <a
                        href={revision.sourceUrl}
                        className="text-xs text-gold-700 hover:text-gold-800 dark:text-gold-300 dark:hover:text-gold-200"
                        target="_blank"
                        rel="noreferrer"
                      >
                        {revision.sourceRef?.slice(0, 12) ?? "source"}
                      </a>
                    ) : null}
                    {formatPublicationLabel(revision.publication) ? (
                      <p className="text-xs text-muted">
                        {revision.publication?.triggerPullRequest?.url ? (
                          <a
                            href={revision.publication.triggerPullRequest.url}
                            className="hover:text-primary"
                            target="_blank"
                            rel="noreferrer"
                          >
                            {formatPublicationLabel(revision.publication)}
                          </a>
                        ) : (
                          formatPublicationLabel(revision.publication)
                        )}
                      </p>
                    ) : null}
                  </div>
                </td>
                <td className="px-3 py-3 whitespace-nowrap text-muted">
                  {availabilityLabel(revision)}
                </td>
                <td className="px-3 py-3 whitespace-nowrap text-muted">
                  {deployedByLabel(revision.deployedBy)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

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
