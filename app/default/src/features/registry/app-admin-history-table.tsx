import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { formatRegistryTime, formatRegistryTimeAgo } from "@/features/registry/format";
import {
  PublicationPullRequestLabel,
  REGISTRY_TABLE_LINK_CLASS,
} from "@/features/registry/publication-pull-request-label";
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

function fullTransitionLabel(revision: AppAdminRegistryRevision): string {
  if (!revision.previousVersion) {
    return `First deployment → ${revision.version}`;
  }
  return `${revision.previousVersion} → ${revision.version}`;
}

function availabilityStatus(revision: AppAdminRegistryRevision): {
  label: string;
  variant: "success" | "warning" | "secondary" | "destructive";
} {
  switch (revision.deploymentState) {
    case "desired":
      return { label: "Current", variant: "success" };
    case "redeployable":
    case "available":
      return { label: "Redeployable", variant: "secondary" };
    case "locked":
      return { label: "Locked", variant: "secondary" };
    case "expired":
      return { label: "Expired", variant: "destructive" };
    default:
      return { label: "—", variant: "secondary" };
  }
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
      <div className="overflow-x-auto rounded-xl border border-border">
        <table className="min-w-full divide-y divide-border text-sm" data-testid="revision-history-table">
          <thead className="bg-foreground/[0.03] text-left text-xs font-medium uppercase tracking-wide text-muted-foreground">
            <tr>
              <th className="px-4 py-3 font-medium">Deployed at</th>
              <th className="px-4 py-3 font-medium">Transition</th>
              <th className="px-4 py-3 font-medium">Availability</th>
              <th className="px-4 py-3 font-medium">Deployed by</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border bg-card text-card-foreground">
            {revisions.map((revision) => {
              const deployedAt = deployedAtLabel(revision.deployedAt);
              const availability = availabilityStatus(revision);
              const pullRequest = revision.publication?.triggerPullRequest;

              return (
                <tr key={revision.id} data-testid="revision-history-row">
                  <td className="px-4 py-3 align-top text-muted-foreground">
                    {deployedAt ? (
                      <time
                        dateTime={deployedAt.dateTime}
                        title={deployedAt.absolute}
                        data-testid="revision-deployed-at"
                      >
                        {deployedAt.relative}
                      </time>
                    ) : (
                      <span>—</span>
                    )}
                  </td>
                  <td className="px-4 py-3 align-top">
                    <div className="flex flex-col gap-1">
                      <RegistryCode title={fullTransitionLabel(revision)}>
                        {transitionLabel(revision)}
                      </RegistryCode>
                      {revision.sourceUrl ? (
                        <a
                          href={revision.sourceUrl}
                          target="_blank"
                          rel="noreferrer"
                          className={REGISTRY_TABLE_LINK_CLASS}
                        >
                          {revision.sourceRef?.slice(0, 12) ?? "source"}
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
                          workflow
                        </a>
                      ) : null}
                    </div>
                  </td>
                  <td className="px-4 py-3 align-top">
                    <Badge variant={availability.variant}>{availability.label}</Badge>
                  </td>
                  <td className="px-4 py-3 align-top text-muted-foreground">
                    {deployedByLabel(revision.deployedBy)}
                  </td>
                </tr>
              );
            })}
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
