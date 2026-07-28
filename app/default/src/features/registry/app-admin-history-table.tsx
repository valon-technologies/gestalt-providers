import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { formatRegistryTime, formatRegistryTimeAgo } from "@/features/registry/format";
import {
  PublicationPullRequestLabel,
  REGISTRY_TABLE_LINK_CLASS,
} from "@/features/registry/publication-pull-request-label";
import { RegistryCode } from "@/features/registry/registry-code";
import {
  decorateRevisionRollout,
  revisionHasActiveRollout,
  revisionRolloutStatusLabel,
  revisionRolloutStatusTimer,
  revisionRolloutStatusVariant,
} from "@/features/registry/revision-history-rows";
import type { AppAdminRegistryRevision, RegistryRollout } from "@/features/registry/types";
import { useLiveNow } from "@/hooks/use-live-now";
import { Loader2 } from "lucide-react";

const ROLLOUT_DURATION_CLASS =
  "inline-block min-w-[4.75rem] tabular-nums text-muted-foreground";

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

function RevisionHistoryRow({
  revision,
  rollout,
  liveNow,
}: {
  revision: AppAdminRegistryRevision;
  rollout?: RegistryRollout;
  liveNow: number;
}) {
  const decoratedRevision = decorateRevisionRollout(revision, rollout);
  const deployedAt = deployedAtLabel(decoratedRevision.deployedAt, liveNow);
  const pullRequest = decoratedRevision.publication?.triggerPullRequest;
  const statusLabel = revisionRolloutStatusLabel(decoratedRevision.rolloutState);
  const statusVariant = revisionRolloutStatusVariant(decoratedRevision.rolloutState);
  const statusTimer = revisionRolloutStatusTimer(decoratedRevision, liveNow);
  const isActiveRollout =
    decoratedRevision.rolloutState === "enrolling" ||
    decoratedRevision.rolloutState === "restarting";

  return (
    <tr key={decoratedRevision.id} data-testid="revision-history-row">
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
          <RegistryCode title={fullTransitionLabel(decoratedRevision)}>
            {transitionLabel(decoratedRevision)}
          </RegistryCode>
          {decoratedRevision.sourceUrl ? (
            <a
              href={decoratedRevision.sourceUrl}
              target="_blank"
              rel="noreferrer"
              className={REGISTRY_TABLE_LINK_CLASS}
            >
              {decoratedRevision.sourceRef?.slice(0, 12) ?? "source"}
            </a>
          ) : null}
          {pullRequest?.number ? (
            <PublicationPullRequestLabel
              pullRequest={pullRequest}
              titleClassName="text-xs text-muted-foreground"
            />
          ) : decoratedRevision.publication?.workflowRunUrl ? (
            <a
              href={decoratedRevision.publication.workflowRunUrl}
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
        {statusLabel && statusVariant ? (
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
                {statusLabel}
              </Badge>
              {statusTimer ? <RevisionRolloutDuration statusTimer={statusTimer} /> : null}
            </div>
          </div>
        ) : (
          <span className="text-muted-foreground">—</span>
        )}
      </td>
      <td className="px-4 py-3 align-top text-muted-foreground">
        {deployedByLabel(decoratedRevision.deployedBy)}
      </td>
    </tr>
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
    enabled: revisionHasActiveRollout(revisions, rollout),
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
      <div className="overflow-x-auto rounded-xl border border-border">
        <table className="min-w-full divide-y divide-border text-sm" data-testid="revision-history-table">
          <thead className="bg-foreground/[0.03] text-left text-xs font-medium uppercase tracking-wide text-muted-foreground">
            <tr>
              <th className="px-4 py-3 font-medium">Deployed at</th>
              <th className="px-4 py-3 font-medium">Transition</th>
              <th className="px-4 py-3 font-medium">Status</th>
              <th className="px-4 py-3 font-medium">Deployed by</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border bg-card text-card-foreground">
            {revisions.map((revision) => (
              <RevisionHistoryRow
                key={revision.id}
                revision={revision}
                rollout={rollout}
                liveNow={liveNow}
              />
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
