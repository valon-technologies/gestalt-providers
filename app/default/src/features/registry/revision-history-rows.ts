import {
  durationSecondsBetween,
  formatDurationSeconds,
  isActiveRegistryRollout,
} from "@/features/registry/format";
import type { AppAdminRegistryRevision, RegistryRollout } from "@/features/registry/types";

export function decorateRevisionRollout(
  revision: AppAdminRegistryRevision,
  rollout?: RegistryRollout,
  currentRevisionId?: string,
): AppAdminRegistryRevision {
  if (
    !rollout ||
    rollout.version !== revision.version ||
    !isActiveRegistryRollout(rollout.state) ||
    (currentRevisionId !== undefined && revision.id !== currentRevisionId)
  ) {
    return revision;
  }
  return {
    ...revision,
    rolloutState: rollout.state,
    rolloutDurationSeconds: undefined,
    rolloutCompletedAt: undefined,
    rolloutFailedAt: undefined,
  };
}

export function revisionHasActiveRollout(
  revisions: AppAdminRegistryRevision[],
  rollout?: RegistryRollout,
): boolean {
  if (!rollout || !isActiveRegistryRollout(rollout.state)) {
    return revisions.some(
      (revision) =>
        revision.rolloutState === "enrolling" || revision.rolloutState === "restarting",
    );
  }
  const currentRevision = revisions[0];
  return currentRevision?.version === rollout.version;
}

export function revisionRolloutStatusLabel(state?: string): string | null {
  switch (state) {
    case "enrolling":
    case "restarting":
      return "Rolling out";
    case "complete":
      return "Current";
    case "failed":
      return "Deploy failed";
    default:
      return null;
  }
}

export function revisionRolloutStatusVariant(
  state?: string,
): "success" | "warning" | "destructive" | null {
  switch (state) {
    case "enrolling":
    case "restarting":
      return "warning";
    case "complete":
      return "success";
    case "failed":
      return "destructive";
    default:
      return null;
  }
}

export function revisionRolloutStatusTimer(
  revision: AppAdminRegistryRevision,
  now?: number | Date,
): string | null {
  const state = revision.rolloutState;
  if (!state) return null;

  if (state === "enrolling" || state === "restarting") {
    const seconds =
      durationSecondsBetween(revision.deployedAt, now ?? Date.now()) ??
      revision.rolloutForSeconds ??
      null;
    return seconds !== null ? `for ${formatDurationSeconds(seconds)}` : null;
  }

  if (state === "complete") {
    const duration =
      revision.rolloutDurationSeconds ??
      (revision.rolloutCompletedAt
        ? durationSecondsBetween(revision.deployedAt, revision.rolloutCompletedAt)
        : null);
    return duration !== null ? `Serving in ${formatDurationSeconds(duration)}` : null;
  }

  const duration =
    revision.rolloutDurationSeconds ??
    (revision.rolloutFailedAt
      ? durationSecondsBetween(revision.deployedAt, revision.rolloutFailedAt)
      : null);
  return duration !== null
    ? `Deploy failed after ${formatDurationSeconds(duration)}`
    : null;
}
