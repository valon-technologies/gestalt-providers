/** Published-version retention lifecycle from gestaltd registry. */
export type VersionDeploymentState =
  | "desired"
  | "redeployable"
  | "locked"
  | "available"
  | "expired"
  | (string & {});

export type PublishedVersionRetention = {
  deploymentState?: string;
  deployableUntil?: string;
};

/** Canonical retention view: state, deadline, labels, and deploy eligibility. */
export type ResolvedPublishedVersionRetention = {
  /** Wire or time-derived state used for labels and gating. */
  effectiveState?: VersionDeploymentState;
  /** Absolute deployable-until when parseable. */
  untilMs: number | null;
  /** Deadline has passed (independent of wire state). */
  deadlinePassed: boolean;
  /** Manual Deploy is allowed for this published version. */
  manuallyDeployable: boolean;
  /** Short status-column label (e.g. Redeployable / Expired). */
  subline: string | null;
  /** Hover-card body with absolute timestamp when available. */
  hoverDetail: string | null;
  /** Action-cell copy when Deploy is blocked by retention. */
  actionUnavailableLabel: string | null;
};

export function normalizeVersionDeploymentState(
  value?: string | null,
): VersionDeploymentState | undefined {
  const trimmed = value?.trim().toLowerCase();
  return trimmed || undefined;
}

export function versionDeployableUntilMs(
  deployableUntil?: string | null,
): number | null {
  if (!deployableUntil) return null;
  const until = new Date(deployableUntil);
  if (Number.isNaN(until.getTime())) return null;
  return until.getTime();
}

export function versionDeployableUntilAbsolute(
  deployableUntil?: string | null,
): string | null {
  const untilMs = versionDeployableUntilMs(deployableUntil);
  if (untilMs === null) return null;
  return new Date(untilMs).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

export function versionDeploymentStateLabel(
  deploymentState?: string | null,
): string | null {
  switch (normalizeVersionDeploymentState(deploymentState)) {
    case "desired":
      return "Desired";
    case "redeployable":
      return "Redeployable";
    case "locked":
      return "Locked";
    case "available":
      return "Available";
    case "expired":
      return "Expired";
    default:
      return null;
  }
}

/**
 * Single retention resolver for subline, hover, search text, and deploy gating.
 * Wire `expired`/`locked` and a past `deployableUntil` all collapse to
 * non-deployable; past deadline wins the short label as Expired.
 */
export function resolvePublishedVersionRetention(
  published: PublishedVersionRetention,
  nowMs: number = Date.now(),
): ResolvedPublishedVersionRetention {
  const wireState = normalizeVersionDeploymentState(published.deploymentState);
  const untilMs = versionDeployableUntilMs(published.deployableUntil);
  const deadlinePassed = untilMs !== null && untilMs < nowMs;
  const effectiveState: VersionDeploymentState | undefined = deadlinePassed
    ? "expired"
    : wireState;
  const manuallyDeployable =
    effectiveState !== "locked" && effectiveState !== "expired";
  const absolute = versionDeployableUntilAbsolute(published.deployableUntil);
  const stateLabel = versionDeploymentStateLabel(effectiveState);

  let subline: string | null = stateLabel;
  if (!subline && absolute) {
    subline = deadlinePassed ? `Expired ${absolute}` : `Until ${absolute}`;
  }

  let hoverDetail: string | null = null;
  if (absolute) {
    if (deadlinePassed || effectiveState === "expired") {
      hoverDetail = `Expired ${absolute}`;
    } else if (stateLabel) {
      hoverDetail = `${stateLabel} until ${absolute}`;
    } else {
      hoverDetail = `Deployable until ${absolute}`;
    }
  }

  const actionUnavailableLabel =
    effectiveState === "locked" || effectiveState === "expired"
      ? (versionDeploymentStateLabel(effectiveState) ?? "Unavailable")
      : null;

  return {
    effectiveState,
    untilMs,
    deadlinePassed,
    manuallyDeployable,
    subline,
    hoverDetail,
    actionUnavailableLabel,
  };
}

export function isVersionManuallyDeployable(
  published: PublishedVersionRetention | string | null | undefined,
  nowMs: number = Date.now(),
): boolean {
  if (published === null || published === undefined) return true;
  if (typeof published === "string") {
    return resolvePublishedVersionRetention(
      { deploymentState: published },
      nowMs,
    ).manuallyDeployable;
  }
  return resolvePublishedVersionRetention(published, nowMs).manuallyDeployable;
}

/** Short status-column label for published retention. */
export function publishedVersionRetentionSubline(
  published: PublishedVersionRetention,
  nowMs: number = Date.now(),
): string | null {
  return resolvePublishedVersionRetention(published, nowMs).subline;
}

/** Hover-card body when retention has an absolute deployable-until. */
export function publishedVersionRetentionHoverDetail(
  published: PublishedVersionRetention,
  nowMs: number = Date.now(),
): string | null {
  return resolvePublishedVersionRetention(published, nowMs).hoverDetail;
}
