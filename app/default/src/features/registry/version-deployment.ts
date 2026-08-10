/** Published-version retention lifecycle from gestaltd registry. */
export type VersionDeploymentState =
  | "desired"
  | "redeployable"
  | "locked"
  | "available"
  | "expired"
  | (string & {});

export function normalizeVersionDeploymentState(
  value?: string | null,
): VersionDeploymentState | undefined {
  const trimmed = value?.trim().toLowerCase();
  return trimmed || undefined;
}

export function isVersionManuallyDeployable(
  deploymentState?: string | null,
): boolean {
  const state = normalizeVersionDeploymentState(deploymentState);
  if (!state) return true;
  return state !== "locked" && state !== "expired";
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

export function versionDeployableUntilAbsolute(
  deployableUntil?: string | null,
): string | null {
  if (!deployableUntil) return null;
  const until = new Date(deployableUntil);
  if (Number.isNaN(until.getTime())) return null;
  return until.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

export function versionDeployableUntilLabel(
  deployableUntil?: string | null,
  nowMs: number = Date.now(),
): string | null {
  const absolute = versionDeployableUntilAbsolute(deployableUntil);
  if (!absolute || !deployableUntil) return null;
  const until = new Date(deployableUntil);
  if (until.getTime() < nowMs) {
    return `Expired ${absolute}`;
  }
  return `Until ${absolute}`;
}

/**
 * Short status-column label for published retention (e.g. "Redeployable").
 * Full deployable-until timestamp belongs in a hover card, not this line.
 */
export function publishedVersionRetentionSubline(published: {
  deploymentState?: string;
  deployableUntil?: string;
}): string | null {
  return (
    versionDeploymentStateLabel(published.deploymentState) ??
    versionDeployableUntilLabel(published.deployableUntil)
  );
}

/** Hover-card body when a retention row has an absolute deployable-until. */
export function publishedVersionRetentionHoverDetail(published: {
  deploymentState?: string;
  deployableUntil?: string;
}): string | null {
  const absolute = versionDeployableUntilAbsolute(published.deployableUntil);
  if (!absolute) return null;
  const until = new Date(published.deployableUntil!);
  if (Number.isNaN(until.getTime())) return null;
  if (until.getTime() < Date.now()) {
    return `Retention expired ${absolute}`;
  }
  const stateLabel = versionDeploymentStateLabel(published.deploymentState);
  if (stateLabel) {
    return `${stateLabel} until ${absolute}`;
  }
  return `Deployable until ${absolute}`;
}
