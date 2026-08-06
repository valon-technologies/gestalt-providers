/**
 * Deploy mode owns how the Versions surface promises work and which
 * row-level deploy actions exist. UI copy and affordances derive from this
 * model instead of re-deriving from a scattered autoDeploy boolean.
 */

export type DeployMode = "automatic" | "manual";

export function resolveDeployMode(autoDeployEnabled: boolean): DeployMode {
  return autoDeployEnabled ? "automatic" : "manual";
}

export type VersionsSurfacePresentation = {
  mode: DeployMode;
  /** Domain framing under the Versions title (what this inventory is). */
  framing: string;
  /** Mode-specific operator promise (one sentence; not repeated on the toggle). */
  modePromise: string;
  pageDescription: string;
  toggleTitle: string;
  /** Operational nuance only — omit when the mode promise already covers it. */
  toggleDescription: string | null;
  /**
   * Table-level explanation when automatic mode hides manual Deploy.
   * Null in manual mode.
   */
  manualDeployBlockedReason: string | null;
  /** Offer per-row Deploy (Retry deploy still allowed when automatic). */
  offerManualDeploy: boolean;
  emptyTitle: string;
  emptyHint: string;
  checkForNewVersionsLabel: string;
  checkForNewVersionsPendingLabel: string;
};

const FRAMING = "Published snapshots ready to deploy to the fleet.";

const MANUAL_DEPLOY_BLOCKED_REASON =
  "To deploy a specific version, turn off automatic deploy.";

export function versionsSurfacePresentation({
  autoDeployEnabled,
  rolloutActive,
  hasDesiredVersion,
}: {
  autoDeployEnabled: boolean;
  rolloutActive: boolean;
  hasDesiredVersion: boolean;
}): VersionsSurfacePresentation {
  const mode = resolveDeployMode(autoDeployEnabled);

  if (mode === "automatic") {
    const modePromise = rolloutActive
      ? "Automatic deploy is on. New versions queue until the current rollout finishes."
      : "Automatic deploy is on. New versions deploy to the fleet without a manual deploy.";
    return {
      mode,
      framing: FRAMING,
      modePromise,
      pageDescription: `${FRAMING} ${modePromise}`,
      toggleTitle: "Automatically deploy new versions",
      toggleDescription: rolloutActive
        ? "New versions queue until the current rollout finishes."
        : null,
      manualDeployBlockedReason: MANUAL_DEPLOY_BLOCKED_REASON,
      offerManualDeploy: false,
      emptyTitle: "No published versions are available.",
      emptyHint:
        "When a build publishes a snapshot, use Check for new versions to load it here.",
      checkForNewVersionsLabel: "Check for new versions",
      checkForNewVersionsPendingLabel: "Checking…",
    };
  }

  const modePromise = rolloutActive
    ? "A rollout is in progress. Watch the fleet strip until replicas converge, then deploy another version if needed."
    : hasDesiredVersion
      ? "Deploy a version to change what's running across the fleet."
      : "No version is serving on the fleet yet. Deploy a version to start.";

  return {
    mode,
    framing: FRAMING,
    modePromise,
    pageDescription: `${FRAMING} ${modePromise}`,
    toggleTitle: "Automatically deploy new versions",
    toggleDescription: rolloutActive
      ? "Deploy is paused while the current rollout finishes. Turn on to deploy new versions automatically afterward."
      : "Deploy versions from the table below. Turn on to deploy new versions automatically.",
    manualDeployBlockedReason: null,
    offerManualDeploy: true,
    emptyTitle: "No published versions are available.",
    emptyHint:
      "When a build publishes a snapshot, use Check for new versions to load it here.",
    checkForNewVersionsLabel: "Check for new versions",
    checkForNewVersionsPendingLabel: "Checking…",
  };
}
