import type {
  AppAdminFleetReplica,
  AppAdminFleetState,
  AppAdminRecovery,
  AppAdminRegistryResponse,
} from "@/features/registry/types";
import {
  fleetStatePresentation,
  hasRecoveredFailedRollout,
  type FleetStatePresentation,
} from "@/features/registry/fleet-state";
import { shortInstanceId } from "@/features/registry/fleet-replicas";
import {
  isActiveRegistryRollout,
  resolveVersionSourceHref,
} from "@/features/registry/format";

export type FleetMetricId =
  | "minimum"
  | "onDesired"
  | "wrongVersion"
  | "unhealthy";

export type FleetMetricFact = {
  id: FleetMetricId;
  label: string;
  value: number;
  testId: string;
};

export type FleetStatusDensity = "quiet" | "diagnostic" | "unknown";

export type FleetFailingReplicaCallout = {
  instanceId: string;
  shortId: string;
  error: string;
};

/** Inline proof that a replica is still on the wrong snapshot (not hover-only). */
export type FleetWrongVersionCallout = {
  instanceId: string;
  shortId: string;
  runningVersion: string;
  desiredVersion: string;
};

/**
 * Canonical Versions runtime strip — one lead beat, one proof channel, explicit
 * ownership so the layout does not narrate the same rollout again.
 */
export type FleetStatusView = {
  density: FleetStatusDensity;
  verdict: FleetStatePresentation;
  /** Compact proof when replica chips are present (counts would duplicate chips). */
  summaryLine: string | null;
  /** Tabular metrics only when there is no per-replica proof channel. */
  metrics: FleetMetricFact[];
  showReplicaChips: boolean;
  showDesiredVersion: boolean;
  /** Published snapshot currently selected as desired, when known. */
  desiredVersion?: string;
  /** Runtime binary commit — diagnostic degraded only, not during convergence. */
  showSourceVersion: boolean;
  sourceVersion?: string;
  /** External href for the desired snapshot (source / PR / workflow), when known. */
  desiredVersionHref?: string | null;
  /** Freshness TTL copy for unknown — page-level "Registry updated" owns clock time. */
  showFreshnessWindow: boolean;
  heartbeatTtlSeconds: number;
  recovery: AppAdminRecovery | null;
  /**
   * When true, the fleet strip owns the active-rollout headline — layout must
   * not render a second "Rolling out" alert.
   */
  ownsActiveRolloutHeadline: boolean;
  failingReplica: FleetFailingReplicaCallout | null;
  wrongVersionReplica: FleetWrongVersionCallout | null;
  pathHint: string | null;
};

const METRIC: Record<
  FleetMetricId,
  { label: string; testId: string; value: (fleet: AppAdminFleetState) => number }
> = {
  minimum: {
    label: "Required minimum",
    testId: "fleet-minimum-instances",
    value: (fleet) => fleet.minimumHealthyInstances,
  },
  onDesired: {
    label: "On desired version",
    testId: "fleet-running-desired",
    value: (fleet) => fleet.runningDesiredVersion,
  },
  wrongVersion: {
    label: "Wrong version",
    testId: "fleet-mismatched",
    value: (fleet) => fleet.mismatched,
  },
  unhealthy: {
    label: "Unhealthy replicas",
    testId: "fleet-errors",
    value: (fleet) => fleet.errors,
  },
};

function metric(id: FleetMetricId, fleet: AppAdminFleetState): FleetMetricFact {
  const def = METRIC[id];
  return {
    id,
    label: def.label,
    value: def.value(fleet),
    testId: def.testId,
  };
}

function metricsFor(
  ids: FleetMetricId[],
  fleet: AppAdminFleetState,
): FleetMetricFact[] {
  return ids.map((id) => metric(id, fleet));
}

/** One-line convergence proof — replaces a StatGroup when chips are visible. */
export function formatFleetConvergenceSummary(fleet: AppAdminFleetState): string {
  const parts = [
    `${fleet.runningDesiredVersion} of ${fleet.liveInstances} on desired version`,
  ];
  if (fleet.mismatched > 0) {
    parts.push(
      `${fleet.mismatched} wrong version`,
    );
  }
  if (fleet.errors > 0) {
    parts.push(
      `${fleet.errors} unhealthy`,
    );
  }
  if (fleet.liveInstances < fleet.minimumHealthyInstances) {
    parts.push(`need ${fleet.minimumHealthyInstances} minimum`);
  }
  return parts.join(" · ");
}

/**
 * One primary next step for the strip. Unhealthy inspect beats wait — a start
 * failure is not fixed by “wait for updating.”
 */
export function resolveFleetPathHint(
  fleet: Pick<AppAdminFleetState, "errors" | "mismatched" | "state">,
): string | null {
  if (fleet.state === "converging") {
    if (fleet.errors > 0) return "Inspect the unhealthy replica.";
    return "Wait for replicas to finish updating.";
  }
  if (fleet.state === "degraded") {
    if (fleet.errors > 0) return "Inspect the unhealthy replica.";
    if (fleet.mismatched > 0) return "Inspect replicas on the wrong version.";
    return null;
  }
  return null;
}

function pickFailingReplica(
  replicas: AppAdminFleetReplica[],
): FleetFailingReplicaCallout | null {
  const failing = replicas.find(
    (replica) =>
      replica.class === "error" && Boolean(replica.lastError?.trim()),
  );
  if (!failing?.lastError?.trim()) return null;
  return {
    instanceId: failing.instanceId,
    shortId: shortInstanceId(failing.instanceId),
    error: failing.lastError.trim(),
  };
}

function pickWrongVersionReplica(
  replicas: AppAdminFleetReplica[],
  desiredVersion: string | undefined,
): FleetWrongVersionCallout | null {
  const desired = desiredVersion?.trim() || "";
  const mismatched = replicas.find(
    (replica) =>
      replica.class === "mismatched" && Boolean(replica.runningVersion?.trim()),
  );
  if (!mismatched?.runningVersion?.trim()) return null;
  return {
    instanceId: mismatched.instanceId,
    shortId: shortInstanceId(mismatched.instanceId),
    runningVersion: mismatched.runningVersion.trim(),
    desiredVersion: desired,
  };
}

function emptyView(
  verdict: FleetStatePresentation,
  recovery: AppAdminRecovery | null,
): FleetStatusView {
  return {
    density: "unknown",
    verdict,
    summaryLine: null,
    metrics: [],
    showReplicaChips: false,
    showDesiredVersion: false,
    showSourceVersion: false,
    showFreshnessWindow: false,
    heartbeatTtlSeconds: 0,
    recovery,
    ownsActiveRolloutHeadline: false,
    failingReplica: null,
    wrongVersionReplica: null,
    pathHint: null,
  };
}

/**
 * State-gated runtime truth for the Versions surface.
 * Which facts appear is a function of fleet health — not a fixed dashboard.
 */
export function presentFleetStatus(
  registry: Pick<
    AppAdminRegistryResponse,
    | "desiredVersion"
    | "fleetState"
    | "recovery"
    | "rollout"
    | "publishedVersions"
    | "pendingVersions"
    | "failedVersions"
  >,
): FleetStatusView {
  const { fleetState } = registry;
  const verdict = fleetStatePresentation(fleetState);
  const recovery = hasRecoveredFailedRollout(registry)
    ? (registry.recovery ?? null)
    : null;
  const rolloutActive = Boolean(
    registry.rollout && isActiveRegistryRollout(registry.rollout.state),
  );

  if (!fleetState) {
    return emptyView(verdict, recovery);
  }

  const desiredVersion =
    fleetState.desiredVersion || registry.desiredVersion || undefined;
  const desiredVersionHref = resolveVersionSourceHref(registry, desiredVersion);
  const sourceVersion = fleetState.sourceVersion || undefined;
  const replicas = fleetState.replicas ?? [];
  const hasReplicaProof = replicas.length > 0;
  const state = fleetState.state;

  if (state === "healthy") {
    return {
      density: "quiet",
      verdict,
      summaryLine: null,
      metrics: [],
      showReplicaChips: false,
      showDesiredVersion: false,
      showSourceVersion: false,
      desiredVersion,
      desiredVersionHref,
      sourceVersion,
      showFreshnessWindow: false,
      heartbeatTtlSeconds: fleetState.heartbeatTtlSeconds,
      recovery,
      // Healthy owns the Versions headline so a stale enrolling/restarting
      // rollout cannot also show a contradictory layout "Rolling out" banner.
      ownsActiveRolloutHeadline: true,
      failingReplica: null,
      wrongVersionReplica: null,
      pathHint: null,
    };
  }

  if (state === "converging") {
    return {
      density: "diagnostic",
      verdict,
      summaryLine: hasReplicaProof
        ? formatFleetConvergenceSummary(fleetState)
        : null,
      metrics: hasReplicaProof
        ? []
        : metricsFor(
            [
              "onDesired",
              ...(fleetState.mismatched > 0
                ? (["wrongVersion"] as const)
                : []),
              ...(fleetState.errors > 0 ? (["unhealthy"] as const) : []),
            ],
            fleetState,
          ),
      showReplicaChips: hasReplicaProof,
      showDesiredVersion: true,
      showSourceVersion: false,
      desiredVersion,
      desiredVersionHref,
      sourceVersion,
      showFreshnessWindow: false,
      heartbeatTtlSeconds: fleetState.heartbeatTtlSeconds,
      recovery,
      ownsActiveRolloutHeadline: true,
      failingReplica: pickFailingReplica(replicas),
      wrongVersionReplica: pickWrongVersionReplica(replicas, desiredVersion),
      pathHint: resolveFleetPathHint(fleetState),
    };
  }

  if (state === "degraded") {
    return {
      density: "diagnostic",
      verdict,
      summaryLine: hasReplicaProof
        ? formatFleetConvergenceSummary(fleetState)
        : null,
      metrics: hasReplicaProof
        ? []
        : metricsFor(
            ["onDesired", "wrongVersion", "unhealthy"],
            fleetState,
          ),
      showReplicaChips: hasReplicaProof,
      showDesiredVersion: true,
      showSourceVersion: true,
      desiredVersion,
      desiredVersionHref,
      sourceVersion,
      showFreshnessWindow: false,
      heartbeatTtlSeconds: fleetState.heartbeatTtlSeconds,
      recovery,
      ownsActiveRolloutHeadline: rolloutActive,
      failingReplica: pickFailingReplica(replicas),
      wrongVersionReplica: pickWrongVersionReplica(replicas, desiredVersion),
      pathHint: resolveFleetPathHint(fleetState),
    };
  }

  // unknown (including unrecognized states with a payload)
  return {
    density: "unknown",
    verdict,
    summaryLine: null,
    metrics: metricsFor(["minimum"], fleetState),
    showReplicaChips: false,
    showDesiredVersion: false,
    showSourceVersion: false,
    desiredVersion,
    desiredVersionHref,
    sourceVersion,
    showFreshnessWindow: fleetState.heartbeatTtlSeconds > 0,
    heartbeatTtlSeconds: fleetState.heartbeatTtlSeconds,
    recovery,
    ownsActiveRolloutHeadline: false,
    failingReplica: null,
    wrongVersionReplica: null,
    pathHint: null,
  };
}
