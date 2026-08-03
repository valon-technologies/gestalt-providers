import type {
  AppAdminFleetState,
  AppAdminRecovery,
  AppAdminRegistryResponse,
} from "@/features/registry/types";
import {
  fleetStatePresentation,
  hasRecoveredFailedRollout,
  type FleetStatePresentation,
} from "@/features/registry/fleet-state";

export type FleetMetricId =
  | "live"
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

export type FleetStatusView = {
  density: FleetStatusDensity;
  verdict: FleetStatePresentation;
  metrics: FleetMetricFact[];
  showIdentity: boolean;
  desiredVersion?: string;
  sourceVersion?: string;
  showEvaluated: boolean;
  showFreshnessWindow: boolean;
  evaluatedAt?: string;
  heartbeatTtlSeconds: number;
  recovery: AppAdminRecovery | null;
};

const METRIC: Record<
  FleetMetricId,
  { label: string; testId: string; value: (fleet: AppAdminFleetState) => number }
> = {
  live: {
    label: "Live replicas",
    testId: "fleet-live-instances",
    value: (fleet) => fleet.liveInstances,
  },
  minimum: {
    label: "Minimum live replicas",
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

/**
 * State-gated runtime truth for the Versions surface.
 * Which facts appear is a function of fleet health — not a fixed dashboard.
 */
export function presentFleetStatus(
  registry: Pick<
    AppAdminRegistryResponse,
    "desiredVersion" | "fleetState" | "recovery" | "rollout"
  >,
): FleetStatusView {
  const { fleetState } = registry;
  const verdict = fleetStatePresentation(fleetState);
  const recovery = hasRecoveredFailedRollout(registry)
    ? (registry.recovery ?? null)
    : null;

  if (!fleetState) {
    return {
      density: "unknown",
      verdict,
      metrics: [],
      showIdentity: false,
      showEvaluated: false,
      showFreshnessWindow: false,
      heartbeatTtlSeconds: 0,
      recovery,
    };
  }

  const desiredVersion =
    fleetState.desiredVersion || registry.desiredVersion || undefined;
  const sourceVersion = fleetState.sourceVersion || undefined;
  const state = fleetState.state;

  if (state === "healthy") {
    return {
      density: "quiet",
      verdict,
      metrics: metricsFor(["live"], fleetState),
      showIdentity: false,
      showEvaluated: true,
      showFreshnessWindow: false,
      evaluatedAt: fleetState.evaluatedAt || undefined,
      heartbeatTtlSeconds: fleetState.heartbeatTtlSeconds,
      recovery,
    };
  }

  if (state === "converging") {
    const ids: FleetMetricId[] = ["live", "minimum", "onDesired"];
    if (fleetState.mismatched > 0) ids.push("wrongVersion");
    if (fleetState.errors > 0) ids.push("unhealthy");
    return {
      density: "diagnostic",
      verdict,
      metrics: metricsFor(ids, fleetState),
      showIdentity: true,
      desiredVersion,
      sourceVersion,
      showEvaluated: true,
      showFreshnessWindow: false,
      evaluatedAt: fleetState.evaluatedAt || undefined,
      heartbeatTtlSeconds: fleetState.heartbeatTtlSeconds,
      recovery,
    };
  }

  if (state === "degraded") {
    return {
      density: "diagnostic",
      verdict,
      metrics: metricsFor(
        ["live", "minimum", "onDesired", "wrongVersion", "unhealthy"],
        fleetState,
      ),
      showIdentity: true,
      desiredVersion,
      sourceVersion,
      showEvaluated: true,
      showFreshnessWindow: false,
      evaluatedAt: fleetState.evaluatedAt || undefined,
      heartbeatTtlSeconds: fleetState.heartbeatTtlSeconds,
      recovery,
    };
  }

  // unknown (including unrecognized states with a payload)
  return {
    density: "unknown",
    verdict,
    metrics: metricsFor(["live", "minimum"], fleetState),
    showIdentity: false,
    showEvaluated: true,
    showFreshnessWindow: fleetState.heartbeatTtlSeconds > 0,
    evaluatedAt: fleetState.evaluatedAt || undefined,
    heartbeatTtlSeconds: fleetState.heartbeatTtlSeconds,
    recovery,
  };
}
