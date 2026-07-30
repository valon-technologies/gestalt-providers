import type {
  AppAdminFleetState,
  AppAdminRegistryResponse,
} from "@/features/registry/types";

export type FleetStateBadgeVariant =
  | "success"
  | "warning"
  | "destructive"
  | "muted";

export type FleetStatePresentation = {
  label: string;
  description: string;
  badgeVariant: FleetStateBadgeVariant;
};

const UNKNOWN_FLEET: FleetStatePresentation = {
  label: "Unknown",
  description:
    "Runtime heartbeat data is unavailable, so current fleet health cannot be determined.",
  badgeVariant: "muted",
};

export function fleetStatePresentation(
  fleetState?: AppAdminFleetState,
): FleetStatePresentation {
  switch (fleetState?.state) {
    case "healthy":
      return {
        label: "Healthy",
        description: "Every live replica is running the desired version.",
        badgeVariant: "success",
      };
    case "converging":
      return {
        label: "Converging",
        description: "The live fleet is still converging on the desired version.",
        badgeVariant: "warning",
      };
    case "degraded":
      return {
        label: "Degraded",
        description:
          "Enough replicas are live, but one or more runtime observations are unhealthy.",
        badgeVariant: "destructive",
      };
    case "unknown":
      return {
        label: "Unknown",
        description:
          "There are not enough fresh heartbeats to determine current fleet health.",
        badgeVariant: "muted",
      };
    default:
      return UNKNOWN_FLEET;
  }
}

export function hasRecoveredFailedRollout(
  registry: Pick<AppAdminRegistryResponse, "rollout" | "recovery">,
): boolean {
  return registry.rollout?.state === "failed" && registry.recovery !== undefined;
}
