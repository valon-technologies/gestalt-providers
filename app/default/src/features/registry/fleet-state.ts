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
    "Replica report data is unavailable, so fleet health can't be determined.",
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
        label: "Rolling out",
        // Strip summary + path hint own the explanation — no restating sentence.
        description: "",
        badgeVariant: "warning",
      };
    case "degraded":
      return {
        label: "Degraded",
        description:
          "Enough replicas are live, but at least one has a version mismatch or error.",
        badgeVariant: "destructive",
      };
    case "unknown":
      return {
        label: "Unknown",
        description:
          "Not enough fresh replica reports to determine fleet health.",
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
