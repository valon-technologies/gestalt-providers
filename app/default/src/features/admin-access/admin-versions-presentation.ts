import type { TableStatusIndicatorVariant } from "@/components/ui/table-status-indicator";
import type {
  AdminFleetReplica,
  AdminFleetState,
  AdminRegistryAppSummary,
} from "@/lib/api";
import { isActiveRegistryRollout } from "@/features/registry/format";
import { fleetStatePresentation } from "@/features/registry/fleet-state";
import { fleetRolloutBadgeLabel } from "@/features/registry/rollout-stepper";
import type {
  AppAdminFleetReplica,
  AppAdminFleetReplicaClass,
} from "@/features/registry/types";
import { textContainsAllSearchTokens } from "@/lib/search-highlight";
import {
  APP_VERSIONS_COHORT,
  APP_VERSIONS_LAST_ROLLOUT_RELOADED,
  APP_VERSIONS_LIVE_ON_TARGET,
  APP_VERSIONS_LIVE_ON_TARGET_PARTIAL,
  APP_VERSIONS_NO_DATA,
  APP_VERSIONS_NOT_INSTALLED,
  APP_VERSIONS_ROLLOUT_RELOADED,
  APP_VERSIONS_ROW_META_SEP,
} from "./admin-access-copy";

export function adminFleetCapacityLabel(fleet: AdminFleetState): string {
  if (fleet.minimumHealthyInstances <= 0) {
    return `${fleet.liveInstances} live`;
  }
  return `${fleet.liveInstances}/${fleet.minimumHealthyInstances} live`;
}

export function adminFleetDiagnostic(fleet: AdminFleetState): string {
  if (
    fleet.minimumHealthyInstances <= 0 ||
    !fleet.sourceVersion ||
    !fleet.desiredVersion
  ) {
    return "Fleet basis unavailable";
  }
  if (fleet.liveInstances < fleet.minimumHealthyInstances) {
    return `Insufficient capacity: ${fleet.liveInstances}/${fleet.minimumHealthyInstances} live`;
  }
  if (fleet.mismatched > 0 || fleet.errors > 0) {
    const mismatch =
      fleet.mismatched === 1
        ? "1 version mismatch"
        : `${fleet.mismatched} version mismatches`;
    const errors =
      fleet.errors === 1 ? "1 error" : `${fleet.errors} errors`;
    return `${mismatch} · ${errors}`;
  }
  return `${fleet.runningDesiredVersion}/${fleet.liveInstances} running desired version`;
}

export function adminHeartbeatAgeLabel(seconds: number, fresh: boolean): string {
  const age = Math.max(0, Math.floor(seconds));
  if (!fresh) return `Stale, ${age}s ago`;
  return `${age}s ago`;
}

export function adminReplicaSourceLabel(
  status: AdminFleetReplica["sourceStatus"],
): string {
  switch (status) {
    case "current":
      return "Current source";
    case "superseded":
      return "Superseded source";
    default:
      return "Source unavailable";
  }
}

export function adminFleetRolloutNote(
  fleet: AdminFleetState,
  rolloutState?: string,
): string | null {
  if (fleet.state === "healthy" && rolloutState === "failed") {
    return "Current fleet is healthy; the last rollout remains failed.";
  }
  return null;
}

export function adminCohortLabel(cohort?: {
  restarted: number;
  acknowledged: number;
} | null): string {
  if (!cohort) return APP_VERSIONS_NO_DATA;
  return APP_VERSIONS_COHORT(cohort.restarted, cohort.acknowledged);
}

function adminVersionsLiveOnTargetLabel(
  replicas: AppAdminFleetReplica[],
): string | null {
  if (replicas.length === 0) return null;
  const onTarget = replicas.filter(
    (replica) => replica.class === "on_desired",
  ).length;
  if (onTarget === replicas.length) {
    return APP_VERSIONS_LIVE_ON_TARGET(replicas.length);
  }
  return APP_VERSIONS_LIVE_ON_TARGET_PARTIAL(onTarget, replicas.length);
}

function adminVersionsRolloutReloadedLabel(
  cohort: { restarted: number; acknowledged: number } | null | undefined,
  rolloutState?: string,
): string | null {
  if (!cohort || cohort.acknowledged <= 0) return null;
  const finished = cohort.restarted >= cohort.acknowledged;
  if (isActiveRegistryRollout(rolloutState) || !finished) {
    return APP_VERSIONS_ROLLOUT_RELOADED(
      cohort.restarted,
      cohort.acknowledged,
    );
  }
  return APP_VERSIONS_LAST_ROLLOUT_RELOADED(
    cohort.restarted,
    cohort.acknowledged,
  );
}

/**
 * One muted list-row line: live hosts on target, plus last-rollout cohort.
 * Those are separate clocks; do not imply they are the same count.
 */
export function adminVersionsRowMetaLine(
  replicas: AppAdminFleetReplica[],
  cohort?: { restarted: number; acknowledged: number } | null,
  rolloutState?: string,
): string | null {
  const live = adminVersionsLiveOnTargetLabel(replicas);
  const rollout = adminVersionsRolloutReloadedLabel(cohort, rolloutState);
  if (live && rollout) return `${live}${APP_VERSIONS_ROW_META_SEP}${rollout}`;
  return live ?? rollout;
}

export function adminVersionsAppSearchText(app: AdminRegistryAppSummary): string {
  return [
    app.app,
    app.registry,
    app.desiredVersion || APP_VERSIONS_NOT_INSTALLED,
    fleetRolloutBadgeLabel(app),
    adminVersionsRowMetaLine([], app.cohort, app.rollout?.state),
  ].join(" ");
}

export function filterAdminVersionsApps(
  apps: AdminRegistryAppSummary[],
  query: string,
): AdminRegistryAppSummary[] {
  const trimmed = query.trim();
  if (!trimmed) return apps;
  return apps.filter((app) =>
    textContainsAllSearchTokens(adminVersionsAppSearchText(app), trimmed),
  );
}

export function adminFleetBadge(fleet: AdminFleetState) {
  return fleetStatePresentation(fleet);
}

export function adminFleetIndicatorVariant(
  fleet: AdminFleetState,
): TableStatusIndicatorVariant {
  switch (fleetStatePresentation(fleet).badgeVariant) {
    case "success":
      return "success";
    case "warning":
      return "warning";
    case "destructive":
      return "danger";
    default:
      return "default";
  }
}

function adminReplicaClass(
  replica: AdminFleetReplica,
  desiredVersion?: string,
): AppAdminFleetReplicaClass {
  const observation = replica.appObservation;
  if (observation.state === "error") return "error";
  const running = observation.runningVersion?.trim() || "";
  const desired =
    observation.desiredVersion?.trim() || desiredVersion?.trim() || "";
  if (running && desired && running !== desired) return "mismatched";
  if (!running && desired) return "mismatched";
  return "on_desired";
}

export function adminFleetReplicaAsAppAdmin(
  replica: AdminFleetReplica,
  desiredVersion?: string,
): AppAdminFleetReplica {
  const observation = replica.appObservation;
  return {
    instanceId: replica.instanceId,
    startedAt: replica.startedAt,
    heartbeatAt: replica.heartbeatAt,
    appState: observation.state,
    runningVersion: observation.runningVersion,
    observedDesiredVersion:
      observation.desiredVersion || desiredVersion,
    observedAt: observation.observedAt,
    lastError: observation.lastError,
    class: adminReplicaClass(replica, desiredVersion),
  };
}

/**
 * Live current-source replicas for Registry fleet chips. Superseded-source
 * Gestaltd hosts still report per-app observations, but they are not the
 * current fleet — list `fleetState` only counts current-source live instances.
 */
export function adminFreshReplicasAsAppAdmin(
  replicas: AdminFleetReplica[] | undefined,
  desiredVersion?: string,
): AppAdminFleetReplica[] {
  return (replicas ?? [])
    .filter((replica) => replica.fresh && replica.currentSource)
    .map((replica) => adminFleetReplicaAsAppAdmin(replica, desiredVersion));
}
