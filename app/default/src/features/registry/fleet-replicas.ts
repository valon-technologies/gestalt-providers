import type { AppAdminFleetReplica } from "@/features/registry/types";

/** Shorten a UUID-like instance id for table/tree display. */
export function shortInstanceId(instanceId: string): string {
  const id = instanceId.trim();
  if (id.length <= 12) return id;
  return id.slice(0, 8);
}

export function replicaClassLabel(className: string): string {
  switch (className) {
    case "on_desired":
      return "On desired version";
    case "mismatched":
      return "Wrong version";
    case "error":
      return "Unhealthy";
    default:
      return className || "Unknown";
  }
}

export type ReplicaClassMarkTone = "success" | "warning" | "danger" | "muted";

/** Status tone for replica status badges (indicator color). */
export function replicaClassMarkTone(className: string): ReplicaClassMarkTone {
  switch (className) {
    case "on_desired":
      return "success";
    case "mismatched":
      return "warning";
    case "error":
      return "danger";
    default:
      return "muted";
  }
}

/** Vivid fill for GitHub Actions–style status indicator shells. */
export function replicaClassDotClass(className: string): string {
  switch (replicaClassMarkTone(className)) {
    case "success":
      return "bg-status-indicator-success";
    case "warning":
      return "bg-status-indicator-warning";
    case "danger":
      return "bg-status-indicator-danger";
    case "muted":
      return "bg-status-indicator-muted";
  }
}

/**
 * GitHub Actions–style mark kind for a replica class.
 * success → check · warning/mismatched → in-progress spinner ·
 * danger → failed X · muted → skipped dash.
 */
export type ReplicaStatusIndicatorKind =
  | "success"
  | "pending"
  | "failure"
  | "skipped";

export function replicaStatusIndicatorKind(
  className: string,
): ReplicaStatusIndicatorKind {
  switch (replicaClassMarkTone(className)) {
    case "success":
      return "success";
    case "warning":
      return "pending";
    case "danger":
      return "failure";
    case "muted":
      return "skipped";
  }
}

function sortReplicas(replicas: AppAdminFleetReplica[]): AppAdminFleetReplica[] {
  return [...replicas].sort((a, b) => a.instanceId.localeCompare(b.instanceId));
}

export type FleetReplicasForSnapshotTable = {
  /** Replicas whose runningVersion matches a snapshot table row. */
  byVersion: Map<string, AppAdminFleetReplica[]>;
  /** Live replicas with no matching snapshot row (unknown / empty version). */
  orphans: AppAdminFleetReplica[];
};

/**
 * Join live replicas onto snapshot versions for table co-location.
 * Do not invent snapshot rows for orphan running versions.
 */
export function partitionFleetReplicasForSnapshotTable(
  replicas: AppAdminFleetReplica[] | undefined,
  snapshotVersions: Iterable<string>,
): FleetReplicasForSnapshotTable {
  const known = new Set(
    [...snapshotVersions].map((version) => version.trim()).filter(Boolean),
  );
  const byVersion = new Map<string, AppAdminFleetReplica[]>();
  const orphans: AppAdminFleetReplica[] = [];
  for (const replica of replicas ?? []) {
    const version = replica.runningVersion?.trim() || "";
    if (!version || !known.has(version)) {
      orphans.push(replica);
      continue;
    }
    const bucket = byVersion.get(version);
    if (bucket) bucket.push(replica);
    else byVersion.set(version, [replica]);
  }
  for (const [version, rows] of byVersion) {
    byVersion.set(version, sortReplicas(rows));
  }
  return { byVersion, orphans: sortReplicas(orphans) };
}

export function fleetReplicasPollKey(
  replicas: AppAdminFleetReplica[] | undefined,
): string {
  if (!replicas?.length) return "";
  return replicas
    .map((replica) =>
      [
        replica.instanceId,
        replica.class,
        replica.appState,
        replica.runningVersion ?? "",
        replica.lastError ?? "",
        replica.heartbeatAt,
      ].join("\0"),
    )
    .sort()
    .join("\n");
}
