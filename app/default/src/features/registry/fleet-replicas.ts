import type { AppAdminFleetReplica } from "@/features/registry/types";

export type FleetReplicaVersionGroup = {
  version: string;
  replicas: AppAdminFleetReplica[];
};

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

/**
 * Group live replicas by observed running version for the Versions tree.
 * Empty / missing runningVersion sorts last under "(no running version)".
 */
export function groupFleetReplicasByRunningVersion(
  replicas: AppAdminFleetReplica[] | undefined,
): FleetReplicaVersionGroup[] {
  if (!replicas?.length) return [];
  const byVersion = new Map<string, AppAdminFleetReplica[]>();
  for (const replica of replicas) {
    const version = replica.runningVersion?.trim() || "";
    const bucket = byVersion.get(version);
    if (bucket) bucket.push(replica);
    else byVersion.set(version, [replica]);
  }
  const groups = [...byVersion.entries()].map(([version, rows]) => ({
    version,
    replicas: [...rows].sort((a, b) =>
      a.instanceId.localeCompare(b.instanceId),
    ),
  }));
  groups.sort((a, b) => {
    if (!a.version && b.version) return 1;
    if (a.version && !b.version) return -1;
    return b.replicas.length - a.replicas.length || a.version.localeCompare(b.version);
  });
  return groups;
}
