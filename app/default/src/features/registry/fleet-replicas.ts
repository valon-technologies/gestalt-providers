import {
  formatRegistryTime,
  formatRegistryTimeAgo,
} from "@/features/registry/format";
import type {
  AppAdminFleetReplica,
  AppAdminFleetState,
} from "@/features/registry/types";

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
 * Status mark kind for a replica class.
 * success → check · mismatched → steady warning · danger → failed X ·
 * muted → skipped dash.
 *
 * Wrong version is a steady drift state — do not use an in-progress spinner
 * (that reads as “still converging”).
 */
export type ReplicaStatusIndicatorKind =
  | "success"
  | "warning"
  | "failure"
  | "skipped";

export function replicaStatusIndicatorKind(
  className: string,
): ReplicaStatusIndicatorKind {
  switch (replicaClassMarkTone(className)) {
    case "success":
      return "success";
    case "warning":
      return "warning";
    case "danger":
      return "failure";
    case "muted":
      return "skipped";
  }
}

/** Operator-facing process state — never leak raw appState enums in primary UI. */
export function formatReplicaAppState(appState: string | undefined): string {
  const raw = appState?.trim().toLowerCase() || "";
  switch (raw) {
    case "running":
      return "Running";
    case "error":
      return "Failed";
    case "starting":
      return "Starting";
    case "stopping":
      return "Stopping";
    case "stopped":
      return "Stopped";
    case "":
      return "—";
    default:
      return raw.charAt(0).toUpperCase() + raw.slice(1);
  }
}

/** Triage order: unhealthy → wrong version → on desired → other, then id. */
export function replicaTriageRank(className: string): number {
  switch (className) {
    case "error":
      return 0;
    case "mismatched":
      return 1;
    case "on_desired":
      return 2;
    default:
      return 3;
  }
}

export function sortReplicasByTriage(
  replicas: AppAdminFleetReplica[],
): AppAdminFleetReplica[] {
  return [...replicas].sort((a, b) => {
    const rank = replicaTriageRank(a.class) - replicaTriageRank(b.class);
    if (rank !== 0) return rank;
    return a.instanceId.localeCompare(b.instanceId);
  });
}

function sortReplicas(replicas: AppAdminFleetReplica[]): AppAdminFleetReplica[] {
  return sortReplicasByTriage(replicas);
}

/** Version alignment for progressive disclosure in the replica hover card. */
export type ReplicaVersionAlignment =
  | { kind: "aligned"; version: string }
  | { kind: "diverged"; running: string; expected: string }
  | { kind: "running_only"; version: string }
  | { kind: "expected_only"; version: string }
  | { kind: "unknown" };

export function replicaVersionAlignment(
  replica: AppAdminFleetReplica,
): ReplicaVersionAlignment {
  const running = replica.runningVersion?.trim() || "";
  const expected = replica.observedDesiredVersion?.trim() || "";
  if (running && expected) {
    return running === expected
      ? { kind: "aligned", version: running }
      : { kind: "diverged", running, expected };
  }
  if (running) return { kind: "running_only", version: running };
  if (expected) return { kind: "expected_only", version: expected };
  return { kind: "unknown" };
}

/**
 * Lean = healthy glance (identity + freshness). Dense = drift / error triage
 * with version compare and process state when they add signal.
 */
export type ReplicaHoverDensity = "lean" | "dense";

export function replicaHoverDensity(
  replica: AppAdminFleetReplica,
): ReplicaHoverDensity {
  if (replica.class !== "on_desired") return "dense";
  if (replica.lastError?.trim()) return "dense";
  const alignment = replicaVersionAlignment(replica);
  if (alignment.kind === "diverged") return "dense";
  const process = formatReplicaAppState(replica.appState);
  if (process !== "Running" && process !== "—") return "dense";
  return "lean";
}

export type ReplicaHoverFreshness = {
  relative: string;
  absolute: string;
  stale: boolean;
};

export function replicaHoverFreshness(
  heartbeatAt: string | undefined,
  options?: { now?: number | Date; heartbeatTtlSeconds?: number },
): ReplicaHoverFreshness {
  const now = options?.now ?? Date.now();
  const absolute = formatRegistryTime(heartbeatAt);
  const relative =
    formatRegistryTimeAgo(heartbeatAt, now, { minUnit: "second" }) || absolute;
  const ttl = options?.heartbeatTtlSeconds ?? 0;
  let stale = false;
  if (ttl > 0 && heartbeatAt) {
    const at = Date.parse(heartbeatAt);
    const nowMs = typeof now === "number" ? now : now.getTime();
    if (!Number.isNaN(at)) stale = nowMs - at > ttl * 1000;
  }
  return { relative, absolute, stale };
}

export type ReplicaHoverPresentation = {
  density: ReplicaHoverDensity;
  shortId: string;
  statusLabel: string;
  /** Clarifies deploy vs process when both axes are in play. */
  statusHint: string | null;
  freshness: ReplicaHoverFreshness;
  instanceId: string;
  /** Process label when it adds tension beyond the status line. */
  processLabel: string | null;
  alignment: ReplicaVersionAlignment;
  lastError: string | null;
};

/**
 * Canonical hover-card content model. UI renders this; do not re-derive
 * lean/dense field sets in components.
 */
export function buildReplicaHoverPresentation(
  replica: AppAdminFleetReplica,
  options?: { now?: number | Date; heartbeatTtlSeconds?: number },
): ReplicaHoverPresentation {
  const density = replicaHoverDensity(replica);
  const alignment = replicaVersionAlignment(replica);
  const process = formatReplicaAppState(replica.appState);
  const statusLabel = replicaClassLabel(replica.class);
  const lastError = replica.lastError?.trim() || null;

  let statusHint: string | null = null;
  if (replica.class === "mismatched" && process === "Running") {
    statusHint = "Process is running; deploy target does not match.";
  } else if (replica.class === "error" && process === "Failed") {
    statusHint = null;
  } else if (replica.class === "error" && process !== "—" && process !== "Failed") {
    statusHint = `Process: ${process}.`;
  }

  // When statusHint already names process vs deploy tension, omit the Process
  // fact row so "running" is not triplicated in dense mismatched hovers.
  const processLabel =
    density === "dense" &&
    process !== "—" &&
    !(replica.class === "mismatched" && process === "Running")
      ? process
      : null;

  return {
    density,
    shortId: shortInstanceId(replica.instanceId),
    statusLabel,
    statusHint,
    freshness: replicaHoverFreshness(replica.heartbeatAt, options),
    instanceId: replica.instanceId,
    processLabel,
    alignment: density === "dense" ? alignment : { kind: "unknown" },
    lastError: density === "dense" ? lastError : null,
  };
}

/** Row chip-rail summary when many identical healthy marks add no scan value. */
export function replicaRowSummary(
  replicas: AppAdminFleetReplica[],
): string | null {
  if (replicas.length < 3) return null;
  if (!replicas.every((replica) => replica.class === "on_desired")) return null;
  return `${replicas.length} on desired version`;
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

/**
 * Canonical presentation identity for a replica chip / hover facts.
 * Heartbeat is intentionally omitted — it is a liveness tick, not layout identity.
 */
export function replicaPresentationKey(replica: AppAdminFleetReplica): string {
  return [
    replica.instanceId,
    replica.class,
    replica.appState,
    replica.runningVersion ?? "",
    replica.observedDesiredVersion ?? "",
    replica.lastError ?? "",
  ].join("\0");
}

/**
 * Equality key for whether replica *presentation* changed (chips / hover facts
 * that affect layout or status). Omits `heartbeatAt`: fleet polls refresh that
 * timestamp often, and treating it as a remount signal closes controlled
 * HoverCards (pointer leave on trigger refresh / content reflow).
 */
export function fleetReplicasPollKey(
  replicas: AppAdminFleetReplica[] | undefined,
): string {
  if (!replicas?.length) return "";
  return replicas
    .map((replica) => replicaPresentationKey(replica))
    .sort()
    .join("\n");
}

/**
 * Liveness-only equality key. Presentation memos omit heartbeat so chips do not
 * remount; open freshness still needs this channel to re-render when polls patch
 * `heartbeatAt` onto presentation-stable replicas.
 */
export function fleetReplicasLivenessKey(
  replicas: AppAdminFleetReplica[] | undefined,
): string {
  if (!replicas?.length) return "";
  return replicas
    .map((replica) => `${replica.instanceId}\0${replica.heartbeatAt}`)
    .sort()
    .join("\n");
}

/**
 * Equality key for fleet-strip presentation driven by aggregates (summary,
 * path hint, callouts) — omits evaluatedAt ticks so heartbeat-only polls do
 * not remount chips.
 */
export function fleetStatePollKey(
  fleet: AppAdminFleetState | undefined,
): string {
  if (!fleet) return "";
  return [
    fleet.state,
    fleet.desiredVersion ?? "",
    fleet.sourceVersion ?? "",
    String(fleet.minimumHealthyInstances),
    String(fleet.liveInstances),
    String(fleet.runningDesiredVersion),
    String(fleet.mismatched),
    String(fleet.errors),
    String(fleet.heartbeatTtlSeconds),
  ].join("\0");
}

/** Presentation fields only — heartbeat is a liveness tick, not chip identity. */
export function fleetReplicaPresentationEqual(
  left: AppAdminFleetReplica,
  right: AppAdminFleetReplica,
): boolean {
  return replicaPresentationKey(left) === replicaPresentationKey(right);
}

/**
 * Keep stable replica object identity across polls when presentation is equal.
 * Heartbeat-only updates patch onto the previous object so chip triggers can
 * memo-skip while open cards still receive a new heartbeatAt when it changes.
 */
export function reconcileFleetReplicas(
  previous: AppAdminFleetReplica[] | undefined,
  next: AppAdminFleetReplica[] | undefined,
): AppAdminFleetReplica[] | undefined {
  if (!next) return next;
  if (!previous?.length) return next;

  const prevById = new Map(
    previous.map((replica) => [replica.instanceId, replica]),
  );
  let allReuse = next.length === previous.length;
  const reconciled = next.map((replica) => {
    const prior = prevById.get(replica.instanceId);
    if (!prior || !fleetReplicaPresentationEqual(prior, replica)) {
      allReuse = false;
      return replica;
    }
    if (prior.heartbeatAt === replica.heartbeatAt) {
      return prior;
    }
    allReuse = false;
    return { ...prior, heartbeatAt: replica.heartbeatAt };
  });

  if (allReuse) {
    for (let i = 0; i < previous.length; i++) {
      if (previous[i] !== reconciled[i]) {
        allReuse = false;
        break;
      }
    }
  }
  return allReuse ? previous : reconciled;
}

export function reconcileFleetState(
  previous: AppAdminFleetState | undefined,
  next: AppAdminFleetState | undefined,
): AppAdminFleetState | undefined {
  if (!next) return next;
  if (!previous) return next;

  const replicas = reconcileFleetReplicas(previous.replicas, next.replicas);
  if (
    replicas === previous.replicas &&
    previous.state === next.state &&
    previous.sourceVersion === next.sourceVersion &&
    previous.desiredVersion === next.desiredVersion &&
    previous.minimumHealthyInstances === next.minimumHealthyInstances &&
    previous.liveInstances === next.liveInstances &&
    previous.runningDesiredVersion === next.runningDesiredVersion &&
    previous.mismatched === next.mismatched &&
    previous.errors === next.errors &&
    previous.heartbeatTtlSeconds === next.heartbeatTtlSeconds
  ) {
    // evaluatedAt / other ticks may differ — keep prior object so strip memo
    // and chip triggers are not invalidated by heartbeat-only polls.
    return previous;
  }

  if (replicas === next.replicas) return next;
  return { ...next, replicas };
}
