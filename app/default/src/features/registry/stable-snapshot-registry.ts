import type {
  AppAdminFailedVersion,
  AppAdminPendingVersion,
  AppAdminPublication,
  AppAdminPublishedVersion,
  AppAdminRegistryResponse,
} from "@/lib/api";

type RegistryRollout = NonNullable<AppAdminRegistryResponse["rollout"]>;

export type SnapshotTableRegistrySlice = Pick<
  AppAdminRegistryResponse,
  | "publishedVersions"
  | "pendingVersions"
  | "failedVersions"
  | "desiredVersion"
  | "rollout"
  | "selectionDisabled"
  | "disabledReason"
  | "autoDeploy"
>;

function publicationKey(publication?: AppAdminPublication): string {
  if (!publication) return "";
  return JSON.stringify(publication);
}

export function rolloutKey(rollout?: RegistryRollout): string {
  if (!rollout) return "";
  return `${rollout.version}\0${rollout.state}`;
}

function publishedVersionKey(version: AppAdminPublishedVersion): string {
  return [
    version.version,
    version.publishedAt,
    version.publishDurationSeconds ?? "",
    version.publishStartedAt ?? "",
    version.sourceRef ?? "",
    version.sourceUrl ?? "",
    (version.platforms ?? []).join(","),
    publicationKey(version.publication),
  ].join("\0");
}

/** Pending timers render from startedAt + live clock — ignore server tick counters on poll. */
function pendingVersionKey(version: AppAdminPendingVersion): string {
  return [
    version.version,
    version.startedAt,
    version.phase,
    version.sourceRef ?? "",
    version.sourceUrl ?? "",
    publicationKey(version.publication),
  ].join("\0");
}

function failedVersionKey(version: AppAdminFailedVersion): string {
  return [
    version.version,
    version.startedAt,
    version.failedAt,
    version.reason,
    version.publishDurationSeconds ?? "",
    version.sourceRef ?? "",
    version.sourceUrl ?? "",
    publicationKey(version.publication),
  ].join("\0");
}

function versionListKey<T>(versions: T[], keyFor: (version: T) => string): string {
  return versions.map(keyFor).sort().join("\n");
}

/**
 * True when two registry payloads are equivalent for the versions table.
 * Ignores pending fields that only tick between polls (updatedAt,
 * publishingForSeconds) because the UI derives those from startedAt +
 * useLiveNow.
 */
export function snapshotRegistryPollEqual(
  left: SnapshotTableRegistrySlice,
  right: SnapshotTableRegistrySlice,
): boolean {
  if (left === right) return true;

  if (left.desiredVersion !== right.desiredVersion) return false;
  if (left.selectionDisabled !== right.selectionDisabled) return false;
  if (left.disabledReason !== right.disabledReason) return false;
  if (rolloutKey(left.rollout) !== rolloutKey(right.rollout)) return false;

  const leftAuto = left.autoDeploy;
  const rightAuto = right.autoDeploy;
  if ((leftAuto?.enabled ?? false) !== (rightAuto?.enabled ?? false)) return false;
  if (leftAuto?.pendingVersion !== rightAuto?.pendingVersion) return false;
  if (leftAuto?.lastError !== rightAuto?.lastError) return false;

  if (
    versionListKey(left.publishedVersions, publishedVersionKey) !==
    versionListKey(right.publishedVersions, publishedVersionKey)
  ) {
    return false;
  }

  if (
    versionListKey(left.pendingVersions ?? [], pendingVersionKey) !==
    versionListKey(right.pendingVersions ?? [], pendingVersionKey)
  ) {
    return false;
  }

  if (
    versionListKey(left.failedVersions ?? [], failedVersionKey) !==
    versionListKey(right.failedVersions ?? [], failedVersionKey)
  ) {
    return false;
  }

  return true;
}
