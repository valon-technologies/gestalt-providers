import {
  durationSecondsBetween,
  formatDurationSeconds,
  formatRegistryTime,
  formatRegistryTimeAgo,
  sortPublishedVersionsNewestFirst,
} from "@/features/registry/format";
import type {
  AppAdminFailedVersion,
  AppAdminPendingVersion,
  AppAdminPublishedVersion,
  AppAdminSnapshotRow,
} from "@/features/registry/types";

export function buildAppAdminSnapshotRows(registry: {
  publishedVersions: AppAdminPublishedVersion[];
  pendingVersions?: AppAdminPendingVersion[];
  failedVersions?: AppAdminFailedVersion[];
}): AppAdminSnapshotRow[] {
  const publishedByVersion = new Map(
    registry.publishedVersions.map((version) => [version.version, version]),
  );
  const pendingByVersion = new Map(
    (registry.pendingVersions ?? []).map((version) => [version.version, version]),
  );
  const rows: AppAdminSnapshotRow[] = [];

  for (const published of sortPublishedVersionsNewestFirst(registry.publishedVersions)) {
    rows.push({
      kind: "published",
      version: published.version,
      sortAt: published.publishedAt,
      published,
    });
  }

  for (const pending of registry.pendingVersions ?? []) {
    if (publishedByVersion.has(pending.version)) continue;
    rows.push({
      kind: "pending",
      version: pending.version,
      sortAt: pending.startedAt,
      pending,
    });
  }

  for (const failed of registry.failedVersions ?? []) {
    if (publishedByVersion.has(failed.version)) continue;
    if (pendingByVersion.has(failed.version)) continue;
    rows.push({
      kind: "failed",
      version: failed.version,
      sortAt: failed.failedAt,
      failed,
    });
  }

  return rows.sort((left, right) => {
    const leftAt = Date.parse(left.sortAt);
    const rightAt = Date.parse(right.sortAt);
    if (!Number.isNaN(leftAt) && !Number.isNaN(rightAt) && leftAt !== rightAt) {
      return rightAt - leftAt;
    }
    return right.version.localeCompare(left.version);
  });
}

export function snapshotPublishedPrimaryLabel(
  row: AppAdminSnapshotRow,
  now: number | Date = Date.now(),
): string {
  if (row.kind === "pending") {
    const seconds =
      row.pending.publishingForSeconds ??
      durationSecondsBetween(row.pending.startedAt, now);
    return seconds !== null ? `for ${formatDurationSeconds(seconds)}` : "—";
  }
  if (row.kind === "failed") {
    return formatRegistryTimeAgo(row.failed.failedAt, now) || formatRegistryTime(row.failed.failedAt);
  }
  return formatRegistryTimeAgo(row.published.publishedAt, now) || formatRegistryTime(row.published.publishedAt);
}

export function snapshotPublishedSecondaryLabel(
  row: AppAdminSnapshotRow,
): string | null {
  if (row.kind === "published") {
    const duration =
      row.published.publishDurationSeconds ??
      (row.published.publishStartedAt
        ? durationSecondsBetween(row.published.publishStartedAt, row.published.publishedAt)
        : null);
    if (duration === null) return null;
    return `in ${formatDurationSeconds(duration)}`;
  }
  if (row.kind === "failed") {
    const duration =
      row.failed.publishDurationSeconds ??
      durationSecondsBetween(row.failed.startedAt, row.failed.failedAt);
    const parts: string[] = [];
    if (duration !== null) {
      parts.push(`after ${formatDurationSeconds(duration)}`);
    }
    const reason = row.failed.reason?.trim();
    if (reason) {
      parts.push(reason);
    }
    return parts.length > 0 ? parts.join(" · ") : null;
  }
  return null;
}
