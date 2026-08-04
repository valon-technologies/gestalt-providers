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
  AppAdminPublication,
  AppAdminPublishedVersion,
  AppAdminSnapshotRow,
} from "@/features/registry/types";

export function formatPublicationLabel(
  publication?: AppAdminPublication,
): string | null {
  const pullRequest = publication?.triggerPullRequest;
  if (pullRequest?.number) {
    if (pullRequest.title?.trim()) {
      return `PR #${pullRequest.number} · ${pullRequest.title.trim()}`;
    }
    return `PR #${pullRequest.number}`;
  }
  if (publication?.workflowRunUrl) return "View workflow run";
  return null;
}

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

export function snapshotLastUpdatedAt(
  row: AppAdminSnapshotRow,
): string | null {
  if (row.kind === "pending") return row.pending.updatedAt;
  if (row.kind === "failed") return row.failed.failedAt;
  return row.published.publishedAt;
}

export function snapshotLastUpdatedLabel(
  row: AppAdminSnapshotRow,
  options?: { now?: number | Date; minRelativeUnit?: "second" | "minute" },
): { relative: string; absolute: string } | null {
  const value = snapshotLastUpdatedAt(row);
  if (!value) return null;
  const relative =
    formatRegistryTimeAgo(value, options?.now, {
      minUnit: options?.minRelativeUnit,
    }) || formatRegistryTime(value);
  const absolute = formatRegistryTime(value);
  if (!relative || relative === "—") return null;
  return { relative, absolute };
}

export function snapshotStatusTimer(
  row: AppAdminSnapshotRow,
  now?: number | Date,
): string | null {
  if (row.kind === "pending") {
    const seconds = durationSecondsBetween(row.pending.startedAt, now ?? Date.now());
    return seconds !== null ? `for ${formatDurationSeconds(seconds)}` : null;
  }
  if (row.kind === "published") {
    const duration =
      row.published.publishDurationSeconds ??
      (row.published.publishStartedAt
        ? durationSecondsBetween(row.published.publishStartedAt, row.published.publishedAt)
        : null);
    return duration !== null ? `Published in ${formatDurationSeconds(duration)}` : null;
  }
  const duration =
    row.failed.publishDurationSeconds ??
    durationSecondsBetween(row.failed.startedAt, row.failed.failedAt);
  return duration !== null ? `Failed after ${formatDurationSeconds(duration)}` : null;
}

export function snapshotFailedReason(row: AppAdminSnapshotRow): string | null {
  if (row.kind !== "failed") return null;
  return row.failed.reason?.trim() || null;
}
