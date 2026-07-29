import type { AppAdminPublishedVersion } from "@/features/registry/types";

const TIME_AGO_UNITS: Array<{ unit: Intl.RelativeTimeFormatUnit; seconds: number }> = [
  { unit: "year", seconds: 31_536_000 },
  { unit: "month", seconds: 2_592_000 },
  { unit: "week", seconds: 604_800 },
  { unit: "day", seconds: 86_400 },
  { unit: "hour", seconds: 3_600 },
  { unit: "minute", seconds: 60 },
  { unit: "second", seconds: 1 },
];

const relativeTimeFormatter = new Intl.RelativeTimeFormat(undefined, {
  numeric: "auto",
});

export function formatDurationSeconds(totalSeconds: number): string {
  const seconds = Math.max(0, Math.round(totalSeconds));
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  if (minutes < 60) {
    return remainder > 0 ? `${minutes}m ${remainder}s` : `${minutes}m`;
  }
  const hours = Math.floor(minutes / 60);
  const remMinutes = minutes % 60;
  return remMinutes > 0 ? `${hours}h ${remMinutes}m` : `${hours}h`;
}

export function durationSecondsBetween(
  start?: string | null,
  end?: string | number | Date | null,
): number | null {
  if (!start || end === null || end === undefined) return null;
  const startMs = Date.parse(start);
  const endMs =
    typeof end === "number"
      ? end
      : end instanceof Date
        ? end.getTime()
        : Date.parse(end);
  if (Number.isNaN(startMs) || Number.isNaN(endMs) || endMs <= startMs) return null;
  return Math.round((endMs - startMs) / 1000);
}

export function formatRegistryTime(value?: string | null): string {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

export function formatRegistryTimeShort(
  value?: string | number | Date | null,
): string {
  if (value === null || value === undefined) return "—";
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleTimeString(undefined, {
    hour: "numeric",
    minute: "2-digit",
  });
}

export function formatRegistryTimeAgo(
  value?: string | null,
  now: number | Date = Date.now(),
  options?: { minUnit?: "second" | "minute" },
): string {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  const nowMs = typeof now === "number" ? now : now.getTime();
  const deltaSeconds = Math.round((date.getTime() - nowMs) / 1000);
  const absSeconds = Math.abs(deltaSeconds);
  const minUnit = options?.minUnit ?? "second";

  for (const { unit, seconds } of TIME_AGO_UNITS) {
    if (minUnit === "minute" && unit === "second") continue;
    if (absSeconds >= seconds || unit === "second") {
      return relativeTimeFormatter.format(Math.round(deltaSeconds / seconds), unit);
    }
  }

  if (minUnit === "minute" && absSeconds < 60) {
    return relativeTimeFormatter.format(deltaSeconds < 0 ? -1 : 0, "minute");
  }

  return "";
}

export function formatPublishedVersionOptionMeta(
  version: AppAdminPublishedVersion,
  now?: number | Date,
): string {
  const parts: string[] = [];
  const pullRequest = version.publication?.triggerPullRequest;
  if (pullRequest?.number) {
    parts.push(
      pullRequest.title
        ? `PR #${pullRequest.number} · ${pullRequest.title}`
        : `PR #${pullRequest.number}`,
    );
  }
  const ago = formatRegistryTimeAgo(version.publishedAt, now);
  if (ago) {
    parts.push(ago);
  }
  return parts.join(" · ");
}

export function formatPublishedVersionOptionLabel(
  version: AppAdminPublishedVersion,
  now?: number | Date,
): string {
  const meta = formatPublishedVersionOptionMeta(version, now);
  return meta ? `${version.version} · ${meta}` : version.version;
}

export function sortPublishedVersionsNewestFirst(
  versions: AppAdminPublishedVersion[],
): AppAdminPublishedVersion[] {
  return versions.slice().sort((left, right) => {
    const leftAt = Date.parse(left.publishedAt);
    const rightAt = Date.parse(right.publishedAt);
    if (!Number.isNaN(leftAt) && !Number.isNaN(rightAt) && leftAt !== rightAt) {
      return rightAt - leftAt;
    }
    return right.version.localeCompare(left.version);
  });
}

export function shortenSourceRef(sourceRef?: string): string {
  const ref = sourceRef?.trim();
  if (!ref) return "";
  return ref.length > 7 ? ref.slice(0, 7) : ref;
}

export function isActiveRegistryRollout(state?: string): boolean {
  return state === "enrolling" || state === "restarting";
}

export function formatRolloutStateLabel(state: string): string {
  const trimmed = state.trim();
  if (!trimmed) return trimmed;
  return trimmed.charAt(0).toUpperCase() + trimmed.slice(1);
}
