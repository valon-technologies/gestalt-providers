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

export function formatRegistryTime(value?: string | null): string {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

export function formatRegistryTimeAgo(
  value?: string | null,
  now: number | Date = Date.now(),
): string {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  const nowMs = typeof now === "number" ? now : now.getTime();
  const deltaSeconds = Math.round((date.getTime() - nowMs) / 1000);
  const absSeconds = Math.abs(deltaSeconds);

  for (const { unit, seconds } of TIME_AGO_UNITS) {
    if (absSeconds >= seconds || unit === "second") {
      return relativeTimeFormatter.format(Math.round(deltaSeconds / seconds), unit);
    }
  }

  return "";
}

export function formatPublishedVersionOptionLabel(
  version: AppAdminPublishedVersion,
  now?: number | Date,
): string {
  const parts = [version.version];
  const pullRequest = version.publication?.triggerPullRequest;
  if (pullRequest?.number) {
    parts.push(`PR #${pullRequest.number}`);
  }
  const ago = formatRegistryTimeAgo(version.publishedAt, now);
  if (ago) {
    parts.push(ago);
  }
  return parts.join(" · ");
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
