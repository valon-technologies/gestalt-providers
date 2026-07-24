import type { Integration } from "@/lib/api";

export function getIntegrationLabel(integration: Integration): string {
  return integration.displayName || integration.name;
}

function getSearchableFields(integration: Integration): string[] {
  return [
    integration.name,
    integration.displayName || "",
    integration.description || "",
  ];
}

export function tokenizeQuery(rawQuery: string): string[] {
  return rawQuery
    .trim()
    .toLowerCase()
    .split(/\s+/)
    .filter((token) => token.length > 0);
}

/** True when every query token appears somewhere in the haystack (word-fuzzy). */
export function matchesSearchQuery(haystack: string, rawQuery: string): boolean {
  const tokens = tokenizeQuery(rawQuery);
  if (tokens.length === 0) return true;
  const lower = haystack.toLowerCase();
  return tokens.every((token) => lower.includes(token));
}

export function filterIntegrations(
  integrations: Integration[],
  rawQuery: string,
): Integration[] {
  const query = rawQuery.trim();
  if (!query) {
    return integrations;
  }

  return integrations.filter((integration) =>
    matchesSearchQuery(getSearchableFields(integration).join(" "), query),
  );
}

export type MatchRange = { start: number; end: number };

/** Merged case-insensitive ranges for every query token in `text`. */
export function matchRangesForQuery(text: string, rawQuery: string): MatchRange[] {
  const tokens = tokenizeQuery(rawQuery);
  if (!text || tokens.length === 0) return [];

  const lower = text.toLowerCase();
  const ranges: MatchRange[] = [];

  for (const token of tokens) {
    let from = 0;
    while (from < lower.length) {
      const index = lower.indexOf(token, from);
      if (index === -1) break;
      ranges.push({ start: index, end: index + token.length });
      from = index + token.length;
    }
  }

  if (ranges.length === 0) return [];

  ranges.sort((a, b) => a.start - b.start || a.end - b.end);
  const merged: MatchRange[] = [];
  for (const range of ranges) {
    const last = merged[merged.length - 1];
    if (last && range.start <= last.end) {
      last.end = Math.max(last.end, range.end);
    } else {
      merged.push({ ...range });
    }
  }
  return merged;
}
