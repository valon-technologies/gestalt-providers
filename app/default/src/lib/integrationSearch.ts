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
