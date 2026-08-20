import type { Integration } from "@/lib/api";
import {
  searchTokensFromQuery,
  textContainsAllSearchTokens,
} from "@/lib/search-highlight";

export function getIntegrationLabel(integration: Integration): string {
  return integration.displayName || integration.name;
}

/**
 * Catalog card subtitle. Omits empty copy and descriptions that only
 * repeat the app name.
 */
export function catalogCardDescription(integration: Integration): string | null {
  const description = integration.description?.trim() ?? "";
  if (!description) return null;
  if (
    description.toLocaleLowerCase() ===
    getIntegrationLabel(integration).toLocaleLowerCase()
  ) {
    return null;
  }
  return description;
}

function getSearchableFields(integration: Integration): string[] {
  return [
    integration.name,
    integration.displayName || "",
    integration.description || "",
  ];
}

/** True when every query token appears in the app name or description. */
export function integrationMatchesQuery(
  integration: Integration,
  rawQuery: string,
): boolean {
  if (!rawQuery.trim()) return true;
  return matchesSearchQuery(getSearchableFields(integration).join(" "), rawQuery);
}

/** Catalog filter tokens — delegates to vendored list-search normalization. */
export function tokenizeQuery(rawQuery: string): string[] {
  return searchTokensFromQuery(rawQuery);
}

/** True when every query token appears somewhere in the haystack (token-AND). */
export function matchesSearchQuery(haystack: string, rawQuery: string): boolean {
  return textContainsAllSearchTokens(haystack, rawQuery);
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
    integrationMatchesQuery(integration, query),
  );
}
