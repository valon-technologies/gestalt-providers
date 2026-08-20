import type { Integration } from "@/lib/api";
import {
  catalogBucketIdFor,
  catalogBucketsPresentIn,
  CATALOG_BUCKETS,
  type CatalogBucket,
} from "@/lib/catalogBuckets";
import {
  filterIntegrations,
  getIntegrationLabel,
  integrationMatchesQuery,
  matchesSearchQuery,
} from "@/lib/integrationSearch";

/** First page of optional apps on the Setup apps step (two-column card grid). */
export const SETUP_APPS_PAGE_SIZE = 8;

export const SETUP_APPS_CATEGORY_ALL = "all";

export const SETUP_APPS_GRID_CLASS =
  "grid grid-cols-1 items-stretch gap-4 sm:grid-cols-2";

/** Overlapping marks on the See-more trigger — ChatGPT plugins store pattern. */
export const SETUP_SEE_MORE_ICON_COUNT = 3;

/**
 * ChatGPT-style overflow label. Names the next one or two apps; three or more
 * remaining become “See A, B, and more”.
 */
export function setupSeeMoreLabel(labels: readonly string[]): string {
  const [first, second] = labels;
  if (!first) return "See more";
  if (!second) return `See ${first}`;
  if (labels.length === 2) return `See ${first} and ${second}`;
  return `See ${first}, ${second}, and more`;
}

export function setupSeeMorePreview(
  remaining: Integration[],
): { icons: Integration[]; label: string } {
  const icons = remaining.slice(0, SETUP_SEE_MORE_ICON_COUNT);
  const labels = remaining.map(getIntegrationLabel);
  return { icons, label: setupSeeMoreLabel(labels) };
}

export type SetupConnectSuggestedItem = {
  appId: string;
  integration: Integration | undefined;
};

/**
 * Setup apps catalog: search uses the same token-AND matcher as /apps,
 * then ANDs the More-apps category chip. A query shows every match — it does
 * not leave hits behind See more.
 */
export function presentSetupConnectApps({
  suggested,
  more,
  query,
  category,
  visibleCount,
  labelFor,
}: {
  suggested: SetupConnectSuggestedItem[];
  more: Integration[];
  query: string;
  category: string;
  visibleCount: number;
  labelFor: (appId: string) => string;
}): {
  visibleSuggested: SetupConnectSuggestedItem[];
  categoryChips: CatalogBucket[];
  effectiveCategory: string;
  filteredMore: Integration[];
  visibleMore: Integration[];
  remainingMore: Integration[];
  moreSectionTitle: string;
  hasSearchQuery: boolean;
} {
  const hasSearchQuery = query.trim().length > 0;
  const visibleSuggested = hasSearchQuery
    ? suggested.filter((item) => {
        if (item.integration) {
          return integrationMatchesQuery(item.integration, query);
        }
        return matchesSearchQuery(labelFor(item.appId), query);
      })
    : suggested;
  const searchScopedMore = filterIntegrations(more, query);
  const categoryChips = catalogBucketsPresentIn(searchScopedMore);
  const effectiveCategory =
    category === SETUP_APPS_CATEGORY_ALL ||
    categoryChips.some((bucket) => bucket.id === category)
      ? category
      : SETUP_APPS_CATEGORY_ALL;
  const filteredMore =
    effectiveCategory === SETUP_APPS_CATEGORY_ALL
      ? searchScopedMore
      : searchScopedMore.filter(
          (integration) => catalogBucketIdFor(integration) === effectiveCategory,
        );
  const pageMatches = hasSearchQuery
    ? filteredMore
    : filteredMore.slice(0, visibleCount);
  const remainingMore = hasSearchQuery
    ? []
    : filteredMore.slice(visibleCount);
  const activeCategory =
    effectiveCategory === SETUP_APPS_CATEGORY_ALL
      ? null
      : (CATALOG_BUCKETS.find((bucket) => bucket.id === effectiveCategory) ??
        null);

  return {
    visibleSuggested,
    categoryChips,
    effectiveCategory,
    filteredMore,
    visibleMore: pageMatches,
    remainingMore,
    moreSectionTitle: activeCategory?.label ?? "More apps",
    hasSearchQuery,
  };
}
