import type { Integration } from "@/lib/api";
import { getIntegrationLabel } from "@/lib/integrationSearch";

/** First page of optional apps on Setup Connect (two-column card grid). */
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
