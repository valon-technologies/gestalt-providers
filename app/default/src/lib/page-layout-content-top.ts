import type { CSSProperties } from "react";

/**
 * Shared page-layout measures for docs, Setup, and Apps.
 */

/**
 * Page top padding (`pt-16`) so content and the sticky PageLayout Pane share
 * one seam under app chrome.
 */
export const PAGE_LAYOUT_CONTENT_TOP_GAP = "4rem";

export const pageLayoutContentTopStyle = {
  "--page-layout-pane-top": `calc(var(--app-sticky-chrome-height) + ${PAGE_LAYOUT_CONTENT_TOP_GAP})`,
  "--page-layout-anchor-offset": `calc(var(--app-sticky-chrome-height) + ${PAGE_LAYOUT_CONTENT_TOP_GAP})`,
} as CSSProperties;

/**
 * Center reading column. Docs, Setup, and single-column Settings tasks share
 * this so the measure stays in the 60-70 character band. Do not restyle with
 * a one-off max-w-*.
 */
export const PAGE_LAYOUT_READING_COLUMN_CLASS =
  "mx-auto min-w-0 w-full max-w-[65ch]";

