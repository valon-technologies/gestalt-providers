/**
 * Row/card stretch-link click intent.
 *
 * A navigable surface renders ONE real `<a href>` with a stretched `::after`
 * overlay (`data-row-link`), so right-click, middle-click, cmd/ctrl-click,
 * hover URL preview, and "open in new tab" use native browser semantics.
 * The pure decision below is what plain left-click does; everything else is
 * the browser.
 *
 * Nested interactive membership (hover suppress + click guards) lives in
 * `@/lib/nested-interactive` — the single source of truth shared with cards
 * and other navigable surfaces. Re-exports keep DataTable import paths stable.
 */

import {
  isInteractiveTarget,
  NESTED_INTERACTIVE_SELECTOR,
  SURFACE_LINK_ANCHOR_ATTR,
} from "@/lib/nested-interactive";

export { isInteractiveTarget };

/** Stable DataTable / card aliases — membership SoT is `@/lib/nested-interactive`. */
export const ROW_LINK_ANCHOR_ATTR = SURFACE_LINK_ANCHOR_ATTR;
export const ROW_LINK_INTERACTIVE_SELECTOR = NESTED_INTERACTIVE_SELECTOR;

export type RowLinkClickIntent = "navigate" | "native" | "suppress";

export type RowLinkClickEvent = {
  button: number;
  metaKey: boolean;
  ctrlKey: boolean;
  shiftKey: boolean;
  altKey: boolean;
  targetIsInteractive: boolean;
};

/**
 * What a left-click on a stretch-linked surface should do:
 * - `"native"`: modifier or non-primary button — let the browser open a new
 *   tab/window. Regression guard for cmd/ctrl/shift-click.
 * - `"suppress"`: click hit a nested interactive — do nothing.
 * - `"navigate"`: plain primary click — caller may SPA-navigate via preventDefault.
 *
 * Middle-click never reaches here (browser fires the native anchor default).
 */
export function rowLinkClickIntent(
  event: RowLinkClickEvent,
): RowLinkClickIntent {
  if (event.button !== 0) return "native";
  if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
    return "native";
  }
  if (event.targetIsInteractive) return "suppress";
  return "navigate";
}
