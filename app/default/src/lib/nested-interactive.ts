/**
 * Nested interactive ownership for navigable surfaces (table rows, cards, …).
 *
 * When a surface paints hover/press on itself (or on descendants) AND contains
 * controls that own their own hit targets, suppress the surface wash while those
 * controls are hovered/pressed — CSS `:has()`, not JS mouseenter bookkeeping.
 *
 * Normative: `guidelines/nested-interactive.md`.
 * Companions: `selectable-rows.md` (fills), `cards.md` (call-site composition),
 * `row-link.ts` (click intent for stretched anchors).
 *
 * Tailwind `@source` only emits utilities that appear as *complete* class
 * strings in source. Suppress presets below are therefore named surface
 * encodings (static literals), not open `rest`/`descendant` template builders.
 * Membership still has one SoT (`NESTED_INTERACTIVE_MATCHERS`); contract tests
 * lock each preset’s `:has()` arg to `nestedInteractiveHasArg`.
 */

/** Opt-out region: hovering/pressing here does not deepen the surface wash. */
export const NESTED_INTERACTIVE_OPT_OUT_ATTR = "data-no-row-click";

/**
 * Stretched navigation anchor for the surface itself (DataTable row link, card
 * stretch link). Excluded from `:has()` suppress selectors — its overlay covers
 * the whole surface, so matching it would cancel the surface wash permanently.
 */
export const SURFACE_LINK_ANCHOR_ATTR = "data-row-link";

const NESTED_INTERACTIVE_MATCHERS = [
  "a",
  "button",
  "input",
  "select",
  "textarea",
  "[role=button]",
  "[role=checkbox]",
  "[role=combobox]",
  `[${NESTED_INTERACTIVE_OPT_OUT_ATTR}]`,
] as const;

/** `Element.closest` selector — same membership as the `:has()` suppress list. */
export const NESTED_INTERACTIVE_SELECTOR = NESTED_INTERACTIVE_MATCHERS.join(",");

export type NestedInteractiveHasOptions = {
  /**
   * Anchor attr to exclude from the `a` match (default: `data-row-link`).
   * Pass `null` when the surface has no stretch overlay.
   */
  excludeAnchorAttr?: string | null;
  /**
   * Omit `a` from `:has()` — sibling-link rows where the destination anchor
   * should still deepen the painted surface (NavList action rows).
   */
  omitLinkMatchers?: boolean;
};

/**
 * CSS `:has()` argument for a pointer state. Suppress presets embed this
 * membership as complete Tailwind literals; contract tests assert equality so
 * CSS and `isInteractiveTarget` cannot drift.
 */
export function nestedInteractiveHasArg(
  state: "hover" | "active",
  options: NestedInteractiveHasOptions = {},
): string {
  const exclude =
    options.excludeAnchorAttr === undefined
      ? SURFACE_LINK_ANCHOR_ATTR
      : options.excludeAnchorAttr;

  const matchers = options.omitLinkMatchers
    ? NESTED_INTERACTIVE_MATCHERS.filter((matcher) => matcher !== "a")
    : NESTED_INTERACTIVE_MATCHERS;

  return matchers.map((matcher) => {
    if (matcher === "a" && exclude) {
      return `a:not([${exclude}]):${state}`;
    }
    return `${matcher}:${state}`;
  }).join(",");
}

/**
 * Named surface suppress encodings — each value must be one complete class
 * string literal (no `${…}` interpolation) so Tailwind `@source` emits the
 * `:has()` cancel rules. Add a new preset when a new rest fill / paint target
 * is needed; do not build utilities from `rest`/`descendant` strings at the
 * call site.
 *
 * @example
 * cn(
 *   "hover:bg-neutral-dark-hover active:bg-neutral-dark-pressed",
 *   nestedInteractiveSuppress.solidSecondary,
 * )
 */
export const nestedInteractiveSuppress = {
  /**
   * Parent paints children (TableBody → `tr`); restore transparent rest.
   */
  tableRow:
    "[&_tr:hover:has(a:not([data-row-link]):hover,button:hover,input:hover,select:hover,textarea:hover,[role=button]:hover,[role=checkbox]:hover,[role=combobox]:hover,[data-no-row-click]:hover)]:bg-transparent [&_tr:active:has(a:not([data-row-link]):active,button:active,input:active,select:active,textarea:active,[role=button]:active,[role=checkbox]:active,[role=combobox]:active,[data-no-row-click]:active)]:bg-transparent",

  /**
   * Self-target solid card / secondary rest (`bg-secondary` ≡ `--neutral-hover`).
   */
  solidSecondary:
    "[&:hover:has(a:not([data-row-link]):hover,button:hover,input:hover,select:hover,textarea:hover,[role=button]:hover,[role=checkbox]:hover,[role=combobox]:hover,[data-no-row-click]:hover)]:bg-secondary [&:active:has(a:not([data-row-link]):active,button:active,input:active,select:active,textarea:active,[role=button]:active,[role=checkbox]:active,[role=combobox]:active,[data-no-row-click]:active)]:bg-secondary",

  /**
   * Solid card on Neutral hover rest whose destination is a stretch
   * `data-row-link` (catalog tiles). Rest must be `bg-neutral-hover` — not
   * `bg-secondary` — so `neutral-dark-hover` is a visible L-step (tenant themes
   * may diverge `--secondary` from `--neutral-hover`). Omit `a` from `:has()` so
   * destination hover still deepens; Open/Add/More suppress via button /
   * data-no-row-click.
   */
  solidNeutralHoverStretchLink:
    "[&:hover:has(button:hover,input:hover,select:hover,textarea:hover,[role=button]:hover,[role=checkbox]:hover,[role=combobox]:hover,[data-no-row-click]:hover)]:bg-neutral-hover [&:active:has(button:active,input:active,select:active,textarea:active,[role=button]:active,[role=checkbox]:active,[role=combobox]:active,[data-no-row-click]:active)]:bg-neutral-hover",

  /**
   * Self-target outline card on white; restore `bg-card`.
   */
  outlineCard:
    "[&:hover:has(a:not([data-row-link]):hover,button:hover,input:hover,select:hover,textarea:hover,[role=button]:hover,[role=checkbox]:hover,[role=combobox]:hover,[data-no-row-click]:hover)]:bg-card [&:active:has(a:not([data-row-link]):active,button:active,input:active,select:active,textarea:active,[role=button]:active,[role=checkbox]:active,[role=combobox]:active,[data-no-row-click]:active)]:bg-card",

  /**
   * Selectable row with a sibling destination link — suppress only while a
   * trailing control owns the pointer; link hover still deepens the row wash.
   */
  selectableRowSiblingControl:
    "[&:hover:has(button:hover,input:hover,select:hover,textarea:hover,[role=button]:hover,[role=checkbox]:hover,[role=combobox]:hover,[data-no-row-click]:hover):not([data-selected]):not([data-soft])]:bg-transparent [&:active:has(button:active,input:active,select:active,textarea:active,[role=button]:active,[role=checkbox]:active,[role=combobox]:active,[data-no-row-click]:active):not([data-selected]):not([data-soft])]:bg-transparent [&:hover:has(button:hover,input:hover,select:hover,textarea:hover,[role=button]:hover,[role=checkbox]:hover,[role=combobox]:hover,[data-no-row-click]:hover):not([data-selected]):not([data-soft])]:text-muted-foreground [&:active:has(button:active,input:active,select:active,textarea:active,[role=button]:active,[role=checkbox]:active,[role=combobox]:active,[data-no-row-click]:active):not([data-selected]):not([data-soft])]:text-muted-foreground [&:hover:has(button:hover,input:hover,select:hover,textarea:hover,[role=button]:hover,[role=checkbox]:hover,[role=combobox]:hover,[data-no-row-click]:hover)[data-selected]]:bg-accent-vivid [&:active:has(button:active,input:active,select:active,textarea:active,[role=button]:active,[role=checkbox]:active,[role=combobox]:active,[data-no-row-click]:active)[data-selected]]:bg-accent-vivid [&:hover:has(button:hover,input:hover,select:hover,textarea:hover,[role=button]:hover,[role=checkbox]:hover,[role=combobox]:hover,[data-no-row-click]:hover)[data-selected]]:text-accent-vivid-foreground [&:active:has(button:active,input:active,select:active,textarea:active,[role=button]:active,[role=checkbox]:active,[role=combobox]:active,[data-no-row-click]:active)[data-selected]]:text-accent-vivid-foreground",
} as const;

export type NestedInteractiveSuppressPreset =
  keyof typeof nestedInteractiveSuppress;

/**
 * True when the click landed on a nested interactive. `exclude` lets a stretch
 * overlay ignore itself: clicking the overlay must navigate, while clicking a
 * control nested inside the linked cell must not.
 */
export function isInteractiveTarget(
  target: EventTarget | null,
  exclude?: Element | null,
): boolean {
  if (!(target instanceof HTMLElement)) return false;
  const hit = target.closest(NESTED_INTERACTIVE_SELECTOR);
  return hit !== null && hit !== exclude;
}
