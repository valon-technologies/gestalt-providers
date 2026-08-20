/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

/**
 * Shared disclosure / trigger chevron motion.
 *
 * Spec: `guidelines/transitions.md` § Caret rotate.
 * Tokens: `duration-overshoot` + `ease-out-back` (see `motion-tokens.md`).
 *
 * Put `group` on the open-state owner (Radix trigger with `data-state`, or a
 * `Button` with `aria-expanded`). This class is motion only.
 *
 * Size / ink: type-line carets add `size-inline-glyph stroke-inline-glyph
 * text-current` (`icons.md`) so they share the letters' weight and color.
 * Carets inside `Button` inherit `--control-icon-*` and add
 * `stroke-inline-glyph text-current` (same ink as the label). Do not add
 * `size-4`, `ml-2`, or `opacity-50`.
 */
export const disclosureCaretClassName =
  "shrink-0 transition-transform duration-overshoot ease-out-back motion-reduce:transition-none group-data-[state=open]:rotate-180 group-aria-expanded:rotate-180";
