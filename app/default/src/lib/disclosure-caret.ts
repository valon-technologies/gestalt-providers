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
 * `Button` with `aria-expanded`). Spacing (`ml-*`) stays at the call site —
 * Button already provides `gap-*`.
 */
export const disclosureCaretClassName =
  "size-4 shrink-0 opacity-50 transition-transform duration-overshoot ease-out-back group-data-[state=open]:rotate-180 group-aria-expanded:rotate-180";
