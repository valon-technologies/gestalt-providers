/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

/**
 * Type-adjacent Lucide mark: size, gap, and stroke track the line of type.
 *
 * Spec: `guidelines/icons.md` § Type-adjacent glyphs.
 * Tokens: `--inline-glyph-size` (1em) + `--inline-glyph-gap` (0.5em).
 * Stroke: `stroke-inline-glyph` maps ancestor `font-medium` / `font-bold`
 * onto Lucide (400→3.5, 500→6). Put the weight class on a common parent of
 * the glyph and the letters — not on a sibling span. Color: `text-current`.
 *
 * Distinct from `--control-icon-*` (button / select / chip chrome) and from
 * Badge's `[&>svg]:size-*` ladder.
 *
 * Gap ownership: flex/grid parent uses `gap-inline-glyph` and the glyph has no
 * margin. `inline-block` parent (SaveStatus) uses `me-inline-glyph` on the
 * glyph so the label still contributes the alphabetic baseline.
 */
export const inlineGlyphClassName =
  "inline-block size-inline-glyph shrink-0 align-inline-glyph me-inline-glyph stroke-inline-glyph text-current";
