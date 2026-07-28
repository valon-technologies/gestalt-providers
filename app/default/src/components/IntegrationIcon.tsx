import { useId } from "react";
import type { CSSProperties } from "react";
import { DefaultIcon } from "@/components/icons";
import { appInitials, describeBrandMark } from "@/lib/app-mark";
import { cn } from "@/lib/cn";
import { renderSafeIcon } from "@/lib/safe-svg";

/**
 * Canonical renderer for an app's mark. Sole owner of the brand-vs-monogram
 * choice, so every surface showing an app resolves it identically.
 *
 * The choice can only be made here: a brand mark counts as present when it
 * actually survives sanitization, which is not knowable before parsing.
 *
 * In `tile` mode the mark sits in a fixed bordered square, and how it fills that
 * square depends on how the artwork was authored. A glyph drawn on transparency
 * is inset, revealing the border, which gives every glyph the same optical size
 * regardless of the padding its author happened to bake into the viewBox. A mark
 * that paints its own background instead fills the tile edge to edge and is
 * clipped by the tile's radius, so its colour bleeds into the corners and covers
 * the border — the treatment app-icon grids use.
 */

const frameSizeClass = {
  sm: "size-8",
  md: "size-10",
  lg: "size-12",
  xl: "size-14",
} as const;

const glyphSizeClass = {
  sm: "[&>svg]:size-4",
  md: "[&>svg]:size-5",
  lg: "[&>svg]:size-7",
  xl: "[&>svg]:size-7",
} as const;

// Serif caps run optically smaller than the brand marks beside them, so the
// monogram sits above the glyph scale to hold equal weight in the grid.
const monogramSizeClass = {
  sm: "text-xs",
  md: "text-base",
  lg: "text-lg",
  xl: "text-xl",
} as const;

// A three-character monogram is half again as wide as a two-character one, so at
// the same size it runs to the tile's edges. Step it down to keep the same
// margin either side.
const wideMonogramSizeClass = {
  sm: "text-[0.625rem]",
  md: "text-sm",
  lg: "text-base",
  xl: "text-lg",
} as const;

export default function IntegrationIcon({
  iconSvg,
  name,
  displayName,
  className,
  size = "md",
  variant = "tile",
}: {
  iconSvg?: string;
  /** Stable app id. Used to derive a monogram when there is no brand mark. */
  name?: string;
  displayName?: string;
  className?: string;
  size?: keyof typeof frameSizeClass;
  /**
   * `tile` — bordered square; glyphs inset, full-bleed marks clipped flush.
   * `bare` — no chrome, for surfaces that supply their own.
   */
  variant?: "tile" | "bare";
}) {
  const iconIDPrefix = `provider-icon-${useId().replace(/:/g, "")}`;
  const iconNode = iconSvg ? renderSafeIcon(iconSvg, iconIDPrefix) : null;
  const hasBrandMark = iconNode != null;
  const shape = hasBrandMark && iconSvg ? describeBrandMark(iconSvg) : null;
  const fullBleed = shape?.fullBleed ?? false;
  const initials = hasBrandMark ? "" : appInitials(displayName, name ?? "");
  const tile = variant === "tile";

  return (
    <div
      data-testid="app-mark"
      data-full-bleed={fullBleed || undefined}
      style={
        shape
          ? ({
              "--mark-inset": `${(shape.inset * 100).toFixed(1)}%`,
              // The tile wears the mark's own background so the fill reaches the
              // tile's corners while the artwork sits inside the safe area.
              ...(shape.backgroundColor
                ? { backgroundColor: shape.backgroundColor }
                : {}),
            } as CSSProperties)
          : undefined
      }
      className={cn(
        "flex shrink-0 items-center justify-center overflow-hidden",
        // A brand mark carries the app's identity, so monochrome marks drawn in
        // currentColor take full ink. Only the placeholder glyph is muted.
        hasBrandMark ? "text-foreground" : "text-muted-foreground",
        frameSizeClass[size],
        tile && "rounded-lg",
        // A full-bleed mark covers the border anyway, and keeping it off avoids
        // a hairline seam showing through the clipped corners.
        tile && !fullBleed && "border border-border bg-background",
        hasBrandMark
          ? // Inset by the amount that normalises this mark's own baked padding,
            // so every glyph's ink lands at one optical size. A full-bleed mark
            // uses it as a safe area instead, clearing the rounded corners.
            "[&>svg]:size-[var(--mark-inset)]"
          : glyphSizeClass[size],
        className,
      )}
    >
      {hasBrandMark ? (
        iconNode
      ) : initials ? (
        <span
          data-testid="app-monogram"
          aria-hidden="true"
          className={cn(
            // Display face at normal weight — the display cut carries the
            // monogram; extra weight only muddies it at this size.
            // Full ink, not the frame's muted tone: a monogram *is* the app's
            // identity, so it reads as a mark rather than as secondary text.
            "select-none font-display font-normal tracking-tight",
            "text-foreground",
            // Size first: `text-base` also carries a line-height, so
            // tailwind-merge drops any `leading-*` that precedes it.
            (initials.length > 2 ? wideMonogramSizeClass : monogramSizeClass)[
              size
            ],
            "leading-none",
            // Trim the box to cap height so flex centring aligns the glyphs
            // rather than the serif's ascent and descent.
            "trim-cap",
          )}
        >
          {initials}
        </span>
      ) : (
        <DefaultIcon />
      )}
    </div>
  );
}
