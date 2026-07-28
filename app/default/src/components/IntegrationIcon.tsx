import { useId } from "react";
import { DefaultIcon } from "@/components/icons";
import { appInitials } from "@/lib/app-mark";
import { cn } from "@/lib/cn";
import { renderSafeIcon } from "@/lib/safe-svg";

/**
 * Canonical renderer for an app's mark. Sole owner of the brand-vs-monogram
 * choice, so every surface showing an app resolves it identically.
 *
 * The choice can only be made here: a brand mark counts as present when it
 * actually survives sanitization, which is not knowable before parsing.
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
// monogram sits one step above the glyph scale to hold equal weight in the grid.
const monogramSizeClass = {
  sm: "text-sm",
  md: "text-base",
  lg: "text-lg",
  xl: "text-xl",
} as const;

export default function IntegrationIcon({
  iconSvg,
  name,
  displayName,
  className,
  size = "md",
  variant = "bare",
}: {
  iconSvg?: string;
  /** Stable app id. Used to derive a monogram when there is no brand mark. */
  name?: string;
  displayName?: string;
  className?: string;
  size?: keyof typeof frameSizeClass;
  /**
   * `bare` — frameless, for cards that own their own hover plate.
   * `plated` — muted tile behind the mark.
   */
  variant?: "bare" | "plated";
}) {
  const iconIDPrefix = `provider-icon-${useId().replace(/:/g, "")}`;
  const iconNode = iconSvg ? renderSafeIcon(iconSvg, iconIDPrefix) : null;
  // Brand marks from /api/v1/apps are full-bleed; derived marks stay inset.
  const hasBrandMark = iconNode != null;
  const initials = hasBrandMark ? "" : appInitials(displayName, name ?? "");

  return (
    <div
      className={cn(
        // Frameless by default — a filled frame matches the card at rest and
        // only appears on card hover (card darkens, plate does not).
        "flex shrink-0 items-center justify-center overflow-hidden text-muted-foreground",
        frameSizeClass[size],
        variant === "plated" && "rounded-lg bg-muted",
        hasBrandMark
          ? // Brand SVGs are full-bleed in the slot; ~12% inset matches the
            // optical padding most catalog marks already bake into their
            // viewBox (one catalog mark was edge-cropped and read oversized).
            "[&>svg]:size-[76%]"
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
            "select-none font-display font-normal leading-none tracking-tight",
            "text-foreground",
            monogramSizeClass[size],
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
