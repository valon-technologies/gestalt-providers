import { useId } from "react";
import { DefaultIcon } from "@/components/icons";
import { cn } from "@/lib/cn";
import { renderSafeIcon } from "@/lib/safe-svg";

/**
 * Canonical renderer for Integration.iconSvg from `/api/v1/apps`.
 * Sizing frame around the shared sanitizer in `@/lib/safe-svg`.
 * Falls back to DefaultIcon when svg is missing or unsafe.
 */

export default function IntegrationIcon({
  iconSvg,
  className,
  size = "md",
}: {
  iconSvg?: string;
  className?: string;
  size?: "sm" | "md" | "lg" | "xl";
}) {
  const iconIDPrefix = `provider-icon-${useId().replace(/:/g, "")}`;
  const iconNode = iconSvg ? renderSafeIcon(iconSvg, iconIDPrefix) : null;
  // Brand marks from /api/v1/apps are full-bleed; glyph fallback stays inset.
  const hasBrandMark = iconNode != null;

  return (
    <div
      className={cn(
        // Frameless mark — no plate. A filled frame matches the card at rest
        // and only appears on card hover (card darkens, plate does not).
        "flex shrink-0 items-center justify-center overflow-hidden text-muted-foreground",
        size === "sm" && "size-8",
        size === "md" && "size-10",
        size === "lg" && "size-12",
        size === "xl" && "size-14",
        hasBrandMark
          ? // Brand SVGs are full-bleed in the slot; ~12% inset matches the
            // optical padding most catalog marks already bake into their
            // viewBox (one catalog mark was edge-cropped and read oversized).
            "[&>svg]:size-[76%]"
          : size === "sm"
            ? "[&>svg]:size-4"
            : size === "lg" || size === "xl"
              ? "[&>svg]:size-7"
              : "[&>svg]:size-5",
        className,
      )}
    >
      {iconNode ?? <DefaultIcon />}
    </div>
  );
}
