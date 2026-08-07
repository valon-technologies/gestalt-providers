/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import * as PopoverPrimitive from "@radix-ui/react-popover";

import { cn } from "@/lib/cn";
import { FLYOUT_VIEWPORT_EDGE_INSET_PX } from "@/lib/flyout";

/**
 * Minimum viewport edge inset for Popover collision (narrow canvases).
 * Larger viewports step up via `resolvePopoverCollisionPadding` so wide
 * dual-month panels keep breathing room against the screen edge.
 */
export const POPOVER_COLLISION_PADDING_PX = FLYOUT_VIEWPORT_EDGE_INSET_PX;

/** Tailwind-aligned floors (px) for collision-padding steps. */
export const POPOVER_COLLISION_PADDING_BREAKPOINTS = {
  lg: 1024,
  xl: 1280,
} as const;

/**
 * Viewport → collision padding. Steps track page gutter rhythm
 * (`container.md`: denser on small, more air on `lg`/`xl`) — Radix only
 * accepts a number, so CSS media queries cannot own this.
 */
export function resolvePopoverCollisionPadding(viewportWidthPx: number): number {
  if (viewportWidthPx >= POPOVER_COLLISION_PADDING_BREAKPOINTS.xl) return 32;
  if (viewportWidthPx >= POPOVER_COLLISION_PADDING_BREAKPOINTS.lg) return 24;
  return POPOVER_COLLISION_PADDING_PX;
}

/** Shared viewport width for collision padding + layout decisions (one subscription). */
export function useViewportWidthPx(): number {
  return React.useSyncExternalStore(
    (onChange) => {
      window.addEventListener("resize", onChange);
      return () => window.removeEventListener("resize", onChange);
    },
    () => window.innerWidth,
    () => POPOVER_COLLISION_PADDING_BREAKPOINTS.lg,
  );
}

const Popover = PopoverPrimitive.Root;

const PopoverTrigger = PopoverPrimitive.Trigger;

const PopoverAnchor = PopoverPrimitive.Anchor;

const PopoverContent = React.forwardRef<
  React.ElementRef<typeof PopoverPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof PopoverPrimitive.Content>
>(({ className, align = "center", sideOffset = 4, collisionPadding, ...props }, ref) => {
  const viewportWidthPx = useViewportWidthPx();
  // Omit → responsive inset. Explicit prop (including 0) wins for rare overrides.
  const resolvedCollisionPadding =
    collisionPadding ?? resolvePopoverCollisionPadding(viewportWidthPx);

  return (
    <PopoverPrimitive.Portal>
      <PopoverPrimitive.Content
        ref={ref}
        align={align}
        sideOffset={sideOffset}
        collisionPadding={resolvedCollisionPadding}
        className={cn(
          // flex + overflow-hidden (not overflow-y-auto): max-h caps the
          // surface; footered menus scroll an inner pane and pin shrink-0
          // footers (flyout.md). Scrolling the whole popover hid Apply/Clear.
          "bg-popover text-popover-foreground data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 data-[state=open]:duration-reveal data-[state=closed]:duration-dismiss data-[side=bottom]:slide-in-from-top-2 data-[side=left]:slide-in-from-right-2 data-[side=right]:slide-in-from-left-2 data-[side=top]:slide-in-from-bottom-2 z-50 flex w-72 max-h-(--radix-popover-content-available-height) flex-col overflow-hidden origin-(--radix-popover-content-transform-origin) rounded-md border p-0 shadow-md outline-none",
          className,
        )}
        {...props}
      />
    </PopoverPrimitive.Portal>
  );
});
PopoverContent.displayName = PopoverPrimitive.Content.displayName;

export { Popover, PopoverAnchor, PopoverContent, PopoverTrigger };
