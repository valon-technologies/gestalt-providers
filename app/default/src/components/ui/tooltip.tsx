"use client";

import * as React from "react";
import * as TooltipPrimitive from "@radix-ui/react-tooltip";

import { cn } from "@/lib/cn";

/**
 * Gestalt console vendor of Valon Registry `tooltip`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/tooltip.tsx`). Mount `TooltipProvider`
 * once near the app root (or around a feature island). Delay defaults to 0
 * (instant open); arrow + motion tokens match Registry when animate utilities
 * are available.
 */

/**
 * Mount once at the app root so every tooltip shares one timing context.
 * Opens instantly (`delayDuration={0}`); raise `delayDuration` for a hover-intent
 * delay, in which case `skipDelayDuration` keeps the group "warm" between triggers.
 */
function TooltipProvider({
  delayDuration = 0,
  skipDelayDuration = 300,
  ...props
}: React.ComponentProps<typeof TooltipPrimitive.Provider>) {
  return (
    <TooltipPrimitive.Provider
      data-slot="tooltip-provider"
      delayDuration={delayDuration}
      skipDelayDuration={skipDelayDuration}
      {...props}
    />
  );
}

function Tooltip(props: React.ComponentProps<typeof TooltipPrimitive.Root>) {
  return <TooltipPrimitive.Root data-slot="tooltip" {...props} />;
}

function TooltipTrigger(
  props: React.ComponentProps<typeof TooltipPrimitive.Trigger>,
) {
  return <TooltipPrimitive.Trigger data-slot="tooltip-trigger" {...props} />;
}

function TooltipContent({
  className,
  sideOffset = 4,
  children,
  ...props
}: React.ComponentProps<typeof TooltipPrimitive.Content>) {
  return (
    <TooltipPrimitive.Portal>
      <TooltipPrimitive.Content
        data-slot="tooltip-content"
        sideOffset={sideOffset}
        className={cn(
          "z-50 w-fit origin-(--radix-tooltip-content-transform-origin) rounded-md bg-foreground px-3 py-1.5 text-xs text-balance text-background",
          // blur + spring spawn (after beui), on Valon motion tokens: grows from the
          // trigger edge with an ease-out-back overshoot as the blur clears.
          // NOTE: Radix tooltip's open state is "instant-open"/"delayed-open" (never
          // "open"), so the ENTER classes are unconditional — the content only mounts
          // while open — and only the EXIT is gated on data-[state=closed].
          // Console does not ship tw-animate; enter/exit utilities are no-ops until
          // that plugin is added — delay + arrow still match Registry.
          "duration-reveal ease-out-back",
          "data-[state=closed]:duration-dismiss data-[state=closed]:ease-out-expo",
          className,
        )}
        {...props}
      >
        {children}
        <TooltipPrimitive.Arrow className="z-50 size-2.5 translate-y-[calc(-50%_-_2px)] rotate-45 rounded-[2px] bg-foreground fill-foreground" />
      </TooltipPrimitive.Content>
    </TooltipPrimitive.Portal>
  );
}

export { Tooltip, TooltipTrigger, TooltipContent, TooltipProvider };
