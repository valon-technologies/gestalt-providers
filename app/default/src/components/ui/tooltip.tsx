"use client";

import * as React from "react";
import * as TooltipPrimitive from "@radix-ui/react-tooltip";

import { cn } from "@/lib/utils";

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

function TooltipTrigger({
  asChild,
  ...props
}: React.ComponentProps<typeof TooltipPrimitive.Trigger>) {
  // asChild projects onto the child — omit data-slot entirely so we do not
  // clear or overwrite the child's slot (InputGroupButton edge inset). Own
  // slot only when Trigger renders its own element.
  return (
    <TooltipPrimitive.Trigger
      asChild={asChild}
      {...(asChild ? null : { "data-slot": "tooltip-trigger" })}
      {...props}
    />
  );
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
          "animate-in fade-in-0 zoom-in-85 blur-in-10 ease-out-back duration-reveal",
          "data-[side=bottom]:slide-in-from-top-1 data-[side=left]:slide-in-from-right-1 data-[side=right]:slide-in-from-left-1 data-[side=top]:slide-in-from-bottom-1",
          "data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=closed]:zoom-out-92 data-[state=closed]:blur-out-6 data-[state=closed]:ease-out-expo data-[state=closed]:duration-dismiss",
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
