/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 * Registry `collapsible` (toolshed#4191 — lock `data-slot` after props for drawer CSS).
 */

import * as React from "react";
import * as CollapsiblePrimitive from "@radix-ui/react-collapsible";

import { listItemInteraction } from "@/lib/list-item-interaction";
import { cn } from "@/lib/cn";

// shadcn Collapsible on Radix, in the shared house style: canonical focus-ring on
// the trigger, Neutral idle hover/press from listItemInteraction
// (selectable-rows.md — same ladder as List Item / table rows; not accent-wash),
// and the same pure-CSS height drawer as Accordion (`interpolate-size` + theme
// keyframes on `[data-slot=collapsible-content]`). Open state is owned here —
// never on Card. Compose card chrome at the call site (Card Collapsible story).
function Collapsible({
  ...props
}: React.ComponentProps<typeof CollapsiblePrimitive.Root>) {
  return <CollapsiblePrimitive.Root data-slot="collapsible" {...props} />;
}

function CollapsibleTrigger({
  className,
  ...props
}: React.ComponentProps<typeof CollapsiblePrimitive.CollapsibleTrigger>) {
  return (
    <CollapsiblePrimitive.CollapsibleTrigger
      data-slot="collapsible-trigger"
      className={cn(
        "focus-ring flex w-full items-center justify-between gap-4 text-left text-sm font-medium text-foreground disabled:pointer-events-none disabled:opacity-50 [&[data-state=open]>svg]:rotate-180",
        // Idle Neutral hover/press snap (hover-pressed-color.md) — reuse List Item.
        // On already-tinted surfaces (e.g. Card solid / bg-secondary ≡ neutral-hover),
        // deepen with Neutral dark at the call site (selectable-rows.md).
        listItemInteraction({ pointer: "css" }),
        className,
      )}
      {...props}
    />
  );
}

function CollapsibleContent({
  className,
  drawerClassName,
  children,
  ...props
}: React.ComponentProps<typeof CollapsiblePrimitive.CollapsibleContent> & {
  /** Classes on the animated Radix node (e.g. drawer easing overrides). */
  drawerClassName?: string;
}) {
  // Height animation owns this node (0 ↔ auto). Padding / border / radius stay
  // on the inner wrapper — same split as AccordionContent — so open/close does
  // not jump or leave a padded sliver while the drawer runs.
  //
  // `data-slot="collapsible-content"` is locked after `{...props}`: theme CSS
  // keys the drawer animation on that slot. Callers (e.g. Alert) must not
  // rename it — put product identity on the root / trigger / inner chrome.
  return (
    <CollapsiblePrimitive.CollapsibleContent
      className={cn(
        "overflow-hidden text-sm [interpolate-size:allow-keywords]",
        drawerClassName,
      )}
      {...props}
      data-slot="collapsible-content"
    >
      <div className={cn(className)}>{children}</div>
    </CollapsiblePrimitive.CollapsibleContent>
  );
}

export { Collapsible, CollapsibleTrigger, CollapsibleContent };
