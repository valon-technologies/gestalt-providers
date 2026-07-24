"use client";

/**
 * Gestalt console vendor of Valon Registry `accordion`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/accordion.tsx`).
 * Synced from toolshed origin/main — token adaptation only (`@/lib/cn` path).
 * Do not restyle chrome at call sites; change Registry first.
 */

import * as React from "react";
import * as AccordionPrimitive from "@radix-ui/react-accordion";

import { ChevronDownIcon } from "@/components/icons";
import { listItemInteraction } from "@/lib/list-item-interaction";
import { cn } from "@/lib/cn";

function Accordion(props: React.ComponentProps<typeof AccordionPrimitive.Root>) {
  return <AccordionPrimitive.Root data-slot="accordion" {...props} />;
}

function AccordionItem({
  className,
  ...props
}: React.ComponentProps<typeof AccordionPrimitive.Item>) {
  return (
    <AccordionPrimitive.Item
      data-slot="accordion-item"
      className={cn(
        "border-b border-border last:border-b-0",
        "[&_[data-slot=accordion-trigger]]:rounded-none",
        "first:[&_[data-slot=accordion-trigger]]:rounded-t-md",
        "[&_[data-slot=accordion-trigger][data-state=open]]:rounded-b-md",
        "not-last:[&_[data-slot=accordion-trigger][data-state=closed]]:rounded-b-none",
        "last:[&_[data-slot=accordion-trigger][data-state=closed]]:rounded-b-md",
        "not-last:[&_[data-slot=accordion-content][data-state=open]>div]:rounded-b-none",
        "last:[&_[data-slot=accordion-content][data-state=open]>div]:rounded-b-md",
        className,
      )}
      {...props}
    />
  );
}

function AccordionTrigger({
  className,
  children,
  ...props
}: React.ComponentProps<typeof AccordionPrimitive.Trigger>) {
  return (
    <AccordionPrimitive.Header className="flex">
      <AccordionPrimitive.Trigger
        data-slot="accordion-trigger"
        className={cn(
          "focus-ring flex flex-1 items-start justify-between gap-4 px-2 py-4 text-left text-sm font-medium text-foreground disabled:pointer-events-none disabled:opacity-50 [&[data-state=open]>svg]:rotate-180",
          listItemInteraction({ pointer: "css" }),
          className,
        )}
        {...props}
      >
        {children}
        <ChevronDownIcon className="pointer-events-none size-4 shrink-0 translate-y-0.5 text-muted-foreground transition-transform duration-overshoot ease-out-back" />
      </AccordionPrimitive.Trigger>
    </AccordionPrimitive.Header>
  );
}

function AccordionContent({
  className,
  children,
  ...props
}: React.ComponentProps<typeof AccordionPrimitive.Content>) {
  return (
    <AccordionPrimitive.Content
      data-slot="accordion-content"
      className="overflow-hidden text-sm [interpolate-size:allow-keywords]"
      {...props}
    >
      <div className={cn("px-2 pt-0 pb-4", className)}>{children}</div>
    </AccordionPrimitive.Content>
  );
}

export { Accordion, AccordionItem, AccordionTrigger, AccordionContent };
