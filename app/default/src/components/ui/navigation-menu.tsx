"use client";


/**
 * Gestalt console vendor of Valon Registry `navigation-menu`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/navigation-menu.tsx`).
 * Synced from toolshed origin/main — token adaptation only (`@/lib/cn` path).
 * Do not restyle chrome at call sites; change Registry first.
 */

import * as React from "react";
import * as NavigationMenuPrimitive from "@radix-ui/react-navigation-menu";
import { cva, type VariantProps } from "class-variance-authority";
import { ChevronDown } from "lucide-react";

import { cn } from "@/lib/cn";

const navigationMenuTriggerStyle = cva(
  // Idle hover/press gated off `data-active` — same contract as listItemInteraction
  // (selectable-rows.md). Selected uses the accent-vivid ladder only.
  [
    "group inline-flex w-max items-center justify-center rounded-md font-medium transition-colors duration-select-out ease-out-quart hover:duration-select-in focus:duration-select-in active:duration-press focus-ring",
    // Button uses [disabled]; Link uses aria-disabled — both share chroma-zero chrome.
    "disabled:cursor-not-allowed disabled:bg-disabled disabled:text-disabled-foreground",
    "aria-disabled:cursor-not-allowed aria-disabled:bg-disabled aria-disabled:text-disabled-foreground",
    // Gate ladders for button AND anchor. `enabled:` only matches form controls, so
    // flat NavigationMenuLink (<a>) must use :not([disabled]):not([aria-disabled]).
    "[&:not([disabled]):not([aria-disabled=true]):not([data-active])]:hover:not-active:bg-neutral-hover [&:not([disabled]):not([aria-disabled=true]):not([data-active])]:hover:text-accent-foreground",
    "[&:not([disabled]):not([aria-disabled=true]):not([data-active])]:focus:not-active:bg-neutral-hover [&:not([disabled]):not([aria-disabled=true]):not([data-active])]:focus:text-accent-foreground",
    "[&:not([disabled]):not([aria-disabled=true]):not([data-active])]:active:bg-neutral-pressed [&:not([disabled]):not([aria-disabled=true]):not([data-active])]:active:text-accent-foreground",
    "[&:not([disabled]):not([aria-disabled=true]):not([data-active])]:data-[state=open]:bg-neutral-hover [&:not([disabled]):not([aria-disabled=true]):not([data-active])]:data-[state=open]:text-accent-foreground",
    "[&:not([disabled]):not([aria-disabled=true])]:data-[active]:bg-accent-vivid [&:not([disabled]):not([aria-disabled=true])]:data-[active]:text-accent-vivid-foreground",
    "[&:not([disabled]):not([aria-disabled=true])]:data-[active]:hover:bg-accent-vivid-hover [&:not([disabled]):not([aria-disabled=true])]:data-[active]:hover:text-accent-vivid-foreground",
    "[&:not([disabled]):not([aria-disabled=true])]:data-[active]:active:bg-accent-vivid-pressed [&:not([disabled]):not([aria-disabled=true])]:data-[active]:active:text-accent-vivid-foreground",
  ].join(" "),
  {
    variants: {
      size: {
        sm: "h-control-sm px-2.5 text-control-sm",
        default: "h-control-default px-3 py-2 text-control-default",
        lg: "h-control-lg px-4 text-control-lg",
      },
    },
    defaultVariants: {
      size: "default",
    },
  },
);

type NavigationMenuSize = NonNullable<VariantProps<typeof navigationMenuTriggerStyle>["size"]>;

const NavigationMenuSizeContext = React.createContext<NavigationMenuSize>("default");

function NavigationMenu({
  className,
  children,
  viewport = true,
  size = "default",
  ...props
}: React.ComponentProps<typeof NavigationMenuPrimitive.Root> & {
  viewport?: boolean;
  size?: NavigationMenuSize;
}) {
  return (
    <NavigationMenuSizeContext.Provider value={size}>
      <NavigationMenuPrimitive.Root
        data-slot="navigation-menu"
        data-viewport={viewport}
        data-size={size}
        className={cn(
          "group/navigation-menu relative flex max-w-max flex-1 items-center justify-center",
          className,
        )}
        {...props}
      >
        {children}
        {viewport ? <NavigationMenuViewport /> : null}
      </NavigationMenuPrimitive.Root>
    </NavigationMenuSizeContext.Provider>
  );
}

function NavigationMenuList({
  className,
  ...props
}: React.ComponentProps<typeof NavigationMenuPrimitive.List>) {
  return (
    <NavigationMenuPrimitive.List
      data-slot="navigation-menu-list"
      className={cn("group flex flex-1 list-none items-center justify-center gap-1", className)}
      {...props}
    />
  );
}

function NavigationMenuItem({
  className,
  ...props
}: React.ComponentProps<typeof NavigationMenuPrimitive.Item>) {
  return (
    <NavigationMenuPrimitive.Item
      data-slot="navigation-menu-item"
      className={cn("relative", className)}
      {...props}
    />
  );
}

function NavigationMenuTrigger({
  className,
  children,
  size,
  ...props
}: React.ComponentProps<typeof NavigationMenuPrimitive.Trigger> &
  VariantProps<typeof navigationMenuTriggerStyle>) {
  const contextSize = React.useContext(NavigationMenuSizeContext);
  return (
    <NavigationMenuPrimitive.Trigger
      data-slot="navigation-menu-trigger"
      className={cn(navigationMenuTriggerStyle({ size: size ?? contextSize }), "group", className)}
      {...props}
    >
      {children}{" "}
      <ChevronDown
        className="relative top-px ml-1 size-3 shrink-0 transition-transform duration-overshoot ease-out-back group-data-[state=open]:rotate-180"
        aria-hidden="true"
      />
    </NavigationMenuPrimitive.Trigger>
  );
}

function NavigationMenuContent({
  className,
  ...props
}: React.ComponentProps<typeof NavigationMenuPrimitive.Content>) {
  return (
    <NavigationMenuPrimitive.Content
      data-slot="navigation-menu-content"
      className={cn(
        "top-0 left-0 w-full p-2 data-[motion^=from-]:animate-in data-[motion^=from-]:fade-in data-[motion^=to-]:animate-out data-[motion^=to-]:fade-out data-[motion=from-end]:slide-in-from-right-52 data-[motion=from-start]:slide-in-from-left-52 data-[motion=to-end]:slide-out-to-right-52 data-[motion=to-start]:slide-out-to-left-52 md:absolute md:w-auto",
        // Dual presentation: motion classes above serve the shared viewport slot;
        // when root sets viewport={false}, Content owns popover surface chrome.
        "group-data-[viewport=false]/navigation-menu:absolute group-data-[viewport=false]/navigation-menu:top-full group-data-[viewport=false]/navigation-menu:mt-1.5 group-data-[viewport=false]/navigation-menu:z-50 group-data-[viewport=false]/navigation-menu:overflow-hidden group-data-[viewport=false]/navigation-menu:rounded-md group-data-[viewport=false]/navigation-menu:border group-data-[viewport=false]/navigation-menu:bg-popover group-data-[viewport=false]/navigation-menu:text-popover-foreground group-data-[viewport=false]/navigation-menu:shadow-md group-data-[viewport=false]/navigation-menu:duration-200 group-data-[viewport=false]/navigation-menu:data-[state=closed]:animate-out group-data-[viewport=false]/navigation-menu:data-[state=closed]:fade-out-0 group-data-[viewport=false]/navigation-menu:data-[state=closed]:zoom-out-95 group-data-[viewport=false]/navigation-menu:data-[state=open]:animate-in group-data-[viewport=false]/navigation-menu:data-[state=open]:fade-in-0 group-data-[viewport=false]/navigation-menu:data-[state=open]:zoom-in-95",
        className,
      )}
      {...props}
    />
  );
}

function NavigationMenuLink({
  className,
  size,
  ...props
}: React.ComponentProps<typeof NavigationMenuPrimitive.Link> &
  VariantProps<typeof navigationMenuTriggerStyle>) {
  const contextSize = React.useContext(NavigationMenuSizeContext);
  return (
    <NavigationMenuPrimitive.Link
      data-slot="navigation-menu-link"
      className={cn(navigationMenuTriggerStyle({ size: size ?? contextSize }), className)}
      {...props}
    />
  );
}

function NavigationMenuViewport({
  className,
  ...props
}: React.ComponentProps<typeof NavigationMenuPrimitive.Viewport>) {
  return (
    <div className={cn("absolute top-full left-0 isolate z-50 flex justify-center")}>
      <NavigationMenuPrimitive.Viewport
        data-slot="navigation-menu-viewport"
        className={cn(
          "origin-top relative mt-1.5 h-[var(--radix-navigation-menu-viewport-height)] w-full overflow-hidden rounded-md border bg-popover text-popover-foreground shadow-md data-[state=closed]:animate-out data-[state=closed]:zoom-out-95 data-[state=open]:animate-in data-[state=open]:zoom-in-90 md:w-[var(--radix-navigation-menu-viewport-width)]",
          className,
        )}
        {...props}
      />
    </div>
  );
}

function NavigationMenuIndicator({
  className,
  ...props
}: React.ComponentProps<typeof NavigationMenuPrimitive.Indicator>) {
  return (
    <NavigationMenuPrimitive.Indicator
      data-slot="navigation-menu-indicator"
      className={cn(
        "top-full z-[1] flex h-1.5 items-end justify-center overflow-hidden data-[state=hidden]:animate-out data-[state=hidden]:fade-out data-[state=visible]:animate-in data-[state=visible]:fade-in",
        className,
      )}
      {...props}
    >
      <div className="bg-border relative top-[60%] size-2 rotate-45 rounded-tl-sm shadow-md" />
    </NavigationMenuPrimitive.Indicator>
  );
}

export {
  NavigationMenu,
  NavigationMenuList,
  NavigationMenuItem,
  NavigationMenuContent,
  NavigationMenuTrigger,
  NavigationMenuLink,
  NavigationMenuIndicator,
  NavigationMenuViewport,
  navigationMenuTriggerStyle,
};
