/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva } from "class-variance-authority";

import { cn } from "@/lib/cn";

// Shared row styling for menu surfaces (DropdownMenu, ContextMenu, Menubar,
// Select, Command): one source of truth for the hover/selection tint,
// asymmetric select motion, icon sizing, and disabled treatment. shadcn
// duplicates these per primitive, which is what lets them drift apart;
// sharing keeps every menu row identical.
//
// Idle hover/press use selectable-rows Neutral roles (`neutral-hover` /
// `neutral-pressed`). Popup selection (Select / Combobox) stays on a blank
// row with a solid trailing check — Accent vivid fill is for persistent list
// surfaces (Listbox / listItemInteraction), not flyout options.
//
// Active/highlight chrome is keyed off `focus` (Radix), `aria-selected` (cmdk),
// or `data-highlighted` (hand-rolled listboxes that need APG `aria-selected`
// for the committed value). Disabled keys off `aria-disabled`, which both
// primitives set consistently.
//
// Slot Lucide marks are sized on their actual parent: the row (`menuItemVariants`)
// or Radix Select `ItemText` via `selectItemTextSlotClassName`. ItemText strips
// `className`, so the row targets `[data-slot=select-item-text]>svg` instead of
// wrapping children — a wrapper would portal into `SelectValue` and break
// trigger truncation. Direct-child `>svg` keeps nested compound glyphs at their
// own size; deep `[&_svg]` would force inner strokes to size-4 and overflow fills.
export const menuItemDirectIconClassName =
  "[&>svg]:pointer-events-none [&>svg]:shrink-0 [&>svg:not([class*='size-'])]:size-4";

export const selectItemTextSlotClassName =
  "[&_[data-slot=select-item-text]]:flex [&_[data-slot=select-item-text]]:items-center [&_[data-slot=select-item-text]]:gap-2 [&_[data-slot=select-item-text]>svg]:pointer-events-none [&_[data-slot=select-item-text]>svg]:shrink-0 [&_[data-slot=select-item-text]>svg:not([class*='size-'])]:size-4";

export const menuItemVariants = cva(
  cn(
    "relative flex cursor-default select-none items-center gap-2 rounded-md py-1.5 text-sm outline-none transition-colors duration-select-out ease-out-quart focus:not-active:bg-neutral-hover focus:text-foreground focus:duration-select-in active:bg-neutral-pressed active:text-foreground active:duration-press aria-disabled:pointer-events-none aria-disabled:opacity-50 aria-disabled:text-disabled-foreground",
    menuItemDirectIconClassName,
  ),
  {
    variants: {
      indicator: {
        none: "px-2",
        leading: "pl-8 pr-2",
        trailing: "pl-2 pr-8",
      },
      highlight: {
        "aria-selected":
          "aria-selected:not-active:bg-neutral-hover aria-selected:text-foreground aria-selected:duration-select-in",
        "data-highlighted":
          "data-[highlighted]:not-active:bg-neutral-hover data-[highlighted]:text-foreground data-[highlighted]:duration-select-in",
      },
    },
    defaultVariants: {
      indicator: "none",
      highlight: "aria-selected",
    },
  },
);

export const menuShortcutClassName =
  "ml-auto text-xs tracking-widest text-muted-foreground";

/** Primary label column in a menu row (icon | label | shortcut). */
export const menuItemLabelClassName =
  "min-w-0 flex-1 break-words [overflow-wrap:anywhere]";

export type MenuItemLabelProps = React.HTMLAttributes<HTMLSpanElement>;

export function MenuItemLabel({
  children,
  className,
  title,
  ...props
}: MenuItemLabelProps) {
  const resolvedTitle =
    title ??
    (typeof children === "string" || typeof children === "number"
      ? String(children)
      : undefined);

  return (
    <span
      className={cn(menuItemLabelClassName, className)}
      title={resolvedTitle || undefined}
      {...props}
    >
      {children}
    </span>
  );
}

/** Wrap primitive text nodes in MenuItemLabel; icons and shortcuts pass through. */
export function withMenuItemLabelChildren(children: React.ReactNode) {
  return React.Children.map(children, (child, index) => {
    if (typeof child === "string" || typeof child === "number") {
      return (
        <MenuItemLabel key={`menu-item-label-${index}`}>{child}</MenuItemLabel>
      );
    }
    if (React.isValidElement(child) && child.type === MenuItemLabel) {
      return child;
    }
    return child;
  });
}
