/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import { cva } from "class-variance-authority";

export const menuItemVariants = cva(
  "relative flex cursor-default select-none items-center gap-2 rounded-md py-1.5 text-sm outline-none transition-colors duration-select-out ease-out-quart focus:not-active:bg-neutral-hover focus:text-foreground focus:duration-select-in aria-selected:not-active:bg-neutral-hover aria-selected:text-foreground aria-selected:duration-select-in active:bg-neutral-pressed active:text-foreground active:duration-press aria-disabled:pointer-events-none aria-disabled:opacity-50 aria-disabled:text-disabled-foreground [&_svg]:pointer-events-none [&_svg]:shrink-0 [&_svg:not([class*='size-'])]:size-4",
  {
    variants: {
      indicator: {
        none: "px-2",
        leading: "pl-8 pr-2",
        trailing: "pl-2 pr-8",
      },
    },
    defaultVariants: {
      indicator: "none",
    },
  },
);
