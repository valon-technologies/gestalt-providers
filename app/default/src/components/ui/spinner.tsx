/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 * Registry `spinner`.
 */

import type { ComponentProps } from "react";

import { cn } from "@/lib/cn";

/**
 * Routine indeterminate busy glyph (RES-20260804-002): a ring with a fading
 * trail (conic-gradient + radial mask), not a hard-edged arc. Use in buttons,
 * rows, tables, and inline chrome. For rare brand / identity loading moments
 * use BrandSpinner.
 */
function Spinner({ className, ...props }: ComponentProps<"span">) {
  return (
    <span
      role="status"
      aria-label="Loading"
      data-slot="spinner"
      className={cn("spinner-trail size-4", className)}
      {...props}
    />
  );
}

export { Spinner };
