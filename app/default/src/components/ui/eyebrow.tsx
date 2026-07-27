/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

// Eyebrow = all-caps microtype above a heading or value (Material "overline",
// card anatomy "eyebrow"). Named primitive so agents don't re-derive utilities
// (eyebrow.md / RES-20260717-006). Default element is span — never a heading.
// Caps via CSS `uppercase` — type real case in markup. Not Label (form) and not
// Badge (filled chip).
// TODO(registry): switch base back to text-xs when Eyebrow ships native 2xs.
const eyebrowVariants = cva(
  "text-2xs font-normal uppercase tracking-eyebrow leading-none",
  {
    variants: {
      // Ink roles map to color.md text hierarchy — pick a tone, never override
      // text-* after eyebrowVariants().
      tone: {
        muted: "text-muted-foreground-soft",
        secondary: "text-muted-foreground",
        brand: "text-brand",
      },
    },
    defaultVariants: {
      tone: "muted",
    },
  },
);

export interface EyebrowProps
  extends React.HTMLAttributes<HTMLSpanElement>,
    VariantProps<typeof eyebrowVariants> {}

const Eyebrow = React.forwardRef<HTMLSpanElement, EyebrowProps>(
  ({ className, tone, ...props }, ref) => (
    <span
      ref={ref}
      data-slot="eyebrow"
      className={cn(eyebrowVariants({ tone }), className)}
      {...props}
    />
  ),
);
Eyebrow.displayName = "Eyebrow";

export { Eyebrow, eyebrowVariants };
