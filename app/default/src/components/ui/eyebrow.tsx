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
const eyebrowVariants = cva(
  "font-normal uppercase tracking-eyebrow leading-none",
  {
    variants: {
      size: {
        default: "text-xs",
        sm: "text-2xs",
      },
      // Ink roles map to color.md text hierarchy — pick a tone, never override
      // text-* after eyebrowVariants(). Default is dense-chrome 60% ink.
      tone: {
        default: "text-muted-foreground",
        muted: "text-muted-foreground-soft",
        accent: "text-accent-strong",
      },
    },
    defaultVariants: {
      size: "default",
      tone: "default",
    },
  },
);

export interface EyebrowProps
  extends React.HTMLAttributes<HTMLSpanElement>,
    VariantProps<typeof eyebrowVariants> {}

const Eyebrow = React.forwardRef<HTMLSpanElement, EyebrowProps>(
  ({ className, size, tone, ...props }, ref) => (
    <span
      ref={ref}
      data-slot="eyebrow"
      className={cn(eyebrowVariants({ size, tone }), className)}
      {...props}
    />
  ),
);
Eyebrow.displayName = "Eyebrow";

export { Eyebrow, eyebrowVariants };
