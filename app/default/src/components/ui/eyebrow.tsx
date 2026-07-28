/**
 * Gestalt console vendor of Valon Registry `eyebrow`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/eyebrow.tsx`).
 * Synced from toolshed origin/main — token adaptation only (`@/lib/cn` path).
 * Do not restyle chrome at call sites; change Registry first.
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
  "text-xs font-normal uppercase tracking-eyebrow leading-none",
  {
    variants: {
      tone: {
        muted: "text-muted-foreground-soft",
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
