/**
 * Gestalt console vendor of Valon Registry `label`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/label.tsx`).
 * Synced from toolshed origin/main — token adaptation only (`@/lib/cn` path).
 * Do not restyle chrome at call sites; change Registry first.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

// Two roles share one <label> primitive (Carbon / form a11y):
//   inline — companion text beside checkbox/radio. Body size + foreground; the
//            label *is* the readable name of the control. Default — preserves
//            the pre-variant Label contract for bare <Label> call sites.
//   field  — caption above a control. Smaller + muted so the control value is
//            primary (Carbon label-01). FieldLabel always opts into this.
// Disabled recolor (disabled-states.md): field uses group/field + data-disabled;
// inline uses peer-disabled on the preceding control. Never opacity.
// Invalid: field captions retint via group-data-[invalid] on Field.
// Presence selector (not =true) — matches bare `data-invalid` and
// `data-invalid={!!error}` per fields.md; =true misses empty-string flags.
const labelVariants = cva(
  "font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:text-disabled-foreground group-data-[disabled]/field:cursor-not-allowed group-data-[disabled]/field:text-disabled-foreground group-data-[invalid]/field:text-destructive",
  {
    variants: {
      variant: {
        inline: "text-sm text-foreground",
        field: "text-xs text-muted-foreground",
      },
    },
    defaultVariants: {
      variant: "inline",
    },
  },
);

export interface LabelProps
  extends React.LabelHTMLAttributes<HTMLLabelElement>,
    VariantProps<typeof labelVariants> {}

const Label = React.forwardRef<HTMLLabelElement, LabelProps>(
  ({ className, variant, ...props }, ref) => (
    <label ref={ref} className={cn(labelVariants({ variant }), className)} {...props} />
  ),
);
Label.displayName = "Label";

export { Label, labelVariants };
