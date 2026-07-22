import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

/**
 * Gestalt console vendor of Valon Registry `label`.
 *
 * Ownership: Valon Registry (`valon-tools/apps/registry/ui/src/ui/label.tsx`).
 */

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
    <label
      ref={ref}
      className={cn(labelVariants({ variant }), className)}
      {...props}
    />
  ),
);
Label.displayName = "Label";

export { Label, labelVariants };
