/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

const textareaVariants = cva(
  "flex min-h-24 w-full rounded-md bg-background px-2 py-2 text-sm transition-[color,border-color] duration-select-out ease-out-quart placeholder:text-muted-foreground aria-[invalid=true]:border-destructive disabled:cursor-not-allowed disabled:bg-disabled disabled:text-disabled-foreground",
  {
    variants: {
      chrome: {
        standalone: "border border-input focus-ring disabled:border-border",
        group: "border-0 shadow-none focus-visible:outline-none",
      },
    },
    defaultVariants: {
      chrome: "standalone",
    },
  },
);

export interface TextareaProps
  extends React.TextareaHTMLAttributes<HTMLTextAreaElement>,
    VariantProps<typeof textareaVariants> {}

const Textarea = React.forwardRef<HTMLTextAreaElement, TextareaProps>(
  ({ className, chrome, ...props }, ref) => (
    <textarea
      className={cn(textareaVariants({ chrome, className }))}
      ref={ref}
      {...props}
    />
  ),
);
Textarea.displayName = "Textarea";

export { Textarea, textareaVariants };
