/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

const inputVariants = cva(
  "flex w-full rounded-md bg-background px-2 py-1 transition-[color,border-color] duration-select-out ease-out-quart file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground aria-[invalid=true]:border-destructive disabled:cursor-not-allowed disabled:bg-disabled disabled:text-disabled-foreground",
  {
    variants: {
      size: {
        sm: "h-control-sm text-control-sm",
        default: "h-control-default text-control-default",
        lg: "h-control-lg text-control-lg",
      },
      chrome: {
        standalone: "border border-input focus-ring disabled:border-border",
        group: "border-0 shadow-none focus-visible:outline-none",
      },
    },
    defaultVariants: {
      size: "default",
      chrome: "standalone",
    },
  },
);

export interface InputProps
  extends Omit<React.InputHTMLAttributes<HTMLInputElement>, "size">,
    VariantProps<typeof inputVariants> {}

const Input = React.forwardRef<HTMLInputElement, InputProps>(
  ({ className, type, size, chrome, ...props }, ref) => (
    <input
      type={type}
      className={cn(inputVariants({ size, chrome, className }))}
      ref={ref}
      {...props}
    />
  ),
);
Input.displayName = "Input";

export { Input, inputVariants };
