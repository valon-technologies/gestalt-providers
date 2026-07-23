/**
 * Gestalt console vendor of Valon Registry `input`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/input.tsx`).
 * Synced from toolshed origin/main — token adaptation only (`@/lib/cn` path).
 * Do not restyle chrome at call sites; change Registry first.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

const inputVariants = cva(
  "flex w-full rounded-md border border-input bg-background px-2 py-1 transition-[color,border-color] duration-select-out ease-out-quart file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-ring aria-[invalid=true]:border-destructive disabled:cursor-not-allowed disabled:border-border disabled:bg-disabled disabled:text-disabled-foreground",
  {
    variants: {
      size: {
        sm: "h-control-sm text-control-sm",
        default: "h-control-default text-control-default",
        lg: "h-control-lg text-control-lg",
      },
    },
    defaultVariants: {
      size: "default",
    },
  },
);

export interface InputProps
  extends Omit<React.InputHTMLAttributes<HTMLInputElement>, "size">,
    VariantProps<typeof inputVariants> {}

const Input = React.forwardRef<HTMLInputElement, InputProps>(({ className, type, size, ...props }, ref) => (
  <input type={type} className={cn(inputVariants({ size, className }))} ref={ref} {...props} />
));
Input.displayName = "Input";

export { Input, inputVariants };
