"use client";

/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import * as SwitchPrimitive from "@radix-ui/react-switch";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

const switchVariants = cva(
  "peer group/switch inline-flex shrink-0 items-center rounded-full p-0.5 focus-ring disabled:cursor-not-allowed disabled:data-[state=unchecked]:bg-disabled disabled:data-[state=unchecked]:outline disabled:data-[state=unchecked]:outline-1 disabled:data-[state=unchecked]:outline-offset-0 disabled:data-[state=unchecked]:outline-border disabled:data-[state=checked]:bg-disabled-foreground data-[state=checked]:bg-accent-solid data-[state=unchecked]:bg-input",
  {
    variants: {
      size: {
        sm: "h-4 w-7",
        default: "h-5 w-9",
        lg: "h-6 w-11",
      },
    },
    defaultVariants: { size: "default" },
  },
);

const switchThumbVariants = cva(
  "pointer-events-none block rounded-full bg-background shadow-none ring-0 transition-transform duration-overshoot ease-out-back data-[state=checked]:translate-x-full data-[state=unchecked]:translate-x-0 group-disabled/switch:data-[state=unchecked]:bg-disabled-foreground group-disabled/switch:data-[state=checked]:bg-background",
  {
    variants: {
      size: {
        sm: "size-3",
        default: "size-4",
        lg: "size-5",
      },
    },
    defaultVariants: { size: "default" },
  },
);

const Switch = React.forwardRef<
  React.ElementRef<typeof SwitchPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof SwitchPrimitive.Root> &
    VariantProps<typeof switchVariants>
>(({ className, size, ...props }, ref) => (
  <SwitchPrimitive.Root
    ref={ref}
    data-slot="switch"
    className={cn(switchVariants({ size, className }))}
    {...props}
  >
    <SwitchPrimitive.Thumb
      data-slot="switch-thumb"
      className={cn(switchThumbVariants({ size }))}
    />
  </SwitchPrimitive.Root>
));
Switch.displayName = SwitchPrimitive.Root.displayName;

export { Switch, switchVariants };
