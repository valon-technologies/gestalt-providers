import * as React from "react";
import * as SwitchPrimitive from "@radix-ui/react-switch";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

/**
 * Gestalt console vendor of Valon Registry `switch`.
 *
 * Ownership: Valon Registry is the canonical source of truth
 * (`valon-tools/apps/registry/ui/src/ui/switch.tsx`). Token adaptation only —
 * same Radix semantics (`role="switch"`).
 *
 * Theme bridge: `--accent-solid` / `--input` / `--disabled*` in
 * `shared/theme.css` + `globals.css` `@theme inline`.
 */

// Sliding on/off Switch (Radix `role="switch"`): track + thumb. Checked fill is
// `accent-solid` (gold-400 mid-tone — same as Radio checked border / Tabs
// underline), never `primary` (that reads as the view's one primary action).
// Track color SNAP (transitions.md); only the thumb's transform eases. Thumb
// travel uses `duration-overshoot` + `ease-out-back` — soft is only ~0.8px of
// overshoot on this short travel and reads as no bounce; full back gives ~2px
// (SegmentedControl keeps soft for its longer pill).
//
// Disabled (checkbox-aligned, achromatic):
//   off  → `--disabled` fill + `--border` outline — same tokens as disabled
//          Checkbox (`disabled:bg-disabled` + `disabled:border-border`). Outline
//          (not layout border) keeps thumb gutters even.
//   on   → darker `--disabled-foreground` fill + bright paper thumb (mimics
//          accent-solid + white without brand hue).
// `peer` wires inline Label `peer-disabled:` recolor. No layout border — a
// transparent border-box border previously made Y gutter 1px vs X 2px.
// Track = thumb + 4px pad; checked translate is `translate-x-full`.
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
  // Disabled thumb: dark on the light outlined off track; bright paper on the
  // darker on track — same contrast idea as enabled (paper thumb on filled track).
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
