import * as React from "react";
import * as RadioGroupPrimitive from "@radix-ui/react-radio-group";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/cn";

/**
 * Gestalt console vendor of Valon Registry `radio-group`.
 *
 * Ownership: Valon Registry is the canonical source of truth
 * (`valon-tools/apps/registry/ui/src/ui/radio-group.tsx`).
 * Token adaptation only — same public API (`RadioGroup`, `RadioGroupItem`,
 * size variants, `focusRing`). Choice-card layouts compose via
 * `@/lib/choice-card-chrome`.
 *
 * @see app/default/src/components/ui/PORTING.md
 * @see valon-tools/apps/registry/ui/src/ui/radio-group.stories.tsx
 */

export {
  choiceCardBelowMediaClassName,
  choiceCardClassName,
  choiceCardContentClassName,
  choiceCardFormFieldsClassName,
  choiceCardFormShellClassName,
  choiceCardForcedColorsClassName,
  choiceCardFocusRingClassName,
  choiceCardHoverClassName,
  choiceCardMediaAboveClassName,
  choiceCardMediaClassName,
  choiceCardRadioClassName,
  choiceCardRadioEyebrowClassName,
  choiceCardSelectionShadowClassName,
  choiceCardSelectionTransition,
  radioLabelWrappedDisabledClassName,
  radioRowClassName,
  radioSelectableHoverClassName,
} from "@/lib/choice-card-chrome";

const RADIO_RING_TRANSITION =
  "transition-[box-shadow] duration-[var(--duration-100)] ease-out-expo motion-reduce:transition-none";

const radioGroupItemVariants = cva(
  cn(
    "peer flex aspect-square shrink-0 items-center justify-center rounded-full border-0 bg-background",
    RADIO_RING_TRANSITION,
    "shadow-[inset_0_0_0_1px_var(--input)]",
    "forced-colors:shadow-none forced-colors:border forced-colors:border-2 forced-colors:border-[ButtonText]",
    "data-[state=checked]:shadow-[inset_0_0_0_5px_var(--accent-solid)]",
    "forced-colors:data-[state=checked]:border-[Highlight] forced-colors:data-[state=checked]:border-[5px]",
    "disabled:cursor-not-allowed disabled:bg-disabled disabled:shadow-[inset_0_0_0_1px_var(--border)]",
    "disabled:data-[state=checked]:bg-disabled disabled:data-[state=checked]:shadow-[inset_0_0_0_5px_var(--border)]",
    "forced-colors:disabled:border-[GrayText]",
    "forced-colors:disabled:data-[state=checked]:border-[GrayText] forced-colors:disabled:data-[state=checked]:border-2",
  ),
  {
    variants: {
      size: {
        sm: "size-3.5",
        default: "size-4",
        lg: "size-5",
      },
      focusRing: {
        item: "focus-ring",
        none: "",
      },
    },
    defaultVariants: { size: "default", focusRing: "item" },
  },
);

const RadioGroup = React.forwardRef<
  React.ElementRef<typeof RadioGroupPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof RadioGroupPrimitive.Root>
>(({ className, ...props }, ref) => (
  <RadioGroupPrimitive.Root
    ref={ref}
    data-slot="radio-group"
    className={cn("grid gap-2", className)}
    {...props}
  />
));
RadioGroup.displayName = RadioGroupPrimitive.Root.displayName;

type RadioGroupItemProps = React.ComponentPropsWithoutRef<
  typeof RadioGroupPrimitive.Item
> &
  VariantProps<typeof radioGroupItemVariants> & {
    /** `item` (default) paints focus-ring on the disk; `none` for parent-owned focus (choice cards). */
    focusRing?: "item" | "none";
  };

const RadioGroupItem = React.forwardRef<
  React.ElementRef<typeof RadioGroupPrimitive.Item>,
  RadioGroupItemProps
>(({ className, size, focusRing = "item", ...props }, ref) => (
  <RadioGroupPrimitive.Item
    ref={ref}
    data-slot="radio-group-item"
    className={cn(radioGroupItemVariants({ size, focusRing }), className)}
    {...props}
  >
    <RadioGroupPrimitive.Indicator forceMount className="hidden" />
  </RadioGroupPrimitive.Item>
));
RadioGroupItem.displayName = RadioGroupPrimitive.Item.displayName;

export { RadioGroup, RadioGroupItem, radioGroupItemVariants };
