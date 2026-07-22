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
 * size variants). Choice-card layouts compose at the call site with
 * {@link choiceCardClassName}.
 *
 * @see app/default/AGENTS.md
 * @see valon-tools/apps/registry/ui/src/ui/radio-group.stories.tsx
 */

/**
 * Registry ChoiceCards / ChoiceCardsGrid tile chrome.
 * Keep in sync with `choiceCardClassName` in
 * `valon-tools/apps/registry/ui/src/ui/radio-group.stories.tsx`.
 * Call sites: layout + children only — do not fork this string.
 */
export const choiceCardClassName = [
  "relative flex cursor-pointer flex-col gap-1 rounded-lg border border-border bg-card p-4 pe-10 leading-normal",
  "hover:bg-neutral-hover active:bg-neutral-pressed",
  "has-[[data-state=checked]]:border-accent-vivid has-[[data-state=checked]]:bg-accent-vivid",
  "has-[[data-state=checked]]:hover:bg-accent-vivid-hover",
  "has-[[data-state=checked]]:active:bg-accent-vivid-pressed",
  "has-[[data-state=checked]]:[&_[data-choice-title]]:text-accent-vivid-foreground",
  "has-[[data-state=checked]]:[&_[data-choice-desc]]:text-accent-vivid-foreground/70",
  "has-[[data-state=checked]]:[&_[data-slot=eyebrow]]:text-accent-vivid-foreground/70",
  "has-[[data-state=checked]]:[&_[role=radio]]:border-transparent",
].join(" ");

const radioGroupItemVariants = cva(
  "group/radio peer flex aspect-square shrink-0 items-center justify-center rounded-full border border-input bg-background text-accent-solid focus-ring disabled:cursor-not-allowed disabled:border-border disabled:bg-disabled disabled:text-disabled-foreground disabled:data-[state=checked]:border-border disabled:data-[state=checked]:bg-disabled disabled:data-[state=checked]:text-disabled-foreground data-[state=checked]:border-accent-solid",
  {
    variants: {
      size: {
        sm: "size-3.5",
        default: "size-4",
        lg: "size-5",
      },
    },
    defaultVariants: { size: "default" },
  },
);

const RADIO_CENTER_IN =
  "scale-0 transition-none group-data-[state=checked]/radio:scale-100 group-data-[state=checked]/radio:transition-transform group-data-[state=checked]/radio:duration-[var(--duration-200)] group-data-[state=checked]/radio:ease-out-quart";

const radioGroupIndicatorVariants = cva(
  cn("block rounded-full bg-current", RADIO_CENTER_IN),
  {
    variants: {
      size: {
        sm: "size-1.5",
        default: "size-2",
        lg: "size-2.5",
      },
    },
    defaultVariants: { size: "default" },
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

const RadioGroupItem = React.forwardRef<
  React.ElementRef<typeof RadioGroupPrimitive.Item>,
  React.ComponentPropsWithoutRef<typeof RadioGroupPrimitive.Item> &
    VariantProps<typeof radioGroupItemVariants>
>(({ className, size, ...props }, ref) => (
  <RadioGroupPrimitive.Item
    ref={ref}
    data-slot="radio-group-item"
    className={cn(radioGroupItemVariants({ size, className }))}
    {...props}
  >
    <RadioGroupPrimitive.Indicator
      forceMount
      className="flex items-center justify-center"
    >
      <span className={cn(radioGroupIndicatorVariants({ size }))} />
    </RadioGroupPrimitive.Indicator>
  </RadioGroupPrimitive.Item>
));
RadioGroupItem.displayName = RadioGroupPrimitive.Item.displayName;

export { RadioGroup, RadioGroupItem, radioGroupItemVariants };
