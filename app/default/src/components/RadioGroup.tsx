import * as React from "react";
import * as RadioGroupPrimitive from "@radix-ui/react-radio-group";
import { cn } from "@/lib/cn";

/**
 * Gestalt console vendor of Valon Registry `radio-group`.
 *
 * Ownership: Valon Registry is the canonical source of truth
 * (`valon-tools/apps/registry/ui/src/ui/radio-group.tsx`, toolshed PR #3495).
 * This file is a **token adaptation only** — same public API
 * (`RadioGroup`, `RadioGroupItem`), same Radix semantics. Do not add
 * Choicebox / choice-card variants here; card layouts compose at the call site
 * (see Registry `ChoiceCardsGrid` story / Build `StarterSuggestions`).
 *
 * Why a local copy: `app/default` is not yet a Valon registry consumer (Gestalt
 * tokens ≠ Valon accent-* tokens). When the console installs from the registry,
 * delete this file and import `@/components/ui/radio-group` instead.
 *
 * @see https://reui.io/docs/radio-group
 * @see https://github.com/valon-technologies/toolshed/pull/3495
 */

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
  React.ComponentPropsWithoutRef<typeof RadioGroupPrimitive.Item>
>(({ className, ...props }, ref) => (
  <RadioGroupPrimitive.Item
    ref={ref}
    data-slot="radio-group-item"
    className={cn(
      // Token map (Valon → Gestalt): border-input → border-alpha;
      // accent-vivid → base-950 / base-100; focus-ring → ring-base-950/10;
      // disabled recolor (not opacity-only), matching Registry checkbox/radio.
      "aspect-square size-4 shrink-0 rounded-full border border-alpha text-base-950 outline-hidden",
      "focus-visible:ring-2 focus-visible:ring-base-950/10",
      "disabled:cursor-not-allowed disabled:border-alpha disabled:bg-alpha-5 disabled:text-faint",
      "data-[state=checked]:border-base-950",
      "dark:text-base-100 dark:focus-visible:ring-base-200/10 dark:data-[state=checked]:border-base-100",
      className,
    )}
    {...props}
  >
    <RadioGroupPrimitive.Indicator
      data-slot="radio-group-indicator"
      className="flex items-center justify-center"
    >
      <span className="block size-2 rounded-full bg-current" />
    </RadioGroupPrimitive.Indicator>
  </RadioGroupPrimitive.Item>
));
RadioGroupItem.displayName = RadioGroupPrimitive.Item.displayName;

export { RadioGroup, RadioGroupItem };
