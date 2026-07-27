/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import { cva, type VariantProps } from "class-variance-authority";

export const choiceCardSelectionTransition =
  "transition-[box-shadow] duration-[var(--duration-100)] ease-out-expo motion-reduce:transition-none";

/** Inset-shadow selection rings — idle hairline, checked accent, disabled-checked mute. */
export const choiceCardSelectionShadowClassName = [
  "shadow-[inset_0_0_0_1px_var(--border)]",
  "has-[[data-slot=radio-group-item][data-state=checked]]:shadow-[inset_0_0_0_2px_var(--accent-solid)]",
  "has-[[data-slot=radio-group-item][data-state=checked]:disabled]:shadow-[inset_0_0_0_2px_var(--border)]",
].join(" ");

/** Inset-shadow selection with forced-colors border fallback (color-only — fixed 2px width). */
export const choiceCardForcedColorsClassName = [
  "forced-colors:shadow-none forced-colors:border forced-colors:border-2 forced-colors:border-[ButtonText]",
  "has-[[data-slot=radio-group-item][data-state=checked]]:forced-colors:border-[Highlight]",
  "has-[[data-slot=radio-group-item]:disabled]:forced-colors:border-[GrayText]",
].join(" ");

/** Parent-owned keyboard focus — accent-solid outline on the card (keyboard only). */
export const choiceCardFocusRingClassName = [
  "has-[[data-slot=radio-group-item]:focus-visible]:outline-2",
  "has-[[data-slot=radio-group-item]:focus-visible]:outline-offset-2",
  "has-[[data-slot=radio-group-item]:focus-visible]:outline-accent-solid",
].join(" ");

/** Neutral List Item hover/press for label-wrapped radio rows and choice cards. */
export const radioSelectableHoverClassName = [
  "transition-[color,background-color,border-color] duration-hover-out ease-out-quart hover:duration-hover-in",
  "hover:bg-neutral-hover active:bg-neutral-pressed",
].join(" ");

/** Disabled recolor when the radio lives inside the label (not peer-sibling). */
export const radioLabelWrappedDisabledClassName = [
  "has-[[data-slot=radio-group-item]:disabled]:cursor-not-allowed",
  "has-[[data-slot=radio-group-item]:disabled]:text-disabled-foreground",
  "has-[[data-slot=radio-group-item]:disabled]:[&_[data-choice-title]]:text-disabled-foreground",
  "has-[[data-slot=radio-group-item]:disabled]:[&_[data-choice-desc]]:text-disabled-foreground",
  "has-[[data-slot=radio-group-item]:disabled]:[&_[data-slot=eyebrow]]:text-disabled-foreground",
  "has-[[data-slot=radio-group-item]:disabled]:hover:bg-transparent",
  "has-[[data-slot=radio-group-item]:disabled]:active:bg-transparent",
].join(" ");

/** Label-wrapped default row with optional List Item wash (`DefaultWithRowHover`). */
export const radioRowClassName = [
  "flex cursor-pointer items-center gap-2 rounded-md px-2 py-1.5",
  radioLabelWrappedDisabledClassName,
  radioSelectableHoverClassName,
].join(" ");

// Optical center with the first text-sm line (20px lh) on a 16px radio disk.
export const choiceCardRadioClassName =
  "col-start-1 self-start mt-[calc((1.25rem-1rem)/2)]";

/** Row 2 placement when row 1 is full-width media (`choiceCardMediaAboveClassName`). */
export const choiceCardBelowMediaClassName = "row-start-2";

export const choiceCardMediaClassName = "col-span-2";

/** Media spans the card width on row 1; pair radio/copy with `choiceCardBelowMediaClassName`. */
export const choiceCardMediaAboveClassName = "col-span-2 row-start-1";

/** Inner wrapper for choice-card nested fields (`CollapsibleContent` className). */
export const choiceCardFormFieldsClassName =
  "grid min-w-0 gap-3 border-t border-border/60 px-4 pb-4 pt-3";

/** Choice-card hover — neutral wash plus outline-card suppress for nested controls. */
export const choiceCardHoverClassName = [
  radioSelectableHoverClassName,
  "[&:hover:has(a:not([data-row-link]):hover,button:not([data-slot=radio-group-item]):hover,input:hover,select:hover,textarea:hover,[role=button]:not([data-slot=radio-group-item]):hover,[role=checkbox]:hover,[role=combobox]:hover,[data-no-row-click]:hover)]:bg-card [&:active:has(a:not([data-row-link]):active,button:not([data-slot=radio-group-item]):active,input:active,select:active,textarea:active,[role=button]:not([data-slot=radio-group-item]):active,[role=checkbox]:active,[role=combobox]:active,[data-no-row-click]:active)]:bg-card",
].join(" ");

const choiceCardSharedClassName = [
  "relative cursor-pointer rounded-lg border-0 bg-card p-4 leading-normal",
  radioLabelWrappedDisabledClassName,
  choiceCardSelectionTransition,
  choiceCardSelectionShadowClassName,
  choiceCardForcedColorsClassName,
  choiceCardFocusRingClassName,
].join(" ");

/** Tile chrome — `indicator: "radio"` (disk column) or `"none"` (borderless card). */
export const choiceCardVariants = cva(choiceCardSharedClassName, {
  variants: {
    indicator: {
      radio: "grid grid-cols-[auto_1fr] gap-x-3 gap-y-1",
      none: "flex flex-col gap-1",
    },
  },
  defaultVariants: { indicator: "radio" },
});

/** Copy column inside a choice-card tile. */
export const choiceCardContentVariants = cva("flex min-w-0 flex-col gap-1", {
  variants: {
    indicator: {
      radio: "col-start-2",
      none: "",
    },
  },
  defaultVariants: { indicator: "radio" },
});

/** Radio disk placement — visible column vs sr-only hidden input. */
export const choiceCardRadioVariants = cva("", {
  variants: {
    indicator: {
      radio: choiceCardRadioClassName,
      none: "sr-only",
    },
  },
  defaultVariants: { indicator: "radio" },
});

export type ChoiceCardIndicator = NonNullable<
  VariantProps<typeof choiceCardVariants>["indicator"]
>;

/** Choice-card with visible radio disk column (default). */
export const choiceCardClassName = choiceCardVariants({ indicator: "radio" });

/** Choice-card without a visible radio disk — pair with `choiceCardRadioHiddenClassName`. */
export const choiceCardNoIndicatorClassName = choiceCardVariants({
  indicator: "none",
});

export const choiceCardContentClassName = choiceCardContentVariants({
  indicator: "radio",
});

export const choiceCardContentNoIndicatorClassName = choiceCardContentVariants({
  indicator: "none",
});

/** Visually hide the radio disk — keep `RadioGroupItem` for radiogroup a11y. */
export const choiceCardRadioHiddenClassName = choiceCardRadioVariants({
  indicator: "none",
});

/** Shell for choice cards that host nested fields (header in Label; drawer outside). */
export const choiceCardFormShellClassName = [
  "relative flex min-w-0 flex-col rounded-lg border-0 bg-card leading-normal",
  choiceCardSelectionTransition,
  choiceCardSelectionShadowClassName,
  choiceCardForcedColorsClassName,
  choiceCardFocusRingClassName,
].join(" ");
