import { cva } from "class-variance-authority";

// Shared row styling for menu surfaces (DropdownMenu, Select, Command): one
// source of truth for the hover/selection tint, asymmetric select motion, icon
// sizing, and disabled treatment. shadcn duplicates these per primitive, which
// is what lets them drift apart; sharing keeps every menu row identical.
//
// Idle hover/press use selectable-rows Neutral roles (`neutral-hover` /
// `neutral-pressed`). Popup selection (Select / Combobox) stays on a blank
// row with a solid trailing check — Accent vivid fill is for persistent list
// surfaces (Listbox / listItemInteraction), not flyout options.
//
// Active state is keyed off both `focus` (Radix) and `aria-selected` (cmdk) so
// the same class works across primitives; disabled likewise keys off
// `aria-disabled`, which BOTH primitives set to "true" when disabled. Their
// `data-disabled` does NOT agree — Radix Select emits an empty value
// (data-disabled=""), cmdk emits "true"/"false" — so a data-[disabled=true]
// selector silently missed disabled Radix Select options. `indicator` sets the
// gutter padding for a leading radio/checkbox or a trailing check; layout that
// legitimately differs (e.g. Select's value flexbox) stays at the call site.
//
// A pressed option must beat its own highlight. That highlight (`aria-selected`
// for cmdk / `focus` for Radix) has EQUAL specificity to `:active` and Tailwind
// emits it later, so `:active` would lose the cascade tie and the press would
// never show on a highlighted option. Instead of forcing it with !important, the
// highlight bg is scoped to `:not(:active)`: highlight and press become mutually
// exclusive, so the press wins with no specificity hack. (Table rows don't need
// this — their competitor is `:hover`, which `:active` already beats.)
export const menuItemVariants = cva(
  "relative flex cursor-default select-none items-center gap-2 rounded-md py-1.5 text-sm outline-none transition-colors duration-select-out ease-out-quart focus:not-active:bg-neutral-hover focus:text-foreground focus:duration-select-in aria-selected:not-active:bg-neutral-hover aria-selected:text-foreground aria-selected:duration-select-in active:bg-neutral-pressed active:text-foreground active:duration-press aria-disabled:pointer-events-none aria-disabled:opacity-50 aria-disabled:text-disabled-foreground [&_svg]:pointer-events-none [&_svg]:shrink-0 [&_svg:not([class*='size-'])]:size-4",
  {
    variants: {
      indicator: {
        none: "px-2",
        leading: "pl-8 pr-2",
        trailing: "pl-2 pr-8",
      },
    },
    defaultVariants: {
      indicator: "none",
    },
  },
);
