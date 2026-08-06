/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import * as TogglePrimitive from "@radix-ui/react-toggle";
import { cva, type VariantProps } from "class-variance-authority";
import { XIcon } from "lucide-react";

import {
  disabledCheckedFillClass,
  disabledSelection,
} from "@/lib/disabled-selection";
import { selectionInteraction } from "@/lib/selection-interaction";
import { cn } from "@/lib/cn";
import { SelectionCheck } from "@/components/ui/selection-check";

/**
 * Interactive Chip — M3 filter / assist roles (chips.md).
 *
 * Not Badge (static). Not Filters (field · operator · value). Enabled selected
 * fill is accent-vivid via selectionInteraction; disabled on/off via
 * disabledSelection (interaction-chrome.md / disabled-states.md).
 *
 * Selected glyph is SelectionCheck (stroke-draw enter / L→R exit wipe). Filter
 * toggles use `drawFrom="toggle"` against `group/chip` + `data-state=on`.
 *
 * `onRemove` is for dismissible *applied* tokens only: one pill `role="group"`
 * owns the chrome; remove is a focusable sibling `<button>` inside that pill
 * (never nested inside a toggle/button). Applied shells do not compose
 * `selectionInteraction` — they are always-on plates, not hover ladders.
 */
const chipShellVariants = cva(
  "relative inline-flex shrink-0 items-center justify-center gap-1 whitespace-nowrap rounded-full text-sm font-medium transition-[color,background-color,border-color] duration-hover-out ease-out-quart hover:duration-hover-in focus-ring after:pointer-events-none after:absolute after:inset-0 after:rounded-[inherit] after:bg-current after:opacity-0 after:transition-none disabled:cursor-not-allowed disabled:shadow-none disabled:after:hidden [&_svg]:pointer-events-none [&_svg]:shrink-0",
  {
    variants: {
      variant: {
        // Resting plate only — filter hover/press/selected live on
        // `chipFilterInteractionClassName` (toggle roots), not applied shells.
        filter: "border border-input bg-background text-foreground",
        // Assist: one-shot action — never stays pressed (Neutral affordance only).
        assist:
          "border border-input bg-background text-foreground enabled:hover:bg-neutral-hover enabled:active:bg-neutral-pressed enabled:active:after:opacity-0 disabled:border-border disabled:bg-transparent disabled:text-disabled-foreground",
      },
      size: {
        sm: "h-control-sm gap-1 px-2.5 text-control-sm [&_svg:not([class*='size-'])]:size-[length:var(--control-icon-sm)]",
        default:
          "h-control-default gap-1.5 px-3 text-control-default [&_svg:not([class*='size-'])]:size-[length:var(--control-icon-default)]",
        lg: "h-control-lg gap-1.5 px-3.5 text-control-lg [&_svg:not([class*='size-'])]:size-[length:var(--control-icon-lg)]",
      },
    },
    defaultVariants: {
      variant: "filter",
      size: "default",
    },
  },
);

/** Enabled + disabled selection chrome for filter toggle roots only. */
const chipFilterInteractionClassName = cn(
  disabledSelection({ marker: "state-on", unchecked: "outline" }),
  selectionInteraction({
    marker: "state-on",
    bordered: true,
    selectedWeight: "inherit",
  }),
);

/**
 * Full filter/assist class recipe for toggle roots and assist buttons.
 * Applied tokens use `chipShellVariants` without this interaction bundle.
 */
const chipVariants = (
  props: Parameters<typeof chipShellVariants>[0],
) => {
  const classes = chipShellVariants(props);
  if (props?.variant === "assist") return classes;
  return cn(classes, chipFilterInteractionClassName);
};

/** Applied-token on fill — same Accent vivid plate as filter `data-state=on`. */
const chipAppliedOnClassName =
  "border-accent-vivid bg-accent-vivid text-accent-vivid-foreground";

type ChipSharedProps = {
  /** Show leading check when filter chip is pressed (default true). */
  showSelectedCheck?: boolean;
};

type ChipRemoveProps = {
  /** Trailing remove — dismissible applied tokens only (sibling button inside pill). */
  onRemove: () => void;
  /** Accessible name for the remove control — must name what is dismissed. */
  removeLabel: string;
};

type ChipNoRemoveProps = {
  onRemove?: undefined;
  removeLabel?: undefined;
};

type FilterChipProps = Omit<
  React.ComponentProps<typeof TogglePrimitive.Root>,
  "children"
> &
  VariantProps<typeof chipShellVariants> &
  ChipSharedProps & {
    variant?: "filter";
    children?: React.ReactNode;
  };

type AssistChipProps = Omit<React.ComponentProps<"button">, "children"> &
  VariantProps<typeof chipShellVariants> &
  ChipSharedProps & {
    variant: "assist";
    children?: React.ReactNode;
  };

export type ChipProps =
  | ((FilterChipProps | AssistChipProps) & ChipRemoveProps)
  | ((FilterChipProps | AssistChipProps) & ChipNoRemoveProps);

function ChipRemove({
  onRemove,
  removeLabel,
  disabled,
}: {
  onRemove: () => void;
  removeLabel: string;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      data-slot="chip-remove"
      aria-label={removeLabel}
      disabled={disabled}
      className="focus-ring relative z-10 -mr-1 inline-flex size-6 shrink-0 items-center justify-center rounded-full text-current hover:bg-current/10 disabled:cursor-not-allowed disabled:hover:bg-transparent"
      onClick={(event) => {
        event.preventDefault();
        event.stopPropagation();
        onRemove();
      }}
    >
      <XIcon className="size-3.5" aria-hidden />
    </button>
  );
}

/**
 * Leading SelectionCheck + label cluster so collapsing the check does not
 * leave flex gap. Shared by standalone Chip and ChipGroupItem.
 *
 * Children keep their own gap (icons + label). Margin after the check is gated
 * on `data-state=on` so the off state does not reserve a hole.
 */
function ChipFilterContent({
  showSelectedCheck = true,
  size = "default",
  children,
}: {
  showSelectedCheck?: boolean;
  size?: VariantProps<typeof chipShellVariants>["size"];
  children?: React.ReactNode;
}) {
  if (!showSelectedCheck) return children;
  const childGap =
    size === "sm" ? "gap-1" : size === "lg" ? "gap-1.5" : "gap-1.5";
  return (
    <span className="inline-flex min-w-0 items-center">
      <SelectionCheck
        drawFrom="toggle"
        tone="current"
        className="group-data-[state=on]/chip:me-1"
      />
      <span className={cn("inline-flex min-w-0 items-center", childGap)}>
        {children}
      </span>
    </span>
  );
}

/**
 * Dismissible applied token — one pill owns chrome; remove is a sibling button
 * inside the pill (not nested in a toggle). Always filter on-chrome without
 * selectionInteraction hover ladders (spans match `:enabled` otherwise).
 */
function ChipApplied({
  className,
  size,
  onRemove,
  removeLabel,
  showSelectedCheck = true,
  disabled,
  children,
  "aria-label": ariaLabel,
}: {
  className?: string;
  size: VariantProps<typeof chipShellVariants>["size"];
  onRemove: () => void;
  removeLabel: string;
  showSelectedCheck?: boolean;
  disabled?: boolean;
  children?: React.ReactNode;
  "aria-label"?: string;
}) {
  return (
    <span
      role="group"
      data-slot="chip"
      data-variant="filter"
      data-state="on"
      aria-disabled={disabled || undefined}
      aria-label={ariaLabel}
      className={cn(
        chipShellVariants({ variant: "filter", size }),
        disabled ? disabledCheckedFillClass : chipAppliedOnClassName,
        disabled && "cursor-not-allowed after:hidden",
        "gap-0.5 pe-1.5",
        className,
      )}
    >
      {showSelectedCheck ? (
        // Mount-as-checked = presence only (no bounce) — applied tokens start on.
        <SelectionCheck checked tone="current" className="-ml-0.5" />
      ) : null}
      {children}
      <ChipRemove
        onRemove={onRemove}
        removeLabel={removeLabel}
        disabled={disabled}
      />
    </span>
  );
}

function Chip({
  className,
  variant = "filter",
  size,
  onRemove,
  removeLabel,
  showSelectedCheck = true,
  children,
  ...props
}: ChipProps) {
  // Applied dismissible token: not a toggle — remove is the only off path.
  // Always filter on-chrome; `variant="assist"` + onRemove is coerced here.
  if (onRemove) {
    const { "aria-label": ariaLabel, disabled } = props as FilterChipProps &
      AssistChipProps;
    return (
      <ChipApplied
        className={className}
        size={size}
        onRemove={onRemove}
        removeLabel={removeLabel}
        showSelectedCheck={showSelectedCheck}
        disabled={disabled}
        aria-label={ariaLabel}
      >
        {children}
      </ChipApplied>
    );
  }

  if (variant === "assist") {
    const { type = "button", ...assistProps } = props as AssistChipProps;
    return (
      <button
        type={type}
        data-slot="chip"
        data-variant="assist"
        className={cn(chipVariants({ variant: "assist", size }), className)}
        {...assistProps}
      >
        {children}
      </button>
    );
  }

  const filterProps = props as FilterChipProps;

  return (
    <TogglePrimitive.Root
      data-slot="chip"
      data-variant="filter"
      className={cn(
        "group/chip",
        chipVariants({ variant: "filter", size }),
        className,
      )}
      {...filterProps}
    >
      <ChipFilterContent showSelectedCheck={showSelectedCheck} size={size}>
        {children}
      </ChipFilterContent>
    </TogglePrimitive.Root>
  );
}

export { Chip, ChipFilterContent, chipVariants };
