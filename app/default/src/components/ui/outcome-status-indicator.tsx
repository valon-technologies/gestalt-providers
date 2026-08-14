/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 * Registry `outcome-status-indicator` (toolshed#4181, #4289). Map Registry
 * mid-dark ramp fills onto `--status-indicator-*`. Failure Badge pairing stays
 * `destructive` here (Registry uses `error`; this Badge has no `error`
 * variant yet).
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import {
  Check,
  Circle,
  CircleAlert,
  CircleDashed,
  Info,
  Loader2,
  Minus,
  SkipForward,
  type LucideIcon,
  X,
} from "lucide-react";

import { cn } from "@/lib/cn";

/**
 * Filled semantic circle + symbol for any outcome (connection, deploy, job, …).
 * Domain-neutral — not CI/run-specific. Workflow UIs use `RunStatusIndicator`
 * as a thin vocabulary adapter over this primitive.
 *
 * Distinct from uptime `StatusIndicator` dots. Table-row gutters use this
 * primitive (`iconOnly`); `TableStatusIndicator` is a `variant` adapter over it.
 *
 * Chromatic outcomes use mid-dark status-indicator fills + white symbols so
 * thin strokes stay legible and meet non-text contrast. Do not reuse
 * `*-solid` / `*-solid-foreground` here — those pairs are dark ink on lighter
 * fills for badges/meters, not Actions-style light-on-fill glyphs.
 */
// One size tier owns both the filled circle and the inner Lucide stroke.
// Glyph class is applied on <Icon> so ancestor menus (`[&_svg]:size-*`) cannot
// enlarge it. Circle class stays on the shell (cva `size`).
const OUTCOME_STATUS_SIZE = {
  sm: { circle: "size-4", glyph: "size-2.5" },
  md: { circle: "size-5", glyph: "size-3" },
  lg: { circle: "size-6", glyph: "size-3.5" },
} as const;

const outcomeStatusIndicatorVariants = cva(
  // overflow-hidden keeps Lucide strokes inside the filled circle.
  "inline-flex shrink-0 items-center justify-center overflow-hidden rounded-full [&>svg]:pointer-events-none",
  {
    variants: {
      status: {
        success: "bg-status-indicator-success text-white",
        failure: "bg-status-indicator-danger text-white",
        warning: "bg-status-indicator-warning text-white",
        info: "bg-status-indicator-info text-white",
        in_progress: "bg-status-indicator-warning text-white",
        canceled: "bg-muted-foreground text-background",
        skipped: "bg-muted-foreground/70 text-background",
        pending: "bg-transparent text-muted-foreground",
        unknown: "bg-foreground/[0.06] text-muted-foreground",
      },
      size: {
        sm: OUTCOME_STATUS_SIZE.sm.circle,
        md: OUTCOME_STATUS_SIZE.md.circle,
        lg: OUTCOME_STATUS_SIZE.lg.circle,
      },
    },
    defaultVariants: {
      status: "unknown",
      size: "md",
    },
  },
);

const STATUS_META: Record<
  NonNullable<VariantProps<typeof outcomeStatusIndicatorVariants>["status"]>,
  { icon: LucideIcon; defaultLabel: string; spin?: boolean }
> = {
  success: { icon: Check, defaultLabel: "Success" },
  failure: { icon: X, defaultLabel: "Failed" },
  warning: { icon: CircleAlert, defaultLabel: "Warning" },
  info: { icon: Info, defaultLabel: "Information" },
  in_progress: { icon: Loader2, defaultLabel: "In progress", spin: true },
  canceled: { icon: Minus, defaultLabel: "Canceled" },
  skipped: { icon: SkipForward, defaultLabel: "Skipped" },
  pending: { icon: Circle, defaultLabel: "Pending" },
  unknown: { icon: CircleDashed, defaultLabel: "Unknown" },
};

export type OutcomeStatus = NonNullable<
  VariantProps<typeof outcomeStatusIndicatorVariants>["status"]
>;

export type OutcomeStatusIndicatorSize = keyof typeof OUTCOME_STATUS_SIZE;

/**
 * Soft feedback Badge variant for each outcome (not action chrome).
 * Failure → `destructive` until Badge gains Registry `error`.
 */
export function outcomeStatusIndicatorBadgeVariant(
  status: OutcomeStatus,
): "success" | "warning" | "destructive" | "info" | "secondary" | "muted" {
  switch (status) {
    case "success":
      return "success";
    case "failure":
      return "destructive";
    case "warning":
    case "in_progress":
      return "warning";
    case "info":
      return "info";
    case "pending":
      return "secondary";
    case "canceled":
    case "skipped":
    case "unknown":
      return "muted";
  }
}

export type OutcomeStatusIndicatorProps = Omit<
  React.HTMLAttributes<HTMLSpanElement>,
  "children"
> &
  VariantProps<typeof outcomeStatusIndicatorVariants> & {
    /** Visible label beside the glyph. */
    label?: string;
    /** Icon-only — still exposes `aria-label`. */
    iconOnly?: boolean;
  };

function OutcomeStatusIndicator({
  className,
  status = "unknown",
  size = "md",
  label,
  iconOnly = false,
  ...props
}: OutcomeStatusIndicatorProps) {
  const resolved = status ?? "unknown";
  const { icon: Icon, defaultLabel, spin } = STATUS_META[resolved];
  // Empty string is treated as omitted (common when binding `label={name}`).
  const customLabel = label != null && label.length > 0 ? label : undefined;
  const visibleLabel = iconOnly ? undefined : (customLabel ?? defaultLabel);
  const ariaLabel = iconOnly ? (customLabel ?? defaultLabel) : undefined;
  // Labeled mode shares a text-sm/leading-5 line — size only scales icon-only gutters.
  const glyphSize = visibleLabel ? "md" : (size ?? "md");

  const glyph = (
    <span
      className={outcomeStatusIndicatorVariants({
        status: resolved,
        size: glyphSize,
      })}
      aria-hidden
    >
      <Icon
        strokeWidth={2.5}
        className={cn(
          OUTCOME_STATUS_SIZE[glyphSize].glyph,
          spin ? "animate-spin motion-reduce:animate-none" : undefined,
        )}
        aria-hidden
      />
    </span>
  );

  return (
    <span
      data-slot="outcome-status-indicator"
      data-status={resolved}
      role={iconOnly ? "img" : undefined}
      aria-label={iconOnly ? ariaLabel : undefined}
      className={cn(
        // First-line rail = label leading-5 (`h-5`): glyph centers on line 1
        // when the label wraps. Size scales the circle inside that rail.
        "inline-flex gap-2",
        visibleLabel ? "items-start" : "items-center",
        iconOnly && "justify-center",
        className,
      )}
      {...props}
    >
      {visibleLabel ? (
        <span className="inline-flex h-5 shrink-0 items-center">{glyph}</span>
      ) : (
        glyph
      )}
      {visibleLabel ? (
        <span className="text-sm leading-5 text-foreground">{visibleLabel}</span>
      ) : null}
    </span>
  );
}

export { OutcomeStatusIndicator, outcomeStatusIndicatorVariants };
