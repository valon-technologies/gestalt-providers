/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import {
  Check,
  Circle,
  CircleDashed,
  Loader2,
  Minus,
  SkipForward,
  type LucideIcon,
  X,
} from "lucide-react";

import { cn } from "@/lib/cn";

/**
 * GitHub Actions–style run/job outcome glyph: filled semantic circle + symbol.
 * Distinct from uptime `StatusIndicator` dots and from table-row
 * `TableStatusIndicator` severity (no running/canceled/skipped there).
 *
 * Chromatic outcomes use mid-chroma status-indicator fills + white symbols so
 * thin strokes stay legible (Registry `*-500` → `--status-indicator-*` bridge;
 * same recipe as fleet replica dots). Do not reuse soft Badge washes here.
 */
const runStatusIndicatorVariants = cva(
  "inline-flex shrink-0 items-center justify-center rounded-full [&>svg]:pointer-events-none",
  {
    variants: {
      status: {
        succeeded: "bg-status-indicator-success text-white",
        // Feedback red (not `--destructive` action), same light-on-fill recipe.
        failed: "bg-status-indicator-danger text-white",
        canceled: "bg-muted-foreground text-background",
        skipped: "bg-muted-foreground/70 text-background",
        running: "bg-status-indicator-warning text-white",
        // Hollow mark is the Lucide Circle stroke alone — no shell border (avoids double ring).
        pending: "bg-transparent text-muted-foreground",
        unknown: "bg-foreground/[0.06] text-muted-foreground",
      },
      size: {
        sm: "size-4 [&>svg]:size-2.5",
        md: "size-5 [&>svg]:size-3",
        lg: "size-6 [&>svg]:size-3.5",
      },
    },
    defaultVariants: {
      status: "unknown",
      size: "md",
    },
  },
);

const STATUS_META: Record<
  NonNullable<VariantProps<typeof runStatusIndicatorVariants>["status"]>,
  { icon: LucideIcon; defaultLabel: string; spin?: boolean }
> = {
  succeeded: { icon: Check, defaultLabel: "Succeeded" },
  failed: { icon: X, defaultLabel: "Failed" },
  canceled: { icon: Minus, defaultLabel: "Canceled" },
  skipped: { icon: SkipForward, defaultLabel: "Skipped" },
  running: { icon: Loader2, defaultLabel: "Running", spin: true },
  pending: { icon: Circle, defaultLabel: "Pending" },
  unknown: { icon: CircleDashed, defaultLabel: "Unknown" },
};

export type RunStatusIndicatorStatus = NonNullable<
  VariantProps<typeof runStatusIndicatorVariants>["status"]
>;

export type RunStatusIndicatorSize = NonNullable<
  VariantProps<typeof runStatusIndicatorVariants>["size"]
>;

/** Badge variant that matches shell fill/ink for each run status. */
export function runStatusIndicatorBadgeVariant(
  status: RunStatusIndicatorStatus,
): "success" | "warning" | "destructive" | "secondary" | "muted" {
  switch (status) {
    case "succeeded":
      return "success";
    case "failed":
      // Registry returns `"error"`; this Badge API uses `"destructive"`.
      return "destructive";
    case "running":
      return "warning";
    case "pending":
      return "secondary";
    case "canceled":
    case "skipped":
    case "unknown":
      return "muted";
  }
}

export type RunStatusIndicatorProps = Omit<
  React.HTMLAttributes<HTMLSpanElement>,
  "children"
> &
  VariantProps<typeof runStatusIndicatorVariants> & {
    /** Visible label beside the glyph. */
    label?: string;
    /** Icon-only — still exposes `aria-label`. */
    iconOnly?: boolean;
  };

function RunStatusIndicator({
  className,
  status = "unknown",
  size = "md",
  label,
  iconOnly = false,
  ...props
}: RunStatusIndicatorProps) {
  const resolved = status ?? "unknown";
  const { icon: Icon, defaultLabel, spin } = STATUS_META[resolved];
  // Empty string is treated as omitted (common when binding `label={run.name}`).
  const customLabel = label != null && label.length > 0 ? label : undefined;
  const visibleLabel = iconOnly ? undefined : (customLabel ?? defaultLabel);
  const ariaLabel = iconOnly ? (customLabel ?? defaultLabel) : undefined;
  // Labeled mode shares a text-sm/leading-5 line — size only scales icon-only gutters.
  const glyphSize = visibleLabel ? "md" : (size ?? "md");

  const glyph = (
    <span
      className={runStatusIndicatorVariants({ status: resolved, size: glyphSize })}
      aria-hidden
    >
      <Icon
        strokeWidth={2.5}
        className={spin ? "animate-spin motion-reduce:animate-none" : undefined}
        aria-hidden
      />
    </span>
  );

  return (
    <span
      data-slot="run-status-indicator"
      data-status={resolved}
      role={iconOnly ? "img" : undefined}
      aria-label={iconOnly ? ariaLabel : undefined}
      className={cn(
        // First-line rail = label leading-5 (`h-5`): glyph centers on line 1
        // when the label wraps (DropzoneIcon / Alert pattern). Size scales the
        // circle inside that rail — do not resize the rail with `size`.
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

export { RunStatusIndicator, runStatusIndicatorVariants };
