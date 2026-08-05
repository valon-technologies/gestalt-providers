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
 * Distinct from uptime StatusIndicator dots and TableStatusIndicator severity.
 *
 * Shell fill/ink uses badge surface tokens so legacy gestalt-shell --success
 * overrides do not recolor glyphs (same bridge as table-status-indicator).
 */
const runStatusIndicatorVariants = cva(
  "inline-flex shrink-0 items-center justify-center rounded-full [&>svg]:pointer-events-none",
  {
    variants: {
      status: {
        // Solid fills match GitHub Actions outcome glyphs (not soft Badge washes).
        succeeded: "bg-success-solid text-success-solid-foreground",
        failed: "bg-destructive text-destructive-foreground",
        canceled: "bg-muted-foreground text-background",
        skipped: "bg-muted-foreground/70 text-background",
        running: "bg-warning-solid text-warning-solid-foreground",
        pending:
          "border border-muted-foreground/40 bg-transparent text-muted-foreground",
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

export function runStatusIndicatorBadgeVariant(
  status: RunStatusIndicatorStatus,
): "success" | "warning" | "destructive" | "info" | "secondary" | "muted" {
  switch (status) {
    case "succeeded":
      return "success";
    case "failed":
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
    label?: string;
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
  const visibleLabel = iconOnly
    ? undefined
    : label !== undefined
      ? label
      : defaultLabel;
  const ariaLabel = iconOnly
    ? label?.length
      ? label
      : defaultLabel
    : undefined;

  return (
    <span
      data-slot="run-status-indicator"
      data-status={resolved}
      role={iconOnly ? "img" : undefined}
      aria-label={iconOnly ? ariaLabel : undefined}
      className={cn(
        "inline-flex items-center gap-2",
        iconOnly && "justify-center",
        className,
      )}
      {...props}
    >
      <span
        className={runStatusIndicatorVariants({ status: resolved, size })}
        aria-hidden
      >
        <Icon
          strokeWidth={2.5}
          className={spin ? "animate-spin" : undefined}
          aria-hidden
        />
      </span>
      {visibleLabel ? (
        <span className="text-sm text-foreground">{visibleLabel}</span>
      ) : null}
    </span>
  );
}

export { RunStatusIndicator, runStatusIndicatorVariants };
