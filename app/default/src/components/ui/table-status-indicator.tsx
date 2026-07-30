/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import {
  Check,
  Circle,
  CircleAlert,
  Info,
  type LucideIcon,
  X,
} from "lucide-react";

import { cn } from "@/lib/cn";

// Carbon-style icon shell — fill/ink must match Badge status variants in this bundle
// (`badge.tsx` uses --badge-* surfaces so legacy gestalt-shell --success overrides
// do not recolor chips). Registry upstream uses bg-success; gp maps badge parity here.
const tableStatusIndicatorVariants = cva(
  "inline-flex shrink-0 items-center justify-center [&>svg]:pointer-events-none [&>svg]:size-3",
  {
    variants: {
      variant: {
        default: "size-5 rounded-full bg-foreground/[0.06] text-foreground/80",
        success:
          "size-5 rounded-full bg-badge-success text-badge-success-foreground",
        danger:
          "size-5 rounded-full bg-badge-destructive text-badge-destructive-foreground",
        warning:
          "size-5 rounded-full bg-badge-warning text-badge-warning-foreground",
        info: "size-5 rounded-full bg-badge-info text-badge-info-foreground",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  },
);

const VARIANT_META: Record<
  NonNullable<VariantProps<typeof tableStatusIndicatorVariants>["variant"]>,
  { icon: LucideIcon; defaultLabel: string }
> = {
  default: { icon: Circle, defaultLabel: "Normal" },
  success: { icon: Check, defaultLabel: "Succeeded" },
  danger: { icon: X, defaultLabel: "Failed" },
  warning: { icon: CircleAlert, defaultLabel: "Caution" },
  info: { icon: Info, defaultLabel: "Information" },
};

export type TableStatusIndicatorVariant = NonNullable<
  VariantProps<typeof tableStatusIndicatorVariants>["variant"]
>;

export function tableStatusIndicatorBadgeVariant(
  variant: TableStatusIndicatorVariant,
): "success" | "warning" | "destructive" | "info" | "secondary" {
  switch (variant) {
    case "success":
      return "success";
    case "warning":
      return "warning";
    case "danger":
      return "destructive";
    case "info":
      return "info";
    case "default":
      return "secondary";
  }
}

export type TableStatusIndicatorProps = React.HTMLAttributes<HTMLSpanElement> &
  VariantProps<typeof tableStatusIndicatorVariants> & {
    label?: string;
    iconOnly?: boolean;
  };

function TableStatusIndicator({
  className,
  variant = "default",
  label,
  iconOnly = false,
  ...props
}: TableStatusIndicatorProps) {
  const resolved = variant ?? "default";
  const { icon: Icon, defaultLabel } = VARIANT_META[resolved];
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
      data-slot="table-status-indicator"
      data-variant={resolved}
      data-testid="table-status-indicator"
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
        className={tableStatusIndicatorVariants({ variant: resolved })}
        aria-hidden
      >
        <Icon strokeWidth={2.5} aria-hidden />
      </span>
      {visibleLabel ? (
        <span className="text-sm text-foreground">{visibleLabel}</span>
      ) : null}
    </span>
  );
}

export { TableStatusIndicator, tableStatusIndicatorVariants };
