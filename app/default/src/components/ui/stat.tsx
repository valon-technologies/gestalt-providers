/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { Minus, TrendingDown, TrendingUp } from "lucide-react";

import { cardVariants } from "@/components/ui/card";
import { Eyebrow } from "@/components/ui/eyebrow";
import { cn } from "@/lib/cn";

const statVariants = cva("flex flex-col gap-2", {
  variants: {
    variant: {
      outline: cn(cardVariants({ variant: "outline" }), "p-4"),
      solid: cn(cardVariants({ variant: "solid" }), "p-4"),
      plain: "",
    },
    align: {
      start: "items-start text-left",
      center: "items-center text-center",
    },
  },
  defaultVariants: {
    variant: "outline",
    align: "start",
  },
});

export interface StatProps
  extends React.HTMLAttributes<HTMLDivElement>,
    VariantProps<typeof statVariants> {}

const Stat = React.forwardRef<HTMLDivElement, StatProps>(
  ({ className, align = "start", variant = "outline", ...props }, ref) => (
    <div
      ref={ref}
      data-slot="stat"
      data-align={align ?? "start"}
      data-variant={variant ?? "outline"}
      className={cn(statVariants({ align, variant }), className)}
      {...props}
    />
  ),
);
Stat.displayName = "Stat";

function StatGroup({ className, children, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="stat-group"
      className={cn("max-w-full overflow-x-clip", className)}
      {...props}
    >
      <div
        data-slot="stat-group-track"
        className={cn(
          "inline-flex max-w-none flex-wrap items-stretch gap-y-8",
          "-ms-[calc(3rem+1px+3rem)]",
          "[&>[data-slot=stat]]:ms-12 [&>[data-slot=stat]]:border-s [&>[data-slot=stat]]:border-border [&>[data-slot=stat]]:ps-12",
        )}
      >
        {children}
      </div>
    </div>
  );
}

function StatLabel(props: React.ComponentProps<typeof Eyebrow>) {
  return <Eyebrow {...props} />;
}

function StatValue({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="stat-value"
      className={cn(
        "font-display text-3xl font-normal tracking-wide text-foreground",
        className,
      )}
      {...props}
    />
  );
}

const statDetailVariants = cva("flex text-sm text-muted-foreground", {
  variants: {
    layout: {
      inline:
        "flex-wrap items-center gap-x-1.5 gap-y-1 [[data-slot=stat][data-align=center]_&]:justify-center",
      stack: "w-full min-w-0 self-stretch flex-col items-stretch gap-2",
    },
  },
  defaultVariants: {
    layout: "inline",
  },
});

type StatDetailProps = React.ComponentProps<"div"> &
  VariantProps<typeof statDetailVariants>;

function StatDetail({ className, layout = "inline", ...props }: StatDetailProps) {
  return (
    <div
      data-slot="stat-detail"
      data-layout={layout ?? "inline"}
      className={cn(statDetailVariants({ layout }), className)}
      {...props}
    />
  );
}

type StatTrendProps = Omit<React.ComponentProps<"div">, "children"> & {
  value: number;
  label?: React.ReactNode;
};

function StatTrend({ value, label, className, ...props }: StatTrendProps) {
  const flat = value === 0;
  const positive = value > 0;
  const Icon = flat ? Minus : positive ? TrendingUp : TrendingDown;
  const tone = flat
    ? "text-muted-foreground"
    : positive
      ? "text-success-foreground"
      : "text-destructive";
  const formatted = flat ? "0%" : `${positive ? "+" : ""}${value}%`;

  return (
    <div
      data-slot="stat-trend"
      className={cn("inline-flex items-center gap-1 text-sm", className)}
      {...props}
    >
      <Icon className={cn("size-3.5 shrink-0", tone)} aria-hidden />
      <span className={cn("font-medium tracking-tight", tone)}>{formatted}</span>
      {label != null ? <span className="text-muted-foreground">{label}</span> : null}
    </div>
  );
}

export {
  Stat,
  StatGroup,
  StatLabel,
  StatValue,
  StatDetail,
  StatTrend,
  statVariants,
  statDetailVariants,
};
