import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

/**
 * Gestalt console vendor of Valon Registry `timeline-steps`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/timeline-steps.tsx`). Token-adapted —
 * primary/muted/border/warning resolve via the console theme bridge.
 */

/* -----------------------------------------------------------------------------
 * Timeline (root container)
 * -------------------------------------------------------------------------- */

const timelineStepsVariants = cva("flex [--timeline-steps-icon-size:2.5rem]", {
  variants: {
    orientation: {
      vertical: "flex-col",
      horizontal: "flex-row overflow-x-auto",
    },
    position: {
      left: "",
      right: "",
      alternate: "",
    },
  },
  defaultVariants: {
    orientation: "vertical",
    position: "left",
  },
});

type TimelineStepsOrientation = NonNullable<
  VariantProps<typeof timelineStepsVariants>["orientation"]
>;
type TimelineStepsPosition = NonNullable<
  VariantProps<typeof timelineStepsVariants>["position"]
>;
type TimelineStepsStatus =
  | "default"
  | "completed"
  | "current"
  | "upcoming"
  | "warning";

const TimelineStepsOrientationContext =
  React.createContext<TimelineStepsOrientation>("vertical");

function useTimelineStepsOrientation(
  orientation?: TimelineStepsOrientation | null,
) {
  const contextOrientation = React.useContext(TimelineStepsOrientationContext);
  return orientation ?? contextOrientation;
}

interface TimelineStepsProps
  extends React.ComponentProps<"div">,
    VariantProps<typeof timelineStepsVariants> {}

function TimelineSteps({
  className,
  orientation,
  position,
  ...props
}: TimelineStepsProps) {
  const resolvedOrientation = orientation ?? "vertical";
  return (
    <TimelineStepsOrientationContext.Provider value={resolvedOrientation}>
      <div
        data-slot="timeline-steps"
        data-orientation={resolvedOrientation}
        data-position={position}
        className={cn(
          timelineStepsVariants({
            orientation: resolvedOrientation,
            position,
          }),
          className,
        )}
        {...props}
      />
    </TimelineStepsOrientationContext.Provider>
  );
}

/* -----------------------------------------------------------------------------
 * TimelineItem
 * -------------------------------------------------------------------------- */

const timelineStepsItemVariants = cva(
  "relative flex flex-col [--timeline-steps-icon-size:2.5rem] has-[[data-size=sm]]:[--timeline-steps-icon-size:1.5rem] has-[[data-size=lg]]:[--timeline-steps-icon-size:3rem]",
  {
    variants: {
      orientation: {
        vertical: "pb-8 last:pb-0",
        horizontal: "flex-1 items-center",
      },
      status: {
        default: "",
        completed: "",
        current: "",
        upcoming: "opacity-60",
        warning: "",
      },
    },
    defaultVariants: {
      orientation: "vertical",
      status: "default",
    },
  },
);

interface TimelineStepsItemProps
  extends React.ComponentProps<"div">,
    VariantProps<typeof timelineStepsItemVariants> {}

function TimelineStepsItem({
  className,
  orientation,
  status,
  ...props
}: TimelineStepsItemProps) {
  const resolvedOrientation = useTimelineStepsOrientation(orientation);
  return (
    <TimelineStepsOrientationContext.Provider value={resolvedOrientation}>
      <div
        data-slot="timeline-steps-item"
        data-orientation={resolvedOrientation}
        data-status={status}
        className={cn(
          timelineStepsItemVariants({
            orientation: resolvedOrientation,
            status,
          }),
          className,
        )}
        {...props}
      />
    </TimelineStepsOrientationContext.Provider>
  );
}

/* -----------------------------------------------------------------------------
 * TimelineConnector (the line connecting items)
 * -------------------------------------------------------------------------- */

const timelineStepsConnectorVariants = cva("", {
  variants: {
    orientation: {
      vertical:
        "absolute left-[calc(var(--timeline-steps-icon-size,2.5rem)/2)] top-[var(--timeline-steps-icon-size,2.5rem)] h-[calc(100%-var(--timeline-steps-icon-size,2.5rem))] w-px -translate-x-1/2",
      horizontal:
        "absolute top-[calc(var(--timeline-steps-icon-size,2.5rem)/2)] left-[calc(50%+var(--timeline-steps-icon-size,2.5rem)/2)] h-px w-[calc(100%-var(--timeline-steps-icon-size,2.5rem))] -translate-y-1/2",
    },
    variant: {
      default: "bg-border",
      dashed: "border-border bg-transparent",
      dotted: "border-border bg-transparent",
    },
    status: {
      default: "",
      completed: "bg-primary",
      current: "",
      upcoming: "bg-muted",
      warning: "bg-warning",
    },
  },
  compoundVariants: [
    {
      orientation: "vertical",
      variant: "dashed",
      className: "border-l border-dashed",
    },
    {
      orientation: "vertical",
      variant: "dotted",
      className: "border-l border-dotted",
    },
    {
      orientation: "horizontal",
      variant: "dashed",
      className: "border-t border-dashed",
    },
    {
      orientation: "horizontal",
      variant: "dotted",
      className: "border-t border-dotted",
    },
    {
      orientation: "vertical",
      variant: "default",
      status: "current",
      className: "bg-gradient-to-b from-primary to-border",
    },
    {
      orientation: "horizontal",
      variant: "default",
      status: "current",
      className: "bg-gradient-to-r from-primary to-border",
    },
  ],
  defaultVariants: {
    orientation: "vertical",
    variant: "default",
    status: "default",
  },
});

interface TimelineStepsConnectorProps
  extends React.ComponentProps<"div">,
    VariantProps<typeof timelineStepsConnectorVariants> {}

function TimelineStepsConnector({
  className,
  orientation,
  variant,
  status,
  ...props
}: TimelineStepsConnectorProps) {
  const resolvedOrientation = useTimelineStepsOrientation(orientation);
  return (
    <div
      data-slot="timeline-steps-connector"
      data-orientation={resolvedOrientation}
      aria-hidden="true"
      className={cn(
        timelineStepsConnectorVariants({
          orientation: resolvedOrientation,
          variant,
          status,
        }),
        className,
      )}
      {...props}
    />
  );
}

/* -----------------------------------------------------------------------------
 * TimelineHeader (contains icon and title row)
 * -------------------------------------------------------------------------- */

const timelineStepsHeaderVariants = cva("flex", {
  variants: {
    orientation: {
      vertical: "items-center gap-3",
      horizontal: "flex-col items-center gap-2 text-center",
    },
  },
  defaultVariants: {
    orientation: "vertical",
  },
});

interface TimelineStepsHeaderProps
  extends React.ComponentProps<"div">,
    VariantProps<typeof timelineStepsHeaderVariants> {}

function TimelineStepsHeader({
  className,
  orientation,
  ...props
}: TimelineStepsHeaderProps) {
  const resolvedOrientation = useTimelineStepsOrientation(orientation);
  return (
    <div
      data-slot="timeline-steps-header"
      data-orientation={resolvedOrientation}
      className={cn(
        timelineStepsHeaderVariants({ orientation: resolvedOrientation }),
        className,
      )}
      {...props}
    />
  );
}

/* -----------------------------------------------------------------------------
 * TimelineStepsIcon (the dot/icon indicator)
 * -------------------------------------------------------------------------- */

const timelineStepsIconVariants = cva(
  "relative z-10 flex shrink-0 items-center justify-center rounded-full border bg-background [--timeline-steps-icon-size:2.5rem]",
  {
    variants: {
      size: {
        sm: "size-6 [--timeline-steps-icon-size:1.5rem] [&>svg]:size-3",
        default: "size-10 [--timeline-steps-icon-size:2.5rem] [&>svg]:size-4",
        lg: "size-12 [--timeline-steps-icon-size:3rem] [&>svg]:size-5",
      },
      variant: {
        default: "border-border text-muted-foreground",
        primary: "border-primary bg-primary text-primary-foreground",
        secondary: "border-secondary bg-secondary text-secondary-foreground",
        destructive:
          "border-destructive bg-destructive text-destructive-foreground",
        outline: "border-border bg-background text-foreground",
        warning: "border-warning bg-warning text-warning-foreground",
      },
    },
    defaultVariants: {
      size: "default",
      variant: "default",
    },
  },
);

interface TimelineStepsIconProps
  extends React.ComponentProps<"div">,
    VariantProps<typeof timelineStepsIconVariants> {}

function TimelineStepsIcon({
  className,
  size,
  variant,
  ...props
}: TimelineStepsIconProps) {
  const resolvedSize = size ?? "default";
  return (
    <div
      data-slot="timeline-steps-icon"
      data-size={resolvedSize}
      className={cn(
        timelineStepsIconVariants({ size: resolvedSize, variant }),
        className,
      )}
      {...props}
    />
  );
}

/* -----------------------------------------------------------------------------
 * TimelineStepsContent (container for description, time, etc.)
 * -------------------------------------------------------------------------- */

const timelineStepsContentVariants = cva("flex flex-col gap-1 pt-0.5 pb-2", {
  variants: {
    orientation: {
      vertical: "ms-[calc(var(--timeline-steps-icon-size,2.5rem)+0.75rem)]",
      horizontal: "mt-2 items-center text-center",
    },
  },
  defaultVariants: {
    orientation: "vertical",
  },
});

interface TimelineStepsContentProps
  extends React.ComponentProps<"div">,
    VariantProps<typeof timelineStepsContentVariants> {}

function TimelineStepsContent({
  className,
  orientation,
  ...props
}: TimelineStepsContentProps) {
  const resolvedOrientation = useTimelineStepsOrientation(orientation);
  return (
    <div
      data-slot="timeline-steps-content"
      data-orientation={resolvedOrientation}
      className={cn(
        timelineStepsContentVariants({ orientation: resolvedOrientation }),
        className,
      )}
      {...props}
    />
  );
}

/* -----------------------------------------------------------------------------
 * TimelineStepsTitle
 * -------------------------------------------------------------------------- */

function TimelineStepsTitle({
  className,
  ...props
}: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="timeline-steps-title"
      className={cn(
        "text-foreground leading-none font-medium tracking-tight",
        className,
      )}
      {...props}
    />
  );
}

/* -----------------------------------------------------------------------------
 * TimelineDescription
 * -------------------------------------------------------------------------- */

function TimelineStepsDescription({
  className,
  ...props
}: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="timeline-steps-description"
      className={cn("text-muted-foreground text-sm", className)}
      {...props}
    />
  );
}

/* -----------------------------------------------------------------------------
 * TimelineStepsTime (timestamp display)
 * -------------------------------------------------------------------------- */

function TimelineStepsTime({
  className,
  ...props
}: React.ComponentProps<"time">) {
  return (
    <time
      data-slot="timeline-steps-time"
      className={cn("text-muted-foreground text-xs", className)}
      {...props}
    />
  );
}

/* -----------------------------------------------------------------------------
 * Exports
 * -------------------------------------------------------------------------- */

export {
  TimelineSteps,
  TimelineStepsItem,
  TimelineStepsConnector,
  TimelineStepsHeader,
  TimelineStepsIcon,
  TimelineStepsContent,
  TimelineStepsTitle,
  TimelineStepsDescription,
  TimelineStepsTime,
  timelineStepsVariants,
  timelineStepsItemVariants,
  timelineStepsConnectorVariants,
  timelineStepsHeaderVariants,
  timelineStepsIconVariants,
  timelineStepsContentVariants,
};

export type {
  TimelineStepsStatus,
  TimelineStepsOrientation,
  TimelineStepsPosition,
};
