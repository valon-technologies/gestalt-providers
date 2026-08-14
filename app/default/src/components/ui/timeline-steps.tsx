/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";
import {
  StepRailIndicator,
  StepRailSeparator,
  stepRailCompletedChromeAccentClassName,
  stepRailRootClassName,
  stepRailSizeVar,
  stepRailTimelineRootClassName,
  type StepRailSize,
} from "@/components/ui/step-rail";
import {
  timelineConnectorLineState,
  timelineItemStatusToRailState,
} from "@/components/ui/timeline-steps-status";

/* -----------------------------------------------------------------------------
 * Timeline (root container)
 * -------------------------------------------------------------------------- */

type TimelineStepsSize = StepRailSize;
type TimelineStepsOrientation = "vertical" | "horizontal";

interface TimelineStepsContextValue {
  orientation: TimelineStepsOrientation;
  /** Resolved icon scale for this scope (item overrides affect children). */
  size: TimelineStepsSize;
  /** Fixed rail lane from the root — connector + header grid stay on this axis. */
  railLaneSize: TimelineStepsSize;
  glyph: "auto" | "none";
  registerItem: (id: string) => void;
  unregisterItem: (id: string) => void;
  getItemIndex: (id: string) => number;
}

interface TimelineStepsItemContextValue {
  status?: "default" | "completed" | "current" | "upcoming" | "warning" | null;
  index: number;
  size: TimelineStepsSize;
}

const TimelineStepsContext = React.createContext<TimelineStepsContextValue>({
  orientation: "vertical",
  size: "default",
  railLaneSize: "default",
  glyph: "auto",
  registerItem: () => {},
  unregisterItem: () => {},
  getItemIndex: () => 0,
});

const TimelineStepsItemContext = React.createContext<TimelineStepsItemContextValue>({
  status: "default",
  index: 0,
  size: "default",
});

function useTimelineStepsContext() {
  return React.useContext(TimelineStepsContext);
}

function useTimelineStepsItemContext() {
  return React.useContext(TimelineStepsItemContext);
}

function useTimelineStepsOrientation(orientation?: TimelineStepsOrientation | null) {
  const { orientation: contextOrientation } = useTimelineStepsContext();
  return orientation ?? contextOrientation;
}

function useTimelineStepsSize(size?: TimelineStepsSize | null) {
  const { size: contextSize } = useTimelineStepsContext();
  return size ?? contextSize;
}

function useTimelineStepsRailLaneSize() {
  const { railLaneSize } = useTimelineStepsContext();
  return railLaneSize;
}

/** Base text scale for a timeline size — children use `em` for proportional rhythm. */
function timelineStepsTextScale(size: TimelineStepsSize): string {
  switch (size) {
    case "xs":
      return "text-xs";
    case "sm":
      return "text-sm";
    case "lg":
      return "text-lg";
    default:
      return "text-base";
  }
}

const timelineStepsVariants = cva(
  [
    "flex",
    stepRailRootClassName,
    stepRailCompletedChromeAccentClassName,
    stepRailTimelineRootClassName,
    "[--step-rail-trigger-pad:0px]",
    "[--timeline-rail-lane-size:var(--step-rail-indicator-size)]",
  ].join(" "),
  {
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
    size: {
      xs: `${stepRailSizeVar("xs")} ${timelineStepsTextScale("xs")}`,
      sm: `${stepRailSizeVar("sm")} ${timelineStepsTextScale("sm")}`,
      default: `${stepRailSizeVar("default")} ${timelineStepsTextScale("default")}`,
      lg: `${stepRailSizeVar("lg")} ${timelineStepsTextScale("lg")}`,
    },
  },
  defaultVariants: {
    orientation: "vertical",
    position: "left",
    size: "default",
  },
});

interface TimelineStepsProps extends React.ComponentProps<"div">, VariantProps<typeof timelineStepsVariants> {
  /** `none` = dot-only indicators (no numerals / checks). Default `auto`. */
  glyph?: "auto" | "none";
}

function TimelineSteps({ className, orientation, position, size, glyph = "auto", ...props }: TimelineStepsProps) {
  const resolvedOrientation = orientation ?? "vertical";
  const resolvedSize = size ?? "default";
  const [order, setOrder] = React.useState<string[]>([]);

  const registerItem = React.useCallback((id: string) => {
    setOrder((prev) => (prev.includes(id) ? prev : [...prev, id]));
  }, []);

  const unregisterItem = React.useCallback((id: string) => {
    setOrder((prev) => prev.filter((itemId) => itemId !== id));
  }, []);

  const getItemIndex = React.useCallback(
    (id: string) => {
      const idx = order.indexOf(id);
      return idx === -1 ? 0 : idx;
    },
    [order],
  );

  const ctx = React.useMemo<TimelineStepsContextValue>(
    () => ({
      orientation: resolvedOrientation,
      size: resolvedSize,
      railLaneSize: resolvedSize,
      glyph,
      registerItem,
      unregisterItem,
      getItemIndex,
    }),
    [resolvedOrientation, resolvedSize, glyph, registerItem, unregisterItem, getItemIndex],
  );

  return (
    <TimelineStepsContext.Provider value={ctx}>
      <div
        data-slot="timeline-steps"
        data-orientation={resolvedOrientation}
        data-position={position}
        data-size={resolvedSize}
        className={cn(
          timelineStepsVariants({ orientation: resolvedOrientation, position, size: resolvedSize }),
          className,
        )}
        {...props}
      />
    </TimelineStepsContext.Provider>
  );
}

/* -----------------------------------------------------------------------------
 * TimelineItem
 * -------------------------------------------------------------------------- */

const timelineStepsItemVariants = cva(
  "relative flex flex-col has-[[data-size=xs]]:[--step-rail-indicator-size:0.625rem] has-[[data-size=sm]]:[--step-rail-indicator-size:1.5rem] has-[[data-size=lg]]:[--step-rail-indicator-size:2.5rem]",
  {
    variants: {
      orientation: {
        vertical: "pb-8 last:pb-0 has-[[data-size=xs]]:pb-5",
        horizontal: "flex-1 items-center",
      },
      status: {
        default: "",
        completed: "",
        current: "",
        upcoming: "",
        warning: "",
      },
      size: {
        xs: `${stepRailSizeVar("xs")} ${timelineStepsTextScale("xs")} [&_[data-slot=timeline-steps-header]]:gap-[0.5em] [&_[data-slot=timeline-steps-content]]:ms-[calc(var(--timeline-rail-lane-size)+0.5em)]`,
        sm: `${stepRailSizeVar("sm")} ${timelineStepsTextScale("sm")}`,
        default: `${stepRailSizeVar("default")} ${timelineStepsTextScale("default")}`,
        lg: `${stepRailSizeVar("lg")} ${timelineStepsTextScale("lg")}`,
      },
    },
    defaultVariants: {
      orientation: "vertical",
      status: "default",
      size: "default",
    },
  },
);

interface TimelineStepsItemProps extends React.ComponentProps<"div">, VariantProps<typeof timelineStepsItemVariants> {
  /** Override auto-assigned step index (0-based). */
  index?: number;
}

function TimelineStepsItem({ className, orientation, status, size, index: indexProp, ...props }: TimelineStepsItemProps) {
  const parent = useTimelineStepsContext();
  const { registerItem, unregisterItem, railLaneSize, glyph, getItemIndex } = parent;
  const itemId = React.useId();
  const resolvedOrientation = orientation ?? parent.orientation;
  const resolvedSize = size ?? parent.size;

  React.useLayoutEffect(() => {
    registerItem(itemId);
    return () => unregisterItem(itemId);
  }, [itemId, registerItem, unregisterItem]);

  const resolvedIndex = indexProp ?? getItemIndex(itemId);
  const itemCtx = React.useMemo(
    () => ({ status, index: resolvedIndex, size: resolvedSize }),
    [status, resolvedIndex, resolvedSize],
  );
  return (
    <TimelineStepsContext.Provider
      value={{
        orientation: resolvedOrientation,
        size: resolvedSize,
        railLaneSize,
        glyph,
        registerItem,
        unregisterItem,
        getItemIndex,
      }}
    >
      <TimelineStepsItemContext.Provider value={itemCtx}>
        <div
          data-slot="timeline-steps-item"
          data-orientation={resolvedOrientation}
          data-status={status}
          data-size={resolvedSize}
          className={cn(
            timelineStepsItemVariants({ orientation: resolvedOrientation, status, size: resolvedSize }),
            className,
          )}
          {...props}
        />
      </TimelineStepsItemContext.Provider>
    </TimelineStepsContext.Provider>
  );
}

/* -----------------------------------------------------------------------------
 * TimelineConnector (shared Stepper rail)
 * -------------------------------------------------------------------------- */

interface TimelineStepsConnectorProps extends React.ComponentProps<"div"> {
  orientation?: TimelineStepsOrientation;
  /** Override auto-derived line state from item status. */
  lineState?: "completed" | "pending";
}

function TimelineStepsConnector({ className, orientation, lineState, ...props }: TimelineStepsConnectorProps) {
  const { status } = useTimelineStepsItemContext();
  const railLaneSize = useTimelineStepsRailLaneSize();
  const resolvedOrientation = useTimelineStepsOrientation(orientation);
  const resolvedLineState = lineState ?? timelineConnectorLineState(status);
  return (
    <StepRailSeparator
      data-slot="timeline-steps-connector"
      orientation={resolvedOrientation}
      size={railLaneSize}
      lineState={resolvedLineState}
      layout="timeline"
      className={cn(
        resolvedOrientation === "vertical" &&
          "left-[calc(var(--timeline-rail-lane-size,2rem)/2)] top-[var(--step-rail-indicator-size,2rem)] h-[calc(100%-var(--step-rail-indicator-size,2rem))] w-[1.5px] -translate-x-1/2",
        resolvedOrientation === "horizontal" &&
          "top-[calc(var(--timeline-rail-lane-size,2rem)/2)] left-[calc(50%+var(--timeline-rail-lane-size,2rem)/2)] h-[1.5px] w-[calc(100%-var(--timeline-rail-lane-size,2rem))] -translate-y-1/2",
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
      vertical:
        "grid grid-cols-[var(--timeline-rail-lane-size,2rem)_minmax(0,1fr)] items-center gap-x-[0.75em]",
      horizontal: "flex-col items-center gap-[0.5em] text-center",
    },
  },
  defaultVariants: {
    orientation: "vertical",
  },
});

interface TimelineStepsHeaderProps
  extends React.ComponentProps<"div">,
    VariantProps<typeof timelineStepsHeaderVariants> {}

function TimelineStepsHeader({ className, orientation, ...props }: TimelineStepsHeaderProps) {
  const resolvedOrientation = useTimelineStepsOrientation(orientation);
  return (
    <div
      data-slot="timeline-steps-header"
      data-orientation={resolvedOrientation}
      className={cn(timelineStepsHeaderVariants({ orientation: resolvedOrientation }), className)}
      {...props}
    />
  );
}

/* -----------------------------------------------------------------------------
 * TimelineStepsIcon (Stepper-matched indicator)
 * -------------------------------------------------------------------------- */

interface TimelineStepsIconProps extends React.ComponentProps<typeof StepRailIndicator> {
  size?: TimelineStepsSize;
  glyph?: "auto" | "none";
}

function TimelineStepsIcon({ className, size, glyph, state, children, ...props }: TimelineStepsIconProps) {
  const { status, index } = useTimelineStepsItemContext();
  const { glyph: contextGlyph, orientation } = useTimelineStepsContext();
  const resolvedSize = useTimelineStepsSize(size);
  const resolvedGlyph = glyph ?? contextGlyph;
  const resolvedState = state ?? timelineItemStatusToRailState(status);
  return (
    <StepRailIndicator
      data-slot="timeline-steps-icon"
      size={resolvedSize}
      state={resolvedState}
      index={index}
      glyph={resolvedGlyph}
      chromeDelayMs={null}
      className={cn(orientation === "vertical" && "col-start-1 justify-self-center shrink-0", className)}
      {...props}
    >
      {children}
    </StepRailIndicator>
  );
}

/* -----------------------------------------------------------------------------
 * TimelineStepsContent (container for description, time, etc.)
 * -------------------------------------------------------------------------- */

const timelineStepsContentVariants = cva("flex flex-col gap-[0.25em] pb-[0.5em]", {
  variants: {
    orientation: {
      vertical:
        "ms-[calc(var(--timeline-rail-lane-size,2rem)+0.75em)] -mt-[calc((var(--step-rail-indicator-size,2rem)-1em)/2-0.25em)] pt-0",
      horizontal: "mt-[0.375em] items-center text-center",
    },
  },
  defaultVariants: {
    orientation: "vertical",
  },
});

interface TimelineStepsContentProps
  extends React.ComponentProps<"div">,
    VariantProps<typeof timelineStepsContentVariants> {}

function TimelineStepsContent({ className, orientation, ...props }: TimelineStepsContentProps) {
  const resolvedOrientation = useTimelineStepsOrientation(orientation);
  return (
    <div
      data-slot="timeline-steps-content"
      data-orientation={resolvedOrientation}
      className={cn(timelineStepsContentVariants({ orientation: resolvedOrientation }), className)}
      {...props}
    />
  );
}

/* -----------------------------------------------------------------------------
 * TimelineStepsTitle
 * -------------------------------------------------------------------------- */

function TimelineStepsTitle({ className, ...props }: React.ComponentProps<"div">) {
  const { status } = useTimelineStepsItemContext();
  const { orientation } = useTimelineStepsContext();
  return (
    <div
      data-slot="timeline-steps-title"
      className={cn(
        "text-pretty text-[1em] leading-none font-medium tracking-tight",
        orientation === "vertical" && "col-start-2 min-w-0",
        status === "upcoming" || status === "default" ? "text-muted-foreground" : "text-foreground",
        className,
      )}
      {...props}
    />
  );
}

/* -----------------------------------------------------------------------------
 * TimelineDescription
 * -------------------------------------------------------------------------- */

function TimelineStepsDescription({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="timeline-steps-description"
      className={cn("text-muted-foreground text-pretty text-[0.875em] leading-snug", className)}
      {...props}
    />
  );
}

/* -----------------------------------------------------------------------------
 * TimelineStepsTime (timestamp display)
 * -------------------------------------------------------------------------- */

function TimelineStepsTime({ className, ...props }: React.ComponentProps<"time">) {
  return (
    <time
      data-slot="timeline-steps-time"
      className={cn("text-muted-foreground text-[0.75em]", className)}
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
  timelineStepsHeaderVariants,
  timelineStepsContentVariants,
};
