import * as React from "react";
import { cn } from "@/lib/cn";

/**
 * Adapted from Valon Registry `timeline-steps` for the Gestalt console token set.
 * @see toolshed/valon-tools/apps/registry/ui/src/ui/timeline-steps.tsx
 */

type TimelineStepsOrientation = "vertical" | "horizontal";
type TimelineStepsStatus = "default" | "completed" | "current" | "upcoming";

const TimelineStepsOrientationContext =
  React.createContext<TimelineStepsOrientation>("vertical");

function useTimelineStepsOrientation(
  orientation?: TimelineStepsOrientation | null,
) {
  const contextOrientation = React.useContext(TimelineStepsOrientationContext);
  return orientation ?? contextOrientation;
}

function TimelineSteps({
  className,
  orientation = "vertical",
  ...props
}: React.ComponentProps<"div"> & { orientation?: TimelineStepsOrientation }) {
  return (
    <TimelineStepsOrientationContext.Provider value={orientation}>
      <div
        data-slot="timeline-steps"
        data-orientation={orientation}
        className={cn(
          "flex [--timeline-steps-icon-size:2.5rem]",
          orientation === "vertical" ? "flex-col" : "flex-row overflow-x-auto",
          className,
        )}
        {...props}
      />
    </TimelineStepsOrientationContext.Provider>
  );
}

function TimelineStepsItem({
  className,
  orientation,
  status = "default",
  ...props
}: React.ComponentProps<"div"> & {
  orientation?: TimelineStepsOrientation;
  status?: TimelineStepsStatus;
}) {
  const resolvedOrientation = useTimelineStepsOrientation(orientation);
  return (
    <TimelineStepsOrientationContext.Provider value={resolvedOrientation}>
      <div
        data-slot="timeline-steps-item"
        data-orientation={resolvedOrientation}
        data-status={status}
        className={cn(
          "relative flex flex-col [--timeline-steps-icon-size:2.5rem]",
          resolvedOrientation === "vertical" ? "pb-8 last:pb-0" : "flex-1 items-center",
          status === "upcoming" && "opacity-60",
          className,
        )}
        {...props}
      />
    </TimelineStepsOrientationContext.Provider>
  );
}

function TimelineStepsConnector({
  className,
  orientation,
  status = "default",
  ...props
}: React.ComponentProps<"div"> & {
  orientation?: TimelineStepsOrientation;
  status?: TimelineStepsStatus;
}) {
  const resolvedOrientation = useTimelineStepsOrientation(orientation);
  return (
    <div
      data-slot="timeline-steps-connector"
      data-orientation={resolvedOrientation}
      aria-hidden="true"
      className={cn(
        resolvedOrientation === "vertical"
          ? "absolute left-[calc(var(--timeline-steps-icon-size,2.5rem)/2)] top-[var(--timeline-steps-icon-size,2.5rem)] h-[calc(100%-var(--timeline-steps-icon-size,2.5rem))] w-px -translate-x-1/2"
          : "absolute top-[calc(var(--timeline-steps-icon-size,2.5rem)/2)] left-[calc(50%+var(--timeline-steps-icon-size,2.5rem)/2)] h-px w-[calc(100%-var(--timeline-steps-icon-size,2.5rem))] -translate-y-1/2",
        status === "completed" && "bg-base-950 dark:bg-base-200",
        status === "current" &&
          (resolvedOrientation === "vertical"
            ? "bg-gradient-to-b from-base-950 to-base-300 dark:from-base-200 dark:to-base-700"
            : "bg-gradient-to-r from-base-950 to-base-300 dark:from-base-200 dark:to-base-700"),
        (status === "default" || status === "upcoming") &&
          "bg-base-300 dark:bg-base-700",
        className,
      )}
      {...props}
    />
  );
}

function TimelineStepsHeader({
  className,
  orientation,
  ...props
}: React.ComponentProps<"div"> & {
  orientation?: TimelineStepsOrientation;
}) {
  const resolvedOrientation = useTimelineStepsOrientation(orientation);
  return (
    <div
      data-slot="timeline-steps-header"
      data-orientation={resolvedOrientation}
      className={cn(
        "flex",
        resolvedOrientation === "vertical"
          ? "items-center gap-3"
          : "flex-col items-center gap-2 text-center",
        className,
      )}
      {...props}
    />
  );
}

function TimelineStepsIcon({
  className,
  size = "default",
  variant = "default",
  ...props
}: React.ComponentProps<"div"> & {
  size?: "sm" | "default" | "lg";
  variant?: "default" | "primary" | "outline";
}) {
  return (
    <div
      data-slot="timeline-steps-icon"
      data-size={size}
      className={cn(
        "relative z-10 flex shrink-0 items-center justify-center rounded-full border bg-background",
        size === "sm" &&
          "size-6 [--timeline-steps-icon-size:1.5rem] [&>svg]:size-3",
        size === "default" &&
          "size-10 [--timeline-steps-icon-size:2.5rem] [&>svg]:size-4",
        size === "lg" &&
          "size-12 [--timeline-steps-icon-size:3rem] [&>svg]:size-5",
        variant === "default" && "border-alpha text-muted",
        variant === "primary" &&
          "border-base-950 bg-base-950 text-white dark:border-base-100 dark:bg-base-100 dark:text-base-950",
        variant === "outline" && "border-alpha-strong bg-background text-primary",
        className,
      )}
      {...props}
    />
  );
}

function TimelineStepsContent({
  className,
  orientation,
  ...props
}: React.ComponentProps<"div"> & {
  orientation?: TimelineStepsOrientation;
}) {
  const resolvedOrientation = useTimelineStepsOrientation(orientation);
  return (
    <div
      data-slot="timeline-steps-content"
      data-orientation={resolvedOrientation}
      className={cn(
        "flex flex-col gap-1 pt-0.5 pb-2",
        resolvedOrientation === "vertical"
          ? "ms-[calc(var(--timeline-steps-icon-size,2.5rem)+0.75rem)]"
          : "mt-2 items-center text-center",
        className,
      )}
      {...props}
    />
  );
}

function TimelineStepsTitle({
  className,
  ...props
}: React.ComponentProps<"h2">) {
  return (
    <h2
      data-slot="timeline-steps-title"
      className={cn(
        "font-heading text-base leading-none tracking-tight text-primary",
        className,
      )}
      {...props}
    />
  );
}

function TimelineStepsDescription({
  className,
  ...props
}: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="timeline-steps-description"
      className={cn("text-sm text-muted", className)}
      {...props}
    />
  );
}

export {
  TimelineSteps,
  TimelineStepsItem,
  TimelineStepsConnector,
  TimelineStepsHeader,
  TimelineStepsIcon,
  TimelineStepsContent,
  TimelineStepsTitle,
  TimelineStepsDescription,
};

export type { TimelineStepsStatus };
