/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 * Registry `status-indicator` with `pulse` opt-out (toolshed#4181). Palette
 * literals map onto `--status-indicator-*`.
 */

import * as React from "react";

import { cn } from "@/lib/cn";

type StatusIndicatorState = "active" | "down" | "fixing" | "idle";
type StatusIndicatorSize = "sm" | "md" | "lg";

export interface StatusIndicatorProps
  extends Omit<React.HTMLAttributes<HTMLDivElement>, "children"> {
  state?: StatusIndicatorState;
  label?: string;
  className?: string;
  size?: StatusIndicatorSize;
  labelClassName?: string;
  /**
   * When true (default), `active` / `down` / `fixing` show the ping pulse.
   * Set false for static dots (e.g. definition-list status rows).
   */
  pulse?: boolean;
}

const STATE_COLOR: Record<StatusIndicatorState, string> = {
  active: "bg-status-indicator-success",
  down: "bg-status-indicator-danger",
  fixing: "bg-status-indicator-warning",
  idle: "bg-status-indicator-muted",
};

const SIZE_CLASS: Record<StatusIndicatorSize, string> = {
  sm: "size-2",
  md: "size-3",
  lg: "size-4",
};

const ANIMATED_STATES: readonly StatusIndicatorState[] = [
  "active",
  "down",
  "fixing",
];

function StatusIndicator({
  state = "idle",
  label,
  className,
  size = "md",
  labelClassName,
  pulse = true,
  ...props
}: StatusIndicatorProps) {
  const color = STATE_COLOR[state];
  const sizeClass = SIZE_CLASS[size];
  const shouldAnimate = pulse && ANIMATED_STATES.includes(state);

  return (
    <div
      data-slot="status-indicator"
      data-state={state}
      data-pulse={shouldAnimate ? "true" : "false"}
      className={cn("flex items-center gap-2", className)}
      {...props}
    >
      <div className="relative flex items-center">
        {shouldAnimate && (
          <span
            aria-hidden
            className={cn(
              "pointer-events-none absolute inline-flex rounded-full opacity-75 animate-ping motion-reduce:animate-none",
              sizeClass,
              color,
            )}
          />
        )}
        <span
          className={cn("relative inline-flex rounded-full", sizeClass, color)}
        />
      </div>
      {label && (
        <p className={cn("text-muted-foreground text-sm", labelClassName)}>
          {label}
        </p>
      )}
    </div>
  );
}

export { StatusIndicator };
export type { StatusIndicatorState, StatusIndicatorSize };
