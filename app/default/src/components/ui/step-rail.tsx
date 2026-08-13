/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";
import { SelectionCheck } from "@/components/ui/selection-check";

/** Shared visual states for step rails (Stepper + TimelineSteps). */
export type StepRailVisualState = "completed" | "active" | "pending";
export type StepRailSize = "xs" | "sm" | "default" | "lg";
export type StepRailOrientation = "horizontal" | "vertical";

/** Root tokens for rail paint + animation timing. */
export const stepRailRootClassName = [
  "[--step-rail-pending:var(--border)]",
  "[--step-rail-duration:var(--duration-250)]",
  "[--step-rail-chrome-settle:var(--duration-75)]",
].join(" ");

export function stepRailSizeVar(size: StepRailSize): string {
  if (size === "xs") return "[--step-rail-indicator-size:0.625rem]";
  if (size === "sm") return "[--step-rail-indicator-size:1.5rem]";
  if (size === "lg") return "[--step-rail-indicator-size:2.5rem]";
  return "[--step-rail-indicator-size:2rem]";
}

export function readStepRailChromeDelayMs(node: HTMLElement | null): number {
  if (!node) return 0;
  const style = getComputedStyle(node);
  const read = (name: string) => {
    const v = style.getPropertyValue(name).trim();
    if (!v) return 0;
    if (v.endsWith("ms")) return Number.parseFloat(v) || 0;
    if (v.endsWith("s")) return (Number.parseFloat(v) || 0) * 1000;
    return Number.parseFloat(v) || 0;
  };
  return read("--step-rail-duration") + read("--step-rail-chrome-settle");
}

export function stepRailLineState(
  orientation: StepRailOrientation,
  index: number,
  activeIndex: number,
): "completed" | "pending" {
  if (orientation === "horizontal") {
    return activeIndex >= index ? "completed" : "pending";
  }
  return activeIndex > index ? "completed" : "pending";
}

/* -----------------------------------------------------------------------------
 * Indicator
 * -------------------------------------------------------------------------- */

export const stepRailIndicatorVariants = cva(
  "relative z-10 flex shrink-0 items-center justify-center rounded-full border font-display text-base font-normal italic leading-none transition-[color,background-color,border-color] duration-hover-out ease-out-quart",
  {
    variants: {
      size: {
        xs: "size-2.5 [--step-rail-indicator-size:0.625rem]",
        sm: "size-6 text-sm [--step-rail-indicator-size:1.5rem] [&>svg]:size-3",
        default: "size-8 text-base [--step-rail-indicator-size:2rem] [&>svg]:size-3.5",
        lg: "size-10 text-lg [--step-rail-indicator-size:2.5rem] [&>svg]:size-4",
      },
      state: {
        active: "border-primary bg-primary text-primary-foreground",
        completed: "border-accent-solid bg-accent-solid text-accent-foreground",
        pending: "border-[1.5px] border-[color:var(--step-rail-pending)] bg-background text-muted-foreground",
        success:
          "border-success-solid bg-success-solid text-success-solid-foreground",
        warning: "border-warning bg-warning text-warning-foreground",
      },
    },
    defaultVariants: {
      size: "default",
      state: "pending",
    },
  },
);

export interface StepRailIndicatorProps
  extends React.ComponentProps<"span">,
    VariantProps<typeof stepRailIndicatorVariants> {
  /** Step index for default numeral glyph (0-based). */
  index?: number;
  /** `none` = dot only (no numeral / check). Default `auto`. */
  glyph?: "auto" | "none";
  /** When set, forward selection lags until the incoming rail arrives. */
  chromeDelayMs?: number | null;
  /** Called when animation runs (Stepper). Prefer over a static `chromeDelayMs`. */
  getChromeDelayMs?: (() => number) | null;
  children?: React.ReactNode;
}

export function StepRailIndicator({
  className,
  size = "default",
  state = "pending",
  index = 0,
  glyph = "auto",
  chromeDelayMs = null,
  getChromeDelayMs = null,
  children,
  ...props
}: StepRailIndicatorProps) {
  const resolvedSize = size ?? "default";
  const dataState = state ?? "pending";
  const isCompleted = dataState === "completed";
  const checkSvg =
    resolvedSize === "xs"
      ? "size-2"
      : resolvedSize === "sm"
        ? "size-3"
        : resolvedSize === "lg"
          ? "size-4"
          : "size-3.5";
  const showGlyph = glyph !== "none";
  const animate = getChromeDelayMs != null || chromeDelayMs != null;
  const resolveDelay = React.useCallback(() => {
    if (getChromeDelayMs) return getChromeDelayMs();
    return chromeDelayMs ?? 0;
  }, [chromeDelayMs, getChromeDelayMs]);

  type IndicatorPaintState = StepRailVisualState | "success" | "warning";
  const prevDataState = React.useRef<IndicatorPaintState | null>(null);
  const [paintState, setPaintState] = React.useState<IndicatorPaintState>(dataState);
  const wasCompleted = React.useRef<boolean | null>(null);
  const [checkVisible, setCheckVisible] = React.useState(isCompleted);
  const [numberVisible, setNumberVisible] = React.useState(!isCompleted);
  const [checkEpoch, setCheckEpoch] = React.useState(0);

  React.useEffect(() => {
    if (!animate) {
      setPaintState(dataState);
      return;
    }
    const prev = prevDataState.current;
    if (prev === null) {
      prevDataState.current = dataState;
      setPaintState(dataState);
      return;
    }
    if (dataState === "active" && prev !== "active" && prev !== "completed") {
      prevDataState.current = dataState;
      setPaintState("pending");
      const delay = resolveDelay();
      if (delay <= 0) {
        setPaintState("active");
        return;
      }
      const id = window.setTimeout(() => setPaintState("active"), delay);
      return () => window.clearTimeout(id);
    }
    prevDataState.current = dataState;
    setPaintState(dataState);
  }, [animate, dataState, resolveDelay]);

  React.useEffect(() => {
    if (!animate) {
      setCheckVisible(isCompleted);
      setNumberVisible(!isCompleted);
      return;
    }
    if (wasCompleted.current === null) {
      wasCompleted.current = isCompleted;
      setCheckVisible(isCompleted);
      setNumberVisible(!isCompleted);
      return;
    }
    if (isCompleted && !wasCompleted.current) {
      wasCompleted.current = true;
      const delay = resolveDelay();
      if (delay <= 0) {
        setCheckVisible(true);
        setNumberVisible(false);
        return;
      }
      const id = window.setTimeout(() => {
        setCheckVisible(true);
        setNumberVisible(false);
      }, delay);
      return () => window.clearTimeout(id);
    }
    if (!isCompleted && wasCompleted.current) {
      wasCompleted.current = false;
      setCheckVisible(false);
      setNumberVisible(true);
      setCheckEpoch((epoch) => epoch + 1);
    }
  }, [animate, isCompleted, resolveDelay]);

  const visualState = animate ? paintState : dataState;
  const showCheck = animate ? checkVisible : isCompleted;
  const showNumber = animate ? numberVisible : !isCompleted;

  return (
    <span
      data-slot="step-rail-indicator"
      data-state={dataState}
      data-paint-state={visualState}
      data-size={resolvedSize}
      className={cn(stepRailIndicatorVariants({ size: resolvedSize, state: visualState }), className)}
      {...props}
    >
      {children ?? (showGlyph ? (
        <>
          <span className={cn("tabular-nums", !showNumber && "invisible")} aria-hidden={!showNumber}>
            {index + 1}
          </span>
          <SelectionCheck
            key={checkEpoch}
            checked={showCheck}
            density="condensed"
            tone="current"
            svgClassName={checkSvg}
            className="pointer-events-none absolute inset-0 m-auto"
          />
        </>
      ) : null)}
    </span>
  );
}

/* -----------------------------------------------------------------------------
 * Separator
 * -------------------------------------------------------------------------- */

export const stepRailSeparatorTrackVariants = cva(
  "pointer-events-none absolute z-[1] overflow-hidden bg-[var(--step-rail-pending)]",
  {
    variants: {
      orientation: {
        horizontal:
          "left-[calc(-50%+var(--step-rail-indicator-size,2rem)*0.5)] top-[calc(var(--step-rail-trigger-pad,0px)+var(--step-rail-indicator-size,2rem)*0.5)] h-[1.5px] w-[calc(100%-var(--step-rail-indicator-size,2rem))] -translate-y-1/2",
        vertical:
          "left-[calc(var(--step-rail-trigger-pad,0px)+var(--step-rail-indicator-size,2rem)*0.5)] top-[calc(var(--step-rail-trigger-pad,0px)+var(--step-rail-indicator-size,2rem))] h-[calc(100%-var(--step-rail-indicator-size,2rem)-var(--step-rail-trigger-pad,0px))] w-[1.5px] -translate-x-1/2",
      },
      layout: {
        stepper: "group-first/step:hidden",
        timeline: "",
        "stepper-vertical": "group-last/step:hidden",
      },
    },
    defaultVariants: {
      orientation: "vertical",
      layout: "timeline",
    },
  },
);

export const stepRailSeparatorFillVariants = cva(
  [
    "size-full bg-accent-solid transition-[clip-path] ease-out-quart",
    "duration-[var(--step-rail-duration,var(--motion-move))]",
  ].join(" "),
  {
    variants: {
      orientation: {
        horizontal: "[clip-path:inset(0_100%_0_0)] data-[state=completed]:[clip-path:inset(0_0_0_0)]",
        vertical: "[clip-path:inset(100%_0_0_0)] data-[state=completed]:[clip-path:inset(0_0_0_0)]",
      },
    },
    defaultVariants: {
      orientation: "vertical",
    },
  },
);

export interface StepRailSeparatorProps extends React.ComponentProps<"div"> {
  orientation?: StepRailOrientation;
  size?: StepRailSize;
  lineState?: "completed" | "pending";
  layout?: "stepper" | "timeline" | "stepper-vertical";
}

export function StepRailSeparator({
  className,
  orientation = "vertical",
  size = "default",
  lineState = "pending",
  layout = "timeline",
  ...props
}: StepRailSeparatorProps) {
  return (
    <div
      data-slot="step-rail-separator"
      data-orientation={orientation}
      data-state={lineState}
      aria-hidden="true"
      className={cn(
        layout !== "timeline" && stepRailSizeVar(size),
        stepRailSeparatorTrackVariants({ orientation, layout }),
        className,
      )}
      {...props}
    >
      <div
        data-slot="step-rail-separator-fill"
        data-state={lineState}
        className={stepRailSeparatorFillVariants({ orientation })}
      />
    </div>
  );
}

export function timelineItemStatusToRailState(
  status?: "default" | "completed" | "current" | "upcoming" | "warning" | null,
): StepRailVisualState | "warning" {
  switch (status) {
    case "completed":
      return "completed";
    case "current":
      return "active";
    case "warning":
      return "warning";
    case "upcoming":
    default:
      return "pending";
  }
}

export function timelineConnectorLineState(
  status?: "default" | "completed" | "current" | "upcoming" | "warning" | null,
): "completed" | "pending" {
  return status === "completed" ? "completed" : "pending";
}
