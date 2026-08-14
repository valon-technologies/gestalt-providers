/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";
import { SelectionCheck } from "@/components/ui/selection-check";

/** Shared visual states for step rails (Stepper + TimelineSteps). */
export type StepRailVisualState = "completed" | "active" | "pending";

/**
 * Semantic rail paint states used by TimelineSteps (not Stepper navigation).
 * Named by status intent (`error`), matching Meter / Alert / Badge — not action `destructive`.
 */
export type StepRailSemanticState = "success" | "warning" | "error";
export type StepRailSize = "xs" | "sm" | "default" | "lg";
export type StepRailOrientation = "horizontal" | "vertical";
/** Indicator inner chrome: numerals/checks (`auto`) vs dot-only display rail (`none`). */
export type StepRailGlyphMode = "auto" | "none";

/** `xs` indicators are dot-only — too small for numerals, checks, or custom glyph chrome. */
export function stepRailResolvedGlyph(size: StepRailSize, glyph: StepRailGlyphMode = "auto"): StepRailGlyphMode {
  return size === "xs" ? "none" : glyph;
}

/**
 * How progress-completed chrome paints. Not TimelineSteps semantic `success`
 * (filled `success-solid` disc) and not a nested OutcomeStatusIndicator.
 * Stepper defaults to `outcome` (OSI success disc + ink connectors);
 * `accent` is the gold opt-in. Rail root tokens stay accent so TimelineSteps
 * completed stays gold.
 */
export type StepRailCompletedChrome = "accent" | "outcome";

/** Gold completed chrome — TimelineSteps default and Stepper `accent` opt-in. */
export const stepRailCompletedChromeAccentClassName = [
  "[--step-rail-completed-border:var(--accent-solid)]",
  "[--step-rail-completed-fill:var(--accent-solid)]",
  "[--step-rail-completed-fg:var(--accent-foreground)]",
  "[--step-rail-completed-line:var(--accent-solid)]",
].join(" ");

/**
 * Remap completed chrome to OutcomeStatusIndicator success paint + ink edge.
 * Port Registry `--color-green-500` onto live `--status-indicator-success`
 * (`@theme inline reference` does not emit `--color-*`).
 */
export const stepRailCompletedChromeOutcomeClassName = [
  "[--step-rail-completed-border:var(--status-indicator-success)]",
  "[--step-rail-completed-fill:var(--status-indicator-success)]",
  "[--step-rail-completed-fg:white]",
  "[--step-rail-completed-line:var(--primary)]",
].join(" ");

/** Root tokens for pending paint + animation timing. Completed chrome is applied separately. */
export const stepRailRootClassName = [
  "[--step-rail-pending:var(--border)]",
  "[--step-rail-duration:var(--duration-250)]",
  "[--step-rail-chrome-settle:var(--duration-75)]",
].join(" ");

/**
 * Timeline connector tokens — ink-alpha on `--foreground` (warm undertone in light/dark).
 * Track = `--step-rail-pending` (same as uncompleted stepper ring); fill = secondary ink.
 */
export const stepRailTimelineRootClassName = [
  "[--step-rail-timeline-track:var(--step-rail-pending)]",
  "[--step-rail-timeline-fill:var(--muted-foreground)]",
].join(" ");

export function stepRailSizeVar(size: StepRailSize): string {
  if (size === "xs") return "[--step-rail-indicator-size:0.625rem]";
  if (size === "sm") return "[--step-rail-indicator-size:1.5rem]";
  if (size === "lg") return "[--step-rail-indicator-size:2.5rem]";
  return "[--step-rail-indicator-size:2rem]";
}

/** Rail lane width for dot-only timelines — scales with `size` but stays smaller than glyph circles. */
export function stepRailDotLaneVar(size: StepRailSize): string {
  if (size === "xs") return "[--step-rail-indicator-size:0.625rem]";
  if (size === "sm") return "[--step-rail-indicator-size:0.875rem]";
  if (size === "lg") return "[--step-rail-indicator-size:1.25rem]";
  return "[--step-rail-indicator-size:1rem]";
}

export function stepRailIndicatorLaneVar(size: StepRailSize, glyph: StepRailGlyphMode = "auto"): string {
  const resolvedGlyph = stepRailResolvedGlyph(size, glyph);
  return resolvedGlyph === "none" ? stepRailDotLaneVar(size) : stepRailSizeVar(size);
}

export function readStepRailTimingMs(node: HTMLElement | null): {
  durationMs: number;
  settleMs: number;
} {
  if (!node) return { durationMs: 0, settleMs: 0 };
  const style = getComputedStyle(node);
  const read = (name: string) => {
    const v = style.getPropertyValue(name).trim();
    if (!v) return 0;
    if (v.endsWith("ms")) return Number.parseFloat(v) || 0;
    if (v.endsWith("s")) return (Number.parseFloat(v) || 0) * 1000;
    return Number.parseFloat(v) || 0;
  };
  return {
    durationMs: read("--step-rail-duration"),
    settleMs: read("--step-rail-chrome-settle"),
  };
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

/**
 * How many rail-duration tokens to wait before this segment starts filling
 * or retracting, so a skip-ahead jump grows as one tail (1→2→3) instead of
 * every segment transitioning at once.
 */
export function stepRailLineStaggerSteps(
  orientation: StepRailOrientation,
  index: number,
  fromActive: number,
  toActive: number,
): number {
  if (fromActive < 0 || toActive < 0 || fromActive === toActive) return 0;
  const fromState = stepRailLineState(orientation, index, fromActive);
  const toState = stepRailLineState(orientation, index, toActive);
  if (fromState === toState) return 0;

  if (toState === "completed") {
    return orientation === "horizontal"
      ? Math.max(0, index - fromActive - 1)
      : Math.max(0, index - fromActive);
  }

  return orientation === "horizontal"
    ? Math.max(0, fromActive - index)
    : Math.max(0, fromActive - 1 - index);
}

/** Stagger of the connector that arrives at this indicator, in rail-duration steps. */
export function stepRailIndicatorArriveStaggerSteps(
  orientation: StepRailOrientation,
  index: number,
  fromActive: number,
  toActive: number,
  nextState: StepRailVisualState,
): number {
  if (nextState === "completed") {
    const sepIndex = orientation === "horizontal" ? index + 1 : index;
    return stepRailLineStaggerSteps(orientation, sepIndex, fromActive, toActive);
  }
  if (nextState === "active") {
    const sepIndex = orientation === "horizontal" ? index : index - 1;
    if (sepIndex < 0) return 0;
    return stepRailLineStaggerSteps(orientation, sepIndex, fromActive, toActive);
  }
  return 0;
}

/** Origin + destination of the current rail motion. Origin stays until the next jump. */
export type StepRailTransition = {
  origin: number | null;
  current: number | null;
};

/**
 * Latch the jump origin when `activeIndex` changes. Later re-renders of the same
 * destination must not collapse origin to current (that would zero stagger CSS).
 */
export function stepRailAdvanceTransition(
  activeIndex: number,
  prev: StepRailTransition,
): StepRailTransition {
  if (activeIndex < 0) return prev;
  if (prev.current === null) return { origin: null, current: activeIndex };
  if (activeIndex === prev.current) return prev;
  return { origin: prev.current, current: activeIndex };
}

export function stepRailFromActiveIndex(transition: StepRailTransition): number {
  return transition.origin ?? transition.current ?? -1;
}

/* -----------------------------------------------------------------------------
 * Indicator
 * -------------------------------------------------------------------------- */

export const stepRailIndicatorVariants = cva(
  "relative z-10 flex shrink-0 items-center justify-center rounded-full border leading-none transition-[color,background-color,border-color] duration-hover-out ease-out-quart",
  {
    variants: {
      size: {
        xs: "",
        sm: "",
        default: "",
        lg: "",
      },
      glyph: {
        auto: "font-display text-base font-normal italic",
        none: "",
      },
      state: {
        active: "border-primary bg-primary text-primary-foreground",
        completed:
          "border-[1.5px] border-[color:var(--step-rail-completed-border)] bg-[var(--step-rail-completed-fill)] text-[color:var(--step-rail-completed-fg)]",
        pending: "border-[1.5px] border-[color:var(--step-rail-pending)] bg-background text-muted-foreground",
        success: "",
        warning: "",
        error: "",
      },
    },
    compoundVariants: [
      // `stepRailResolvedGlyph` always coerces xs → glyph none before CVA runs, so the
      // live xs paint path is `none`/`xs`. Cap custom SVG children there (not on dead
      // `auto`/`xs`). Keep a matching auto/xs size shell only as a defensive bypass.
      {
        glyph: "auto",
        size: "xs",
        class: "size-2.5 text-base [--step-rail-indicator-size:0.625rem] [&>svg]:size-2",
      },
      {
        glyph: "auto",
        size: "sm",
        class: "size-6 text-sm [--step-rail-indicator-size:1.5rem] [&>svg]:size-3",
      },
      {
        glyph: "auto",
        size: "default",
        class: "size-8 text-base [--step-rail-indicator-size:2rem] [&>svg]:size-3.5",
      },
      {
        glyph: "auto",
        size: "lg",
        class: "size-10 text-lg [--step-rail-indicator-size:2.5rem] [&>svg]:size-4",
      },
      {
        glyph: "none",
        size: "xs",
        class: "size-2.5 [--step-rail-indicator-size:0.625rem] [&>svg]:size-2",
      },
      {
        glyph: "none",
        size: "sm",
        class: "size-3.5 [--step-rail-indicator-size:0.875rem]",
      },
      {
        glyph: "none",
        size: "default",
        class: "size-4 [--step-rail-indicator-size:1rem]",
      },
      {
        glyph: "none",
        size: "lg",
        class: "size-5 [--step-rail-indicator-size:1.25rem]",
      },
      {
        state: "success",
        class:
          "border-success-solid bg-success-solid text-success-solid-foreground [&>svg]:stroke-[2.5]",
      },
      {
        state: "warning",
        class:
          "border-warning-solid bg-warning-solid text-warning-solid-foreground [&>svg]:stroke-[2.5]",
      },
      {
        state: "error",
        class:
          "border-error-solid bg-error-solid text-error-solid-foreground [&>svg]:stroke-[2.5]",
      },
    ],
    defaultVariants: {
      size: "default",
      glyph: "auto",
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
  glyph?: StepRailGlyphMode;
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
  const resolvedGlyph = stepRailResolvedGlyph(resolvedSize, glyph ?? "auto");
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
  const showGlyph = resolvedGlyph !== "none";
  const animate = getChromeDelayMs != null || chromeDelayMs != null;
  const resolveDelay = React.useCallback(() => {
    if (getChromeDelayMs) return getChromeDelayMs();
    return chromeDelayMs ?? 0;
  }, [chromeDelayMs, getChromeDelayMs]);
  // Sample delay at the state transition. Callback identity is not a cancel signal.
  const resolveDelayRef = React.useRef(resolveDelay);
  resolveDelayRef.current = resolveDelay;

  type IndicatorPaintState = StepRailVisualState | StepRailSemanticState;
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
      const delay = resolveDelayRef.current();
      if (delay <= 0) {
        setPaintState("active");
        return;
      }
      const id = window.setTimeout(() => setPaintState("active"), delay);
      return () => window.clearTimeout(id);
    }
    prevDataState.current = dataState;
    setPaintState(dataState);
  }, [animate, dataState]);

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
      const delay = resolveDelayRef.current();
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
  }, [animate, isCompleted]);

  const visualState = animate ? paintState : dataState;
  const showCheck = animate ? checkVisible : isCompleted;
  const showNumber = animate ? numberVisible : !isCompleted;

  return (
    <span
      data-slot="step-rail-indicator"
      data-state={dataState}
      data-paint-state={visualState}
      data-size={resolvedSize}
      data-glyph={resolvedGlyph}
      className={cn(
        stepRailIndicatorVariants({ size: resolvedSize, glyph: resolvedGlyph, state: visualState }),
        className,
      )}
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
  "pointer-events-none absolute z-[1] overflow-hidden",
  {
    variants: {
      orientation: {
        horizontal:
          "left-[calc(-50%+var(--step-rail-indicator-size,2rem)*0.5)] top-[calc(var(--step-rail-trigger-pad,0px)+var(--step-rail-indicator-size,2rem)*0.5)] h-[1.5px] w-[calc(100%-var(--step-rail-indicator-size,2rem))] -translate-y-1/2",
        vertical:
          "left-[calc(var(--step-rail-trigger-pad,0px)+var(--step-rail-indicator-size,2rem)*0.5)] w-[1.5px] -translate-x-1/2",
      },
      layout: {
        stepper: "group-first/step:hidden bg-[var(--step-rail-pending)]",
        timeline: "bg-[var(--step-rail-timeline-track)]",
        "stepper-vertical": "group-last/step:hidden bg-[var(--step-rail-pending)]",
      },
    },
    compoundVariants: [
      {
        orientation: "vertical",
        layout: "timeline",
        class:
          "top-[var(--step-rail-indicator-size,2rem)] h-[calc(100%-var(--step-rail-indicator-size,2rem))]",
      },
      {
        orientation: "vertical",
        layout: "stepper-vertical",
        class:
          "top-[calc(var(--step-rail-trigger-pad,0px)+var(--step-rail-indicator-size,2rem))] -bottom-[var(--step-rail-trigger-pad,0px)]",
      },
    ],
    defaultVariants: {
      orientation: "vertical",
      layout: "timeline",
    },
  },
);

export const stepRailSeparatorFillVariants = cva(
  [
    "size-full transition-[clip-path] ease-out-quart",
    "duration-[var(--step-rail-duration,var(--motion-move))]",
    "delay-[calc(var(--step-rail-line-stagger,0)*var(--step-rail-duration,0ms))]",
  ].join(" "),
  {
    variants: {
      orientation: {
        horizontal: "[clip-path:inset(0_100%_0_0)] data-[state=completed]:[clip-path:inset(0_0_0_0)]",
        // Grow origin→destination (top→bottom). `inset(100% 0 0 0)` would fill
        // upward, so a 1→3 jump reads as 2→1 and 3→2.
        vertical: "[clip-path:inset(0_0_100%_0)] data-[state=completed]:[clip-path:inset(0_0_0_0)]",
      },
      layout: {
        stepper: "bg-[var(--step-rail-completed-line)]",
        "stepper-vertical": "bg-[var(--step-rail-completed-line)]",
        timeline: "bg-[var(--step-rail-timeline-fill)]",
      },
    },
    defaultVariants: {
      orientation: "vertical",
      layout: "timeline",
    },
  },
);

export interface StepRailSeparatorProps extends React.ComponentProps<"div"> {
  orientation?: StepRailOrientation;
  size?: StepRailSize;
  lineState?: "completed" | "pending";
  layout?: "stepper" | "timeline" | "stepper-vertical";
  /** Unitless multiplier of `--step-rail-duration` before this fill starts. */
  staggerSteps?: number;
}

export function StepRailSeparator({
  className,
  orientation = "vertical",
  size = "default",
  lineState = "pending",
  layout = "timeline",
  staggerSteps = 0,
  style,
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
      style={
        {
          ...style,
          "--step-rail-line-stagger": staggerSteps,
        } as React.CSSProperties
      }
      {...props}
    >
      <div
        data-slot="step-rail-separator-fill"
        data-state={lineState}
        className={stepRailSeparatorFillVariants({ orientation, layout })}
      />
    </div>
  );
}

