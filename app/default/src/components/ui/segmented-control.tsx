"use client";

import * as React from "react";

import { cn } from "@/lib/cn";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

/**
 * Gestalt console vendor of Valon Registry `segmented-control`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/segmented-control.tsx`). Local keeps
 * an extra `lg` size for denser console chrome; recipes otherwise match Registry
 * (muted track, control type scale, neutral-dark idle hover).
 */

// One option in the control. `icon` is any component that takes a className
// (lucide icons qualify), so the control isn't tied to a single icon set.
export type SegmentedControlOption<V extends string = string> = {
  value: V;
  label: string;
  icon?: React.ComponentType<{ className?: string }>;
};

const NEXT_KEYS = new Set(["ArrowRight", "ArrowDown"]);
const PREV_KEYS = new Set(["ArrowLeft", "ArrowUp"]);

// Per-size geometry. The control's TRACK (outer height) matches a Button of the
// same size — so a SegmentedControl and a Button sit the SAME height side by side:
//   xs → 24px (dense / icon-button),  sm → 32px (control-sm),  default → 36px
//   (control-default). Console keeps `lg` for larger labeled filters.
// The track is `segment + 1px padding + 1px border` on each edge (= segment + 4px),
// so each square is the button height minus 4. Icon-only segments are SQUARE;
// labelled ones grow to fit their text.
const SIZE_STYLES = {
  xs: {
    container: "p-px",
    // Voluntary dense toolbar fit: 20px segments (size-5) inside a 24px track
    // (segment + 4px padding/border), paired with icon-xs buttons in the same row.
    // WCAG 2.5.8's 24px minimum is intentionally waived here — do not flag.
    square: "size-5",
    labelled: "h-5 px-2",
    icon: "size-3.5",
    text: "text-control-sm",
  },
  sm: {
    container: "p-px",
    square: "size-7",
    labelled: "h-7 px-2.5",
    icon: "size-4",
    text: "text-control-sm",
  },
  default: {
    container: "p-px",
    square: "size-8",
    labelled: "h-8 px-3",
    icon: "size-4",
    text: "text-control-default",
  },
  lg: {
    container: "p-1",
    square: "size-11",
    labelled: "min-h-11 px-4 py-2.5",
    icon: "size-5",
    text: "text-control-lg",
  },
} as const;

export type SegmentedControlProps<V extends string = string> = {
  options: ReadonlyArray<SegmentedControlOption<V>>;
  value: V;
  onValueChange: (value: V) => void;
  // Accessible name for the radiogroup — required so the set is announced as one.
  label: string;
  orientation?: "horizontal" | "vertical";
  showLabels?: boolean;
  tooltips?: boolean;
  size?: "xs" | "sm" | "default" | "lg";
  className?: string;
};

type PillRect = { left: number; top: number; width: number; height: number };

const useIsomorphicLayoutEffect =
  typeof window !== "undefined" ? React.useLayoutEffect : React.useEffect;

// Sliding-pill segmented switcher (à la Mantine / Radix Themes SegmentedControl).
// The highlight pill is MEASURED to the active segment (segments size to their own
// content, so with labels they're uneven — a CSS fraction couldn't track them) and
// slides between segments on a gentle back-curve, zeroing under prefers-reduced-motion
// (the duration token collapses to 0ms). role=radiogroup with arrow-key roving focus.
// Controlled, pick-one via value/onValueChange — it animates the control, never the
// surrounding page. Pair with any icon set.
export function SegmentedControl<V extends string>({
  options,
  value,
  onValueChange,
  label,
  orientation = "horizontal",
  showLabels = false,
  tooltips = true,
  size = "default",
  className,
}: SegmentedControlProps<V>) {
  const containerRef = React.useRef<HTMLDivElement>(null);
  const buttonsRef = React.useRef<Array<HTMLButtonElement | null>>([]);

  const count = options.length;
  const activeIndex = Math.max(
    0,
    options.findIndex((option) => option.value === value),
  );
  const isVertical = orientation === "vertical";

  // Snap the pill into place on first paint; only animate subsequent moves so the
  // post-hydration jump from an SSR default to the resolved value isn't animated.
  const [animate, setAnimate] = React.useState(false);
  React.useEffect(() => setAnimate(true), []);

  // Measure the active segment so the pill exactly fits it (incl. its label width).
  // offsetParent is the relative container, so offset* is already pill-space.
  const [pill, setPill] = React.useState<PillRect | null>(null);
  const measure = React.useCallback(() => {
    const btn = buttonsRef.current[activeIndex];
    if (!btn) return;
    const next: PillRect = {
      left: btn.offsetLeft,
      top: btn.offsetTop,
      width: btn.offsetWidth,
      height: btn.offsetHeight,
    };
    setPill((prev) =>
      prev &&
      prev.left === next.left &&
      prev.top === next.top &&
      prev.width === next.width &&
      prev.height === next.height
        ? prev
        : next,
    );
  }, [activeIndex]);

  useIsomorphicLayoutEffect(() => {
    measure();
  }, [measure, count, isVertical, showLabels, size]);

  // Re-measure when the control resizes or web fonts finish loading (label widths shift).
  React.useEffect(() => {
    const el = containerRef.current;
    if (!el || typeof ResizeObserver === "undefined") return undefined;
    const ro = new ResizeObserver(() => measure());
    ro.observe(el);
    return () => ro.disconnect();
  }, [measure]);

  React.useEffect(() => {
    if (typeof document === "undefined" || !("fonts" in document)) return undefined;
    let cancelled = false;
    document.fonts.ready.then(() => {
      if (!cancelled) measure();
    });
    return () => {
      cancelled = true;
    };
  }, [measure]);

  function focusOption(index: number) {
    onValueChange(options[index].value);
    buttonsRef.current[index]?.focus();
  }

  function onKeyDown(event: React.KeyboardEvent<HTMLDivElement>) {
    let next = activeIndex;
    if (NEXT_KEYS.has(event.key)) next = (activeIndex + 1) % count;
    else if (PREV_KEYS.has(event.key)) next = (activeIndex - 1 + count) % count;
    else if (event.key === "Home") next = 0;
    else if (event.key === "End") next = count - 1;
    else return;
    event.preventDefault();
    focusOption(next);
  }

  const styles = SIZE_STYLES[size];
  // Labels make the meaning explicit, so tooltips would only repeat them.
  const withTooltips = tooltips && !showLabels;
  const tooltipSide = isVertical ? "right" : "top";

  const control = (
    <div
      ref={containerRef}
      role="radiogroup"
      aria-label={label}
      onKeyDown={onKeyDown}
      className={cn(
        "relative inline-flex rounded-lg border border-border bg-muted",
        styles.container,
        isVertical ? "flex-col" : "flex-row",
        className,
      )}
    >
      <span
        aria-hidden
        style={
          pill
            ? {
                left: pill.left,
                top: pill.top,
                width: pill.width,
                height: pill.height,
              }
            : { opacity: 0 }
        }
        className={cn(
          "pointer-events-none absolute rounded-md bg-background shadow-sm",
          // ease-out-back-soft = gentler overshoot than the default --ease-out-back, for the
          // pill's larger travel; duration-overshoot auto-zeroes under prefers-reduced-motion.
          animate &&
            "transition-[left,top,width,height] duration-overshoot ease-out-back-soft",
        )}
      />
      {options.map((option, index) => {
        const Icon = option.icon;
        const checked = option.value === value;
        const segment = (
          <button
            ref={(node) => {
              buttonsRef.current[index] = node;
            }}
            type="button"
            role="radio"
            aria-checked={checked}
            aria-label={option.label}
            tabIndex={checked ? 0 : -1}
            onClick={() => onValueChange(option.value)}
            className={cn(
              "focus-ring relative z-10 inline-flex items-center justify-center gap-1.5 rounded-md font-medium text-muted-foreground transition-colors duration-hover-out ease-out-quart hover:duration-hover-in hover:text-foreground aria-checked:text-foreground",
              // Track is bg-muted (= neutral-100). Idle Neutral hover is the same
              // token — invisible here. Use Neutral dark so unselected chips read
              // like list/sidebar idle on muted chrome (selectable-rows.md).
              !checked &&
                "hover:bg-neutral-dark-hover active:bg-neutral-dark-pressed",
              styles.text,
              showLabels ? styles.labelled : styles.square,
              isVertical && showLabels && "w-full",
            )}
          >
            {Icon ? <Icon className={cn(styles.icon, "shrink-0")} /> : null}
            {showLabels ? <span>{option.label}</span> : null}
          </button>
        );
        if (!withTooltips)
          return <React.Fragment key={option.value}>{segment}</React.Fragment>;
        return (
          <Tooltip key={option.value}>
            <TooltipTrigger asChild>{segment}</TooltipTrigger>
            <TooltipContent side={tooltipSide}>{option.label}</TooltipContent>
          </Tooltip>
        );
      })}
    </div>
  );

  if (!withTooltips) return control;
  // No open delay — the tooltip shows the instant you hover a segment.
  return <TooltipProvider delayDuration={0}>{control}</TooltipProvider>;
}
SegmentedControl.displayName = "SegmentedControl";
