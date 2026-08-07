/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";

import { cn } from "@/lib/cn";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

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
//   (control-default).
// Outer height is always `segment + 4px` chrome: default uses 2px padding (no
// border); outline uses 1px border + 1px padding. Icon-only segments are SQUARE;
// labelled ones grow to fit their text.
const SIZE_STYLES = {
  xs: {
    // Voluntary dense toolbar fit: 20px segments (size-5) inside a 24px track
    // (segment + 4px chrome), paired with icon-xs buttons in the same row.
    // WCAG 2.5.8's 24px minimum is intentionally waived here — do not flag.
    square: "size-5",
    labelled: "h-5 px-2",
    icon: "size-3.5",
    text: "text-control-sm",
  },
  sm: {
    square: "size-7",
    labelled: "h-7 px-2.5",
    icon: "size-4",
    text: "text-control-sm",
  },
  default: {
    square: "size-8",
    labelled: "h-8 px-3",
    icon: "size-4",
    text: "text-control-default",
  },
} as const;

// Track chrome. `default` = Radix Themes surface (borderless muted well on paper).
// `outline` = bordered track for muted chrome (sidebar / rail) where bg-muted
// would otherwise vanish into the parent.
const VARIANT_STYLES = {
  default:
    "bg-muted p-0.5 forced-colors:border forced-colors:border-[ButtonText] forced-colors:p-px",
  outline: "border border-border bg-muted p-px forced-colors:border-[ButtonText]",
} as const;

export type SegmentedControlVariant = keyof typeof VARIANT_STYLES;

export type SegmentedControlNameProps =
  | {
      /** Visible label id — preferred when a section label already names the control. */
      labelledBy: string;
      label?: never;
    }
  | {
      /** Accessible name when no visible label is associated. */
      label: string;
      labelledBy?: never;
    };

export type SegmentedControlProps<V extends string = string> = {
  options: ReadonlyArray<SegmentedControlOption<V>>;
  value: V;
  onValueChange: (value: V) => void;
  orientation?: "horizontal" | "vertical";
  showLabels?: boolean;
  tooltips?: boolean;
  size?: "xs" | "sm" | "default";
  /**
   * Track chrome. `default` is borderless muted (paper). `outline` adds a
   * hairline border for placement on muted surfaces (sidebar, rail).
   */
  variant?: SegmentedControlVariant;
  /**
   * When this control swaps a single content region, pass that region's DOM
   * id so every radio exposes `aria-controls`. Use a stable `useId()` — never
   * the selected option value when that value is also written to the URL hash.
   */
  panelId?: string;
  className?: string;
} & SegmentedControlNameProps;

/** Exactly one non-empty accessible name — mirrors Slider thumb enforcement. */
export function resolveSegmentedControlNameProps(
  props: SegmentedControlNameProps,
): { "aria-labelledby": string } | { "aria-label": string } {
  const labelledBy =
    "labelledBy" in props && typeof props.labelledBy === "string"
      ? props.labelledBy.trim()
      : "";
  if (labelledBy) {
    return { "aria-labelledby": labelledBy };
  }
  const label =
    "label" in props && typeof props.label === "string" ? props.label.trim() : "";
  if (label) {
    return { "aria-label": label };
  }
  throw new Error(
    "SegmentedControl requires an accessible name via `label` or `labelledBy`",
  );
}

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
  labelledBy,
  orientation = "horizontal",
  showLabels = false,
  tooltips = true,
  size = "default",
  variant = "default",
  panelId,
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

  // Pill geometry is measured from the active segment (labels make widths uneven,
  // so a CSS fraction can't track them). Two phases:
  //   1. Unmeasured — hide the pill; never apply transitions (or CSS interpolates
  //      from the absolute defaults 0×0 at 0,0 into the first real rect).
  //   2. Measured — paint the correct rect once, then enable transitions so only
  //      subsequent value / layout moves animate (incl. ThemeToggle's post-hydration
  //      preference resolve, which must not slide from the origin).
  const [pill, setPill] = React.useState<PillRect | null>(null);
  const [animate, setAnimate] = React.useState(false);

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
      prev && prev.left === next.left && prev.top === next.top && prev.width === next.width && prev.height === next.height
        ? prev
        : next,
    );
  }, [activeIndex]);

  useIsomorphicLayoutEffect(() => {
    measure();
  }, [measure, count, isVertical, showLabels, size, variant]);

  // Enable transitions only after a measured rect has painted — never on mount alone.
  React.useEffect(() => {
    if (pill == null || animate) return;
    setAnimate(true);
  }, [pill, animate]);

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
    // Keep page scroll stable under sticky app chrome (console).
    buttonsRef.current[index]?.focus({ preventScroll: true });
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
  const variantStyles = VARIANT_STYLES[variant];
  // Labels make the meaning explicit, so tooltips would only repeat them.
  const withTooltips = tooltips && !showLabels;
  const tooltipSide = isVertical ? "right" : "top";

  const nameProps = resolveSegmentedControlNameProps(
    labelledBy !== undefined
      ? { labelledBy }
      : { label: label as string },
  );

  const control = (
    <div
      ref={containerRef}
      role="radiogroup"
      data-variant={variant}
      {...nameProps}
      onKeyDown={onKeyDown}
      className={cn(
        "relative inline-flex rounded-md",
        variantStyles,
        isVertical ? "flex-col" : "flex-row",
        className,
      )}
    >
      <span
        aria-hidden
        style={pill ? { left: pill.left, top: pill.top, width: pill.width, height: pill.height } : { opacity: 0 }}
        className={cn(
          // Radix Themes `surface`: raised chip via fill + inset 1px hairline
          // (elevation.md canvas rule — not shadow-md/lg). Inset (not spread)
          // keeps the ring inside the measured pill so neighboring segment
          // hover washes cannot paint over it — same pattern as radio-group /
          // choice-card chrome. Forced-colors discards box-shadow; remap a
          // system border so the active chip still delineates.
          "pointer-events-none absolute rounded-md bg-background shadow-[inset_0_0_0_1px_var(--border)] forced-colors:border forced-colors:border-[Highlight] forced-colors:shadow-none",
          // ease-out-back-soft = gentler overshoot than the default --ease-out-back, for the
          // pill's larger travel; duration-overshoot auto-zeroes under prefers-reduced-motion.
          animate && "transition-[left,top,width,height] duration-overshoot ease-out-back-soft",
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
            aria-controls={panelId}
            tabIndex={checked ? 0 : -1}
            onClick={() => onValueChange(option.value)}
            className={cn(
              "focus-ring relative z-10 inline-flex items-center justify-center gap-1.5 rounded-md font-medium text-muted-foreground transition-colors duration-hover-out ease-out-quart hover:duration-hover-in hover:text-foreground aria-checked:text-foreground",
              // Track is bg-muted (= neutral-hover). Idle Neutral hover is the same
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
        if (!withTooltips) return <React.Fragment key={option.value}>{segment}</React.Fragment>;
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
