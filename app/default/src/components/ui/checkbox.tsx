"use client";


/**
 * Gestalt console vendor of Valon Registry `checkbox`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/checkbox.tsx`).
 * Synced from toolshed origin/main — token adaptation only (`@/lib/cn` path).
 * Do not restyle chrome at call sites; change Registry first.
 */

import * as React from "react";
import * as CheckboxPrimitive from "@radix-ui/react-checkbox";
import { cva, type VariantProps } from "class-variance-authority";

import { SelectionCheck } from "@/components/ui/selection-check";
import { cn } from "@/lib/cn";

// A checkbox pairs with its label text, so its size tracks the font-size token
// (sm/default/lg = 14/14/16px) rather than the input/button control-height —
// hence a glyph scale of 14/16/20px. The indicator is inset ~2px on every side
// for breathing room around the checkmark. Background color is intentionally not
// transitioned: the fill snaps instantly on both check and uncheck.
//
// Check glyph: SelectionCheck density=condensed (drawFrom=checkbox) — stroke
// draw-in on enter, L→R clip wipe on exit.
// Indeterminate line: same enter/exit contract — dashoffset draws L→R on enter;
// clip-path wipes L→R on exit (then dashoffset snaps for the next draw-in).
//
// indeterminate → checked handoff: `data-check-draw=pending` holds the check
// erased until the minus wipe finishes (--duration-200), then the attribute is
// removed so the check draws in without overlapping the bar. Normal
// unchecked → checked never sets the attribute.
const checkboxVariants = cva(
  "group/checkbox peer flex shrink-0 items-center justify-center border border-input focus-ring disabled:cursor-not-allowed disabled:border-border disabled:bg-disabled disabled:data-[state=checked]:bg-disabled disabled:data-[state=indeterminate]:bg-disabled disabled:data-[state=checked]:text-disabled-foreground disabled:data-[state=indeterminate]:text-disabled-foreground data-[state=checked]:border-accent-vivid data-[state=checked]:bg-accent-vivid data-[state=checked]:text-accent-vivid-foreground data-[state=indeterminate]:border-accent-vivid data-[state=indeterminate]:bg-accent-vivid data-[state=indeterminate]:text-accent-vivid-foreground",
  {
    variants: {
      size: {
        sm: "size-3.5 rounded-[2px]",
        default: "size-4 rounded-[3px]",
        lg: "size-5 rounded-[4px]",
      },
    },
    defaultVariants: { size: "default" },
  },
);

const checkboxIndicatorVariants = cva("", {
  variants: {
    size: {
      sm: "size-2.5",
      default: "size-3",
      lg: "size-3.5",
    },
  },
  defaultVariants: { size: "default" },
});

// Enter: dashoffset draws with reveal timing. Exit: dashoffset stays drawn during
// the L→R clip wipe, then snaps to 1 after --duration-200 so the next enter can
// redraw (same contract as SelectionCheck).
// Indeterminate uses ease-in-out (symmetric wipe) rather than ease-out-quart —
// the bar should accelerate and decelerate evenly on both draw-in and L→R exit.
const INDETERMINATE_DRAW =
  "[stroke-dashoffset:1] transition-[stroke-dashoffset] duration-0 delay-[var(--duration-200)] ease-in-out group-data-[state=indeterminate]/checkbox:[stroke-dashoffset:0] group-data-[state=indeterminate]/checkbox:duration-reveal group-data-[state=indeterminate]/checkbox:delay-100";

const INDETERMINATE_EXIT_CLIP =
  "[clip-path:inset(0_0_0_100%)] transition-[clip-path] duration-[var(--duration-200)] ease-in-out group-data-[state=indeterminate]/checkbox:[clip-path:inset(0_0_0_0)] group-data-[state=indeterminate]/checkbox:duration-0";

// Bounce on checked; suppress during minus→check handoff so it fires with the draw.
const CHECK_BOUNCE =
  "group-data-[state=checked]/checkbox:animate-[valon-check-bounce_var(--duration-200)_var(--ease-out-quart)_calc(2*var(--duration-200))] group-data-[state=checked]/checkbox:group-data-[check-draw=pending]/checkbox:[animation:none]";

function markHandoffMs(el: Element): number {
  if (
    typeof window !== "undefined" &&
    window.matchMedia("(prefers-reduced-motion: reduce)").matches
  ) {
    return 0;
  }
  const raw = getComputedStyle(el).getPropertyValue("--duration-200").trim();
  const n = Number.parseFloat(raw);
  if (!Number.isFinite(n)) return 200;
  if (raw.endsWith("ms")) return n;
  if (raw.endsWith("s")) return n * 1000;
  return n;
}

const Checkbox = React.forwardRef<
  React.ElementRef<typeof CheckboxPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof CheckboxPrimitive.Root> &
    VariantProps<typeof checkboxVariants>
>(({ className, size, ...props }, ref) => {
  const rootRef = React.useRef<React.ElementRef<typeof CheckboxPrimitive.Root>>(null);
  const prevStateRef = React.useRef<string | null>(null);
  const handoffTimerRef = React.useRef<number | null>(null);
  const [checkDraw, setCheckDraw] = React.useState<"ready" | "pending">("ready");

  React.useEffect(
    () => () => {
      if (handoffTimerRef.current != null) {
        window.clearTimeout(handoffTimerRef.current);
      }
    },
    [],
  );

  React.useLayoutEffect(() => {
    const node = rootRef.current;
    if (!node) return;
    const state = node.getAttribute("data-state") ?? "unchecked";
    const prev = prevStateRef.current;

    if (prev === "indeterminate" && state === "checked") {
      prevStateRef.current = state;
      setCheckDraw("pending");
      if (handoffTimerRef.current != null) {
        window.clearTimeout(handoffTimerRef.current);
      }
      const ms = markHandoffMs(node);
      if (ms <= 0) {
        setCheckDraw("ready");
        return;
      }
      handoffTimerRef.current = window.setTimeout(() => {
        handoffTimerRef.current = null;
        setCheckDraw("ready");
      }, ms);
      return;
    }

    if (prev !== state) {
      prevStateRef.current = state;
      if (state !== "checked") {
        if (handoffTimerRef.current != null) {
          window.clearTimeout(handoffTimerRef.current);
          handoffTimerRef.current = null;
        }
        setCheckDraw("ready");
      }
    }
  });

  return (
    <CheckboxPrimitive.Root
      ref={(node) => {
        rootRef.current = node;
        if (typeof ref === "function") ref(node);
        else if (ref) ref.current = node;
      }}
      data-check-draw={checkDraw === "pending" ? "pending" : undefined}
      className={cn(checkboxVariants({ size, className }))}
      {...props}
    >
      <CheckboxPrimitive.Indicator
        forceMount
        className={cn(
          "relative flex items-center justify-center text-current",
          CHECK_BOUNCE,
        )}
      >
        <SelectionCheck
          density="condensed"
          drawFrom="checkbox"
          className="contents"
          svgClassName={checkboxIndicatorVariants({ size })}
        />
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth={3.5}
          className={cn(
            "absolute",
            checkboxIndicatorVariants({ size }),
            INDETERMINATE_EXIT_CLIP,
          )}
          aria-hidden
        >
          <line
            x1="5"
            y1="12"
            x2="19"
            y2="12"
            strokeLinecap="round"
            pathLength={1}
            strokeDasharray={1}
            className={INDETERMINATE_DRAW}
          />
        </svg>
      </CheckboxPrimitive.Indicator>
    </CheckboxPrimitive.Root>
  );
});
Checkbox.displayName = CheckboxPrimitive.Root.displayName;

export { Checkbox, checkboxVariants };
