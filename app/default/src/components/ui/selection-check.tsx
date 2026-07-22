"use client";

import * as React from "react";

import { cn } from "@/lib/cn";

/**
 * Gestalt console vendor of Valon Registry `selection-check`.
 *
 * Ownership: Valon Registry
 * (`valon-tools/apps/registry/ui/src/ui/selection-check.tsx`). Token
 * adaptation only — `cn` import path.
 */

// Canonical Valon selection checkmark — stroke-draw glyph shared by Checkbox,
// Combobox, Select, and Listbox. Two paths (down into the vertex, then up out
// of it) with pathLength=1 dashoffset draw-in; bounce finishes after both enter
// strokes. Bounce (self-drawn only): celebrates unchecked → checked while
// mounted. Mount-as-checked is presence (popover reopen / ItemIndicator) — no
// bounce. Checkbox drives bounce from the root `group/checkbox` instead.
//
// Enter vs exit (pure CSS, asymmetric):
// - Enter: clip snaps open; strokes draw with dashoffset (down, then up).
// - Exit: clip-path wipes left→right (inset left 0→100%) while strokes stay
//   drawn; after the wipe, dashoffset snaps to 1 so the next enter can redraw.
//
// Density:
// - `default` — menu/list indicators (Select / Combobox / Listbox): thinner
//   stroke, wider mark so the check reads open on a blank or gold row
// - `condensed` — Checkbox: tighter mark + heavier stroke that fits the box
//
// Tone (self-drawn only):
// - `solid` — blank-row indicators (Select / Combobox flyouts): --accent-solid ink
// - `current` — filled-row indicators (Listbox): currentColor

type CheckDensity = "default" | "condensed";

/** Who owns checked state for the stroke-draw selectors. */
type CheckDrawFrom = "self" | "checkbox";

const DENSITY = {
  // Wider arms (~3→21 in the 24 box) + lighter stroke for menu rows.
  default: {
    strokeWidth: 2.5,
    down: "M3 12.75l7.5 6",
    up: "M10.5 18.75l10.5 -13.5",
    svgClassName: "size-3.5",
  },
  // Original checkbox proportions — heavy stroke in a tight box.
  condensed: {
    strokeWidth: 3.5,
    down: "M4.5 12.75l6 6",
    up: "M10.5 18.75l9 -13.5",
    svgClassName: "size-3.5",
  },
} as const;

// Stroke draw-in on enter; on exit dashoffset snaps to 1 only AFTER the L→R
// clip wipe (delay = wipe duration) so the wipe has ink to reveal through.
// Spelled literally — Tailwind cannot see interpolated group names.
const SELF_DOWN_DRAW =
  "[stroke-dashoffset:1] transition-[stroke-dashoffset] duration-0 delay-[var(--duration-200)] ease-out-quart group-data-[state=checked]/selection-check:[stroke-dashoffset:0] group-data-[state=checked]/selection-check:duration-[var(--duration-200)] group-data-[state=checked]/selection-check:delay-0";

const SELF_UP_DRAW =
  "[stroke-dashoffset:1] transition-[stroke-dashoffset] duration-0 delay-[var(--duration-200)] ease-out-quart group-data-[state=checked]/selection-check:[stroke-dashoffset:0] group-data-[state=checked]/selection-check:duration-[var(--duration-200)] group-data-[state=checked]/selection-check:delay-[var(--duration-200)]";

// Checkbox draw matches self-drawn checked timing. Parent may set
// `data-check-draw=pending` during indeterminate→checked so the minus wipe
// finishes first — pending forces the mark erased without changing the
// unchecked→checked path (opt-out, not opt-in on ready).
const CHECKBOX_DOWN_DRAW =
  "[stroke-dashoffset:1] transition-[stroke-dashoffset] duration-0 delay-[var(--duration-200)] ease-out-quart group-data-[state=checked]/checkbox:[stroke-dashoffset:0] group-data-[state=checked]/checkbox:duration-[var(--duration-200)] group-data-[state=checked]/checkbox:delay-0 group-data-[state=checked]/checkbox:group-data-[check-draw=pending]/checkbox:[stroke-dashoffset:1] group-data-[state=checked]/checkbox:group-data-[check-draw=pending]/checkbox:duration-0 group-data-[state=checked]/checkbox:group-data-[check-draw=pending]/checkbox:delay-0";

const CHECKBOX_UP_DRAW =
  "[stroke-dashoffset:1] transition-[stroke-dashoffset] duration-0 delay-[var(--duration-200)] ease-out-quart group-data-[state=checked]/checkbox:[stroke-dashoffset:0] group-data-[state=checked]/checkbox:duration-[var(--duration-200)] group-data-[state=checked]/checkbox:delay-[var(--duration-200)] group-data-[state=checked]/checkbox:group-data-[check-draw=pending]/checkbox:[stroke-dashoffset:1] group-data-[state=checked]/checkbox:group-data-[check-draw=pending]/checkbox:duration-0 group-data-[state=checked]/checkbox:group-data-[check-draw=pending]/checkbox:delay-0";

// L→R exit wipe: unchecked clips from the left; checked is fully open. Enter
// snaps clip open (duration-0 when checked); exit animates the inset.
// Pending keeps the clip closed during indeterminate→checked handoff.
const SELF_EXIT_CLIP =
  "[clip-path:inset(0_0_0_100%)] transition-[clip-path] duration-[var(--duration-200)] ease-out-quart group-data-[state=checked]/selection-check:[clip-path:inset(0_0_0_0)] group-data-[state=checked]/selection-check:duration-0";

const CHECKBOX_EXIT_CLIP =
  "[clip-path:inset(0_0_0_100%)] transition-[clip-path] duration-[var(--duration-200)] ease-out-quart group-data-[state=checked]/checkbox:[clip-path:inset(0_0_0_0)] group-data-[state=checked]/checkbox:duration-0 group-data-[state=checked]/checkbox:group-data-[check-draw=pending]/checkbox:[clip-path:inset(0_0_0_100%)] group-data-[state=checked]/checkbox:group-data-[check-draw=pending]/checkbox:duration-0";
function drawClasses(drawFrom: CheckDrawFrom) {
  if (drawFrom === "checkbox") {
    return {
      down: CHECKBOX_DOWN_DRAW,
      up: CHECKBOX_UP_DRAW,
      clip: CHECKBOX_EXIT_CLIP,
    };
  }
  return { down: SELF_DOWN_DRAW, up: SELF_UP_DRAW, clip: SELF_EXIT_CLIP };
}

export type SelectionCheckProps = {
  checked?: boolean;
  density?: CheckDensity;
  /**
   * `self` (default) — this node is the `group/selection-check` and owns
   * `data-state`. `checkbox` — strokes follow the parent `group/checkbox`
   * (Checkbox Indicator with forceMount); no local bounce.
   */
  drawFrom?: CheckDrawFrom;
  tone?: "current" | "solid";
  className?: string;
  /** Overrides the density default SVG size (Checkbox size scale). */
  svgClassName?: string;
};

export function SelectionCheck({
  checked = true,
  density = "default",
  drawFrom = "self",
  tone = "current",
  className,
  svgClassName,
}: SelectionCheckProps) {
  const preset = DENSITY[density];
  const { down, up, clip } = drawClasses(drawFrom);
  const prevChecked = React.useRef<boolean | null>(null);
  const [bounce, setBounce] = React.useState(false);

  React.useEffect(() => {
    if (drawFrom !== "self") return;
    if (prevChecked.current === null) {
      // First paint: presence only — do not celebrate mount-as-checked.
      prevChecked.current = checked;
      return;
    }
    if (checked && !prevChecked.current) {
      setBounce(true);
    } else if (!checked) {
      setBounce(false);
    }
    prevChecked.current = checked;
  }, [checked, drawFrom]);

  return (
    <span
      data-slot="selection-check"
      data-density={density}
      data-state={drawFrom === "self" ? (checked ? "checked" : "unchecked") : undefined}
      className={cn(
        drawFrom === "self" && "group/selection-check",
        "flex size-4 shrink-0 items-center justify-center",
        tone === "solid" ? "text-accent-solid" : "text-current",
        drawFrom === "self" &&
          bounce &&
          "animate-[valon-check-bounce_var(--duration-200)_var(--ease-out-quart)_calc(2*var(--duration-200))]",
        className,
      )}
      aria-hidden
    >
      <svg
        xmlns="http://www.w3.org/2000/svg"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth={preset.strokeWidth}
        className={cn(preset.svgClassName, svgClassName, clip)}
      >
        <path
          d={preset.down}
          strokeLinecap="round"
          pathLength={1}
          strokeDasharray={1}
          className={down}
        />
        <path
          d={preset.up}
          strokeLinecap="round"
          pathLength={1}
          strokeDasharray={1}
          className={up}
        />
      </svg>
    </span>
  );
}
