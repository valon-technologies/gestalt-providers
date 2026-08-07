/**
 * Disabled checked vs unchecked chrome for selectable inputs.
 *
 * Normative: `guidelines/disabled-states.md` (Disabled selection).
 * Enabled selected chrome stays in `selection-interaction.ts` /
 * `guidelines/interaction-chrome.md`.
 *
 * Disabled is still a RECOLOR (chroma-zero), never a dim of the accent fill.
 * Checked must read **fuller** than unchecked so the on-state survives disable:
 *
 * | Unchecked | Checked |
 * | `outline` → transparent + `--border` | `--disabled-foreground` fill + matching border + paper ink |
 * | `fill` → `--disabled` fill + `--border` | same fuller checked pairing |
 *
 * Markers follow the host: Radix Toggle/Chip use `data-state=on|off`;
 * Checkbox/Switch/Radio use `data-state=checked|unchecked`.
 */

export type DisabledSelectionMarker = "state-on" | "checked";

/** Unchecked disabled surface — outline pills stay empty; boxes/tracks use `--disabled`. */
export type DisabledUncheckedSurface = "outline" | "fill";

export type DisabledSelectionOptions = {
  marker?: DisabledSelectionMarker;
  unchecked?: DisabledUncheckedSurface;
  /** Treat indeterminate like checked (Checkbox). */
  indeterminate?: boolean;
};

/** Shared checked-disabled fill — darker achromatic than `--disabled`. */
export const disabledCheckedFillClass =
  "bg-disabled-foreground border-disabled-foreground text-background";

function stateOnChrome(unchecked: DisabledUncheckedSurface): string {
  const off =
    unchecked === "outline"
      ? "disabled:data-[state=off]:border-border disabled:data-[state=off]:bg-transparent disabled:data-[state=off]:text-disabled-foreground"
      : "disabled:data-[state=off]:border-border disabled:data-[state=off]:bg-disabled disabled:data-[state=off]:text-disabled-foreground";
  const on =
    "disabled:data-[state=on]:border-disabled-foreground disabled:data-[state=on]:bg-disabled-foreground disabled:data-[state=on]:text-background";
  return `${off} ${on}`;
}

function checkedChrome(
  unchecked: DisabledUncheckedSurface,
  indeterminate: boolean,
): string {
  const off =
    unchecked === "outline"
      ? "disabled:data-[state=unchecked]:border-border disabled:data-[state=unchecked]:bg-transparent disabled:data-[state=unchecked]:text-disabled-foreground"
      : "disabled:data-[state=unchecked]:border-border disabled:data-[state=unchecked]:bg-disabled disabled:data-[state=unchecked]:text-disabled-foreground";
  const on =
    "disabled:data-[state=checked]:border-disabled-foreground disabled:data-[state=checked]:bg-disabled-foreground disabled:data-[state=checked]:text-background";
  const parts = [off, on];
  if (indeterminate) {
    parts.push(
      "disabled:data-[state=indeterminate]:border-disabled-foreground disabled:data-[state=indeterminate]:bg-disabled-foreground disabled:data-[state=indeterminate]:text-background",
    );
  }
  return parts.join(" ");
}

/**
 * Disabled unchecked vs checked (on) color split for selectable controls.
 * Does not set cursor / after:hidden — hosts keep those with their base chrome.
 */
export function disabledSelection(
  options: DisabledSelectionOptions = {},
): string {
  const marker = options.marker ?? "checked";
  const unchecked = options.unchecked ?? "fill";
  const indeterminate = options.indeterminate ?? false;

  if (indeterminate && marker !== "checked") {
    throw new Error(
      'disabledSelection: indeterminate only applies with marker "checked"',
    );
  }

  if (marker === "state-on") {
    return stateOnChrome(unchecked);
  }

  return checkedChrome(unchecked, indeterminate);
}
