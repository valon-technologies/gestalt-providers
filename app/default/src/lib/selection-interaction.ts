/**
 * Control / selectable interaction chrome — one ladder, many markers.
 *
 * Normative router: `guidelines/interaction-chrome.md`.
 * Row surface detail: `guidelines/selectable-rows.md`.
 * Disabled checked/unchecked: `disabled-selection.ts` / `disabled-states.md`.
 * Mechanism (L-step vs scrim): `hover-pressed-color.md` / `press-feedback.md`.
 *
 * | Regime | Rest | Hover | Press |
 * | Neutral (idle) | transparent / outline | `neutral-hover` | `neutral-pressed` |
 * | Soft (`data-soft`, rows only) | `accent` | `accent-fill-hover` | `accent-fill-pressed` |
 * | Selected | `accent-vivid` | `accent-vivid-hover` | `accent-vivid-pressed` |
 *
 * Prefer this helper (or `listItemInteraction` for row `data-selected` + soft)
 * over hand-rolled hover/press or Button `state-overlay` scrims on mid-chroma
 * selected fills.
 */

export type SelectionPointer = "css" | "css-group" | "rac";

/** How the host marks selected (and soft, for rows). */
export type SelectionMarker = "data-selected" | "state-on" | "data-active";

export type SelectionInteractionOptions = {
  pointer?: SelectionPointer;
  marker?: SelectionMarker;
  /**
   * Accent soft regime (`data-soft`) — calendar range middle / soft fill.
   * Only valid with `marker: "data-selected"`. Defaults to `true` for that marker.
   */
  soft?: boolean;
  /**
   * When selected, border tracks the fill at rest / hover / press
   * (`border-accent-vivid*`). Use on outlined chips / outline toggles so the
   * edge does not flash idle `border-input` or transparent.
   */
  bordered?: boolean;
  /**
   * `medium` matches List Item selected weight. Controls that already set
   * `font-medium` on the base (Toggle, Chip) should pass `inherit`.
   */
  selectedWeight?: "medium" | "inherit";
};

type PointerVerbs = {
  hover: string;
  press: string;
};

function verbs(pointer: SelectionPointer): PointerVerbs {
  switch (pointer) {
    case "css-group":
      return { hover: "group-hover", press: "group-active" };
    case "rac":
      return { hover: "data-[hovered]", press: "data-[pressed]" };
    case "css":
    default:
      return { hover: "hover", press: "active" };
  }
}

function dataSelectedLadder(
  pointer: SelectionPointer,
  soft: boolean,
  bordered: boolean,
  selectedWeight: "medium" | "inherit",
): string {
  const { hover, press } = verbs(pointer);
  const idleGate = soft
    ? "[&:not([data-selected]):not([data-soft])]"
    : "[&:not([data-selected])]";
  const weight = selectedWeight === "medium" ? " data-[selected]:font-medium" : "";
  const parts = [
    `${idleGate}:${hover}:bg-neutral-hover ${idleGate}:${hover}:text-foreground`,
    `${idleGate}:${press}:bg-neutral-pressed ${idleGate}:${press}:text-foreground`,
  ];
  if (soft) {
    parts.push(
      "data-[soft]:bg-accent data-[soft]:font-normal data-[soft]:text-accent-foreground",
      `data-[soft]:${hover}:bg-accent-fill-hover data-[soft]:${hover}:text-accent-foreground`,
      `data-[soft]:${press}:bg-accent-fill-pressed data-[soft]:${press}:text-accent-foreground`,
    );
  }
  parts.push(
    `data-[selected]:bg-accent-vivid${weight} data-[selected]:text-accent-vivid-foreground`,
    `data-[selected]:${hover}:bg-accent-vivid-hover data-[selected]:${hover}:text-accent-vivid-foreground`,
    `data-[selected]:${press}:bg-accent-vivid-pressed data-[selected]:${press}:text-accent-vivid-foreground`,
  );
  if (bordered) {
    parts.push(
      "data-[selected]:border-accent-vivid",
      `data-[selected]:${hover}:border-accent-vivid-hover`,
      `data-[selected]:${press}:border-accent-vivid-pressed`,
    );
  }
  if (pointer === "css-group") {
    parts.push(
      "group-disabled:bg-transparent group-disabled:group-hover:bg-transparent group-disabled:group-active:bg-transparent",
    );
  }
  return parts.join(" ");
}

/** Selected border tracks Accent vivid fill — compose when the fill ladder already sits on the base. */
export const selectionBorderTrackStateOn =
  "data-[state=on]:enabled:border-accent-vivid data-[state=on]:enabled:hover:border-accent-vivid-hover data-[state=on]:enabled:active:border-accent-vivid-pressed";

function stateOnLadder(bordered: boolean): string {
  // Radix Toggle / Chip — mid-chroma confirm via L-step; kill Button scrim on selected.
  const parts = [
    "data-[state=off]:enabled:hover:bg-neutral-hover data-[state=off]:enabled:hover:text-foreground",
    "data-[state=off]:enabled:active:bg-neutral-pressed data-[state=off]:enabled:active:after:opacity-0",
    "data-[state=on]:enabled:bg-accent-vivid data-[state=on]:enabled:text-accent-vivid-foreground",
    "data-[state=on]:enabled:hover:bg-accent-vivid-hover data-[state=on]:enabled:hover:text-accent-vivid-foreground",
    "data-[state=on]:enabled:active:bg-accent-vivid-pressed data-[state=on]:enabled:active:text-accent-vivid-foreground",
    "data-[state=on]:enabled:active:after:opacity-0",
  ];
  if (bordered) {
    parts.push(selectionBorderTrackStateOn);
  }
  return parts.join(" ");
}

function dataActiveLadder(
  bordered: boolean,
  selectedWeight: "medium" | "inherit",
): string {
  const parts = [
    "[&:not([data-active])]:hover:bg-neutral-hover [&:not([data-active])]:hover:text-foreground",
    "[&:not([data-active])]:active:bg-neutral-pressed [&:not([data-active])]:active:text-foreground",
    "data-[active]:bg-accent-vivid data-[active]:text-accent-vivid-foreground",
    "data-[active]:hover:bg-accent-vivid-hover data-[active]:hover:text-accent-vivid-foreground",
    "data-[active]:active:bg-accent-vivid-pressed data-[active]:active:text-accent-vivid-foreground",
  ];
  if (selectedWeight === "medium") {
    parts.push("data-[active]:font-medium");
  }
  if (bordered) {
    parts.push(
      "data-[active]:border-accent-vivid",
      "data-[active]:hover:border-accent-vivid-hover",
      "data-[active]:active:border-accent-vivid-pressed",
    );
  }
  return parts.join(" ");
}

/**
 * Build Neutral idle + Accent vivid selected (+ optional soft / bordered) classes.
 */
export function selectionInteraction(
  options: SelectionInteractionOptions = {},
): string {
  const pointer = options.pointer ?? "css";
  const marker = options.marker ?? "data-selected";
  const soft = options.soft ?? marker === "data-selected";
  const bordered = options.bordered ?? false;
  const selectedWeight =
    options.selectedWeight ??
    (marker === "data-selected" ? "medium" : "inherit");

  if (soft && marker !== "data-selected") {
    throw new Error(
      'selectionInteraction: soft regime requires marker "data-selected"',
    );
  }

  if (marker === "state-on") {
    if (pointer !== "css") {
      throw new Error(
        'selectionInteraction: marker "state-on" only supports pointer "css"',
      );
    }
    return stateOnLadder(bordered);
  }

  if (marker === "data-active") {
    if (pointer !== "css") {
      throw new Error(
        'selectionInteraction: marker "data-active" only supports pointer "css"',
      );
    }
    return dataActiveLadder(bordered, selectedWeight);
  }

  return dataSelectedLadder(pointer, soft, bordered, selectedWeight);
}
