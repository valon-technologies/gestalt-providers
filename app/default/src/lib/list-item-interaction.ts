/**
 * Gestalt console vendor of Valon Registry `list-item-interaction`.
 *
 * Ownership: Valon Registry (`valon-tools/apps/registry/ui/src/lib/list-item-interaction.ts`).
 * Synced from toolshed origin/main — import-path adaptation only.
 */

import { cva, type VariantProps } from "class-variance-authority";

/**
 * Selectable-surface interaction chrome (white / card).
 *
 * Normative: `guidelines/selectable-rows.md`. Prefer these role utilities —
 * never invent fills, never author `list-item-*` / `sidebar-item-selected-*`
 * (those are legacy aliases of the roles below).
 *
 * | Regime | Rest | Hover | Press |
 * | Idle | transparent | `neutral-hover` | `neutral-pressed` |
 * | Soft (`data-soft`) | `accent` | `accent-fill-hover` | `accent-fill-pressed` |
 * | Selected (`data-selected`) | `accent-vivid` | `accent-vivid-hover` | `accent-vivid-pressed` |
 *
 * Mark selection / soft with **presence** of `data-selected` / `data-soft`.
 * Selected beats soft when both would apply — set only one.
 * Calendar **today** is not soft: use `data-today` + `bg-muted` (see calendar.tsx /
 * selectable-rows.md).
 *
 * Pointer:
 * - `css` — `:hover`/`:active` on the painted element
 * - `css-group` — `group-hover`/`group-active` when a parent `.group` is the
 *   hit target but wash must stay on a smaller child (e.g. Stepper indicator
 *   shell so Neutral fill never covers the progress rail)
 * - `rac` — React Aria hovered/pressed
 */
export const listItemInteraction = cva("", {
  variants: {
    pointer: {
      css: [
        "[&:not([data-selected]):not([data-soft])]:hover:bg-neutral-hover [&:not([data-selected]):not([data-soft])]:hover:text-foreground",
        "[&:not([data-selected]):not([data-soft])]:active:bg-neutral-pressed [&:not([data-selected]):not([data-soft])]:active:text-foreground",
        "data-[soft]:bg-accent data-[soft]:font-normal data-[soft]:text-accent-foreground",
        "data-[soft]:hover:bg-accent-fill-hover data-[soft]:hover:text-accent-foreground",
        "data-[soft]:active:bg-accent-fill-pressed data-[soft]:active:text-accent-foreground",
        "data-[selected]:bg-accent-vivid data-[selected]:font-medium data-[selected]:text-accent-vivid-foreground",
        "data-[selected]:hover:bg-accent-vivid-hover data-[selected]:hover:text-accent-vivid-foreground",
        "data-[selected]:active:bg-accent-vivid-pressed data-[selected]:active:text-accent-vivid-foreground",
      ].join(" "),
      "css-group": [
        "[&:not([data-selected]):not([data-soft])]:group-hover:bg-neutral-hover [&:not([data-selected]):not([data-soft])]:group-hover:text-foreground",
        "[&:not([data-selected]):not([data-soft])]:group-active:bg-neutral-pressed [&:not([data-selected]):not([data-soft])]:group-active:text-foreground",
        "data-[soft]:bg-accent data-[soft]:font-normal data-[soft]:text-accent-foreground",
        "data-[soft]:group-hover:bg-accent-fill-hover data-[soft]:group-hover:text-accent-foreground",
        "data-[soft]:group-active:bg-accent-fill-pressed data-[soft]:group-active:text-accent-foreground",
        "data-[selected]:bg-accent-vivid data-[selected]:font-medium data-[selected]:text-accent-vivid-foreground",
        "data-[selected]:group-hover:bg-accent-vivid-hover data-[selected]:group-hover:text-accent-vivid-foreground",
        "data-[selected]:group-active:bg-accent-vivid-pressed data-[selected]:group-active:text-accent-vivid-foreground",
        // Parent `.group` is `disabled` — kill wash (Button cursor contract).
        "group-disabled:bg-transparent group-disabled:group-hover:bg-transparent group-disabled:group-active:bg-transparent",
      ].join(" "),
      rac: [
        "[&:not([data-selected]):not([data-soft])]:data-[hovered]:bg-neutral-hover [&:not([data-selected]):not([data-soft])]:data-[hovered]:text-foreground",
        "[&:not([data-selected]):not([data-soft])]:data-[pressed]:bg-neutral-pressed [&:not([data-selected]):not([data-soft])]:data-[pressed]:text-foreground",
        "data-[soft]:bg-accent data-[soft]:font-normal data-[soft]:text-accent-foreground",
        "data-[soft]:data-[hovered]:bg-accent-fill-hover data-[soft]:data-[hovered]:text-accent-foreground",
        "data-[soft]:data-[pressed]:bg-accent-fill-pressed data-[soft]:data-[pressed]:text-accent-foreground",
        "data-[selected]:bg-accent-vivid data-[selected]:font-medium data-[selected]:text-accent-vivid-foreground",
        "data-[selected]:data-[hovered]:bg-accent-vivid-hover data-[selected]:data-[hovered]:text-accent-vivid-foreground",
        "data-[selected]:data-[pressed]:bg-accent-vivid-pressed data-[selected]:data-[pressed]:text-accent-vivid-foreground",
      ].join(" "),
    },
  },
  defaultVariants: {
    pointer: "css",
  },
});

export type ListItemInteractionProps = VariantProps<typeof listItemInteraction>;
