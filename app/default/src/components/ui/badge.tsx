/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";

import { ghostQuietChromeClassName } from "@/lib/press-feedback";
import { cn } from "@/lib/cn";

// User-chosen Badge `color` (entity labels) is a *hue identity*. Presentation
// splits on relative luminance: pale → soft wash toward white + dark ink;
// deep → solid fill + light ink (Forest / Indigo / deep band). Mid-slates
// like #64748B land on the deep path. Spec: guidelines/badges-and-tags.md ·
// gestalt-workdocs label-color-pickers · issue-171.

const HEX6 = /^#[0-9a-fA-F]{6}$/;

/** Dark ink on soft (pale) washes. */
export const BADGE_CUSTOM_COLOR_INK = "#0a0a0a";
/** Light ink on solid deep fills. */
export const BADGE_CUSTOM_COLOR_INK_LIGHT = "#ffffff";
/** Above this relative luminance → soft wash + dark ink; else solid + light. */
export const BADGE_CUSTOM_COLOR_SOFT_LUMINANCE = 0.45;

function relativeLuminance(hex: string): number {
  const channel = (start: number) => {
    const c = Number.parseInt(hex.slice(start, start + 2), 16) / 255;
    return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
  };
  // hex is #RRGGBB — skip '#'
  return 0.2126 * channel(1) + 0.7152 * channel(3) + 0.0722 * channel(5);
}

/**
 * Fill + ink for Badge `color`. Pale identities soft-mix toward white with dark
 * ink; deep identities keep the solid hex with light ink.
 */
export function badgeCustomColorStyle(color: string): React.CSSProperties {
  if (!HEX6.test(color)) {
    return { backgroundColor: color, color: BADGE_CUSTOM_COLOR_INK };
  }
  if (relativeLuminance(color) > BADGE_CUSTOM_COLOR_SOFT_LUMINANCE) {
    return {
      // 50% identity into white — pale chips stay chromatic + AA with dark ink.
      backgroundColor: `color-mix(in oklch, ${color} 50%, white)`,
      color: BADGE_CUSTOM_COLOR_INK,
    };
  }
  return {
    backgroundColor: color,
    color: BADGE_CUSTOM_COLOR_INK_LIGHT,
  };
}

const badgeVariants = cva(
  // Badges are single-line soft-rects (`rounded-sm` / --radius-sm ≈ 4px) — squarer
  // than a capsule, tighter than Button's `rounded-md`, matching Registry status
  // chips. Without nowrap, label text wraps inside narrow table/sidebar cells.
  // Type / pad / icon live on `size` — not the base — so sm/default/lg read as
  // distinct envelopes (badges-and-tags.md), never padding-only nudges.
  // No transition: Badge is static metadata; color/surface feedback snaps
  // (`transitions.md`). Bare Tailwind color-transition utilities carry a 150ms
  // default — never add them here. toolshed#4057 / #4081
  // `text-box: trim-both cap alphabetic` trims font ascent/descent so flex
  // centering aligns glyph ink (counts look top-heavy otherwise). Ignored where
  // unsupported — degrades to ordinary leading-none centering.
  "inline-flex items-center justify-center whitespace-nowrap rounded-sm font-normal [text-box:trim-both_cap_alphabetic] [&>svg]:shrink-0",
  {
    variants: {
      variant: {
        default: "bg-primary text-primary-foreground",
        secondary: "bg-foreground/[0.06] text-foreground/80",
        // Rest is --muted (≡ --neutral-hover). Hover deepens with the muted-chrome
        // band (`hover-pressed-color.md`) — never muted/80, which lightens the fill.
        // Same perceived step as Button ghost's on-color scrim (`press-feedback.md`).
        muted:
          "bg-muted text-muted-foreground hover:bg-neutral-dark-hover hover:text-foreground",
        outline: "border border-border text-foreground",
        // Transparent quiet chrome SoT: `@/lib/press-feedback` (same as Button ghost).
        ghost: ghostQuietChromeClassName,
        // Registry Badge — uses --badge-* status surfaces (shared/theme.css)
        // so legacy gestalt-shell --success grove overrides do not affect chips.
        success: "bg-badge-success text-badge-success-foreground",
        warning: "bg-badge-warning text-badge-warning-foreground",
        info: "bg-badge-info text-badge-info-foreground",
        destructive: "bg-badge-destructive text-badge-destructive-foreground",
        // Internal: reached only via the `color` prop. No token fill/hover so the
        // soft wash + dark ink fully own the look.
        custom: "",
      },
      size: {
        // Display-label ladder (not control-h): type is the primary differentiator;
        // pad + icon climb with it. Dense chrome → default metadata → emphasized.
        // `leading-none` lives on size (after text-*) — tailwind-merge drops a
        // base leading when size adds font-size (cn / font-size↔leading conflict).
        sm: "gap-0.5 px-1 py-px text-2xs leading-none [&>svg]:size-2.5",
        default: "gap-1 px-1.5 py-0.5 text-xs leading-none [&>svg]:size-3",
        lg: "gap-1.5 px-2 py-1 text-sm leading-none [&>svg]:size-3.5",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  },
);

// `custom` is internal — selected only by passing `color`, never by a consumer.
type BadgeVariant = Exclude<NonNullable<VariantProps<typeof badgeVariants>["variant"]>, "custom">;

export interface BadgeProps extends Omit<React.HTMLAttributes<HTMLSpanElement>, "color"> {
  variant?: BadgeVariant;
  size?: VariantProps<typeof badgeVariants>["size"];
  asChild?: boolean;
  // Two color sources, mutually exclusive:
  //   - `variant` → a theme-token preset that CARRIES MEANING (status/category).
  //   - `color`   → an arbitrary 6-digit hex chosen by the USER (e.g. a label);
  //                 `variant` is ignored. Pale → soft wash + dark ink; deep →
  //                 solid fill + light ink (badgeCustomColorStyle / issue-171).
  color?: string;
}

function Badge({ className, variant, size, color, asChild = false, style, ...props }: BadgeProps) {
  const Comp = asChild ? Slot : "span";
  const custom = color != null;
  return (
    <Comp
      data-slot="badge"
      data-variant={custom ? "custom" : variant}
      className={cn(badgeVariants({ variant: custom ? "custom" : variant, size }), className)}
      style={custom ? { ...badgeCustomColorStyle(color), ...style } : style}
      {...props}
    />
  );
}

export { Badge, badgeVariants };
