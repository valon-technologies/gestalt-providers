/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 * Registry `banner`.
 *
 * Console adaptation: sticky stacking is owned by `__root.tsx` (DevWorktreeBanner +
 * AppTopBar). Do not add sticky/fixed on this root.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { X } from "lucide-react";

import { Button } from "@/components/ui/button";
import { cn } from "@/lib/cn";

// Full-bleed, page/app-level system-message chrome (guidelines/banner.md) — sibling to
// Alert (in-region) and Sonner (transient), not a mode of either. Geometry is baked into
// the base classes and never meant to be overridden at the call site: full width, square
// corners, no border chrome. That's the fix for the `rounded-none` override this
// component replaces (registry-call-site-overrides.md).
const bannerVariants = cva(
  "group/banner flex w-full flex-wrap items-center gap-x-3 gap-y-1 px-4 py-2 text-sm text-foreground",
  {
    variants: {
      variant: {
        default: "bg-muted",
        info: "bg-info",
        warning: "bg-warning",
        destructive: "bg-error",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  },
);

// No `success` variant: a green bar that persists is one-shot feedback wearing standing-
// state clothing (Sonner's job, not this one's). `default` is the neutral/announcement
// tone. Text stays `text-foreground` (not the `-foreground` token pair) so the hue lives
// in the fill, matching Alert's existing ink treatment on these washes.
//
// No default ARIA role: a bar present at first paint isn't a live-region "announcement,"
// so baking in role="status" would be wrong as often as right. Pass `role`/`aria-label`
// explicitly per call site (see guidelines/banner.md § Accessibility) — `role="alert"`
// only for a genuine WCAG 2.2.4 emergency.
//
// No built-in visibility, position, or persistence state — `Banner` is presentational like
// Alert; the app owns show/hide, sticky/fixed placement, and any dismissal persistence
// (registry-overwrite-surface.md).

function Banner({
  className,
  variant,
  ...props
}: React.ComponentProps<"div"> & VariantProps<typeof bannerVariants>) {
  return (
    <div
      data-slot="banner"
      className={cn(bannerVariants({ variant }), className)}
      {...props}
    />
  );
}

// Decorative by construction — Title and/or Description carry the meaning, so the icon
// is aria-hidden unconditionally rather than left to the call site.
function BannerIcon({ className, ...props }: Omit<React.ComponentProps<"span">, "aria-hidden">) {
  return (
    <span
      data-slot="banner-icon"
      className={cn("flex shrink-0 items-center [&>svg]:size-4", className)}
      {...props}
      aria-hidden="true"
    />
  );
}

// Optional kind label for metadata chrome (worktree name, impersonation target). Sibling
// of Description; the root `gap-x-3` is the space between them. Never truncates.
function BannerTitle({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="banner-title"
      className={cn("min-w-0 shrink-0 font-medium tracking-tight", className)}
      {...props}
    />
  );
}

// Wraps, never truncates — a full-bleed bar that clips its own message at narrow
// widths is a reflow failure (WCAG 2.2 1.4.10), not a design choice. Primary ink until
// a Title is present; then the value mutes, same Title/Description pairing as Alert.
function BannerDescription({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="banner-description"
      className={cn(
        "min-w-0 flex-1 text-foreground group-has-[[data-slot=banner-title]]/banner:text-muted-foreground",
        className,
      )}
      {...props}
    />
  );
}

function BannerActions({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="banner-actions"
      className={cn("ml-auto flex shrink-0 items-center gap-1", className)}
      {...props}
    />
  );
}

// `aria-label` is required (not merely optional, as Button's own type allows) — the name
// must state the object being dismissed ("Dismiss maintenance notice"), never bare "Close".
// No internal open state: the call site owns visibility, same contract as Alert.
function BannerClose({
  className,
  "aria-label": ariaLabel,
  ...props
}: Omit<React.ComponentProps<typeof Button>, "aria-label" | "type"> & { "aria-label": string }) {
  return (
    <Button
      data-slot="banner-close"
      variant="ghost"
      size="icon-sm"
      aria-label={ariaLabel}
      className={className}
      {...props}
      type="button"
    >
      <X />
    </Button>
  );
}

export { Banner, BannerIcon, BannerTitle, BannerDescription, BannerActions, BannerClose };
