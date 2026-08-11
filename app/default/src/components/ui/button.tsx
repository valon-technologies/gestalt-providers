/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";
import {
  ghostQuietChromePaintClassName,
  pressFeedbackScrimClassName,
  pressFeedbackScrimOptOutClassName,
  secondarySurfaceFillClassName,
} from "@/lib/press-feedback";
import { Spinner } from "@/components/ui/spinner";

// Hover/press feedback is an on-color state-layer overlay, not a brightness
// filter (press-feedback.md / RES-20260617-004): the `::after` scrim paints in
// currentColor — the variant's on-color/label ink — so it lightens a near-black
// fill and darkens a near-cream one with no luminance check, where
// `brightness()` had no headroom on the dark `default` fill. Opacity snaps
// (never transitioned). Disabled is a recolor to a flat neutral, not opacity-50
// (disabled-states.md / RES-20260617-003): the brand hue is erased so disabled
// can't read as a dimmer enabled. Disabled opts out of the scrim.
//
// Scrim SoT: `@/lib/press-feedback` (toolshed#4081). Ghost paint shares
// `ghostQuietChromePaintClassName` with Badge ghost.
//
// We deliberately DON'T set `disabled:pointer-events-none` — that would suppress
// `cursor-not-allowed`. A disabled control can still match `:hover`, but every
// variant's hover utility (danger/ghost bg/text, and the scrim)
// is overridden by a `disabled:` rule emitted LATER at equal specificity, so the
// disabled recolor always wins the cascade — no hover leaks through.
//
// Loading is a separate path (disabled-states.md / buttons.md / RES-20260804-001):
// `aria-disabled` + `data-loading` + Spinner, still focusable, keeps enabled
// chrome. Never reuse the disabled recolor for transient busy. `loading` and
// `disabled` are mutually exclusive — loading wins and strips native `disabled`.
// Busy-only chrome (wait cursor) keys off `data-loading:` so caller
// `aria-disabled` (discoverable-but-unavailable) does not inherit wait.
// Variant hover locks still use `aria-disabled:` (shared with unavailable).
// `asChild` does not support loading.
//
// Icon glyphs scale per size via --control-icon-* (control-sizing.md /
// RES-20260701-002). `xs` / `icon-xs` use `--control-radius-xs` (`rounded-sm`).
// Icons with an explicit size-* class opt out of the default. Loading Spinner is
// a span (`data-slot=spinner`), so size variants also size that slot.
const buttonVariants = cva(
  [
    // On-color ::after scrim SoT: `@/lib/press-feedback` (press-feedback.md).
    pressFeedbackScrimClassName,
    "inline-flex items-center justify-center gap-1.5 whitespace-nowrap rounded-md text-sm font-medium transition-[color,background-color,border-color] duration-hover-out ease-out-quart hover:duration-hover-in focus-ring disabled:cursor-not-allowed disabled:border-border disabled:bg-disabled disabled:text-disabled-foreground disabled:shadow-none",
    pressFeedbackScrimOptOutClassName,
    "data-loading:cursor-wait data-loading:[&_svg]:hidden [&_svg]:pointer-events-none [&_svg]:shrink-0 [&_svg:not([class*='size-'])]:size-[length:var(--control-icon-default)] [&_[data-slot=spinner]]:size-[length:var(--control-icon-default)]",
  ],
  {
    variants: {
      variant: {
        default: "bg-primary text-primary-foreground",
        destructive: "bg-destructive text-destructive-foreground",
        success:
          "border border-success-foreground/30 bg-success/40 text-success-foreground hover:border-success-foreground/50 hover:bg-success hover:text-success-foreground aria-disabled:hover:border-success-foreground/30 aria-disabled:hover:bg-success/40 aria-disabled:hover:text-success-foreground aria-disabled:active:bg-success/40",
        danger:
          "border border-destructive/30 bg-destructive/5 text-destructive hover:border-destructive hover:bg-destructive hover:text-destructive-foreground aria-disabled:hover:border-destructive/30 aria-disabled:hover:bg-destructive/5 aria-disabled:hover:text-destructive aria-disabled:active:bg-destructive/5",
        outline:
          "border border-input bg-transparent disabled:bg-transparent aria-disabled:hover:bg-transparent aria-disabled:active:bg-transparent",
        // Soft deepen of the parent — ink-alpha fill (Badge secondary), not
        // solid `bg-secondary` (that reads as a gray chip on Alert washes).
        secondary: [secondarySurfaceFillClassName, "text-foreground"],
        // Transparent quiet chrome paint SoT: ghostQuietChromePaintClassName.
        // Hover/press come from the base scrim — never --accent-hover (menus/breadcrumbs).
        ghost: [
          ghostQuietChromePaintClassName,
          "aria-checked:bg-foreground aria-checked:text-background disabled:bg-transparent aria-disabled:hover:text-muted-foreground",
        ],
        ghostSuccess:
          "text-muted-foreground hover:bg-success hover:text-success-foreground active:bg-success/70 disabled:bg-transparent aria-disabled:hover:bg-transparent aria-disabled:hover:text-muted-foreground aria-disabled:active:bg-transparent",
        ghostDestructive:
          "text-muted-foreground hover:bg-destructive hover:text-destructive-foreground active:bg-destructive/70 disabled:bg-transparent aria-disabled:hover:bg-transparent aria-disabled:hover:text-muted-foreground aria-disabled:active:bg-transparent",
      },
      size: {
        xs: "h-control-xs gap-1 rounded-sm px-2 text-control-xs has-[>svg]:px-1.5 [&_svg:not([class*='size-'])]:size-[length:var(--control-icon-xs)] [&_[data-slot=spinner]]:size-[length:var(--control-icon-xs)]",
        sm: "h-control-sm rounded-md px-2 text-control-sm [&_svg:not([class*='size-'])]:size-[length:var(--control-icon-sm)] [&_[data-slot=spinner]]:size-[length:var(--control-icon-sm)]",
        default: "h-control-default px-2 py-2 text-control-default",
        lg: "h-control-lg rounded-md px-8 text-control-lg [&_svg:not([class*='size-'])]:size-[length:var(--control-icon-lg)] [&_[data-slot=spinner]]:size-[length:var(--control-icon-lg)]",
        icon: "size-control-default",
        "icon-xs":
          "size-control-xs rounded-sm [&_svg:not([class*='size-'])]:size-[length:var(--control-icon-xs)] [&_[data-slot=spinner]]:size-[length:var(--control-icon-xs)]",
        "icon-sm":
          "size-control-sm [&_svg:not([class*='size-'])]:size-[length:var(--control-icon-sm)] [&_[data-slot=spinner]]:size-[length:var(--control-icon-sm)]",
        "icon-lg":
          "size-control-lg [&_svg:not([class*='size-'])]:size-[length:var(--control-icon-lg)] [&_[data-slot=spinner]]:size-[length:var(--control-icon-lg)]",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  },
);

function isIconSize(size: ButtonProps["size"]): boolean {
  return typeof size === "string" && size.startsWith("icon");
}

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {
  asChild?: boolean;
  /**
   * Transient busy state. Shows Spinner, sets `aria-disabled` / `aria-busy`,
   * blocks activation, and keeps enabled chrome — never the disabled recolor.
   * Mutually exclusive with `disabled` (loading wins). Unsupported with `asChild`.
   */
  loading?: boolean;
}

const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      className,
      variant,
      size,
      asChild = false,
      loading = false,
      disabled,
      children,
      onClick,
      "aria-disabled": ariaDisabledProp,
      "aria-busy": ariaBusyProp,
      ...props
    },
    ref,
  ) => {
    const Comp = asChild ? Slot : "button";
    const isLoading = Boolean(loading) && !asChild;
    if (process.env.NODE_ENV !== "production" && loading && asChild) {
      console.warn("Button: `loading` is unsupported with `asChild` and was ignored.");
    }
    // Busy ≠ unavailable: never apply native disabled while loading.
    const isDisabled = Boolean(disabled) && !isLoading;
    // Loading owns these ARIA attrs only while busy. Idle passes caller values
    // through — never write `undefined` after `{...props}` and wipe them.
    // Decorative leading/trailing SVG glyphs hide via data-loading:[&_svg]:hidden
    // so busy shows one indicator (Spinner); label text stays visible.

    return (
      <Comp
        className={cn(buttonVariants({ variant, size }), className)}
        ref={ref}
        {...props}
        disabled={isDisabled}
        aria-disabled={isLoading ? true : ariaDisabledProp}
        aria-busy={isLoading ? true : ariaBusyProp}
        data-loading={isLoading ? "" : undefined}
        onClick={(event) => {
          if (isLoading) {
            // Also covers HTML implicit form submit (synthetic click on the
            // default submit button) — preventDefault cancels the submit.
            event.preventDefault();
            return;
          }
          onClick?.(event);
        }}
      >
        {isLoading ? (
          <>
            {/* aria-hidden: button owns aria-busy; avoid double status announcements.
                size-* is owned by buttonVariants via [data-slot=spinner]. */}
            <Spinner className="text-current" aria-hidden />
            {isIconSize(size) ? (
              <span className="sr-only">{children}</span>
            ) : (
              children
            )}
          </>
        ) : (
          children
        )}
      </Comp>
    );
  },
);
Button.displayName = "Button";

export { Button, buttonVariants };
