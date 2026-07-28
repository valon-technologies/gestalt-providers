"use client";

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

/**
 * Gestalt console vendor of Valon Registry `avatar`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/avatar.tsx`). Local keeps an extra `xl`
 * size for the nav user chip; recipes otherwise match Registry muted/border tokens.
 */

// Dependency-free: the only consumer renders initials (no image URL from the
// backend), so there is nothing to gain from a load-status library. The image,
// when present, is absolutely overlaid on the fallback and self-clips to the
// circle (rounded-full on the img) — it paints on top once it loads and removes
// itself on error, so the fallback shows through with no shared loaded/error
// state. If real image URLs ever need a no-flash/delay gate, revisit (e.g.
// @radix-ui/react-avatar) behind this API.
//
// NOTE the Root deliberately has NO overflow-hidden: a scroll container
// synthesizes its baseline from its bottom edge, which would float the circle
// above adjacent text. Without it, the Root exposes its inner initials' baseline,
// so a parent `items-baseline` row lines the initials up with neighbouring text
// directly (no translate nudge). Sized to the metadata-row band
// (sm/default/lg = 24/28/32px); initials font scales with the box. Console `xl`
// = 40px for the nav account chip.
const avatarVariants = cva("relative flex shrink-0 select-none rounded-full", {
  variants: {
    size: {
      sm: "size-6 text-[0.625rem]",
      default: "size-7 text-xs",
      lg: "size-8 text-sm",
      xl: "size-10 text-sm",
    },
    // outline: Notion-style low-prominence chip — a hairline ring on the page
    // surface instead of a filled disc, so the initials read more clearly.
    variant: {
      solid: "bg-muted",
      outline: "border border-border bg-background",
    },
  },
  defaultVariants: { size: "default", variant: "solid" },
});

const Avatar = React.forwardRef<
  HTMLSpanElement,
  React.ComponentPropsWithoutRef<"span"> & VariantProps<typeof avatarVariants>
>(({ className, size, variant, ...props }, ref) => (
  <span
    ref={ref}
    data-slot="avatar"
    className={cn(avatarVariants({ size, variant, className }))}
    {...props}
  />
));
Avatar.displayName = "Avatar";

const AvatarImage = React.forwardRef<
  HTMLImageElement,
  React.ComponentPropsWithoutRef<"img">
>(({ className, src, onError, ...props }, ref) => {
  const [failed, setFailed] = React.useState(false);
  React.useEffect(() => setFailed(false), [src]);
  if (!src || failed) return null;
  return (
    <img
      ref={ref}
      data-slot="avatar-image"
      src={src}
      className={cn(
        "absolute inset-0 size-full rounded-full object-cover",
        className,
      )}
      onError={(event) => {
        setFailed(true);
        onError?.(event);
      }}
      {...props}
    />
  );
});
AvatarImage.displayName = "AvatarImage";

const AvatarFallback = React.forwardRef<
  HTMLSpanElement,
  React.ComponentPropsWithoutRef<"span">
>(({ className, ...props }, ref) => (
  <span
    ref={ref}
    data-slot="avatar-fallback"
    className={cn(
      "flex size-full items-center justify-center font-medium text-muted-foreground",
      className,
    )}
    {...props}
  />
));
AvatarFallback.displayName = "AvatarFallback";

export { Avatar, AvatarImage, AvatarFallback, avatarVariants };
export type AvatarProps = React.ComponentPropsWithoutRef<typeof Avatar>;
export type AvatarImageProps = React.ComponentPropsWithoutRef<typeof AvatarImage>;
export type AvatarFallbackProps = React.ComponentPropsWithoutRef<
  typeof AvatarFallback
>;
export type AvatarRef = React.ElementRef<typeof Avatar>;
export type AvatarSize = NonNullable<VariantProps<typeof avatarVariants>["size"]>;
export type AvatarVariant = NonNullable<
  VariantProps<typeof avatarVariants>["variant"]
>;
