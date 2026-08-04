/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

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
// directly (no translate nudge) — see Registry guidelines/avatar.md. Initials are
// centered with size-matched `leading-*` (not flex items-center) so that baseline
// still participates. Sized to the metadata-row band (sm/default/lg = 24/28/32px)
// plus xl (40px) for account / profile chrome; initials font scales with the box.
const avatarVariants = cva(
  // inline-flex + leading matched to size: initials stay vertically centered
  // via line-height while the text baseline still participates in a parent
  // items-baseline row (avatar.md). Flex+items-center on the fallback would
  // synthesize the baseline to the bottom edge and break that contract.
  "relative inline-flex aspect-square shrink-0 select-none rounded-full",  {
    variants: {
      size: {
        sm: "size-6 text-[0.625rem] leading-6",
        default: "size-7 text-xs leading-7",
        lg: "size-8 text-sm leading-8",
        /** Account / profile chip — larger than metadata-row lg. */
        xl: "size-10 text-sm leading-10",
      },
      // outline: Notion-style low-prominence chip — a hairline ring on the page
      // surface instead of a filled disc, so the initials read more clearly.
      // solid: resting --muted-strong fill (achromatic quiet ramp L-step).
      // --muted ≡ --neutral-hover, so bg-muted vanishes on Neutral menu/list
      // row hover — muted-strong keeps a readable true-grey disc.
      variant: {
        solid: "bg-muted-strong",
        outline: "border border-border bg-background",
      },
    },
    defaultVariants: { size: "default", variant: "solid" },
  },
);

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
      // Block + inherited leading (from Root size) centers initials without
      // flex items-center, which would synthesize baseline to the box bottom.
      "block size-full text-center font-medium text-muted-foreground",
      className,
    )}
    {...props}
  />
));
AvatarFallback.displayName = "AvatarFallback";

export { Avatar, AvatarImage, AvatarFallback, avatarVariants };
