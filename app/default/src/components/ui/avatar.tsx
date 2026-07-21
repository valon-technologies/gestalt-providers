import {
  type ComponentPropsWithoutRef,
  type ElementRef,
  forwardRef,
  useEffect,
  useState,
} from "react";
import { cn } from "@/lib/cn";

const sizeClass = {
  sm: "size-6 text-[0.625rem]",
  default: "size-7 text-xs",
  lg: "size-8 text-sm",
  xl: "size-10 text-sm",
} as const;

const variantClass = {
  solid: "bg-alpha-10",
  outline: "border border-alpha bg-base-white dark:bg-surface",
} as const;

export type AvatarSize = keyof typeof sizeClass;
export type AvatarVariant = keyof typeof variantClass;

export const Avatar = forwardRef<
  HTMLSpanElement,
  ComponentPropsWithoutRef<"span"> & {
    size?: AvatarSize;
    variant?: AvatarVariant;
  }
>(function Avatar(
  { className, size = "default", variant = "solid", ...props },
  ref,
) {
  return (
    <span
      ref={ref}
      data-slot="avatar"
      className={cn(
        "relative flex shrink-0 select-none rounded-full",
        sizeClass[size],
        variantClass[variant],
        className,
      )}
      {...props}
    />
  );
});

export const AvatarImage = forwardRef<
  HTMLImageElement,
  ComponentPropsWithoutRef<"img">
>(function AvatarImage({ className, src, onError, ...props }, ref) {
  const [failed, setFailed] = useState(false);
  useEffect(() => setFailed(false), [src]);
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

export const AvatarFallback = forwardRef<
  HTMLSpanElement,
  ComponentPropsWithoutRef<"span">
>(function AvatarFallback({ className, ...props }, ref) {
  return (
    <span
      ref={ref}
      data-slot="avatar-fallback"
      className={cn(
        "flex size-full items-center justify-center font-medium text-muted",
        className,
      )}
      {...props}
    />
  );
});

export type AvatarProps = ComponentPropsWithoutRef<typeof Avatar>;
export type AvatarImageProps = ComponentPropsWithoutRef<typeof AvatarImage>;
export type AvatarFallbackProps = ComponentPropsWithoutRef<
  typeof AvatarFallback
>;
export type AvatarRef = ElementRef<typeof Avatar>;
