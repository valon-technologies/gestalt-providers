import { type ComponentPropsWithoutRef, forwardRef } from "react";
import { cn } from "@/lib/utils";

const sizeClass = {
  sm: "size-6 text-[0.625rem]",
  default: "size-7 text-xs",
  lg: "size-8 text-sm",
  xl: "size-10 text-sm",
} as const;

const variantClass = {
  solid: "bg-muted",
  outline: "border border-border bg-background",
} as const;

export const Avatar = forwardRef<
  HTMLSpanElement,
  ComponentPropsWithoutRef<"span"> & {
    size?: keyof typeof sizeClass;
    variant?: keyof typeof variantClass;
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

export const AvatarFallback = forwardRef<
  HTMLSpanElement,
  ComponentPropsWithoutRef<"span">
>(function AvatarFallback({ className, ...props }, ref) {
  return (
    <span
      ref={ref}
      data-slot="avatar-fallback"
      className={cn(
        "flex size-full items-center justify-center font-medium text-muted-foreground",
        className,
      )}
      {...props}
    />
  );
});
