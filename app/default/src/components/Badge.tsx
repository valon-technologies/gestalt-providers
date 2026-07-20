import type { HTMLAttributes } from "react";
import { cn } from "@/lib/cn";

/**
 * Gestalt console vendor of Valon Registry `badge`.
 * Ownership: Registry is canonical (`valon-tools/.../badge.tsx`). Token-adapted
 * only — same `variant` / `size` contract. Prefer Registry install when the
 * console adopts Valon tokens.
 *
 * @see https://github.com/valon-technologies/toolshed (registry badge)
 */

type BadgeVariant =
  | "default"
  | "secondary"
  | "muted"
  | "outline"
  | "success"
  | "warning"
  | "destructive";

type BadgeSize = "sm" | "default";

const variantClass: Record<BadgeVariant, string> = {
  default: "bg-base-950 text-white dark:bg-base-100 dark:text-base-950",
  secondary: "bg-alpha-10 text-primary",
  muted: "bg-alpha-5 text-muted",
  outline: "border border-alpha text-primary",
  success:
    "bg-grove-100 text-grove-700 dark:bg-grove-700/20 dark:text-grove-200",
  warning: "bg-gold-100 text-gold-700 dark:bg-gold-700/20 dark:text-gold-200",
  destructive: "bg-ember-500/15 text-ember-700 dark:text-ember-500",
};

const sizeClass: Record<BadgeSize, string> = {
  sm: "px-1.5 py-0.5",
  default: "px-2 py-0.5",
};

export function Badge({
  className,
  variant = "default",
  size = "default",
  ...props
}: HTMLAttributes<HTMLSpanElement> & {
  variant?: BadgeVariant;
  size?: BadgeSize;
}) {
  return (
    <span
      data-slot="badge"
      data-variant={variant}
      className={cn(
        "inline-flex items-center justify-center gap-1 whitespace-nowrap rounded-full text-xs font-medium",
        variantClass[variant],
        sizeClass[size],
        className,
      )}
      {...props}
    />
  );
}
