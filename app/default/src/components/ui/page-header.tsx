import {
  type ComponentProps,
  type HTMLAttributes,
} from "react";
import { cn } from "@/lib/cn";

/**
 * Gestalt console vendor of Valon Registry `page-header`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/page-header.tsx`). Token-adapted only —
 * same public API (`align`, title `size` / `display`). Prefer Registry install
 * when the console consumes Valon registry.
 *
 * Eyebrow is a separate primitive — compose `<Eyebrow>` above `PageHeaderTitle`
 * (default muted tone). Do not fold it into this component.
 *
 * @see toolshed/valon-tools/registry/guidelines/page-header.md
 * @see toolshed/valon-tools/registry/guidelines/eyebrow.md
 */

type PageHeaderAlign = "between" | "center";

type PageHeaderTitleSize = "sm" | "default" | "lg" | "xl" | "entity";

const alignClass: Record<PageHeaderAlign, string> = {
  between: "flex-col sm:flex-row sm:items-end sm:justify-between",
  center:
    "flex-col items-center text-center [&_[data-slot=page-header-content]]:items-center [&_[data-slot=page-header-actions]]:justify-center",
};

const titleSizeClass: Record<PageHeaderTitleSize, string> = {
  sm: "text-xl",
  default: "text-2xl",
  lg: "text-4xl",
  xl: "text-5xl",
  /** Detail record title above a document body — above typeset H1. */
  entity: "text-4xl font-semibold leading-tight",
};

export type PageHeaderProps = HTMLAttributes<HTMLElement> & {
  align?: PageHeaderAlign;
};

export function PageHeader({
  className,
  align = "between",
  ...props
}: PageHeaderProps) {
  return (
    <header
      data-slot="page-header"
      className={cn("flex w-full gap-x-4 gap-y-3", alignClass[align], className)}
      {...props}
    />
  );
}

export function PageHeaderContent({
  className,
  ...props
}: HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      data-slot="page-header-content"
      className={cn("flex min-w-0 flex-col gap-1.5", className)}
      {...props}
    />
  );
}

export type PageHeaderTitleProps = ComponentProps<"h1"> & {
  size?: PageHeaderTitleSize;
  /** Display face at lg/xl/entity — maps to console `font-heading`. */
  display?: boolean;
};

export function PageHeaderTitle({
  className,
  size = "default",
  display = true,
  ...props
}: PageHeaderTitleProps) {
  const useDisplay =
    display && (size === "lg" || size === "xl" || size === "entity");

  return (
    <h1
      data-slot="page-header-title"
      className={cn(
        // Registry: font-sans tracking-tight text-balance text-foreground
        "font-normal tracking-tight text-balance text-primary",
        titleSizeClass[size],
        // Registry gates font-display to lg/xl/entity; console heading face.
        useDisplay ? "font-heading font-normal" : "font-sans",
        className,
      )}
      {...props}
    />
  );
}

export function PageHeaderDescription({
  className,
  ...props
}: ComponentProps<"p">) {
  return (
    <p
      data-slot="page-header-description"
      className={cn("text-pretty text-sm text-muted", className)}
      {...props}
    />
  );
}

export function PageHeaderActions({
  className,
  ...props
}: HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      data-slot="page-header-actions"
      className={cn("flex shrink-0 items-center gap-2", className)}
      {...props}
    />
  );
}
