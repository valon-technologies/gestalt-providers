/**
 * Gestalt console vendor of Valon Registry `link`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/link.tsx`).
 * Synced from toolshed origin/main — token adaptation only (`@/lib/cn` path).
 */

import * as React from "react";
import { Slot } from "@radix-ui/react-slot";

import { cn } from "@/lib/cn";

// The one Valon link treatment, owned here so every link (Link, BreadcrumbLink,
// Plate markdown links, app-level entity links) stays in sync. Link text uses --link
// (gold-500); the draw underline uses --accent-solid (gold-400).
type LinkUnderlineVariant = "hover" | "always";

const linkColor =
  "text-link outline-none hover:text-link-hover active:text-link-pressed focus-visible:rounded-sm focus-visible:ring-4 focus-visible:ring-accent/55";

const linkUnderlineHover =
  "box-decoration-clone bg-no-repeat bg-[image:linear-gradient(var(--accent-solid),var(--accent-solid))] [background-position:100%_calc(100%_-_0.02em)] [background-size:0%_1.5px] transition-[background-size] duration-[var(--motion-move)] ease-[var(--ease-out-quart)] hover:[background-position:0%_calc(100%_-_0.02em)] hover:[background-size:100%_1.5px] group-hover/link:[background-position:0%_calc(100%_-_0.02em)] group-hover/link:[background-size:100%_1.5px] motion-reduce:transition-none";

const linkUnderlineAlways =
  "box-decoration-clone bg-no-repeat bg-[image:linear-gradient(var(--accent-solid),var(--accent-solid))] [background-position:0%_calc(100%_-_0.02em)] [background-size:100%_1.5px]";

const linkUnderline = linkUnderlineHover;

function linkUnderlineFor(variant: LinkUnderlineVariant = "hover") {
  return variant === "always" ? linkUnderlineAlways : linkUnderlineHover;
}

function linkAnchorClassName(variant: LinkUnderlineVariant = "hover", className?: string) {
  return cn("group/link", linkColor, linkUnderlineFor(variant), className);
}

interface LinkProps extends React.ComponentProps<"a"> {
  asChild?: boolean;
  /** Leading icon (e.g. a lucide glyph or color swatch). When set, the underline
   *  is scoped to the text and the anchor renders as a flex row; `asChild` is ignored. */
  icon?: React.ReactNode;
  /** `hover` draws the accent underline on hover; `always` keeps it visible at rest. */
  underlineVariant?: LinkUnderlineVariant;
}

function Link({
  asChild = false,
  icon,
  underlineVariant = "hover",
  className,
  children,
  ...props
}: LinkProps) {
  const underline = linkUnderlineFor(underlineVariant);

  if (icon) {
    return (
      <a
        data-slot="link"
        className={cn("group/link inline-flex max-w-full items-center gap-1.5", linkColor, className)}
        {...props}
      >
        <span className="shrink-0 text-muted-foreground group-hover/link:text-link [&>svg]:size-3.5">{icon}</span>
        <span className={cn("truncate", underline)}>{children}</span>
      </a>
    );
  }
  const Comp = asChild ? Slot : "a";
  return (
    <Comp data-slot="link" className={cn("group/link", linkColor, underline, className)} {...props}>
      {children}
    </Comp>
  );
}

export { Link, linkAnchorClassName, linkColor, linkUnderline, linkUnderlineFor };
export type { LinkProps, LinkUnderlineVariant };
