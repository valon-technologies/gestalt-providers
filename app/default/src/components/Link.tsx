import * as React from "react";
import { Slot } from "@radix-ui/react-slot";
import { cn } from "@/lib/cn";

/**
 * Gestalt console vendor of Valon Registry `link`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/link.tsx`). Token-adapted only — same
 * API (`asChild`, `icon`, `underlineVariant`). Gestalt maps link ink / underline
 * to `--brand` (no separate `--link` / `--accent-solid` tokens yet).
 *
 * Prefer Registry install when the console consumes Valon registry.
 *
 * @see toolshed/valon-tools/registry/guidelines/links.md
 */

type LinkUnderlineVariant = "hover" | "always";

// Rest + hover snap use brand (Gestalt's AA-safe gold). Color does not
// transition — only underline geometry animates (Registry links guideline).
const linkColor =
  "text-brand outline-none hover:text-brand active:text-brand focus-visible:rounded-sm focus-visible:ring-4 focus-visible:ring-brand/55";

const linkUnderlineHover =
  "box-decoration-clone bg-no-repeat bg-[image:linear-gradient(var(--brand),var(--brand))] [background-position:100%_calc(100%_-_0.02em)] [background-size:0%_1.5px] transition-[background-size] duration-150 ease-out hover:[background-position:0%_calc(100%_-_0.02em)] hover:[background-size:100%_1.5px] group-hover/link:[background-position:0%_calc(100%_-_0.02em)] group-hover/link:[background-size:100%_1.5px] motion-reduce:transition-none";

const linkUnderlineAlways =
  "box-decoration-clone bg-no-repeat bg-[image:linear-gradient(var(--brand),var(--brand))] [background-position:0%_calc(100%_-_0.02em)] [background-size:100%_1.5px]";

const linkUnderline = linkUnderlineHover;

function linkUnderlineFor(variant: LinkUnderlineVariant = "hover") {
  return variant === "always" ? linkUnderlineAlways : linkUnderlineHover;
}

function linkAnchorClassName(
  variant: LinkUnderlineVariant = "hover",
  className?: string,
) {
  return cn("group/link", linkColor, linkUnderlineFor(variant), className);
}

interface LinkProps extends React.ComponentProps<"a"> {
  asChild?: boolean;
  /** Leading icon. When set, underline is scoped to the text; `asChild` ignored. */
  icon?: React.ReactNode;
  /** `hover` draws underline on hover; `always` keeps it at rest. */
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
        className={cn(
          "group/link inline-flex max-w-full items-center gap-1.5",
          linkColor,
          className,
        )}
        {...props}
      >
        <span className="shrink-0 text-muted group-hover/link:text-brand [&>svg]:size-3.5">
          {icon}
        </span>
        <span className={cn("truncate", underline)}>{children}</span>
      </a>
    );
  }
  const Comp = asChild ? Slot : "a";
  return (
    <Comp
      data-slot="link"
      className={cn("group/link", linkColor, underline, className)}
      {...props}
    >
      {children}
    </Comp>
  );
}

export { Link, linkAnchorClassName, linkColor, linkUnderline, linkUnderlineFor };
export type { LinkProps, LinkUnderlineVariant };
