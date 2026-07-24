"use client";


/**
 * Gestalt console vendor of Valon Registry `section-header`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/section-header.tsx`).
 * Synced from toolshed origin/main — token adaptation only (`@/lib/cn` path).
 * Do not restyle chrome at call sites; change Registry first.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

// In-page region chrome: title · description · actions *outside* a Card / above
// a card grid. Sibling to PageHeader (document h1). Default title is h2 — safe
// to repeat. Spec: guidelines/section-header.md · RES-20260721-008.
// Melange Heading only — Season stays on PageHeader display tiers / StatValue.
// Description size + content gap track the title tier (same Colors ratio idea).
// Content owns the scale; Title/Description read it (no layout-effect sync).

const sectionHeaderVariants = cva("flex w-full gap-x-4 gap-y-3", {
  variants: {
    align: {
      // Baseline (not end): actions sit on the title line when a description stacks
      // under it — items-end drops the button to the description.
      between: "flex-col sm:flex-row sm:items-baseline sm:justify-between",
      center:
        "flex-col items-center text-center [&_[data-slot=section-header-content]]:items-center [&_[data-slot=section-header-actions]]:justify-center",
    },
  },
  defaultVariants: {
    align: "between",
  },
});

interface SectionHeaderProps
  extends React.ComponentProps<"div">,
    VariantProps<typeof sectionHeaderVariants> {}

function SectionHeader({ className, align, ...props }: SectionHeaderProps) {
  return (
    <div
      data-slot="section-header"
      className={cn(sectionHeaderVariants({ align }), className)}
      {...props}
    />
  );
}

const sectionHeaderTitleVariants = cva(
  "font-sans font-normal text-balance text-foreground",
  {
    variants: {
      size: {
        /** Heading SM — Melange; dense rails / nested panels. */
        sm: "text-heading-sm",
        /** Heading MD — Melange; everyday in-page section (Base Palette scale). */
        default: "text-heading-md",
        /** Heading LG — Melange; heavier section emphasis. */
        lg: "text-heading-lg tracking-heading",
      },
    },
    defaultVariants: {
      size: "default",
    },
  },
);

type SectionHeaderSize = NonNullable<VariantProps<typeof sectionHeaderTitleVariants>["size"]>;

const SectionHeaderScaleContext = React.createContext<SectionHeaderSize>("default");

const sectionHeaderContentVariants = cva("flex min-w-0 flex-col", {
  variants: {
    size: {
      sm: "gap-1.5",
      default: "gap-2",
      lg: "gap-2.5",
    },
  },
  defaultVariants: {
    size: "default",
  },
});

interface SectionHeaderContentProps
  extends React.ComponentProps<"div">,
    VariantProps<typeof sectionHeaderContentVariants> {}

function SectionHeaderContent({
  className,
  size = "default",
  ...props
}: SectionHeaderContentProps) {
  const resolved = size ?? "default";
  return (
    <SectionHeaderScaleContext.Provider value={resolved}>
      <div
        data-slot="section-header-content"
        data-size={resolved}
        className={cn(sectionHeaderContentVariants({ size: resolved }), className)}
        {...props}
      />
    </SectionHeaderScaleContext.Provider>
  );
}

type SectionHeaderTitleTag = "h2" | "h3";

interface SectionHeaderTitleProps
  extends Omit<React.ComponentProps<"h2">, "as">,
    VariantProps<typeof sectionHeaderTitleVariants> {
  /** Heading level. Default `h2` — never `h1` (that is PageHeader). */
  as?: SectionHeaderTitleTag;
}

function SectionHeaderTitle({
  className,
  size: sizeProp,
  as: Comp = "h2",
  ...props
}: SectionHeaderTitleProps) {
  const scale = React.useContext(SectionHeaderScaleContext);
  const size = sizeProp ?? scale;

  return (
    <Comp
      data-slot="section-header-title"
      data-size={size}
      className={cn(sectionHeaderTitleVariants({ size }), className)}
      {...props}
    />
  );
}

const sectionHeaderDescriptionVariants = cva(
  "max-w-xl text-balance font-normal text-muted-foreground",
  {
    variants: {
      size: {
        sm: "text-xs",
        default: "text-sm",
        lg: "text-base",
      },
    },
    defaultVariants: {
      size: "default",
    },
  },
);

/** Secondary ink via `--muted-foreground` (theme ink-alpha at 60%). */
function SectionHeaderDescription({ className, ...props }: React.ComponentProps<"p">) {
  const size = React.useContext(SectionHeaderScaleContext);
  return (
    <p
      data-slot="section-header-description"
      data-size={size}
      className={cn(sectionHeaderDescriptionVariants({ size }), className)}
      {...props}
    />
  );
}

function SectionHeaderActions({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="section-header-actions"
      className={cn("flex shrink-0 items-center gap-2", className)}
      {...props}
    />
  );
}

export {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderTitle,
  SectionHeaderDescription,
  SectionHeaderActions,
  sectionHeaderVariants,
  sectionHeaderContentVariants,
  sectionHeaderTitleVariants,
  sectionHeaderDescriptionVariants,
};
