"use client";


/**
 * Gestalt console vendor of Valon Registry `page-header`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/page-header.tsx`).
 * Synced from toolshed origin/main — token adaptation only (`@/lib/cn` path).
 * Do not restyle chrome at call sites; change Registry first.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

const pageHeaderVariants = cva("flex w-full gap-x-4 gap-y-3", {
  variants: {
    align: {
      between: "flex-col sm:flex-row sm:items-end sm:justify-between",
      center:
        "flex-col items-center text-center [&_[data-slot=page-header-content]]:items-center [&_[data-slot=page-header-actions]]:justify-center",
    },
  },
  defaultVariants: {
    align: "between",
  },
});

interface PageHeaderProps
  extends React.ComponentProps<"header">,
    VariantProps<typeof pageHeaderVariants> {}

function PageHeader({ className, align, ...props }: PageHeaderProps) {
  return (
    <header
      data-slot="page-header"
      className={cn(pageHeaderVariants({ align }), className)}
      {...props}
    />
  );
}

// Product roles map onto the brand type scale (theme tokens from valon.ai/style).
// Everyday defaults stay Melange Heading; Season (`font-display`) is opt-in on
// lg / xl / entity at Display SM+ (44px) only. Display compounds pin `font-normal`
// so Season stays Regular — Melange `entity` keeps semibold when display is off.
const pageHeaderTitleVariants = cva(
  "font-sans font-normal text-balance text-foreground",
  {
    variants: {
      size: {
        /** Heading MD — Melange; dense nested pages. */
        sm: "text-heading-md",
        /** Heading LG — Melange; everyday app page title. */
        default: "text-heading-lg tracking-heading",
        /** Display SM — Season; intentional display (Colors-block scale). */
        lg: "text-display-sm tracking-display",
        /** Display XL — Season; landing / hero. */
        xl: "text-display-xl tracking-display-tight",
        /** Detail record title — Display SM; Melange semibold when display off. */
        entity: "text-display-sm tracking-display font-semibold",
      },
      display: {
        true: "",
        false: "",
      },
    },
    compoundVariants: [
      { size: "lg", display: true, class: "font-display font-normal" },
      { size: "xl", display: true, class: "font-display font-normal" },
      { size: "entity", display: true, class: "font-display font-normal" },
    ],
    defaultVariants: {
      size: "default",
      display: true,
    },
  },
);

type PageHeaderSize = NonNullable<VariantProps<typeof pageHeaderTitleVariants>["size"]>;

/** Content owns the title→description→gap scale. Title/Description read it. */
const PageHeaderScaleContext = React.createContext<PageHeaderSize>("default");

// Title→description rhythm anchored on valon.ai/style Colors (44px → 18px / 12px gap).
// Larger title tiers step description and gap up together.
const pageHeaderContentVariants = cva("flex min-w-0 flex-col", {
  variants: {
    size: {
      sm: "gap-1.5",
      default: "gap-2",
      lg: "gap-3",
      xl: "gap-5",
      entity: "gap-3",
    },
  },
  defaultVariants: {
    size: "default",
  },
});

interface PageHeaderContentProps
  extends React.ComponentProps<"div">,
    VariantProps<typeof pageHeaderContentVariants> {}

function PageHeaderContent({ className, size = "default", ...props }: PageHeaderContentProps) {
  const resolved = size ?? "default";
  return (
    <PageHeaderScaleContext.Provider value={resolved}>
      <div
        data-slot="page-header-content"
        data-size={resolved}
        className={cn(pageHeaderContentVariants({ size: resolved }), className)}
        {...props}
      />
    </PageHeaderScaleContext.Provider>
  );
}

interface PageHeaderTitleProps
  extends Omit<React.ComponentProps<"h1">, "onClick">,
    VariantProps<typeof pageHeaderTitleVariants> {
  /** When set, the title text is an in-header link (SPA or full navigation). */
  href?: string;
  /** When set (and `href` is absent), the title text is an in-header button. */
  onNavigate?: () => void;
}

const pageHeaderTitleInteractiveClassName =
  "cursor-pointer border-0 bg-transparent p-0 text-left font-[inherit] text-inherit no-underline hover:text-inherit focus-ring rounded-sm";

function PageHeaderTitle({
  className,
  size: sizeProp,
  display,
  href,
  onNavigate,
  children,
  ...props
}: PageHeaderTitleProps) {
  const scale = React.useContext(PageHeaderScaleContext);
  const size = sizeProp ?? scale;

  let body = children;
  if (href) {
    body = (
      <a href={href} className={pageHeaderTitleInteractiveClassName}>
        {children}
      </a>
    );
  } else if (onNavigate) {
    body = (
      <button type="button" className={pageHeaderTitleInteractiveClassName} onClick={onNavigate}>
        {children}
      </button>
    );
  }

  return (
    <h1
      data-slot="page-header-title"
      data-size={size}
      className={cn(pageHeaderTitleVariants({ size, display }), className)}
      {...props}
    >
      {body}
    </h1>
  );
}

const pageHeaderDescriptionVariants = cva(
  // Pin font-normal so app body weight (e.g. Instrument Sans 435) cannot
  // thicken Registry description copy — same contract as FieldDescription.
  "max-w-xl text-balance font-normal text-muted-foreground",
  {
    variants: {
      size: {
        sm: "text-xs",
        default: "text-sm",
        /** Colors block: Body LG under Display SM. */
        lg: "text-body-lg",
        xl: "text-heading-lg",
        entity: "text-body-lg",
      },
    },
    defaultVariants: {
      size: "default",
    },
  },
);

/** Secondary ink via `--muted-foreground` (theme ink-alpha at 60%). */
function PageHeaderDescription({ className, ...props }: React.ComponentProps<"p">) {
  const size = React.useContext(PageHeaderScaleContext);
  return (
    <p
      data-slot="page-header-description"
      data-size={size}
      className={cn(pageHeaderDescriptionVariants({ size }), className)}
      {...props}
    />
  );
}

function PageHeaderActions({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="page-header-actions"
      className={cn("flex shrink-0 items-center gap-2", className)}
      {...props}
    />
  );
}

export {
  PageHeader,
  PageHeaderContent,
  PageHeaderTitle,
  PageHeaderDescription,
  PageHeaderActions,
  pageHeaderVariants,
  pageHeaderContentVariants,
  pageHeaderTitleVariants,
  pageHeaderDescriptionVariants,
};
