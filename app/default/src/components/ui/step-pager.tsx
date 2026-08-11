/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 * Registry `step-pager` (toolshed#4190).
 */

import * as React from "react";
import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";
import { ChevronLeft, ChevronRight } from "lucide-react";

import { cardVariants } from "@/components/ui/card";
import { Eyebrow } from "@/components/ui/eyebrow";
import { cn } from "@/lib/cn";

// StepPager — previous/next destination cards for ordered journeys (docs
// chapter edges, Build wizard steps). Not Pagination (numbered dataset paging)
// and not Stepper (in-flow process rail). Research: RES-20260810-002;
// copy-review: StepPager over DocPager/Pagination/Footer.
//
// Compound: StepPager / StepPagerPrevious / StepPagerNext / StepPagerStartSpacer.
// Use asChild so the card can be a <button>, router <Link>, or <a>.
// Surfaces: solid/outline compose Card tokens (rounded-lg override); ghost is
// quiet destination chrome (Neutral wash — not Button ghostQuietChrome).
// Disabled is a RECOLOR (disabled-states.md), never opacity dim. Links that
// cannot use native disabled use aria-disabled + tabindex=-1 at the call site.

const stepPagerDisabledChrome =
  "disabled:cursor-not-allowed disabled:border-border disabled:bg-disabled disabled:text-disabled-foreground disabled:shadow-none disabled:hover:bg-disabled disabled:active:bg-disabled disabled:[&_[data-slot=eyebrow]]:text-disabled-foreground disabled:[&_svg]:text-disabled-foreground aria-disabled:cursor-not-allowed aria-disabled:border-border aria-disabled:bg-disabled aria-disabled:text-disabled-foreground aria-disabled:shadow-none aria-disabled:hover:bg-disabled aria-disabled:active:bg-disabled aria-disabled:[&_[data-slot=eyebrow]]:text-disabled-foreground aria-disabled:[&_svg]:text-disabled-foreground";

const stepPagerLinkVariants = cva(
  // rounded-lg matches Alert / section card shells (not Card's rounded-xl).
  cn(
    "group flex w-fit max-w-xs flex-col text-left text-foreground transition-[background-color,border-color,color] duration-hover-out ease-out-quart hover:duration-hover-in focus-ring",
    stepPagerDisabledChrome,
  ),
  {
    variants: {
      direction: {
        previous: "",
        next: "ms-auto items-end",
      },
      // Padding lives with the chrome so surfaces stay self-contained.
      // Hover ladders follow interactive cards.md (solid → Neutral dark; outline → secondary).
      variant: {
        solid: cn(
          cardVariants({ variant: "solid" }),
          "rounded-lg px-5 py-5 hover:bg-neutral-dark-hover active:bg-neutral-dark-pressed",
        ),
        outline: cn(
          cardVariants({ variant: "outline" }),
          "rounded-lg px-5 py-5 hover:bg-secondary active:bg-neutral-pressed",
        ),
        // Transparent quiet chrome — same padding as filled cards; Neutral wash on hover
        // (not Button's on-color scrim; destination titles keep foreground ink).
        ghost:
          "rounded-lg bg-transparent px-5 py-5 hover:bg-neutral-hover active:bg-neutral-pressed",
      },
    },
    defaultVariants: {
      direction: "previous",
      variant: "solid",
    },
  },
);

type StepPagerVariant = NonNullable<
  VariantProps<typeof stepPagerLinkVariants>["variant"]
>;

const StepPagerVariantContext = React.createContext<StepPagerVariant>("solid");

type StepPagerProps = Omit<React.ComponentProps<"nav">, "aria-label"> & {
  variant?: StepPagerVariant;
  /** Required landmark name (NavList contract) — pages often already have app nav + TOC. */
  "aria-label": string;
};

function StepPager({
  className,
  variant = "solid",
  ...props
}: StepPagerProps) {
  return (
    <StepPagerVariantContext.Provider value={variant ?? "solid"}>
      <nav
        data-slot="step-pager"
        data-variant={variant ?? "solid"}
        className={cn(
          "flex flex-wrap items-stretch justify-between gap-3 border-t border-alpha pt-6",
          className,
        )}
        {...props}
      />
    </StepPagerVariantContext.Provider>
  );
}

/** Keeps the Next card end-aligned when there is no Previous destination. */
function StepPagerStartSpacer({
  className,
  ...props
}: React.ComponentProps<"span">) {
  return (
    <span
      data-slot="step-pager-start-spacer"
      className={cn("hidden sm:block", className)}
      aria-hidden
      {...props}
    />
  );
}

type StepPagerLinkProps = Omit<React.ComponentProps<"a">, "children" | "title"> &
  VariantProps<typeof stepPagerLinkVariants> & {
    title: string;
    asChild?: boolean;
    children?: React.ReactElement;
    /** Override the default Previous/Next eyebrow (real case; Eyebrow uppercases). */
    label?: string;
  };

function StepPagerLink({
  className,
  direction = "previous",
  variant: variantProp,
  title,
  label,
  asChild = false,
  children,
  ...props
}: StepPagerLinkProps) {
  const variantFromContext = React.useContext(StepPagerVariantContext);
  const variant = variantProp ?? variantFromContext;
  const isNext = direction === "next";
  const eyebrow = label ?? (isNext ? "Next" : "Previous");
  const ChevronIcon = isNext ? ChevronRight : ChevronLeft;

  const content = (
    <span
      className={cn(
        "grid w-full items-start gap-x-1.5 gap-y-2.5 font-heading text-xl font-normal leading-tight",
        isNext
          ? "grid-cols-[minmax(0,1fr)_auto]"
          : "grid-cols-[auto_minmax(0,1fr)]",
      )}
    >
      <Eyebrow
        size="sm"
        className={isNext ? "col-start-1 row-start-1" : "col-start-2 row-start-1"}
      >
        {eyebrow}
      </Eyebrow>
      <ChevronIcon
        aria-hidden
        strokeWidth={2.25}
        className={cn(
          // Size against the title's text-xl; optically center in the first line
          // box (cap → descender) so wrapping titles don't drag the caret down.
          "mt-[calc((1lh-0.9em)/2)] size-[0.9em] shrink-0 text-muted-foreground transition-[color] duration-hover-out ease-out-quart group-hover:text-foreground group-hover:duration-hover-in",
          isNext ? "col-start-2 row-start-2" : "col-start-1 row-start-2",
        )}
      />
      <span
        className={cn(
          "min-w-0 text-pretty",
          isNext ? "col-start-1 row-start-2" : "col-start-2 row-start-2",
        )}
      >
        {title}
      </span>
    </span>
  );

  const shared = {
    "data-slot": "step-pager-link" as const,
    "data-direction": direction ?? "previous",
    "data-variant": variant ?? "solid",
    className: cn(stepPagerLinkVariants({ direction, variant }), className),
    ...props,
  };

  if (asChild) {
    if (!React.isValidElement(children)) {
      throw new Error("StepPagerLink asChild requires a single React element child.");
    }
    return (
      <Slot {...shared}>
        {React.cloneElement(children, undefined, content)}
      </Slot>
    );
  }

  return <a {...shared}>{content}</a>;
}

function StepPagerPrevious({
  direction: _direction,
  label,
  ...props
}: Omit<StepPagerLinkProps, "direction"> & { direction?: never }) {
  return (
    <StepPagerLink
      data-slot="step-pager-previous"
      direction="previous"
      label={label}
      {...props}
    />
  );
}

function StepPagerNext({
  direction: _direction,
  label,
  ...props
}: Omit<StepPagerLinkProps, "direction"> & { direction?: never }) {
  return (
    <StepPagerLink
      data-slot="step-pager-next"
      direction="next"
      label={label}
      {...props}
    />
  );
}

export {
  StepPager,
  StepPagerPrevious,
  StepPagerNext,
  StepPagerLink,
  StepPagerStartSpacer,
  stepPagerLinkVariants,
};
