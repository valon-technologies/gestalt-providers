/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 * Registry `description-list`.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cardVariants } from "@/components/ui/card";
import { cn } from "@/lib/cn";

// A label/value definition list (`<dl>`). Registry name for the key-value
// "data list" pattern (Radix Themes DataList / HTML description list) — prefer
// this over Field for read-only metadata (fields.md).
//
// `row` lays each pair as a fixed term column + value; `stacked` puts the value
// under the term (narrow panes, mobile). Term column width is a CSS var so a
// consumer can widen it once on the list. Hairline row separators are opt-out
// via `divided`. `surface="outline"` composes Card outline fill/border (Stat
// pattern) for inspector / docs panels without a hand-rolled bordered table.
// `density` owns compactness on the list (Tree / SelectionCheck pattern):
// row padding, and for `row` the term-to-value gutter (gap-3 default, gap-2
// condensed). Stacked keeps its own small gap between term and value.
const TERM_WIDTH_DEFAULT = "7rem";

const descriptionListVariants = cva("min-w-0 text-sm", {
  variants: {
    variant: {
      row: "[&_[data-slot=description-item]]:flex [&_[data-slot=description-item]]:items-baseline [&_[data-slot=description-term]]:w-[var(--dl-term-width)] [&_[data-slot=description-term]]:shrink-0 [&_[data-slot=description-details]]:min-w-0 [&_[data-slot=description-details]]:flex-1",
      stacked:
        "[&_[data-slot=description-item]]:flex [&_[data-slot=description-item]]:flex-col [&_[data-slot=description-item]]:gap-0.5",
    },
    divided: {
      true: "divide-y divide-border/60",
      false: "",
    },
    density: {
      // Roomy default — readable KV panels and outline cards.
      default: "[&_[data-slot=description-item]]:py-3",
      // Tight inspector / dialog metadata (prior plain default).
      condensed: "[&_[data-slot=description-item]]:py-1.5",
    },
    surface: {
      // Flush in a parent pane — no own border/fill.
      plain: "",
      // Compose Card outline tokens; override radius to section/Alert `rounded-lg`
      // (not Card's `rounded-xl`). Horizontal pad on rows (not the shell) so
      // `divide-y` hairlines run edge-to-edge — vertical rhythm stays on `density`.
      outline: cn(
        cardVariants({ variant: "outline" }),
        "overflow-hidden rounded-lg [&_[data-slot=description-item]]:px-4",
      ),
    },
  },
  compoundVariants: [
    {
      variant: "row",
      density: "default",
      class: "[&_[data-slot=description-item]]:gap-3",
    },
    {
      variant: "row",
      density: "condensed",
      class: "[&_[data-slot=description-item]]:gap-2",
    },
  ],
  defaultVariants: {
    variant: "row",
    divided: true,
    density: "default",
    surface: "plain",
  },
});

interface DescriptionListProps
  extends React.ComponentProps<"dl">,
    VariantProps<typeof descriptionListVariants> {
  termWidth?: string;
}

function DescriptionList({
  className,
  variant,
  divided,
  density,
  surface,
  termWidth = TERM_WIDTH_DEFAULT,
  style,
  ...props
}: DescriptionListProps) {
  return (
    <dl
      data-slot="description-list"
      data-variant={variant ?? "row"}
      data-density={density ?? "default"}
      data-surface={surface ?? "plain"}
      className={cn(
        descriptionListVariants({ variant, divided, density, surface }),
        className,
      )}
      style={{ "--dl-term-width": termWidth, ...style } as React.CSSProperties}
      {...props}
    />
  );
}

function DescriptionItem({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="description-item"
      className={className}
      {...props}
    />
  );
}

function DescriptionTerm({ className, ...props }: React.ComponentProps<"dt">) {
  return (
    <dt
      data-slot="description-term"
      className={cn(
        "font-display text-sm italic tracking-wide text-muted-foreground",
        className,
      )}
      {...props}
    />
  );
}

// text-pretty: avoid orphans on wrapping prose values (docs MCP tables, etc.).
const descriptionDetailsVariants = cva("m-0 break-words text-pretty", {
  variants: {
    mono: {
      true: "font-mono",
      false: "",
    },
    // Status tint for value cells (e.g. a span's OK/ERROR status), on the shared
    // semantic STATE tokens rather than a call-site color override.
    tone: {
      default: "text-foreground",
      success: "text-success-ink",
      warning: "text-warning-ink",
      error: "text-error-ink",
    },
  },
  defaultVariants: {
    mono: false,
    tone: "default",
  },
});

interface DescriptionDetailsProps
  extends React.ComponentProps<"dd">,
    VariantProps<typeof descriptionDetailsVariants> {}

function DescriptionDetails({
  className,
  mono,
  tone,
  ...props
}: DescriptionDetailsProps) {
  return (
    <dd
      data-slot="description-details"
      className={cn(descriptionDetailsVariants({ mono, tone }), className)}
      {...props}
    />
  );
}

export {
  DescriptionList,
  DescriptionItem,
  DescriptionTerm,
  DescriptionDetails,
  descriptionListVariants,
};
