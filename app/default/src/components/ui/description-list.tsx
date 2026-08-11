/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 * Registry `description-list` (toolshed#4188 outline surface).
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
const TERM_WIDTH_DEFAULT = "7rem";

const descriptionListVariants = cva("min-w-0 text-sm", {
  variants: {
    variant: {
      row: "[&_[data-slot=description-item]]:flex [&_[data-slot=description-item]]:items-baseline [&_[data-slot=description-item]]:gap-3 [&_[data-slot=description-term]]:w-[var(--dl-term-width)] [&_[data-slot=description-term]]:shrink-0 [&_[data-slot=description-details]]:min-w-0 [&_[data-slot=description-details]]:flex-1",
      stacked:
        "[&_[data-slot=description-item]]:flex [&_[data-slot=description-item]]:flex-col [&_[data-slot=description-item]]:gap-0.5",
    },
    divided: {
      true: "divide-y divide-border/60",
      false: "",
    },
    surface: {
      // Flush in a parent pane — no own border/fill.
      plain: "",
      // Compose Card outline tokens; override radius to section/Alert `rounded-lg`
      // (not Card's `rounded-xl`). Pad rows (not the shell) so `divide-y`
      // hairlines run edge-to-edge — same ownership as Item / StatusList.
      outline: cn(
        cardVariants({ variant: "outline" }),
        "overflow-hidden rounded-lg [&_[data-slot=description-item]]:px-4 [&_[data-slot=description-item]]:py-3",
      ),
    },
  },
  defaultVariants: {
    variant: "row",
    divided: true,
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
  surface,
  termWidth = TERM_WIDTH_DEFAULT,
  style,
  ...props
}: DescriptionListProps) {
  return (
    <dl
      data-slot="description-list"
      data-variant={variant ?? "row"}
      data-surface={surface ?? "plain"}
      className={cn(
        descriptionListVariants({ variant, divided, surface }),
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
      className={cn("py-1.5", className)}
      {...props}
    />
  );
}

function DescriptionTerm({ className, ...props }: React.ComponentProps<"dt">) {
  return (
    <dt
      data-slot="description-term"
      className={cn("font-display text-xs italic text-muted-foreground", className)}
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
