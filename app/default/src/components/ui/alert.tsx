/**
 * Gestalt console vendor of Valon Registry `alert`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/alert.tsx`).
 * Synced from toolshed origin/main — token adaptation only (`@/lib/cn` path).
 * Do not restyle chrome at call sites; change Registry first.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

// shadcn Alert, extended for Valon: a trailing `AlertActions` column (so an inline
// notice can carry buttons), and the status-intent ramp on the shared --info/--success/
// --warning/--error STATE tokens (not the --destructive ACTION color). A leading `>svg`
// shifts the layout into its icon slot automatically.
//
// Surfaces are borderless solid washes — the same status fills Badge / Sonner use
// (`bg-success` etc., guidelines/color.md). No hairline border: the wash alone carries
// the intent. Title/body ink stays neutral (`text-foreground`) so the hue lives in the
// fill, not the copy — chromatic `-foreground` on a pale wash reads busy and fights
// nested chips. Nested chips should still contrast — prefer Badge `default` (or a loud
// fill); same-intent wash-on-wash or `secondary`/`outline` on a chromatic surface blend.
//
// Flush edge (square strip above an editor): the call site owns the separator —
// `rounded-none border-b border-border`. Do not rely on zeroing sides of a base border;
// Alert no longer ships a full `border` box.
//
// `layout`:
// - `default` — the 3-column grid (icon | 1fr content | auto actions). Best for a notice
//   with short trailing actions.
// - `banner` — a WRAPPING control bar: a flex row where the actions flow onto a second line
//   (right-aligned) instead of compressing the content into a narrow column, and the leading
//   icon aligns to the first line of text. Use for a persistent bar that carries real
//   controls; the component also drops `role="alert"` (it isn't an assertive live region).
const alertVariants = cva(
  "group/alert relative w-full rounded-lg text-sm text-foreground",
  {
    variants: {
      variant: {
        default: "bg-muted",
        info: "bg-info",
        success: "bg-success",
        warning: "bg-warning",
        destructive: "bg-error",
      },
      layout: {
        default:
          "grid grid-cols-[0_1fr_auto] items-start gap-y-0.5 px-4 py-3 has-[>svg]:grid-cols-[calc(var(--spacing)*4)_1fr_auto] has-[>svg]:gap-x-2 [&>svg]:size-4 [&>svg]:translate-y-0.5 [&>svg]:text-current",
        banner:
          "flex flex-wrap items-start gap-x-2 gap-y-2 px-4 py-2 [&>svg]:size-4 [&>svg]:shrink-0",
      },
    },
    defaultVariants: {
      variant: "default",
      layout: "default",
    },
  },
);

function Alert({
  className,
  variant,
  layout,
  ...props
}: React.ComponentProps<"div"> & VariantProps<typeof alertVariants>) {
  const resolvedLayout = layout ?? "default";
  return (
    <div
      data-slot="alert"
      data-layout={resolvedLayout}
      role={resolvedLayout === "banner" ? undefined : "alert"}
      className={cn(alertVariants({ variant, layout }), className)}
      {...props}
    />
  );
}

function AlertTitle({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="alert-title"
      className={cn("line-clamp-1 min-h-4 font-medium tracking-tight group-data-[layout=default]/alert:col-start-2", className)}
      {...props}
    />
  );
}

// Flows inline for a single metadata line, and stacks block children (paragraphs) when
// there are several. In `default` it sits in the content column; in `banner` it grows to
// keep its width (never squashed) so the actions wrap below it instead of compressing it.
// Secondary ink: `text-muted-foreground` — theme ink-alpha at 60% of `--foreground`
// (guidelines/color.md § Text ink hierarchy). Do not hand-roll `text-foreground/N`;
// the old solid-gray muted that failed AA on status washes is gone after #3654.
function AlertDescription({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="alert-description"
      className={cn(
        "text-sm text-muted-foreground [&_p]:leading-relaxed [&_p+p]:mt-1",
        "group-data-[layout=default]/alert:col-start-2",
        "group-data-[layout=banner]/alert:min-w-0 group-data-[layout=banner]/alert:grow group-data-[layout=banner]/alert:basis-64",
        className,
      )}
      {...props}
    />
  );
}

// Trailing controls (buttons). In `default` they span the content rows in the grid's third
// column; in `banner` they hug the right edge and wrap, as a group, onto the next line.
function AlertActions({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="alert-actions"
      className={cn(
        "flex items-center gap-1",
        "group-data-[layout=default]/alert:col-start-3 group-data-[layout=default]/alert:row-span-full",
        "group-data-[layout=banner]/alert:ml-auto group-data-[layout=banner]/alert:flex-wrap group-data-[layout=banner]/alert:justify-end",
        className,
      )}
      {...props}
    />
  );
}

export { Alert, AlertTitle, AlertDescription, AlertActions };
