/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 * Registry `alert`.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { ChevronDownIcon } from "lucide-react";

import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { cn } from "@/lib/cn";

// shadcn Alert, extended for Gestalt console: a trailing `AlertActions` column (so an inline
// notice can carry buttons), and the status-intent ramp on the shared --info/--success/
// --warning/--error STATE tokens (not the --destructive ACTION color). A leading `>svg`
// shifts the layout into its icon slot automatically.
//
// Surfaces: status washes are borderless solid fills (`bg-success` etc.,
// guidelines/color.md) — the wash alone carries intent. `outline` is quiet Card
// chrome (`border-border` + `bg-card`) for optional tip / CLI help without a
// status wash (replaces the former Tip primitive). Title ink stays neutral
// (`text-foreground`) so chromatic hue lives in the fill, not the copy.
// Nested chips should still contrast — prefer Badge `default` (or a loud fill);
// same-intent wash-on-wash or `secondary`/`outline` Button on a chromatic surface blend.
//
// Flush edge (square strip above an editor): the call site owns the separator —
// `rounded-none border-b border-border`. Do not rely on zeroing sides of a base border;
// Alert no longer ships a full `border` box on status washes.
//
// Composition (agents / call sites — do not invent a fourth slot):
// - Sole primary message → `AlertTitle` (or Description alone; Description is
//   `text-foreground` until a Title is present, then it becomes muted secondary).
// - Never put the only line of copy in Description *and* expect it to look primary
//   under an older “always muted” mental model — the primitive now encodes hierarchy.
// - Trailing Retry / actions → `AlertActions`. Alignment is layout-owned: both
//   `default` and `banner` baseline the first text line so the leading icon and
//   control labels share the copy band. Banner (flex) needs a control-sm
//   `translate-y-1` nudge; default (grid) must not — the same nudge over-corrects.
//   Do not invent call-site `mt-*` / nested icon flex to “fix” either.
// - Collapsible secondary help → `collapsible` + `AlertTrigger` +
//   `AlertCollapsibleContent` (Alert owns the surface + disclosure; do not hand-roll
//   Collapsible + alertVariants unless you need a non-Alert root). Quiet CLI tips
//   use `variant="outline"` (+ optional `animateSize`); status recovery uses a wash.
//
// `layout`:
// - `default` — the 3-column grid (icon | 1fr content | auto actions). Best for a notice
//   with short trailing actions.
// - `banner` — a WRAPPING control bar: flex row where actions flow onto a second
//   line (right-aligned) instead of crushing content. Leading icon is a direct
//   `>svg` child; `AlertDescription` is the text column beside it (`text-pretty`,
//   never an inline glyph inside Description — that wraps under the icon). Drops
//   `role="alert"` (persistent control bar, not an assertive live region).
// - `chrome` — wash/chrome + radius only. Used automatically when `collapsible` is set; also
//   available via `alertVariants` for rare non-Alert Collapsible roots.
const alertVariants = cva(
  "group/alert relative w-full rounded-lg text-sm text-foreground",
  {
    variants: {
      variant: {
        default: "bg-muted",
        // Quiet Card chrome — optional tip / CLI help (no status wash).
        outline: "border border-border bg-card",
        info: "bg-info",
        success: "bg-success",
        warning: "bg-warning",
        destructive: "bg-error",
      },
      layout: {
        // First-line baseline when actions are present — leading icon stays on
        // the copy band. CSS grid already exposes the control's alphabetic
        // baseline (no translate nudge — that over-corrects vs flex/banner).
        default:
          "grid grid-cols-[0_1fr_auto] items-start gap-y-0.5 px-4 py-3 has-[[data-slot=alert-actions]]:items-baseline has-[>svg]:grid-cols-[calc(var(--spacing)*4)_1fr_auto] has-[>svg]:gap-x-2 [&>svg]:size-4 [&>svg]:translate-y-0.5 [&>svg]:text-current",
        // Flex synthesizes a padded <button> baseline from the margin box, so
        // actions need translate-y-1; pb-5 keeps the painted bottom inset equal
        // to the side gap after that nudge. Icon: translate-y-0.5 optical nudge
        // while participating in the first-line band (not self-start — flex
        // start is the wrap-line top, not the text line box).
        banner:
          "flex flex-wrap items-start gap-x-2 gap-y-2 p-4 has-[[data-slot=alert-actions]]:items-baseline has-[[data-slot=alert-actions]]:pb-5 has-[>svg]:gap-x-2 [&>svg]:size-4 [&>svg]:shrink-0 [&>svg]:translate-y-0.5 [&>svg]:text-current",
        chrome: "",
      },
    },
    defaultVariants: {
      variant: "default",
      layout: "default",
    },
  },
);

type AlertVariantProps = VariantProps<typeof alertVariants>;

type AlertStaticProps = AlertVariantProps &
  React.ComponentProps<"div"> & {
    collapsible?: false;
  };

type AlertCollapsibleProps = AlertVariantProps &
  Omit<React.ComponentProps<typeof Collapsible>, "className"> & {
    collapsible: true;
    className?: string;
    /** Collapsible alerts always use the chrome (surface-only) layout. */
    layout?: "chrome";
    /**
     * Content-width drawer: root `width: fit-content` + panel max-width animation
     * (`data-animate-size` in theme CSS). Default off — status washes usually sit
     * in a parent column (`w-full`). Opt in for compact outline CLI tips.
     */
    animateSize?: boolean;
  };

type AlertProps = AlertStaticProps | AlertCollapsibleProps;

function Alert(props: AlertProps) {
  if (props.collapsible) {
    const {
      className,
      variant,
      layout: _layout,
      collapsible: _collapsible,
      animateSize = false,
      ...collapsibleProps
    } = props;
    return (
      <Collapsible
        data-slot="alert"
        data-layout="chrome"
        data-collapsible=""
        data-variant={variant ?? "default"}
        data-animate-size={animateSize ? "" : undefined}
        className={cn(alertVariants({ variant, layout: "chrome" }), className)}
        {...collapsibleProps}
      />
    );
  }

  const { className, variant, layout, collapsible: _collapsible, ...divProps } =
    props;
  const resolvedLayout = layout ?? "default";
  // banner / chrome / outline are structural surfaces, not assertive live regions.
  const live = resolvedLayout === "default" && variant !== "outline";
  return (
    <div
      data-slot="alert"
      data-layout={resolvedLayout}
      data-variant={variant ?? "default"}
      role={live ? "alert" : undefined}
      className={cn(alertVariants({ variant, layout }), className)}
      {...divProps}
    />
  );
}

function AlertTitle({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="alert-title"
      className={cn(
        "line-clamp-1 min-h-4 font-medium tracking-tight group-data-[layout=default]/alert:col-start-2",
        className,
      )}
      {...props}
    />
  );
}

// Flows inline for a single metadata line, and stacks block children (paragraphs) when
// there are several. In `default` it sits in the content column; in `banner` it is the
// text column beside a direct leading `>svg` — `min-w-0` + `text-pretty` so wraps stay
// in the column (never under the icon), and `basis-64` keeps it from being crushed
// before actions wrap. Ink: primary (`text-foreground`) when it is the only copy;
// muted only when an `AlertTitle` is also present (secondary under the title). Do not
// hand-roll `text-foreground/N`; theme ink-alpha muted is `text-muted-foreground`
// (guidelines/color.md § Text ink hierarchy).
function AlertDescription({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="alert-description"
      className={cn(
        "text-sm text-foreground group-has-[[data-slot=alert-title]]/alert:text-muted-foreground [&_p]:leading-relaxed [&_p+p]:mt-1",
        "group-data-[layout=default]/alert:col-start-2",
        "group-data-[layout=banner]/alert:min-w-0 group-data-[layout=banner]/alert:grow group-data-[layout=banner]/alert:basis-64 group-data-[layout=banner]/alert:text-pretty",
        className,
      )}
      {...props}
    />
  );
}

// Trailing controls (buttons). In `default` they span the content rows in the grid's third
// column; in `banner` they hug the right edge and wrap, as a group, onto the next line.
//
// Baseline: both layouts use parent `items-baseline` so control labels share the copy
// alphabetic baseline. Banner (flex) still needs `translate-y-1` — a padded `<button>`
// under flex baseline synthesizes from the margin box (~4px high). Default (grid) does
// not — the same nudge over-corrects. Banner root `pb-5` absorbs the flex nudge so the
// painted bottom inset stays equal to the side gap.
function AlertActions({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="alert-actions"
      className={cn(
        "flex items-center gap-1",
        "group-data-[layout=default]/alert:col-start-3 group-data-[layout=default]/alert:row-span-full",
        "group-data-[layout=banner]/alert:ml-auto group-data-[layout=banner]/alert:flex-wrap group-data-[layout=banner]/alert:justify-end group-data-[layout=banner]/alert:translate-y-1",
        className,
      )}
      {...props}
    />
  );
}

// Disclosure trigger for `Alert collapsible`. Ships the Accordion caret idiom;
// CollapsibleTrigger owns focus-ring + Neutral idle hover/press for white/card.
// On a status / muted wash, Neutral reads gray — override to deepen the shade
// (pale L-step tokens / Neutral dark on muted). Outline keeps Neutral idle
// (card surface — same as Collapsible on Card outline).
function AlertTrigger({
  className,
  children,
  ...props
}: React.ComponentProps<typeof CollapsibleTrigger>) {
  return (
    <CollapsibleTrigger
      data-slot="alert-trigger"
      className={cn(
        "rounded-lg px-4 py-3 data-[state=closed]:rounded-lg data-[state=open]:rounded-b-none",
        // Same selector family as listItemInteraction idle — higher specificity
        // via group-data-[variant] so Neutral gray never paints on a wash.
        "group-data-[variant=default]/alert:[&:not([data-selected]):not([data-soft])]:hover:bg-neutral-dark-hover",
        "group-data-[variant=default]/alert:[&:not([data-selected]):not([data-soft])]:active:bg-neutral-dark-pressed",
        "group-data-[variant=info]/alert:[&:not([data-selected]):not([data-soft])]:hover:bg-info-hover",
        "group-data-[variant=info]/alert:[&:not([data-selected]):not([data-soft])]:active:bg-info-pressed",
        "group-data-[variant=success]/alert:[&:not([data-selected]):not([data-soft])]:hover:bg-success-hover",
        "group-data-[variant=success]/alert:[&:not([data-selected]):not([data-soft])]:active:bg-success-pressed",
        "group-data-[variant=warning]/alert:[&:not([data-selected]):not([data-soft])]:hover:bg-warning-hover",
        "group-data-[variant=warning]/alert:[&:not([data-selected]):not([data-soft])]:active:bg-warning-pressed",
        "group-data-[variant=destructive]/alert:[&:not([data-selected]):not([data-soft])]:hover:bg-error-hover",
        "group-data-[variant=destructive]/alert:[&:not([data-selected]):not([data-soft])]:active:bg-error-pressed",
        className,
      )}
      {...props}
    >
      {children}
      <ChevronDownIcon
        aria-hidden="true"
        className="pointer-events-none size-4 shrink-0 text-muted-foreground transition-transform duration-overshoot ease-out-back"
      />
    </CollapsibleTrigger>
  );
}

// Drawer body for `Alert collapsible`. Padding / border sit on CollapsibleContent's
// inner wrapper (same split as AccordionContent) so height animation stays clean.
// Do not rename `data-slot` on CollapsibleContent — theme drawer CSS keys on
// `collapsible-content` (Alert identity lives on the root + AlertTrigger).
//
// Drawer hairline — same proportion as Default (`border-border/50` on muted):
// Neutral is wash-100 → edge-200 at /50; status uses the same adjacent ramp
// step (light `*-200`, dark `*-800` on the `*-900` wash). Not `*-solid` (too
// loud) and not `*-hover` (ΔL ≈ 0.012 — invisible as a 1px rule; wrong direction
// in dark).
function AlertCollapsibleContent({
  className,
  ...props
}: React.ComponentProps<typeof CollapsibleContent>) {
  return (
    <CollapsibleContent
      className={cn(
        "space-y-2 rounded-b-lg border-t border-border/50 px-4 py-3",
        // Outline (card chrome) uses a full hairline — /50 reads weak on bg-card.
        "group-data-[variant=outline]/alert:border-border",
        "group-data-[variant=info]/alert:border-blue-200/50 dark:group-data-[variant=info]/alert:border-blue-800/50",
        "group-data-[variant=success]/alert:border-green-200/50 dark:group-data-[variant=success]/alert:border-green-800/50",
        "group-data-[variant=warning]/alert:border-yellow-200/50 dark:group-data-[variant=warning]/alert:border-yellow-800/50",
        "group-data-[variant=destructive]/alert:border-red-200/50 dark:group-data-[variant=destructive]/alert:border-red-800/50",
        className,
      )}
      {...props}
    />
  );
}

export {
  Alert,
  AlertTitle,
  AlertDescription,
  AlertActions,
  AlertTrigger,
  AlertCollapsibleContent,
  alertVariants,
};
