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
// Composition (agents / call sites — keep the parts as direct children):
// - Optional leading icon → a direct `>svg` child (decorative; `aria-hidden`).
// - Sole primary message → `AlertTitle` (or Description alone).
// - Default layout is the stacked callout: icon rail + copy column. Title sits
//   beside the icon; Description stays in that same copy column under the title.
//   Title+Description stack is Description `mt-1.5` (both layouts). Description
//   stays `text-foreground` on default. Banner still mutes Description when a
//   Title is present (toolbar metadata).
// - Trailing Retry / actions → `AlertActions`. Alignment is layout-owned: both
//   `default` and `banner` baseline a single copy line so the leading icon and
//   control labels share the copy band. Title+Description stacks `items-start`
//   so a taller control cannot open the description `mt-1.5` body gap. Do not
//   invent call-site `mt-*` / nested icon flex to “fix” either.
// - Collapsible secondary help → `collapsible` + `AlertTrigger` +
//   `AlertCollapsibleContent` (Alert owns the surface + disclosure; do not hand-roll
//   Collapsible + alertVariants unless you need a non-Alert root). Quiet CLI tips
//   use `variant="outline"` (+ optional `animateSize`); status recovery uses a wash.
//
// `layout` is geometry only. Live region follows layout + variant, same as Registry:
// default (except outline) is `role="alert"`; banner, chrome, and outline are not.
// Persistent in-page guidance is `Callout` in `src/components/`, not a silenced Alert.
// - `default` — stacked callout grid (icon | copy | actions). Title and
//   Description share column 2; Description stacks under Title.
// - `banner` — a WRAPPING control bar: CSS grid keeps Title + Description in a
//   copy column while actions move to a right-aligned second row when needed.
//   Leading icon is a direct `>svg` child. Never put the glyph inside
//   Description, where it would wrap under the icon.
// - `chrome` — wash/chrome + radius only. Used automatically when `collapsible` is set; also
//   available via `alertVariants` for rare non-Alert Collapsible roots.
const alertVariantClasses = {
  default: "bg-muted",
  // Quiet Card chrome — optional tip / CLI help (no status wash).
  outline: "border border-border bg-card",
  info: "bg-info",
  success: "bg-success",
  warning: "bg-warning",
  destructive: "bg-error",
} as const;

const alertLayoutClasses = {
  // Copy track is `1fr` so shrink-wrap canvases keep max-content. Title and
  // Description already `min-w-0`, which is enough for a definite parent to wrap.
  // First-line baseline when actions are present — leading icon stays on
  // the copy band. CSS grid already exposes the control's alphabetic
  // baseline (no translate nudge — that over-corrects vs flex/banner).
  default:
    "grid grid-cols-[0_1fr_auto] items-start gap-y-0 p-4 has-[[data-slot=alert-actions]]:items-baseline has-[>svg]:grid-cols-[calc(var(--spacing)*4)_1fr_auto] has-[>[data-slot=alert-icon]]:grid-cols-[calc(var(--spacing)*4)_1fr_auto] has-[>svg]:gap-x-2 has-[>[data-slot=alert-icon]]:gap-x-2 [&>svg]:col-start-1 [&>svg]:row-start-1 [&>svg]:size-4 [&>svg]:shrink-0 [&>svg]:translate-y-0.5 [&>svg]:text-current [&>[data-slot=alert-icon]]:col-start-1 [&>[data-slot=alert-icon]]:row-start-1 [&>[data-slot=alert-icon]]:shrink-0 [&>[data-slot=alert-icon]]:translate-y-0.5",
  // CSS grid keeps the copy in one column and moves actions to distinct rows
  // at narrow container widths. The icon column is conditional at every width.
  banner:
    "grid grid-cols-[0_1fr_auto] items-start gap-y-0 p-4 has-[[data-slot=alert-actions]]:items-baseline has-[>svg]:grid-cols-[calc(var(--spacing)*4)_1fr_auto] has-[>[data-slot=alert-icon]]:grid-cols-[calc(var(--spacing)*4)_1fr_auto] has-[>svg]:gap-x-2 has-[>[data-slot=alert-icon]]:gap-x-2 [&>svg]:col-start-1 [&>svg]:row-start-1 [&>svg]:size-4 [&>svg]:shrink-0 [&>svg]:translate-y-0.5 [&>svg]:text-current [&>[data-slot=alert-icon]]:col-start-1 [&>[data-slot=alert-icon]]:row-start-1 [&>[data-slot=alert-icon]]:shrink-0 [&>[data-slot=alert-icon]]:translate-y-0.5 @max-[480px]/alert:has-[[data-slot=alert-actions]]:grid-cols-[0_1fr] @max-[480px]/alert:has-[[data-slot=alert-actions]]:has-[>svg]:grid-cols-[calc(var(--spacing)*4)_1fr] @max-[480px]/alert:has-[[data-slot=alert-actions]]:has-[>[data-slot=alert-icon]]:grid-cols-[calc(var(--spacing)*4)_1fr] @max-[480px]/alert:has-[[data-slot=alert-actions]]:[&>[data-slot=alert-actions]]:mt-2 @max-[480px]/alert:has-[[data-slot=alert-actions]]:[&>[data-slot=alert-actions]]:col-start-2 @max-[480px]/alert:has-[[data-slot=alert-actions]]:[&>[data-slot=alert-actions]]:col-end-3 @max-[480px]/alert:has-[[data-slot=alert-actions]]:[&>[data-slot=alert-actions]]:[grid-row:2] @max-[480px]/alert:has-[[data-slot=alert-actions]]:has-[[data-slot=alert-title]]:has-[[data-slot=alert-description]]:[&>[data-slot=alert-actions]]:[grid-row:3] @max-[480px]/alert:has-[[data-slot=alert-actions]]:[&>[data-slot=alert-actions]]:justify-self-end",
  chrome: "",
} as const;

const alertRootClasses = "group/alert relative w-full rounded-lg text-sm text-foreground";

const alertVariants = cva(
  alertRootClasses,
  {
    variants: {
      variant: alertVariantClasses,
      layout: alertLayoutClasses,
    },
    defaultVariants: {
      variant: "default",
      layout: "default",
    },
  },
);

const alertSurfaceVariants = cva(
  alertRootClasses,
  {
    variants: {
      variant: alertVariantClasses,
    },
    defaultVariants: {
      variant: "default",
    },
  },
);

const alertLayoutVariants = cva(
  // Title+Description is an explicit two-row copy stack on both layouts.
  // Copy-internal gap is Description `mt-1.5` (14px UI unit). Do not put that
  // on the grid — a shared `gap-y` also spaces wrapped actions. `items-start`
  // so a baseline-aligned control cannot inflate the title row.
  // `grid-rows-[auto_auto]` so `AlertActions` `row-span-full` (`1 / -1`)
  // actually spans — negative lines only count the explicit grid. Banner wrap
  // spaces actions with `mt-2`, independent of the copy stack.
  "w-full has-[[data-slot=alert-title]]:has-[[data-slot=alert-description]]:grid-rows-[auto_auto] has-[[data-slot=alert-title]]:has-[[data-slot=alert-description]]:items-start",
  {
    variants: {
      layout: alertLayoutClasses,
    },
    defaultVariants: {
      layout: "default",
    },
  },
);

type AlertVariantProps = VariantProps<typeof alertVariants>;

type AlertStaticProps = AlertVariantProps &
  Omit<React.ComponentProps<"div">, "role"> & {
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

  const {
    className,
    variant,
    layout,
    collapsible: _collapsible,
    children,
    ...divProps
  } = props;
  const resolvedLayout = layout ?? "default";
  const live = resolvedLayout === "default" && variant !== "outline";
  return (
    <div
      data-slot="alert"
      data-layout={resolvedLayout}
      data-variant={variant ?? "default"}
      className={cn(alertSurfaceVariants({ variant }), className)}
      {...divProps}
      role={live ? "alert" : undefined}
    >
      <div
        data-slot="alert-content"
        className={cn("w-full", resolvedLayout === "banner" && "@container/alert")}
      >
        <div data-slot="alert-layout" className={alertLayoutVariants({ layout: resolvedLayout })}>
          {children}
        </div>
      </div>
    </div>
  );
}

function AlertTitle({ className, ...props }: React.ComponentProps<"div">) {
  // Inherited `text-sm` leading — titles wrap (`wrap-break-word`). `leading-none`
  // is for one-line chrome (Eyebrow); on two lines it stacks the next line into
  // descenders.
  return (
    <div
      data-slot="alert-title"
      className={cn(
        "min-h-4 min-w-0 wrap-break-word font-semibold tracking-tight group-data-[layout=default]/alert:col-start-2 group-data-[layout=banner]/alert:col-start-2 group-data-[layout=banner]/alert:row-start-1",
        className,
      )}
      {...props}
    />
  );
}

// Flows inline for a single metadata line, and stacks block children (paragraphs) when
// there are several. Both layouts keep Description in the copy column (beside the
// icon rail). With a Title, both layouts stack Description under Title with
// `mt-1.5`. In `banner` it still occupies that column and moves below Title
// when both are present.
// Ink: primary (`text-foreground`) on default. Banner mutes Description when a
// Title is also present (toolbar metadata). Do not hand-roll `text-foreground/N`;
// theme ink-alpha muted is `text-muted-foreground` (guidelines/color.md § Text
// ink hierarchy).
function AlertDescription({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="alert-description"
      className={cn(
        "min-w-0 wrap-break-word text-sm text-foreground text-pretty group-has-[[data-slot=alert-title]]/alert:mt-1.5 group-data-[layout=default]/alert:col-start-2 group-data-[layout=default]/alert:group-has-[[data-slot=alert-title]]/alert:row-start-2 group-data-[layout=banner]/alert:col-start-2 group-data-[layout=banner]/alert:row-start-1 group-data-[layout=banner]/alert:group-has-[[data-slot=alert-title]]/alert:row-start-2 group-data-[layout=banner]/alert:group-has-[[data-slot=alert-title]]/alert:text-muted-foreground [&_p]:leading-relaxed [&_p+p]:mt-1",
        className,
      )}
      {...props}
    />
  );
}

// Trailing controls (buttons). Both layouts span the explicit copy rows in the
// third column (`row-span-full` = `1 / -1`) so a taller control cannot inflate
// the title row. Title+Description defines those rows (`grid-rows-[auto_auto]`).
// In `banner`, the container query moves them to a right-aligned second row
// (or third row when Title and Description are both present) with `grid-row: 2`
// / `grid-row: 3` so the span does not leak, and `mt-2` for the copy→control
// gutter (independent of Description `mt-1.5`).
//
// Single-line copy still uses parent `items-baseline` so control labels share
// the alphabetic baseline. Title+Description stacks switch the grid to
// `items-start` so that baseline alignment cannot inflate the title row.
function AlertActions({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="alert-actions"
      className={cn(
        "flex items-center gap-1",
        "group-data-[layout=default]/alert:col-start-3 group-data-[layout=default]/alert:row-span-full",
        "group-data-[layout=banner]/alert:col-start-3 group-data-[layout=banner]/alert:row-span-full group-data-[layout=banner]/alert:ml-auto group-data-[layout=banner]/alert:flex-wrap group-data-[layout=banner]/alert:justify-end group-data-[layout=banner]/alert:justify-self-end",
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
// Drawer hairline — same adjacent ramp step as Default (`border-border/50` on
// muted): status uses the explicit light `*-200` / dark `*-800` edge on its
// `*-100` / `*-900` wash. Not `*-foreground` (on-wash ink), `*-solid` (too loud),
// or `*-hover` (the fill step, not a 1px rule).
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
        "group-data-[variant=info]/alert:border-blue-200 dark:group-data-[variant=info]/alert:border-blue-800",
        "group-data-[variant=success]/alert:border-green-200 dark:group-data-[variant=success]/alert:border-green-800",
        "group-data-[variant=warning]/alert:border-yellow-200 dark:group-data-[variant=warning]/alert:border-yellow-800",
        "group-data-[variant=destructive]/alert:border-red-200 dark:group-data-[variant=destructive]/alert:border-red-800",
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
  alertSurfaceVariants,
  alertLayoutVariants,
};
