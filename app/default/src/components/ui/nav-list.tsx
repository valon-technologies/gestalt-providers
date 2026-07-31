/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";
import { listItemInteraction } from "@/lib/list-item-interaction";
import { nestedInteractiveSuppress } from "@/lib/nested-interactive";
import { eyebrowVariants } from "@/components/ui/eyebrow";
import { Separator } from "@/components/ui/separator";

// Vertical list of navigation destinations, for an in-page rail (PageLayoutPane),
// a Sheet, or a Flyout. Spec: guidelines/nav-list.md.
//
// Deliberately not a slot of `page-layout`: Primer, Fluent, Mantine and Atlassian
// all keep the nav list independent so the same list works outside the rail.
//
// Router-agnostic. `NavListItem` defaults to an `<a>` and takes `asChild`, so the
// consumer projects the treatment onto their own router link — the registry never
// depends on a router (links.md, buttons.md).
//
// Not `sidebar`: that is the fixed viewport-height app shell. Not `navigation-menu`:
// that is the horizontal Radix menubar. Not a `menu`: these are links inside
// `role="navigation"`, so they must not take menu roles or a roving-focus model.

interface NavListProps extends React.ComponentProps<"nav"> {
  /**
   * Accessible name for the nav landmark. Required — an unlabelled `<nav>` is not
   * reachable by landmark navigation, and pages here carry more than one.
   */
  "aria-label": string;
  /** Class for the inner `<ul>`, when the list needs different spacing than the nav. */
  listClassName?: string;
}

/** Nav landmark wrapping a `<ul>` of destinations. */
function NavList({ className, listClassName, children, ...props }: NavListProps) {
  return (
    <nav data-slot="nav-list" className={cn("min-w-0", className)} {...props}>
      <ul
        data-slot="nav-list-items"
        className={cn("flex min-w-0 flex-col gap-0.5", listClassName)}
      >
        {children}
      </ul>
    </nav>
  );
}

/**
 * A labelled run of items inside a NavList. Renders a nested `<li><ul>` so the
 * list stays a valid single list to assistive tech, and wires the group's label to
 * the sublist with `aria-labelledby`.
 */
function NavListGroup({
  className,
  label,
  children,
  ...props
}: Omit<React.ComponentProps<"li">, "children"> & {
  /** Group heading text. Rendered as an Eyebrow — never a heading element. */
  label?: React.ReactNode;
  children: React.ReactNode;
}) {
  const labelId = React.useId();
  return (
    <li data-slot="nav-list-group" className={cn("min-w-0", className)} {...props}>
      {label ? <NavListGroupLabel id={labelId}>{label}</NavListGroupLabel> : null}
      <ul
        aria-labelledby={label ? labelId : undefined}
        className="flex min-w-0 flex-col gap-0.5"
      >
        {children}
      </ul>
    </li>
  );
}

/**
 * Group label. Dense-chrome Eyebrow (`text-2xs`), never an `h*` — a rail label in
 * the document outline would pollute it (eyebrow.md).
 */
function NavListGroupLabel({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="nav-list-group-label"
      className={cn(
        eyebrowVariants({ size: "sm", tone: "secondary" }),
        "px-3 pt-4 pb-1.5",
        className,
      )}
      {...props}
    />
  );
}

const navListItemVariants = cva(
  [
    // Grid so media hold their columns and only the label truncates. Action-mode
    // rows add a separate outer column for the sibling action slot below.
    "grid min-w-0 grid-cols-[auto_minmax(0,1fr)_auto] items-center gap-2",
    "rounded-md px-3 py-2 text-sm text-muted-foreground",
    // Focus geometry is selected by `focusRing`; outward is the default, while
    // clipped rails can opt into an inset ring at the placement boundary.
    // Nav links carry no resting underline — the treatment is the wash (links.md).
    "no-underline",
    // Single-line labels in a rail truncate rather than wrap (harden.md).
    "[&>[data-slot=nav-list-item-label]]:truncate",
  ].join(" "),
  {
    variants: {
      // Idle / selected wash comes from the shared three-regime role ladder, so a
      // rail item cannot drift from list rows, calendar days, or sidebar items.
      pointer: {
        css: "",
        "css-group": "",
      },
      focusRing: {
        outward: "focus-ring",
        inset: "focus-ring-inset",
        "inset-on-accent": "focus-ring-inset-on-accent",
      },
      actionRow: {
        true: "rounded-none text-inherit",
        false: "",
      },
    },
    defaultVariants: {
      pointer: "css",
      focusRing: "outward",
    },
  },
);

interface NavListItemProps
  extends Omit<React.ComponentProps<"a">, "children" | "aria-current">,
    VariantProps<typeof navListItemVariants> {
  /** Project the treatment onto a router link. */
  asChild?: boolean;
  /** Optional trailing content rendered as a sibling of the destination link. */
  actions?: React.ReactNode;
  /**
   * Current destination. Emits `aria-current` and the selected wash.
   *
   * The consumer computes this from their router; the component owns the ARIA and
   * the paint. Marked by *presence* of `data-selected`, never `data-selected="false"` —
   * React serializes booleans onto `data-*`, and the role ladder keys off presence
   * (selectable-rows.md), so a `false` string would leak the idle gray.
   */
  active?: boolean;
  /**
   * `aria-current` value when `active`. Route changes use `page`; in-page section
   * anchors (TOC, scroll-spy) use `location`.
   */
  current?: "page" | "location";
  children?: React.ReactNode;
}

/**
 * One destination. Renders `<li><a>`; `asChild` swaps the `<a>` for a router link.
 *
 * ```tsx
 * <NavListItem asChild active={pathname === "/settings/tokens"}>
 *   <Link to="/settings/tokens">API tokens</Link>
 * </NavListItem>
 * ```
 */
function NavListItem({
  className,
  asChild = false,
  active = false,
  current = "page",
  pointer,
  focusRing,
  actions,
  children,
  ...props
}: NavListItemProps) {
  const Comp = asChild ? Slot : "a";
  return (
    <li
      data-slot="nav-list-item"
      data-selected={actions && active ? "" : undefined}
      className={cn(
        "min-w-0",
        actions &&
          "grid min-w-0 grid-cols-[minmax(0,1fr)_auto] rounded-md text-muted-foreground",
        actions && listItemInteraction({ pointer }),
        actions && nestedInteractiveSuppress.selectableRowSiblingControl,
      )}
    >
      <Comp
        data-slot="nav-list-item-link"
        data-selected={active ? "" : undefined}
        className={cn(
          navListItemVariants({ pointer, focusRing, actionRow: Boolean(actions) }),
          !actions && listItemInteraction({ pointer }),
          actions && "col-start-1 row-start-1",
          className,
        )}
        {...props}
        aria-current={active ? current : undefined}
      >
        {children}
      </Comp>
      {actions ? <NavListItemActions>{actions}</NavListItemActions> : null}
    </li>
  );
}

/** Leading icon slot. Decorative by default — icons carry no accessible name. */
function NavListItemMedia({ className, ...props }: React.ComponentProps<"span">) {
  return (
    <span
      data-slot="nav-list-item-media"
      aria-hidden="true"
      className={cn(
        "col-start-1 flex shrink-0 items-center [&_svg]:size-4 [&_svg]:shrink-0",
        className,
      )}
      {...props}
    />
  );
}

/** The label. Occupies the fluid column and truncates. */
function NavListItemLabel({ className, ...props }: React.ComponentProps<"span">) {
  return (
    <span
      data-slot="nav-list-item-label"
      className={cn("col-start-2 min-w-0", className)}
      {...props}
    />
  );
}

/** Trailing sibling slot for static metadata or an independent control. */
function NavListItemActions({ className, ...props }: React.ComponentProps<"span">) {
  return (
    <span
      data-slot="nav-list-item-actions"
      data-no-row-click
      className={cn(
        "col-start-2 row-start-1 flex shrink-0 items-center gap-1 px-3",
        className,
      )}
      {...props}
    />
  );
}

/** Rule between runs of items. */
function NavListSeparator({
  className,
  ...props
}: React.ComponentProps<typeof Separator>) {
  return (
    <li data-slot="nav-list-separator" className="min-w-0" role="presentation">
      <Separator className={cn("my-2", className)} {...props} />
    </li>
  );
}

export {
  NavList,
  NavListGroup,
  NavListGroupLabel,
  NavListItem,
  NavListItemMedia,
  NavListItemLabel,
  NavListItemActions,
  NavListSeparator,
  navListItemVariants,
};
