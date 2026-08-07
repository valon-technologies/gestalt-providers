/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { MenuIcon, XIcon } from "lucide-react";

import { cn } from "@/lib/cn";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetClose,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet";

// Mobile stand-in for a PageLayout Pane on viewports below `lg`. Closed: a bar
// with a menu control + the current destination label. Open: the same nav tree
// in a left Sheet. Spec: guidelines/page-layout-pane-mobile-nav.md.
//
// Companion to `page-layout` — not a slot of it. Pass this as `paneMobile` when
// the Pane hosts a longer list (docs, catalog, workspace). Handful of modes →
// SegmentedControl instead.
//
// Router-agnostic. Consumers pass the same NavList (or other nav) used in
// `pane`, and close the sheet on navigate via controlled `open` / `onOpenChange`
// or by remounting after a route change.

interface PageLayoutPaneMobileNavProps
  extends Omit<React.ComponentProps<"div">, "children"> {
  /** Current destination shown next to the menu control. */
  label: string;
  /** Nav body — normally the same `NavList` as desktop `pane`. */
  children: React.ReactNode;
  /** Controlled open state. Omit for uncontrolled. */
  open?: boolean;
  /** Controlled open change handler. */
  onOpenChange?: (open: boolean) => void;
  /**
   * Accessible name for the menu control. Defaults to
   * `Navigation, current: {label}`.
   */
  "aria-label"?: string;
  /** Sheet title announced to assistive tech. Defaults to `Navigation`. */
  sheetTitle?: string;
}

/**
 * Tailwind-docs-style mobile pane: menu bar + current label → left Sheet.
 * Place in `PageLayout`'s `paneMobile` slot.
 */
function PageLayoutPaneMobileNav({
  label,
  children,
  open: openProp,
  onOpenChange,
  "aria-label": ariaLabel,
  sheetTitle = "Navigation",
  className,
  ...props
}: PageLayoutPaneMobileNavProps) {
  const [uncontrolledOpen, setUncontrolledOpen] = React.useState(false);
  const isControlled = openProp !== undefined;
  const open = isControlled ? openProp : uncontrolledOpen;

  const handleOpenChange = React.useCallback(
    (next: boolean) => {
      if (!isControlled) {
        setUncontrolledOpen(next);
      }
      onOpenChange?.(next);
    },
    [isControlled, onOpenChange],
  );

  const triggerLabel = ariaLabel ?? `Navigation, current: ${label}`;

  return (
    <div
      data-slot="page-layout-pane-mobile-nav"
      className={cn("flex w-full min-w-0 items-center gap-2", className)}
      {...props}
    >
      <Sheet open={open} onOpenChange={handleOpenChange}>
        <SheetTrigger asChild>
          <Button
            type="button"
            variant="ghost"
            size="icon"
            className="shrink-0"
            aria-label={triggerLabel}
          >
            <MenuIcon />
          </Button>
        </SheetTrigger>
        <span className="min-w-0 truncate text-sm font-medium text-foreground">
          {label}
        </span>
        <SheetContent
          side="left"
          showCloseButton={false}
          className="flex w-[min(100%,18rem)] flex-col gap-0 p-0 sm:max-w-xs"
        >
          {/*
            Titled bar owns title + dismiss. Sheet's default absolute close
            (top-4/right-4) is Dialog geometry and fights this header's py-3
            inset — one chrome row keeps top/bottom/right equal.
          */}
          <SheetHeader className="flex-row items-center justify-between gap-2 space-y-0 border-b border-border p-3">
            <SheetTitle className="min-w-0 truncate">{sheetTitle}</SheetTitle>
            <SheetDescription className="sr-only">
              Section navigation for this page.
            </SheetDescription>
            <SheetClose asChild>
              <Button
                type="button"
                variant="ghost"
                size="icon-sm"
                className="shrink-0"
                aria-label="Close"
              >
                <XIcon />
              </Button>
            </SheetClose>
          </SheetHeader>
          <div className="min-h-0 flex-1 overflow-y-auto overscroll-contain p-3">
            {children}
          </div>
        </SheetContent>
      </Sheet>
    </div>
  );
}

export { PageLayoutPaneMobileNav };
export type { PageLayoutPaneMobileNavProps };
