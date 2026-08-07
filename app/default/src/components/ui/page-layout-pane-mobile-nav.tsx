/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { ChevronDown } from "lucide-react";

import { cn } from "@/lib/cn";
import { disclosureCaretClassName } from "@/lib/disclosure-caret";
import {
  Collapsible,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";

// Mobile stand-in for a PageLayout Pane on viewports below `lg`. Secondary app
// chrome: in-flow Menu bar (PageLayout owns sticky on the tall `paneMobile`
// wrapper so it shares overscroll bounce with AppTopBar). Open state fills the
// remaining viewport and locks document scroll. Pattern: nextjs.org/docs.
// Spec: guidelines/page-layout-pane-mobile-nav.md.
//
// Companion to `page-layout` — not a slot of it. Pass this as `paneMobile` when
// the Pane hosts a longer list (docs, catalog, workspace). Handful of modes →
// SegmentedControl instead.
//
// Router-agnostic. Consumers pass the same NavList (or other nav) used in
// `pane`, and close on navigate via controlled `open` / `onOpenChange`.

/** Same content column as AppTopBarInner / Container — keep strings in sync. */
const pageColumnClassName = "mx-auto w-full max-w-7xl px-6";

interface PageLayoutPaneMobileNavProps
  extends Omit<React.ComponentProps<"div">, "children"> {
  /**
   * Visible label on the bar. Defaults to `Menu` (Next.js docs pattern).
   * Pass a section name only when the product needs a contextual bar title.
   */
  label?: string;
  /** Nav body — normally the same `NavList` as desktop `pane`. */
  children: React.ReactNode;
  /** Controlled open state. Omit for uncontrolled. */
  open?: boolean;
  /** Controlled open change handler. */
  onOpenChange?: (open: boolean) => void;
  /** Accessible name for the bar control. Defaults to `label`. */
  "aria-label"?: string;
}

/**
 * Lock document scroll while the overlay menu owns the gesture.
 * Restores the previous inline overflow on cleanup / close.
 */
function useDocumentScrollLock(locked: boolean) {
  React.useEffect(() => {
    if (!locked) return;
    const { body } = document;
    const previousOverflow = body.style.overflow;
    const previousPaddingRight = body.style.paddingRight;
    const scrollbarGap = window.innerWidth - document.documentElement.clientWidth;
    body.style.overflow = "hidden";
    if (scrollbarGap > 0) {
      body.style.paddingRight = `${scrollbarGap}px`;
    }
    return () => {
      body.style.overflow = previousOverflow;
      body.style.paddingRight = previousPaddingRight;
    };
  }, [locked]);
}

/**
 * Next.js-docs-style mobile pane: Menu bar + caret → viewport-filling overlay.
 * Sticky is owned by `PageLayout`'s tall `paneMobile` wrapper (same bounce as
 * AppTopBar). Place in `PageLayout`'s `paneMobile` slot.
 */
function PageLayoutPaneMobileNav({
  label = "Menu",
  children,
  open: openProp,
  onOpenChange,
  "aria-label": ariaLabel,
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

  useDocumentScrollLock(open);

  return (
    <div
      data-slot="page-layout-pane-mobile-nav"
      data-state={open ? "open" : "closed"}
      className={cn(
        // Edge-to-edge under the padded content column (margin bleed — not
        // transform). Sticky lives on PageLayout's pane-mobile wrapper.
        "w-screen max-w-[100vw] min-w-0 bg-background ml-[calc(50%-50vw)]",
        className,
      )}
      {...props}
    >
      <Collapsible
        open={open}
        onOpenChange={handleOpenChange}
        className="relative w-full"
      >
        <CollapsibleTrigger
          className={cn(
            // In-flow secondary chrome — scrolls/bounces with sticky AppTopBar.
            "group flex h-[length:var(--page-layout-mobile-nav-height,3rem)] w-full items-center border-b border-border bg-background text-sm font-medium",
            "hover:bg-background active:bg-background",
          )}
          aria-label={ariaLabel ?? label}
        >
          <span
            className={cn(
              pageColumnClassName,
              "flex min-w-0 items-center justify-start gap-1.5",
            )}
          >
            <ChevronDown className={disclosureCaretClassName} aria-hidden />
            <span className="min-w-0 truncate">{label}</span>
          </span>
        </CollapsibleTrigger>

        {open ? (
          <div
            role="region"
            aria-label={ariaLabel ?? label}
            className="fixed inset-x-0 bottom-0 top-[calc(var(--page-layout-pane-top,0px)+var(--page-layout-mobile-nav-height,3rem))] z-40 bg-background"
          >
            <div
              className={cn(
                pageColumnClassName,
                "h-full overflow-y-auto overscroll-contain py-3",
              )}
            >
              {children}
            </div>
          </div>
        ) : null}
      </Collapsible>
    </div>
  );
}

export { PageLayoutPaneMobileNav };
export type { PageLayoutPaneMobileNavProps };
