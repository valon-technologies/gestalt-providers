/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { ChevronDown } from "lucide-react";
import { FocusScope } from "@radix-ui/react-focus-scope";
import { RemoveScroll } from "react-remove-scroll";

import { cn } from "@/lib/cn";
import { disclosureCaretClassName } from "@/lib/disclosure-caret";
import { appTopBarColumnVariants } from "@/components/ui/app-top-bar";

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
// Open model: modal dialog overlay (role=dialog + aria-modal + FocusScope trap),
// not Collapsible/Sheet. AppTopBar stays interactive (z-50 above this chrome);
// PageLayout header / columns / footer are inert while open.
//
// Router-agnostic. Consumers pass the same NavList (or other nav) used in
// `pane`, and close on navigate via controlled `open` / `onOpenChange`.

/** Aligns with AppTopBarInner — same variant SoT, not a duplicated class string. */
const pageColumnClassName = appTopBarColumnVariants();

/** PageLayout bands that sit under the open Menu overlay and must not take focus. */
const PAGE_LAYOUT_INERT_SLOTS = [
  "page-layout-header",
  "page-layout-columns",
  "page-layout-footer",
] as const;

interface PageLayoutPaneMobileNavProps
  extends Omit<React.ComponentProps<"div">, "children"> {
  /**
   * Visible label on the bar. Defaults to `Menu` (Next.js docs pattern).
   * Pass a section name only when the product needs a contextual bar title.
   */
  label?: string;
  /**
   * Accessible name for the open dialog. Defaults to `Sections` so it stays
   * distinct from the Menu trigger label.
   */
  panelLabel?: string;
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
 * While the overlay owns the gesture: Esc closes; PageLayout header / columns /
 * footer are `inert`; focus returns to the Menu trigger only when open ends.
 * Focus trapping inside the dialog is owned by FocusScope. Document scroll
 * lock is owned by `RemoveScroll` (same library Radix Dialog uses) — not a
 * DIY `body.style.overflow` toggle, which misses `html` and iOS overscroll.
 *
 * `onClose` is read from a ref so inline parent `onOpenChange` identity churn
 * cannot re-run this effect (which would steal focus from the active nav item).
 */
function useOpenOverlayChrome(
  open: boolean,
  onClose: () => void,
  rootRef: React.RefObject<HTMLDivElement | null>,
  triggerRef: React.RefObject<HTMLButtonElement | null>,
) {
  const onCloseRef = React.useRef(onClose);
  onCloseRef.current = onClose;

  React.useEffect(() => {
    if (!open) return;

    const layout = rootRef.current?.closest('[data-slot="page-layout"]');
    const inertTargets: HTMLElement[] = [];
    if (layout) {
      for (const slot of PAGE_LAYOUT_INERT_SLOTS) {
        const el = layout.querySelector(`[data-slot="${slot}"]`);
        if (!(el instanceof HTMLElement)) continue;
        if (el.hasAttribute("inert")) continue;
        el.setAttribute("inert", "");
        inertTargets.push(el);
      }
    }

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      event.preventDefault();
      onCloseRef.current();
    };
    document.addEventListener("keydown", onKeyDown);

    return () => {
      document.removeEventListener("keydown", onKeyDown);
      for (const el of inertTargets) {
        el.removeAttribute("inert");
      }
      // Skip restore when the host is display:none (lg+ / useCloseOpenWhenHostHidden)
      // — focusing a hidden trigger dumps focus onto <body>.
      const trigger = triggerRef.current;
      if (trigger && trigger.getClientRects().length > 0) {
        trigger.focus();
      }
    };
  }, [open, rootRef, triggerRef]);
}

/**
 * `paneMobile` is `lg:hidden` — CSS alone cannot release `open` side effects
 * (RemoveScroll, inert bands, focus trap). Force-close when this chrome has no
 * layout box (ancestor `display: none`), not merely when the viewport is ≥ lg:
 * stories and other hosts render the primitive outside that wrapper and must
 * stay openable on a desktop canvas.
 */
function useCloseOpenWhenHostHidden(
  open: boolean,
  onClose: () => void,
  rootRef: React.RefObject<HTMLDivElement | null>,
) {
  const onCloseRef = React.useRef(onClose);
  onCloseRef.current = onClose;

  React.useEffect(() => {
    if (!open) return;

    const closeIfHidden = () => {
      const el = rootRef.current;
      if (!el) return;
      // `display: none` (e.g. `lg:hidden` on `page-layout-pane-mobile`) → no rects.
      if (el.getClientRects().length === 0) {
        onCloseRef.current();
      }
    };

    closeIfHidden();
    // ResizeObserver does not fire when an ancestor flips to display:none — also
    // re-check on viewport/media changes that typically drive that class.
    window.addEventListener("resize", closeIfHidden);
    const mq = window.matchMedia("(min-width: 1024px)");
    mq.addEventListener("change", closeIfHidden);
    return () => {
      window.removeEventListener("resize", closeIfHidden);
      mq.removeEventListener("change", closeIfHidden);
    };
  }, [open, rootRef]);
}

/**
 * Panel `top` tracks the live Menu bar bottom — not `--page-layout-pane-top` +
 * nav height. That token math only matches after sticky docks; host padding or
 * sticky AppTopBar geometry would otherwise cover the trigger.
 */
function useOverlayTopPx(
  open: boolean,
  triggerRef: React.RefObject<HTMLButtonElement | null>,
) {
  const [topPx, setTopPx] = React.useState<number | null>(null);

  React.useLayoutEffect(() => {
    if (!open) {
      setTopPx(null);
      return;
    }

    const sync = () => {
      const trigger = triggerRef.current;
      if (!trigger) return;
      setTopPx(trigger.getBoundingClientRect().bottom);
    };

    sync();
    window.addEventListener("resize", sync);
    // Capture scroll from nested scrollports (page column, sticky chrome).
    window.addEventListener("scroll", sync, true);
    const ro =
      typeof ResizeObserver !== "undefined"
        ? new ResizeObserver(sync)
        : null;
    if (triggerRef.current && ro) ro.observe(triggerRef.current);

    return () => {
      window.removeEventListener("resize", sync);
      window.removeEventListener("scroll", sync, true);
      ro?.disconnect();
    };
  }, [open, triggerRef]);

  return topPx;
}

/**
 * Next.js-docs-style mobile pane: Menu bar + caret → viewport-filling overlay.
 * Sticky is owned by `PageLayout`'s tall `paneMobile` wrapper (same bounce as
 * AppTopBar). Place in `PageLayout`'s `paneMobile` slot.
 */
function PageLayoutPaneMobileNav({
  label = "Menu",
  panelLabel = "Sections",
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
  const rootRef = React.useRef<HTMLDivElement>(null);
  const triggerRef = React.useRef<HTMLButtonElement>(null);
  const panelId = React.useId();
  const titleId = React.useId();

  const handleOpenChange = React.useCallback(
    (next: boolean) => {
      if (!isControlled) {
        setUncontrolledOpen(next);
      }
      onOpenChange?.(next);
    },
    [isControlled, onOpenChange],
  );

  const close = React.useCallback(() => {
    handleOpenChange(false);
  }, [handleOpenChange]);

  const toggle = React.useCallback(() => {
    handleOpenChange(!open);
  }, [handleOpenChange, open]);

  useOpenOverlayChrome(open, close, rootRef, triggerRef);
  useCloseOpenWhenHostHidden(open, close, rootRef);
  const overlayTopPx = useOverlayTopPx(open, triggerRef);

  const triggerName = ariaLabel ?? label;

  return (
    <div
      ref={rootRef}
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
      <div className="relative w-full">
        <button
          ref={triggerRef}
          type="button"
          className={cn(
            // In-flow secondary chrome — scrolls/bounces with sticky AppTopBar.
            // No Neutral list-item hover — the bar reads as chrome, not a row.
            "group focus-ring flex h-[length:var(--page-layout-mobile-nav-height,3rem)] w-full items-center border-b border-border bg-background text-left text-sm font-medium text-foreground",
            "hover:bg-background active:bg-background",
          )}
          aria-label={triggerName}
          aria-expanded={open}
          aria-controls={open ? panelId : undefined}
          aria-haspopup="dialog"
          onClick={toggle}
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
        </button>

        {open ? (
          <RemoveScroll allowPinchZoom>
            <FocusScope
              loop
              trapped
              // Trigger owns open focus; restore on close is in useOpenOverlayChrome.
              onMountAutoFocus={(event) => {
                event.preventDefault();
                const panel = document.getElementById(panelId);
                const firstFocusable = panel?.querySelector<HTMLElement>(
                  'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])',
                );
                (firstFocusable ?? panel)?.focus();
              }}
              onUnmountAutoFocus={(event) => {
                event.preventDefault();
              }}
            >
              <div
                id={panelId}
                role="dialog"
                aria-modal="true"
                aria-labelledby={titleId}
                tabIndex={-1}
                className="fixed inset-x-0 bottom-0 z-40 bg-background outline-none"
                style={
                  overlayTopPx == null
                    ? undefined
                    : { top: overlayTopPx }
                }
              >
                <h2 id={titleId} className="sr-only">
                  {panelLabel}
                </h2>
                <div
                  className={cn(
                    pageColumnClassName,
                    "h-full overflow-y-auto overscroll-contain py-3",
                  )}
                >
                  {children}
                </div>
              </div>
            </FocusScope>
          </RemoveScroll>
        ) : null}
      </div>
    </div>
  );
}

export { PageLayoutPaneMobileNav };
export type { PageLayoutPaneMobileNavProps };
