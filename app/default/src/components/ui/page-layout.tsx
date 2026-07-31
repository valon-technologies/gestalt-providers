/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

// In-page page geometry: an optional full-width header band, a fixed-width start
// Pane for section navigation, a fluid content column, and an optional end Aside
// for a table of contents. Spec: guidelines/page-layout.md.
//
// This is NOT an app shell. `sidebar` is the fixed, viewport-height, collapsible
// chrome; `inspector-rail` is the resizable viewport-height end rail. PageLayout
// lives in document flow inside the page column, so the document keeps owning the
// scroll and every `sticky` offset in the app stays valid.
//
// Naming follows Primer, which draws this exact line: a Pane "sits only beside the
// content area", unlike a Sidebar that "spans adjacent to the header, content, and
// footer areas".
//
// The column itself (mx-auto w-full max-w-*) is NOT owned here — per
// guidelines/container.md the registry ships no Container primitive and no
// content-width token, so the consuming app wraps PageLayout in its own column.
//
// Track widths read `--page-layout-pane-width` / `--page-layout-aside-width`
// (defaults in globals.css). `tracks="compact"` narrows the Pane to 11rem for
// dense section rails; Settings keeps the default 13.75rem (220px).

/** Where the header sits relative to the Pane. */
type PageLayoutHeaderPlacement = "above" | "content";

type PageLayoutContextValue = {
  headerPlacement: PageLayoutHeaderPlacement;
};

const PageLayoutContext = React.createContext<PageLayoutContextValue | null>(null);

/**
 * Heading level the routed content should render its own title at.
 *
 * `2` when PageLayout rendered a header above the Pane — the page already owns an
 * `<h1>`, so content titles are `SectionHeader` (`h2`). `1` when it did not — the
 * content owns the `<h1>` via `PageHeader`. Keeps one `<h1>` per page
 * (guidelines/page-header.md) without the call site tracking it by hand.
 */
function usePageHeadingLevel(): 1 | 2 {
  const ctx = React.useContext(PageLayoutContext);
  return ctx?.headerPlacement === "above" ? 2 : 1;
}

// Row gap is the vertical rhythm between the header band and the columns; column
// gap is the gutter between Pane / Content / Aside. One `gap` step drives both so
// inner and outer rhythm come from one scale (cards.md: padding = grid gap).
const pageLayoutGapVariants = {
  default: "gap-8 lg:gap-10",
  compact: "gap-6 lg:gap-8",
} as const;

const pageLayoutVariants = cva("grid w-full grid-cols-1", {
  variants: {
    gap: pageLayoutGapVariants,
  },
  defaultVariants: {
    gap: "default",
  },
});

const pageLayoutTrackVariants = cva("", {
  variants: {
    tracks: {
      default: "",
      compact: "[--page-layout-pane-width:11rem]",
    },
  },
  defaultVariants: {
    tracks: "default",
  },
});

const pageLayoutColumnsVariants = cva("grid min-w-0 grid-cols-1", {
  variants: {
    gap: pageLayoutGapVariants,
  },
  defaultVariants: {
    gap: "default",
  },
});

const PAGE_LAYOUT_COLUMN_TEMPLATES = {
  paneAndAside:
    "lg:grid-cols-[var(--page-layout-pane-width)_minmax(0,1fr)] xl:grid-cols-[var(--page-layout-pane-width)_minmax(0,1fr)_var(--page-layout-aside-width)]",
  paneOnly: "lg:grid-cols-[var(--page-layout-pane-width)_minmax(0,1fr)]",
  asideOnly: "xl:grid-cols-[minmax(0,1fr)_var(--page-layout-aside-width)]",
} as const;

function pageLayoutColumnsTracksClassName({
  pane,
  aside,
}: {
  pane?: React.ReactNode;
  aside?: React.ReactNode;
}) {
  if (pane && aside) return PAGE_LAYOUT_COLUMN_TEMPLATES.paneAndAside;
  if (pane) return PAGE_LAYOUT_COLUMN_TEMPLATES.paneOnly;
  if (aside) return PAGE_LAYOUT_COLUMN_TEMPLATES.asideOnly;
  return "";
}

interface PageLayoutProps
  extends Omit<React.ComponentProps<"div">, "children">,
    VariantProps<typeof pageLayoutVariants>,
    VariantProps<typeof pageLayoutTrackVariants> {
  /**
   * Full-width band above the columns — normally a `PageHeader`.
   *
   * Render it here only when the header is invariant across every destination the
   * Pane offers (a Settings page whose rail switches sections *within* Settings).
   * When each Pane destination has its own title (a docs page), leave this unset
   * and let the content column own the `PageHeader` instead.
   */
  header?: React.ReactNode;
  /** Start-side rail content — normally a `NavList`. Hidden below `lg`. */
  pane?: React.ReactNode;
  /**
   * Stand-in shown *instead of* `pane` below `lg`, where the rail track collapses.
   * Supply a `SegmentedControl` for a handful of destinations, or a disclosure /
   * `Sheet` for a longer list. Omitting it means the navigation is unreachable on
   * small viewports — acceptable only when the Pane is a pure convenience (a TOC).
   */
  paneMobile?: React.ReactNode;
  /** End-side rail content — normally a `TableOfContents`. Hidden below `xl`. */
  aside?: React.ReactNode;
  /** Footer band below the columns, spanning the full width. */
  footer?: React.ReactNode;
  children: React.ReactNode;
}

/**
 * Page geometry root. Wrap it in the app's own centered column.
 *
 * Two columns unlock at `lg`; the end Aside waits for `xl` because three tracks
 * at `lg` leave the prose column too narrow to read.
 */
function PageLayout({
  className,
  gap,
  tracks,
  header,
  pane,
  paneMobile,
  aside,
  footer,
  children,
  ...props
}: PageLayoutProps) {
  const headerPlacement: PageLayoutHeaderPlacement = header ? "above" : "content";
  const ctx = React.useMemo<PageLayoutContextValue>(
    () => ({ headerPlacement }),
    [headerPlacement],
  );

  return (
    <PageLayoutContext.Provider value={ctx}>
      <div
        data-slot="page-layout"
        data-header-placement={headerPlacement}
        data-tracks={tracks ?? "default"}
        className={cn(
          pageLayoutVariants({ gap }),
          pageLayoutTrackVariants({ tracks }),
          className,
        )}
        {...props}
      >
        {header ? <PageLayoutHeader>{header}</PageLayoutHeader> : null}

        {paneMobile ? (
          <div
            data-slot="page-layout-pane-mobile"
            className="w-full min-w-0 overflow-x-auto lg:hidden"
          >
            {paneMobile}
          </div>
        ) : null}

        {/*
          One nested grid holds the tracks so the header and footer above/below can
          span the full width without being grid items of the same template. Source
          order carries side — there is no `position` prop, so the DOM order a
          screen reader and a keyboard walk always matches the visual order.
        */}
        <div
          data-slot="page-layout-columns"
          className={cn(
            pageLayoutColumnsVariants({ gap }),
            pageLayoutColumnsTracksClassName({ pane, aside }),
          )}
        >
          {pane ? <PageLayoutPane>{pane}</PageLayoutPane> : null}
          <PageLayoutContent>{children}</PageLayoutContent>
          {aside ? <PageLayoutAside>{aside}</PageLayoutAside> : null}
        </div>

        {footer ? <PageLayoutFooter>{footer}</PageLayoutFooter> : null}
      </div>
    </PageLayoutContext.Provider>
  );
}

/**
 * Full-width band above the columns. `PageLayout` renders this around `header`;
 * export it for layouts composed by hand.
 */
function PageLayoutHeader({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div data-slot="page-layout-header" className={cn("min-w-0", className)} {...props} />
  );
}

// The sticky offset must clear whatever fixed chrome the app puts above the page.
// `--page-layout-pane-top` is the seam: apps set it once (next to their nav-height
// token) instead of scattering a magic `top-*` across every page. 0px is correct
// for an app with no fixed chrome, so the default is safe rather than merely inert.
const pageLayoutPaneVariants = cva(
  "hidden min-w-0 lg:block",
  {
    variants: {
      sticky: {
        // `overflow-y-auto` computes a non-visible x overflow too, which clips
        // NavList's standard outward focus ring. Keep one spacing step of
        // scrollport breathing room so the ring remains fully visible.
        true: "lg:sticky lg:top-[var(--page-layout-pane-top,0px)] lg:max-h-[calc(100svh-var(--page-layout-pane-top,0px)-var(--page-layout-pane-bottom,0px))] lg:overflow-y-auto lg:overscroll-contain lg:p-1",
        false: "",
      },
    },
    defaultVariants: {
      sticky: true,
    },
  },
);

interface PageLayoutPaneProps
  extends React.ComponentProps<"div">,
    VariantProps<typeof pageLayoutPaneVariants> {}

/**
 * Start-side rail track. A plain `div`, not `<aside>` — the `<nav aria-label>`
 * the consumer puts inside is the landmark; an unlabelled `complementary` wrapper
 * around it would be landmark noise (banner.md: label it or it is not one).
 */
function PageLayoutPane({ className, sticky, ...props }: PageLayoutPaneProps) {
  return (
    <div
      data-slot="page-layout-pane"
      className={cn(pageLayoutPaneVariants({ sticky }), className)}
      {...props}
    />
  );
}

/** Fluid content column. Renders `<main>`; `min-w-0` so wide tables can scroll. */
function PageLayoutContent({ className, ...props }: React.ComponentProps<"main">) {
  return (
    <main data-slot="page-layout-content" className={cn("min-w-0", className)} {...props} />
  );
}

const pageLayoutAsideVariants = cva("hidden min-w-0 xl:block", {
  variants: {
    sticky: {
      // The Aside is the same clipping scrollport as the Pane; keep focus rings
      // visible for TableOfContents buttons and any other interactive content.
      true: "xl:sticky xl:top-[var(--page-layout-pane-top,0px)] xl:max-h-[calc(100svh-var(--page-layout-pane-top,0px)-var(--page-layout-pane-bottom,0px))] xl:overflow-y-auto xl:overscroll-contain xl:p-1",
      false: "",
    },
  },
  defaultVariants: {
    sticky: true,
  },
});

interface PageLayoutAsideProps
  extends React.ComponentProps<"div">,
    VariantProps<typeof pageLayoutAsideVariants> {}

/**
 * End-side rail track, for a `TableOfContents`. Also a plain `div` — same landmark
 * reasoning as the Pane. Appears at `xl`, one step later than the Pane.
 */
function PageLayoutAside({ className, sticky, ...props }: PageLayoutAsideProps) {
  return (
    <div
      data-slot="page-layout-aside"
      className={cn(pageLayoutAsideVariants({ sticky }), className)}
      {...props}
    />
  );
}

/** Full-width band below the columns. */
function PageLayoutFooter({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div data-slot="page-layout-footer" className={cn("min-w-0", className)} {...props} />
  );
}

export {
  PageLayout,
  PageLayoutHeader,
  PageLayoutPane,
  PageLayoutContent,
  PageLayoutAside,
  PageLayoutFooter,
  usePageHeadingLevel,
  pageLayoutVariants,
  pageLayoutTrackVariants,
  pageLayoutPaneVariants,
  pageLayoutAsideVariants,
  PAGE_LAYOUT_COLUMN_TEMPLATES,
};
