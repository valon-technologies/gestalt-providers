import {
  useCallback,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
} from "react";
import { useRouterState } from "@tanstack/react-router";
import Container from "@/components/Container";
import { PageLayout } from "@/components/ui/page-layout";
import {
  TableOfContents,
  type TableOfContentsItem,
} from "@/components/ui/table-of-contents";
import { useScrollSpy } from "@/hooks/use-scroll-spy";
import { usePageLayoutAnchorOffsetPx } from "@/lib/page-layout-anchor-offset";
import { getActiveDocsNavItem } from "./docs-data";
import { DocsAudienceCallout } from "./DocsAudienceCallout";
import { DocsMobileNav, DocsNavList } from "./DocsMobileNav";
import { DocsJourneyFooter } from "./DocsOnThisPage";
import { DOCS_PAGE_TOP_GAP } from "./docs-chrome";

/** Same seam for sticky rails, hash / TOC scroll-mt, and (via DocsPageBody) h2 gaps. */
const docsShellStyle = {
  "--page-layout-pane-top": `calc(var(--app-sticky-chrome-height) + ${DOCS_PAGE_TOP_GAP})`,
  "--page-layout-anchor-offset": `calc(var(--app-sticky-chrome-height) + ${DOCS_PAGE_TOP_GAP})`,
} as CSSProperties;

export default function DocsShell({
  children,
}: {
  children: React.ReactNode;
}) {
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const activeItem = getActiveDocsNavItem(pathname);
  const hasOnThisPage = activeItem.subsections.length > 0;

  const tocItems = useMemo((): TableOfContentsItem[] => {
    return activeItem.subsections.map((subsection) => ({
      id: subsection.id,
      title: subsection.label,
      depth: 1,
    }));
  }, [activeItem.subsections]);

  const scrollRootRef = useRef<HTMLElement | null>(null);
  const pageLayoutRef = useRef<HTMLDivElement | null>(null);
  // Probe from PageLayout so docs' overridden --page-layout-anchor-offset wins
  // over :root (scroll-mt and scroll-spy stay on the same seam).
  const activationOffset = usePageLayoutAnchorOffsetPx(
    undefined,
    pageLayoutRef,
  );
  const locationHash = useRouterState({
    select: (state) => state.location.hash,
  });

  useLayoutEffect(() => {
    scrollRootRef.current = document.documentElement;
  }, []);

  const sectionsKey = `${pathname}:${tocItems.map((item) => item.id).join(",")}`;
  const getEntries = useCallback(() => {
    return tocItems.flatMap((item) => {
      if (item.kind === "separator") return [];
      const el = document.getElementById(item.id);
      return el
        ? [{ id: item.id, top: el.getBoundingClientRect().top }]
        : [];
    });
  }, [tocItems]);

  const { activeId, activate } = useScrollSpy({
    scrollRootRef,
    getEntries,
    sectionsKey,
    activationOffset,
    forceLastAtBottom: true,
    enabled: hasOnThisPage,
    observeWindow: true,
  });

  // Client navigations land under sticky chrome (worktree banner + top bar).
  // Root publishes `--app-sticky-chrome-height` in its layout effect first;
  // `scroll-mt` on the hash target then clears that measured stack.
  useLayoutEffect(() => {
    const id = locationHash.replace(/^#/, "");
    if (!id) return;
    const el = document.getElementById(id);
    if (!el) return;
    activate(id);
    el.scrollIntoView({ block: "start" });
  }, [activate, locationHash, pathname, sectionsKey]);

  const onTocSelect = useCallback(
    (id: string) => {
      const el = document.getElementById(id);
      if (!el) return;
      activate(id);
      const url = new URL(window.location.href);
      url.hash = id;
      window.history.replaceState(null, "", url);
      el.scrollIntoView({ behavior: "smooth", block: "start" });
    },
    [activate],
  );

  return (
    <Container className="pb-16 pt-16">
      {/*
        Scope chrome CSS vars here (Container does not forward `style`; PageLayout
        does not forward `ref`). Sticky rails + scroll-mt inherit; scroll-spy
        probes from this node so it sees the docs override, not `:root`.
      */}
      <div ref={pageLayoutRef} style={docsShellStyle}>
        <PageLayout
          tracks="compact"
          pane={<DocsNavList activeId={activeItem.id} />}
          paneMobile={
            <DocsMobileNav pathname={pathname} activeItem={activeItem} />
          }
          // Always pass an Aside so PageLayout keeps the three-track template
          // (pane | content | aside). Omitting it collapses to pane+content and
          // the center column grows on pages without an on-this-page list.
          aside={
            hasOnThisPage ? (
              <TableOfContents
                items={tocItems}
                activeId={activeId}
                onItemSelect={onTocSelect}
                label="On this page"
              />
            ) : (
              <div aria-hidden="true" />
            )
          }
        >
          <article className="mx-auto min-w-0 w-full max-w-[65ch]">
            {/*
              Reading measure: center track is ~800px (~82–110ch) without a cap.
              65ch keeps body lines in the comfortable 60–70 character band;
              `mx-auto` centers that measure in the PageLayout content track.
            */}
            {hasOnThisPage ? (
              <div
                className="mb-6 rounded-xl border border-border bg-card p-4 xl:hidden"
                data-testid="docs-on-this-page-mobile"
              >
                <p className="mb-2 text-sm font-medium text-foreground">
                  On this page
                </p>
                <TableOfContents
                  items={tocItems}
                  activeId={activeId}
                  onItemSelect={onTocSelect}
                  label="On this page"
                />
              </div>
            ) : null}
            {activeItem.audience === "admin" ? <DocsAudienceCallout /> : null}
            {children}
            <DocsJourneyFooter item={activeItem} />
          </article>
        </PageLayout>
      </div>
    </Container>
  );
}
