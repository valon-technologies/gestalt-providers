import { useCallback, useLayoutEffect, useMemo, useRef } from "react";
import { useNavigate, useRouterState } from "@tanstack/react-router";
import Container from "@/components/Container";
import { PageLayout } from "@/components/ui/page-layout";
import {
  TableOfContents,
  type TableOfContentsItem,
} from "@/components/ui/table-of-contents";
import { useScrollSpy } from "@/hooks/use-scroll-spy";
import { usePageLayoutAnchorOffsetPx } from "@/lib/page-layout-anchor-offset";
import { pageLayoutContentTopStyle, PAGE_LAYOUT_READING_COLUMN_CLASS } from "@/lib/page-layout-content-top";
import { getActiveDocsNavItem } from "./docs-data";
import { DocsAudienceCallout } from "./DocsAudienceCallout";
import { DocsMobileNav, DocsNavList } from "./DocsMobileNav";
import { DocsJourneyFooter } from "./DocsJourneyFooter";

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
  const navigate = useNavigate();

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
  // Hash-backed option switchers omit matching DOM ids — scroll to the
  // switcher that owns this hash so `#agent-codex` does not land on Install.
  useLayoutEffect(() => {
    const id = locationHash.replace(/^#/, "");
    if (!id) return;
    const heading = document.getElementById(id);
    if (heading) {
      activate(id);
      heading.scrollIntoView({ block: "start" });
      return;
    }
    const switcher = document.querySelector<HTMLElement>(
      `[data-docs-hash-ids~="${CSS.escape(id)}"]`,
    );
    switcher?.scrollIntoView({ block: "start" });
  }, [activate, locationHash, pathname, sectionsKey]);

  const onTocSelect = useCallback(
    (id: string) => {
      const el = document.getElementById(id);
      if (!el) return;
      activate(id);
      void navigate({ to: pathname, hash: id, replace: true });
      el.scrollIntoView({ behavior: "smooth", block: "start" });
    },
    [activate, navigate, pathname],
  );

  return (
    <Container className="pb-16 pt-16">
      {/*
        Scope chrome CSS vars here (Container does not forward `style`; PageLayout
        does not forward `ref`). Sticky rails + scroll-mt inherit; scroll-spy
        probes from this node so it sees the docs override, not `:root`.
      */}
      <div ref={pageLayoutRef} style={pageLayoutContentTopStyle}>
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
          <article className={PAGE_LAYOUT_READING_COLUMN_CLASS}>
            {/*
              On-this-page lives only in PageLayout Aside (xl+); no stacked
              duplicate below that breakpoint — nav already covers the page.
            */}
            {activeItem.audience === "admin" ? <DocsAudienceCallout /> : null}
            {children}
            <DocsJourneyFooter item={activeItem} />
          </article>
        </PageLayout>
      </div>
    </Container>
  );
}
