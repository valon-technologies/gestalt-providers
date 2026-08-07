import { useCallback, useLayoutEffect, useMemo, useRef, useState } from "react";
import { Link, useRouterState } from "@tanstack/react-router";
import Container from "@/components/Container";
import {
  NavList,
  NavListItem,
  NavListItemLabel,
} from "@/components/ui/nav-list";
import { PageLayout } from "@/components/ui/page-layout";
import {
  TableOfContents,
  type TableOfContentsItem,
} from "@/components/ui/table-of-contents";
import { useScrollSpy } from "@/hooks/use-scroll-spy";
import { docsNavItems, getActiveDocsNavItem } from "./docs-data";

/** Fallback before CSS vars resolve — nav + gap (~65px + 24px). */
const DOCS_TOC_ACTIVATION_OFFSET_FALLBACK = 112;

function readPageLayoutPaneTopPx(): number {
  const raw = getComputedStyle(document.documentElement)
    .getPropertyValue("--page-layout-pane-top")
    .trim();
  const px = Number.parseFloat(raw);
  return Number.isFinite(px) ? px : DOCS_TOC_ACTIVATION_OFFSET_FALLBACK;
}

export default function DocsShell({
  children,
}: {
  children: React.ReactNode;
}) {
  const pathname = useRouterState({ select: (state) => state.location.pathname });
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
  const [activationOffset, setActivationOffset] = useState(
    DOCS_TOC_ACTIVATION_OFFSET_FALLBACK,
  );

  useLayoutEffect(() => {
    scrollRootRef.current = document.documentElement;
    const syncOffset = () => setActivationOffset(readPageLayoutPaneTopPx());
    syncOffset();
    window.addEventListener("resize", syncOffset);
    const ro =
      typeof ResizeObserver !== "undefined"
        ? new ResizeObserver(syncOffset)
        : null;
    const chrome = document.querySelector("[data-slot='app-sticky-chrome']");
    if (chrome && ro) ro.observe(chrome);
    return () => {
      window.removeEventListener("resize", syncOffset);
      ro?.disconnect();
    };
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
    <Container className="py-16">
      <PageLayout
        pane={
          <NavList aria-label="Documentation">
            {docsNavItems.map((item) => (
              <NavListItem
                key={item.id}
                asChild
                active={item.id === activeItem.id}
              >
                <Link to={item.href}>
                  <NavListItemLabel>{item.label}</NavListItemLabel>
                </Link>
              </NavListItem>
            ))}
          </NavList>
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
        <article className="min-w-0">{children}</article>
      </PageLayout>
    </Container>
  );
}
