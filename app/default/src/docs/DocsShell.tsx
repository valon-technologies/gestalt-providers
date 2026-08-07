import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { Link, useRouterState } from "@tanstack/react-router";
import Container from "@/components/Container";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import {
  NavList,
  NavListItem,
  NavListItemLabel,
} from "@/components/ui/nav-list";
import { PageLayout } from "@/components/ui/page-layout";
import { PageLayoutPaneMobileNav } from "@/components/ui/page-layout-pane-mobile-nav";
import {
  TableOfContents,
  type TableOfContentsItem,
} from "@/components/ui/table-of-contents";
import { useScrollSpy } from "@/hooks/use-scroll-spy";
import { usePageLayoutAnchorOffsetPx } from "@/lib/page-layout-anchor-offset";
import { docsNavItems, getActiveDocsNavItem } from "./docs-data";

export default function DocsShell({
  children,
}: {
  children: React.ReactNode;
}) {
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const activeItem = getActiveDocsNavItem(pathname);
  const hasOnThisPage = activeItem.subsections.length > 0;
  const [mobileNavOpen, setMobileNavOpen] = useState(false);

  useEffect(() => {
    setMobileNavOpen(false);
  }, [pathname]);

  const tocItems = useMemo((): TableOfContentsItem[] => {
    return activeItem.subsections.map((subsection) => ({
      id: subsection.id,
      title: subsection.label,
      depth: 1,
    }));
  }, [activeItem.subsections]);

  const scrollRootRef = useRef<HTMLElement | null>(null);
  const activationOffset = usePageLayoutAnchorOffsetPx();

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

  const nav = (
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
  );

  return (
    <Container className="pb-16">
      <PageLayout
        tracks="compact"
        pane={nav}
        paneMobile={
          <PageLayoutPaneMobileNav
            open={mobileNavOpen}
            onOpenChange={setMobileNavOpen}
            panelLabel="Documentation sections"
          >
            {nav}
          </PageLayoutPaneMobileNav>
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
        <article className="min-w-0">
          <Breadcrumb className="mt-6 mb-6">
            <BreadcrumbList>
              <BreadcrumbItem>
                <BreadcrumbLink asChild>
                  <Link to="/docs/getting-started">Docs</Link>
                </BreadcrumbLink>
              </BreadcrumbItem>
              <BreadcrumbSeparator />
              <BreadcrumbItem>
                <BreadcrumbPage>{activeItem.label}</BreadcrumbPage>
              </BreadcrumbItem>
            </BreadcrumbList>
          </Breadcrumb>
          {children}
        </article>
      </PageLayout>
    </Container>
  );
}
