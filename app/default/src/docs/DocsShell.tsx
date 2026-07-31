import { Link, useRouterState } from "@tanstack/react-router";
import Container from "@/components/Container";
import {
  NavList,
  NavListGroup,
  NavListItem,
  NavListItemLabel,
} from "@/components/ui/nav-list";
import { docsNavItems, getActiveDocsNavItem } from "./docs-data";

export default function DocsShell({
  children,
}: {
  children: React.ReactNode;
}) {
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const activeItem = getActiveDocsNavItem(pathname);

  return (
    <Container as="main" className="py-16">
      <div className="grid gap-10 xl:grid-cols-[11rem_minmax(0,1fr)_240px]">
        <div className="hidden w-44 shrink-0 xl:block">
          <div className="sticky top-[var(--page-layout-pane-top)]">
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
          </div>
        </div>

        <article className="min-w-0">{children}</article>

        <div className="hidden xl:block">
          <div className="sticky top-[var(--page-layout-pane-top)] space-y-6">
            {activeItem.subsections.length > 0 && (
              <NavList aria-label="On This Page">
                <NavListGroup label="On This Page">
                  {activeItem.subsections.map((subsection) => (
                    <NavListItem
                      key={subsection.id}
                      href={`#${subsection.id}`}
                    >
                      <NavListItemLabel>{subsection.label}</NavListItemLabel>
                    </NavListItem>
                  ))}
                </NavListGroup>
              </NavList>
            )}
          </div>
        </div>
      </div>
    </Container>
  );
}
