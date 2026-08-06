import { Link, useRouterState } from "@tanstack/react-router";
import Container from "@/components/Container";
import {
  NavList,
  NavListGroup,
  NavListItem,
  NavListItemLabel,
} from "@/components/ui/nav-list";
import { PageLayout } from "@/components/ui/page-layout";
import { docsNavItems, getActiveDocsNavItem } from "./docs-data";

export default function DocsShell({
  children,
}: {
  children: React.ReactNode;
}) {
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const activeItem = getActiveDocsNavItem(pathname);
  const hasOnThisPage = activeItem.subsections.length > 0;

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
            <NavList aria-label="On This Page">
              <NavListGroup label="On This Page">
                {activeItem.subsections.map((subsection) => (
                  <NavListItem key={subsection.id} href={`#${subsection.id}`}>
                    <NavListItemLabel>{subsection.label}</NavListItemLabel>
                  </NavListItem>
                ))}
              </NavListGroup>
            </NavList>
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
