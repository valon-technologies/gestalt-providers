import { Link, useRouterState } from "@tanstack/react-router";
import Container from "@/components/Container";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
} from "@/components/ui/sidebar";
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
        <aside className="hidden w-44 shrink-0 xl:block">
          <div className="sticky top-24">
            <SidebarProvider defaultWidth="11rem" className="min-h-0 w-full">
              <Sidebar collapsible="none" className="h-full">
                <SidebarContent
                  className="overflow-visible"
                  aria-label="Documentation"
                >
                  <SidebarGroup className="p-0">
                    <SidebarGroupContent>
                      <SidebarMenu>
                        {docsNavItems.map((item) => (
                          <SidebarMenuItem key={item.id}>
                            <SidebarMenuButton
                              asChild
                              isActive={item.id === activeItem.id}
                            >
                              <Link to={item.href}>{item.label}</Link>
                            </SidebarMenuButton>
                          </SidebarMenuItem>
                        ))}
                      </SidebarMenu>
                    </SidebarGroupContent>
                  </SidebarGroup>
                </SidebarContent>
              </Sidebar>
            </SidebarProvider>
          </div>
        </aside>

        <article className="min-w-0">{children}</article>

        <aside className="hidden xl:block">
          <div className="sticky top-24 space-y-6">
            {activeItem.subsections.length > 0 && (
              <div>
                <p className="text-xs font-medium uppercase tracking-[0.16em] text-muted-foreground-soft">
                  On This Page
                </p>
                <nav className="mt-3 space-y-0.5">
                  {activeItem.subsections.map((subsection) => (
                    <a
                      key={subsection.id}
                      href={`#${subsection.id}`}
                      className="block border-l-2 border-transparent py-1.5 pl-3 text-sm text-muted-foreground transition-colors duration-150 hover:border-base-300 hover:text-foreground dark:hover:border-base-600"
                    >
                      {subsection.label}
                    </a>
                  ))}
                </nav>
              </div>
            )}
          </div>
        </aside>
      </div>
    </Container>
  );
}
