import { Link, useMatchRoute } from "@tanstack/react-router";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
} from "@/components/ui/sidebar";
import type { AppAdminNavId, AppUserNavId } from "./app-nav";

type NavItem = {
  id: AppUserNavId | AppAdminNavId;
  label: string;
  to: string;
};

function WorkspaceNavItem({
  app,
  item,
  testId,
  exact = false,
}: {
  app: string;
  item: NavItem;
  testId: string;
  exact?: boolean;
}) {
  const matchRoute = useMatchRoute();
  const isActive = Boolean(
    matchRoute({
      to: item.to,
      params: { app },
      fuzzy: !exact,
    }),
  );

  return (
    <SidebarMenuItem>
      <SidebarMenuButton asChild isActive={isActive}>
        <Link to={item.to} params={{ app }} data-testid={testId}>
          {item.label}
        </Link>
      </SidebarMenuButton>
    </SidebarMenuItem>
  );
}

export function AppWorkspaceNav({
  app,
  userItems,
  adminItems,
  adminGroupVisible,
}: {
  app: string;
  userItems: ReadonlyArray<NavItem>;
  adminItems: ReadonlyArray<NavItem>;
  adminGroupVisible: boolean;
}) {
  return (
    <SidebarProvider defaultWidth="11rem" className="min-h-0 w-full">
      <Sidebar collapsible="none" className="h-full">
        <SidebarContent className="overflow-visible" aria-label="App workspace">
          <SidebarGroup className="p-0">
            <SidebarGroupContent>
              <SidebarMenu>
                {userItems.map((item) => (
                  <WorkspaceNavItem
                    key={item.id}
                    app={app}
                    item={item}
                    exact={item.id === "overview"}
                    testId={`app-workspace-nav-${item.id}`}
                  />
                ))}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>

          {adminGroupVisible && adminItems.length > 0 ? (
            <SidebarGroup className="mt-2 p-0">
              <SidebarGroupLabel>Admin</SidebarGroupLabel>
              <SidebarGroupContent>
                <SidebarMenu>
                  {adminItems.map((item) => (
                    <WorkspaceNavItem
                      key={item.id}
                      app={app}
                      item={item}
                      testId={`app-admin-nav-${item.id}`}
                    />
                  ))}
                </SidebarMenu>
              </SidebarGroupContent>
            </SidebarGroup>
          ) : null}
        </SidebarContent>
      </Sidebar>
    </SidebarProvider>
  );
}
