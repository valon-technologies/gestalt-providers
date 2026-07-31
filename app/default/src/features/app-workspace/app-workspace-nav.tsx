import { Link, useMatchRoute } from "@tanstack/react-router";
import {
  NavList,
  NavListGroup,
  NavListItem,
  NavListItemLabel,
} from "@/components/ui/nav-list";
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
    <NavListItem asChild active={isActive}>
      <Link to={item.to} params={{ app }} data-testid={testId}>
        <NavListItemLabel>{item.label}</NavListItemLabel>
      </Link>
    </NavListItem>
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
    <NavList aria-label="App workspace">
      {userItems.map((item) => (
        <WorkspaceNavItem
          key={item.id}
          app={app}
          item={item}
          exact={item.id === "overview"}
          testId={`app-workspace-nav-${item.id}`}
        />
      ))}

      {adminGroupVisible && adminItems.length > 0 ? (
        <NavListGroup label="Admin" className="mt-2">
          {adminItems.map((item) => (
            <WorkspaceNavItem
              key={item.id}
              app={app}
              item={item}
              testId={`app-admin-nav-${item.id}`}
            />
          ))}
        </NavListGroup>
      ) : null}
    </NavList>
  );
}
