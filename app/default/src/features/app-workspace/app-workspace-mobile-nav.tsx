import { useEffect, useMemo, useState } from "react";
import { PageLayoutPaneMobileNav } from "@/components/ui/page-layout-pane-mobile-nav";
import type { AppAdminNavId, AppUserNavId } from "./app-nav";
import { AppWorkspaceNav } from "./app-workspace-nav";

type WorkspaceNavId = AppUserNavId | AppAdminNavId;

type NavItem = {
  id: WorkspaceNavId;
  label: string;
  to: string;
};

function activeWorkspaceNavLabel(
  pathname: string,
  app: string,
  userItems: ReadonlyArray<NavItem>,
  adminItems: ReadonlyArray<NavItem>,
  adminGroupVisible: boolean,
): string {
  const items = [
    ...userItems,
    ...(adminGroupVisible ? adminItems : []),
  ];
  for (const item of items) {
    if (item.id === "overview") continue;
    const path = item.to.replace("$app", app);
    if (pathname === path || pathname.startsWith(`${path}/`)) {
      return item.label;
    }
  }
  return items.find((item) => item.id === "overview")?.label ?? "Overview";
}

/**
 * Mobile stand-in for the app-workspace Pane: same NavList (incl. Admin group)
 * inside PageLayoutPaneMobileNav (menu + label → left Sheet).
 */
export function AppWorkspaceMobileNav({
  app,
  pathname,
  userItems,
  adminItems,
  adminGroupVisible,
}: {
  app: string;
  pathname: string;
  userItems: ReadonlyArray<NavItem>;
  adminItems: ReadonlyArray<NavItem>;
  adminGroupVisible: boolean;
}) {
  const [open, setOpen] = useState(false);
  const destinationCount =
    userItems.length + (adminGroupVisible ? adminItems.length : 0);

  const activeLabel = useMemo(
    () =>
      activeWorkspaceNavLabel(
        pathname,
        app,
        userItems,
        adminItems,
        adminGroupVisible,
      ),
    [adminGroupVisible, adminItems, app, pathname, userItems],
  );

  useEffect(() => {
    setOpen(false);
  }, [pathname]);

  if (destinationCount < 2) return null;

  return (
    <PageLayoutPaneMobileNav
      label={activeLabel}
      open={open}
      onOpenChange={setOpen}
      data-testid="app-workspace-mobile-nav"
    >
      <AppWorkspaceNav
        app={app}
        userItems={userItems}
        adminItems={adminItems}
        adminGroupVisible={adminGroupVisible}
      />
    </PageLayoutPaneMobileNav>
  );
}
