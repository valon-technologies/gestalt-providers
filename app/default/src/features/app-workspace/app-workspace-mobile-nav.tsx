import { useEffect, useState } from "react";
import { PageLayoutPaneMobileNav } from "@/components/ui/page-layout-pane-mobile-nav";
import type { AppAdminNavId, AppUserNavId } from "./app-nav";
import { AppWorkspaceNav } from "./app-workspace-nav";

type WorkspaceNavId = AppUserNavId | AppAdminNavId;

type NavItem = {
  id: WorkspaceNavId;
  label: string;
  to: string;
};

/**
 * Mobile stand-in for the app-workspace Pane: same NavList (incl. Admin group)
 * inside PageLayoutPaneMobileNav (Menu bar + caret → full-width disclosure).
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

  useEffect(() => {
    setOpen(false);
  }, [pathname]);

  if (destinationCount < 2) return null;

  return (
    <PageLayoutPaneMobileNav
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
