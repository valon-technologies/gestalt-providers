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
  items: ReadonlyArray<NavItem>,
): string | null {
  const exact = items.find((item) => item.to === pathname);
  if (exact) return exact.label;
  const prefix = items
    .filter((item) => pathname.startsWith(`${item.to}/`))
    .sort((a, b) => b.to.length - a.to.length)[0];
  return prefix?.label ?? null;
}

/**
 * Mobile stand-in for the app-workspace Pane: same NavList (incl. Admin group)
 * inside PageLayoutPaneMobileNav (Menu bar + caret → full-width disclosure).
 * Visible bar stays "Menu"; aria-label keeps the current section for SR users.
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

  const activeLabel = useMemo(() => {
    const items = [
      ...userItems,
      ...(adminGroupVisible ? adminItems : []),
    ];
    return activeWorkspaceNavLabel(pathname, items);
  }, [adminGroupVisible, adminItems, pathname, userItems]);

  useEffect(() => {
    setOpen(false);
  }, [pathname]);

  if (destinationCount < 2) return null;

  return (
    <PageLayoutPaneMobileNav
      open={open}
      onOpenChange={setOpen}
      panelLabel="App sections"
      aria-label={
        activeLabel
          ? `App sections, current: ${activeLabel}`
          : "App sections"
      }
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
