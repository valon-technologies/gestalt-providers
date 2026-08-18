import { useEffect, useState } from "react";
import { Link } from "@tanstack/react-router";
import { PageLayoutPaneMobileNav } from "@/components/ui/page-layout-pane-mobile-nav";
import {
  NavList,
  NavListGroup,
  NavListItem,
  NavListItemLabel,
} from "@/components/ui/nav-list";
import {
  DOCS_NAV_GROUPS,
  docsNavItemsByGroup,
  type DocsNavItem,
} from "./docs-data";

/**
 * Mobile stand-in for the docs Pane. Same destinations as the desktop rail,
 * inside PageLayoutPaneMobileNav (Menu bar → overlay). SegmentedControl is
 * wrong for grouped docs destinations.
 */
export function DocsMobileNav({
  pathname,
  activeItem,
}: {
  pathname: string;
  activeItem: DocsNavItem;
}) {
  const [open, setOpen] = useState(false);

  useEffect(() => {
    setOpen(false);
  }, [pathname]);

  return (
    <PageLayoutPaneMobileNav
      open={open}
      onOpenChange={setOpen}
      panelLabel="Documentation"
      aria-label={`Documentation, current: ${activeItem.label}`}
      data-testid="docs-mobile-nav"
    >
      <DocsNavList activeId={activeItem.id} />
    </PageLayoutPaneMobileNav>
  );
}

export function DocsNavList({ activeId }: { activeId: string }) {
  return (
    <NavList aria-label="Documentation">
      {DOCS_NAV_GROUPS.map((group) => {
        const items = docsNavItemsByGroup(group.id);
        if (items.length === 0) return null;
        return (
          <NavListGroup key={group.id} label={group.label}>
            {items.map((item) => (
              <NavListItem
                key={item.id}
                asChild
                active={item.id === activeId}
              >
                <Link to={item.href}>
                  <NavListItemLabel>{item.label}</NavListItemLabel>
                </Link>
              </NavListItem>
            ))}
          </NavListGroup>
        );
      })}
    </NavList>
  );
}
