import { useState } from "react";
import { Link, Outlet, useRouterState } from "@tanstack/react-router";
import Container from "@/components/Container";
import { PageLayout } from "@/components/ui/page-layout";
import { PageLayoutPaneMobileNav } from "@/components/ui/page-layout-pane-mobile-nav";
import {
  NavList,
  NavListItem,
  NavListItemLabel,
} from "@/components/ui/nav-list";
import {
  ADMIN_SECTIONS_NAV_LABEL,
} from "./admin-access-copy";
import { ADMIN_NAV, adminNavIdForPathname } from "./admin-nav";

export function AdminLayout() {
  const pathname = useRouterState({
    select: (state) => {
      // Outlet renders `matches`. The rail must use that same committed tree so
      // a pending URL cannot highlight a section the center column has not shown.
      for (let index = state.matches.length - 1; index >= 0; index -= 1) {
        const path = state.matches[index]?.pathname;
        if (path) return path;
      }
      return state.location.pathname;
    },
  });
  const activeId = adminNavIdForPathname(pathname);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  // Versions is a table: omit the empty aside so the content track uses that
  // column, and drop the 65ch docs measure. Follow the committed route so the
  // pane does not resize while Outlet still shows the previous page.
  const expandContent = activeId === "versions";

  const pane = (
    <NavList aria-label={ADMIN_SECTIONS_NAV_LABEL}>
      {ADMIN_NAV.map((item) => (
        <NavListItem key={item.id} asChild active={activeId === item.id}>
          <Link
            to={item.to}
            onClick={() => setMobileNavOpen(false)}
            data-testid={`admin-nav-${item.id}`}
          >
            <NavListItemLabel>{item.label}</NavListItemLabel>
          </Link>
        </NavListItem>
      ))}
    </NavList>
  );

  return (
    <Container>
      <PageLayout
        tracks="compact"
        pane={pane}
        paneMobile={
          <PageLayoutPaneMobileNav
            open={mobileNavOpen}
            onOpenChange={setMobileNavOpen}
            panelLabel={ADMIN_SECTIONS_NAV_LABEL}
          >
            {pane}
          </PageLayoutPaneMobileNav>
        }
        aside={expandContent ? undefined : <div aria-hidden="true" />}
      >
        <div
          className={
            expandContent
              ? "min-w-0 w-full"
              : "min-w-0 w-full max-w-[65ch]"
          }
        >
          <Outlet />
        </div>
      </PageLayout>
    </Container>
  );
}
