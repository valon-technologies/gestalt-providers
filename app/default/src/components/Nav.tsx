import { Link, useRouterState } from "@tanstack/react-router";
import { clearSession, sessionDisplayLabel, sessionInitials } from "@/lib/auth";
import { appPath } from "@/lib/mount";
import { useAuthInfoQuery, useAuthSessionQuery, useGestaltAdminQuery } from "@/lib/queries";
import { canShowAdminNav } from "@/features/admin-access/admin-access-gate";
import { AccountMenu } from "./AccountMenu";
import { chromeProductNav } from "./chrome-nav";
import {
  AppTopBar,
  AppTopBarBrand,
  AppTopBarCenter,
  AppTopBarEnd,
  AppTopBarInner,
  AppTopBarStart,
} from "./ui/app-top-bar";
import { AppLogoName } from "./ui/app-logo";
import {
  NavigationMenu,
  NavigationMenuItem,
  NavigationMenuLink,
  NavigationMenuList,
} from "./ui/navigation-menu";
import { ThemeToggle } from "./ui/theme-toggle";

export default function Nav() {
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const sessionQuery = useAuthSessionQuery();
  const session = sessionQuery.data ?? null;
  const displayLabel = sessionDisplayLabel(session);
  const initials = sessionInitials(session);
  const authInfoQuery = useAuthInfoQuery(!!displayLabel);
  const loginSupported = authInfoQuery.data?.loginSupported ?? false;
  const gestaltAdminQuery = useGestaltAdminQuery({
    enabled: Boolean(displayLabel),
  });
  const showAdmin = canShowAdminNav(gestaltAdminQuery.data);
  const links = chromeProductNav(showAdmin);

  async function handleLogout() {
    clearSession();
    const returnTo = encodeURIComponent(appPath("/"));
    window.location.href = `/api/v1/auth/logout?returnTo=${returnTo}`;
  }

  return (
    <AppTopBar>
      <AppTopBarInner>
        <AppTopBarStart>
          {/* SPA link semantics via asChild — caller supplies AppLogoName inside Link. */}
          <AppTopBarBrand size="lg" asChild>
            <Link to="/apps">
              <AppLogoName size="lg">Gestalt</AppLogoName>
            </Link>
          </AppTopBarBrand>
        </AppTopBarStart>

        {/* Primary destinations — keep visible below lg (no Sheet mobile nav yet). */}
        <AppTopBarCenter className="flex">
          <NavigationMenu
            viewport={false}
            size="lg"
            aria-label="Primary"
            className="max-w-none flex-none"
          >
            <NavigationMenuList>
              {links.map((link) => {
                const isActive =
                  pathname === link.to || pathname.startsWith(link.to + "/");
                return (
                  <NavigationMenuItem key={link.to}>
                    <NavigationMenuLink asChild active={isActive}>
                      <Link to={link.to}>{link.label}</Link>
                    </NavigationMenuLink>
                  </NavigationMenuItem>
                );
              })}
            </NavigationMenuList>
          </NavigationMenu>
        </AppTopBarCenter>

        <AppTopBarEnd className="gap-3">
          {/* Theme lives in AccountMenu when signed in; header access for guests. */}
          {!displayLabel && <ThemeToggle placement="header" size="sm" />}
          {displayLabel && (
            <AccountMenu
              displayLabel={displayLabel}
              email={session?.email}
              initials={initials}
              loginSupported={loginSupported}
              onLogout={handleLogout}
            />
          )}
        </AppTopBarEnd>
      </AppTopBarInner>
    </AppTopBar>
  );
}
