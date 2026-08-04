import { Link, useRouterState } from "@tanstack/react-router";
import { logout } from "@/lib/api";
import { clearSession, sessionDisplayLabel, sessionInitials } from "@/lib/auth";
import { BUILD_PATH } from "@/lib/constants";
import { serverLoginURL } from "@/lib/authReturn";
import { appPath } from "@/lib/mount";
import { useAuthInfoQuery, useAuthSessionQuery } from "@/lib/queries";
import { AccountMenu } from "./AccountMenu";
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

const links = [
  { href: "/apps", label: "Apps" },
  { href: BUILD_PATH, label: "Build" },
];

export default function Nav() {
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const sessionQuery = useAuthSessionQuery();
  const session = sessionQuery.data ?? null;
  const displayLabel = sessionDisplayLabel(session);
  const initials = sessionInitials(session);
  const authInfoQuery = useAuthInfoQuery(!!displayLabel);
  const loginSupported = authInfoQuery.data?.loginSupported ?? false;

  async function handleLogout() {
    await logout().catch(() => {});
    clearSession();
    window.location.href = serverLoginURL(appPath("/apps"));
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

        {/* Two primary destinations — keep visible below lg (no Sheet mobile nav yet). */}
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
                  pathname === link.href || pathname.startsWith(link.href + "/");
                return (
                  <NavigationMenuItem key={link.href}>
                    <NavigationMenuLink asChild active={isActive}>
                      <Link to={link.href}>{link.label}</Link>
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
