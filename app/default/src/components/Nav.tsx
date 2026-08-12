import { Link, useRouterState } from "@tanstack/react-router";
import { clearSession, sessionDisplayLabel, sessionInitials } from "@/lib/auth";
import { BUILD_PATH } from "@/lib/constants";
import { appPath } from "@/lib/mount";
import { useAuthInfoQuery, useAuthSessionQuery } from "@/lib/queries";
import { usePlatformBrand } from "@/hooks/use-platform-brand";
import { AccountMenu } from "./AccountMenu";
import {
  AppTopBar,
  AppTopBarBrand,
  AppTopBarCenter,
  AppTopBarEnd,
  AppTopBarInner,
  AppTopBarStart,
} from "./ui/app-top-bar";
import { AppLogoMark, AppLogoName } from "./ui/app-logo";
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

function PlatformBrandMark({ markSrc }: { markSrc: string }) {
  // Mono marks paint via mask so light/dark both read as foreground ink.
  return (
    <AppLogoMark variant="plain" aria-hidden="true">
      <span
        className="block size-full bg-foreground"
        style={{
          maskImage: `url(${markSrc})`,
          maskSize: "contain",
          maskRepeat: "no-repeat",
          maskPosition: "center",
          WebkitMaskImage: `url(${markSrc})`,
          WebkitMaskSize: "contain",
          WebkitMaskRepeat: "no-repeat",
          WebkitMaskPosition: "center",
        }}
      />
    </AppLogoMark>
  );
}

export default function Nav() {
  const brand = usePlatformBrand();
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const sessionQuery = useAuthSessionQuery();
  const session = sessionQuery.data ?? null;
  const displayLabel = sessionDisplayLabel(session);
  const initials = sessionInitials(session);
  const authInfoQuery = useAuthInfoQuery(!!displayLabel);
  const loginSupported = authInfoQuery.data?.loginSupported ?? false;

  async function handleLogout() {
    clearSession();
    const returnTo = encodeURIComponent(appPath("/"));
    window.location.href = `/api/v1/auth/logout?returnTo=${returnTo}`;
  }

  return (
    <AppTopBar>
      <AppTopBarInner>
        <AppTopBarStart>
          {/* SPA link semantics via asChild — caller supplies lockup parts inside Link. */}
          <AppTopBarBrand size="lg" asChild>
            <Link to="/apps">
              {brand.markSrc ? <PlatformBrandMark markSrc={brand.markSrc} /> : null}
              <AppLogoName size="lg">{brand.name}</AppLogoName>
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
