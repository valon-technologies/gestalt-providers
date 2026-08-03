import { Link, useRouterState } from "@tanstack/react-router";
import { logout } from "@/lib/api";
import { clearSession, sessionDisplayLabel, sessionInitials } from "@/lib/auth";
import { BUILD_PATH } from "@/lib/constants";
import { serverLoginURL } from "@/lib/authReturn";
import { appPath } from "@/lib/mount";
import { useAuthInfoQuery, useAuthSessionQuery } from "@/lib/queries";
import { AccountMenu } from "./AccountMenu";
import Container from "./Container";
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
    <header className="border-b border-border py-3 bg-background/80 backdrop-blur-xs">
      <Container className="grid grid-cols-[1fr_auto_1fr] items-baseline gap-x-4">
        <div className="justify-self-start">
          <Link
            to="/apps"
            className="font-heading text-2xl font-bold leading-none text-foreground"
          >
            Gestalt
          </Link>
        </div>

        <NavigationMenu
          viewport={false}
          size="lg"
          aria-label="Primary"
          className="max-w-none flex-none justify-self-center"
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

        <div className="flex items-center justify-self-end gap-3 self-center">
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
        </div>
      </Container>
    </header>
  );
}
