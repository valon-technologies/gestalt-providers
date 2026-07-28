import { Link, useRouterState } from "@tanstack/react-router";
import { logout } from "@/lib/api";
import { clearSession, sessionDisplayLabel, sessionInitials } from "@/lib/auth";
import { BUILD_PATH, DOCS_PATH } from "@/lib/constants";
import { serverLoginURL } from "@/lib/authReturn";
import { appPath } from "@/lib/mount";
import { useAuthInfoQuery, useAuthSessionQuery } from "@/lib/queries";
import Container from "./Container";
import { Avatar, AvatarFallback } from "./ui/avatar";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "./ui/dropdown-menu";
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
  { href: "/workflows", label: "Workflows" },
  { href: DOCS_PATH, label: "Docs" },
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
    <header className="border-b border-border py-3 bg-background/80 backdrop-blur-xs sticky top-0 z-50">
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
          <ThemeToggle size="sm" />
          {displayLabel && (
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <button
                  type="button"
                  className="focus-ring rounded-full"
                  aria-label="Open user menu"
                >
                  <Avatar size="xl" variant="solid" aria-hidden>
                    <AvatarFallback>{initials}</AvatarFallback>
                  </Avatar>
                </button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="w-56">
                <DropdownMenuLabel>
                  <p className="truncate font-semibold">{displayLabel}</p>
                  {session?.email && session.email !== displayLabel && (
                    <p className="mt-0.5 truncate text-xs font-normal text-muted-foreground">
                      {session.email}
                    </p>
                  )}
                </DropdownMenuLabel>
                <DropdownMenuSeparator />
                <DropdownMenuItem asChild>
                  <Link to="/settings">Settings</Link>
                </DropdownMenuItem>
                {loginSupported && (
                  <DropdownMenuItem onClick={() => void handleLogout()}>
                    Log out
                  </DropdownMenuItem>
                )}
              </DropdownMenuContent>
            </DropdownMenu>
          )}
        </div>
      </Container>
    </header>
  );
}
