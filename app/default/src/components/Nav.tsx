import { useEffect, useRef, useState } from "react";
import { Link, useRouterState } from "@tanstack/react-router";
import {
  type AuthSession,
  getAuthInfo,
  getAuthSession,
  logout,
} from "@/lib/api";
import {
  clearSession,
  getCachedSession,
  sessionDisplayLabel,
  sessionInitials,
  setCachedSession,
  type CachedAuthSession,
} from "@/lib/auth";
import { DOCS_PATH, BUILD_PATH } from "@/lib/constants";
import { serverLoginURL } from "@/lib/authReturn";
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
  { href: DOCS_PATH, label: "Docs" },
];

export default function Nav() {
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const [session, setSession] = useState<CachedAuthSession | null>(null);
  const [loginSupported, setLoginSupported] = useState(false);
  const sessionRefreshGeneration = useRef(0);

  useEffect(() => {
    const generation = ++sessionRefreshGeneration.current;
    setSession(getCachedSession());
    getAuthSession()
      .then((nextSession: AuthSession) => {
        if (generation !== sessionRefreshGeneration.current) return;
        setCachedSession(nextSession);
        setSession(nextSession);
      })
      .catch(() => {});
  }, []);
  const displayLabel = sessionDisplayLabel(session);
  const initials = sessionInitials(session);

  useEffect(() => {
    if (!displayLabel) {
      setLoginSupported(false);
      return;
    }

    let active = true;
    getAuthInfo()
      .then((info) => {
        if (active) {
          setLoginSupported(info.loginSupported);
        }
      })
      .catch(() => {
        if (active) {
          setLoginSupported(true);
        }
      });

    return () => {
      active = false;
    };
  }, [displayLabel]);

  async function handleLogout() {
    sessionRefreshGeneration.current++;
    await logout().catch(() => {});
    clearSession();
    window.location.href = serverLoginURL("/apps");
  }

  return (
    <header className="border-b border-alpha py-3 bg-background/80 backdrop-blur-xs sticky top-0 z-50">
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
                    <p className="mt-0.5 truncate text-xs font-normal text-faint">
                      {session.email}
                    </p>
                  )}
                </DropdownMenuLabel>
                <DropdownMenuSeparator />
                <DropdownMenuItem asChild>
                  <Link to="/settings">Settings</Link>
                </DropdownMenuItem>
                {loginSupported && (
                  <DropdownMenuItem onClick={handleLogout}>Log out</DropdownMenuItem>
                )}
              </DropdownMenuContent>
            </DropdownMenu>
          )}
        </div>
      </Container>
    </header>
  );
}
