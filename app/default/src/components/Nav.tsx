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
  setCachedSession,
  type CachedAuthSession,
} from "@/lib/auth";
import { DOCS_PATH, BUILD_PATH } from "@/lib/constants";
import { serverLoginURL } from "@/lib/authReturn";
import { useTheme } from "@/hooks/use-theme";
import Container from "./Container";
import { MoonIcon, SunIcon, SunMoonIcon } from "./icons";

const links = [
  { href: BUILD_PATH, label: "Build" },
  { href: "/authorization", label: "Authorization" },
  { href: "/apps", label: "Apps" },
  { href: DOCS_PATH, label: "Docs" },
];

export default function Nav() {
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const [session, setSession] = useState<CachedAuthSession | null>(null);
  const [loginSupported, setLoginSupported] = useState(false);
  const { theme, setTheme } = useTheme();
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
  const ThemeIcon = theme === "light" ? SunIcon : theme === "dark" ? MoonIcon : SunMoonIcon;

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
    <nav className="border-b border-alpha py-3 bg-background/80 backdrop-blur-xs sticky top-0 z-50">
      <Container className="flex items-center justify-between">
        <div className="flex items-center gap-8">
          <Link to="/apps" className="text-lg font-heading font-bold text-primary">
            Gestalt
          </Link>
          <div className="flex gap-5">
            {links.map((link) => {
              const isActive =
                pathname === link.href ||
                (link.href === "/authorization" && pathname === "/tokens") ||
                pathname.startsWith(link.href + "/");
              const className = `text-sm transition-colors duration-150 ${
                isActive
                  ? "text-primary font-medium"
                  : "text-muted hover:text-secondary"
              }`;
              return (
                <Link key={link.href} to={link.href} className={className}>
                  {link.label}
                </Link>
              );
            })}
          </div>
        </div>
        <div className="flex items-center gap-4">
          <button
            onClick={() => {
              if (theme === "light") setTheme("dark");
              else if (theme === "dark") setTheme("system");
              else setTheme("light");
            }}
            className="flex h-8 w-8 items-center justify-center rounded-md text-muted transition-all duration-150 hover:bg-alpha-5"
            title={theme === "light" ? "Light mode" : theme === "dark" ? "Dark mode" : "System preference"}
            aria-label="Toggle theme"
          >
            <ThemeIcon className="h-[18px] w-[18px]" />
          </button>
          {displayLabel && (
            <>
              <span className="text-sm text-faint">{displayLabel}</span>
              {loginSupported && (
                <button
                  onClick={handleLogout}
                  className="text-sm text-muted hover:text-primary transition-colors duration-150"
                >
                  Logout
                </button>
              )}
            </>
          )}
        </div>
      </Container>
    </nav>
  );
}
