
import { useEffect, useRef, useState } from "react";
import { Link, useRouterState } from "@tanstack/react-router";
import {
  type AuthSession,
  getAgentSessions,
  getAuthInfo,
  getAuthSession,
  isAPIErrorStatus,
  logout,
} from "@/lib/api";
import {
  clearSession,
  getCachedSession,
  sessionDisplayLabel,
  setCachedSession,
  type CachedAuthSession,
} from "@/lib/auth";
import { DOCS_PATH, LOGIN_PATH } from "@/lib/constants";
import { useTheme } from "@/hooks/use-theme";
import Container from "./Container";
import { MoonIcon, SunIcon, SunMoonIcon } from "./icons";

const links = [
  { href: "/", label: "Dashboard" },
  { href: "/authorization", label: "Authorization" },
  { href: "/apps", label: "Apps" },
  { href: "/workflows", label: "Workflows" },
  { href: "/agents", label: "Agents" },
  { href: DOCS_PATH, label: "Docs" },
];

export default function Nav() {
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const [session, setSession] = useState<CachedAuthSession | null>(null);
  const [loginSupported, setLoginSupported] = useState(false);
  const [agentAvailable, setAgentAvailable] = useState(false);
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
      setAgentAvailable(false);
      return;
    }

    let active = true;
    getAuthInfo()
      .then(async (info) => {
        if (active) {
          setLoginSupported(info.loginSupported);
        }
        if (typeof info.features?.agent === "boolean") {
          if (active) {
            setAgentAvailable(info.features.agent);
          }
          return;
        }
        try {
          await getAgentSessions({ view: "summary", limit: 1 });
          if (active) {
            setAgentAvailable(true);
          }
        } catch (err) {
          if (active) {
            setAgentAvailable(!isAPIErrorStatus(err, 412));
          }
        }
      })
      .catch(() => {
        if (active) {
          setLoginSupported(true);
          setAgentAvailable(true);
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
    window.location.href = LOGIN_PATH;
  }

  return (
    <nav className="border-b border-alpha py-3 bg-background/80 backdrop-blur-xs sticky top-0 z-50">
      <Container className="flex items-center justify-between">
        <div className="flex items-center gap-8">
          <Link to="/" className="text-lg font-heading font-bold text-primary">
            Gestalt
          </Link>
          <div className="flex gap-5">
            {links.filter((link) => link.href !== "/agents" || agentAvailable).map((link) => {
              const isActive =
                pathname === link.href ||
                (link.href === "/authorization" && pathname === "/tokens") ||
                (link.href !== "/" && pathname.startsWith(link.href + "/"));
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
