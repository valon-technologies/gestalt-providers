import { Link, useRouterState } from "@tanstack/react-router";
import { logout } from "@/lib/api";
import { clearSession, sessionDisplayLabel } from "@/lib/auth";
import { DOCS_PATH } from "@/lib/constants";
import { serverLoginURL } from "@/lib/authReturn";
import { appPath } from "@/lib/mount";
import { useAuthInfoQuery, useAuthSessionQuery } from "@/lib/queries";
import { useTheme } from "@/hooks/use-theme";
import Container from "./Container";
import { MoonIcon, SunIcon, SunMoonIcon } from "./icons";

const links = [
  { href: "/authorization", label: "Authorization" },
  { href: "/apps", label: "Apps" },
  { href: "/workflows", label: "Workflows" },
  { href: DOCS_PATH, label: "Docs" },
];

export default function Nav() {
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const sessionQuery = useAuthSessionQuery();
  const session = sessionQuery.data ?? null;
  const displayLabel = sessionDisplayLabel(session);
  const authInfoQuery = useAuthInfoQuery(!!displayLabel);
  const loginSupported = authInfoQuery.data?.loginSupported ?? false;
  const { theme, setTheme } = useTheme();
  const ThemeIcon = theme === "light" ? SunIcon : theme === "dark" ? MoonIcon : SunMoonIcon;

  async function handleLogout() {
    await logout().catch(() => {});
    clearSession();
    window.location.href = serverLoginURL(appPath("/apps"));
  }

  return (
    <nav className="border-b border-border py-3 bg-background/80 backdrop-blur-xs sticky top-0 z-50">
      <Container className="flex items-center justify-between">
        <div className="flex items-center gap-8">
          <Link to={appPath("/apps")} className="text-lg font-heading font-bold text-foreground">
            Gestalt
          </Link>
          <div className="flex gap-5">
            {links.map((link) => {
              const isActive =
                pathname === link.href ||
                (link.href === "/authorization" && pathname === "/tokens") ||
                (link.href !== "/" && pathname.startsWith(link.href + "/"));
              const className = `text-sm transition-colors duration-150 ${
                isActive
                  ? "text-foreground font-medium"
                  : "text-muted-foreground hover:text-foreground/80"
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
            className="flex h-8 w-8 items-center justify-center rounded-md text-muted-foreground transition-all duration-150 hover:bg-accent hover:text-accent-foreground"
            title={theme === "light" ? "Light mode" : theme === "dark" ? "Dark mode" : "System preference"}
            aria-label="Toggle theme"
          >
            <ThemeIcon className="h-[18px] w-[18px]" />
          </button>
          {displayLabel && (
            <>
              <span className="text-sm text-muted-foreground/70">{displayLabel}</span>
              {loginSupported && (
                <button
                  onClick={() => void handleLogout()}
                  className="text-sm text-muted-foreground hover:text-foreground transition-colors duration-150"
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
