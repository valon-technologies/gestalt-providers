import { Link } from "@tanstack/react-router";
import { Eyebrow } from "@/components/ui/eyebrow";
import type { AppAdminNavId, AppUserNavId } from "./app-nav";

type NavItem = {
  id: AppUserNavId | AppAdminNavId;
  label: string;
  to: string;
};

const navLinkClass =
  "block rounded-md px-3 py-2 text-sm text-muted-foreground transition-colors duration-150 hover:text-foreground";
const navLinkActiveClass =
  "block rounded-md bg-alpha-5 px-3 py-2 text-sm font-medium text-foreground transition-colors duration-150";

export function AppWorkspaceNav({
  app,
  userItems,
  adminItems,
  adminGroupVisible,
}: {
  app: string;
  userItems: ReadonlyArray<NavItem>;
  adminItems: ReadonlyArray<NavItem>;
  adminGroupVisible: boolean;
}) {
  return (
    <nav className="space-y-6" aria-label="App workspace">
      <div className="space-y-0.5">
        {userItems.map((item) => (
          <Link
            key={item.id}
            to={item.to}
            params={{ app }}
            className={navLinkClass}
            activeProps={{ className: navLinkActiveClass }}
            activeOptions={{ exact: item.id === "overview" }}
            data-testid={`app-workspace-nav-${item.id}`}
          >
            {item.label}
          </Link>
        ))}
      </div>

      {adminGroupVisible && adminItems.length > 0 ? (
        <div>
          <Eyebrow size="sm" tone="secondary" className="mb-2 block px-3">
            Admin
          </Eyebrow>
          <div className="space-y-0.5">
            {adminItems.map((item) => (
              <Link
                key={item.id}
                to={item.to}
                params={{ app }}
                className={navLinkClass}
                activeProps={{ className: navLinkActiveClass }}
                data-testid={`app-admin-nav-${item.id}`}
              >
                {item.label}
              </Link>
            ))}
          </div>
        </div>
      ) : null}
    </nav>
  );
}
