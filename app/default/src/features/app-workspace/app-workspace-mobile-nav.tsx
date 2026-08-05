import { useEffect, useMemo, useState } from "react";
import { ChevronDownIcon } from "@/components/icons";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { disclosureCaretClassName } from "@/lib/disclosure-caret";
import { cn } from "@/lib/cn";
import type { AppAdminNavId, AppUserNavId } from "./app-nav";
import { AppWorkspaceNav } from "./app-workspace-nav";

type WorkspaceNavId = AppUserNavId | AppAdminNavId;

type NavItem = {
  id: WorkspaceNavId;
  label: string;
  to: string;
};

function activeWorkspaceNavLabel(
  pathname: string,
  app: string,
  userItems: ReadonlyArray<NavItem>,
  adminItems: ReadonlyArray<NavItem>,
  adminGroupVisible: boolean,
): string {
  const items = [
    ...userItems,
    ...(adminGroupVisible ? adminItems : []),
  ];
  for (const item of items) {
    if (item.id === "overview") continue;
    const path = item.to.replace("$app", app);
    if (pathname === path || pathname.startsWith(`${path}/`)) {
      return item.label;
    }
  }
  return items.find((item) => item.id === "overview")?.label ?? "Overview";
}

/**
 * Mobile stand-in for the app-workspace Pane: same NavList (incl. Admin group)
 * inside a disclosure. SegmentedControl is wrong here — destinations are a
 * section map with groups, not a flat handful of modes.
 */
export function AppWorkspaceMobileNav({
  app,
  pathname,
  userItems,
  adminItems,
  adminGroupVisible,
}: {
  app: string;
  pathname: string;
  userItems: ReadonlyArray<NavItem>;
  adminItems: ReadonlyArray<NavItem>;
  adminGroupVisible: boolean;
}) {
  const [open, setOpen] = useState(false);
  const destinationCount =
    userItems.length + (adminGroupVisible ? adminItems.length : 0);

  const activeLabel = useMemo(
    () =>
      activeWorkspaceNavLabel(
        pathname,
        app,
        userItems,
        adminItems,
        adminGroupVisible,
      ),
    [adminGroupVisible, adminItems, app, pathname, userItems],
  );

  // Close after navigation so the content column owns the screen again.
  useEffect(() => {
    setOpen(false);
  }, [pathname]);

  if (destinationCount < 2) return null;

  return (
    <Collapsible
      open={open}
      onOpenChange={setOpen}
      className="w-full min-w-0"
      data-testid="app-workspace-mobile-nav"
    >
      <CollapsibleTrigger
        className={cn(
          "group rounded-lg border border-border bg-background px-3 py-2",
          "hover:bg-neutral-hover active:bg-neutral-pressed",
        )}
        aria-label={`App sections, current: ${activeLabel}`}
      >
        <span className="min-w-0 truncate">{activeLabel}</span>
        <ChevronDownIcon
          className={cn(disclosureCaretClassName, "ml-auto text-muted-foreground")}
          aria-hidden
        />
      </CollapsibleTrigger>
      <CollapsibleContent className="pt-2">
        <div className="rounded-lg border border-border bg-background p-2">
          <AppWorkspaceNav
            app={app}
            userItems={userItems}
            adminItems={adminItems}
            adminGroupVisible={adminGroupVisible}
          />
        </div>
      </CollapsibleContent>
    </Collapsible>
  );
}
