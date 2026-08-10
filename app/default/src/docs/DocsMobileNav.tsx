import { useEffect, useState } from "react";
import { Link } from "@tanstack/react-router";
import { ChevronDownIcon } from "@/components/icons";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import {
  NavList,
  NavListGroup,
  NavListItem,
  NavListItemLabel,
} from "@/components/ui/nav-list";
import { disclosureCaretClassName } from "@/lib/disclosure-caret";
import { cn } from "@/lib/cn";
import {
  DOCS_NAV_GROUPS,
  docsNavItemsByGroup,
  type DocsNavItem,
} from "./docs-data";

/**
 * Mobile stand-in for the docs Pane. Same destinations as the desktop rail,
 * inside a disclosure — SegmentedControl is wrong for 8 grouped pages.
 */
export function DocsMobileNav({
  pathname,
  activeItem,
}: {
  pathname: string;
  activeItem: DocsNavItem;
}) {
  const [open, setOpen] = useState(false);

  useEffect(() => {
    setOpen(false);
  }, [pathname]);

  return (
    <Collapsible
      open={open}
      onOpenChange={setOpen}
      className="w-full min-w-0"
      data-testid="docs-mobile-nav"
    >
      <CollapsibleTrigger
        className={cn(
          "group flex w-full items-center rounded-lg border border-border bg-background px-3 py-2 text-sm font-medium text-foreground",
          "hover:bg-neutral-hover active:bg-neutral-pressed",
        )}
        aria-label={`Documentation, current: ${activeItem.label}`}
      >
        <span className="min-w-0 truncate">{activeItem.label}</span>
        <ChevronDownIcon
          className={cn(disclosureCaretClassName, "ml-auto text-muted-foreground")}
          aria-hidden
        />
      </CollapsibleTrigger>
      <CollapsibleContent className="pt-2">
        <div className="rounded-lg border border-border bg-background p-2">
          <DocsNavList activeId={activeItem.id} />
        </div>
      </CollapsibleContent>
    </Collapsible>
  );
}

export function DocsNavList({ activeId }: { activeId: string }) {
  return (
    <NavList aria-label="Documentation">
      {DOCS_NAV_GROUPS.map((group) => {
        const items = docsNavItemsByGroup(group.id);
        if (items.length === 0) return null;
        return (
          <NavListGroup key={group.id} label={group.label}>
            {items.map((item) => (
              <NavListItem
                key={item.id}
                asChild
                active={item.id === activeId}
              >
                <Link to={item.href}>
                  <NavListItemLabel>{item.label}</NavListItemLabel>
                </Link>
              </NavListItem>
            ))}
          </NavListGroup>
        );
      })}
    </NavList>
  );
}
