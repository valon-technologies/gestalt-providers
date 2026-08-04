import { Link } from "@tanstack/react-router";
import { DOCS_PATH } from "@/lib/constants";
import { cn } from "@/lib/cn";
import { Avatar, AvatarFallback } from "./ui/avatar";
import { Button } from "./ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "./ui/dropdown-menu";
import { ThemeToggle } from "./ui/theme-toggle";

/** Stable id for the visible Theme section label → radiogroup `aria-labelledby`. */
export const ACCOUNT_MENU_THEME_LABEL_ID = "account-menu-theme-label";

/** Visible section label (and accessible name source for the theme control). */
export const ACCOUNT_MENU_THEME_SECTION_LABEL = "Theme";

/**
 * Signed-in utilities in the account flyout.
 * Docs is account-gated (docs routes require auth); not shown in guest chrome.
 */
export const ACCOUNT_MENU_UTILITY_LINKS = [
  { to: DOCS_PATH, label: "Docs" },
  { to: "/settings", label: "Settings" },
] as const;

export type AccountMenuProps = {
  displayLabel: string;
  email?: string | null;
  initials: string;
  loginSupported: boolean;
  onLogout: () => void | Promise<void>;
};

/**
 * Signed-in account chrome: identity → utilities → theme → session.
 * Owns flyout IA; Nav only decides when this menu is shown.
 */
export function AccountMenu({
  displayLabel,
  email,
  initials,
  loginSupported,
  onLogout,
}: AccountMenuProps) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        {/* Interactive account chip (Registry AppTopBar recipe): ghost Button
            wraps Avatar xl; rest pins Neutral-hover so Neutral-dark can climb
            (solid's muted-strong is for static / row-hover discs). Hover = fill
            step only (−0.025 L) — suppress ghost hover scrim. Press/open = further
            fill (−0.015) plus the ghost press scrim. Stacking fill+scrim on hover
            made press invisible. */}
        <Button
          type="button"
          variant="ghost"
          size="icon-lg"
          className="group rounded-full p-0 hover:after:opacity-0 active:after:opacity-[var(--state-overlay-press,0.14)] data-[state=open]:after:opacity-[var(--state-overlay-press,0.14)]"
          aria-label="Open user menu"
        >
          <Avatar
            size="xl"
            variant="solid"
            aria-hidden
            className={cn(
              "bg-neutral-hover transition-[background-color] duration-hover-out ease-out-quart group-hover:duration-hover-in",
              "group-hover:bg-neutral-dark-hover",
              "group-active:bg-neutral-dark-pressed",
              "group-data-[state=open]:bg-neutral-dark-pressed",
            )}
          >
            <AvatarFallback>{initials}</AvatarFallback>
          </Avatar>
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-56">
        <DropdownMenuLabel>
          <p className="truncate font-semibold">{displayLabel}</p>
          {email && email !== displayLabel && (
            <p className="mt-0.5 truncate text-xs font-normal text-muted-foreground">
              {email}
            </p>
          )}
        </DropdownMenuLabel>

        <DropdownMenuSeparator />

        <DropdownMenuGroup>
          {ACCOUNT_MENU_UTILITY_LINKS.map((link) => (
            <DropdownMenuItem key={link.to} asChild>
              <Link to={link.to}>{link.label}</Link>
            </DropdownMenuItem>
          ))}
        </DropdownMenuGroup>

        <DropdownMenuSeparator />

        <DropdownMenuGroup>
          <DropdownMenuLabel
            id={ACCOUNT_MENU_THEME_LABEL_ID}
            className="text-xs font-medium text-muted-foreground"
          >
            {ACCOUNT_MENU_THEME_SECTION_LABEL}
          </DropdownMenuLabel>
          <div
            className="px-2 pb-1.5"
            // Keep the menu open while interacting with the segmented control.
            onPointerDown={(event) => event.preventDefault()}
          >
            <ThemeToggle
              placement="menu"
              size="sm"
              labelledBy={ACCOUNT_MENU_THEME_LABEL_ID}
            />
          </div>
        </DropdownMenuGroup>

        {loginSupported && (
          <>
            <DropdownMenuSeparator />
            <DropdownMenuItem onClick={() => void onLogout()}>
              Log out
            </DropdownMenuItem>
          </>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
