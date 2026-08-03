import { Link } from "@tanstack/react-router";
import { DOCS_PATH } from "@/lib/constants";
import { Avatar, AvatarFallback } from "./ui/avatar";
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
          {email && email !== displayLabel && (
            <p className="mt-0.5 truncate text-xs font-normal text-muted-foreground">
              {email}
            </p>
          )}
        </DropdownMenuLabel>

        <DropdownMenuSeparator />

        <DropdownMenuGroup>
          <DropdownMenuItem asChild>
            <Link to={DOCS_PATH}>Docs</Link>
          </DropdownMenuItem>
          <DropdownMenuItem asChild>
            <Link to="/settings">Settings</Link>
          </DropdownMenuItem>
        </DropdownMenuGroup>

        <DropdownMenuSeparator />

        <DropdownMenuGroup>
          <DropdownMenuLabel className="text-xs font-medium text-muted-foreground">
            Theme
          </DropdownMenuLabel>
          <div
            className="px-2 pb-1.5"
            // Keep the menu open while interacting with the segmented control.
            onPointerDown={(event) => event.preventDefault()}
          >
            <ThemeToggle placement="menu" size="sm" />
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
