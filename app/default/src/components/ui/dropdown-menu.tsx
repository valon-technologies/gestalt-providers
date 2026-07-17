import {
  Menu,
  MenuButton,
  MenuItem,
  MenuItems,
  type MenuItemsProps,
} from "@headlessui/react";
import {
  type ButtonHTMLAttributes,
  type ComponentPropsWithoutRef,
  type ReactElement,
  type ReactNode,
  cloneElement,
  isValidElement,
} from "react";
import { cn } from "@/lib/cn";

/**
 * Action-menu flyout: trigger + portaled panel.
 * Avatar is never the interactive surface — wrap it in DropdownMenuTrigger.
 */
export function DropdownMenu({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <Menu as="div" className={cn("relative", className)}>
      {children}
    </Menu>
  );
}

export function DropdownMenuTrigger({
  className,
  children,
  ...props
}: ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <MenuButton
      className={cn(
        "focus-ring inline-flex items-center justify-center rounded-full",
        "transition-[color,background-color,border-color] duration-hover-out ease-out-quart",
        "hover:bg-alpha-5 hover:duration-hover-in",
        className,
      )}
      {...props}
    >
      {children}
    </MenuButton>
  );
}

export function DropdownMenuContent({
  className,
  children,
  ...props
}: {
  className?: string;
  children: ReactNode;
} & Omit<MenuItemsProps, "className" | "children">) {
  return (
    <MenuItems
      transition
      anchor="bottom end"
      className={cn(
        "z-50 min-w-[8rem] overflow-hidden rounded-xl border border-alpha bg-base-white p-1 text-primary shadow-dropdown outline-none dark:bg-surface",
        "origin-top-right [--anchor-gap:4px]",
        "transition-[opacity,transform] duration-reveal ease-out-back",
        "data-closed:duration-dismiss data-closed:ease-out-expo",
        "data-closed:scale-95 data-closed:opacity-0",
        className,
      )}
      {...props}
    >
      {children}
    </MenuItems>
  );
}

export function DropdownMenuLabel({
  className,
  ...props
}: ComponentPropsWithoutRef<"div">) {
  return (
    <div
      className={cn("px-2 py-1.5 text-sm font-semibold text-primary", className)}
      {...props}
    />
  );
}

export function DropdownMenuSeparator({
  className,
  ...props
}: ComponentPropsWithoutRef<"div">) {
  return (
    <div
      role="separator"
      className={cn("-mx-1 my-1 h-px bg-alpha-10", className)}
      {...props}
    />
  );
}

const itemClassName = cn(
  "relative flex w-full cursor-default select-none items-center gap-2 rounded-md px-2 py-1.5 text-sm outline-none",
  "text-secondary",
  "transition-[color,background-color] duration-select-out ease-out-quart",
  "data-focus:bg-alpha-5 data-focus:text-primary data-focus:duration-select-in",
);

export function DropdownMenuItem({
  className,
  children,
  onClick,
  disabled,
}: {
  className?: string;
  children: ReactNode;
  onClick?: () => void;
  disabled?: boolean;
}) {
  return (
    <MenuItem disabled={disabled}>
      <button
        type="button"
        onClick={onClick}
        className={cn(itemClassName, "text-left", className)}
      >
        {children}
      </button>
    </MenuItem>
  );
}

/** Menu row that navigates — pass a single <Link> / <a> child. */
export function DropdownMenuLinkItem({
  className,
  children,
}: {
  className?: string;
  children: ReactElement<{ className?: string }>;
}) {
  if (!isValidElement(children)) {
    return null;
  }
  return (
    <MenuItem>
      {cloneElement(children, {
        className: cn(
          itemClassName,
          "block no-underline",
          className,
          children.props.className,
        ),
      })}
    </MenuItem>
  );
}
