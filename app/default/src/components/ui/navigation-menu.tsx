import {
  createContext,
  useContext,
  type ComponentPropsWithoutRef,
  type ReactElement,
  cloneElement,
  isValidElement,
} from "react";
import { cn } from "@/lib/cn";

const sizeClass = {
  sm: "h-8 px-2.5 text-sm",
  default: "h-9 px-3 py-2 text-sm",
  lg: "h-10 px-4 text-base",
} as const;

export type NavigationMenuSize = keyof typeof sizeClass;

const NavigationMenuSizeContext =
  createContext<NavigationMenuSize>("default");

function linkClassName(size: NavigationMenuSize) {
  return cn(
    "group inline-flex w-max items-center justify-center rounded-md font-medium outline-none",
    "text-secondary hover:bg-alpha-5 hover:text-primary",
    "focus-ring",
    // Selected chrome: accent FILL + ink on-fill (never text-brand).
    "data-active:bg-accent-subtle data-active:text-accent-foreground",
    "data-active:hover:bg-accent-subtle",
    "disabled:pointer-events-none disabled:opacity-50",
    sizeClass[size],
  );
}

/** Flat peer links for site destinations. */
export function NavigationMenu({
  className,
  children,
  size = "default",
  viewport: _viewport = true,
  ...props
}: ComponentPropsWithoutRef<"nav"> & {
  size?: NavigationMenuSize;
  /** Kept for API parity; flat links do not render a dropdown viewport. */
  viewport?: boolean;
}) {
  return (
    <NavigationMenuSizeContext.Provider value={size}>
      <nav
        data-slot="navigation-menu"
        data-size={size}
        className={cn(
          "group/navigation-menu relative flex max-w-max flex-1 items-center justify-center",
          className,
        )}
        {...props}
      >
        {children}
      </nav>
    </NavigationMenuSizeContext.Provider>
  );
}

export function NavigationMenuList({
  className,
  ...props
}: ComponentPropsWithoutRef<"ul">) {
  return (
    <ul
      data-slot="navigation-menu-list"
      className={cn(
        "group flex flex-1 list-none items-center justify-center gap-1",
        className,
      )}
      {...props}
    />
  );
}

export function NavigationMenuItem({
  className,
  ...props
}: ComponentPropsWithoutRef<"li">) {
  return (
    <li
      data-slot="navigation-menu-item"
      className={cn("relative", className)}
      {...props}
    />
  );
}

export function NavigationMenuLink({
  className,
  size,
  active,
  asChild,
  children,
  ...props
}: ComponentPropsWithoutRef<"a"> & {
  size?: NavigationMenuSize;
  active?: boolean;
  asChild?: boolean;
}) {
  const contextSize = useContext(NavigationMenuSizeContext);
  const resolvedSize = size ?? contextSize;
  const classes = cn(linkClassName(resolvedSize), className);

  if (asChild && isValidElement(children)) {
    const child = children as ReactElement<{ className?: string }>;
    return cloneElement(child, {
      className: cn(classes, child.props.className),
      "data-slot": "navigation-menu-link",
      "data-active": active || undefined,
    } as never);
  }

  return (
    <a
      data-slot="navigation-menu-link"
      data-active={active || undefined}
      className={classes}
      {...props}
    >
      {children}
    </a>
  );
}
