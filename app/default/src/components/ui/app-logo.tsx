/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

const appLogoVariants = cva("inline-flex min-w-0 shrink-0 items-center gap-2");

const appLogoMarkVariants = cva("flex shrink-0 items-center justify-center [&_svg]:size-4", {
  variants: {
    variant: {
      plain: "",
      tile: "rounded-lg bg-sidebar-primary text-sidebar-primary-foreground",
    },
    size: {
      sm: "size-6 [&_svg]:size-3.5",
      default: "size-8",
      lg: "size-10 [&_svg]:size-5",
    },
  },
  defaultVariants: {
    variant: "tile",
    size: "default",
  },
});

// Chrome wordmark scale — heading steps only (not PageHeader display tiers).
// Size names are chrome-local: default/md/lg ≠ PageHeader's same labels.
const appLogoNameVariants = cva("font-display font-normal text-foreground", {
  variants: {
    size: {
      /** Dense chrome floor (~18px); display-face exception below the 32px display gate. */
      default: "text-heading-sm tracking-display",
      md: "text-heading-lg tracking-heading",
      lg: "text-heading-xl tracking-display",
    },
  },
  defaultVariants: {
    size: "default",
  },
});

type AppLogoNameSize = NonNullable<VariantProps<typeof appLogoNameVariants>["size"]>;
type AppLogoMarkSize = NonNullable<VariantProps<typeof appLogoMarkVariants>["size"]>;

const AppLogoSizeContext = React.createContext<AppLogoNameSize>("default");

const markSizeFromNameSize = (size: AppLogoNameSize): AppLogoMarkSize =>
  size === "lg" ? "lg" : "default";

const appLogoInteractiveClassName =
  "cursor-pointer border-0 bg-transparent p-0 text-left font-[inherit] text-inherit no-underline hover:text-inherit focus-ring rounded-sm";

/**
 * Polymorphic chrome root (div | a | button | Slot). Shared attrs are typed on
 * HTMLElement so href/onNavigate branches do not fight div-only refs/props.
 * `onClick` stays owned by `onNavigate` (not overridable at the call site).
 */
interface AppLogoProps extends Omit<React.HTMLAttributes<HTMLElement>, "onClick"> {
  asChild?: boolean;
  /** When set, the lockup is an in-header link (SPA or full navigation). */
  href?: string;
  /** When set (and `href` is absent), the lockup is an in-header button. */
  onNavigate?: () => void;
  /** Default size for `AppLogoName` / `AppLogoMark` unless overridden on the part. */
  size?: AppLogoNameSize;
}

/** App shell brand lockup — mark + wordmark (+ optional tagline). Not PageHeader. */
function AppLogo({
  asChild = false,
  href,
  onNavigate,
  size = "default",
  className,
  children,
  ...props
}: AppLogoProps) {
  const body = (
    <AppLogoSizeContext.Provider value={size}>{children}</AppLogoSizeContext.Provider>
  );

  if (asChild) {
    return (
      <AppLogoSizeContext.Provider value={size}>
        <Slot
          data-slot="app-logo"
          className={cn(appLogoVariants(), appLogoInteractiveClassName, className)}
          {...props}
        >
          {children}
        </Slot>
      </AppLogoSizeContext.Provider>
    );
  }

  if (href) {
    return (
      <a
        data-slot="app-logo"
        href={href}
        className={cn(appLogoVariants(), appLogoInteractiveClassName, className)}
        {...props}
      >
        {body}
      </a>
    );
  }

  if (onNavigate) {
    return (
      <button
        type="button"
        data-slot="app-logo"
        className={cn(appLogoVariants(), appLogoInteractiveClassName, className)}
        onClick={onNavigate}
        {...props}
      >
        {body}
      </button>
    );
  }

  return (
    <div data-slot="app-logo" className={cn(appLogoVariants(), className)} {...props}>
      {body}
    </div>
  );
}

interface AppLogoMarkProps
  extends React.ComponentProps<"div">,
    VariantProps<typeof appLogoMarkVariants> {}

function AppLogoMark({ className, variant, size, ...props }: AppLogoMarkProps) {
  const contextSize = React.useContext(AppLogoSizeContext);
  return (
    <div
      data-slot="app-logo-mark"
      className={cn(
        appLogoMarkVariants({ variant, size: size ?? markSizeFromNameSize(contextSize) }),
        className,
      )}
      {...props}
    />
  );
}

interface AppLogoNameProps
  extends React.ComponentProps<"span">,
    VariantProps<typeof appLogoNameVariants> {}

/** Product wordmark — display face at a fixed chrome scale, not size-gated like PageHeader. */
function AppLogoName({ className, size, ...props }: AppLogoNameProps) {
  const contextSize = React.useContext(AppLogoSizeContext);
  return (
    <span
      data-slot="app-logo-name"
      className={cn(appLogoNameVariants({ size: size ?? contextSize }), className)}
      {...props}
    />
  );
}

function AppLogoTagline({ className, ...props }: React.ComponentProps<"span">) {
  return (
    <span
      data-slot="app-logo-tagline"
      className={cn("text-xs text-muted-foreground", className)}
      {...props}
    />
  );
}

/** Name + tagline stack; auto-hides when a parent Sidebar is collapsed to icons. */
function AppLogoText({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="app-logo-text"
      className={cn(
        "flex min-w-0 flex-col gap-0.5 leading-none group-data-[collapsible=icon]:hidden",
        className,
      )}
      {...props}
    />
  );
}

export type { AppLogoProps };
export {
  AppLogo,
  AppLogoMark,
  AppLogoName,
  AppLogoTagline,
  AppLogoText,
  appLogoVariants,
  appLogoMarkVariants,
  appLogoNameVariants,
};
