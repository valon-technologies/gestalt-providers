/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 *
 * Console adaptation: sticky stacking is owned by `__root.tsx` (DevWorktreeBanner +
 * chrome). Do not reintroduce Registry sticky positioning on this root. Column
 * clamp matches `Container` so the bar stays aligned with page content.
 */

import * as React from "react";
import { cva } from "class-variance-authority";

import { AppLogo, AppLogoName } from "@/components/ui/app-logo";
import type { AppLogoProps } from "@/components/ui/app-logo";
import { cn } from "@/lib/cn";

// Persistent app chrome: brand (Start) · peer nav (Center) · utilities (End).
// Sibling to PageHeader (per-view h1) — not a document title row.
// Layout inspired by shadcn-space topbar-04 (centered nav between flanking zones).
// AppTopBarInner and AppTopBarPage share appTopBarColumnVariants so chrome and
// page body stay column-aligned — see guidelines/container.md §2.
//
// Brand wordmark: AppTopBarBrand → AppLogo + AppLogoName (display face). Do not use
// PageHeaderTitle here; display face is size-gated there (Season except sm).

const appTopBarVariants = cva("w-full border-b bg-background");

/** One clamp + horizontal gutter for chrome inner row and the page column below. */
const appTopBarColumnVariants = cva("mx-auto w-full max-w-7xl px-6");

const appTopBarInnerVariants = cva("flex items-center justify-between gap-4 py-2.5");

const appTopBarPageVariants = cva("py-4 md:py-6");

function AppTopBar({ className, ...props }: React.ComponentProps<"header">) {
  return (
    <header
      data-slot="app-top-bar"
      className={cn(appTopBarVariants(), className)}
      {...props}
    />
  );
}

function AppTopBarInner({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="app-top-bar-inner"
      className={cn(appTopBarColumnVariants(), appTopBarInnerVariants(), className)}
      {...props}
    />
  );
}

function AppTopBarPage({ className, ...props }: React.ComponentProps<"main">) {
  return (
    <main
      data-slot="app-top-bar-page"
      className={cn(appTopBarColumnVariants(), appTopBarPageVariants(), className)}
      {...props}
    />
  );
}

function AppTopBarStart({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="app-top-bar-start"
      className={cn("flex min-w-0 shrink-0 items-center gap-3", className)}
      {...props}
    />
  );
}

function AppTopBarCenter({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="app-top-bar-center"
      className={cn(
        "hidden min-w-0 flex-1 items-center justify-center lg:flex",
        className,
      )}
      {...props}
    />
  );
}

function AppTopBarEnd({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="app-top-bar-end"
      className={cn("flex shrink-0 items-center gap-2", className)}
      {...props}
    />
  );
}

type AppTopBarBrandProps = Omit<AppLogoProps, "children"> & {
  children?: React.ReactNode;
};

/** App top-bar wordmark — `AppLogo` + `AppLogoName` with display typography. */
function AppTopBarBrand({
  className,
  size,
  href,
  onNavigate,
  children,
  ...props
}: AppTopBarBrandProps) {
  return (
    <AppLogo href={href} onNavigate={onNavigate} size={size} className={className} {...props}>
      <AppLogoName size={size}>{children}</AppLogoName>
    </AppLogo>
  );
}

export {
  AppTopBar,
  AppTopBarInner,
  AppTopBarPage,
  AppTopBarStart,
  AppTopBarCenter,
  AppTopBarEnd,
  AppTopBarBrand,
  appTopBarVariants,
  appTopBarColumnVariants,
  appTopBarInnerVariants,
  appTopBarPageVariants,
};
