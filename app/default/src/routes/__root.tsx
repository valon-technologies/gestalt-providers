import { Outlet, createRootRoute } from "@tanstack/react-router";
import { useRef } from "react";
import AuthGuard from "@/components/AuthGuard";
import { DevWorktreeBanner } from "@/components/DevWorktreeBanner";
import Nav from "@/components/Nav";
import { ThemeSwitcher } from "@/components/ThemeSwitcher";
import { Toaster } from "@/components/ui/sonner";
import { useSyncStickyAppChromeHeight } from "@/hooks/use-sync-sticky-app-chrome-height";
import { isLocalDevChrome } from "@/lib/local-dev-chrome";

export const rootRoute = createRootRoute({
  component: RootLayout,
});

/** Console chrome: top nav persists; route content is session-gated. */
function RootLayout() {
  const showLocalDevChrome = isLocalDevChrome();
  const stickyChromeRef = useRef<HTMLDivElement>(null);
  useSyncStickyAppChromeHeight(stickyChromeRef);

  return (
    <div className="min-h-screen">
      <div
        ref={stickyChromeRef}
        data-slot="app-sticky-chrome"
        className="sticky top-0 z-50"
      >
        <DevWorktreeBanner />
        <Nav />
      </div>
      <AuthGuard>
        <Outlet />
      </AuthGuard>
      {showLocalDevChrome ? <ThemeSwitcher /> : null}
      <Toaster />
    </div>
  );
}
