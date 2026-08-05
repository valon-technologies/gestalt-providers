import { Outlet, createRootRoute } from "@tanstack/react-router";
import AuthGuard from "@/components/AuthGuard";
import { DevWorktreeBanner } from "@/components/DevWorktreeBanner";
import Nav from "@/components/Nav";
import { ThemeSwitcher } from "@/components/ThemeSwitcher";
import { Toaster } from "@/components/ui/sonner";
import { isLocalDevChrome } from "@/lib/local-dev-chrome";

export const rootRoute = createRootRoute({
  component: RootLayout,
});

/** Console chrome: top nav persists; route content is session-gated. */
function RootLayout() {
  const showLocalDevChrome = isLocalDevChrome();

  return (
    <div className="min-h-screen">
      <div className="sticky top-0 z-50">
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
