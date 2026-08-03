import { Outlet, createRootRoute } from "@tanstack/react-router";
import AuthGuard from "@/components/AuthGuard";
import { DevWorktreeBanner } from "@/components/DevWorktreeBanner";
import Nav from "@/components/Nav";
import { ThemeSwitcher } from "@/components/ThemeSwitcher";

export const rootRoute = createRootRoute({
  component: RootLayout,
});

/** Console chrome: top nav persists; route content is session-gated. */
function RootLayout() {
  return (
    <div className="min-h-screen">
      <div className="sticky top-0 z-50">
        {import.meta.env.DEV ? <DevWorktreeBanner /> : null}
        <Nav />
      </div>
      <AuthGuard>
        <Outlet />
      </AuthGuard>
      {import.meta.env.DEV ? <ThemeSwitcher /> : null}
    </div>
  );
}
