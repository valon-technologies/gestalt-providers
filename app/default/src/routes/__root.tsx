import { Outlet, createRootRoute } from "@tanstack/react-router";
import AuthGuard from "@/components/AuthGuard";
import Nav from "@/components/Nav";

export const rootRoute = createRootRoute({
  component: RootLayout,
});

/** Console chrome: session gate + top nav persist across route changes. */
function RootLayout() {
  return (
    <AuthGuard>
      <div className="min-h-screen">
        <Nav />
        <Outlet />
      </div>
    </AuthGuard>
  );
}
