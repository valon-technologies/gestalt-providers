import { useMemo } from "react";
import { Link, Outlet, useParams } from "@tanstack/react-router";
import AuthGuard from "@/components/AuthGuard";
import Container from "@/components/Container";
import Nav from "@/components/Nav";
import { RolloutBadge } from "@/features/registry/rollout-badge";
import { formatRolloutStateLabel, isActiveRegistryRollout } from "@/features/registry/format";
import { RegistryCode } from "@/features/registry/registry-code";
import { APP_ADMIN_NAV_ITEMS } from "@/features/registry/app-admin-nav";
import {
  AppAdminRegistryProvider,
} from "@/features/registry/app-admin-registry-context";
import { useDocumentTitle } from "@/hooks/use-document-title";
import {
  useAppAdminRegistryQuery,
  useDeployAppAdminVersionMutation,
  useIntegrationsQuery,
} from "@/lib/queries";
import { isAPIErrorStatus } from "@/lib/api";
import type { AppAdminOutletContext } from "./app-admin-outlet-context";

const APPS_PATH = "/apps";

export default function AppAdminLayout() {
  const { app: appName } = useParams({ from: "/apps/$app/admin" });
  useDocumentTitle(`${appName} · App management`);
  const integrationsQuery = useIntegrationsQuery();
  const registryQuery = useAppAdminRegistryQuery(appName);
  const deployMutation = useDeployAppAdminVersionMutation(appName);

  const forbidden =
    registryQuery.isError && isAPIErrorStatus(registryQuery.error, 403);
  const registry = registryQuery.data;
  const appMountedPath = useMemo(() => {
    const mountedPath = integrationsQuery.data
      ?.find((integration) => integration.name === appName)
      ?.mountedPath?.trim();
    return mountedPath || undefined;
  }, [appName, integrationsQuery.data]);
  const deployConflict =
    deployMutation.isError && isAPIErrorStatus(deployMutation.error, 409);
  const deployFailed = deployMutation.isError && !deployConflict;
  const deployError =
    deployFailed && registry
      ? deployMutation.error instanceof Error
        ? deployMutation.error.message
        : "Failed to deploy version"
      : null;

  const outletContext: AppAdminOutletContext | undefined = registry
    ? {
        appName,
        registry,
        appMountedPath,
        deployingVersion:
          deployMutation.isPending ? deployMutation.variables : null,
        onDeployVersion: (version) => deployMutation.mutate(version),
        deployError,
      }
    : undefined;

  const rolloutActive = registry?.rollout
    ? isActiveRegistryRollout(registry.rollout.state)
    : false;

  return (
    <AuthGuard>
      <div className="min-h-screen">
        <Nav />
        <Container as="main" className="py-12">
          <div className="mb-8 animate-fade-in-up">
            <Link
              to={APPS_PATH}
              className="text-sm text-muted-foreground transition-colors hover:text-foreground"
            >
              ← Back to apps
            </Link>
          </div>

          {registryQuery.isPending ? (
            <p className="text-sm text-muted-foreground">Loading app registry…</p>
          ) : forbidden ? (
            <div
              className="animate-fade-in-up rounded-2xl border border-border bg-card p-6 text-card-foreground"
              data-testid="app-admin-access-denied"
            >
              <h1 className="text-2xl font-heading text-foreground">Access denied</h1>
              <p className="mt-3 text-sm text-muted-foreground">
                You do not have permission to manage this app.
              </p>
            </div>
          ) : registry ? (
            <div className="animate-fade-in-up [animation-delay:60ms] lg:grid lg:grid-cols-[220px_minmax(0,1fr)] lg:gap-10">
              <aside className="mb-8 lg:mb-0">
                <div className="lg:sticky lg:top-24">
                  <div className="mb-6 lg:hidden">
                    <h1 className="text-2xl font-heading text-foreground">{appName}</h1>
                    <p className="mt-1 text-sm text-muted-foreground">
                      Registry: {registry.registry}
                    </p>
                  </div>
                  <nav
                    className="flex flex-wrap gap-1 lg:flex-col lg:gap-0.5"
                    aria-label="App admin"
                  >
                    {APP_ADMIN_NAV_ITEMS.map((item) => (
                      <Link
                        key={item.id}
                        to={item.to}
                        params={{ app: appName }}
                        className="rounded-md px-3 py-2 text-sm text-muted-foreground transition-colors duration-150 hover:text-foreground"
                        activeProps={{
                          className:
                            "rounded-md bg-accent px-3 py-2 text-sm font-medium text-accent-foreground transition-colors duration-150",
                        }}
                        data-testid={`app-admin-nav-${item.id}`}
                      >
                        {item.label}
                      </Link>
                    ))}
                  </nav>
                </div>
              </aside>

              <div className="min-w-0 space-y-8">
                <div className="hidden flex-wrap items-start justify-between gap-4 lg:flex">
                  <div className="min-w-0 space-y-1">
                    <h1 className="text-2xl font-heading text-foreground">{appName}</h1>
                    <p className="text-sm text-muted-foreground">
                      Registry: {registry.registry}
                    </p>
                    {registry.desiredVersion ? (
                      <p className="text-sm text-muted-foreground">
                        Desired version:{" "}
                        <RegistryCode>{registry.desiredVersion}</RegistryCode>
                      </p>
                    ) : null}
                    {appMountedPath ? (
                      <a
                        href={appMountedPath}
                        className="inline-flex text-sm font-medium text-primary transition-colors hover:text-primary"
                        data-testid="open-app-link"
                      >
                        Open app →
                      </a>
                    ) : null}
                  </div>
                  <RolloutBadge app={registry} />
                </div>

                {rolloutActive && registry.rollout ? (
                  <p
                    className="rounded-lg border border-info-foreground/40 bg-info px-4 py-3 text-sm text-info-foreground"
                    data-testid="rollout-active-banner"
                  >
                    Rollout {formatRolloutStateLabel(registry.rollout.state)}:{" "}
                    <RegistryCode>{registry.rollout.version}</RegistryCode>
                  </p>
                ) : null}

                <AppAdminRegistryProvider value={outletContext!}>
                  <Outlet />
                </AppAdminRegistryProvider>
              </div>
            </div>
          ) : (
            <p className="text-sm text-destructive">
              {registryQuery.error instanceof Error
                ? registryQuery.error.message
                : "Failed to load app registry"}
            </p>
          )}
        </Container>
      </div>
    </AuthGuard>
  );
}
