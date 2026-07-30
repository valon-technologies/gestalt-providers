import { Link, Outlet, useParams, useRouterState } from "@tanstack/react-router";
import { useCallback, useMemo } from "react";
import { isAPIErrorStatus } from "@/lib/api";
import { canManageApp, primaryConnectLabel } from "@/lib/catalogFilters";
import { getIntegrationLabel } from "@/lib/integrationSearch";
import {
  shouldShowIntegrationSettings,
  normalizeIntegrationStatus,
} from "@/lib/integrationStatus";
import {
  useAppAdminRegistryQuery,
  useAppAuthorizationMembersQuery,
  useDeployAppAdminVersionMutation,
  useIntegrationsQuery,
  useInvalidateIntegrations,
} from "@/lib/queries";
import Container from "@/components/Container";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import { SpinnerIcon } from "@/components/icons";
import { useDocumentTitle } from "@/hooks/use-document-title";
import { AppAdminRegistryProvider } from "@/features/registry/app-admin-registry-context";
import { isActiveRegistryRollout } from "@/features/registry/format";
import { RolloutBadge } from "@/features/registry/rollout-badge";
import { RegistryCode } from "@/features/registry/registry-code";
import {
  adminSurfaceForPathname,
  APP_ADMIN_NAV,
  APP_USER_NAV,
} from "@/features/app-workspace/app-nav";
import {
  AppWorkspaceProvider,
  hasAdminSurface,
  showAdminGroup,
  type AppWorkspaceCapabilities,
} from "@/features/app-workspace/app-workspace-context";
import { APP_SECTION_CARD } from "@/features/app-workspace/app-workspace-shared";
import type { AppAdminOutletContext } from "@/pages/app-admin-outlet-context";
import { AppWorkspaceNav } from "@/features/app-workspace/app-workspace-nav";

export default function AppWorkspaceLayout() {
  const { app: rawApp } = useParams({ from: "/apps/$app" });
  const app = decodeURIComponent(rawApp);
  const integrationsQuery = useIntegrationsQuery();
  const invalidateIntegrations = useInvalidateIntegrations();
  const integration =
    integrationsQuery.data?.find((item) => item.name === app) ?? null;
  const loading = integrationsQuery.isPending;
  const label = integration ? getIntegrationLabel(integration) : app;

  useDocumentTitle(label);

  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const isAdminPath = pathname.includes(`/apps/${app}/admin`);
  const canManageRegistry = Boolean(integration && canManageApp(integration));
  const registryQuery = useAppAdminRegistryQuery(app);
  const membersQuery = useAppAuthorizationMembersQuery(app);
  const deployMutation = useDeployAppAdminVersionMutation(app);
  const registryForbidden =
    registryQuery.isError && isAPIErrorStatus(registryQuery.error, 403);
  const registry = registryQuery.data;

  const authorizationAdmin = membersQuery.isSuccess;
  const authorizationProbeDone = membersQuery.isFetched;

  const status = integration
    ? normalizeIntegrationStatus(integration, "current_user")
    : null;
  const connectLabel = integration
    ? primaryConnectLabel(integration, "current_user")
    : null;
  const showManageConnection = Boolean(
    status &&
      !connectLabel &&
      (status.connected || shouldShowIntegrationSettings(status, false)),
  );
  const showConnectionNav = Boolean(
    connectLabel ||
      showManageConnection ||
      (integration?.connections?.length ?? 0) > 0,
  );

  const capabilities = useMemo<AppWorkspaceCapabilities>(() => {
    const registryAdmin =
      !registryForbidden &&
      (canManageRegistry || registryQuery.isSuccess);
    const authorization = authorizationAdmin;
    const workflows = registryAdmin || authorization;
    return {
      registry: registryAdmin,
      workflows,
      authorization,
    };
  }, [
    authorizationAdmin,
    canManageRegistry,
    registryForbidden,
    registryQuery.isSuccess,
  ]);

  const adminCapabilitiesReady =
    authorizationProbeDone &&
    (!canManageRegistry || registryQuery.isFetched);

  const error =
    integrationsQuery.error instanceof Error
      ? integrationsQuery.error.message
      : integrationsQuery.error
        ? "Failed to load app"
        : !loading && integrationsQuery.data && !integration && !isAdminPath
          ? `App “${app}” was not found in this workspace.`
          : null;

  const mountedPath = integration?.mountedPath?.trim();
  const deployConflict =
    deployMutation.isError && isAPIErrorStatus(deployMutation.error, 409);
  const deployFailed = deployMutation.isError && !deployConflict;
  const deployError =
    deployFailed && registry
      ? deployMutation.error instanceof Error
        ? deployMutation.error.message
        : "Failed to deploy version"
      : null;

  const onDeployVersion = useCallback(
    (version: string) => {
      deployMutation.mutate(version);
    },
    [deployMutation],
  );

  const registryOutlet = useMemo<AppAdminOutletContext | undefined>(() => {
    if (!registry || !capabilities.registry) return undefined;
    return {
      appName: app,
      registry,
      appMountedPath: mountedPath,
      deployingVersion:
        deployMutation.isPending ? deployMutation.variables : null,
      onDeployVersion,
      deployError,
      checkForNewVersions: registryQuery.checkForNewVersions,
      isCheckingForNewVersions: registryQuery.isCheckingForNewVersions,
      registryUpdatedAt: registryQuery.isFetched ? registryQuery.dataUpdatedAt : null,
    };
  }, [
    app,
    capabilities.registry,
    deployError,
    deployMutation.isPending,
    deployMutation.variables,
    mountedPath,
    onDeployVersion,
    registry,
    registryQuery.checkForNewVersions,
    registryQuery.isCheckingForNewVersions,
    registryQuery.dataUpdatedAt,
    registryQuery.isFetched,
  ]);

  const workspaceValue = useMemo(
    () => ({
      app,
      integration,
      loading,
      error,
      capabilities,
      showConnectionNav,
      registryOutlet,
      reloadIntegration: () => {
        void invalidateIntegrations();
      },
    }),
    [
      app,
      integration,
      loading,
      error,
      capabilities,
      showConnectionNav,
      registryOutlet,
      invalidateIntegrations,
    ],
  );

  const userNavItems = APP_USER_NAV.filter((item) =>
    "when" in item ? item.when !== "hasConnection" || showConnectionNav : true,
  );
  const adminNavItems = APP_ADMIN_NAV.filter((item) =>
    hasAdminSurface(capabilities, item.requires),
  );
  const adminGroupVisible =
    adminCapabilitiesReady && showAdminGroup(capabilities);

  const rolloutActive = registry?.rollout
    ? isActiveRegistryRollout(registry.rollout.state)
    : false;

  const requiredAdminSurface = isAdminPath
    ? adminSurfaceForPathname(pathname, app)
    : null;
  const adminSurfaceReady =
    !requiredAdminSurface ||
    (authorizationProbeDone &&
      (requiredAdminSurface !== "registry" || !canManageRegistry || registryQuery.isFetched));
  const adminSurfaceLoading = Boolean(requiredAdminSurface && !adminSurfaceReady);
  const adminAccessDenied = Boolean(
    requiredAdminSurface &&
      adminSurfaceReady &&
      !hasAdminSurface(capabilities, requiredAdminSurface),
  );

  const content = (
    <AppWorkspaceProvider value={workspaceValue}>
      <Container as="main" className="py-12">
        <div className="mb-6">
          <Breadcrumb>
            <BreadcrumbList>
              <BreadcrumbItem>
                <BreadcrumbLink asChild>
                  <Link to="/apps">Apps</Link>
                </BreadcrumbLink>
              </BreadcrumbItem>
              <BreadcrumbSeparator />
              <BreadcrumbItem>
                <BreadcrumbPage>{label}</BreadcrumbPage>
              </BreadcrumbItem>
            </BreadcrumbList>
          </Breadcrumb>
        </div>

        {loading ? (
          <p className="flex items-center gap-1.5 text-sm text-faint">
            <SpinnerIcon className="size-4 animate-spin" aria-hidden />
            Loading app…
          </p>
        ) : null}

        {error && !integration ? (
          <div className={APP_SECTION_CARD}>
            <p className="text-sm text-ember-500">{error}</p>
            <p className="mt-3 text-sm text-muted-foreground">
              <Link to="/apps" className="underline">
                Back to Apps
              </Link>
            </p>
          </div>
        ) : null}

        {(integration || isAdminPath) ? (
          <div className="grid gap-10 lg:grid-cols-[11rem_minmax(0,1fr)]">
            <aside className="min-w-0 w-44 shrink-0">
              <div className="lg:sticky lg:top-24">
                <AppWorkspaceNav
                  app={app}
                  userItems={userNavItems}
                  adminItems={adminNavItems}
                  adminGroupVisible={adminGroupVisible}
                />
              </div>
            </aside>

            <div className="min-w-0 space-y-8">
              {capabilities.registry && registry ? (
                <div className="hidden flex-wrap items-start justify-between gap-4 border-b border-border pb-6 lg:flex">
                  <div className="min-w-0 space-y-1">
                    <p className="text-sm text-muted-foreground">
                      Registry: {registry.registry}
                    </p>
                    {registry.desiredVersion ? (
                      <p className="text-sm text-muted-foreground">
                        Desired version:{" "}
                        <RegistryCode>{registry.desiredVersion}</RegistryCode>
                      </p>
                    ) : null}
                  </div>
                  <RolloutBadge app={registry} />
                </div>
              ) : null}

              {rolloutActive && registry?.rollout ? (
                <p
                  className="rounded-lg border border-info-foreground/40 bg-info px-4 py-3 text-sm text-info-foreground"
                  data-testid="rollout-active-banner"
                >
                  Rollout in progress:{" "}
                  <RegistryCode>{registry.rollout.version}</RegistryCode>
                </p>
              ) : null}

              {adminSurfaceLoading ? (
                <p className="text-sm text-muted-foreground">
                  Loading app admin…
                </p>
              ) : adminAccessDenied ? (
                <div
                  className="rounded-2xl border border-border bg-card p-6 text-card-foreground"
                  data-testid="app-admin-access-denied"
                >
                  <h1 className="text-2xl font-heading text-foreground">
                    Access denied
                  </h1>
                  <p className="mt-3 text-sm text-muted-foreground">
                    You do not have permission to manage this section of the app.
                  </p>
                </div>
              ) : (
                <Outlet />
              )}
            </div>
          </div>
        ) : null}
      </Container>
    </AppWorkspaceProvider>
  );

  if (registryOutlet) {
    return (
      <AppAdminRegistryProvider value={registryOutlet}>
        {content}
      </AppAdminRegistryProvider>
    );
  }

  return content;
}
