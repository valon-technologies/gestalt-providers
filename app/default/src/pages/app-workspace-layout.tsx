import { Link, Outlet, useParams, useRouterState } from "@tanstack/react-router";
import { Info } from "lucide-react";
import { useCallback, useMemo } from "react";
import { isAPIErrorStatus } from "@/lib/api";
import {
  appShowsCredentialSurface,
  canManageApp,
} from "@/lib/catalogFilters";
import { getIntegrationLabel } from "@/lib/integrationSearch";
import {
  useAppAdminRegistryQuery,
  useAppAuthorizationMembersQuery,
  useDeployAppAdminVersionMutation,
  useIntegrationsQuery,
  useInvalidateIntegrations,
  useWorkflowRunQuery,
  workspaceConnectionView,
} from "@/lib/queries";
import { Code } from "@/components/ui/code";
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
import {
  Alert,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert";
import { useDocumentTitle } from "@/hooks/use-document-title";
import { presentFleetStatus } from "@/features/registry/fleet-status-presentation";
import { ReplicaHoverExclusiveProvider } from "@/features/registry/replica-hover-exclusive";
import {
  AppAdminRegistryProvider,
  type AppAdminRegistryContextValue,
} from "@/features/registry/app-admin-registry-context";
import { isActiveRegistryRollout } from "@/features/registry/format";
import { RegistryCode } from "@/features/registry/registry-code";
import {
  shortRunId,
  targetLabel,
} from "@/features/app-workflows/workflow-format";
import { useResolvedWorkflowRunRoute } from "@/features/app-workflows/use-resolved-workflow-run-route";
import {
  adminSurfaceForPathname,
  APP_ADMIN_NAV,
  APP_APPS_NAV,
  APP_USER_NAV,
  isAppAdminChromePath,
  isAppMetricsPath,
  isAppVersionsAdminPath,
  workflowAdminBreadcrumbTrail,
  workflowAdminRunIdFromPathname,
  workspaceDocumentTitle,
  workspaceLocationForPathname,
} from "@/features/app-workspace/app-nav";
import {
  AppWorkspaceProvider,
  hasAdminSurface,
  showAdminGroup,
  type AppWorkspaceCapabilities,
} from "@/features/app-workspace/app-workspace-context";
import { APP_SECTION_CARD } from "@/features/app-workspace/app-workspace-shared";
import { AppWorkspaceMobileNav } from "@/features/app-workspace/app-workspace-mobile-nav";
import { AppWorkspaceNav } from "@/features/app-workspace/app-workspace-nav";
import { PageLayout } from "@/components/ui/page-layout";
import { userFacingError, CONNECTION_STATUS_UNAVAILABLE } from "@/lib/user-facing-error";
import ErrorNotice from "@/components/ErrorNotice";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";

export default function AppWorkspaceLayout() {
  const { app: rawApp } = useParams({ from: "/apps/$app" });
  const app = decodeURIComponent(rawApp);
  const integrationsQuery = useIntegrationsQuery();
  const invalidateIntegrations = useInvalidateIntegrations();
  const integration =
    integrationsQuery.data?.find((item) => item.name === app) ?? null;
  const connectionView = workspaceConnectionView({
    directoryPending: integrationsQuery.isPending,
    overlayPending: integrationsQuery.overlayPending,
    overlayError: integrationsQuery.overlayError,
  });
  const loading = connectionView.status === "loading";
  const overlayUnavailable =
    connectionView.status === "overlay_unavailable" ? connectionView.error : null;
  const label = integration ? getIntegrationLabel(integration) : app;

  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const workspaceLocation = useMemo(
    () => workspaceLocationForPathname(pathname, app),
    [pathname, app],
  );
  const workflowRouteRunId = useMemo(
    () => workflowAdminRunIdFromPathname(pathname, app),
    [pathname, app],
  );
  const resolvedWorkflowRun = useResolvedWorkflowRunRoute(
    app,
    workflowRouteRunId ?? "",
  );
  const workflowRunId = workflowRouteRunId
    ? resolvedWorkflowRun.publicRunId
    : null;
  const workflowRunQuery = useWorkflowRunQuery(app, workflowRunId);
  const workflowRunLabel = useMemo(() => {
    const run = workflowRunQuery.data;
    if (!run) return undefined;
    return (
      targetLabel(run.target).trim() ||
      run.definitionId?.trim() ||
      undefined
    );
  }, [workflowRunQuery.data]);
  const workflowRunShortId = useMemo(() => {
    if (workflowRunQuery.data?.id) {
      return shortRunId(workflowRunQuery.data.id);
    }
    if (workflowRouteRunId) {
      return shortRunId(workflowRouteRunId);
    }
    return undefined;
  }, [workflowRouteRunId, workflowRunQuery.data?.id]);
  useDocumentTitle(
    workspaceDocumentTitle(label, workspaceLocation, {
      pathname,
      app,
      runLabel: workflowRunLabel,
    }),
  );

  const isVersionsPath =
    pathname === `/apps/${app}/versions` ||
    pathname.startsWith(`/apps/${app}/versions/`);
  const isMetricsPath = isAppMetricsPath(pathname, app);
  const isAdminChromePath = isAppAdminChromePath(pathname, app);
  const canManageRegistry = Boolean(integration && canManageApp(integration));
  const registryQuery = useAppAdminRegistryQuery(app);
  const membersQuery = useAppAuthorizationMembersQuery(app);
  const deployMutation = useDeployAppAdminVersionMutation(app);
  const registryForbidden =
    registryQuery.isError && isAPIErrorStatus(registryQuery.error, 403);
  const registry = registryQuery.data;

  const authorizationAdmin = membersQuery.isSuccess;
  const authorizationProbeDone = membersQuery.isFetched;

  const showConnectionNav = Boolean(
    integration && appShowsCredentialSurface(integration, "current_user"),
  );

  const capabilities = useMemo<AppWorkspaceCapabilities>(() => {
    const registryAdmin = registryQuery.isSuccess;
    const authorization = authorizationAdmin;
    const workflows = registryAdmin || authorization;
    return {
      registry: registryAdmin,
      workflows,
      authorization,
    };
  }, [authorizationAdmin, registryQuery.isSuccess]);

  const adminCapabilitiesReady =
    authorizationProbeDone &&
    (!canManageRegistry || registryQuery.isFetched);

  const error =
    integrationsQuery.error
      ? userFacingError(integrationsQuery.error, "Unable to load this app. Try again.")
      : overlayUnavailable
        ? userFacingError(overlayUnavailable, CONNECTION_STATUS_UNAVAILABLE)
        : !loading && integrationsQuery.data && !integration && !isAdminChromePath
        ? `App “${app}” was not found in this workspace.`
        : null;

  const mountedPath = integration?.mountedPath?.trim();
  const deployConflict =
    deployMutation.isError && isAPIErrorStatus(deployMutation.error, 409);
  const deployFailed = deployMutation.isError && !deployConflict;
  const deployError =
    deployFailed && registry
      ? userFacingError(deployMutation.error, "Couldn't deploy this version. Try again.")
      : null;

  const registryError =
    registryQuery.isError && !registryForbidden
      ? userFacingError(
          registryQuery.error,
          "Couldn't load versions. Try again.",
        )
      : null;

  // Depend on `mutate` only — the mutation result object is a new reference
  // every layout render, and a fresh onDeployVersion remounts the snapshots
  // table (and HoverCard triggers) on every registry poll.
  const deployVersion = deployMutation.mutate;
  const onDeployVersion = useCallback(
    (version: string) => {
      deployVersion(version);
    },
    [deployVersion],
  );

  const registryOutlet = useMemo<AppAdminRegistryContextValue | undefined>(() => {
    if (!registry || !capabilities.registry) return undefined;
    return {
      appName: app,
      registry,
      appMountedPath: mountedPath,
      deployingVersion:
        deployMutation.isPending ? deployMutation.variables : null,
      onDeployVersion,
      deployError,
      registryError,
      checkForNewVersions: registryQuery.checkForNewVersions,
      isCheckingForNewVersions: registryQuery.isCheckingForNewVersions,
      registryUpdatedAt: registryQuery.isFetched ? registryQuery.dataUpdatedAt : null,
    };
  }, [
    app,
    capabilities.registry,
    deployError,
    registryError,
    deployMutation.isPending,
    deployMutation.variables,
    mountedPath,
    onDeployVersion,
    registry,
    registryQuery.checkForNewVersions,
    registryQuery.dataUpdatedAt,
    registryQuery.isCheckingForNewVersions,
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
      reloadIntegration: () => invalidateIntegrations(),
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
    "when" in item
      ? item.when !== "hasCredentialSurface" || showConnectionNav
      : true,
  );
  const appsNavItems = APP_APPS_NAV.filter((item) =>
    hasAdminSurface(capabilities, item.requires),
  );
  const adminNavItems = APP_ADMIN_NAV.filter((item) =>
    hasAdminSurface(capabilities, item.requires),
  );
  const appsGroupVisible =
    adminCapabilitiesReady && capabilities.workflows;
  const adminGroupVisible =
    adminCapabilitiesReady && showAdminGroup(capabilities);

  const rolloutActive = registry?.rollout
    ? isActiveRegistryRollout(registry.rollout.state)
    : false;

  const requiredAdminSurface =
    isAdminChromePath
      ? adminSurfaceForPathname(pathname, app)
      : null;
  const adminSurfaceReady =
    !requiredAdminSurface ||
    (authorizationProbeDone &&
      (requiredAdminSurface !== "registry" || !canManageRegistry || registryQuery.isFetched));
  const adminSurfaceLoading = Boolean(requiredAdminSurface && !adminSurfaceReady);
  const adminSurfaceError =
    requiredAdminSurface === "registry" && registryError ? registryError : null;
  const adminAccessDenied = Boolean(
    requiredAdminSurface &&
      adminSurfaceReady &&
      !hasAdminSurface(capabilities, requiredAdminSurface),
  );
  const isWorkflowsPath = pathname.includes(`/apps/${app}/admin/workflows`);
  const showFleetState =
    isAppVersionsAdminPath(pathname, app) &&
    !isWorkflowsPath &&
    capabilities.registry &&
    registry;
  const fleetView = showFleetState && registry ? presentFleetStatus(registry) : null;
  const showRolloutBanner = Boolean(
    showFleetState &&
      rolloutActive &&
      registry?.rollout &&
      fleetView &&
      !fleetView.ownsActiveRolloutHeadline,
  );
  const workflowTrail = workflowAdminBreadcrumbTrail(pathname, app, {
    runLabel: workflowRunLabel,
    runShortId: workflowRunShortId,
  });
  const workspaceBreadcrumb = (
    <Breadcrumb>
      <BreadcrumbList>
        <BreadcrumbItem>
          <BreadcrumbLink asChild>
            <Link to="/apps">Apps</Link>
          </BreadcrumbLink>
        </BreadcrumbItem>
        <BreadcrumbSeparator />
        {workspaceLocation.isOverview ? (
          <BreadcrumbItem>
            <BreadcrumbPage>{label}</BreadcrumbPage>
          </BreadcrumbItem>
        ) : (
          <>
            <BreadcrumbItem>
              <BreadcrumbLink asChild>
                <Link to="/apps/$app" params={{ app }}>
                  {label}
                </Link>
              </BreadcrumbLink>
            </BreadcrumbItem>
            <BreadcrumbSeparator />
            {workflowTrail ? (
              <>
                <BreadcrumbItem>
                  <BreadcrumbLink asChild>
                    <Link
                      to="/apps/$app/admin/workflows"
                      params={{ app }}
                    >
                      {workspaceLocation.label}
                    </Link>
                  </BreadcrumbLink>
                </BreadcrumbItem>
                {workflowTrail.map((segment, index) => {
                  const isLast = index === workflowTrail.length - 1;
                  return (
                    <span key={`${segment.label}-${index}`} className="contents">
                      <BreadcrumbSeparator />
                      <BreadcrumbItem className="inline-flex max-w-full items-center gap-2">
                        {isLast || !segment.link ? (
                          <BreadcrumbPage className="min-w-0 truncate">
                            {segment.label}
                          </BreadcrumbPage>
                        ) : (
                          <BreadcrumbLink asChild>
                            <Link
                              to={segment.link.to}
                              params={segment.link.params}
                              className="min-w-0 truncate"
                            >
                              {segment.label}
                            </Link>
                          </BreadcrumbLink>
                        )}
                        {segment.code ? (
                          <Code className="shrink-0 text-xs text-muted-foreground">
                            {segment.code}
                          </Code>
                        ) : null}
                      </BreadcrumbItem>
                    </span>
                  );
                })}
              </>
            ) : (
              <BreadcrumbItem>
                <BreadcrumbPage>
                  {workspaceLocation.label}
                </BreadcrumbPage>
              </BreadcrumbItem>
            )}
          </>
        )}
      </BreadcrumbList>
    </Breadcrumb>
  );
  const content = (
    <AppWorkspaceProvider value={workspaceValue}>
      <Container>
        {(integration || isAdminChromePath) ? (
          <PageLayout
            tracks="compact"
            pane={
              <AppWorkspaceNav
                app={app}
                userItems={userNavItems}
                appsItems={appsNavItems}
                appsGroupVisible={appsGroupVisible}
                adminItems={adminNavItems}
                adminGroupVisible={adminGroupVisible}
              />
            }
            paneMobile={
              <AppWorkspaceMobileNav
                app={app}
                pathname={pathname}
                userItems={userNavItems}
                appsItems={appsNavItems}
                appsGroupVisible={appsGroupVisible}
                adminItems={adminNavItems}
                adminGroupVisible={adminGroupVisible}
              />
            }
          >
            <div className="space-y-8">
              {workspaceBreadcrumb}
              <ReplicaHoverExclusiveProvider>
              {loading ? (
                <p className="flex items-center gap-1.5 text-sm text-muted-foreground-soft">
                  <SpinnerIcon className="size-4 motion-safe:animate-spin" aria-hidden />
                  Loading app…
                </p>
              ) : overlayUnavailable ? (
                <ErrorNotice
                  message={userFacingError(
                    overlayUnavailable,
                    CONNECTION_STATUS_UNAVAILABLE,
                  )}
                  retrying={integrationsQuery.overlayFetching}
                  onRetry={() => {
                    void integrationsQuery.refetchOverlay();
                  }}
                />
              ) : (
                <>
              {showRolloutBanner && registry?.rollout ? (
                <Alert
                  variant="info"
                  data-testid="rollout-active-banner"
                  aria-label="Rollout in progress"
                >
                  <Info aria-hidden="true" />
                  <AlertTitle>Rolling out</AlertTitle>
                  <AlertDescription className="text-pretty">
                    Deploying{" "}
                    <RegistryCode>{registry.rollout.version}</RegistryCode>{" "}
                    across the fleet.
                  </AlertDescription>
                </Alert>
              ) : null}

              {adminSurfaceLoading ? (
                <section
                  aria-busy="true"
                  aria-label={
                    isVersionsPath
                      ? "Versions"
                      : isMetricsPath
                        ? "Metrics"
                        : "App admin"
                  }
                  data-testid="app-admin-surface-loading"
                >
                  {isVersionsPath ? (
                    <div className="space-y-6">
                      <PageHeader>
                        <PageHeaderContent size="md">
                          <PageHeaderTitle>Versions</PageHeaderTitle>
                          <PageHeaderDescription>
                            Loading deployment status…
                          </PageHeaderDescription>
                        </PageHeaderContent>
                      </PageHeader>
                      <div
                        className="h-16 animate-pulse rounded-lg bg-muted"
                        aria-hidden="true"
                      />
                      <div
                        className="h-40 animate-pulse rounded-lg bg-muted"
                        aria-hidden="true"
                      />
                    </div>
                  ) : (
                    <p className="text-sm text-muted-foreground">
                      Loading app admin…
                    </p>
                  )}
                </section>
              ) : adminSurfaceError ? (
                <ErrorNotice
                  message={adminSurfaceError}
                  onRetry={registryQuery.checkForNewVersions}
                  retrying={registryQuery.isCheckingForNewVersions}
                />
              ) : adminAccessDenied ? (
                <div
                  className="rounded-2xl border border-border bg-card p-6 text-card-foreground"
                  data-testid="app-admin-access-denied"
                >
                  <PageHeader>
                    <PageHeaderContent size="sm">
                      <PageHeaderTitle>Access denied</PageHeaderTitle>
                      <PageHeaderDescription>
                        You do not have permission to manage this section of the app.
                      </PageHeaderDescription>
                    </PageHeaderContent>
                  </PageHeader>
                </div>
              ) : (
                <Outlet />
              )}
                </>
              )}
              </ReplicaHoverExclusiveProvider>
            </div>
          </PageLayout>
        ) : (
          <>
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
              <p className="flex items-center gap-1.5 text-sm text-muted-foreground-soft">
                <SpinnerIcon className="size-4 motion-safe:animate-spin" aria-hidden />
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
          </>
        )}
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
