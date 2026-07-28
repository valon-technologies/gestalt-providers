import { useMemo } from "react";
import { Link, useParams } from "@tanstack/react-router";
import Container from "@/components/Container";
import { AppAdminVersionPanel } from "@/features/registry/app-admin-version-panel";
import { useDocumentTitle } from "@/hooks/use-document-title";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  useAppAdminRegistryQuery,
  useDeployAppAdminVersionMutation,
  useIntegrationsQuery,
  useUpdateAppAdminAutoDeployMutation,
} from "@/lib/queries";
import { isAPIErrorStatus } from "@/lib/api";

const APPS_PATH = "/apps";

export default function AppAdminPage() {
  const { app: appName } = useParams({ from: "/apps/$app/admin" });
  useDocumentTitle(`${appName} · App management`);
  const integrationsQuery = useIntegrationsQuery();
  const registryQuery = useAppAdminRegistryQuery(appName);
  const deployMutation = useDeployAppAdminVersionMutation(appName);
  const autoDeployMutation = useUpdateAppAdminAutoDeployMutation(appName);

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
  const autoDeployFailed = autoDeployMutation.isError;
  const autoDeployError =
    autoDeployFailed && autoDeployMutation.error instanceof Error
      ? autoDeployMutation.error.message
      : autoDeployFailed
        ? "Failed to update auto-deploy"
        : null;
  const error =
    registryQuery.isError && !forbidden
      ? registryQuery.error instanceof Error
        ? registryQuery.error.message
        : "Failed to load app registry"
      : deployFailed && !registry
        ? deployMutation.error instanceof Error
          ? deployMutation.error.message
          : "Failed to deploy version"
        : deployFailed
          ? deployMutation.error instanceof Error
            ? deployMutation.error.message
            : "Failed to deploy version"
          : null;

  return (
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
              className="animate-fade-in-up rounded-2xl border border-alpha bg-base-white p-6 dark:bg-surface"
              data-testid="app-admin-access-denied"
            >
              <PageHeader>
                <PageHeaderContent size="lg">
                  <PageHeaderTitle>Access denied</PageHeaderTitle>
                  <PageHeaderDescription>
                    You do not have permission to manage this app.
                  </PageHeaderDescription>
                </PageHeaderContent>
              </PageHeader>
            </div>
          ) : error && !registry ? (
            <p className="text-sm text-ember-500">{error}</p>
          ) : registry ? (
            <div className="animate-fade-in-up [animation-delay:60ms]">
              <AppAdminVersionPanel
                registry={registry}
                appMountedPath={appMountedPath}
                deployingVersion={
                  deployMutation.isPending ? deployMutation.variables : null
                }
                onDeployVersion={(version) => deployMutation.mutate(version)}
                onCheckForNewVersions={registryQuery.checkForNewVersions}
                isCheckingForNewVersions={registryQuery.isFetching}
                onAutoDeployChange={(enabled) => autoDeployMutation.mutate(enabled)}
                isUpdatingAutoDeploy={autoDeployMutation.isPending}
                autoDeployError={autoDeployError}
                error={error}
              />
            </div>
          ) : null}
        </Container>
  );
}
