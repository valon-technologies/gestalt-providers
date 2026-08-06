import { useEffect, useMemo } from "react";
import { Button } from "@/components/ui/button";
import { AppAdminAutoDeployToggle } from "@/features/registry/app-admin-auto-deploy-toggle";
import { AppAdminFleetState } from "@/features/registry/app-admin-fleet-state";
import { useAppAdminRegistryContext } from "@/features/registry/app-admin-registry-context";
import { AppAdminSnapshotsTable } from "@/features/registry/app-admin-snapshots-table";
import { versionsSurfacePresentation } from "@/features/registry/deploy-mode";
import {
  formatRegistryDisabledReason,
  formatRegistryTime,
  formatRegistryTimeShort,
  isActiveRegistryRollout,
} from "@/features/registry/format";
import { presentFleetStatus } from "@/features/registry/fleet-status-presentation";
import {
  useAppAdminRegistryHistoryQuery,
  useAppAdminRegistryQuery,
  useUpdateAppAdminAutoDeployMutation,
} from "@/lib/queries";
import { TooltipProvider } from "@/components/ui/tooltip";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";

function RegistryRefreshedAt({
  appName,
  checkingLabel,
}: {
  appName: string;
  checkingLabel: string;
}) {
  const registryQuery = useAppAdminRegistryQuery(appName);
  const registryUpdatedAt = registryQuery.isFetched
    ? registryQuery.dataUpdatedAt
    : null;
  const registryUpdatedIso = registryUpdatedAt
    ? new Date(registryUpdatedAt).toISOString()
    : null;

  if (registryQuery.isCheckingForNewVersions) {
    return (
      <p className="text-xs text-muted-foreground" aria-live="polite">
        {checkingLabel}
      </p>
    );
  }

  if (!registryUpdatedAt) {
    return null;
  }

  return (
    <p className="text-xs text-muted-foreground" data-testid="registry-refreshed-at">
      Registry updated{" "}
      <time
        dateTime={registryUpdatedIso ?? undefined}
        title={registryUpdatedIso ? formatRegistryTime(registryUpdatedIso) : undefined}
      >
        {formatRegistryTimeShort(registryUpdatedAt)}
      </time>
    </p>
  );
}

function RegistryCheckForNewVersionsButton({
  appName,
  idleLabel,
  pendingLabel,
}: {
  appName: string;
  idleLabel: string;
  pendingLabel: string;
}) {
  const registryQuery = useAppAdminRegistryQuery(appName);

  const checking = registryQuery.isCheckingForNewVersions;
  return (
    <Button
      type="button"
      variant="outline"
      size="sm"
      onClick={registryQuery.checkForNewVersions}
      loading={checking}
      data-testid="check-for-new-versions"
    >
      {checking ? pendingLabel : idleLabel}
    </Button>
  );
}

export default function AppAdminSnapshotsPage() {
  const {
    appName,
    registry,
    deployingVersion,
    onDeployVersion,
    deployError,
  } = useAppAdminRegistryContext();
  const historyQuery = useAppAdminRegistryHistoryQuery(appName, true);
  const autoDeployMutation = useUpdateAppAdminAutoDeployMutation(appName);
  const historyRevisions = useMemo(
    () => historyQuery.data?.pages.flatMap((page) => page.revisions) ?? [],
    [historyQuery.data?.pages],
  );

  useEffect(() => {
    if (historyQuery.hasNextPage && !historyQuery.isFetchingNextPage) {
      void historyQuery.fetchNextPage();
    }
  }, [
    historyQuery.data?.pages,
    historyQuery.fetchNextPage,
    historyQuery.hasNextPage,
    historyQuery.isFetchingNextPage,
  ]);

  const autoDeployEnabled = registry.autoDeploy?.enabled ?? false;
  const rolloutActive = Boolean(
    registry.rollout && isActiveRegistryRollout(registry.rollout.state),
  );
  const surface = versionsSurfacePresentation({
    autoDeployEnabled,
    rolloutActive,
    hasDesiredVersion: Boolean(registry.desiredVersion),
  });
  const controlsDisabled =
    registry.selectionDisabled || deployingVersion !== null;
  const autoDeployError = autoDeployMutation.isError
    ? "Couldn't update automatic deploy. Check your connection and try again."
    : null;
  const disabledReason = formatRegistryDisabledReason(registry.disabledReason);
  const fleetView = presentFleetStatus(registry);

  return (
    <section aria-label="Versions">
      <PageHeader>
        <PageHeaderContent size="md">
          <PageHeaderTitle>Versions</PageHeaderTitle>
          <PageHeaderDescription>{surface.pageDescription}</PageHeaderDescription>
        </PageHeaderContent>
      </PageHeader>

      <div className="mt-6 space-y-8">
        <AppAdminFleetState registry={registry} view={fleetView} />

        <AppAdminAutoDeployToggle
          autoDeploy={registry.autoDeploy ?? { enabled: false }}
          title={surface.toggleTitle}
          description={surface.toggleDescription}
          adjacentHint={surface.manualDeployBlockedReason}
          disabled={autoDeployMutation.isPending}
          updating={autoDeployMutation.isPending}
          updateError={autoDeployError}
          onChange={(enabled) => autoDeployMutation.mutate(enabled)}
        />

        <TooltipProvider>
          <AppAdminSnapshotsTable
            registry={registry}
            historyRevisions={historyRevisions}
            controlsDisabled={controlsDisabled}
            offerManualDeploy={surface.offerManualDeploy}
            emptyTitle={surface.emptyTitle}
            emptyHint={surface.emptyHint}
            deployingVersion={deployingVersion}
            onDeployVersion={onDeployVersion}
            toolbarTrailing={
              <>
                <RegistryRefreshedAt
                  appName={appName}
                  checkingLabel={surface.checkForNewVersionsPendingLabel}
                />
                <RegistryCheckForNewVersionsButton
                  appName={appName}
                  idleLabel={surface.checkForNewVersionsLabel}
                  pendingLabel={surface.checkForNewVersionsPendingLabel}
                />
              </>
            }
          />
        </TooltipProvider>

        {registry.selectionDisabled && disabledReason ? (
          <p
            className="text-sm text-muted-foreground"
            data-testid="selection-disabled-reason"
          >
            {disabledReason}
          </p>
        ) : null}

        {deployError ? (
          <p className="text-sm text-destructive">{deployError}</p>
        ) : null}
      </div>
    </section>
  );
}
