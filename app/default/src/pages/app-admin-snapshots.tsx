import { Button } from "@/components/ui/button";
import { AppAdminAutoDeployToggle } from "@/features/registry/app-admin-auto-deploy-toggle";
import { useAppAdminRegistryContext } from "@/features/registry/app-admin-registry-context";
import { AppAdminSnapshotsTable } from "@/features/registry/app-admin-snapshots-table";
import {
  formatRegistryTime,
  formatRegistryTimeShort,
} from "@/features/registry/format";
import { RolloutPhaseStepper } from "@/features/registry/rollout-phase-stepper";
import { useUpdateAppAdminAutoDeployMutation } from "@/lib/queries";
import { Loader2 } from "lucide-react";

export default function AppAdminSnapshotsPage() {
  const {
    appName,
    registry,
    deployingVersion,
    onDeployVersion,
    deployError,
    checkForNewVersions,
    isCheckingForNewVersions,
    registryUpdatedAt,
  } = useAppAdminRegistryContext();
  const autoDeployMutation = useUpdateAppAdminAutoDeployMutation(appName);

  const autoDeployEnabled = registry.autoDeploy?.enabled ?? false;
  const controlsDisabled =
    registry.selectionDisabled || deployingVersion !== null || autoDeployEnabled;
  const autoDeployError =
    autoDeployMutation.isError && autoDeployMutation.error instanceof Error
      ? autoDeployMutation.error.message
      : autoDeployMutation.isError
        ? "Failed to update auto-deploy"
        : null;
  const registryUpdatedIso = registryUpdatedAt
    ? new Date(registryUpdatedAt).toISOString()
    : null;

  return (
    <section aria-label="Published snapshots">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-2xl font-heading text-foreground">
            Published snapshots
          </h1>
          <p className="mt-1 text-sm text-muted-foreground">
            {autoDeployEnabled
              ? "Automatic deploy is on — new snapshots are admitted without manual deploy."
              : registry.desiredVersion
                ? "Deploy any published snapshot across the fleet."
                : "No version is installed yet. Deploy a published snapshot to install this app across the fleet."}
          </p>
        </div>
        <div className="flex flex-col items-start gap-2 sm:flex-row sm:items-center sm:gap-4">
          {isCheckingForNewVersions ? (
            <p className="text-sm text-muted-foreground">Refreshing…</p>
          ) : registryUpdatedAt ? (
            <p className="text-sm text-muted-foreground" data-testid="registry-refreshed-at">
              Refreshed at{" "}
              <time
                dateTime={registryUpdatedIso ?? undefined}
                title={registryUpdatedIso ? formatRegistryTime(registryUpdatedIso) : undefined}
              >
                {formatRegistryTimeShort(registryUpdatedAt)}
              </time>
            </p>
          ) : null}
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={checkForNewVersions}
            disabled={isCheckingForNewVersions}
            data-testid="check-for-new-versions"
          >
            {isCheckingForNewVersions ? (
              <>
                <Loader2 className="size-4 animate-spin" aria-hidden="true" />
                Checking…
              </>
            ) : (
              "Check for new versions"
            )}
          </Button>
        </div>
      </div>

      <div className="mt-6 space-y-8">
        <RolloutPhaseStepper rollout={registry.rollout} />

        <AppAdminAutoDeployToggle
          autoDeploy={registry.autoDeploy ?? { enabled: false }}
          disabled={autoDeployMutation.isPending}
          updating={autoDeployMutation.isPending}
          updateError={autoDeployError}
          onChange={(enabled) => autoDeployMutation.mutate(enabled)}
        />

        <AppAdminSnapshotsTable
          registry={registry}
          controlsDisabled={controlsDisabled}
          deployingVersion={deployingVersion}
          onDeployVersion={onDeployVersion}
        />

        {registry.selectionDisabled && registry.disabledReason ? (
          <p
            className="text-sm text-muted-foreground"
            data-testid="selection-disabled-reason"
          >
            {registry.disabledReason}
          </p>
        ) : null}

        {deployError ? (
          <p className="text-sm text-destructive">{deployError}</p>
        ) : null}
      </div>
    </section>
  );
}
