import { AppAdminSnapshotsTable } from "@/features/registry/app-admin-snapshots-table";
import { useAppAdminRegistryContext } from "@/features/registry/app-admin-registry-context";

export default function AppAdminSnapshotsPage() {
  const { registry, deployingVersion, onDeployVersion, deployError } =
    useAppAdminRegistryContext();
  const controlsDisabled =
    registry.selectionDisabled || deployingVersion !== null;

  return (
    <section aria-label="Published snapshots">
      <h1 className="text-2xl font-heading text-foreground">
        Published snapshots
      </h1>
      <p className="mt-1 text-sm text-muted-foreground">
        {registry.desiredVersion
          ? "Deploy any published snapshot across the fleet."
          : "No version is installed yet. Deploy a published snapshot to install this app across the fleet."}
      </p>

      <div className="mt-6 space-y-4">
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
