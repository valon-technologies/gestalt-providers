import {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderDescription,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import { AppAdminSnapshotsTable } from "@/features/registry/app-admin-snapshots-table";
import { useAppAdminRegistryContext } from "@/features/registry/app-admin-registry-context";

export default function AppAdminSnapshotsPage() {
  const { registry, deployingVersion, onDeployVersion, deployError } =
    useAppAdminRegistryContext();
  const controlsDisabled =
    registry.selectionDisabled || deployingVersion !== null;

  return (
    <section className="space-y-4 rounded-2xl border border-border bg-card p-6 text-card-foreground">
      <SectionHeader>
        <SectionHeaderContent>
          <SectionHeaderTitle>Published snapshots</SectionHeaderTitle>
          <SectionHeaderDescription>
            {registry.desiredVersion
              ? "Deploy any published snapshot across the fleet."
              : "No version is installed yet. Deploy a published snapshot to install this app across the fleet."}
          </SectionHeaderDescription>
        </SectionHeaderContent>
      </SectionHeader>

      <AppAdminSnapshotsTable
        registry={registry}
        controlsDisabled={controlsDisabled}
        deployingVersion={deployingVersion}
        onDeployVersion={onDeployVersion}
      />

      {registry.selectionDisabled && registry.disabledReason ? (
        <p className="text-sm text-muted-foreground" data-testid="selection-disabled-reason">
          {registry.disabledReason}
        </p>
      ) : null}

      {deployError ? <p className="text-sm text-destructive">{deployError}</p> : null}
    </section>
  );
}
