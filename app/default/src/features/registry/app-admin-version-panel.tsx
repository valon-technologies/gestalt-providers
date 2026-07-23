import {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderDescription,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import { AppAdminSnapshotsTable } from "@/features/registry/app-admin-snapshots-table";
import type { AppAdminPublishedVersion } from "@/features/registry/types";
import { isActiveRegistryRollout } from "@/features/registry/format";
import { RegistryCode } from "@/features/registry/registry-code";
import { RolloutBadge } from "@/features/registry/rollout-badge";
import type { RegistryAppSummary } from "@/features/registry/types";

export function AppAdminVersionPanel({
  registry,
  deployingVersion,
  onDeployVersion,
  error,
}: {
  registry: RegistryAppSummary & {
    publishedVersions: AppAdminPublishedVersion[];
    selectionDisabled: boolean;
    disabledReason?: string;
    desiredVersion?: string;
  };
  deployingVersion: string | null;
  onDeployVersion: (version: string) => void;
  error: string | null;
}) {
  const rolloutActive = registry.rollout
    ? isActiveRegistryRollout(registry.rollout.state)
    : false;
  const controlsDisabled = registry.selectionDisabled || deployingVersion !== null;

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0 space-y-1">
          <h1 className="text-2xl font-heading text-primary">{registry.app}</h1>
          <p className="text-sm text-muted">Registry: {registry.registry}</p>
          <p className="text-sm text-faint">App management</p>
        </div>
        <RolloutBadge app={registry} />
      </div>

      {rolloutActive && registry.rollout ? (
        <p
          className="rounded-lg border border-gold-200 bg-gold-50 px-4 py-3 text-sm text-gold-900 dark:border-gold-800 dark:bg-gold-950/40 dark:text-gold-100"
          data-testid="rollout-active-banner"
        >
          Rollout {registry.rollout.state}:{" "}
          <RegistryCode>{registry.rollout.version}</RegistryCode>
        </p>
      ) : null}

      <section className="space-y-4 rounded-2xl border border-alpha bg-base-white p-6 dark:bg-surface">
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
          <p className="text-sm text-muted" data-testid="selection-disabled-reason">
            {registry.disabledReason}
          </p>
        ) : null}

        {error ? <p className="text-sm text-ember-500">{error}</p> : null}
      </section>
    </div>
  );
}
