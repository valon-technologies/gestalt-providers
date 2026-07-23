import Button from "@/components/Button";
import { INPUT_CLASSES } from "@/lib/constants";
import { PublishedVersionDetail } from "@/features/registry/published-version-detail";
import {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderDescription,
  SectionHeaderTitle,
} from "@/features/registry/section-header";
import type { AppAdminPublishedVersion } from "@/features/registry/types";
import { isActiveRegistryRollout } from "@/features/registry/format";
import { RegistryCode } from "@/features/registry/registry-code";
import { RolloutBadge } from "@/features/registry/rollout-badge";
import type { RegistryAppSummary } from "@/features/registry/types";

export function AppAdminVersionPanel({
  registry,
  selectedVersion,
  onSelectedVersionChange,
  onSelectVersion,
  submitting,
  error,
}: {
  registry: RegistryAppSummary & {
    publishedVersions: AppAdminPublishedVersion[];
    selectionDisabled: boolean;
    disabledReason?: string;
  };
  selectedVersion: string;
  onSelectedVersionChange: (version: string) => void;
  onSelectVersion: () => void;
  submitting: boolean;
  error: string | null;
}) {
  const selectedPublished = registry.publishedVersions.find(
    (version) => version.version === selectedVersion,
  );
  const rolloutActive = registry.rollout
    ? isActiveRegistryRollout(registry.rollout.state)
    : false;
  const controlsDisabled = registry.selectionDisabled || submitting;
  const canSubmit =
    !!selectedVersion &&
    !controlsDisabled &&
    selectedVersion !== registry.desiredVersion;

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
            <SectionHeaderTitle>Desired version</SectionHeaderTitle>
            <SectionHeaderDescription>
              {registry.desiredVersion
                ? "Select a published version to upgrade, revert, or reinstall across the fleet."
                : "No version is installed yet. Select a published version to install this app across the fleet."}
            </SectionHeaderDescription>
          </SectionHeaderContent>
        </SectionHeader>

        {registry.publishedVersions.length === 0 ? (
          <p className="text-sm text-muted">No published versions are available.</p>
        ) : (
          <div className="space-y-4">
            <div>
              <label htmlFor="app-admin-version-select" className="label-text">
                Published version
              </label>
              <select
                id="app-admin-version-select"
                data-testid="version-select"
                className={`mt-1.5 w-full max-w-xl ${INPUT_CLASSES}`}
                value={selectedVersion}
                disabled={controlsDisabled}
                onChange={(event) => onSelectedVersionChange(event.target.value)}
              >
                {registry.publishedVersions.map((version) => (
                  <option key={version.version} value={version.version}>
                    {version.version}
                  </option>
                ))}
              </select>
            </div>

            {selectedPublished ? (
              <PublishedVersionDetail version={selectedPublished} />
            ) : null}

            {registry.selectionDisabled && registry.disabledReason ? (
              <p className="text-sm text-muted" data-testid="selection-disabled-reason">
                {registry.disabledReason}
              </p>
            ) : null}

            {error ? <p className="text-sm text-ember-500">{error}</p> : null}

            <div className="flex justify-end">
              <Button
                type="button"
                data-testid="select-version-button"
                disabled={!canSubmit}
                onClick={onSelectVersion}
              >
                {submitting ? "Selecting..." : "Select version"}
              </Button>
            </div>
          </div>
        )}
      </section>
    </div>
  );
}
