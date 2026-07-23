import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderDescription,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import { PublishedVersionDetail } from "@/features/registry/published-version-detail";
import type { AppAdminPublishedVersion } from "@/features/registry/types";
import {
  formatPublishedVersionOptionMeta,
  isActiveRegistryRollout,
  sortPublishedVersionsNewestFirst,
} from "@/features/registry/format";
import { RegistryCode } from "@/features/registry/registry-code";
import { RolloutBadge } from "@/features/registry/rollout-badge";
import type { RegistryAppSummary } from "@/features/registry/types";

function PublishedVersionOptionLabel({
  version,
}: {
  version: AppAdminPublishedVersion;
}) {
  const meta = formatPublishedVersionOptionMeta(version);
  return (
    <span className="flex min-w-0 items-center gap-1.5">
      <span className="font-mono text-[0.92em]">{version.version}</span>
      {meta ? <span className="font-sans text-muted-foreground">· {meta}</span> : null}
    </span>
  );
}

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
  const publishedVersions = sortPublishedVersionsNewestFirst(registry.publishedVersions);
  const selectedPublished = publishedVersions.find(
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

        {publishedVersions.length === 0 ? (
          <p className="text-sm text-muted">No published versions are available.</p>
        ) : (
          <div className="space-y-4">
            <div className="space-y-1.5">
              <Label htmlFor="app-admin-version-select" variant="field">
                Published version
              </Label>
              <Select
                value={selectedVersion}
                disabled={controlsDisabled}
                onValueChange={onSelectedVersionChange}
              >
                <SelectTrigger
                  id="app-admin-version-select"
                  data-testid="version-select"
                  className="max-w-xl"
                >
                  <SelectValue placeholder="Select a published version" />
                </SelectTrigger>
                <SelectContent>
                  {publishedVersions.map((version) => (
                    <SelectItem key={version.version} value={version.version}>
                      <PublishedVersionOptionLabel version={version} />
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
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
