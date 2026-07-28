import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import {
  SectionHeader,
  SectionHeaderActions,
  SectionHeaderContent,
  SectionHeaderDescription,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import { Eyebrow } from "@/components/ui/eyebrow";
import {
  PageHeader,
  PageHeaderActions,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import { AppAdminAutoDeployToggle } from "@/features/registry/app-admin-auto-deploy-toggle";
import { AppAdminHistoryTable } from "@/features/registry/app-admin-history-table";
import { AppAdminSnapshotsTable } from "@/features/registry/app-admin-snapshots-table";
import { RolloutPhaseStepper } from "@/features/registry/rollout-phase-stepper";
import { RegistryCode } from "@/features/registry/registry-code";
import { RolloutBadge } from "@/features/registry/rollout-badge";
import type { AppAdminRegistryResponse } from "@/features/registry/types";
import { useAppAdminRegistryHistoryQuery } from "@/lib/queries";
import { Loader2 } from "lucide-react";

type AppAdminTab = "snapshots" | "history";

export function AppAdminVersionPanel({
  registry,
  appMountedPath,
  deployingVersion,
  onDeployVersion,
  onCheckForNewVersions,
  isCheckingForNewVersions = false,
  onAutoDeployChange,
  isUpdatingAutoDeploy = false,
  autoDeployError = null,
  error,
}: {
  registry: AppAdminRegistryResponse;
  appMountedPath?: string;
  deployingVersion: string | null;
  onDeployVersion: (version: string) => void;
  onCheckForNewVersions: () => void;
  isCheckingForNewVersions?: boolean;
  onAutoDeployChange: (enabled: boolean) => void;
  isUpdatingAutoDeploy?: boolean;
  autoDeployError?: string | null;
  error: string | null;
}) {
  const [activeTab, setActiveTab] = useState<AppAdminTab>("snapshots");
  const historyQuery = useAppAdminRegistryHistoryQuery(
    registry.app,
    activeTab === "history",
  );
  const historyRevisions =
    historyQuery.data?.pages.flatMap((page) => page.revisions) ?? [];
  const historyError = historyQuery.error
    ? historyQuery.error instanceof Error
      ? historyQuery.error.message
      : "Failed to load revision history"
    : null;

  const controlsDisabled = registry.selectionDisabled || deployingVersion !== null;
  const autoDeployEnabled = registry.autoDeploy?.enabled ?? false;
  const manualDeployDisabled = controlsDisabled || autoDeployEnabled;

  useEffect(() => {
    setActiveTab("snapshots");
  }, [registry.app]);

  return (
    <div className="space-y-8">
      <PageHeader>
        <PageHeaderContent size="entity">
          <Eyebrow tone="brand">App management</Eyebrow>
          <PageHeaderTitle>{registry.app}</PageHeaderTitle>
          <PageHeaderDescription>
            Registry: {registry.registry}
            {registry.desiredVersion ? (
              <>
                {" "}
                · Desired version:{" "}
                <RegistryCode>{registry.desiredVersion}</RegistryCode>
              </>
            ) : null}
          </PageHeaderDescription>
          {appMountedPath ? (
            <a
              href={appMountedPath}
              className="mt-2 inline-flex text-sm font-medium text-primary transition-colors hover:text-primary"
              data-testid="open-app-link"
            >
              Open app →
            </a>
          ) : null}
        </PageHeaderContent>
        <PageHeaderActions>
          <RolloutBadge app={registry} />
        </PageHeaderActions>
      </PageHeader>

      <RolloutPhaseStepper rollout={registry.rollout} />

      <AppAdminAutoDeployToggle
        autoDeploy={registry.autoDeploy ?? { enabled: false }}
        disabled={isUpdatingAutoDeploy}
        updating={isUpdatingAutoDeploy}
        updateError={autoDeployError}
        onChange={onAutoDeployChange}
      />

      <div className="flex gap-2 border-b border-border">
        <button
          type="button"
          className={`-mb-px border-b-2 px-3 py-2 text-sm font-medium transition-colors ${
            activeTab === "snapshots"
              ? "border-primary text-foreground"
              : "border-transparent text-muted-foreground hover:text-foreground"
          }`}
          onClick={() => setActiveTab("snapshots")}
          data-testid="app-admin-tab-snapshots"
        >
          Published snapshots
        </button>
        <button
          type="button"
          className={`-mb-px border-b-2 px-3 py-2 text-sm font-medium transition-colors ${
            activeTab === "history"
              ? "border-primary text-foreground"
              : "border-transparent text-muted-foreground hover:text-foreground"
          }`}
          onClick={() => setActiveTab("history")}
          data-testid="app-admin-tab-history"
        >
          Revision history
        </button>
      </div>

      {activeTab === "snapshots" ? (
        <section className="space-y-4 rounded-2xl border border-border bg-card p-6 text-card-foreground">
          <SectionHeader>
            <SectionHeaderContent>
              <SectionHeaderTitle>Published snapshots</SectionHeaderTitle>
              <SectionHeaderDescription>
                {autoDeployEnabled
                  ? "Automatic deploy is on — new snapshots are admitted without manual deploy."
                  : registry.desiredVersion
                    ? "Deploy any published snapshot across the fleet."
                    : "No version is installed yet. Deploy a published snapshot to install this app across the fleet."}
              </SectionHeaderDescription>
            </SectionHeaderContent>
            <SectionHeaderActions>
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={onCheckForNewVersions}
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
            </SectionHeaderActions>
          </SectionHeader>

          <AppAdminSnapshotsTable
            registry={registry}
            controlsDisabled={manualDeployDisabled}
            deployingVersion={deployingVersion}
            onDeployVersion={onDeployVersion}
          />

          {registry.selectionDisabled && registry.disabledReason ? (
            <p className="text-sm text-muted-foreground" data-testid="selection-disabled-reason">
              {registry.disabledReason}
            </p>
          ) : null}

          {error ? <p className="text-sm text-destructive">{error}</p> : null}
        </section>
      ) : (
        <section className="space-y-4 rounded-2xl border border-border bg-card p-6 text-card-foreground">
          <SectionHeader>
            <SectionHeaderContent>
              <SectionHeaderTitle>Revision history</SectionHeaderTitle>
              <SectionHeaderDescription>
                Accepted fleet version changes in reverse chronological order.
              </SectionHeaderDescription>
            </SectionHeaderContent>
          </SectionHeader>

          <AppAdminHistoryTable
            revisions={historyRevisions}
            loading={historyQuery.isPending}
            loadingMore={historyQuery.isFetchingNextPage}
            error={historyError}
            hasMore={historyQuery.hasNextPage}
            onLoadMore={() => void historyQuery.fetchNextPage()}
          />
        </section>
      )}
    </div>
  );
}
