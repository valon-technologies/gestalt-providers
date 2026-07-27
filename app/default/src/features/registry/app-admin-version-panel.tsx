import { useEffect, useState } from "react";
import {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderDescription,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import { AppAdminHistoryTable } from "@/features/registry/app-admin-history-table";
import { AppAdminSnapshotsTable } from "@/features/registry/app-admin-snapshots-table";
import { isActiveRegistryRollout, formatRolloutStateLabel } from "@/features/registry/format";
import { RegistryCode } from "@/features/registry/registry-code";
import { RolloutBadge } from "@/features/registry/rollout-badge";
import type { AppAdminRegistryResponse } from "@/features/registry/types";
import { useAppAdminRegistryHistoryQuery } from "@/lib/queries";

type AppAdminTab = "snapshots" | "history";

export function AppAdminVersionPanel({
  registry,
  appMountedPath,
  deployingVersion,
  onDeployVersion,
  error,
}: {
  registry: AppAdminRegistryResponse;
  appMountedPath?: string;
  deployingVersion: string | null;
  onDeployVersion: (version: string) => void;
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

  const rolloutActive = registry.rollout
    ? isActiveRegistryRollout(registry.rollout.state)
    : false;
  const controlsDisabled = registry.selectionDisabled || deployingVersion !== null;

  useEffect(() => {
    setActiveTab("snapshots");
  }, [registry.app]);

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0 space-y-1">
          <h1 className="text-2xl font-heading text-foreground">{registry.app}</h1>
          <p className="text-sm text-muted-foreground">Registry: {registry.registry}</p>
          <p className="text-sm text-muted-foreground/70">App management</p>
          {registry.desiredVersion ? (
            <p className="text-sm text-muted-foreground">
              Desired version: <RegistryCode>{registry.desiredVersion}</RegistryCode>
            </p>
          ) : null}
          {appMountedPath ? (
            <a
              href={appMountedPath}
              className="inline-flex text-sm font-medium text-primary transition-colors hover:text-primary"
              data-testid="open-app-link"
            >
              Open app →
            </a>
          ) : null}
        </div>
        <RolloutBadge app={registry} />
      </div>

      {rolloutActive && registry.rollout ? (
        <p
          className="rounded-lg border border-info-foreground/40 bg-info px-4 py-3 text-sm text-info-foreground"
          data-testid="rollout-active-banner"
        >
          Rollout {formatRolloutStateLabel(registry.rollout.state)}:{" "}
          <RegistryCode>{registry.rollout.version}</RegistryCode>
        </p>
      ) : null}

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
