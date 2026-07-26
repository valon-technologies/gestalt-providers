import { useCallback, useEffect, useState } from "react";
import {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderDescription,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import { AppAdminHistoryTable } from "@/features/registry/app-admin-history-table";
import { AppAdminSnapshotsTable } from "@/features/registry/app-admin-snapshots-table";
import { isActiveRegistryRollout } from "@/features/registry/format";
import { RegistryCode } from "@/features/registry/registry-code";
import { RolloutBadge } from "@/features/registry/rollout-badge";
import type {
  AppAdminRegistryResponse,
  AppAdminRegistryRevision,
} from "@/features/registry/types";
import { getAppAdminRegistryHistory } from "@/lib/api";

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
  const [historyRevisions, setHistoryRevisions] = useState<AppAdminRegistryRevision[]>([]);
  const [historyCursor, setHistoryCursor] = useState<string | undefined>();
  const [historyLoaded, setHistoryLoaded] = useState(false);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyLoadingMore, setHistoryLoadingMore] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);

  const rolloutActive = registry.rollout
    ? isActiveRegistryRollout(registry.rollout.state)
    : false;
  const controlsDisabled = registry.selectionDisabled || deployingVersion !== null;

  const loadHistory = useCallback(
    async (cursor?: string) => {
      const loadingMore = Boolean(cursor);
      if (loadingMore) {
        setHistoryLoadingMore(true);
      } else {
        setHistoryLoading(true);
      }
      setHistoryError(null);
      try {
        const response = await getAppAdminRegistryHistory(registry.app, {
          limit: 50,
          cursor,
        });
        setHistoryRevisions((current) =>
          loadingMore ? [...current, ...response.revisions] : response.revisions,
        );
        setHistoryCursor(response.nextCursor);
        setHistoryLoaded(true);
      } catch (err) {
        setHistoryError(err instanceof Error ? err.message : "Failed to load revision history");
      } finally {
        setHistoryLoading(false);
        setHistoryLoadingMore(false);
      }
    },
    [registry.app],
  );

  useEffect(() => {
    setActiveTab("snapshots");
    setHistoryRevisions([]);
    setHistoryCursor(undefined);
    setHistoryLoaded(false);
    setHistoryLoading(false);
    setHistoryLoadingMore(false);
    setHistoryError(null);
  }, [registry.app]);

  useEffect(() => {
    if (activeTab !== "history" || historyLoaded || historyLoading) return;
    void loadHistory();
  }, [activeTab, historyLoaded, historyLoading, loadHistory]);

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0 space-y-1">
          <h1 className="text-2xl font-heading text-primary">{registry.app}</h1>
          <p className="text-sm text-muted">Registry: {registry.registry}</p>
          <p className="text-sm text-faint">App management</p>
          {registry.desiredVersion ? (
            <p className="text-sm text-muted">
              Desired version: <RegistryCode>{registry.desiredVersion}</RegistryCode>
            </p>
          ) : null}
          {appMountedPath ? (
            <a
              href={appMountedPath}
              className="inline-flex text-sm font-medium text-gold-700 transition-colors hover:text-gold-800 dark:text-gold-300 dark:hover:text-gold-200"
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
          className="rounded-lg border border-gold-200 bg-gold-50 px-4 py-3 text-sm text-gold-900 dark:border-gold-800 dark:bg-gold-950/40 dark:text-gold-100"
          data-testid="rollout-active-banner"
        >
          Rollout {registry.rollout.state}:{" "}
          <RegistryCode>{registry.rollout.version}</RegistryCode>
        </p>
      ) : null}

      <div className="flex gap-2 border-b border-alpha">
        <button
          type="button"
          className={`-mb-px border-b-2 px-3 py-2 text-sm font-medium transition-colors ${
            activeTab === "snapshots"
              ? "border-gold-600 text-primary"
              : "border-transparent text-muted hover:text-primary"
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
              ? "border-gold-600 text-primary"
              : "border-transparent text-muted hover:text-primary"
          }`}
          onClick={() => setActiveTab("history")}
          data-testid="app-admin-tab-history"
        >
          Revision history
        </button>
      </div>

      {activeTab === "snapshots" ? (
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
      ) : (
        <section className="space-y-4 rounded-2xl border border-alpha bg-base-white p-6 dark:bg-surface">
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
            loading={historyLoading}
            loadingMore={historyLoadingMore}
            error={historyError}
            hasMore={Boolean(historyCursor)}
            onLoadMore={() => void loadHistory(historyCursor)}
          />
        </section>
      )}
    </div>
  );
}
