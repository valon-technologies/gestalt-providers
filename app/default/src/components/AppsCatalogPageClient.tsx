import { useDeferredValue, useEffect, useMemo, useState } from "react";
import type { Integration } from "@/lib/api";
import { groupCatalogForBrowse } from "@/lib/catalogBuckets";
import {
  countNeedsAttention,
  filterCatalogIntegrations,
  type ConnectionFilter,
} from "@/lib/catalogFilters";
import { CONNECTION_RETURN_PATH_STORAGE_KEY } from "@/lib/constants";
import Container from "@/components/Container";
import IntegrationCard from "@/components/IntegrationCard";
import PluginSearchBar from "@/components/PluginSearchBar";
import { Eyebrow } from "@/components/ui/eyebrow";
import {
  PageHeader,
  PageHeaderActions,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import { SegmentedControl } from "@/components/ui/segmented-control";
import { SpinnerIcon } from "@/components/icons";
import Button from "@/components/Button";
import {
  useIntegrationsQuery,
  useInvalidateIntegrations,
} from "@/hooks/use-server-queries";

const APPS_PATH = "/apps";
const LEGACY_INTEGRATIONS_PATH = "/integrations";

const CONNECTION_FILTERS: Array<{ value: ConnectionFilter; label: string }> = [
  { value: "all", label: "All" },
  { value: "needs_connection", label: "To connect" },
  { value: "ready", label: "Ready" },
];

export default function AppsCatalogPageClient() {
  const integrationsQuery = useIntegrationsQuery();
  const invalidateIntegrations = useInvalidateIntegrations();

  const integrations: Integration[] = integrationsQuery.data ?? [];
  // Full-page loading only on cold cache — revisits render immediately.
  const loading = integrationsQuery.isPending;
  const error =
    integrationsQuery.error instanceof Error
      ? integrationsQuery.error.message
      : integrationsQuery.error
        ? "Couldn't load apps. Refresh the page and try again."
        : null;

  const [query, setQuery] = useState("");
  const [connectionFilter, setConnectionFilter] =
    useState<ConnectionFilter>("all");
  const [toast, setToast] = useState<string | null>(() => {
    if (typeof window === "undefined") {
      return null;
    }
    const connected = new URLSearchParams(window.location.search).get(
      "connected",
    );
    return connected ? `${connected} connected successfully.` : null;
  });
  const deferredQuery = useDeferredValue(query);
  const filteredIntegrations = filterCatalogIntegrations(integrations, {
    query: deferredQuery,
    connection: connectionFilter,
    surface: "all",
  });
  const { installed, sections: catalogSections } = useMemo(
    () => groupCatalogForBrowse(filteredIntegrations),
    [filteredIntegrations],
  );
  const needsAttentionCount = countNeedsAttention(integrations);
  const hasSearchQuery = query.trim().length > 0;
  const hasActiveFilters = connectionFilter !== "all" || hasSearchQuery;
  const hasCatalogContent = installed.length > 0 || catalogSections.length > 0;

  useEffect(() => {
    if (window.location.pathname !== LEGACY_INTEGRATIONS_PATH) {
      return;
    }
    window.history.replaceState(
      null,
      "",
      `${APPS_PATH}${window.location.search}${window.location.hash}`,
    );
  }, []);

  useEffect(() => {
    if (!toast) {
      window.sessionStorage.removeItem(CONNECTION_RETURN_PATH_STORAGE_KEY);
      return;
    }

    const returnPath = window.sessionStorage.getItem(
      CONNECTION_RETURN_PATH_STORAGE_KEY,
    );
    window.sessionStorage.removeItem(CONNECTION_RETURN_PATH_STORAGE_KEY);
    if (returnPath) {
      const nextURL = new URL(returnPath, window.location.origin);
      if (
        nextURL.origin === window.location.origin &&
        nextURL.pathname.startsWith("/")
      ) {
        window.location.replace(
          `${nextURL.pathname}${nextURL.search}${nextURL.hash}`,
        );
        return;
      }
    }

    window.history.replaceState(null, "", APPS_PATH);
  }, [toast]);

  async function refreshIntegrations(options?: { background?: boolean }) {
    try {
      await invalidateIntegrations();
    } catch {
      if (options?.background) {
        setToast("Couldn't refresh apps. Try again.");
      }
    }
  }

  function clearFilters() {
    setQuery("");
    setConnectionFilter("all");
  }

  return (
    <Container as="main" className="py-12">
      {toast && (
        <div className="mb-8 flex items-center justify-between rounded-lg border border-grove-200 bg-grove-50 px-5 py-3.5 text-sm text-grove-700 dark:border-grove-600 dark:bg-grove-700/20 dark:text-grove-200">
          <span>{toast}</span>
          <button
            onClick={() => setToast(null)}
            className="ml-4 text-grove-400 hover:text-grove-600 dark:text-grove-500 dark:hover:text-grove-200 transition-colors duration-150"
            aria-label="Dismiss notification"
          >
            &times;
          </button>
        </div>
      )}

      <PageHeader>
        <PageHeaderContent>
          <div className="flex flex-col gap-3">
            <Eyebrow>Catalog</Eyebrow>
            <PageHeaderTitle size="lg">Apps</PageHeaderTitle>
          </div>
          <PageHeaderDescription>
            Browse installed apps, then discover more by category. Connect
            credentials, then open an app to manage access.
          </PageHeaderDescription>
        </PageHeaderContent>
        <PageHeaderActions className="w-full max-w-md sm:w-auto">
          <div className="flex w-full flex-col gap-3 sm:items-end">
            <PluginSearchBar
              query={query}
              onQueryChange={setQuery}
              disabled={loading || !!error || integrations.length === 0}
            />
            <SegmentedControl
              value={connectionFilter}
              onValueChange={(value) =>
                setConnectionFilter(value as ConnectionFilter)
              }
              options={CONNECTION_FILTERS}
              label="Filter by connection status"
              size="sm"
              className="w-full sm:w-auto"
            />
          </div>
        </PageHeaderActions>
      </PageHeader>

      {!loading && !error && needsAttentionCount > 0 ? (
        <div
          className="mt-6 rounded-lg border border-amber-200 bg-amber-100 px-5 py-3.5 text-sm text-amber-700 dark:border-amber-600/40 dark:bg-amber-700/20 dark:text-amber-200"
          role="status"
          data-testid="apps-needs-attention-callout"
        >
          {needsAttentionCount === 1
            ? "1 app needs attention — it’s listed first below. Open it to reconnect or finish setup."
            : `${needsAttentionCount} apps need attention — they’re listed first below. Open one to reconnect or finish setup.`}
        </div>
      ) : null}

      {loading && (
        <p className="mt-10 flex items-center gap-1.5 text-sm text-faint">
          <SpinnerIcon className="size-4 animate-spin" aria-hidden />
          Loading...
        </p>
      )}

      {error && (
        <div className="mt-10 flex flex-col items-start gap-3">
          <p className="text-sm text-ember-500">
            {error === "Failed to load"
              ? "Couldn't load apps. Refresh the page and try again."
              : error}
          </p>
          <Button
            type="button"
            variant="secondary"
            onClick={() => {
              void integrationsQuery.refetch();
            }}
          >
            Retry
          </Button>
        </div>
      )}

      {!loading && !error && integrations.length === 0 && (
        <p className="mt-10 text-sm text-faint">
          No apps are available yet. Ask your admin if you expected to see ones
          here.
        </p>
      )}

      {!loading &&
        !error &&
        integrations.length > 0 &&
        !hasCatalogContent && (
          <div className="mt-10 flex flex-col items-start gap-3">
            <p className="text-sm text-faint">
              {hasSearchQuery && connectionFilter === "all" ? (
                <>
                  No apps match <span>{`"${query.trim()}"`}</span>. Try a
                  different search, or clear it.
                </>
              ) : hasActiveFilters ? (
                hasSearchQuery ? (
                  <>
                    No apps match <span>{`"${query.trim()}"`}</span> with this
                    filter. Try All, or clear search.
                  </>
                ) : (
                  "No apps match this filter. Try All, or clear search."
                )
              ) : (
                "No apps are available yet. Ask your admin if you expected to see ones here."
              )}
            </p>
            {hasActiveFilters ? (
              <Button type="button" variant="secondary" onClick={clearFilters}>
                Clear filters
              </Button>
            ) : null}
          </div>
        )}

      {!loading && !error && hasCatalogContent && (
        <div className="mt-10 space-y-12" data-testid="plugin-grid">
          {installed.length > 0 ? (
            <section
              aria-labelledby="catalog-bucket-installed"
              data-testid="catalog-bucket-installed"
            >
              <div className="mb-4 max-w-2xl">
                <h2
                  id="catalog-bucket-installed"
                  className="font-heading text-xl text-foreground"
                >
                  Installed
                </h2>
                <p className="mt-1 text-sm text-muted-foreground">
                  Apps you’re already connected to — open one to manage access.
                </p>
              </div>
              <div className="grid grid-cols-1 gap-6 sm:grid-cols-2">
                {installed.map((integration) => (
                  <IntegrationCard
                    key={integration.name}
                    integration={integration}
                    highlightQuery={deferredQuery}
                    onConnected={() =>
                      void refreshIntegrations({ background: true })
                    }
                    onDisconnected={() =>
                      void refreshIntegrations({ background: true })
                    }
                    onStatusMessage={setToast}
                    returnPath={APPS_PATH}
                  />
                ))}
              </div>
            </section>
          ) : null}

          {catalogSections.map(({ bucket, integrations: sectionApps }) => (
            <section
              key={bucket.id}
              aria-labelledby={`catalog-bucket-${bucket.id}`}
              data-testid={`catalog-bucket-${bucket.id}`}
            >
              <div className="mb-4 max-w-2xl">
                <h2
                  id={`catalog-bucket-${bucket.id}`}
                  className="font-heading text-xl text-foreground"
                >
                  {bucket.label}
                </h2>
                {bucket.description ? (
                  <p className="mt-1 text-sm text-muted-foreground">
                    {bucket.description}
                  </p>
                ) : null}
              </div>
              <div className="grid grid-cols-1 gap-6 sm:grid-cols-2">
                {sectionApps.map((integration) => (
                  <IntegrationCard
                    key={integration.name}
                    integration={integration}
                    highlightQuery={deferredQuery}
                    onConnected={() =>
                      void refreshIntegrations({ background: true })
                    }
                    onDisconnected={() =>
                      void refreshIntegrations({ background: true })
                    }
                    onStatusMessage={setToast}
                    returnPath={APPS_PATH}
                  />
                ))}
              </div>
            </section>
          ))}
        </div>
      )}
    </Container>
  );
}
