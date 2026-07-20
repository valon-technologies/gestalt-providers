
import { useDeferredValue, useEffect, useState } from "react";
import { getIntegrations, Integration } from "@/lib/api";
import { CONNECTION_RETURN_PATH_STORAGE_KEY } from "@/lib/constants";
import { filterIntegrations } from "@/lib/integrationSearch";
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
import { SpinnerIcon } from "@/components/icons";

const APPS_PATH = "/apps";
const LEGACY_INTEGRATIONS_PATH = "/integrations";

export default function AppsCatalogPageClient() {
  const [integrations, setIntegrations] = useState<Integration[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [toast, setToast] = useState<string | null>(() => {
    if (typeof window === "undefined") {
      return null;
    }
    const connected = new URLSearchParams(window.location.search).get("connected");
    return connected ? `${connected} connected successfully.` : null;
  });
  const deferredQuery = useDeferredValue(query);
  const filteredIntegrations = filterIntegrations(integrations, deferredQuery);
  const hasSearchQuery = query.trim().length > 0;

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
      if (nextURL.origin === window.location.origin && nextURL.pathname.startsWith("/")) {
        window.location.replace(`${nextURL.pathname}${nextURL.search}${nextURL.hash}`);
        return;
      }
    }

    window.history.replaceState(null, "", APPS_PATH);
  }, [toast]);

  function loadIntegrations() {
    getIntegrations()
      .then(setIntegrations)
      .catch((err) => {
        setError(err instanceof Error ? err.message : "Failed to load");
      })
      .finally(() => setLoading(false));
  }

  useEffect(() => { loadIntegrations(); }, []);

  return (
    <Container as="main" className="py-12">
      {toast && (
        <div className="mb-8 flex items-center justify-between rounded-lg border border-grove-200 bg-grove-50 px-5 py-3.5 text-sm text-grove-700 dark:border-grove-600 dark:bg-grove-700/20 dark:text-grove-200">
          <span>{toast}</span>
          <button
            onClick={() => setToast(null)}
            className="ml-4 text-grove-400 hover:text-grove-600 dark:text-grove-500 dark:hover:text-grove-200 transition-colors duration-150"
            aria-label="Dismiss"
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
            Browse apps and open an app to manage connection, access, and
            workflows.
          </PageHeaderDescription>
        </PageHeaderContent>
        <PageHeaderActions className="w-full sm:w-auto">
          <PluginSearchBar
            integrations={integrations}
            query={query}
            onQueryChange={setQuery}
            disabled={loading || !!error || integrations.length === 0}
          />
        </PageHeaderActions>
      </PageHeader>

      {loading && (
        <p className="mt-10 flex items-center gap-1.5 text-sm text-faint">
          <SpinnerIcon className="size-4 animate-spin" aria-hidden />
          Loading...
        </p>
      )}

      {error && <p className="mt-10 text-sm text-ember-500">{error}</p>}

      {!loading && !error && integrations.length === 0 && (
        <p className="mt-10 text-sm text-faint">
          No apps registered.
        </p>
      )}

      {!loading && !error && integrations.length > 0 && filteredIntegrations.length === 0 && hasSearchQuery && (
        <p className="mt-10 text-sm text-faint">
          No apps match <span>{`"${query.trim()}"`}</span>.
        </p>
      )}

      {!loading && !error && filteredIntegrations.length > 0 && (
        <div
          className="mt-10 grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3"
          data-testid="plugin-grid"
        >
          {filteredIntegrations.map((integration) => (
            <IntegrationCard
              key={integration.name}
              integration={integration}
              onConnected={loadIntegrations}
              onDisconnected={loadIntegrations}
              returnPath={APPS_PATH}
            />
          ))}
        </div>
      )}
    </Container>
  );
}
