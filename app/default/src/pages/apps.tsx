import { useDeferredValue, useEffect, useState } from "react";
import { CONNECTION_RETURN_PATH_STORAGE_KEY } from "@/lib/constants";
import { filterIntegrations } from "@/lib/integrationSearch";
import { appPath } from "@/lib/mount";
import { useIntegrationsQuery, useInvalidateIntegrations } from "@/lib/queries";
import Container from "@/components/Container";
import IntegrationCard from "@/components/IntegrationCard";
import PluginSearchBar from "@/components/PluginSearchBar";
import { SpinnerIcon } from "@/components/icons";

const APPS_PATH = "/apps";
const LEGACY_INTEGRATIONS_PATH = "/integrations";

export default function AppsPage() {
  const integrationsQuery = useIntegrationsQuery();
  const invalidateIntegrations = useInvalidateIntegrations();
  const integrations = integrationsQuery.data ?? [];
  const loading = integrationsQuery.isPending;
  const error = integrationsQuery.error
    ? integrationsQuery.error instanceof Error
      ? integrationsQuery.error.message
      : "Failed to load"
    : null;
  const [query, setQuery] = useState("");
  const [toast, setToast] = useState<string | null>(() => {
    const connected = new URLSearchParams(window.location.search).get("connected");
    return connected ? `${connected} connected successfully.` : null;
  });
  const deferredQuery = useDeferredValue(query);
  const filteredIntegrations = filterIntegrations(integrations, deferredQuery);
  const hasSearchQuery = query.trim().length > 0;

  useEffect(() => {
    if (window.location.pathname !== appPath(LEGACY_INTEGRATIONS_PATH)) {
      return;
    }
    window.history.replaceState(
      null,
      "",
      `${appPath(APPS_PATH)}${window.location.search}${window.location.hash}`,
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

    window.history.replaceState(null, "", appPath(APPS_PATH));
  }, [toast]);

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

          <div className="animate-fade-in-up flex flex-col gap-6 md:flex-row md:items-start md:justify-between">
            <div>
              <span className="label-text">Catalog</span>
              <h1 className="mt-2 text-2xl font-heading text-primary">
                Apps
              </h1>
              <p className="mt-2 text-sm text-muted-foreground">
                Browse and connect apps.
              </p>
            </div>
            <div className="w-full md:w-auto">
              <PluginSearchBar
                query={query}
                onQueryChange={setQuery}
                disabled={loading || !!error || integrations.length === 0}
              />
            </div>
          </div>

          {loading && (
            <p className="mt-10 flex items-center gap-1.5 text-sm text-muted-foreground-soft">
              <SpinnerIcon className="size-4 animate-spin" aria-hidden />
              Loading...
            </p>
          )}

          {error && <p className="mt-10 text-sm text-ember-500">{error}</p>}

          {!loading && !error && integrations.length === 0 && (
            <p className="mt-10 text-sm text-muted-foreground-soft">
              No apps registered.
            </p>
          )}

          {!loading && !error && integrations.length > 0 && filteredIntegrations.length === 0 && hasSearchQuery && (
            <p className="mt-10 text-sm text-muted-foreground-soft">
              No apps match <span>{`"${query.trim()}"`}</span>.
            </p>
          )}

          {!loading && !error && filteredIntegrations.length > 0 && (
            <div
              className="mt-10 grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3 animate-fade-in-up [animation-delay:60ms]"
              data-testid="plugin-grid"
            >
              {filteredIntegrations.map((integration) => (
                <IntegrationCard
                  key={integration.name}
                  integration={integration}
                  onConnected={invalidateIntegrations}
                  onDisconnected={invalidateIntegrations}
                  returnPath={appPath(APPS_PATH)}
                />
              ))}
            </div>
          )}
    </Container>
  );
}
