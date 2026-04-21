"use client";

import { useDeferredValue, useEffect, useState } from "react";
import { getIntegrations, Integration } from "@/lib/api";
import { filterIntegrations } from "@/lib/integrationSearch";
import Nav from "@/components/Nav";
import IntegrationCard from "@/components/IntegrationCard";
import PluginSearchBar from "@/components/PluginSearchBar";
import AuthGuard from "@/components/AuthGuard";

export default function IntegrationsPage() {
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
    if (toast) {
      window.history.replaceState(null, "", "/integrations");
    }
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
    <AuthGuard>
      <div className="min-h-screen">
        <Nav />
        <main className="mx-auto max-w-5xl px-6 py-12">
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
              <h1 className="mt-2 text-2xl font-heading font-bold text-primary">
                Plugins
              </h1>
              <p className="mt-2 text-sm text-muted">
                Browse and connect plugins.
              </p>
            </div>
            <div className="w-full md:w-auto">
              <PluginSearchBar
                integrations={integrations}
                query={query}
                onQueryChange={setQuery}
                disabled={loading || !!error || integrations.length === 0}
              />
            </div>
          </div>

          {loading && (
            <p className="mt-10 text-sm text-faint">Loading...</p>
          )}

          {error && <p className="mt-10 text-sm text-ember-500">{error}</p>}

          {!loading && !error && integrations.length === 0 && (
            <p className="mt-10 text-sm text-faint">
              No plugins registered.
            </p>
          )}

          {!loading && !error && integrations.length > 0 && filteredIntegrations.length === 0 && hasSearchQuery && (
            <p className="mt-10 text-sm text-faint">
              No plugins match <span>{`"${query.trim()}"`}</span>.
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
                  onConnected={loadIntegrations}
                  onDisconnected={loadIntegrations}
                />
              ))}
            </div>
          )}
        </main>
      </div>
    </AuthGuard>
  );
}
