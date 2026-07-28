import { useNavigate, useRouterState } from "@tanstack/react-router";
import {
  useCallback,
  useDeferredValue,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type { Integration } from "@/lib/api";
import { groupCatalogForBrowse } from "@/lib/catalogBuckets";
import {
  countNeedsAttention,
  filterCatalogIntegrations,
} from "@/lib/catalogFilters";
import { CONNECTION_RETURN_PATH_STORAGE_KEY } from "@/lib/constants";
import { sanitizeAuthReturnPath } from "@/lib/authReturn";
import { appPath } from "@/lib/mount";
import Container from "@/components/Container";
import IntegrationCard from "@/components/IntegrationCard";
import PluginSearchBar from "@/components/PluginSearchBar";
import {
  PageHeader,
  PageHeaderActions,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
} from "@/components/ui/sidebar";
import { useScrollSpy } from "@/hooks/use-scroll-spy";
import { SpinnerIcon } from "@/components/icons";
import Button from "@/components/Button";
import {
  useIntegrationsQuery,
  useInvalidateIntegrations,
} from "@/lib/queries";

const APPS_PATH = appPath("/apps");
/** Offset below the viewport top for section scroll-spy + scroll-margin on headings. */
/** Must sit below `scroll-mt-24` (96px) so a clicked heading still counts as
 *  crossed after `scrollIntoView` parks it on the scroll-margin. */
const CATALOG_TOC_ACTIVATION_OFFSET = 112;

export default function AppsCatalogPageClient() {
  const navigate = useNavigate();
  const locationSearch = useRouterState({
    select: (state) => state.location.search,
  });
  const connectedParam = new URLSearchParams(locationSearch).get("connected");
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
  const [toast, setToast] = useState<string | null>(() =>
    connectedParam ? `${connectedParam} connected successfully.` : null,
  );
  const deferredQuery = useDeferredValue(query);
  const filteredIntegrations = filterCatalogIntegrations(integrations, {
    query: deferredQuery,
    connection: "all",
    surface: "all",
  });
  const { installed, sections: catalogSections } = useMemo(
    () => groupCatalogForBrowse(filteredIntegrations),
    [filteredIntegrations],
  );
  const needsAttentionCount = countNeedsAttention(filteredIntegrations);
  const hasSearchQuery = query.trim().length > 0;
  const hasCatalogContent = installed.length > 0 || catalogSections.length > 0;

  const catalogNavSections = useMemo(() => {
    const sections: { id: string; label: string }[] = [];
    if (installed.length > 0) {
      sections.push({
        id: "catalog-bucket-installed",
        label: "Installed",
      });
    }
    for (const { bucket } of catalogSections) {
      sections.push({
        id: `catalog-bucket-${bucket.id}`,
        label: bucket.label,
      });
    }
    return sections;
  }, [catalogSections, installed.length]);

  const scrollRootRef = useRef<HTMLElement | null>(null);
  useLayoutEffect(() => {
    scrollRootRef.current = document.documentElement;
  }, []);

  const linkItems = catalogNavSections;
  const sectionsKey = linkItems.map((item) => item.id).join(",");
  const getEntries = useCallback(() => {
    return linkItems.flatMap((item) => {
      const el = document.getElementById(item.id);
      return el
        ? [{ id: item.id, top: el.getBoundingClientRect().top }]
        : [];
    });
  }, [linkItems]);

  const { activeId, activate } = useScrollSpy({
    scrollRootRef,
    getEntries,
    sectionsKey,
    activationOffset: CATALOG_TOC_ACTIVATION_OFFSET,
    forceLastAtBottom: true,
    enabled: hasCatalogContent && linkItems.length > 0,
    observeWindow: true,
  });

  const onNavSectionSelect = useCallback(
    (id: string) => {
      const el = document.getElementById(id);
      if (!el) return;
      activate(id);
      el.scrollIntoView({ behavior: "smooth", block: "start" });
    },
    [activate],
  );

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
      const safePath = sanitizeAuthReturnPath(returnPath);
      const nextURL = new URL(safePath, window.location.origin);
      if (
        nextURL.origin === window.location.origin &&
        nextURL.pathname.startsWith("/")
      ) {
        const search = Object.fromEntries(nextURL.searchParams.entries());
        void navigate({
          to: nextURL.pathname,
          replace: true,
          hash: nextURL.hash || undefined,
          ...(Object.keys(search).length > 0 ? { search } : {}),
        });
        return;
      }
    }

    void navigate({ to: "/apps", replace: true });
  }, [navigate, toast]);

  async function refreshIntegrations(options?: { background?: boolean }) {
    try {
      await invalidateIntegrations();
    } catch {
      if (options?.background) {
        setToast("Couldn't refresh apps. Try again.");
      }
    }
  }

  return (
    <Container as="main" className="pt-12 pb-24">
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

      <PageHeader className="animate-fade-in-up">
        <PageHeaderContent size="lg">
          <PageHeaderTitle>Apps</PageHeaderTitle>
          <PageHeaderDescription>
            Browse installed apps, then discover more by category. Connect
            credentials, then open an app to manage access.
          </PageHeaderDescription>
        </PageHeaderContent>
        <PageHeaderActions className="w-full max-w-md sm:w-auto">
          <PluginSearchBar
            query={query}
            onQueryChange={setQuery}
            disabled={loading || !!error || integrations.length === 0}
          />
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
        <p className="mt-10 flex items-center gap-1.5 text-sm text-muted-foreground-soft">
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
        <p className="mt-10 text-sm text-muted-foreground-soft">
          No apps are available yet. Ask your admin if you expected to see ones
          here.
        </p>
      )}

      {!loading &&
        !error &&
        integrations.length > 0 &&
        !hasCatalogContent && (
          <div className="mt-10 flex flex-col items-start gap-3">
            <p className="text-sm text-muted-foreground-soft">
              {hasSearchQuery ? (
                <>
                  No apps match <span>{`"${query.trim()}"`}</span>. Try a
                  different search, or clear it.
                </>
              ) : (
                "No apps are available yet. Ask your admin if you expected to see ones here."
              )}
            </p>
            {hasSearchQuery ? (
              <Button
                type="button"
                variant="secondary"
                onClick={() => setQuery("")}
              >
                Clear search
              </Button>
            ) : null}
          </div>
        )}

      {!loading && !error && hasCatalogContent && (
        <div className="mt-10 flex gap-8" data-testid="plugin-grid">
          {catalogNavSections.length > 0 ? (
            <aside
              className="hidden w-44 shrink-0 lg:block"
              data-testid="apps-catalog-toc"
            >
              <div className="sticky top-24 h-[calc(100vh-7rem)]">
                <SidebarProvider
                  defaultWidth="11rem"
                  className="h-full min-h-0 w-full"
                >
                  <Sidebar collapsible="none" className="h-full">
                    <SidebarContent className="overflow-visible">
                      {installed.length > 0 ? (
                        <SidebarGroup className="p-0">
                          <SidebarGroupContent>
                            <SidebarMenu>
                              <SidebarMenuItem>
                                <SidebarMenuButton
                                  isActive={
                                    activeId === "catalog-bucket-installed"
                                  }
                                  onClick={() =>
                                    onNavSectionSelect(
                                      "catalog-bucket-installed",
                                    )
                                  }
                                >
                                  Installed
                                </SidebarMenuButton>
                              </SidebarMenuItem>
                            </SidebarMenu>
                          </SidebarGroupContent>
                        </SidebarGroup>
                      ) : null}
                      {catalogSections.length > 0 ? (
                        <SidebarGroup
                          className={
                            installed.length > 0 ? "mt-2 p-0" : "p-0"
                          }
                        >
                          <SidebarGroupLabel>Categories</SidebarGroupLabel>
                          <SidebarGroupContent>
                            <SidebarMenu>
                              {catalogSections.map(({ bucket }) => (
                                <SidebarMenuItem key={bucket.id}>
                                  <SidebarMenuButton
                                    isActive={
                                      activeId ===
                                      `catalog-bucket-${bucket.id}`
                                    }
                                    onClick={() =>
                                      onNavSectionSelect(
                                        `catalog-bucket-${bucket.id}`,
                                      )
                                    }
                                  >
                                    {bucket.label}
                                  </SidebarMenuButton>
                                </SidebarMenuItem>
                              ))}
                            </SidebarMenu>
                          </SidebarGroupContent>
                        </SidebarGroup>
                      ) : null}
                    </SidebarContent>
                  </Sidebar>
                </SidebarProvider>
              </div>
            </aside>
          ) : null}

          <div className="min-w-0 flex-1 space-y-12">
            {installed.length > 0 ? (
              <section
                aria-labelledby="catalog-bucket-installed"
                data-testid="catalog-bucket-installed"
              >
                <div className="mb-4 max-w-2xl">
                  <h2
                    id="catalog-bucket-installed"
                    className="scroll-mt-24 font-heading text-2xl text-foreground"
                  >
                    Installed
                  </h2>
                  <p className="mt-1 text-sm text-muted-foreground">
                    Apps you’re already connected to — open one to manage
                    access.
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
                    className="scroll-mt-24 font-heading text-2xl text-foreground"
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
        </div>
      )}
    </Container>
  );
}
