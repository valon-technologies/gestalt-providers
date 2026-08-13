import { Link, useNavigate, useRouterState } from "@tanstack/react-router";
import {
  useCallback,
  useDeferredValue,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { CircleAlert } from "lucide-react";
import type { Integration } from "@/lib/api";
import { groupCatalogForBrowse } from "@/lib/catalogBuckets";
import {
  catalogFacetChipAriaLabel,
  catalogFacetsToFilterOptions,
  CATALOG_FACETS,
  countCatalogFacets,
  isCatalogFacetId,
  pruneActiveCatalogFacets,
  type CatalogFacetId,
} from "@/lib/catalogFacets";
import {
  filterCatalogIntegrations,
  listNeedsAttention,
} from "@/lib/catalogFilters";
import { getIntegrationLabel } from "@/lib/integrationSearch";
import { normalizeIntegrationStatus } from "@/lib/integrationStatus";
import { CONNECTION_RETURN_PATH_STORAGE_KEY } from "@/lib/constants";
import { sanitizeAuthReturnPath } from "@/lib/authReturn";
import { appPath } from "@/lib/mount";
import { CONNECTION_CONNECTED_LABEL } from "@/features/app-workspace/connection-surface-copy";
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
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderDescription,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import {
  NavList,
  NavListGroup,
  NavListItem,
  NavListItemLabel,
} from "@/components/ui/nav-list";
import { PageLayout } from "@/components/ui/page-layout";
import { PageLayoutPaneMobileNav } from "@/components/ui/page-layout-pane-mobile-nav";
import {
  Alert,
  AlertActions,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert";
import { Button as UiButton } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ChipGroup, ChipGroupItem } from "@/components/ui/chip-group";
import { useScrollSpy } from "@/hooks/use-scroll-spy";
import { usePageLayoutAnchorOffsetPx } from "@/lib/page-layout-anchor-offset";
import { pageLayoutContentTopStyle } from "@/lib/page-layout-content-top";
import { CheckCircleIcon, CloseIcon, SpinnerIcon } from "@/components/icons";
import Button from "@/components/Button";
import ErrorNotice from "@/components/ErrorNotice";
import { useBuildSession } from "@/hooks/use-build-session";
import {
  useIntegrationsQuery,
  useInvalidateIntegrations,
  useTokensQuery,
} from "@/lib/queries";
import {
  buildWorkspaceSnapshotFromSession,
  catalogLoadStateFromQuery,
  isActivationDue,
  isBuildComplete,
  readResumeBannerDismissed,
  readSetupSkipped,
  writeResumeBannerDismissed,
} from "@/lib/buildPaths";
import {
  APPS_CATALOG_UNAVAILABLE,
  userFacingError,
} from "@/lib/user-facing-error";

function resolveConnectedAppLabel(
  connectedParam: string,
  integrations: Integration[],
): string {
  const match = integrations.find(
    (integration) =>
      integration.name === connectedParam ||
      getIntegrationLabel(integration) === connectedParam,
  );
  return match ? getIntegrationLabel(match) : connectedParam;
}

function needsAttentionAlertCopy(apps: Integration[]): {
  title: string;
  description?: string;
} {
  if (apps.length === 1) {
    const app = apps[0];
    const label = getIntegrationLabel(app);
    const reason = normalizeIntegrationStatus(app, "current_user").summaryLabel;
    return { title: `${label} needs attention: ${reason}` };
  }

  return {
    title: `${apps.length} apps need attention`,
    description: apps
      .map((app) => {
        const label = getIntegrationLabel(app);
        const reason = normalizeIntegrationStatus(app, "current_user").summaryLabel;
        return `${label}: ${reason}`;
      })
      .join(" · "),
  };
}

const APPS_PATH = appPath("/apps");

function CatalogBucketSectionHeader({
  id,
  title,
  description,
}: {
  id: string;
  title: string;
  description?: string;
}) {
  return (
    <SectionHeader>
      <SectionHeaderContent size="lg">
        <SectionHeaderTitle
          id={id}
          className="scroll-mt-[var(--page-layout-anchor-offset)]"
        >
          {title}
        </SectionHeaderTitle>
        {description ? (
          <SectionHeaderDescription>{description}</SectionHeaderDescription>
        ) : null}
      </SectionHeaderContent>
    </SectionHeader>
  );
}

export default function AppsCatalogPageClient() {
  const navigate = useNavigate();
  const session = useBuildSession();
  const locationSearch = useRouterState({
    select: (state) => state.location.search,
  });
  const connectedParam = new URLSearchParams(locationSearch).get("connected");
  const integrationsQuery = useIntegrationsQuery();
  const tokensQuery = useTokensQuery();
  const invalidateIntegrations = useInvalidateIntegrations();
  const activationCheckedRef = useRef(false);
  const [resumeBannerDismissed, setResumeBannerDismissed] = useState(
    readResumeBannerDismissed,
  );

  const integrations: Integration[] = integrationsQuery.data ?? [];
  const tokens = tokensQuery.data ?? [];
  const tokensReady = !tokensQuery.isPending;
  const integrationsReady = !integrationsQuery.isPending;
  // Full-page loading only on cold cache — revisits render immediately.
  const loading = integrationsQuery.isPending;
  const error = integrationsQuery.error
    ? userFacingError(integrationsQuery.error, APPS_CATALOG_UNAVAILABLE)
    : null;

  const [query, setQuery] = useState("");
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [activeFacets, setActiveFacets] = useState<CatalogFacetId[]>([]);
  const [connectedNotice, setConnectedNotice] = useState<string | null>(() =>
    connectedParam,
  );
  const [flashError, setFlashError] = useState<string | null>(null);
  const deferredQuery = useDeferredValue(query);
  // Facet counts / visibility own the search universe, not the full catalog —
  // chips must not overstate apps that search already excluded. Other facets
  // stay independent (AND axes), so counts ignore active facet selection.
  const searchScopedIntegrations = useMemo(
    () =>
      filterCatalogIntegrations(integrations, {
        query: deferredQuery,
        connection: "all",
        surface: "all",
        admin: false,
      }),
    [integrations, deferredQuery],
  );
  const facetCounts = useMemo(
    () => countCatalogFacets(searchScopedIntegrations),
    [searchScopedIntegrations],
  );
  const visibleFacets = useMemo(
    () => CATALOG_FACETS.filter((facet) => facetCounts[facet.id] > 0),
    [facetCounts],
  );
  useEffect(() => {
    setActiveFacets((prev) => {
      const next = pruneActiveCatalogFacets(prev, facetCounts);
      return next.length === prev.length &&
        next.every((id, index) => id === prev[index])
        ? prev
        : next;
    });
  }, [facetCounts]);
  const facetFilter = catalogFacetsToFilterOptions(activeFacets);
  const filteredIntegrations = filterCatalogIntegrations(integrations, {
    query: deferredQuery,
    ...facetFilter,
  });
  const { installed, sections: catalogSections } = useMemo(
    () => groupCatalogForBrowse(filteredIntegrations),
    [filteredIntegrations],
  );
  const needsAttentionApps = useMemo(
    () => listNeedsAttention(filteredIntegrations),
    [filteredIntegrations],
  );
  const needsAttentionCopy =
    needsAttentionApps.length > 0
      ? needsAttentionAlertCopy(needsAttentionApps)
      : null;
  const connectedSuccessLabel = connectedNotice
    ? resolveConnectedAppLabel(connectedNotice, integrations)
    : null;
  const hasSearchQuery = query.trim().length > 0;
  const hasActiveFacets = activeFacets.length > 0;
  const hasActiveNarrowing = hasSearchQuery || hasActiveFacets;
  const hasCatalogContent = installed.length > 0 || catalogSections.length > 0;

  function onFacetsChange(next: string[]) {
    setActiveFacets(next.filter(isCatalogFacetId));
  }

  function clearFilters() {
    setQuery("");
    setActiveFacets([]);
  }

  const catalogNavSections = useMemo(() => {
    const sections: { id: string; label: string }[] = [];
    if (installed.length > 0) {
      sections.push({
        id: "catalog-bucket-installed",
        label: CONNECTION_CONNECTED_LABEL,
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

  const pageLayoutRef = useRef<HTMLDivElement | null>(null);
  const tocActivationOffset = usePageLayoutAnchorOffsetPx(
    undefined,
    pageLayoutRef,
  );

  const { activeId, activate } = useScrollSpy({
    scrollRootRef,
    getEntries,
    sectionsKey,
    activationOffset: tocActivationOffset,
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
      setMobileNavOpen(false);
    },
    [activate],
  );

  const catalogLoadState = catalogLoadStateFromQuery(integrationsQuery);
  const setupSnapshot = useMemo(
    () =>
      buildWorkspaceSnapshotFromSession(
        {
          activeExemplarId: session.activeExemplarId,
          mcpInstalled: session.mcpInstalled,
          apiToken: session.apiToken,
          apiTokenGrantId: session.apiTokenGrantId,
          tokenName: session.tokenName,
          selectedTokenId: session.selectedTokenId,
          selectedInstallAgent: session.selectedInstallAgent,
          welcomeSeen: session.welcomeSeen,
          trySeen: session.trySeen,
        },
        integrations,
        tokens,
        catalogLoadState,
      ),
    [
      integrations,
      tokens,
      catalogLoadState,
      session.activeExemplarId,
      session.mcpInstalled,
      session.apiToken,
      session.apiTokenGrantId,
      session.tokenName,
      session.selectedTokenId,
      session.selectedInstallAgent,
      session.welcomeSeen,
      session.trySeen,
    ],
  );

  const setupComplete = isBuildComplete(setupSnapshot);
  const setupSkipped = readSetupSkipped();
  const showResumeBanner =
    tokensReady &&
    integrationsReady &&
    !setupComplete &&
    (setupSkipped || session.welcomeSeen) &&
    !resumeBannerDismissed;

  useEffect(() => {
    if (!tokensReady || !integrationsReady || activationCheckedRef.current) {
      return;
    }
    activationCheckedRef.current = true;
    if (
      isActivationDue({
        tokens,
        integrations,
        skipped: setupSkipped,
        complete: setupComplete,
      })
    ) {
      void navigate({
        to: "/setup/$stepId",
        params: { stepId: "welcome" },
        replace: true,
      });
    }
  }, [
    tokensReady,
    integrationsReady,
    tokens,
    integrations,
    setupSkipped,
    setupComplete,
    navigate,
  ]);

  useEffect(() => {
    if (!connectedNotice) {
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
  }, [navigate, connectedNotice]);

  async function refreshIntegrations(options?: { background?: boolean }) {
    try {
      await invalidateIntegrations();
    } catch {
      if (options?.background) {
        setFlashError("Couldn't refresh apps. Try again.");
      }
    }
  }

  const catalogPane =
    catalogNavSections.length > 0 ? (
      <div data-testid="apps-catalog-toc">
        <NavList aria-label="App catalog sections">
        {installed.length > 0 ? (
          <NavListItem
            href="#catalog-bucket-installed"
            active={activeId === "catalog-bucket-installed"}
            current="location"
            onClick={(event) => {
              event.preventDefault();
              onNavSectionSelect("catalog-bucket-installed");
            }}
          >
            <NavListItemLabel>{CONNECTION_CONNECTED_LABEL}</NavListItemLabel>
          </NavListItem>
        ) : null}
        {catalogSections.length > 0 ? (
          <NavListGroup
            label="Categories"
            className={installed.length > 0 ? "mt-2" : undefined}
          >
            {catalogSections.map(({ bucket }) => {
              const id = `catalog-bucket-${bucket.id}`;
              return (
                <NavListItem
                  key={bucket.id}
                  href={`#${id}`}
                  active={activeId === id}
                  current="location"
                  onClick={(event) => {
                    event.preventDefault();
                    onNavSectionSelect(id);
                  }}
                >
                  <NavListItemLabel>{bucket.label}</NavListItemLabel>
                </NavListItem>
              );
            })}
          </NavListGroup>
        ) : null}
        </NavList>
      </div>
    ) : undefined;

  return (
    <Container>
      <div ref={pageLayoutRef} style={pageLayoutContentTopStyle}>
      <PageLayout
        tracks="compact"
        pane={catalogPane}
        paneMobile={
          catalogPane ? (
            <PageLayoutPaneMobileNav
              open={mobileNavOpen}
              onOpenChange={setMobileNavOpen}
              panelLabel="App catalog sections"
            >
              {catalogPane}
            </PageLayoutPaneMobileNav>
          ) : undefined
        }
      >
        <PageHeader className="mb-8">
          <PageHeaderContent size="lg">
            <PageHeaderTitle>Apps</PageHeaderTitle>
            <PageHeaderDescription>
              Browse installed apps by category, or connect a new one. Open a
              web app from its card when one is available.
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
        {!loading && !error && visibleFacets.length > 0 ? (
          <ChipGroup
            type="multiple"
            size="sm"
            value={activeFacets}
            onValueChange={onFacetsChange}
            aria-label="Filter apps"
            className="mb-6"
            data-testid="apps-catalog-facets"
          >
            {visibleFacets.map((facet) => {
              const count = facetCounts[facet.id];
              return (
                <ChipGroupItem
                  key={facet.id}
                  value={facet.id}
                  aria-label={catalogFacetChipAriaLabel(facet.label, count)}
                  data-testid={`apps-catalog-facet-${facet.id}`}
                >
                  {facet.label}
                  <Badge size="sm" variant="secondary" aria-hidden>
                    {count}
                  </Badge>
                </ChipGroupItem>
              );
            })}
          </ChipGroup>
        ) : null}
        {connectedSuccessLabel ||
        needsAttentionCopy ||
        flashError ||
        showResumeBanner ? (
          <div className="mb-6 space-y-3">
            {showResumeBanner ? (
              <Alert variant="info" data-testid="setup-resume-banner">
                <AlertTitle>Finish setup</AlertTitle>
                <AlertDescription>
                  Pick up where you left off and connect your assistant to this
                  workspace.
                </AlertDescription>
                <AlertActions>
                  <UiButton variant="secondary" size="sm" asChild>
                    <Link to="/setup">Resume</Link>
                  </UiButton>
                  <UiButton
                    type="button"
                    variant="ghost"
                    size="icon-xs"
                    aria-label="Dismiss setup reminder"
                    onClick={() => {
                      writeResumeBannerDismissed(true);
                      setResumeBannerDismissed(true);
                    }}
                  >
                    <CloseIcon className="size-4" />
                  </UiButton>
                </AlertActions>
              </Alert>
            ) : null}

            {connectedSuccessLabel ? (
              <Alert
                variant="success"
                layout="banner"
                data-testid="apps-connected-toast"
              >
                <CheckCircleIcon aria-hidden />
                <AlertTitle>
                  {connectedSuccessLabel} connected successfully.
                </AlertTitle>
                <AlertActions>
                  <UiButton
                    type="button"
                    variant="ghost"
                    size="icon-xs"
                    aria-label="Dismiss notification"
                    onClick={() => setConnectedNotice(null)}
                  >
                    <CloseIcon className="size-4" />
                  </UiButton>
                </AlertActions>
              </Alert>
            ) : null}

            {needsAttentionCopy ? (
              <Alert
                variant="warning"
                data-testid="apps-needs-attention-callout"
              >
                <CircleAlert aria-hidden />
                <AlertTitle>{needsAttentionCopy.title}</AlertTitle>
                {needsAttentionCopy.description ? (
                  <AlertDescription>
                    {needsAttentionCopy.description}
                  </AlertDescription>
                ) : null}
              </Alert>
            ) : null}

            {flashError ? (
              <Alert variant="destructive" data-testid="apps-flash-error">
                <CircleAlert aria-hidden />
                <AlertTitle>{flashError}</AlertTitle>
                <AlertActions>
                  <UiButton
                    type="button"
                    variant="ghost"
                    size="icon-xs"
                    aria-label="Dismiss error"
                    onClick={() => setFlashError(null)}
                  >
                    <CloseIcon className="size-4" />
                  </UiButton>
                </AlertActions>
              </Alert>
            ) : null}
          </div>
        ) : null}

        {loading && (
          <p
            className="flex items-center gap-1.5 text-sm text-muted-foreground-soft"
            data-testid="apps-catalog-loading"
          >
            <SpinnerIcon className="size-4 animate-spin" aria-hidden />
            Loading apps…
          </p>
        )}

        {error && (
          <ErrorNotice
            message={error}
            retrying={integrationsQuery.isFetching}
            onRetry={() => {
              void integrationsQuery.refetch();
            }}
          />
        )}

        {!loading && !error && integrations.length === 0 && (
          <p className="text-sm text-muted-foreground-soft">
            No apps are available yet. Ask your admin if you expected to see ones
            here.
          </p>
        )}

        {!loading &&
          !error &&
          integrations.length > 0 &&
          !hasCatalogContent && (
            <div className="flex flex-col items-start gap-3">
              <p className="text-sm text-muted-foreground-soft">
                {hasSearchQuery && hasActiveFacets ? (
                  <>
                    No apps match <span>{`"${query.trim()}"`}</span> with the
                    current filters. Try adjusting search or filters.
                  </>
                ) : hasSearchQuery ? (
                  <>
                    No apps match <span>{`"${query.trim()}"`}</span>. Try a
                    different search, or clear it.
                  </>
                ) : hasActiveFacets ? (
                  "No apps match these filters. Try clearing one or more filters."
                ) : (
                  "No apps are available yet. Ask your admin if you expected to see ones here."
                )}
              </p>
              {hasActiveNarrowing ? (
                <Button
                  type="button"
                  variant="secondary"
                  data-testid="apps-catalog-clear-filters"
                  onClick={clearFilters}
                >
                  {hasSearchQuery && hasActiveFacets
                    ? "Clear search and filters"
                    : hasActiveFacets
                      ? "Clear filters"
                      : "Clear search"}
                </Button>
              ) : null}
            </div>
          )}

        {!loading && !error && hasCatalogContent ? (
          <div className="space-y-12" data-testid="plugin-grid">
            {installed.length > 0 ? (
              <section
                aria-labelledby="catalog-bucket-installed"
                className="flex flex-col gap-4"
                data-testid="catalog-bucket-installed"
              >
                <CatalogBucketSectionHeader
                  id="catalog-bucket-installed"
                  title={CONNECTION_CONNECTED_LABEL}
                  description="Apps you’re already connected to. Use Open app when available, or the card menu to manage the app."
                />
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
                className="flex flex-col gap-4"
                data-testid={`catalog-bucket-${bucket.id}`}
              >
                <CatalogBucketSectionHeader
                  id={`catalog-bucket-${bucket.id}`}
                  title={bucket.label}
                  description={bucket.description}
                />
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
                      returnPath={APPS_PATH}
                    />
                  ))}
                </div>
              </section>
            ))}
          </div>
        ) : null}
      </PageLayout>
      </div>
    </Container>
  );
}
