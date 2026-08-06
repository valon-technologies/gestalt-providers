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
import { CircleAlert } from "lucide-react";
import type { Integration } from "@/lib/api";
import { groupCatalogForBrowse } from "@/lib/catalogBuckets";
import {
  filterCatalogIntegrations,
  listNeedsAttention,
} from "@/lib/catalogFilters";
import { getIntegrationLabel } from "@/lib/integrationSearch";
import { normalizeIntegrationStatus } from "@/lib/integrationStatus";
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
import {
  Alert,
  AlertActions,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert";
import { Button as UiButton } from "@/components/ui/button";
import { useScrollSpy } from "@/hooks/use-scroll-spy";
import { CheckCircleIcon, CloseIcon, SpinnerIcon } from "@/components/icons";
import Button from "@/components/Button";
import {
  useIntegrationsQuery,
  useInvalidateIntegrations,
} from "@/lib/queries";

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
/** Offset below the viewport top for section scroll-spy + scroll-margin on headings. */
/** Must sit below `scroll-mt-24` (96px) so a clicked heading still counts as
 *  crossed after `scrollIntoView` parks it on the scroll-margin. */
const CATALOG_TOC_ACTIVATION_OFFSET = 112;

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
        <SectionHeaderTitle id={id} className="scroll-mt-24">
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
  const [connectedNotice, setConnectedNotice] = useState<string | null>(() =>
    connectedParam,
  );
  const [flashError, setFlashError] = useState<string | null>(null);
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
            <NavListItemLabel>Installed</NavListItemLabel>
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
    <Container className="pt-12 pb-24">
      <PageLayout
        header={
          <PageHeader>
            <PageHeaderContent size="lg">
              <PageHeaderTitle>Apps</PageHeaderTitle>
              <PageHeaderDescription>
                Browse installed apps, then discover more by category. Connect
                an account, then open an app to manage access.
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
        }
        pane={catalogPane}
      >
        {connectedSuccessLabel ||
        needsAttentionCopy ||
        flashError ? (
          <div className="mb-6 space-y-3">
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
          <p className="flex items-center gap-1.5 text-sm text-muted-foreground-soft">
            <SpinnerIcon className="size-4 animate-spin" aria-hidden />
            Loading...
          </p>
        )}

        {error && (
          <div className="flex flex-col items-start gap-3">
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
                  title="Installed"
                  description="Apps you’re already connected to — open one to manage access."
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
    </Container>
  );
}
