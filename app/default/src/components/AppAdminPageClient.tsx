import { Link as RouterLink, useNavigate, useParams, useSearch } from "@tanstack/react-router";
import { useEffect, useMemo, useState } from "react";
import {
  APIError,
  getAppAuthorizationMembers,
  getAuthSession,
  getIntegrationOperations,
  getManagedIdentities,
  getManagedIdentityGrants,
  type AppAuthorizationMember,
  type AuthSession,
  type IntegrationOperation,
  type ManagedIdentity,
  type ManagedIdentityGrant,
} from "@/lib/api";
import {
  badgeVariantFromTone,
  getAppSurfaces,
  primaryConnectLabel,
} from "@/lib/catalogFilters";
import { getAppPromptExamples } from "@/lib/appPromptExamples";
import { DOCS_PATH } from "@/lib/constants";
import { normalizeIntegrationStatus, shouldShowIntegrationSettings } from "@/lib/integrationStatus";
import { getIntegrationLabel } from "@/lib/integrationSearch";
import {
  useIntegrationsQuery,
  useInvalidateIntegrations,
} from "@/hooks/use-server-queries";
import AppPromptExamplePromo from "@/components/AppPromptExamplePromo";
import AppWorkflowRunsPanel from "@/components/AppWorkflowRunsPanel";
import { Badge } from "@/components/Badge";
import Button from "@/components/Button";
import Container from "@/components/Container";
import IntegrationCard from "@/components/IntegrationCard";
import IntegrationIcon from "@/components/IntegrationIcon";
import { Link } from "@/components/Link";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import { Eyebrow } from "@/components/ui/eyebrow";
import {
  PageHeader,
  PageHeaderActions,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import { SegmentedControl } from "@/components/ui/segmented-control";
import { SelectionCheck } from "@/components/ui/selection-check";
import { SpinnerIcon } from "@/components/icons";
import { useDocumentTitle } from "@/hooks/use-document-title";

type AppAdminSection = "overview" | "access" | "workflows" | "operations";

type AppAccessGrant = {
  identity: ManagedIdentity;
  grant: ManagedIdentityGrant;
};

const SECTION_OPTIONS: Array<{ value: AppAdminSection; label: string }> = [
  { value: "overview", label: "Overview" },
  { value: "access", label: "Access" },
  { value: "workflows", label: "Workflows" },
  { value: "operations", label: "Operations" },
];

const SECTION_CARD =
  "rounded-lg border border-alpha bg-base-white p-6 dark:bg-surface";

function memberLabel(member: AppAuthorizationMember): string {
  if (member.email?.trim()) return member.email.trim();
  if (member.selectorValue?.trim()) return member.selectorValue.trim();
  if (member.subjectId?.trim()) return member.subjectId.trim();
  return "Unknown";
}

function memberMeta(member: AppAuthorizationMember): string {
  if (member.selectorKind === "subject_id" && member.selectorValue?.trim()) {
    return `subject_id: ${member.selectorValue.trim()}`;
  }
  if (member.subjectId?.trim() && member.email) {
    return member.subjectId.trim();
  }
  return member.selectorKind || "";
}

export default function AppAdminPageClient() {
  const { appName: rawAppName } = useParams({ from: "/apps/$appName" });
  const { section: sectionSearch } = useSearch({ from: "/apps/$appName" });
  const navigate = useNavigate({ from: "/apps/$appName" });
  const appName = decodeURIComponent(rawAppName);
  const section: AppAdminSection = sectionSearch ?? "overview";

  function setSection(next: AppAdminSection) {
    void navigate({
      search: (prev) => ({
        ...prev,
        section: next === "overview" ? undefined : next,
      }),
      replace: true,
    });
  }

  const integrationsQuery = useIntegrationsQuery();
  const invalidateIntegrations = useInvalidateIntegrations();

  const integration =
    integrationsQuery.data?.find((item) => item.name === appName) ?? null;
  const loading = integrationsQuery.isPending;
  const error =
    integrationsQuery.error instanceof Error
      ? integrationsQuery.error.message
      : integrationsQuery.error
        ? "Failed to load app"
        : !loading && integrationsQuery.data && !integration
          ? `App “${appName}” was not found in this workspace.`
          : null;

  const [session, setSession] = useState<AuthSession | null>(null);
  const [accessGrants, setAccessGrants] = useState<AppAccessGrant[]>([]);
  const [accessLoading, setAccessLoading] = useState(true);
  const [accessError, setAccessError] = useState<string | null>(null);
  const [members, setMembers] = useState<AppAuthorizationMember[]>([]);
  const [membersLoading, setMembersLoading] = useState(true);
  const [membersError, setMembersError] = useState<string | null>(null);
  const [membersForbidden, setMembersForbidden] = useState(false);
  const [operations, setOperations] = useState<IntegrationOperation[]>([]);
  const [operationsLoading, setOperationsLoading] = useState(false);
  const [operationsError, setOperationsError] = useState<string | null>(null);
  const [settingsOpen, setSettingsOpen] = useState(false);

  const label = integration ? getIntegrationLabel(integration) : appName;
  const promptExample = getAppPromptExamples(appName, label)[0]!;

  useDocumentTitle(label);

  function loadIntegration() {
    void invalidateIntegrations();
  }

  useEffect(() => {
    let active = true;
    getAuthSession()
      .then((value) => {
        if (active) setSession(value);
      })
      .catch(() => {
        if (active) setSession(null);
      });
    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    let active = true;
    setAccessLoading(true);
    setAccessError(null);

    getManagedIdentities()
      .then(async (identities) => {
        const grants = await Promise.all(
          identities.map(async (identity) => {
            try {
              const identityGrants = await getManagedIdentityGrants(
                identity.subjectId,
              );
              return identityGrants
                .filter((grant) => grant.plugin === appName)
                .map((grant) => ({ identity, grant }));
            } catch {
              return [] as AppAccessGrant[];
            }
          }),
        );
        if (!active) return;
        setAccessGrants(grants.flat());
      })
      .catch((err) => {
        if (!active) return;
        setAccessError(
          err instanceof Error ? err.message : "Failed to load access",
        );
        setAccessGrants([]);
      })
      .finally(() => {
        if (active) setAccessLoading(false);
      });

    return () => {
      active = false;
    };
  }, [appName]);

  useEffect(() => {
    let active = true;
    setMembersLoading(true);
    setMembersError(null);
    setMembersForbidden(false);

    getAppAuthorizationMembers(appName)
      .then((rows) => {
        if (!active) return;
        setMembers(rows);
      })
      .catch((err) => {
        if (!active) return;
        setMembers([]);
        if (err instanceof APIError && (err.status === 403 || err.status === 401)) {
          setMembersForbidden(true);
          setMembersError(null);
          return;
        }
        setMembersError(
          err instanceof Error ? err.message : "Failed to load members",
        );
      })
      .finally(() => {
        if (active) setMembersLoading(false);
      });

    return () => {
      active = false;
    };
  }, [appName]);

  useEffect(() => {
    if (section !== "operations") return;
    let active = true;
    setOperationsLoading(true);
    setOperationsError(null);
    getIntegrationOperations(appName)
      .then((ops) => {
        if (!active) return;
        setOperations(ops);
      })
      .catch((err) => {
        if (!active) return;
        setOperations([]);
        setOperationsError(
          err instanceof Error ? err.message : "Failed to load operations",
        );
      })
      .finally(() => {
        if (active) setOperationsLoading(false);
      });
    return () => {
      active = false;
    };
  }, [appName, section]);

  const status = integration
    ? normalizeIntegrationStatus(integration, "current_user")
    : null;
  const mountedPath = integration?.mountedPath?.trim();
  const surfaces = integration ? getAppSurfaces(integration) : null;
  const connectLabel = integration
    ? primaryConnectLabel(integration, "current_user")
    : null;
  const showManageConnection = Boolean(
    status &&
      !connectLabel &&
      (status.connected || shouldShowIntegrationSettings(status, false)),
  );

  function openConnectionSettings() {
    setSection("overview");
    setSettingsOpen(true);
    requestAnimationFrame(() => {
      document
        .getElementById("app-admin-connection")
        ?.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  }

  const checklist = useMemo(() => {
    if (!status) return [] as Array<{ id: string; label: string; done: boolean }>;
    const items: Array<{ id: string; label: string; done: boolean; skip?: boolean }> = [
      {
        id: "connected",
        label: "Ready",
        done: status.connected && status.tone === "success",
      },
      {
        id: "ui",
        label: "Has an app page",
        done: Boolean(surfaces?.hasUi),
        skip: !surfaces?.hasUi,
      },
      {
        id: "mcp",
        label: "Works with AI clients",
        done: Boolean(surfaces?.hasMcp),
        skip: !surfaces?.hasMcp,
      },
    ];
    return items
      .filter((item) => !item.skip)
      .map(({ id, label, done }) => ({ id, label, done }));
  }, [status, surfaces]);

  useEffect(() => {
    if (section !== "overview") {
      setSettingsOpen(false);
    }
  }, [section]);

  const memberCounts = useMemo(() => {
    const effective = members.filter((row) => row.effective).length;
    const staticCount = members.filter((row) => row.source === "static").length;
    const dynamicCount = members.filter(
      (row) => row.source === "dynamic",
    ).length;
    const shadowed = members.filter(
      (row) => row.source === "dynamic" && !row.effective,
    ).length;
    return { effective, staticCount, dynamicCount, shadowed };
  }, [members]);

  const visibleOperations = useMemo(
    () =>
      operations.filter(
        (op) => op.visible !== false && typeof op.id === "string" && op.id,
      ),
    [operations],
  );

  return (
    <Container as="main" className="py-12">
      <div className="mb-6">
        <Breadcrumb>
          <BreadcrumbList>
            <BreadcrumbItem>
              <BreadcrumbLink asChild>
                <RouterLink to="/apps">Apps</RouterLink>
              </BreadcrumbLink>
            </BreadcrumbItem>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              <BreadcrumbPage>{label}</BreadcrumbPage>
            </BreadcrumbItem>
          </BreadcrumbList>
        </Breadcrumb>
      </div>

      {loading ? (
        <p className="flex items-center gap-1.5 text-sm text-faint">
          <SpinnerIcon className="size-4 animate-spin" aria-hidden />
          Loading app…
        </p>
      ) : null}

      {error && !integration ? (
        <div className={SECTION_CARD}>
          <p className="text-sm text-ember-500">{error}</p>
          <p className="mt-3 text-sm text-muted-foreground">
            <Link asChild>
              <RouterLink to="/apps">Back to Apps</RouterLink>
            </Link>
          </p>
        </div>
      ) : null}

      {integration ? (
        <div className="grid gap-10 xl:grid-cols-[220px_minmax(0,1fr)_240px]">
          <aside className="hidden xl:block">
            <div className="sticky top-24">
              <nav className="space-y-0.5" aria-label="App sections">
                {SECTION_OPTIONS.map((option) => {
                  const isActive = option.value === section;
                  return (
                    <button
                      key={option.value}
                      type="button"
                      onClick={() => setSection(option.value)}
                      className={`block w-full rounded-md px-3 py-2 text-left text-sm transition-colors duration-150 ${
                        isActive
                          ? "bg-alpha-5 font-medium text-foreground"
                          : "text-muted-foreground hover:text-foreground"
                      }`}
                    >
                      {option.label}
                    </button>
                  );
                })}
              </nav>
            </div>
          </aside>

          <div className="min-w-0">
          <PageHeader>
            <PageHeaderContent size="lg">
              <div className="flex items-start gap-4">
                <IntegrationIcon
                  iconSvg={integration.iconSvg}
                  size="lg"
                  className="shrink-0"
                />
                <div className="flex min-w-0 flex-col gap-3">
                  <Eyebrow>App</Eyebrow>
                  <PageHeaderTitle>{label}</PageHeaderTitle>
                  {integration.description ? (
                    <PageHeaderDescription>
                      {integration.description}
                    </PageHeaderDescription>
                  ) : (
                    <PageHeaderDescription>
                      Manage connection, access, workflows, and operations for{" "}
                      <code className="font-mono text-xs">
                        {integration.name}
                      </code>
                      .
                    </PageHeaderDescription>
                  )}
                </div>
              </div>
            </PageHeaderContent>
            <PageHeaderActions className="flex flex-wrap items-center gap-2">
              {status ? (
                <Badge
                  variant={badgeVariantFromTone(status.tone)}
                  aria-label={status.summaryLabel}
                >
                  {status.summaryLabel}
                </Badge>
              ) : null}
              {surfaces?.hasUi ? (
                <Badge variant="secondary" size="sm">
                  App
                </Badge>
              ) : null}
              {surfaces?.hasMcp ? (
                <Badge variant="secondary" size="sm">
                  Works with AI
                </Badge>
              ) : null}
              {connectLabel ? (
                <Button type="button" onClick={openConnectionSettings}>
                  {connectLabel}
                </Button>
              ) : null}
              {showManageConnection ? (
                <Button
                  type="button"
                  variant="secondary"
                  onClick={openConnectionSettings}
                >
                  Manage connection
                </Button>
              ) : null}
              {mountedPath ? (
                <Button
                  type="button"
                  variant="secondary"
                  onClick={() => window.location.assign(mountedPath)}
                >
                  Open app
                </Button>
              ) : null}
              <Button
                type="button"
                variant="secondary"
                onClick={() => {
                  window.location.assign(DOCS_PATH);
                }}
              >
                Docs
              </Button>
            </PageHeaderActions>
          </PageHeader>

          <div className="mt-8 xl:hidden">
            <SegmentedControl
              label="App sections"
              options={SECTION_OPTIONS}
              value={section}
              onValueChange={setSection}
              showLabels
              size="sm"
            />
          </div>

          <div className="mt-8">
            {section === "overview" ? (
              <section className="space-y-6" aria-label="Overview">
                <AppPromptExamplePromo
                  displayName={promptExample.displayName}
                  body={promptExample.body}
                />

                {checklist.length > 0 ? (
                  <div
                    className={SECTION_CARD}
                    data-testid="app-admin-checklist"
                  >
                    <h2 className="text-lg font-heading text-foreground">
                      Setup
                    </h2>
                    <p className="mt-1 text-sm text-muted-foreground">
                      {checklist.filter((item) => item.done).length}/
                      {checklist.length} ready
                    </p>
                    <ul className="mt-4 space-y-2">
                      {checklist.map((item) => (
                        <li
                          key={item.id}
                          className="flex items-center gap-2 text-sm text-foreground"
                        >
                          <SelectionCheck
                            checked={item.done}
                            tone="solid"
                            density="default"
                          />
                          {item.label}
                        </li>
                      ))}
                    </ul>
                  </div>
                ) : null}

                <div
                  className={SECTION_CARD}
                  id="app-admin-connection"
                  data-testid="app-admin-connection"
                >
                  <h2 className="text-lg font-heading text-foreground">
                    Connection
                  </h2>
                  <p className="mt-1 text-sm text-muted-foreground">
                    Connect or reconnect credentials for this app under your
                    user. Disconnect anytime from settings.
                  </p>
                  <p className="mt-3 text-xs text-faint">
                    Connecting grants this workspace permission to use the app
                    with your credentials. Review the provider’s privacy policy
                    before continuing.
                  </p>
                  <div className="mt-5 max-w-xl">
                    <IntegrationCard
                      integration={integration}
                      onConnected={loadIntegration}
                      onDisconnected={loadIntegration}
                      returnPath={`/apps/${encodeURIComponent(appName)}`}
                      disableNavigation
                      settingsOpen={settingsOpen}
                      onSettingsOpenChange={setSettingsOpen}
                    />
                  </div>
                  {status && status.connections.length > 0 ? (
                    <ul className="mt-5 divide-y divide-alpha rounded-lg border border-alpha">
                      {status.connections.map((connection) => (
                        <li
                          key={connection.key}
                          className="flex flex-col gap-1 px-4 py-3 sm:flex-row sm:items-center sm:justify-between"
                        >
                          <div>
                            <p className="text-sm font-medium text-foreground">
                              {connection.label}
                            </p>
                            <p className="mt-0.5 text-xs text-muted-foreground">
                              {connection.ownerLabel}
                              {connection.credentialLabel
                                ? ` · ${connection.credentialLabel}`
                                : ""}
                              {connection.healthLabel
                                ? ` · ${connection.healthLabel}`
                                : ""}
                            </p>
                          </div>
                          <Badge
                            variant={
                              connection.connected ? "success" : "muted"
                            }
                            size="sm"
                          >
                            {connection.statusLabel}
                          </Badge>
                        </li>
                      ))}
                    </ul>
                  ) : null}
                </div>

                <div className={SECTION_CARD}>
                  <h2 className="text-lg font-heading text-foreground">Details</h2>
                  <dl className="mt-4 grid gap-3 sm:grid-cols-2">
                    <div>
                      <dt className="text-xs font-medium text-muted-foreground">
                        App name
                      </dt>
                      <dd className="mt-1 font-mono text-sm text-foreground">
                        {integration.name}
                      </dd>
                    </div>
                    <div>
                      <dt className="text-xs font-medium text-muted-foreground">Status</dt>
                      <dd className="mt-1 text-sm text-foreground">
                        {status?.summaryLabel || "—"}
                      </dd>
                    </div>
                    <div>
                      <dt className="text-xs font-medium text-muted-foreground">
                        Surfaces
                      </dt>
                      <dd className="mt-1 flex flex-wrap gap-1.5">
                        <Badge size="sm" variant="secondary">
                          API
                        </Badge>
                        {surfaces?.hasMcp ? (
                          <Badge size="sm" variant="secondary">
                            Works with AI
                          </Badge>
                        ) : null}
                        {surfaces?.hasUi ? (
                          <Badge size="sm" variant="secondary">
                            App
                          </Badge>
                        ) : null}
                      </dd>
                    </div>
                    {mountedPath ? (
                      <div className="sm:col-span-2">
                        <dt className="text-xs font-medium text-muted-foreground">
                          Mounted path
                        </dt>
                        <dd className="mt-1 font-mono text-sm text-foreground">
                          {mountedPath}
                        </dd>
                      </div>
                    ) : null}
                  </dl>
                </div>
              </section>
            ) : null}

            {section === "access" ? (
              <section className="space-y-6" aria-label="Access">
                <div className={SECTION_CARD}>
                  <h2 className="text-lg font-heading text-foreground">
                    Your access
                  </h2>
                  <p className="mt-1 text-sm text-muted-foreground">
                    Connection and credentials for the signed-in user.
                  </p>
                  <dl className="mt-4 grid gap-3 sm:grid-cols-2">
                    <div>
                      <dt className="text-xs font-medium text-muted-foreground">User</dt>
                      <dd className="mt-1 text-sm text-foreground">
                        {session?.email ||
                          session?.displayName ||
                          session?.subjectId ||
                          "—"}
                      </dd>
                    </div>
                    <div>
                      <dt className="text-xs font-medium text-muted-foreground">
                        Connection
                      </dt>
                      <dd className="mt-1 text-sm text-foreground">
                        {status?.summaryLabel || "—"}
                      </dd>
                    </div>
                  </dl>
                </div>

                <div className={SECTION_CARD}>
                  <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
                    <div>
                      <h2 className="text-lg font-heading text-foreground">
                        Members
                      </h2>
                      <p className="mt-1 text-sm text-muted-foreground">
                        Who has access to this app (static policy + dynamic
                        grants). Same roster as the admin Authorization tab.
                      </p>
                    </div>
                    <Link href="/admin/" underlineVariant="always">
                      Open admin Authorization
                    </Link>
                  </div>

                  {!membersLoading && !membersForbidden && !membersError ? (
                    <div className="mt-5 grid grid-cols-2 gap-3 sm:grid-cols-4">
                      <SummaryStat
                        label="Effective"
                        value={String(memberCounts.effective)}
                      />
                      <SummaryStat
                        label="Static"
                        value={String(memberCounts.staticCount)}
                      />
                      <SummaryStat
                        label="Dynamic"
                        value={String(memberCounts.dynamicCount)}
                      />
                      <SummaryStat
                        label="Shadowed"
                        value={String(memberCounts.shadowed)}
                      />
                    </div>
                  ) : null}

                  {membersLoading ? (
                    <p className="mt-5 flex items-center gap-1.5 text-sm text-faint">
                      <SpinnerIcon
                        className="size-4 animate-spin"
                        aria-hidden
                      />
                      Loading members…
                    </p>
                  ) : null}

                  {membersForbidden ? (
                    <p className="mt-5 text-sm text-muted-foreground">
                      Member roster requires app authorization admin access.
                      Manage members in{" "}
                      <Link href="/admin/" underlineVariant="always">
                        /admin/
                      </Link>{" "}
                      or ask a Gestalt admin.
                    </p>
                  ) : null}

                  {membersError ? (
                    <p className="mt-5 text-sm text-ember-500">{membersError}</p>
                  ) : null}

                  {!membersLoading &&
                  !membersForbidden &&
                  !membersError &&
                  members.length === 0 ? (
                    <p className="mt-5 text-sm text-faint">
                      No members found for this app.
                    </p>
                  ) : null}

                  {!membersLoading && members.length > 0 ? (
                    <ul
                      className="mt-5 divide-y divide-alpha rounded-lg border border-alpha"
                      data-testid="app-members-list"
                    >
                      {members.map((member, index) => (
                        <li
                          key={`${memberLabel(member)}:${member.role}:${index}`}
                          className="flex flex-col gap-2 px-4 py-3 sm:flex-row sm:items-center sm:justify-between"
                        >
                          <div className="min-w-0">
                            <p className="truncate text-sm font-medium text-foreground">
                              {memberLabel(member)}
                            </p>
                            {memberMeta(member) ? (
                              <p className="mt-0.5 font-mono text-xs text-muted-foreground">
                                {memberMeta(member)}
                              </p>
                            ) : null}
                            {!member.effective && member.shadowedBy ? (
                              <p className="mt-1 text-xs text-muted-foreground">
                                Shadowed by {member.shadowedBy}
                              </p>
                            ) : null}
                          </div>
                          <div className="flex flex-wrap gap-1.5">
                            <Badge variant="secondary" size="sm">
                              {member.role || "role"}
                            </Badge>
                            <Badge
                              variant={
                                member.source === "static"
                                  ? "muted"
                                  : "outline"
                              }
                              size="sm"
                            >
                              {member.source || "unknown"}
                              {member.mutable === false ? " · locked" : ""}
                            </Badge>
                            <Badge
                              variant={
                                member.effective ? "success" : "warning"
                              }
                              size="sm"
                            >
                              {member.effective ? "Effective" : "Shadowed"}
                            </Badge>
                          </div>
                        </li>
                      ))}
                    </ul>
                  ) : null}
                </div>

                <div className={SECTION_CARD}>
                  <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
                    <div>
                      <h2 className="text-lg font-heading text-foreground">
                        Agent identities
                      </h2>
                      <p className="mt-1 text-sm text-muted-foreground">
                        Managed identities with an authorization grant for this
                        app — usually the <code className="font-mono text-xs">runAs</code>{" "}
                        subject for schedules.
                      </p>
                    </div>
                    <Link asChild>
                      <RouterLink to="/identities">Manage identities</RouterLink>
                    </Link>
                  </div>

                  {accessLoading ? (
                    <p className="mt-5 flex items-center gap-1.5 text-sm text-faint">
                      <SpinnerIcon
                        className="size-4 animate-spin"
                        aria-hidden
                      />
                      Loading access…
                    </p>
                  ) : null}

                  {accessError ? (
                    <p className="mt-5 text-sm text-ember-500">{accessError}</p>
                  ) : null}

                  {!accessLoading &&
                  !accessError &&
                  accessGrants.length === 0 ? (
                    <p className="mt-5 text-sm text-faint">
                      No agent identities have a grant for this app yet.
                    </p>
                  ) : null}

                  {!accessLoading && accessGrants.length > 0 ? (
                    <ul className="mt-5 divide-y divide-alpha rounded-lg border border-alpha">
                      {accessGrants.map(({ identity, grant }) => (
                        <li
                          key={`${identity.subjectId}:${grant.plugin}`}
                          className="flex flex-col gap-1 px-4 py-3 sm:flex-row sm:items-center sm:justify-between"
                        >
                          <div className="min-w-0">
                            <Link asChild>
                              <RouterLink
                                to="/identities"
                                search={{ id: identity.subjectId }}
                              >
                                {identity.displayName || identity.subjectId}
                              </RouterLink>
                            </Link>
                            <p className="mt-0.5 font-mono text-xs text-muted-foreground">
                              {identity.subjectId}
                            </p>
                          </div>
                          <Badge variant="secondary" size="sm">
                            {grant.role}
                            {grant.source === "static" ? " · static" : ""}
                          </Badge>
                        </li>
                      ))}
                    </ul>
                  ) : null}
                </div>
              </section>
            ) : null}

            {section === "workflows" ? (
              <section className={SECTION_CARD} aria-label="Workflows">
                <AppWorkflowRunsPanel appName={appName} />
              </section>
            ) : null}

            {section === "operations" ? (
              <section className={SECTION_CARD} aria-label="Operations">
                <h2 className="text-lg font-heading text-foreground">
                  Operations
                </h2>
                <p className="mt-1 text-sm text-muted-foreground">
                  What this app can do — the callable operation catalog.
                </p>

                {operationsLoading ? (
                  <p className="mt-5 flex items-center gap-1.5 text-sm text-faint">
                    <SpinnerIcon className="size-4 animate-spin" aria-hidden />
                    Loading operations…
                  </p>
                ) : null}

                {operationsError ? (
                  <p className="mt-5 text-sm text-ember-500">
                    {operationsError}
                  </p>
                ) : null}

                {!operationsLoading &&
                !operationsError &&
                visibleOperations.length === 0 ? (
                  <p className="mt-5 text-sm text-faint">
                    No visible operations for this app.
                  </p>
                ) : null}

                {!operationsLoading && visibleOperations.length > 0 ? (
                  <ul
                    className="mt-5 divide-y divide-alpha rounded-lg border border-alpha"
                    data-testid="app-operations-list"
                  >
                    {visibleOperations.map((operation) => (
                      <li key={operation.id} className="px-4 py-3">
                        <div className="flex flex-wrap items-center gap-2">
                          <code className="font-mono text-sm text-foreground">
                            {operation.id}
                          </code>
                          {operation.readOnly ? (
                            <Badge variant="muted" size="sm">
                              read-only
                            </Badge>
                          ) : null}
                        </div>
                        {operation.title && operation.title !== operation.id ? (
                          <p className="mt-1 text-sm text-foreground">
                            {operation.title}
                          </p>
                        ) : null}
                        {operation.description ? (
                          <p className="mt-1 text-sm text-muted-foreground">
                            {operation.description}
                          </p>
                        ) : null}
                        {operation.tags && operation.tags.length > 0 ? (
                          <p className="mt-2 text-xs text-faint">
                            {operation.tags.join(" · ")}
                          </p>
                        ) : null}
                      </li>
                    ))}
                  </ul>
                ) : null}
              </section>
            ) : null}
          </div>
          </div>

          <aside className="hidden xl:block" aria-hidden />
        </div>
      ) : null}
    </Container>
  );
}

function SummaryStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-alpha bg-alpha-5 px-3 py-2">
      <p className="text-xs font-medium text-muted-foreground">{label}</p>
      <p className="mt-0.5 text-lg font-heading text-foreground">{value}</p>
    </div>
  );
}
