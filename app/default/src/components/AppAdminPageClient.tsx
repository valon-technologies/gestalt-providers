import { Link as RouterLink, useParams } from "@tanstack/react-router";
import { useEffect, useMemo, useState } from "react";
import {
  APIError,
  getAppAuthorizationMembers,
  getAuthSession,
  getIntegrationOperations,
  getIntegrations,
  getManagedIdentities,
  getManagedIdentityGrants,
  type AppAuthorizationMember,
  type AuthSession,
  type Integration,
  type IntegrationOperation,
  type ManagedIdentity,
  type ManagedIdentityGrant,
} from "@/lib/api";
import { normalizeIntegrationStatus } from "@/lib/integrationStatus";
import { getIntegrationLabel } from "@/lib/integrationSearch";
import AppWorkflowRunsPanel from "@/components/AppWorkflowRunsPanel";
import { Badge } from "@/components/Badge";
import Button from "@/components/Button";
import Container from "@/components/Container";
import IntegrationCard from "@/components/IntegrationCard";
import IntegrationIcon from "@/components/IntegrationIcon";
import { Link } from "@/components/Link";
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
  const appName = decodeURIComponent(rawAppName);
  const [section, setSection] = useState<AppAdminSection>("overview");
  const [integration, setIntegration] = useState<Integration | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
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

  const label = integration ? getIntegrationLabel(integration) : appName;

  useDocumentTitle(label);

  function loadIntegration() {
    setLoading(true);
    setError(null);
    getIntegrations()
      .then((integrations) => {
        const match =
          integrations.find((item) => item.name === appName) ?? null;
        setIntegration(match);
        if (!match) {
          setError(`App “${appName}” was not found in this workspace.`);
        }
      })
      .catch((err) => {
        setError(err instanceof Error ? err.message : "Failed to load app");
        setIntegration(null);
      })
      .finally(() => setLoading(false));
  }

  useEffect(() => {
    loadIntegration();
  }, [appName]);

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
        <Link asChild underlineVariant="hover">
          <RouterLink to="/apps">← Apps</RouterLink>
        </Link>
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
          <p className="mt-3 text-sm text-muted">
            <Link asChild>
              <RouterLink to="/apps">Back to Apps</RouterLink>
            </Link>
          </p>
        </div>
      ) : null}

      {integration ? (
        <>
          <PageHeader>
            <PageHeaderContent>
              <div className="flex items-start gap-4">
                <IntegrationIcon
                  iconSvg={integration.iconSvg}
                  size="lg"
                  className="shrink-0"
                />
                <div className="flex min-w-0 flex-col gap-3">
                  <Eyebrow>App</Eyebrow>
                  <PageHeaderTitle size="lg">{label}</PageHeaderTitle>
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
            <PageHeaderActions className="flex flex-wrap gap-2">
              {status ? (
                <Badge
                  variant={
                    status.connected
                      ? "success"
                      : status.tone === "danger"
                        ? "destructive"
                        : status.tone === "warning"
                          ? "warning"
                          : "muted"
                  }
                >
                  {status.summaryLabel}
                </Badge>
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
            </PageHeaderActions>
          </PageHeader>

          <div className="mt-8">
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
                <div className={SECTION_CARD}>
                  <h2 className="text-lg font-heading text-primary">
                    Connection
                  </h2>
                  <p className="mt-1 text-sm text-muted">
                    Connect or reconnect credentials for this app under your
                    user. Connection instances live here (P1 from the app admin
                    research).
                  </p>
                  <div className="mt-5 max-w-xl">
                    <IntegrationCard
                      integration={integration}
                      onConnected={loadIntegration}
                      onDisconnected={loadIntegration}
                      returnPath={`/apps/${encodeURIComponent(appName)}`}
                      disableNavigation
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
                            <p className="text-sm font-medium text-primary">
                              {connection.label}
                            </p>
                            <p className="mt-0.5 text-xs text-muted">
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
                  <h2 className="text-lg font-heading text-primary">Details</h2>
                  <dl className="mt-4 grid gap-3 sm:grid-cols-2">
                    <div>
                      <dt className="text-xs font-medium text-muted">
                        App name
                      </dt>
                      <dd className="mt-1 font-mono text-sm text-primary">
                        {integration.name}
                      </dd>
                    </div>
                    <div>
                      <dt className="text-xs font-medium text-muted">Status</dt>
                      <dd className="mt-1 text-sm text-primary">
                        {status?.summaryLabel || "—"}
                      </dd>
                    </div>
                    {mountedPath ? (
                      <div className="sm:col-span-2">
                        <dt className="text-xs font-medium text-muted">
                          Mounted path
                        </dt>
                        <dd className="mt-1 font-mono text-sm text-primary">
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
                  <h2 className="text-lg font-heading text-primary">
                    Your access
                  </h2>
                  <p className="mt-1 text-sm text-muted">
                    Connection and credentials for the signed-in user.
                  </p>
                  <dl className="mt-4 grid gap-3 sm:grid-cols-2">
                    <div>
                      <dt className="text-xs font-medium text-muted">User</dt>
                      <dd className="mt-1 text-sm text-primary">
                        {session?.email ||
                          session?.displayName ||
                          session?.subjectId ||
                          "—"}
                      </dd>
                    </div>
                    <div>
                      <dt className="text-xs font-medium text-muted">
                        Connection
                      </dt>
                      <dd className="mt-1 text-sm text-primary">
                        {status?.summaryLabel || "—"}
                      </dd>
                    </div>
                  </dl>
                </div>

                <div className={SECTION_CARD}>
                  <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
                    <div>
                      <h2 className="text-lg font-heading text-primary">
                        Members
                      </h2>
                      <p className="mt-1 text-sm text-muted">
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
                    <p className="mt-5 text-sm text-muted">
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
                            <p className="truncate text-sm font-medium text-primary">
                              {memberLabel(member)}
                            </p>
                            {memberMeta(member) ? (
                              <p className="mt-0.5 font-mono text-xs text-muted">
                                {memberMeta(member)}
                              </p>
                            ) : null}
                            {!member.effective && member.shadowedBy ? (
                              <p className="mt-1 text-xs text-muted">
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
                      <h2 className="text-lg font-heading text-primary">
                        Agent identities
                      </h2>
                      <p className="mt-1 text-sm text-muted">
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
                            <p className="mt-0.5 font-mono text-xs text-muted">
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
                <h2 className="text-lg font-heading text-primary">
                  Operations
                </h2>
                <p className="mt-1 text-sm text-muted">
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
                          <code className="font-mono text-sm text-primary">
                            {operation.id}
                          </code>
                          {operation.readOnly ? (
                            <Badge variant="muted" size="sm">
                              read-only
                            </Badge>
                          ) : null}
                        </div>
                        {operation.title && operation.title !== operation.id ? (
                          <p className="mt-1 text-sm text-primary">
                            {operation.title}
                          </p>
                        ) : null}
                        {operation.description ? (
                          <p className="mt-1 text-sm text-muted">
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
        </>
      ) : null}
    </Container>
  );
}

function SummaryStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-alpha bg-alpha-5 px-3 py-2">
      <p className="text-xs font-medium text-muted">{label}</p>
      <p className="mt-0.5 text-lg font-heading text-primary">{value}</p>
    </div>
  );
}
