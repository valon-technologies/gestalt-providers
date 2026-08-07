
import {
  Combobox,
  ComboboxButton,
  ComboboxInput,
  ComboboxOption,
  ComboboxOptions,
} from "@headlessui/react";
import { Link } from "@tanstack/react-router";
import { useState } from "react";
import {
  connectManagedIdentityManualIntegration,
  disconnectManagedIdentityIntegration,
  startManagedIdentityIntegrationOAuth,
  type Integration,
  type ManagedIdentityGrant,
} from "@/lib/api";
import { getUserEmail } from "@/lib/auth";
import {
  CONNECTION_RETURN_PATH_STORAGE_KEY,
  INPUT_CLASSES,
} from "@/lib/constants";
import { filterIntegrations, getIntegrationLabel } from "@/lib/integrationSearch";
import {
  SETTINGS_IDENTITIES_PATH,
  settingsIdentityDetailPath,
} from "@/lib/managed-identity-paths";
import { appPath } from "@/lib/mount";
import {
  useDeleteManagedIdentityGrantMutation,
  useDeleteManagedIdentityMemberMutation,
  useDeleteManagedIdentityMutation,
  useIntegrationsQuery,
  useInvalidateManagedIdentity,
  useManagedIdentityGrantsQuery,
  useManagedIdentityIntegrationsQuery,
  useManagedIdentityMembersQuery,
  useManagedIdentityQuery,
  usePutManagedIdentityGrantMutation,
  usePutManagedIdentityMemberMutation,
  useUpdateManagedIdentityMutation,
} from "@/lib/queries";
import Button from "./Button";
import Container from "./Container";
import IntegrationCard from "./IntegrationCard";
import { Eyebrow } from "@/components/ui/eyebrow";
import {
  PageHeader,
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
import { SearchIcon } from "./icons";

const SECTION_CARD =
  "rounded-lg border border-border bg-card p-6 text-card-foreground";

function mergeGrantPluginOptions(
  visibleIntegrations: Integration[],
  grants: ManagedIdentityGrant[],
): Integration[] {
  const byName = new Map<string, Integration>();
  for (const integration of [
    ...visibleIntegrations,
    ...grants.map((grant) => ({ name: grant.plugin })),
  ]) {
    const name = integration.name?.trim();
    if (!name || byName.has(name)) continue;
    byName.set(name, { ...integration, name });
  }
  return [...byName.values()].sort((left, right) => {
    const labelCompare = getIntegrationLabel(left).localeCompare(getIntegrationLabel(right));
    if (labelCompare !== 0) return labelCompare;
    return left.name.localeCompare(right.name);
  });
}

function resolveGrantPluginOption(
  integrations: Integration[],
  selectedPlugin: string,
  pluginQuery: string,
): Integration | null {
  if (selectedPlugin) {
    return integrations.find((integration) => integration.name === selectedPlugin) ?? {
      name: selectedPlugin,
    };
  }
  const normalizedQuery = pluginQuery.trim().toLowerCase();
  if (!normalizedQuery) return null;
  return (
    integrations.find((integration) => {
      const label = getIntegrationLabel(integration).trim().toLowerCase();
      return integration.name.toLowerCase() === normalizedQuery || label === normalizedQuery;
    }) ?? null
  );
}

function ChevronUpDownIcon({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 20 20"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="m6 8 4-4 4 4" />
      <path d="m6 12 4 4 4-4" />
    </svg>
  );
}

export default function ManagedIdentityDetailView({
  identityID,
  listTo = SETTINGS_IDENTITIES_PATH,
  embedded = false,
}: {
  identityID: string;
  listTo?: string;
  embedded?: boolean;
}) {
  const identityQuery = useManagedIdentityQuery(identityID);
  const membersQuery = useManagedIdentityMembersQuery(identityID);
  const grantsQuery = useManagedIdentityGrantsQuery(identityID);
  const visibleIntegrationsQuery = useIntegrationsQuery();
  const managedIntegrationsQuery = useManagedIdentityIntegrationsQuery(identityID);
  const updateIdentity = useUpdateManagedIdentityMutation(identityID);
  const deleteIdentity = useDeleteManagedIdentityMutation(identityID);
  const putMember = usePutManagedIdentityMemberMutation(identityID);
  const deleteMember = useDeleteManagedIdentityMemberMutation(identityID);
  const putGrant = usePutManagedIdentityGrantMutation(identityID);
  const deleteGrant = useDeleteManagedIdentityGrantMutation(identityID);
  const invalidateIdentity = useInvalidateManagedIdentity(identityID);

  const identity = identityQuery.data ?? null;
  const members = membersQuery.data ?? [];
  const grants = grantsQuery.data ?? [];
  const visibleIntegrations = visibleIntegrationsQuery.data ?? [];
  const managedIntegrations = managedIntegrationsQuery.data ?? [];
  const managedIntegrationError = managedIntegrationsQuery.error
    ? managedIntegrationsQuery.error instanceof Error
      ? managedIntegrationsQuery.error.message
      : "Failed to load app connections"
    : null;
  const loading =
    identityQuery.isPending ||
    membersQuery.isPending ||
    grantsQuery.isPending;
  const [error, setError] = useState<string | null>(null);
  const [selectedGrantPlugin, setSelectedGrantPlugin] = useState("");
  const [grantPluginQuery, setGrantPluginQuery] = useState("");
  const [selectedGrantRole, setSelectedGrantRole] =
    useState<ManagedIdentityGrant["role"]>("viewer");
  const [grantSelectionError, setGrantSelectionError] = useState<string | null>(null);
  const loadError =
    error ??
    (identityQuery.isError
      ? identityQuery.error instanceof Error
        ? identityQuery.error.message
        : "Failed to load identity"
      : null);

  const currentUserEmail = getUserEmail()?.trim().toLowerCase() || "";
  const role =
    members.find((member) => member.email?.trim().toLowerCase() === currentUserEmail)
      ?.role ?? "viewer";
  const canAdmin = role === "admin";
  const canConnect = role === "editor" || role === "admin";
  const connectionReturnPath = settingsIdentityDetailPath(identityID);
  const grantPluginOptions = mergeGrantPluginOptions(
    visibleIntegrations,
    grants,
  );
  const filteredGrantPluginOptions = filterIntegrations(
    grantPluginOptions,
    grantPluginQuery,
  );
  const activeGrantPlugin = resolveGrantPluginOption(
    grantPluginOptions,
    selectedGrantPlugin,
    grantPluginQuery,
  );
  const activeGrantPluginName = activeGrantPlugin?.name ?? "";
  const canSubmitGrant =
    !!activeGrantPluginName && !!selectedGrantRole && !putGrant.isPending;

  async function handleRename(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const displayName = (new FormData(e.currentTarget).get("displayName") as string)?.trim();
    if (!displayName) return;

    setError(null);
    try {
      await updateIdentity.mutateAsync(displayName);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to update identity");
    }
  }

  async function handleDelete() {
    if (!window.confirm("Delete this identity and all of its tokens, members, grants, and connections?")) {
      return;
    }
    setError(null);
    try {
      await deleteIdentity.mutateAsync();
      window.location.href = appPath(listTo);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete identity");
    }
  }

  async function handleMemberSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const form = e.currentTarget;
    const fd = new FormData(form);
    const email = (fd.get("email") as string)?.trim();
    const role = (fd.get("role") as string)?.trim() as ManagedIdentityGrant["role"];
    if (!email || !role) return;

    setError(null);
    try {
      await putMember.mutateAsync({ email, role });
      form.reset();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to update member");
    }
  }

  async function handleRemoveMember(member: { subjectId: string; email?: string }) {
    const label = member.email || member.subjectId;
    if (!window.confirm(`Remove ${label} from this identity?`)) {
      return;
    }
    setError(null);
    try {
      await deleteMember.mutateAsync(member.subjectId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to remove member");
    }
  }

  function resetGrantForm() {
    setSelectedGrantPlugin("");
    setGrantPluginQuery("");
    setSelectedGrantRole("viewer");
    setGrantSelectionError(null);
  }

  function selectGrantPlugin(integration: Integration | null) {
    const nextPlugin = integration?.name ?? "";
    setSelectedGrantPlugin(nextPlugin);
    setGrantPluginQuery(integration ? getIntegrationLabel(integration) : "");
    setGrantSelectionError(null);
  }

  async function handleGrantSubmit() {
    if (!activeGrantPluginName) return;

    setError(null);
    setGrantSelectionError(null);
    try {
      await putGrant.mutateAsync({
        plugin: activeGrantPluginName,
        role: selectedGrantRole,
      });
      resetGrantForm();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to update grant");
    }
  }

  async function handleDeleteGrant(plugin: string) {
    setError(null);
    try {
      await deleteGrant.mutateAsync(plugin);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to remove grant");
    }
  }

  const grantBusy = putGrant.isPending || deleteGrant.isPending;
  const memberBusy = putMember.isPending || deleteMember.isPending;

  function rememberConnectionReturnPath() {
    window.sessionStorage.setItem(
      CONNECTION_RETURN_PATH_STORAGE_KEY,
      connectionReturnPath,
    );
  }

  function forgetConnectionReturnPath() {
    window.sessionStorage.removeItem(CONNECTION_RETURN_PATH_STORAGE_KEY);
  }

  const detailBody = (
    <>
      <div className={embedded ? undefined : "animate-fade-in-up"}>
        <Link
          to={listTo}
          className="text-sm text-muted-foreground hover:text-foreground transition-colors duration-150"
        >
          &larr; Back to identities
        </Link>
        <PageHeader className="mt-5">
          <PageHeaderContent size="entity">
            <Eyebrow tone="accent">Managed Identity</Eyebrow>
            <PageHeaderTitle>
              {identity?.displayName || "Loading identity"}
            </PageHeaderTitle>
            {identity ? (
              <PageHeaderDescription>
                You currently have{" "}
                <span className="font-medium text-foreground">{role}</span> access.
              </PageHeaderDescription>
            ) : null}
          </PageHeaderContent>
        </PageHeader>
      </div>

      {loadError && <p className="mt-6 text-sm text-destructive">{loadError}</p>}
      {loading ? <p className="mt-10 text-sm text-muted-foreground/70">Loading...</p> : null}

      {!loading && identity ? (
        <div className="mt-10 space-y-6 animate-fade-in-up [animation-delay:60ms]">
          <section className={SECTION_CARD}>
            <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
              <div>
                <span className="label-text">Overview</span>
                <p className="mt-3 text-sm text-muted-foreground">
                  Subject ID: <code className="font-mono text-xs text-foreground">{identity.subjectId}</code>
                </p>
                <p className="mt-2 text-sm text-muted-foreground">
                  Local ID: <code className="font-mono text-xs text-foreground">{identity.id}</code>
                </p>
                <p className="mt-2 text-sm text-muted-foreground">
                  Created {new Date(identity.createdAt).toLocaleString()} · Updated {new Date(identity.updatedAt).toLocaleString()}
                </p>
              </div>
              {canAdmin ? (
                <div className="w-full max-w-xl">
                  <form onSubmit={handleRename} className="flex flex-col gap-3 sm:flex-row sm:items-end">
                    <div className="flex-1">
                      <label htmlFor="identity-name" className="label-text block">
                        Display name
                      </label>
                      <input
                        id="identity-name"
                        name="displayName"
                        type="text"
                        required
                        defaultValue={identity.displayName}
                        className={`mt-2 w-full ${INPUT_CLASSES}`}
                      />
                    </div>
                    <Button type="submit" disabled={updateIdentity.isPending}>
                      {updateIdentity.isPending ? "Saving..." : "Rename"}
                    </Button>
                  </form>
                  <div className="mt-4">
                    <Button
                      variant="danger"
                      onClick={handleDelete}
                      disabled={deleteIdentity.isPending}
                    >
                      {deleteIdentity.isPending ? "Deleting..." : "Delete Identity"}
                    </Button>
                  </div>
                </div>
              ) : null}
            </div>
          </section>

          <section className={SECTION_CARD}>
            <SectionHeader>
              <SectionHeaderContent size="sm">
                <Eyebrow>Sharing</Eyebrow>
                <SectionHeaderTitle>Members</SectionHeaderTitle>
              </SectionHeaderContent>
            </SectionHeader>
            {canAdmin ? (
              <form onSubmit={handleMemberSubmit} className="mt-6 flex flex-col gap-3 lg:flex-row lg:items-end">
                <div className="flex-1">
                  <label htmlFor="member-email" className="label-text block">
                    User email
                  </label>
                  <input
                    id="member-email"
                    name="email"
                    type="email"
                    required
                    placeholder="teammate@example.com"
                    className={`mt-2 w-full ${INPUT_CLASSES}`}
                  />
                </div>
                <div className="w-full lg:w-48">
                  <label htmlFor="member-role" className="label-text block">
                    Role
                  </label>
                  <select
                    id="member-role"
                    name="role"
                    defaultValue="viewer"
                    className={`mt-2 w-full ${INPUT_CLASSES}`}
                  >
                    <option value="viewer">viewer</option>
                    <option value="editor">editor</option>
                    <option value="admin">admin</option>
                  </select>
                </div>
                <Button type="submit" disabled={memberBusy}>
                  {memberBusy ? "Saving..." : "Add or Update Member"}
                </Button>
              </form>
            ) : null}
            <div className="mt-6 overflow-x-auto rounded-lg border border-border">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-left">
                    <th className="px-5 py-3.5 label-text">Email</th>
                    <th className="px-5 py-3.5 label-text">Subject</th>
                    <th className="px-5 py-3.5 label-text">Role</th>
                    <th className="px-5 py-3.5 label-text"></th>
                  </tr>
                </thead>
                <tbody>
                  {members.map((member) => (
                    <tr key={member.subjectId} className="border-b border-border last:border-b-0">
                      <td className="px-5 py-4 text-foreground font-medium">{member.email || "-"}</td>
                      <td className="px-5 py-4 text-muted-foreground font-mono text-xs">{member.subjectId}</td>
                      <td className="px-5 py-4 text-muted-foreground">{member.role}</td>
                      <td className="px-5 py-4">
                        {canAdmin ? (
                          <Button
                            variant="danger"
                            onClick={() => handleRemoveMember(member)}
                            disabled={memberBusy}
                          >
                            Remove
                          </Button>
                        ) : null}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className={SECTION_CARD}>
            <SectionHeader>
              <SectionHeaderContent size="sm">
                <Eyebrow>Authorization</Eyebrow>
                <SectionHeaderTitle>Identity app access</SectionHeaderTitle>
                <SectionHeaderDescription>
                  Grants are identity-level roles for apps that enforce authorization.
                </SectionHeaderDescription>
              </SectionHeaderContent>
            </SectionHeader>
            {canAdmin ? (
              <form
                onSubmit={(event) => {
                  event.preventDefault();
                  void handleGrantSubmit();
                }}
                className="mt-6 flex flex-col gap-3 xl:flex-row xl:items-end"
              >
                <div className="flex-1">
                  <label htmlFor="grant-plugin" className="label-text block">
                    App
                  </label>
                  <div className="relative mt-2">
                    <SearchIcon className="pointer-events-none absolute left-3 top-1/2 z-10 h-4 w-4 -translate-y-1/2 text-muted-foreground/70" />
                    <Combobox value={activeGrantPlugin} onChange={selectGrantPlugin} immediate>
                      <ComboboxInput
                        id="grant-plugin"
                        aria-label="App"
                        autoComplete="off"
                        className={`w-full pl-9 pr-10 ${INPUT_CLASSES}`}
                        displayValue={() => grantPluginQuery}
                        onChange={(event) => {
                          setSelectedGrantPlugin("");
                          setGrantPluginQuery(event.target.value);
                          setGrantSelectionError(null);
                        }}
                        placeholder="Choose a visible app"
                      />
                      <ComboboxButton className="absolute right-3 top-1/2 z-10 -translate-y-1/2 text-muted-foreground/70 transition-colors duration-150 hover:text-muted-foreground">
                        <ChevronUpDownIcon className="h-4 w-4" />
                      </ComboboxButton>
                      <ComboboxOptions className="absolute left-0 top-full z-20 mt-2 max-h-80 w-full overflow-auto rounded-lg border border-border bg-popover p-1 text-popover-foreground shadow-dropdown">
                        {filteredGrantPluginOptions.length > 0 ? (
                          filteredGrantPluginOptions.map((integration) => {
                            const secondaryText =
                              integration.displayName &&
                              integration.displayName !== integration.name
                                ? integration.name
                                : integration.description;
                            return (
                              <ComboboxOption
                                key={integration.name}
                                value={integration}
                                className="cursor-pointer rounded-md px-3 py-2 transition-colors duration-150 data-[focus]:bg-accent data-[focus]:text-accent-foreground"
                              >
                                <div className="text-sm font-medium text-current">
                                  {getIntegrationLabel(integration)}
                                </div>
                                {secondaryText ? (
                                  <div className="mt-0.5 text-xs text-current opacity-70">
                                    {secondaryText}
                                  </div>
                                ) : null}
                              </ComboboxOption>
                            );
                          })
                        ) : (
                          <div className="px-3 py-2 text-sm text-muted-foreground">
                            No matching apps.
                          </div>
                        )}
                      </ComboboxOptions>
                    </Combobox>
                  </div>
                </div>
                <div className="w-full xl:w-48">
                  <label htmlFor="grant-role" className="label-text block">
                    Role
                  </label>
                  <select
                    id="grant-role"
                    aria-label="Grant role"
                    value={selectedGrantRole}
                    onChange={(event) =>
                      setSelectedGrantRole(event.target.value as ManagedIdentityGrant["role"])
                    }
                    className={`mt-2 w-full ${INPUT_CLASSES}`}
                  >
                    <option value="viewer">viewer</option>
                    <option value="editor">editor</option>
                    <option value="admin">admin</option>
                  </select>
                  {grantSelectionError ? (
                    <p className="mt-2 text-xs text-destructive">{grantSelectionError}</p>
                  ) : null}
                </div>
                <Button type="submit" disabled={!canSubmitGrant}>
                  {grantBusy ? "Saving..." : "Save App Access"}
                </Button>
              </form>
            ) : null}
            {grants.length === 0 ? (
              <p className="mt-6 text-sm text-muted-foreground">
                No identity-level app access grants. Protected apps need a grant here.
              </p>
            ) : (
              <div className="mt-6 overflow-x-auto rounded-lg border border-border">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-left">
                      <th className="px-5 py-3.5 label-text">App</th>
                      <th className="px-5 py-3.5 label-text">Role</th>
                      <th className="px-5 py-3.5 label-text">Source</th>
                      <th className="px-5 py-3.5 label-text"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {grants.map((grant) => (
                      <tr key={grant.plugin} className="border-b border-border last:border-b-0">
                        <td className="px-5 py-4 text-foreground font-medium">{grant.plugin}</td>
                        <td className="px-5 py-4 text-muted-foreground">{grant.role}</td>
                        <td className="px-5 py-4 text-muted-foreground">{grant.source}</td>
                        <td className="px-5 py-4">
                          {canAdmin && grant.mutable ? (
                            <Button
                              variant="danger"
                              onClick={() => handleDeleteGrant(grant.plugin)}
                              disabled={grantBusy}
                            >
                              Remove
                            </Button>
                          ) : null}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>

          <section className={SECTION_CARD}>
            <SectionHeader>
              <SectionHeaderContent size="sm">
                <Eyebrow>Connections</Eyebrow>
                <SectionHeaderTitle>App connections</SectionHeaderTitle>
                <SectionHeaderDescription>
                  Connections store OAuth or manual credentials for this identity. They do not add app roles.
                </SectionHeaderDescription>
              </SectionHeaderContent>
            </SectionHeader>
            {managedIntegrationError ? (
              <p className="mt-6 text-sm text-destructive">{managedIntegrationError}</p>
            ) : managedIntegrations.length === 0 ? (
              <p className="mt-6 text-sm text-muted-foreground">
                No apps are available to connect for this identity.
              </p>
            ) : (
              <div className="mt-6 grid grid-cols-1 gap-4 md:grid-cols-2">
                {managedIntegrations.map((integration) => (
                  <IntegrationCard
                    key={integration.name}
                    integration={integration}
                    startOAuth={async (plugin, scopes, connectionParams, instance, connection, returnPath) => {
                      rememberConnectionReturnPath();
                      try {
                        return await startManagedIdentityIntegrationOAuth(
                          identityID,
                          plugin,
                          scopes,
                          connectionParams,
                          instance,
                          connection,
                          returnPath,
                        );
                      } catch (err) {
                        forgetConnectionReturnPath();
                        throw err;
                      }
                    }}
                    connectManual={async (plugin, credential, connectionParams, instance, connection, returnPath) => {
                      const result = await connectManagedIdentityManualIntegration(
                        identityID,
                        plugin,
                        credential,
                        connectionParams,
                        instance,
                        connection,
                        returnPath,
                      );
                      if (result.status === "selection_required") {
                        rememberConnectionReturnPath();
                      }
                      return result;
                    }}
                    disconnect={(plugin, instance, connection) =>
                      disconnectManagedIdentityIntegration(
                        identityID,
                        plugin,
                        instance,
                        connection,
                      )
                    }
                    onConnected={invalidateIdentity}
                    onDisconnected={invalidateIdentity}
                    returnPath={connectionReturnPath}
                    readOnly={!canConnect}
                    disableNavigation
                    connectionContext="managed_subject"
                  />
                ))}
              </div>
            )}
          </section>
        </div>
      ) : null}
    </>
  );

  if (embedded) {
    return (
      <section className="scroll-mt-24" aria-label="Managed identity">
        {detailBody}
      </section>
    );
  }

  return (
    <Container as="main" className="py-12">
      {detailBody}
    </Container>
  );
}
