"use client";

import {
  Combobox,
  ComboboxButton,
  ComboboxInput,
  ComboboxOption,
  ComboboxOptions,
} from "@headlessui/react";
import Link from "next/link";
import { useCallback, useEffect, useRef, useState } from "react";
import {
  type APIToken,
  connectManagedIdentityManualIntegration,
  deleteManagedIdentity,
  deleteManagedIdentityGrant,
  disconnectManagedIdentityIntegration,
  deleteManagedIdentityMember,
  getManagedIdentity,
  getManagedIdentityGrants,
  getIntegrations,
  getManagedIdentityIntegrations,
  getManagedIdentityMembers,
  getManagedIdentityTokens,
  putManagedIdentityGrant,
  putManagedIdentityMember,
  startManagedIdentityIntegrationOAuth,
  updateManagedIdentity,
  type Integration,
  type ManagedIdentity,
  type ManagedIdentityGrant,
  type ManagedIdentityMember,
} from "@/lib/api";
import { getUserEmail } from "@/lib/auth";
import {
  CONNECTION_RETURN_PATH_STORAGE_KEY,
  INPUT_CLASSES,
} from "@/lib/constants";
import { filterIntegrations, getIntegrationLabel } from "@/lib/integrationSearch";
import Button from "./Button";
import IntegrationCard from "./IntegrationCard";
import IdentityTokenCreateForm from "./IdentityTokenCreateForm";
import IdentityTokenTable from "./IdentityTokenTable";
import { SearchIcon } from "./icons";

const SECTION_CARD =
  "rounded-lg border border-alpha bg-base-white p-6 dark:bg-surface";

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
}: {
  identityID: string;
}) {
  const [identity, setIdentity] = useState<ManagedIdentity | null>(null);
  const [members, setMembers] = useState<ManagedIdentityMember[]>([]);
  const [grants, setGrants] = useState<ManagedIdentityGrant[]>([]);
  const [tokens, setTokens] = useState<APIToken[]>([]);
  const [visibleIntegrations, setVisibleIntegrations] = useState<Integration[]>([]);
  const [managedIntegrations, setManagedIntegrations] = useState<Integration[]>([]);
  const [managedIntegrationError, setManagedIntegrationError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [savingName, setSavingName] = useState(false);
  const [memberBusy, setMemberBusy] = useState(false);
  const [grantBusy, setGrantBusy] = useState(false);
  const [selectedGrantPlugin, setSelectedGrantPlugin] = useState("");
  const [grantPluginQuery, setGrantPluginQuery] = useState("");
  const [selectedGrantRole, setSelectedGrantRole] =
    useState<ManagedIdentityGrant["role"]>("viewer");
  const [grantSelectionError, setGrantSelectionError] = useState<string | null>(null);
  const [deleteBusy, setDeleteBusy] = useState(false);
  const loadRequestIdRef = useRef(0);

  const loadAll = useCallback(async () => {
    if (!identityID) return;
    const requestID = loadRequestIdRef.current + 1;
    loadRequestIdRef.current = requestID;

    try {
      const managedIntegrationsResult = getManagedIdentityIntegrations(identityID)
        .then((integrations) => ({ integrations, error: null as string | null }))
        .catch((err) => ({
          integrations: [] as Integration[],
          error: err instanceof Error ? err.message : "Failed to load app connections",
        }));
      const [
        nextIdentity,
        nextMembers,
        nextGrants,
        nextTokens,
        nextVisibleIntegrations,
        nextManagedIntegrationsResult,
      ] =
        await Promise.all([
          getManagedIdentity(identityID),
          getManagedIdentityMembers(identityID),
          getManagedIdentityGrants(identityID),
          getManagedIdentityTokens(identityID),
          getIntegrations().catch(() => [] as Integration[]),
          managedIntegrationsResult,
        ]);
      if (loadRequestIdRef.current !== requestID) return;
      setIdentity(nextIdentity);
      setMembers(nextMembers);
      setGrants(nextGrants);
      setTokens(nextTokens);
      setVisibleIntegrations(nextVisibleIntegrations);
      setManagedIntegrations(nextManagedIntegrationsResult.integrations);
      setManagedIntegrationError(nextManagedIntegrationsResult.error);
      setError(null);
    } catch (err) {
      if (loadRequestIdRef.current !== requestID) return;
      setError(err instanceof Error ? err.message : "Failed to load identity");
    } finally {
      if (loadRequestIdRef.current === requestID) {
        setLoading(false);
      }
    }
  }, [identityID]);

  useEffect(() => {
    void loadAll();
  }, [loadAll]);

  const currentUserEmail = getUserEmail()?.trim().toLowerCase() || "";
  const role =
    members.find((member) => member.email?.trim().toLowerCase() === currentUserEmail)
      ?.role ?? "viewer";
  const canAdmin = role === "admin";
  const canConnect = role === "editor" || role === "admin";
  const connectionReturnPath = `/identities?id=${encodeURIComponent(identityID)}`;
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
  const canSubmitGrant = !!activeGrantPluginName && !!selectedGrantRole && !grantBusy;

  async function handleRename(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const displayName = (new FormData(e.currentTarget).get("displayName") as string)?.trim();
    if (!displayName) return;

    setSavingName(true);
    setError(null);
    try {
      await updateManagedIdentity(identityID, displayName);
      await loadAll();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to update identity");
    } finally {
      setSavingName(false);
    }
  }

  async function handleDelete() {
    if (!window.confirm("Delete this identity and all of its tokens, members, grants, and connections?")) {
      return;
    }
    setDeleteBusy(true);
    setError(null);
    try {
      await deleteManagedIdentity(identityID);
      window.location.href = "/identities";
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete identity");
      setDeleteBusy(false);
    }
  }

  async function handleMemberSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const form = e.currentTarget;
    const fd = new FormData(form);
    const email = (fd.get("email") as string)?.trim();
    const role = (fd.get("role") as string)?.trim() as ManagedIdentityMember["role"];
    if (!email || !role) return;

    setMemberBusy(true);
    setError(null);
    try {
      await putManagedIdentityMember(identityID, email, role);
      form.reset();
      await loadAll();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to update member");
    } finally {
      setMemberBusy(false);
    }
  }

  async function handleRemoveMember(member: ManagedIdentityMember) {
    const label = member.email || member.subjectId;
    if (!window.confirm(`Remove ${label} from this identity?`)) {
      return;
    }
    setMemberBusy(true);
    setError(null);
    try {
      await deleteManagedIdentityMember(identityID, member.subjectId);
      await loadAll();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to remove member");
    } finally {
      setMemberBusy(false);
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

    setGrantBusy(true);
    setError(null);
    setGrantSelectionError(null);
    try {
      await putManagedIdentityGrant(identityID, activeGrantPluginName, selectedGrantRole);
      resetGrantForm();
      await loadAll();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to update grant");
    } finally {
      setGrantBusy(false);
    }
  }

  async function handleDeleteGrant(plugin: string) {
    setGrantBusy(true);
    setError(null);
    try {
      await deleteManagedIdentityGrant(identityID, plugin);
      await loadAll();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to remove grant");
    } finally {
      setGrantBusy(false);
    }
  }

  function rememberConnectionReturnPath() {
    if (typeof window === "undefined") return;
    window.sessionStorage.setItem(
      CONNECTION_RETURN_PATH_STORAGE_KEY,
      connectionReturnPath,
    );
  }

  function forgetConnectionReturnPath() {
    if (typeof window === "undefined") return;
    window.sessionStorage.removeItem(CONNECTION_RETURN_PATH_STORAGE_KEY);
  }

  return (
    <main className="mx-auto max-w-6xl px-6 py-12">
      <div className="animate-fade-in-up">
        <Link href="/identities" className="text-sm text-muted hover:text-primary transition-colors duration-150">
          &larr; Back to identities
        </Link>
        <span className="mt-5 block label-text">Managed Identity</span>
        <h1 className="mt-2 text-2xl font-heading font-bold text-primary">
          {identity?.displayName || "Loading identity"}
        </h1>
        {identity ? (
          <p className="mt-2 text-sm text-muted">
            You currently have <span className="font-medium text-primary">{role}</span> access.
          </p>
        ) : null}
      </div>

      {error && <p className="mt-6 text-sm text-ember-500">{error}</p>}
      {loading ? <p className="mt-10 text-sm text-faint">Loading...</p> : null}

      {!loading && identity ? (
        <div className="mt-10 space-y-6 animate-fade-in-up [animation-delay:60ms]">
          <section className={SECTION_CARD}>
            <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
              <div>
                <span className="label-text">Overview</span>
                <p className="mt-3 text-sm text-muted">
                  Subject ID: <code className="font-mono text-xs text-primary">{identity.subjectId}</code>
                </p>
                <p className="mt-2 text-sm text-muted">
                  Local ID: <code className="font-mono text-xs text-primary">{identity.id}</code>
                </p>
                <p className="mt-2 text-sm text-muted">
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
                    <Button type="submit" disabled={savingName}>
                      {savingName ? "Saving..." : "Rename"}
                    </Button>
                  </form>
                  <div className="mt-4">
                    <Button
                      variant="danger"
                      onClick={handleDelete}
                      disabled={deleteBusy}
                    >
                      {deleteBusy ? "Deleting..." : "Delete Identity"}
                    </Button>
                  </div>
                </div>
              ) : null}
            </div>
          </section>

          <section className={SECTION_CARD}>
            <div>
              <span className="label-text">Sharing</span>
              <h2 className="mt-2 text-lg font-heading font-bold text-primary">Members</h2>
            </div>
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
            <div className="mt-6 overflow-x-auto rounded-lg border border-alpha">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-alpha text-left">
                    <th className="px-5 py-3.5 label-text">Email</th>
                    <th className="px-5 py-3.5 label-text">Subject</th>
                    <th className="px-5 py-3.5 label-text">Role</th>
                    <th className="px-5 py-3.5 label-text"></th>
                  </tr>
                </thead>
                <tbody>
                  {members.map((member) => (
                    <tr key={member.subjectId} className="border-b border-alpha last:border-b-0">
                      <td className="px-5 py-4 text-primary font-medium">{member.email || "-"}</td>
                      <td className="px-5 py-4 text-muted font-mono text-xs">{member.subjectId}</td>
                      <td className="px-5 py-4 text-muted">{member.role}</td>
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
            <span className="label-text">Authorization</span>
            <h2 className="mt-2 text-lg font-heading font-bold text-primary">Identity App Access</h2>
            <p className="mt-2 text-sm text-muted">
              Grants are identity-level roles for apps that enforce authorization. API keys do not create these grants; they only authenticate as this identity.
            </p>
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
                    <SearchIcon className="pointer-events-none absolute left-3 top-1/2 z-10 h-4 w-4 -translate-y-1/2 text-faint" />
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
                      <ComboboxButton className="absolute right-3 top-1/2 z-10 -translate-y-1/2 text-faint transition-colors duration-150 hover:text-muted">
                        <ChevronUpDownIcon className="h-4 w-4" />
                      </ComboboxButton>
                      <ComboboxOptions className="absolute left-0 top-full z-20 mt-2 max-h-80 w-full overflow-auto rounded-lg border border-alpha bg-base-white p-1 shadow-dropdown dark:bg-surface">
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
                                className="cursor-pointer rounded-md px-3 py-2 transition-colors duration-150 data-[focus]:bg-base-100 dark:data-[focus]:bg-surface-raised"
                              >
                                <div className="text-sm font-medium text-primary">
                                  {getIntegrationLabel(integration)}
                                </div>
                                {secondaryText ? (
                                  <div className="mt-0.5 text-xs text-muted">
                                    {secondaryText}
                                  </div>
                                ) : null}
                              </ComboboxOption>
                            );
                          })
                        ) : (
                          <div className="px-3 py-2 text-sm text-muted">
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
                    <p className="mt-2 text-xs text-ember-500">{grantSelectionError}</p>
                  ) : null}
                </div>
                <Button type="submit" disabled={!canSubmitGrant}>
                  {grantBusy ? "Saving..." : "Save App Access"}
                </Button>
              </form>
            ) : null}
            {grants.length === 0 ? (
              <p className="mt-6 text-sm text-muted">
                No identity-level app access grants. Protected apps need a grant here; API keys can still be created below.
              </p>
            ) : (
              <div className="mt-6 overflow-x-auto rounded-lg border border-alpha">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-alpha text-left">
                      <th className="px-5 py-3.5 label-text">App</th>
                      <th className="px-5 py-3.5 label-text">Role</th>
                      <th className="px-5 py-3.5 label-text">Source</th>
                      <th className="px-5 py-3.5 label-text"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {grants.map((grant) => (
                      <tr key={grant.plugin} className="border-b border-alpha last:border-b-0">
                        <td className="px-5 py-4 text-primary font-medium">{grant.plugin}</td>
                        <td className="px-5 py-4 text-muted">{grant.role}</td>
                        <td className="px-5 py-4 text-muted">{grant.source}</td>
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
            <span className="label-text">Connections</span>
            <h2 className="mt-2 text-lg font-heading font-bold text-primary">App Connections</h2>
            <p className="mt-2 text-sm text-muted">
              Connections store OAuth or manual credentials for this identity. They do not add app roles or change API-key limits.
            </p>
            {managedIntegrationError ? (
              <p className="mt-6 text-sm text-ember-500">{managedIntegrationError}</p>
            ) : managedIntegrations.length === 0 ? (
              <p className="mt-6 text-sm text-muted">
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
                    onConnected={loadAll}
                    onDisconnected={loadAll}
                    returnPath={connectionReturnPath}
                    readOnly={!canConnect}
                    disableNavigation
                    connectionContext="managed_subject"
                  />
                ))}
              </div>
            )}
          </section>

          <section className={SECTION_CARD}>
            <span className="label-text">API Access</span>
            <h2 className="mt-2 text-lg font-heading font-bold text-primary">Identity API Keys</h2>
            <p className="mt-2 text-sm text-muted">
              API keys authenticate as this identity. By default, a key follows managed identity app access and connector credentials at use time; token limits only narrow one key.
            </p>
            {canAdmin ? (
              <IdentityTokenCreateForm
                identityID={identityID}
                grants={grants}
                onCreated={loadAll}
              />
            ) : (
              <p className="mt-6 text-sm text-muted">
                Only identity admins can create subject-owned API tokens.
              </p>
            )}
            <div className="mt-8">
              <IdentityTokenTable
                identityID={identityID}
                tokens={tokens}
                canRevoke={canAdmin}
                onRevoked={loadAll}
              />
            </div>
          </section>
        </div>
      ) : null}
    </main>
  );
}
