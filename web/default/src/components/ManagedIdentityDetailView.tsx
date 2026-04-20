"use client";

import Link from "next/link";
import { useCallback, useEffect, useRef, useState } from "react";
import {
  APIError,
  type APIToken,
  deleteManagedIdentity,
  deleteManagedIdentityGrant,
  deleteManagedIdentityMember,
  getManagedIdentity,
  getManagedIdentityGrants,
  getManagedIdentityIntegrations,
  getManagedIdentityMembers,
  getManagedIdentityTokens,
  putManagedIdentityGrant,
  putManagedIdentityMember,
  startManagedIdentityOAuth,
  connectManagedIdentityManual,
  disconnectManagedIdentityIntegration,
  updateManagedIdentity,
  type Integration,
  type ManagedIdentity,
  type ManagedIdentityGrant,
  type ManagedIdentityMember,
} from "@/lib/api";
import { INPUT_CLASSES } from "@/lib/constants";
import Button from "./Button";
import IdentityTokenCreateForm from "./IdentityTokenCreateForm";
import IdentityTokenTable from "./IdentityTokenTable";
import IntegrationCard from "./IntegrationCard";

const SECTION_CARD =
  "rounded-lg border border-alpha bg-base-white p-6 dark:bg-surface";

function uniqueSorted(values: string[]): string[] {
  return Array.from(new Set(values.filter(Boolean))).sort();
}

function formatOperations(operations?: string[]): string {
  return operations?.length ? operations.join(", ") : "All operations";
}

function isManagedIdentityConnectionsUnavailable(err: unknown, identityID: string): boolean {
  if (!(err instanceof Error)) {
    return false;
  }
  if (err instanceof APIError && err.status === 404) {
    return true;
  }
  return (
    err instanceof APIError &&
    err.message.includes(`/api/v1/identities/${identityID}/integrations`) &&
    err.message.includes("Expected JSON response")
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
  const [integrations, setIntegrations] = useState<Integration[]>([]);
  const [connectionsUnavailable, setConnectionsUnavailable] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [savingName, setSavingName] = useState(false);
  const [memberBusy, setMemberBusy] = useState(false);
  const [grantBusy, setGrantBusy] = useState(false);
  const [deleteBusy, setDeleteBusy] = useState(false);
  const loadRequestIdRef = useRef(0);

  const loadAll = useCallback(async () => {
    if (!identityID) return;
    const requestID = loadRequestIdRef.current + 1;
    loadRequestIdRef.current = requestID;

    try {
      const [nextIdentity, nextMembers, nextGrants, nextTokens] = await Promise.all([
        getManagedIdentity(identityID),
        getManagedIdentityMembers(identityID),
        getManagedIdentityGrants(identityID),
        getManagedIdentityTokens(identityID),
      ]);
      let nextIntegrations: Integration[] = [];
      let nextConnectionsUnavailable = false;
      try {
        nextIntegrations = await getManagedIdentityIntegrations(identityID);
      } catch (err) {
        if (isManagedIdentityConnectionsUnavailable(err, identityID)) {
          nextConnectionsUnavailable = true;
        } else {
          throw err;
        }
      }
      if (loadRequestIdRef.current !== requestID) return;
      setIdentity(nextIdentity);
      setMembers(nextMembers);
      setGrants(nextGrants);
      setTokens(nextTokens);
      setIntegrations(nextIntegrations);
      setConnectionsUnavailable(nextConnectionsUnavailable);
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

  const role = identity?.role;
  const canEdit = role === "editor" || role === "admin";
  const canAdmin = role === "admin";
  const returnPath = `/identities?id=${encodeURIComponent(identityID)}`;
  const pluginOptions = uniqueSorted([
    ...integrations.map((integration) => integration.name),
    ...grants.map((grant) => grant.plugin),
  ]);

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

  async function handleRemoveMember(email: string) {
    if (!window.confirm(`Remove ${email} from this identity?`)) {
      return;
    }
    setMemberBusy(true);
    setError(null);
    try {
      await deleteManagedIdentityMember(identityID, email);
      await loadAll();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to remove member");
    } finally {
      setMemberBusy(false);
    }
  }

  async function handleGrantSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const form = e.currentTarget;
    const fd = new FormData(form);
    const plugin = (fd.get("plugin") as string)?.trim();
    const rawOperations = (fd.get("operations") as string)?.trim() || "";
    if (!plugin) return;
    const operations = uniqueSorted(rawOperations.split(",").map((value) => value.trim()));

    setGrantBusy(true);
    setError(null);
    try {
      await putManagedIdentityGrant(identityID, plugin, operations.length > 0 ? operations : undefined);
      form.reset();
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
            You currently have <span className="font-medium text-primary">{identity.role}</span> access.
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
                  Identity ID: <code className="font-mono text-xs text-primary">{identity.id}</code>
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
                    <th className="px-5 py-3.5 label-text">Role</th>
                    <th className="px-5 py-3.5 label-text">Added</th>
                    <th className="px-5 py-3.5 label-text"></th>
                  </tr>
                </thead>
                <tbody>
                  {members.map((member) => (
                    <tr key={member.email} className="border-b border-alpha last:border-b-0">
                      <td className="px-5 py-4 text-primary font-medium">{member.email}</td>
                      <td className="px-5 py-4 text-muted">{member.role}</td>
                      <td className="px-5 py-4 text-muted font-mono text-xs">
                        {new Date(member.createdAt).toLocaleDateString()}
                      </td>
                      <td className="px-5 py-4">
                        {canAdmin ? (
                          <Button
                            variant="danger"
                            onClick={() => handleRemoveMember(member.email)}
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
            <h2 className="mt-2 text-lg font-heading font-bold text-primary">Plugin Grants</h2>
            {canEdit ? (
              <form onSubmit={handleGrantSubmit} className="mt-6 flex flex-col gap-3 xl:flex-row xl:items-end">
                <div className="flex-1">
                  <label htmlFor="grant-plugin" className="label-text block">
                    Plugin
                  </label>
                  <input
                    id="grant-plugin"
                    name="plugin"
                    list="identity-plugin-options"
                    required
                    placeholder="Choose a visible plugin"
                    className={`mt-2 w-full ${INPUT_CLASSES}`}
                  />
                  <datalist id="identity-plugin-options">
                    {pluginOptions.map((plugin) => (
                      <option key={plugin} value={plugin} />
                    ))}
                  </datalist>
                </div>
                <div className="flex-1">
                  <label htmlFor="grant-operations" className="label-text block">
                    Operations
                  </label>
                  <input
                    id="grant-operations"
                    name="operations"
                    type="text"
                    placeholder="Optional, comma-separated"
                    className={`mt-2 w-full ${INPUT_CLASSES}`}
                  />
                </div>
                <Button type="submit" disabled={grantBusy}>
                  {grantBusy ? "Saving..." : "Set Grant"}
                </Button>
              </form>
            ) : null}
            <div className="mt-6 overflow-x-auto rounded-lg border border-alpha">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-alpha text-left">
                    <th className="px-5 py-3.5 label-text">Plugin</th>
                    <th className="px-5 py-3.5 label-text">Operations</th>
                    <th className="px-5 py-3.5 label-text">Updated</th>
                    <th className="px-5 py-3.5 label-text"></th>
                  </tr>
                </thead>
                <tbody>
                  {grants.map((grant) => (
                    <tr key={grant.plugin} className="border-b border-alpha last:border-b-0">
                      <td className="px-5 py-4 text-primary font-medium">{grant.plugin}</td>
                      <td className="px-5 py-4 text-muted">{formatOperations(grant.operations)}</td>
                      <td className="px-5 py-4 text-muted font-mono text-xs">
                        {new Date(grant.updatedAt).toLocaleDateString()}
                      </td>
                      <td className="px-5 py-4">
                        {canEdit ? (
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
          </section>

          <section className={SECTION_CARD}>
            <span className="label-text">Connections</span>
            <h2 className="mt-2 text-lg font-heading font-bold text-primary">Plugin Connections</h2>
            {connectionsUnavailable ? (
              <p className="mt-6 text-sm text-muted">
                Managed identity plugin connections are unavailable on this server.
              </p>
            ) : (
              <div className="mt-6 grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3">
                {integrations.map((integration) => (
                  <IntegrationCard
                    key={integration.name}
                    integration={integration}
                    onConnected={loadAll}
                    onDisconnected={loadAll}
                    startOAuth={(integrationName, scopes, connectionParams, instance, connection, nextReturnPath) =>
                      startManagedIdentityOAuth(
                        identityID,
                        integrationName,
                        scopes,
                        connectionParams,
                        instance,
                        connection,
                        nextReturnPath,
                      )
                    }
                    connectManual={(integrationName, credential, connectionParams, instance, connection, nextReturnPath) =>
                      connectManagedIdentityManual(
                        identityID,
                        integrationName,
                        credential,
                        connectionParams,
                        instance,
                        connection,
                        nextReturnPath,
                      )
                    }
                    disconnect={(integrationName, instance, connection) =>
                      disconnectManagedIdentityIntegration(identityID, integrationName, instance, connection)
                    }
                    returnPath={returnPath}
                    readOnly={!canEdit}
                    disableNavigation
                  />
                ))}
              </div>
            )}
          </section>

          <section className={SECTION_CARD}>
            <span className="label-text">API Access</span>
            <h2 className="mt-2 text-lg font-heading font-bold text-primary">Identity Tokens</h2>
            <IdentityTokenCreateForm
              identityID={identityID}
              grants={grants}
              onCreated={loadAll}
            />
            <div className="mt-8">
              <IdentityTokenTable
                identityID={identityID}
                tokens={tokens}
                canRevoke={canEdit}
                onRevoked={loadAll}
              />
            </div>
          </section>
        </div>
      ) : null}
    </main>
  );
}
