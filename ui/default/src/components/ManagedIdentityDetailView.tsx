"use client";

import {
  Combobox,
  ComboboxButton,
  ComboboxInput,
  ComboboxOption,
  ComboboxOptions,
  Popover,
  PopoverButton,
  PopoverPanel,
} from "@headlessui/react";
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
  getIntegrationOperations,
  getIntegrations,
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
  type IntegrationOperation,
  type ManagedIdentity,
  type ManagedIdentityGrant,
  type ManagedIdentityMember,
} from "@/lib/api";
import { INPUT_CLASSES } from "@/lib/constants";
import { filterIntegrations, getIntegrationLabel } from "@/lib/integrationSearch";
import Button from "./Button";
import IdentityTokenCreateForm from "./IdentityTokenCreateForm";
import IdentityTokenTable from "./IdentityTokenTable";
import IntegrationCard from "./IntegrationCard";
import { CheckIcon, SearchIcon } from "./icons";

const SECTION_CARD =
  "rounded-lg border border-alpha bg-base-white p-6 dark:bg-surface";
const DROPDOWN_TRIGGER_CLASSES =
  `${INPUT_CLASSES} flex items-center justify-between gap-3 text-left`;

function uniqueSorted(values: string[]): string[] {
  return Array.from(new Set(values.filter(Boolean))).sort();
}

function uniqueOperationsByID(
  operations: IntegrationOperation[],
): IntegrationOperation[] {
  const seen = new Set<string>();
  const unique: IntegrationOperation[] = [];
  for (const operation of operations) {
    const id = operation.id?.trim();
    if (!id || seen.has(id)) continue;
    seen.add(id);
    unique.push({ ...operation, id });
  }
  return unique.sort((left, right) => left.id.localeCompare(right.id));
}

function mergeGrantPluginOptions(
  visibleIntegrations: Integration[],
  connectedIntegrations: Integration[],
  grants: ManagedIdentityGrant[],
): Integration[] {
  const byName = new Map<string, Integration>();
  for (const integration of [
    ...visibleIntegrations,
    ...connectedIntegrations,
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

function filterGrantOperations(
  operations: IntegrationOperation[],
  rawQuery: string,
): IntegrationOperation[] {
  const query = rawQuery.trim().toLowerCase();
  if (!query) return operations;
  return operations.filter((operation) =>
    [operation.id, operation.title || "", operation.description || ""].some((value) =>
      value.toLowerCase().includes(query),
    ),
  );
}

function operationSecondaryText(operation: IntegrationOperation): string | null {
  if (operation.title && operation.title !== operation.id) {
    return operation.title;
  }
  if (operation.description) {
    return operation.description;
  }
  return null;
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
  const [visibleIntegrations, setVisibleIntegrations] = useState<Integration[]>([]);
  const [connectionsUnavailable, setConnectionsUnavailable] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [savingName, setSavingName] = useState(false);
  const [memberBusy, setMemberBusy] = useState(false);
  const [grantBusy, setGrantBusy] = useState(false);
  const [selectedGrantPlugin, setSelectedGrantPlugin] = useState("");
  const [grantPluginQuery, setGrantPluginQuery] = useState("");
  const [grantOperationsByPlugin, setGrantOperationsByPlugin] = useState<
    Record<string, IntegrationOperation[]>
  >({});
  const [selectedGrantOperations, setSelectedGrantOperations] = useState<string[]>([]);
  const [grantOperationsQuery, setGrantOperationsQuery] = useState("");
  const [grantOperationsLoading, setGrantOperationsLoading] = useState(false);
  const [grantSelectionError, setGrantSelectionError] = useState<string | null>(null);
  const [deleteBusy, setDeleteBusy] = useState(false);
  const loadRequestIdRef = useRef(0);
  const grantOperationsRequestIdRef = useRef(0);

  const loadAll = useCallback(async () => {
    if (!identityID) return;
    const requestID = loadRequestIdRef.current + 1;
    loadRequestIdRef.current = requestID;

    try {
      const [nextIdentity, nextMembers, nextGrants, nextTokens, nextVisibleIntegrations] =
        await Promise.all([
        getManagedIdentity(identityID),
        getManagedIdentityMembers(identityID),
        getManagedIdentityGrants(identityID),
        getManagedIdentityTokens(identityID),
        getIntegrations().catch(() => [] as Integration[]),
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
      setVisibleIntegrations(nextVisibleIntegrations);
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
  const grantPluginOptions = mergeGrantPluginOptions(
    visibleIntegrations,
    integrations,
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
  const activeGrantOperations = activeGrantPluginName
    ? grantOperationsByPlugin[activeGrantPluginName] ?? []
    : [];
  const filteredGrantOperations = filterGrantOperations(
    activeGrantOperations,
    grantOperationsQuery,
  );
  const canSubmitGrant =
    !!activeGrantPluginName &&
    !grantBusy &&
    !grantOperationsLoading;

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

  useEffect(() => {
    if (!activeGrantPluginName) {
      grantOperationsRequestIdRef.current += 1;
      setGrantOperationsLoading(false);
      return;
    }
    if (grantOperationsByPlugin[activeGrantPluginName]) {
      return;
    }

    const requestID = grantOperationsRequestIdRef.current + 1;
    grantOperationsRequestIdRef.current = requestID;
    setGrantOperationsLoading(true);
    setGrantSelectionError(null);

    void getIntegrationOperations(activeGrantPluginName)
      .then((operations) => {
        if (grantOperationsRequestIdRef.current !== requestID) return;
        setGrantOperationsByPlugin((current) => ({
          ...current,
          [activeGrantPluginName]: uniqueOperationsByID(operations),
        }));
      })
      .catch((err) => {
        if (grantOperationsRequestIdRef.current !== requestID) return;
        setGrantSelectionError(
          err instanceof Error
            ? err.message
            : `Failed to load operations for ${activeGrantPluginName}`,
        );
      })
      .finally(() => {
        if (grantOperationsRequestIdRef.current === requestID) {
          setGrantOperationsLoading(false);
        }
      });
  }, [activeGrantPluginName, grantOperationsByPlugin]);

  function resetGrantForm() {
    setSelectedGrantPlugin("");
    setGrantPluginQuery("");
    setSelectedGrantOperations([]);
    setGrantOperationsQuery("");
    setGrantSelectionError(null);
  }

  function selectGrantPlugin(integration: Integration | null) {
    const nextPlugin = integration?.name ?? "";
    setSelectedGrantPlugin(nextPlugin);
    setGrantPluginQuery(integration ? getIntegrationLabel(integration) : "");
    setSelectedGrantOperations([]);
    setGrantOperationsQuery("");
    setGrantSelectionError(null);
  }

  function toggleGrantOperation(operationID: string) {
    setSelectedGrantOperations((current) =>
      current.includes(operationID)
        ? current.filter((operation) => operation !== operationID)
        : [...current, operationID].sort(),
    );
  }

  async function handleGrantSubmit() {
    if (!activeGrantPluginName) return;
    const operations =
      selectedGrantOperations.length > 0 ? selectedGrantOperations : undefined;

    setGrantBusy(true);
    setError(null);
    setGrantSelectionError(null);
    try {
      await putManagedIdentityGrant(identityID, activeGrantPluginName, operations);
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
              <form
                onSubmit={(event) => {
                  event.preventDefault();
                  void handleGrantSubmit();
                }}
                className="mt-6 flex flex-col gap-3 xl:flex-row xl:items-end"
              >
                <div className="flex-1">
                  <label htmlFor="grant-plugin" className="label-text block">
                    Plugin
                  </label>
                  <div className="relative mt-2">
                    <SearchIcon className="pointer-events-none absolute left-3 top-1/2 z-10 h-4 w-4 -translate-y-1/2 text-faint" />
                    <Combobox value={activeGrantPlugin} onChange={selectGrantPlugin} immediate>
                      <ComboboxInput
                        id="grant-plugin"
                        aria-label="Plugin"
                        autoComplete="off"
                        className={`w-full pl-9 pr-10 ${INPUT_CLASSES}`}
                        displayValue={() => grantPluginQuery}
                        onChange={(event) => {
                          setSelectedGrantPlugin("");
                          setGrantPluginQuery(event.target.value);
                          setSelectedGrantOperations([]);
                          setGrantOperationsQuery("");
                          setGrantSelectionError(null);
                        }}
                        placeholder="Choose a visible plugin"
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
                            No matching plugins.
                          </div>
                        )}
                      </ComboboxOptions>
                    </Combobox>
                  </div>
                </div>
                <div className="flex-1">
                  <label id="grant-operations-label" className="label-text block">
                    Operations
                  </label>
                  <Popover className="relative mt-2">
                    <PopoverButton
                      aria-labelledby="grant-operations-label"
                      disabled={!activeGrantPluginName || grantOperationsLoading}
                      className={`${DROPDOWN_TRIGGER_CLASSES} ${
                        !activeGrantPluginName || grantOperationsLoading
                          ? "cursor-not-allowed opacity-60"
                          : ""
                      }`}
                    >
                      <span className="min-w-0 flex-1 truncate text-sm text-left text-primary">
                        {!activeGrantPluginName
                          ? "Select a plugin first"
                          : grantOperationsLoading
                            ? "Loading operations..."
                            : selectedGrantOperations.length === 0
                              ? "Optional, select operations"
                              : selectedGrantOperations.length <= 2
                                ? selectedGrantOperations.join(", ")
                                : `${selectedGrantOperations.length} operations selected`}
                      </span>
                      <ChevronUpDownIcon className="h-4 w-4 shrink-0 text-faint" />
                    </PopoverButton>
                    <PopoverPanel className="absolute left-0 top-full z-20 mt-2 w-full rounded-lg border border-alpha bg-base-white p-3 shadow-dropdown dark:bg-surface">
                      <div className="relative">
                        <SearchIcon className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-faint" />
                        <input
                          aria-label="Filter operations"
                          value={grantOperationsQuery}
                          onChange={(event) => setGrantOperationsQuery(event.target.value)}
                          placeholder="Filter operations"
                          className={`w-full pl-9 ${INPUT_CLASSES}`}
                        />
                      </div>
                      <div className="mt-3 max-h-64 overflow-auto">
                        {filteredGrantOperations.length > 0 ? (
                          <div className="space-y-2">
                            {filteredGrantOperations.map((operation) => {
                              const secondaryText = operationSecondaryText(operation);
                              return (
                                <label
                                  key={operation.id}
                                  className="flex cursor-pointer items-start gap-3 rounded-md px-3 py-2 transition-colors duration-150 hover:bg-base-100 dark:hover:bg-surface-raised"
                                >
                                  <input
                                    type="checkbox"
                                    checked={selectedGrantOperations.includes(operation.id)}
                                    onChange={() => toggleGrantOperation(operation.id)}
                                    className="mt-0.5 h-4 w-4"
                                  />
                                  <span className="min-w-0 flex-1">
                                    <span className="block text-sm font-medium text-primary">
                                      {operation.id}
                                    </span>
                                    {secondaryText ? (
                                      <span className="mt-0.5 block text-xs text-muted">
                                        {secondaryText}
                                      </span>
                                    ) : null}
                                  </span>
                                </label>
                              );
                            })}
                          </div>
                        ) : (
                          <p className="px-3 py-2 text-sm text-muted">
                            {grantSelectionError
                              ? "Operations are unavailable right now."
                              : activeGrantOperations.length > 0
                              ? "No matching operations."
                              : "This plugin does not expose grantable operations."}
                          </p>
                        )}
                      </div>
                      <div className="mt-3 flex items-center justify-between gap-3 border-t border-alpha pt-3">
                        <button
                          type="button"
                          onClick={() => setSelectedGrantOperations([])}
                          className="text-sm text-muted transition-colors duration-150 hover:text-primary"
                        >
                          Clear selection
                        </button>
                        <p className="text-right text-xs text-faint">
                          Leave all unchecked to grant full access.
                        </p>
                      </div>
                    </PopoverPanel>
                  </Popover>
                  <p className="mt-2 text-xs text-faint">
                    {!activeGrantPluginName
                      ? "Select a plugin first to load operations."
                      : grantSelectionError
                        ? "Operations could not be loaded. Leave all unchecked to grant full access."
                        : "Leave all unchecked to grant full access."}
                  </p>
                  {grantSelectionError ? (
                    <p className="mt-2 text-xs text-ember-500">{grantSelectionError}</p>
                  ) : null}
                </div>
                <Button type="submit" disabled={!canSubmitGrant}>
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
