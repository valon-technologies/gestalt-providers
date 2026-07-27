import { useState } from "react";
import { Link, useRouterState } from "@tanstack/react-router";
import { INPUT_CLASSES } from "@/lib/constants";
import {
  useAuthInfoQuery,
  useCreateManagedIdentityMutation,
  useManagedIdentitiesQuery,
} from "@/lib/queries";
import AuthGuard from "@/components/AuthGuard";
import Button from "@/components/Button";
import Container from "@/components/Container";
import IdentitySummaryCard from "@/components/IdentitySummaryCard";
import ManagedIdentityDetailView from "@/components/ManagedIdentityDetailView";
import Nav from "@/components/Nav";

function managedIdentityLocalIDFromName(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 128);
}

function canonicalManagedIdentityID(value: string): string {
  const trimmed = value.trim();
  if (!trimmed || trimmed.includes(":")) return trimmed;
  return `service_account:${trimmed}`;
}

export default function ManagedIdentitiesPage() {
  const search = useRouterState({ select: (state) => state.location.search });
  const identityID = canonicalManagedIdentityID(
    new URLSearchParams(search).get("id") || "",
  );
  const authInfoQuery = useAuthInfoQuery();
  const identitiesAvailable =
    authInfoQuery.data === undefined
      ? authInfoQuery.isPending
        ? null
        : true
      : authInfoQuery.data.provider !== "none";
  const identitiesQuery = useManagedIdentitiesQuery(
    identitiesAvailable === true && !identityID,
  );
  const createIdentity = useCreateManagedIdentityMutation();
  const [displayNameInput, setDisplayNameInput] = useState("");
  const [identityLocalID, setIdentityLocalID] = useState("");
  const [identityIDEdited, setIdentityIDEdited] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);

  const identities = identitiesQuery.data ?? [];
  const loading = identitiesQuery.isPending;
  const error = identitiesQuery.error
    ? identitiesQuery.error instanceof Error
      ? identitiesQuery.error.message
      : "Failed to load identities"
    : null;

  async function handleCreate(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const form = e.currentTarget;
    const fd = new FormData(form);
    const displayName = (fd.get("displayName") as string)?.trim();
    const id = (fd.get("identityID") as string)?.trim();
    if (!displayName || !id) return;

    setCreateError(null);
    try {
      const identity = await createIdentity.mutateAsync({ id, displayName });
      window.location.href = `/identities?id=${encodeURIComponent(identity.subjectId)}`;
    } catch (err) {
      setCreateError(err instanceof Error ? err.message : "Failed to create identity");
    }
  }

  return (
    <AuthGuard>
      <div className="min-h-screen">
        <Nav />
        {identitiesAvailable === null ? (
          <Container as="main" className="py-12">
            <p className="text-sm text-faint">Loading...</p>
          </Container>
        ) : identitiesAvailable === false ? (
          <Container as="main" className="py-12">
            <div className="mx-auto max-w-3xl">
              <div className="animate-fade-in-up">
                <span className="label-text">Workspace</span>
                <h1 className="mt-2 text-2xl font-heading text-primary">
                  Agent Identities
                </h1>
                <p className="mt-4 text-sm text-muted">
                  Managed identities require platform auth and are unavailable when auth is disabled.
                </p>
                <Link
                  to="/"
                  className="mt-6 inline-flex text-sm text-muted transition-colors duration-150 hover:text-primary"
                >
                  &larr; Back to dashboard
                </Link>
              </div>
            </div>
          </Container>
        ) : identityID ? (
          <ManagedIdentityDetailView identityID={identityID} />
        ) : (
          <Container as="main" className="py-12">
            <div className="animate-fade-in-up">
              <span className="label-text">Workspace</span>
              <h1 className="mt-2 text-2xl font-heading text-primary">
                Agent Identities
              </h1>
              <p className="mt-2 text-sm text-muted">
                Create and manage shared non-human identities for tokens and app authorization.
              </p>
            </div>

            <form
              onSubmit={handleCreate}
              className="mt-8 grid gap-3 rounded-lg border border-alpha bg-base-white p-5 dark:bg-surface lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto] lg:items-end"
            >
              <div>
                <label htmlFor="identity-display-name" className="label-text block">
                  Display name
                </label>
                <input
                  id="identity-display-name"
                  name="displayName"
                  type="text"
                  required
                  placeholder="e.g. Release Bot"
                  value={displayNameInput}
                  onChange={(event) => {
                    const nextValue = event.target.value;
                    setDisplayNameInput(nextValue);
                    if (!identityIDEdited) {
                      setIdentityLocalID(managedIdentityLocalIDFromName(nextValue));
                    }
                  }}
                  className={`mt-2 w-full ${INPUT_CLASSES}`}
                />
              </div>
              <div>
                <label htmlFor="identity-id" className="label-text block">
                  Identity ID
                </label>
                <div className="mt-2 flex rounded-md border border-alpha bg-base-white transition-all duration-150 focus-within:border-alpha-strong focus-within:ring-2 focus-within:ring-foreground/10 dark:bg-surface">
                  <span className="flex items-center border-r border-alpha px-3 font-mono text-sm text-faint">
                    service_account:
                  </span>
                  <input
                    id="identity-id"
                    name="identityID"
                    type="text"
                    required
                    pattern="[A-Za-z0-9._-]{1,128}"
                    placeholder="release-bot"
                    value={identityLocalID}
                    onChange={(event) => {
                      setIdentityIDEdited(true);
                      setIdentityLocalID(event.target.value);
                    }}
                    className="min-w-0 flex-1 bg-transparent px-4 py-3 text-primary placeholder:text-faint focus:outline-hidden"
                  />
                </div>
                <p className="mt-2 text-xs text-faint">
                  Letters, numbers, dots, underscores, and hyphens.
                </p>
              </div>
              <div>
                <Button type="submit" disabled={createIdentity.isPending}>
                  {createIdentity.isPending ? "Creating..." : "Create Identity"}
                </Button>
              </div>
            </form>

            {createError && <p className="mt-4 text-sm text-ember-500">{createError}</p>}
            {error && <p className="mt-6 text-sm text-ember-500">{error}</p>}

            {loading ? (
              <p className="mt-10 text-sm text-faint">Loading...</p>
            ) : !error && identities.length === 0 ? (
              <p className="mt-10 text-sm text-faint">
                No managed identities yet.
              </p>
            ) : !error ? (
              <div className="mt-10 grid grid-cols-1 gap-4 md:grid-cols-2 animate-fade-in-up [animation-delay:60ms]">
                {identities.map((identity) => (
                  <IdentitySummaryCard key={identity.id} identity={identity} />
                ))}
              </div>
            ) : null}
          </Container>
        )}
      </div>
    </AuthGuard>
  );
}
