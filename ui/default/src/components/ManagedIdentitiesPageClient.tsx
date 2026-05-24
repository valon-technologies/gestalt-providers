"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import {
  createManagedIdentity,
  getAuthInfo,
  getManagedIdentities,
  type ManagedIdentity,
} from "@/lib/api";
import { INPUT_CLASSES } from "@/lib/constants";
import AuthGuard from "@/components/AuthGuard";
import Button from "@/components/Button";
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

export default function ManagedIdentitiesPageClient() {
  const searchParams = useSearchParams();
  const identityID = canonicalManagedIdentityID(searchParams.get("id") || "");
  const [identitiesAvailable, setIdentitiesAvailable] = useState<boolean | null>(null);
  const [identities, setIdentities] = useState<ManagedIdentity[]>([]);
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [createError, setCreateError] = useState<string | null>(null);
  const [displayNameInput, setDisplayNameInput] = useState("");
  const [identityLocalID, setIdentityLocalID] = useState("");
  const [identityIDEdited, setIdentityIDEdited] = useState(false);
  const loadRequestIdRef = useRef(0);

  async function loadIdentities() {
    const requestID = loadRequestIdRef.current + 1;
    loadRequestIdRef.current = requestID;

    try {
      const nextIdentities = await getManagedIdentities();
      if (loadRequestIdRef.current !== requestID) return;
      setIdentities(nextIdentities);
      setError(null);
    } catch (err) {
      if (loadRequestIdRef.current !== requestID) return;
      setError(err instanceof Error ? err.message : "Failed to load identities");
    } finally {
      if (loadRequestIdRef.current === requestID) {
        setLoading(false);
      }
    }
  }

  useEffect(() => {
    let active = true;
    getAuthInfo()
      .then((info) => {
        if (!active) return;
        const available = info.provider !== "none";
        setIdentitiesAvailable(available);
        if (!available) {
          setLoading(false);
        }
      })
      .catch(() => {
        if (!active) return;
        setIdentitiesAvailable(true);
      });

    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (identitiesAvailable !== true || identityID) return;
    void loadIdentities();
  }, [identitiesAvailable, identityID]);

  async function handleCreate(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const form = e.currentTarget;
    const fd = new FormData(form);
    const displayName = (fd.get("displayName") as string)?.trim();
    const id = (fd.get("identityID") as string)?.trim();
    if (!displayName || !id) return;

    setCreating(true);
    setCreateError(null);
    try {
      const identity = await createManagedIdentity(id, displayName);
      window.location.href = `/identities?id=${encodeURIComponent(identity.subjectId)}`;
    } catch (err) {
      setCreateError(err instanceof Error ? err.message : "Failed to create identity");
    } finally {
      setCreating(false);
    }
  }

  return (
    <AuthGuard>
      <div className="min-h-screen">
        <Nav />
        {identitiesAvailable === null ? (
          <main className="mx-auto max-w-5xl px-6 py-12">
            <p className="text-sm text-faint">Loading...</p>
          </main>
        ) : identitiesAvailable === false ? (
          <main className="mx-auto max-w-3xl px-6 py-12">
            <div className="animate-fade-in-up">
              <span className="label-text">Workspace</span>
              <h1 className="mt-2 text-2xl font-heading font-bold text-primary">
                Agent Identities
              </h1>
              <p className="mt-4 text-sm text-muted">
                Managed identities require platform auth and are unavailable when auth is disabled.
              </p>
              <Link
                href="/"
                className="mt-6 inline-flex text-sm text-muted transition-colors duration-150 hover:text-primary"
              >
                &larr; Back to dashboard
              </Link>
            </div>
          </main>
        ) : identityID ? (
          <ManagedIdentityDetailView identityID={identityID} />
        ) : (
          <main className="mx-auto max-w-5xl px-6 py-12">
            <div className="animate-fade-in-up">
              <span className="label-text">Workspace</span>
              <h1 className="mt-2 text-2xl font-heading font-bold text-primary">
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
                <div className="mt-2 flex rounded-md border border-alpha bg-base-white transition-all duration-150 focus-within:border-alpha-strong focus-within:ring-2 focus-within:ring-alpha-5 dark:bg-surface">
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
                    className="min-w-0 flex-1 bg-transparent px-4 py-3 text-primary placeholder:text-faint focus:outline-none"
                  />
                </div>
                <p className="mt-2 text-xs text-faint">
                  Letters, numbers, dots, underscores, and hyphens.
                </p>
              </div>
              <div>
                <Button type="submit" disabled={creating}>
                  {creating ? "Creating..." : "Create Identity"}
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
          </main>
        )}
      </div>
    </AuthGuard>
  );
}
