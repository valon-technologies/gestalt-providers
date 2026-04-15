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

export default function ManagedIdentitiesPageClient() {
  const searchParams = useSearchParams();
  const identityID = searchParams.get("id")?.trim() || "";
  const [identitiesAvailable, setIdentitiesAvailable] = useState<boolean | null>(null);
  const [identities, setIdentities] = useState<ManagedIdentity[]>([]);
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [createError, setCreateError] = useState<string | null>(null);
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
    const displayName = (new FormData(form).get("displayName") as string)?.trim();
    if (!displayName) return;

    setCreating(true);
    setCreateError(null);
    try {
      const identity = await createManagedIdentity(displayName);
      window.location.href = `/identities?id=${encodeURIComponent(identity.id)}`;
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
                Create and manage shared non-human identities for tokens and plugin connections.
              </p>
            </div>

            <form
              onSubmit={handleCreate}
              className="mt-8 flex flex-col gap-3 rounded-lg border border-alpha bg-base-white p-5 dark:bg-surface sm:flex-row sm:items-end"
            >
              <div className="flex-1">
                <label htmlFor="identity-display-name" className="label-text block">
                  Display name
                </label>
                <input
                  id="identity-display-name"
                  name="displayName"
                  type="text"
                  required
                  placeholder="e.g. Release Bot"
                  className={`mt-2 w-full ${INPUT_CLASSES}`}
                />
              </div>
              <Button type="submit" disabled={creating}>
                {creating ? "Creating..." : "Create Identity"}
              </Button>
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
