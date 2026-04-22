"use client";

import { useEffect, useRef, useState } from "react";
import {
  createManagedIdentity,
  getAuthInfo,
  getManagedIdentities,
  getTokens,
  type APIToken,
  type ManagedIdentity,
} from "@/lib/api";
import { INPUT_CLASSES } from "@/lib/constants";
import AuthGuard from "@/components/AuthGuard";
import Button from "@/components/Button";
import IdentitySummaryCard from "@/components/IdentitySummaryCard";
import Nav from "@/components/Nav";
import TokenCreateForm from "@/components/TokenCreateForm";
import TokenTable from "@/components/TokenTable";

export default function AuthorizationPageClient() {
  const [identitiesAvailable, setIdentitiesAvailable] = useState<boolean | null>(null);

  const [tokens, setTokens] = useState<APIToken[]>([]);
  const [tokensLoading, setTokensLoading] = useState(true);
  const [tokensError, setTokensError] = useState<string | null>(null);

  const [identities, setIdentities] = useState<ManagedIdentity[]>([]);
  const [identitiesLoading, setIdentitiesLoading] = useState(true);
  const [identitiesError, setIdentitiesError] = useState<string | null>(null);

  const [creatingIdentity, setCreatingIdentity] = useState(false);
  const [createIdentityError, setCreateIdentityError] = useState<string | null>(null);

  const tokenLoadRequestIdRef = useRef(0);
  const identityLoadRequestIdRef = useRef(0);

  async function loadTokens() {
    const requestID = tokenLoadRequestIdRef.current + 1;
    tokenLoadRequestIdRef.current = requestID;

    try {
      const nextTokens = await getTokens();
      if (tokenLoadRequestIdRef.current !== requestID) return;
      setTokens(nextTokens);
      setTokensError(null);
    } catch (err) {
      if (tokenLoadRequestIdRef.current !== requestID) return;
      setTokensError(err instanceof Error ? err.message : "Failed to load tokens");
    } finally {
      if (tokenLoadRequestIdRef.current === requestID) {
        setTokensLoading(false);
      }
    }
  }

  async function loadIdentities() {
    const requestID = identityLoadRequestIdRef.current + 1;
    identityLoadRequestIdRef.current = requestID;

    try {
      const nextIdentities = await getManagedIdentities();
      if (identityLoadRequestIdRef.current !== requestID) return;
      setIdentities(nextIdentities);
      setIdentitiesError(null);
    } catch (err) {
      if (identityLoadRequestIdRef.current !== requestID) return;
      setIdentitiesError(err instanceof Error ? err.message : "Failed to load identities");
    } finally {
      if (identityLoadRequestIdRef.current === requestID) {
        setIdentitiesLoading(false);
      }
    }
  }

  useEffect(() => {
    void loadTokens();
  }, []);

  useEffect(() => {
    let active = true;

    getAuthInfo()
      .then((info) => {
        if (!active) return;
        setIdentitiesAvailable(info.provider !== "none");
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
    if (identitiesAvailable === null) return;
    if (identitiesAvailable === false) {
      setIdentitiesLoading(false);
      return;
    }
    void loadIdentities();
  }, [identitiesAvailable]);

  async function handleCreateIdentity(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const form = e.currentTarget;
    const displayName = (new FormData(form).get("displayName") as string)?.trim();
    if (!displayName) return;

    setCreatingIdentity(true);
    setCreateIdentityError(null);

    try {
      const identity = await createManagedIdentity(displayName);
      window.location.href = `/identities?id=${encodeURIComponent(identity.id)}`;
    } catch (err) {
      setCreateIdentityError(
        err instanceof Error ? err.message : "Failed to create identity",
      );
    } finally {
      setCreatingIdentity(false);
    }
  }

  return (
    <AuthGuard>
      <div className="min-h-screen">
        <Nav />
        <main className="mx-auto max-w-5xl px-6 py-12">
          <div className="animate-fade-in-up">
            <span className="label-text">Security</span>
            <h1 className="mt-2 text-2xl font-heading font-bold text-primary">
              Authorization
            </h1>
            <p className="mt-3 max-w-3xl text-sm text-muted">
              API tokens belong to your current signed-in identity. Managed identities are
              shared principals for automation, plugin connections, and tokens that should
              outlive an individual user.
            </p>

            <div className="mt-6 flex flex-wrap gap-3">
              <a
                href="#tokens"
                className="rounded-full border border-alpha bg-base-white px-4 py-2 text-sm text-primary transition-colors duration-150 hover:border-alpha-strong hover:bg-alpha-5 dark:bg-surface"
              >
                Your tokens
              </a>
              <a
                href="#identities"
                className="rounded-full border border-alpha bg-base-white px-4 py-2 text-sm text-primary transition-colors duration-150 hover:border-alpha-strong hover:bg-alpha-5 dark:bg-surface"
              >
                Managed identities
              </a>
            </div>
          </div>

          <section
            id="tokens"
            className="mt-12 animate-fade-in-up rounded-2xl border border-alpha bg-base-white p-6 [animation-delay:120ms] dark:bg-surface"
          >
            <AuthorizationSectionIntro
              eyebrow="Current User"
              title="Your API Tokens"
              description="Create personal tokens for local tooling, scripts, and one-off integrations. These act as you."
            />

            <div className="mt-8">
              <div className="rounded-xl border border-alpha bg-base-white p-5 dark:bg-surface-raised">
                <TokenCreateForm onCreated={loadTokens} />
              </div>
            </div>

            {tokensError && <p className="mt-4 text-sm text-ember-500">{tokensError}</p>}

            {tokensLoading ? (
              <p className="mt-10 text-sm text-faint">Loading...</p>
            ) : !tokensError ? (
              <div className="mt-8">
                <TokenTable tokens={tokens} onRevoked={loadTokens} />
              </div>
            ) : null}
          </section>

          <section
            id="identities"
            className="mt-12 animate-fade-in-up rounded-2xl border border-alpha bg-base-white p-6 [animation-delay:180ms] dark:bg-surface"
          >
            <AuthorizationSectionIntro
              eyebrow="Workspace"
              title="Managed Identities"
              description="Use a shared identity for release bots, service accounts, and integration credentials that should not belong to one person."
            />

            {identitiesAvailable === null ? (
              <p className="mt-8 text-sm text-faint">Loading...</p>
            ) : identitiesAvailable === false ? (
              <div className="mt-8 rounded-xl border border-alpha bg-alpha-5 p-5">
                <p className="text-sm text-muted">
                  Managed identities require platform auth and are unavailable when auth is
                  disabled.
                </p>
              </div>
            ) : (
              <>
                <div className="mt-8">
                  <form
                    onSubmit={handleCreateIdentity}
                    className="rounded-xl border border-alpha bg-base-white p-5 dark:bg-surface-raised"
                  >
                    <div className="flex flex-col gap-3 sm:flex-row sm:items-end">
                      <div className="flex-1">
                        <label
                          htmlFor="identity-display-name"
                          className="label-text block"
                        >
                          Identity name
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
                      <Button type="submit" disabled={creatingIdentity} className="sm:shrink-0">
                        {creatingIdentity ? "Creating..." : "Create Identity"}
                      </Button>
                    </div>
                  </form>
                </div>

                {createIdentityError && (
                  <p className="mt-4 text-sm text-ember-500">{createIdentityError}</p>
                )}
                {identitiesError && (
                  <p className="mt-4 text-sm text-ember-500">{identitiesError}</p>
                )}

                {identitiesLoading ? (
                  <p className="mt-10 text-sm text-faint">Loading...</p>
                ) : !identitiesError && identities.length === 0 ? (
                  <div className="mt-8 rounded-xl border border-dashed border-alpha p-8 text-center">
                    <p className="text-sm text-faint">No managed identities yet.</p>
                  </div>
                ) : !identitiesError ? (
                  <div className="mt-8 grid grid-cols-1 gap-4 md:grid-cols-2">
                    {identities.map((identity) => (
                      <IdentitySummaryCard key={identity.id} identity={identity} />
                    ))}
                  </div>
                ) : null}
              </>
            )}
          </section>
        </main>
      </div>
    </AuthGuard>
  );
}

function AuthorizationSectionIntro({
  eyebrow,
  title,
  description,
}: {
  eyebrow: string;
  title: string;
  description: string;
}) {
  return (
    <div>
      <span className="label-text">{eyebrow}</span>
      <h2 className="mt-2 text-xl font-heading font-bold text-primary">{title}</h2>
      <p className="mt-2 max-w-3xl text-sm text-muted">{description}</p>
    </div>
  );
}
