"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  getAuthInfo,
  getIntegrations,
  getManagedIdentities,
  getTokens,
  getWorkflowRuns,
} from "@/lib/api";
import Nav from "@/components/Nav";
import AuthGuard from "@/components/AuthGuard";

export default function DashboardPage() {
  const [data, setData] = useState<{
    identitiesAvailable: boolean;
    identities: number | null;
    integrations: number | null;
    tokens: number | null;
    workflowRuns: number | null;
    error: string | null;
  }>({
    identitiesAvailable: false,
    identities: null,
    integrations: null,
    tokens: null,
    workflowRuns: null,
    error: null,
  });

  useEffect(() => {
    let active = true;

    getAuthInfo()
      .catch(() => ({
        provider: "unknown",
        displayName: "Unknown",
        loginSupported: true,
      }))
      .then((authInfo) => {
        if (!active) return;
        const identitiesAvailable = authInfo.provider !== "none";
        return Promise.allSettled([
          identitiesAvailable ? getManagedIdentities() : Promise.resolve(null),
          getIntegrations(),
          getTokens(),
          getWorkflowRuns(),
        ]).then(([identitiesResult, integrationsResult, tokensResult, workflowRunsResult]) => {
          if (!active) return;

          const error =
            identitiesAvailable && identitiesResult.status === "rejected"
              ? errorMessage(identitiesResult.reason)
              : integrationsResult.status === "rejected"
                ? errorMessage(integrationsResult.reason)
                : tokensResult.status === "rejected"
                  ? errorMessage(tokensResult.reason)
                  : workflowRunsResult.status === "rejected"
                    ? errorMessage(workflowRunsResult.reason)
                  : null;

          setData({
            identitiesAvailable,
            identities:
              identitiesAvailable && identitiesResult.status === "fulfilled"
                ? identitiesResult.value?.length ?? null
                : null,
            integrations:
              integrationsResult.status === "fulfilled"
                ? integrationsResult.value.length
                : null,
            tokens:
              tokensResult.status === "fulfilled"
                ? tokensResult.value.length
                : null,
            workflowRuns:
              workflowRunsResult.status === "fulfilled"
                ? workflowRunsResult.value.length
                : null,
            error,
          });
        });
      });

    return () => {
      active = false;
    };
  }, []);

  return (
    <AuthGuard>
      <div className="min-h-screen">
        <Nav />
        <main className="mx-auto max-w-5xl px-6 py-12">
          <div className="animate-fade-in-up">
            <span className="label-text">Overview</span>
            <h1 className="mt-2 text-2xl font-heading font-bold text-primary">
              Dashboard
            </h1>
            <p className="mt-2 text-sm text-muted">
              Manage the client-facing plugin workspace from one place.
            </p>
          </div>

          {data.error && (
            <p className="mt-8 text-sm text-ember-500">{data.error}</p>
          )}

          <div className="mt-10 grid grid-cols-1 gap-5 sm:grid-cols-2 xl:grid-cols-3 animate-fade-in-up [animation-delay:60ms]">
            {data.identitiesAvailable ? (
              <Link
                href="/identities"
                className="group rounded-lg border border-alpha bg-base-100 p-8 transition-all duration-150 hover:border-alpha-strong hover:shadow-card dark:bg-surface"
              >
                <span className="label-text">Identities</span>
                <p className="mt-3 text-3xl font-heading font-bold text-primary">
                  {data.identities ?? "--"}
                </p>
                <p className="mt-3 text-sm text-muted group-hover:text-primary transition-colors duration-150">
                  Manage identities
                  <span className="inline-block ml-1 transition-transform duration-150 group-hover:translate-x-0.5">
                    &rarr;
                  </span>
                </p>
              </Link>
            ) : null}
            <Link
              href="/integrations"
              className="group rounded-lg border border-alpha bg-base-100 p-8 transition-all duration-150 hover:border-alpha-strong hover:shadow-card dark:bg-surface"
            >
              <span className="label-text">Plugins</span>
              <p className="mt-3 text-3xl font-heading font-bold text-primary">
                {data.integrations ?? "--"}
              </p>
              <p className="mt-3 text-sm text-muted group-hover:text-primary transition-colors duration-150">
                Manage plugins
                <span className="inline-block ml-1 transition-transform duration-150 group-hover:translate-x-0.5">
                  &rarr;
                </span>
              </p>
            </Link>
            <Link
              href="/workflows"
              className="group rounded-lg border border-alpha bg-base-100 p-8 transition-all duration-150 hover:border-alpha-strong hover:shadow-card dark:bg-surface"
            >
              <span className="label-text">Workflows</span>
              <p className="mt-3 text-3xl font-heading font-bold text-primary">
                {data.workflowRuns ?? "--"}
              </p>
              <p className="mt-3 text-sm text-muted group-hover:text-primary transition-colors duration-150">
                Manage schedules, triggers, and runs
                <span className="inline-block ml-1 transition-transform duration-150 group-hover:translate-x-0.5">
                  &rarr;
                </span>
              </p>
            </Link>
            <Link
              href="/tokens"
              className="group rounded-lg border border-alpha bg-base-100 p-8 transition-all duration-150 hover:border-alpha-strong hover:shadow-card dark:bg-surface"
            >
              <span className="label-text">API Tokens</span>
              <p className="mt-3 text-3xl font-heading font-bold text-primary">
                {data.tokens ?? "--"}
              </p>
              <p className="mt-3 text-sm text-muted group-hover:text-primary transition-colors duration-150">
                Manage tokens
                <span className="inline-block ml-1 transition-transform duration-150 group-hover:translate-x-0.5">
                  &rarr;
                </span>
              </p>
            </Link>
          </div>
        </main>
      </div>
    </AuthGuard>
  );
}

function errorMessage(reason: unknown): string {
  if (reason instanceof Error) {
    return reason.message;
  }
  if (typeof reason === "string") {
    return reason;
  }
  return "Failed to load";
}
