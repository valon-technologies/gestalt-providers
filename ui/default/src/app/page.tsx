"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  getAuthInfo,
  getAgentRuns,
  getIntegrations,
  getManagedIdentities,
  getTokens,
  getWorkflowEventTriggers,
  getWorkflowRuns,
  getWorkflowSchedules,
  isAPIErrorStatus,
} from "@/lib/api";
import Nav from "@/components/Nav";
import AuthGuard from "@/components/AuthGuard";

export default function DashboardPage() {
  const [data, setData] = useState<{
    identitiesAvailable: boolean;
    identities: number | null;
    integrations: number | null;
    tokens: number | null;
    workflowResources: number | null;
    agentAvailable: boolean;
    agentRuns: number | null;
    error: string | null;
  }>({
    identitiesAvailable: false,
    identities: null,
    integrations: null,
    tokens: null,
    workflowResources: null,
    agentAvailable: false,
    agentRuns: null,
    error: null,
  });

  useEffect(() => {
    let active = true;

    getAuthInfo()
      .catch(() => ({
        provider: "unknown",
        displayName: "Unknown",
        loginSupported: true,
        features: undefined,
      }))
      .then((authInfo) => {
        if (!active) return;
        const identitiesAvailable = authInfo.provider !== "none";
        const agentFeature = authInfo.features?.agent;
        return Promise.allSettled([
          identitiesAvailable ? getManagedIdentities() : Promise.resolve(null),
          getIntegrations(),
          getTokens(),
          getWorkflowSchedules(),
          getWorkflowEventTriggers(),
          getWorkflowRuns(),
          agentFeature === false ? Promise.resolve(null) : getAgentRuns(),
        ]).then(([
          identitiesResult,
          integrationsResult,
          tokensResult,
          workflowSchedulesResult,
          workflowTriggersResult,
          workflowRunsResult,
          agentRunsResult,
        ]) => {
          if (!active) return;

          const error =
            identitiesAvailable && identitiesResult.status === "rejected"
              ? errorMessage(identitiesResult.reason)
              : integrationsResult.status === "rejected"
                ? errorMessage(integrationsResult.reason)
                : tokensResult.status === "rejected"
                  ? errorMessage(tokensResult.reason)
                  : null;

          const workflowResources =
            workflowSchedulesResult.status === "fulfilled" &&
            workflowTriggersResult.status === "fulfilled" &&
            workflowRunsResult.status === "fulfilled"
              ? workflowSchedulesResult.value.length +
                workflowTriggersResult.value.length +
                workflowRunsResult.value.length
              : null;
          const agentAvailable =
            typeof agentFeature === "boolean"
              ? agentFeature
              : !(
                  agentRunsResult.status === "rejected" &&
                  isAPIErrorStatus(agentRunsResult.reason, 412)
                );

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
            workflowResources,
            agentAvailable,
            agentRuns:
              agentAvailable &&
              agentRunsResult.status === "fulfilled" &&
              agentRunsResult.value
                ? agentRunsResult.value.length
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

          <div className="mt-10 grid grid-cols-1 gap-5 sm:grid-cols-2 xl:grid-cols-4 animate-fade-in-up [animation-delay:60ms]">
            <Link
              href="/authorization"
              className="group rounded-lg border border-alpha bg-base-100 p-8 transition-all duration-150 hover:border-alpha-strong hover:shadow-card dark:bg-surface"
            >
              <span className="label-text">Authorization</span>
              <div className="mt-4 grid grid-cols-2 gap-5">
                <div>
                  <p className="text-3xl font-heading font-bold text-primary">
                    {data.tokens ?? "--"}
                  </p>
                  <p className="mt-2 text-xs uppercase tracking-[0.14em] text-faint">
                    Tokens
                  </p>
                </div>
                <div>
                  <p className="text-3xl font-heading font-bold text-primary">
                    {data.identitiesAvailable ? (data.identities ?? "--") : "Off"}
                  </p>
                  <p className="mt-2 text-xs uppercase tracking-[0.14em] text-faint">
                    {data.identitiesAvailable ? "Identities" : "Auth Disabled"}
                  </p>
                </div>
              </div>
              <p className="mt-4 text-sm text-muted group-hover:text-primary transition-colors duration-150">
                Manage personal access and shared automation
                <span className="inline-block ml-1 transition-transform duration-150 group-hover:translate-x-0.5">
                  &rarr;
                </span>
              </p>
            </Link>
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
                {data.workflowResources ?? "--"}
              </p>
              <p className="mt-3 text-sm text-muted group-hover:text-primary transition-colors duration-150">
                Manage schedules, triggers, and runs
                <span className="inline-block ml-1 transition-transform duration-150 group-hover:translate-x-0.5">
                  &rarr;
                </span>
              </p>
            </Link>
            {data.agentAvailable && (
              <Link
                href="/agents"
                className="group rounded-lg border border-alpha bg-base-100 p-8 transition-all duration-150 hover:border-alpha-strong hover:shadow-card dark:bg-surface"
              >
                <span className="label-text">Agents</span>
                <p className="mt-3 text-3xl font-heading font-bold text-primary">
                  {data.agentRuns ?? "--"}
                </p>
                <p className="mt-3 text-sm text-muted group-hover:text-primary transition-colors duration-150">
                  Start and inspect agent runs
                  <span className="inline-block ml-1 transition-transform duration-150 group-hover:translate-x-0.5">
                    &rarr;
                  </span>
                </p>
              </Link>
            )}
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
