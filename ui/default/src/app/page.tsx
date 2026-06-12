"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  getAuthInfo,
  getAgentSessions,
  getIntegrations,
  getTokens,
  getWorkflowRuns,
  isAPIErrorStatus,
} from "@/lib/api";
import Nav from "@/components/Nav";
import AuthGuard from "@/components/AuthGuard";
import Container from "@/components/Container";

export default function DashboardPage() {
  const [data, setData] = useState<{
    integrations: number | null;
    tokens: number | null;
    workflowResources: number | null;
    agentAvailable: boolean;
    agentSessions: number | null;
    error: string | null;
  }>({
    integrations: null,
    tokens: null,
    workflowResources: null,
    agentAvailable: false,
    agentSessions: null,
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
        const agentFeature = authInfo.features?.agent;
        return Promise.allSettled([
          getIntegrations(),
          getTokens(),
          getWorkflowRuns(),
          agentFeature === false
            ? Promise.resolve(null)
            : getAgentSessions({ view: "summary", limit: 50 }),
        ]).then(([
          integrationsResult,
          tokensResult,
          workflowRunsResult,
          agentSessionsResult,
        ]) => {
          if (!active) return;

          const error =
            integrationsResult.status === "rejected"
              ? errorMessage(integrationsResult.reason)
              : tokensResult.status === "rejected"
                ? errorMessage(tokensResult.reason)
                : null;

          const workflowResources =
            workflowRunsResult.status === "fulfilled"
              ? workflowRunsResult.value.length
              : null;
          const agentAvailable =
            typeof agentFeature === "boolean"
              ? agentFeature
              : !(
                  agentSessionsResult.status === "rejected" &&
                  isAPIErrorStatus(agentSessionsResult.reason, 412)
                );

          setData({
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
            agentSessions:
              agentAvailable &&
              agentSessionsResult.status === "fulfilled" &&
              agentSessionsResult.value
                ? agentSessionsResult.value.length
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
        <Container as="main" className="py-12">
          <div className="animate-fade-in-up">
            <span className="label-text">Overview</span>
            <h1 className="mt-2 text-2xl font-heading font-bold text-primary">
              Dashboard
            </h1>
            <p className="mt-2 text-sm text-muted">
              Manage the client-facing app workspace from one place.
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
              <p className="mt-3 text-3xl font-heading font-bold text-primary">
                {data.tokens ?? "--"}
              </p>
              <p className="mt-3 text-sm text-muted group-hover:text-primary transition-colors duration-150">
                Manage API tokens
                <span className="inline-block ml-1 transition-transform duration-150 group-hover:translate-x-0.5">
                  &rarr;
                </span>
              </p>
            </Link>
            <Link
              href="/apps"
              className="group rounded-lg border border-alpha bg-base-100 p-8 transition-all duration-150 hover:border-alpha-strong hover:shadow-card dark:bg-surface"
            >
              <span className="label-text">Apps</span>
              <p className="mt-3 text-3xl font-heading font-bold text-primary">
                {data.integrations ?? "--"}
              </p>
              <p className="mt-3 text-sm text-muted group-hover:text-primary transition-colors duration-150">
                Manage apps
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
                Inspect workflow runs
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
                  {data.agentSessions ?? "--"}
                </p>
                <p className="mt-3 text-sm text-muted group-hover:text-primary transition-colors duration-150">
                  View agent sessions
                  <span className="inline-block ml-1 transition-transform duration-150 group-hover:translate-x-0.5">
                    &rarr;
                  </span>
                </p>
              </Link>
            )}
          </div>
        </Container>
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
