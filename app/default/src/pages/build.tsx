import { useEffect, useState } from "react";
import { Link } from "@tanstack/react-router";
import AuthGuard from "@/components/AuthGuard";
import Container from "@/components/Container";
import Nav from "@/components/Nav";
import ShikiCode from "@/components/ShikiCode";
import { CheckIcon, CopyIcon } from "@/components/icons";
import { useDocumentTitle } from "@/hooks/use-document-title";
import { getIntegrations, getTokens } from "@/lib/api";
import {
  BUILD_STEPS,
  STARTER_AHAS,
  connectedAppIds,
  isBuildComplete,
  selectActiveStarter,
  type BuildWorkspaceSnapshot,
  type StarterAha,
} from "@/lib/buildPaths";
import { DOCS_PATH } from "@/lib/constants";

export default function BuildPage() {
  useDocumentTitle("Build");

  const [snapshot, setSnapshot] = useState<BuildWorkspaceSnapshot>({
    integrations: [],
    tokens: 0,
  });
  const [error, setError] = useState<string | null>(null);
  const [loaded, setLoaded] = useState(false);
  const [selectedStarterId, setSelectedStarterId] = useState<StarterAha["id"]>(
    "google_calendar",
  );

  useEffect(() => {
    let active = true;

    Promise.allSettled([getIntegrations(), getTokens()]).then(
      ([integrationsResult, tokensResult]) => {
        if (!active) return;

        const integrations =
          integrationsResult.status === "fulfilled"
            ? integrationsResult.value
            : [];
        const tokens =
          tokensResult.status === "fulfilled" ? tokensResult.value.length : 0;

        const next: BuildWorkspaceSnapshot = { integrations, tokens };
        setSnapshot(next);
        setSelectedStarterId(selectActiveStarter(integrations).id);
        setError(
          integrationsResult.status === "rejected"
            ? errorMessage(integrationsResult.reason)
            : tokensResult.status === "rejected"
              ? errorMessage(tokensResult.reason)
              : null,
        );
        setLoaded(true);
      },
    );

    return () => {
      active = false;
    };
  }, []);

  const activeStarter =
    STARTER_AHAS.find((aha) => aha.id === selectedStarterId) ?? STARTER_AHAS[0];
  const connected = connectedAppIds(snapshot.integrations);
  const complete = loaded && isBuildComplete(snapshot);

  return (
    <AuthGuard>
      <div className="min-h-screen">
        <Nav />
        <Container as="main" className="py-12">
          <div className="animate-fade-in-up">
            <span className="label-text">Get started</span>
            <h1 className="mt-2 text-2xl font-heading text-primary">Build</h1>
            <p className="mt-2 max-w-2xl text-sm text-muted">
              Connect an app, grant access, and make your first call in this
              workspace. Start with Google Calendar, or use Slack or Notion if
              you already live there.
            </p>
          </div>

          {error && (
            <p className="mt-8 text-sm text-ember-500">{error}</p>
          )}

          <ol className="mt-10 space-y-5 animate-fade-in-up [animation-delay:60ms]">
            {BUILD_STEPS.map((step, index) => {
              const done = loaded && step.isComplete(snapshot);
              return (
                <li
                  key={step.id}
                  className="rounded-lg border border-alpha bg-base-100 p-6 dark:bg-surface"
                >
                  <div className="flex items-start gap-4">
                    <span
                      className={`flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-sm font-medium ${
                        done
                          ? "bg-grove-600 text-white dark:bg-grove-500"
                          : "bg-alpha-10 text-muted"
                      }`}
                      aria-hidden
                    >
                      {done ? <CheckIcon className="h-4 w-4" /> : index + 1}
                    </span>
                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                        <h2 className="text-lg font-heading text-primary">
                          {step.title}
                        </h2>
                        {done && (
                          <span className="text-xs font-medium uppercase tracking-[0.14em] text-grove-700 dark:text-grove-200">
                            Done
                          </span>
                        )}
                      </div>
                      <p className="mt-2 text-sm text-muted">
                        {step.description}
                      </p>

                      {step.id === "connect" && (
                        <StarterSuggestions
                          connected={connected}
                          selectedId={selectedStarterId}
                          onSelect={setSelectedStarterId}
                        />
                      )}

                      {step.id === "invoke" && (
                        <div className="mt-5 space-y-3">
                          <p className="text-sm font-medium text-primary">
                            {activeStarter.label}: {activeStarter.ahaTitle}
                          </p>
                          <p className="text-sm text-muted">
                            {activeStarter.ahaDescription}
                          </p>
                          <RecipeCodeBlock code={activeStarter.invokeRecipe} />
                          <div className="flex flex-wrap gap-3 text-sm">
                            {STARTER_AHAS.filter(
                              (aha) => aha.id !== activeStarter.id,
                            ).map((aha) => (
                              <button
                                key={aha.id}
                                type="button"
                                onClick={() => setSelectedStarterId(aha.id)}
                                className="text-muted underline-offset-2 transition-colors duration-150 hover:text-primary hover:underline"
                              >
                                {aha.label} recipe
                              </button>
                            ))}
                          </div>
                        </div>
                      )}

                      <div className="mt-5">
                        <Link
                          to={step.to}
                          className="inline-flex rounded-md bg-base-950 px-5 py-2 text-sm font-medium text-white transition-all duration-150 hover:bg-base-900 dark:bg-base-100 dark:text-base-950 dark:hover:bg-base-200"
                        >
                          {step.ctaLabel}
                          <span className="ml-1.5" aria-hidden>
                            &rarr;
                          </span>
                        </Link>
                      </div>
                    </div>
                  </div>
                </li>
              );
            })}
          </ol>

          {complete && (
            <div className="mt-8 rounded-lg border border-grove-600/30 bg-grove-600/5 p-6 animate-fade-in-up dark:border-grove-400/20 dark:bg-grove-500/10">
              <h2 className="text-lg font-heading text-primary">
                You&apos;re ready to build
              </h2>
              <p className="mt-2 text-sm text-muted">
                Keep exploring apps, refine authorization, or dig into the full
                docs when you need CLI detail.
              </p>
              <div className="mt-4 flex flex-wrap gap-4 text-sm">
                <Link to="/apps" className="text-primary hover:underline">
                  Apps
                </Link>
                <Link
                  to="/authorization"
                  className="text-primary hover:underline"
                >
                  Authorization
                </Link>
                <Link
                  to={`${DOCS_PATH}/getting-started`}
                  className="text-primary hover:underline"
                >
                  Docs
                </Link>
                <Link
                  to={`${DOCS_PATH}/invoke`}
                  className="text-primary hover:underline"
                >
                  Invoke
                </Link>
              </div>
            </div>
          )}

          <p className="mt-10 text-sm text-muted animate-fade-in-up [animation-delay:120ms]">
            Using the CLI?{" "}
            <Link
              to={`${DOCS_PATH}/getting-started`}
              className="text-primary hover:underline"
            >
              Open Getting Started
            </Link>
          </p>
        </Container>
      </div>
    </AuthGuard>
  );
}

function StarterSuggestions({
  connected,
  selectedId,
  onSelect,
}: {
  connected: Set<string>;
  selectedId: StarterAha["id"];
  onSelect: (id: StarterAha["id"]) => void;
}) {
  return (
    <ul className="mt-5 grid gap-3 sm:grid-cols-3">
      {STARTER_AHAS.map((aha) => {
        const isConnected = connected.has(aha.appId);
        const isSelected = selectedId === aha.id;
        return (
          <li key={aha.id}>
            <button
              type="button"
              onClick={() => onSelect(aha.id)}
              className={`w-full rounded-md border px-4 py-3 text-left transition-colors duration-150 ${
                isSelected
                  ? "border-alpha-strong bg-alpha-5 text-primary"
                  : "border-alpha bg-transparent text-muted hover:border-alpha-strong hover:text-primary"
              }`}
            >
              <span className="block text-xs font-medium uppercase tracking-[0.14em] text-faint">
                {aha.priority === "primary" ? "Primary" : "Alternate"}
                {isConnected ? " · Connected" : ""}
              </span>
              <span className="mt-1 block text-sm font-medium text-primary">
                {aha.label}
              </span>
              <span className="mt-1 block text-sm text-muted">
                {aha.ahaTitle}
              </span>
            </button>
          </li>
        );
      })}
    </ul>
  );
}

function RecipeCodeBlock({ code }: { code: string }) {
  const [copied, setCopied] = useState(false);

  async function handleCopy() {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      /* ignore */
    }
  }

  return (
    <div className="group relative">
      <div className="doc-code">
        <ShikiCode language="shellscript" text={code} />
      </div>
      <button
        type="button"
        onClick={handleCopy}
        className="absolute right-3 top-3 rounded-md p-1.5 text-muted opacity-0 transition-all duration-150 hover:bg-alpha-5 hover:text-primary group-hover:opacity-100"
        title="Copy to clipboard"
        aria-label="Copy to clipboard"
      >
        {copied ? (
          <CheckIcon className="h-4 w-4 text-grove-600 dark:text-grove-200" />
        ) : (
          <CopyIcon className="h-4 w-4" />
        )}
      </button>
    </div>
  );
}

function errorMessage(reason: unknown): string {
  if (reason instanceof Error) return reason.message;
  if (typeof reason === "string") return reason;
  return "Failed to load workspace status";
}
