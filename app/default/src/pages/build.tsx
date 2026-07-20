import { useEffect, useState } from "react";
import { Link } from "@tanstack/react-router";
import { Badge } from "@/components/Badge";
import Container from "@/components/Container";
import IntegrationIcon from "@/components/IntegrationIcon";
import { Link as UiLink } from "@/components/Link";
import { RadioGroup, RadioGroupItem } from "@/components/RadioGroup";
import { Eyebrow } from "@/components/ui/eyebrow";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import { SegmentedControl } from "@/components/ui/segmented-control";
import ShikiCode from "@/components/ShikiCode";
import TokenCreateForm from "@/components/TokenCreateForm";
import {
  TimelineSteps,
  TimelineStepsConnector,
  TimelineStepsContent,
  TimelineStepsDescription,
  TimelineStepsHeader,
  TimelineStepsIcon,
  TimelineStepsItem,
  TimelineStepsTitle,
  type TimelineStepsStatus,
} from "@/components/TimelineSteps";
import { CheckIcon, ClaudeIcon, CodexIcon, CopyIcon, CursorIcon } from "@/components/icons";
import { useDocumentTitle } from "@/hooks/use-document-title";
import {
  getIntegrations,
  getTokens,
  startIntegrationOAuth,
  type Integration,
} from "@/lib/api";
import {
  BUILD_STEPS,
  STARTER_AHAS,
  connectedAppIds,
  selectActiveStarter,
  type BuildStep,
  type BuildStepId,
  type BuildWorkspaceSnapshot,
  type StarterAha,
} from "@/lib/buildPaths";
import { cn } from "@/lib/cn";
import {
  BUILD_PATH,
  CONNECTION_RETURN_PATH_STORAGE_KEY,
  DOCS_PATH,
} from "@/lib/constants";

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
  // User can expand a completed step to change app / review (ux-flow-reviewer).
  const [focusedStepId, setFocusedStepId] = useState<BuildStepId | null>(null);

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

  async function refreshTokens() {
    try {
      const tokens = await getTokens();
      setSnapshot((prev) => ({ ...prev, tokens: tokens.length }));
      setError(null);
    } catch (err) {
      setError(errorMessage(err));
    }
  }

  const activeStarter =
    STARTER_AHAS.find((aha) => aha.id === selectedStarterId) ?? STARTER_AHAS[0];
  const connected = connectedAppIds(snapshot.integrations);
  const firstIncomplete = BUILD_STEPS.findIndex(
    (step) => !(loaded && step.isComplete(snapshot)),
  );
  // While the workspace snapshot loads, keep step 1 current so the timeline
  // never renders as three dead upcoming headers (screenshot 3.39.31).
  // When the journey is complete, keep the last step expanded so the aha
  // guide + congrats stay visible.
  const naturalCurrentId: BuildStepId =
    !loaded
      ? "connect"
      : firstIncomplete >= 0
        ? BUILD_STEPS[firstIncomplete]!.id
        : BUILD_STEPS[BUILD_STEPS.length - 1]!.id;
  const expandedStepId = focusedStepId ?? naturalCurrentId;

  return (
    <Container as="main" className="py-12">
      {/* Page-local reading column: keep Nav on content width; clamp this
          journey so the timeline + 3-up cards do not stretch to 80rem. */}
      <div className="w-full max-w-4xl">
        <PageHeader className="animate-fade-in-up">
          <PageHeaderContent>
            <div className="flex flex-col gap-3">
              <Eyebrow>Get started</Eyebrow>
              <PageHeaderTitle size="lg">Build</PageHeaderTitle>
            </div>
            <PageHeaderDescription className="max-w-2xl">
              Connect an app, grant access, and make your first call in this
              workspace. Start with Google Calendar, or use Slack or Notion if
              you already live there.
            </PageHeaderDescription>
          </PageHeaderContent>
        </PageHeader>

        {error && (
          <p className="mt-8 text-sm text-ember-500">{error}</p>
        )}

        <TimelineSteps
          className="mt-10 animate-fade-in-up [animation-delay:60ms]"
          aria-busy={!loaded}
        >
          {BUILD_STEPS.map((step, index) => {
            const done = loaded && step.isComplete(snapshot);
            const isExpanded = step.id === expandedStepId;
            const isNaturalCurrent =
              !done && step.id === naturalCurrentId;
            const status = stepStatus(
              done,
              isNaturalCurrent || (!done && isExpanded),
            );
            const isLast = index === BUILD_STEPS.length - 1;

            return (
              <BuildTimelineStep
                key={step.id}
                step={step}
                index={index}
                status={status}
                done={done}
                expanded={isExpanded}
                revisiting={focusedStepId === step.id && done}
                isLast={isLast}
                loaded={loaded}
                connected={connected}
                integrations={snapshot.integrations}
                selectedStarterId={selectedStarterId}
                onSelectStarter={setSelectedStarterId}
                activeStarter={activeStarter}
                onRefreshTokens={refreshTokens}
                onFocusStep={(id) => setFocusedStepId(id)}
                onClearFocus={() => setFocusedStepId(null)}
              />
            );
          })}
        </TimelineSteps>

        <p className="mt-10 text-sm text-muted animate-fade-in-up [animation-delay:120ms]">
          Using the CLI?{" "}
          <UiLink asChild className="text-sm">
            <Link to={`${DOCS_PATH}/getting-started`}>
              Open Getting Started
            </Link>
          </UiLink>
        </p>
      </div>
    </Container>
  );
}

function stepStatus(
  done: boolean,
  isCurrent: boolean,
): TimelineStepsStatus {
  if (done) return "completed";
  if (isCurrent) return "current";
  return "upcoming";
}

function BuildTimelineStep({
  step,
  index,
  status,
  done,
  expanded,
  revisiting,
  isLast,
  loaded,
  connected,
  integrations,
  selectedStarterId,
  onSelectStarter,
  activeStarter,
  onRefreshTokens,
  onFocusStep,
  onClearFocus,
}: {
  step: BuildStep;
  index: number;
  status: TimelineStepsStatus;
  done: boolean;
  expanded: boolean;
  revisiting: boolean;
  isLast: boolean;
  loaded: boolean;
  connected: Set<string>;
  integrations: Integration[];
  selectedStarterId: StarterAha["id"];
  onSelectStarter: (id: StarterAha["id"]) => void;
  activeStarter: StarterAha;
  onRefreshTokens: () => void | Promise<void>;
  onFocusStep: (id: BuildStepId) => void;
  onClearFocus: () => void;
}) {
  // Focus + progress (RES-20260717-005): all labels visible; expand one body.
  // Completed steps stay revisitable via the header (ux-flow-reviewer).
  const iconVariant =
    status === "completed"
      ? "primary"
      : status === "current" || (expanded && !done)
        ? "outline"
        : "default";
  const selectedConnected = connected.has(activeStarter.appId);
  const showFullBody = expanded && (status === "current" || done || !loaded);

  return (
    <TimelineStepsItem
      status={status}
      className={status === "upcoming" && !expanded ? "opacity-100" : undefined}
    >
      {isLast ? null : <TimelineStepsConnector status={status} />}
      <TimelineStepsHeader>
        <TimelineStepsIcon variant={iconVariant}>
          {done ? (
            <CheckIcon className="h-4 w-4" />
          ) : (
            <span className="text-sm font-medium">{index + 1}</span>
          )}
        </TimelineStepsIcon>
        {done ? (
          <button
            type="button"
            onClick={() =>
              expanded ? onClearFocus() : onFocusStep(step.id)
            }
            className="text-left"
            aria-expanded={expanded}
          >
            <span className="font-heading text-base leading-none tracking-tight text-primary">
              {step.title}
            </span>
          </button>
        ) : (
          <TimelineStepsTitle
            className={
              status === "upcoming" && !expanded ? "text-muted" : undefined
            }
          >
            {step.title}
          </TimelineStepsTitle>
        )}
      </TimelineStepsHeader>

      {done && !expanded ? (
        <TimelineStepsContent>
          <TimelineStepsDescription>
            {completedSummary(step.id, activeStarter)}
          </TimelineStepsDescription>
          <UiLink asChild className="mt-1 inline-flex w-fit self-start text-sm">
            <button type="button" onClick={() => onFocusStep(step.id)}>
              {step.id === "connect" ? "Change app" : "Review"}
            </button>
          </UiLink>
        </TimelineStepsContent>
      ) : null}

      {showFullBody ? (
        <TimelineStepsContent className="gap-3">
          {!loaded && step.id === "connect" ? (
            <>
              <TimelineStepsDescription>
                Checking this workspace…
              </TimelineStepsDescription>
              <StarterSuggestions
                connected={connected}
                integrations={integrations}
                selectedId={selectedStarterId}
                onSelect={onSelectStarter}
                catalogReady={false}
              />
            </>
          ) : (
            <>
              <TimelineStepsDescription>
                {step.description}
              </TimelineStepsDescription>

              {step.id === "connect" && (
                <StarterSuggestions
                  connected={connected}
                  integrations={integrations}
                  selectedId={selectedStarterId}
                  onSelect={onSelectStarter}
                  catalogReady={loaded}
                />
              )}

              {step.id === "connect" ? (
                <ConnectStepActions
                  starter={activeStarter}
                  alreadyConnected={selectedConnected}
                />
              ) : null}

              {step.id === "authorize" ? (
                <AuthorizeStepActions onCreated={onRefreshTokens} />
              ) : null}

              {step.id === "invoke" ? (
                <InvokeStepActions starter={activeStarter} />
              ) : null}

              {revisiting ? (
                <UiLink asChild className="inline-flex w-fit self-start text-sm">
                  <button type="button" onClick={onClearFocus}>
                    Back to current step
                  </button>
                </UiLink>
              ) : null}
            </>
          )}
        </TimelineStepsContent>
      ) : null}
    </TimelineStepsItem>
  );
}

function completedSummary(
  stepId: BuildStep["id"],
  starter: StarterAha,
): string {
  switch (stepId) {
    case "connect":
      return `${starter.label} is connected.`;
    case "authorize":
      return "API token ready.";
    case "invoke":
      return "First-call recipe ready.";
  }
}

function AuthorizeStepActions({
  onCreated,
}: {
  onCreated: () => void | Promise<void>;
}) {
  return (
    <div className="space-y-4">
      <div className="rounded-xl border border-alpha bg-base-100 p-4 dark:bg-surface">
        <TokenCreateForm onCreated={onCreated} />
      </div>
      <Link
        to="/settings"
        hash="authorization"
        className="text-sm text-muted underline-offset-2 transition-colors duration-150 hover:text-primary hover:underline"
      >
        Manage all tokens
      </Link>
    </div>
  );
}

function InvokeStepActions({ starter }: { starter: StarterAha }) {
  const origin =
    typeof window !== "undefined" ? window.location.origin : "https://your-gestalt-host";
  const [mcpClient, setMcpClient] = useState<"cursor" | "claude" | "codex">(
    "cursor",
  );

  const cursorConfig = `{
  "mcpServers": {
    "gestalt": {
      "url": "${origin}/mcp",
      "headers": {
        "Authorization": "Bearer gst_api_YOUR_TOKEN"
      }
    }
  }
}`;

  const claudeConfig = `{
  "mcpServers": {
    "gestalt": {
      "type": "http",
      "url": "${origin}/mcp",
      "headers": {
        "Authorization": "Bearer gst_api_YOUR_TOKEN"
      }
    }
  }
}`;

  const codexCommand = `export GESTALT_API_KEY=gst_api_YOUR_TOKEN
codex mcp add gestalt --url "${origin}/mcp" --bearer-token-env-var GESTALT_API_KEY`;

  return (
    <div className="space-y-5">
      <div className="space-y-1">
        <p className="text-sm font-medium text-primary">
          {starter.label}: {starter.ahaTitle}
        </p>
        <p className="text-sm text-muted">{starter.ahaDescription}</p>
      </div>

      <ol className="list-decimal space-y-4 ps-5 text-sm text-muted">
        <li className="space-y-2">
          <p>
            <span className="font-medium text-primary">Add Gestalt as an MCP server</span>{" "}
            in your AI client. Paste the token you just created in place of{" "}
            <code className="font-mono text-xs text-primary">gst_api_YOUR_TOKEN</code>.
          </p>
          <SegmentedControl
            label="AI client"
            size="sm"
            showLabels
            value={mcpClient}
            onValueChange={setMcpClient}
            options={[
              { value: "cursor", label: "Cursor", icon: CursorIcon },
              { value: "claude", label: "Claude Code", icon: ClaudeIcon },
              { value: "codex", label: "Codex", icon: CodexIcon },
            ]}
          />
          {mcpClient === "cursor" ? (
            <div className="space-y-2">
              <p>
                Create{" "}
                <code className="font-mono text-xs text-primary">
                  .cursor/mcp.json
                </code>{" "}
                in your project (or{" "}
                <code className="font-mono text-xs text-primary">
                  ~/.cursor/mcp.json
                </code>{" "}
                globally), then reload MCP / restart Cursor.
              </p>
              <RecipeCodeBlock code={cursorConfig} language="json" />
            </div>
          ) : null}
          {mcpClient === "claude" ? (
            <div className="space-y-2">
              <p>
                Add to{" "}
                <code className="font-mono text-xs text-primary">.mcp.json</code>{" "}
                (project) or{" "}
                <code className="font-mono text-xs text-primary">
                  ~/.claude.json
                </code>
                , or run the CLI add command from{" "}
                <UiLink asChild className="text-sm">
                  <Link to={`${DOCS_PATH}/mcp`}>Use with MCP</Link>
                </UiLink>
                .
              </p>
              <RecipeCodeBlock code={claudeConfig} language="json" />
            </div>
          ) : null}
          {mcpClient === "codex" ? (
            <div className="space-y-2">
              <p>
                Set your token in the environment, then register this workspace
                with the Codex CLI:
              </p>
              <RecipeCodeBlock code={codexCommand} language="shellscript" />
            </div>
          ) : null}
          <p>
            Endpoint for this workspace:{" "}
            <code className="font-mono text-xs text-primary">{`${origin}/mcp`}</code>
          </p>
        </li>

        <li className="space-y-2">
          <p>
            <span className="font-medium text-primary">
              Paste this prompt
            </span>{" "}
            in that client and send it. You should get a real{" "}
            {starter.label} result back.
          </p>
          <RecipeCodeBlock code={starter.llmPrompt} language="text" />
        </li>
      </ol>

      <div
        role="note"
        className="space-y-3 rounded-lg border border-alpha bg-alpha-5 px-4 py-4 dark:bg-surface-raised"
      >
        <p className="text-sm font-heading text-primary">
          Congratulations — you&apos;re ready to build
        </p>
        <p className="text-sm text-muted">
          You should see something like this:
        </p>
        <RecipeCodeBlock code={starter.expectedResult} language="text" />
        <p className="text-sm text-muted">What&apos;s next</p>
        <div className="flex flex-wrap gap-4">
          <UiLink asChild className="text-sm">
            <Link to="/apps">Browse apps</Link>
          </UiLink>
          <UiLink asChild className="text-sm">
            <Link to="/identities">Agent identities</Link>
          </UiLink>
          <UiLink asChild className="text-sm">
            <Link to={`${DOCS_PATH}/getting-started`}>Read the docs</Link>
          </UiLink>
        </div>
      </div>

      <details className="group rounded-lg border border-alpha bg-alpha-5 open:pb-3 dark:bg-surface-raised">
        <summary className="cursor-pointer list-none px-4 py-3 text-sm font-medium text-primary marker:content-none [&::-webkit-details-marker]:hidden">
          Prefer the CLI instead
        </summary>
        <div className="space-y-3 border-t border-alpha px-4 pt-3">
          <p className="text-sm text-muted">
            Same aha via{" "}
            <code className="font-mono text-xs">gestalt apps invoke</code> — see{" "}
            <UiLink asChild className="text-sm">
              <Link to={`${DOCS_PATH}/invoke`}>CLI invoke docs</Link>
            </UiLink>
            .
          </p>
          <RecipeCodeBlock code={starter.invokeRecipe} language="shellscript" />
        </div>
      </details>

      <UiLink asChild className="inline-flex w-fit text-sm">
        <Link to={`${DOCS_PATH}/mcp`}>Full MCP setup</Link>
      </UiLink>
    </div>
  );
}

function ConnectStepActions({
  starter,
  alreadyConnected,
}: {
  starter: StarterAha;
  alreadyConnected: boolean;
}) {
  const [connecting, setConnecting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function handleConnect() {
    setConnecting(true);
    setError(null);
    try {
      window.sessionStorage.setItem(
        CONNECTION_RETURN_PATH_STORAGE_KEY,
        BUILD_PATH,
      );
      const { url } = await startIntegrationOAuth(
        starter.appId,
        undefined,
        undefined,
        undefined,
        undefined,
        BUILD_PATH,
      );
      window.location.href = url;
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to start connect");
      setConnecting(false);
    }
  }

  return (
    <div className="flex flex-col items-start gap-2">
      {alreadyConnected ? (
        <p className="text-sm text-muted">
          {starter.label} is connected — continue to grant access below.
        </p>
      ) : (
        <button
          type="button"
          onClick={handleConnect}
          disabled={connecting}
          className="inline-flex w-fit rounded-md bg-base-950 px-5 py-2 text-sm font-medium text-white transition-all duration-150 hover:bg-base-900 disabled:cursor-not-allowed disabled:opacity-60 dark:bg-base-100 dark:text-base-950 dark:hover:bg-base-200"
        >
          {connecting ? "Connecting…" : `Connect ${starter.label}`}
        </button>
      )}
      <Link
        to="/apps"
        className="text-sm text-muted underline-offset-2 transition-colors duration-150 hover:text-primary hover:underline"
      >
        See all apps
      </Link>
      {error && <p className="text-sm text-ember-500">{error}</p>}
    </div>
  );
}

function StarterSuggestions({
  connected,
  integrations,
  selectedId,
  onSelect,
  catalogReady,
}: {
  connected: Set<string>;
  integrations: Integration[];
  selectedId: StarterAha["id"];
  onSelect: (id: StarterAha["id"]) => void;
  catalogReady: boolean;
}) {
  // Composition only (ReUI / Registry ChoiceCardsGrid): RadioGroup owns
  // exclusive selection; the label is the choice-card chrome. Icons come from
  // the same Integration.iconSvg source as the Apps catalog — wait for that
  // fetch so we do not flash DefaultIcon placeholders on first paint.
  if (!catalogReady) {
    return (
      <div
        className="grid grid-cols-1 gap-3 sm:grid-cols-3"
        aria-busy="true"
        aria-label="Loading starter apps"
      >
        {STARTER_AHAS.map((aha) => (
          <div
            key={aha.id}
            className="flex h-full items-start gap-3 rounded-lg border border-alpha bg-base-100 p-4 pe-10 dark:bg-surface"
          >
            <div className="h-12 w-12 shrink-0 animate-pulse rounded-lg bg-alpha-10" />
            <div className="flex min-w-0 flex-1 flex-col gap-2">
              <div className="h-4 w-24 animate-pulse rounded bg-alpha-10" />
              <div className="h-4 w-full animate-pulse rounded bg-alpha-10" />
            </div>
          </div>
        ))}
      </div>
    );
  }

  const iconsByAppId = new Map(
    integrations.map((integration) => [integration.name, integration.iconSvg]),
  );

  return (
    <RadioGroup
      value={selectedId}
      onValueChange={(value) => onSelect(value as StarterAha["id"])}
      className="grid grid-cols-1 gap-3 sm:grid-cols-3"
      aria-label="Starter apps"
    >
      {STARTER_AHAS.map((aha) => {
        const isConnected = connected.has(aha.appId);
        const inputId = `starter-${aha.id}`;
        return (
          <label
            key={aha.id}
            htmlFor={inputId}
            className={cn(
              "relative flex h-full cursor-pointer items-start gap-3 rounded-lg border border-alpha bg-base-100 p-4 pe-10 transition-colors duration-150 dark:bg-surface",
              "hover:border-alpha-strong",
              "has-[[data-state=checked]]:border-base-950 has-[[data-state=checked]]:bg-alpha-5",
              "dark:has-[[data-state=checked]]:border-base-100",
            )}
          >
            <RadioGroupItem
              value={aha.id}
              id={inputId}
              className="absolute end-3 top-3"
            />
            <IntegrationIcon
              iconSvg={iconsByAppId.get(aha.appId)}
              size="lg"
              className="bg-background dark:bg-background"
            />
            <span className="flex min-w-0 flex-1 flex-col gap-1">
              <span className="flex flex-wrap items-center gap-2">
                <span className="text-sm font-medium text-primary">
                  {aha.label}
                </span>
                {isConnected ? (
                  <Badge variant="success" size="sm">
                    Connected
                  </Badge>
                ) : null}
              </span>
              <span className="text-sm text-muted">{aha.ahaTitle}</span>
            </span>
          </label>
        );
      })}
    </RadioGroup>
  );
}

function RecipeCodeBlock({
  code,
  language = "shellscript",
}: {
  code: string;
  language?: "shellscript" | "json" | "text";
}) {
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
        <ShikiCode language={language} text={code} />
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
