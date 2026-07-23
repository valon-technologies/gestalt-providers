import { useEffect, useState } from "react";
import {
  Link,
  Navigate,
  useNavigate,
  useParams,
} from "@tanstack/react-router";
import { Badge } from "@/components/Badge";
import Container from "@/components/Container";
import IntegrationCard from "@/components/IntegrationCard";
import IntegrationIcon from "@/components/IntegrationIcon";
import { Link as UiLink } from "@/components/Link";
import { RadioGroup, RadioGroupItem, choiceCardClassName } from "@/components/RadioGroup";
import { Eyebrow } from "@/components/ui/eyebrow";
import { Label } from "@/components/ui/label";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderDescription,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import { CodeBlock, LanguageTabsCodeBlock } from "@/components/ui/code-block";
import { Code } from "@/components/ui/code";
import { cardVariants } from "@/components/ui/card";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import {
  InputGroup,
  InputGroupAddon,
  InputGroupButton,
  InputGroupInput,
} from "@/components/ui/input-group";
import {
  Field,
  FieldDescription,
  FieldLabel,
} from "@/components/ui/field";
import {
  Stepper,
  StepperIndicator,
  StepperItem,
  StepperList,
  StepperSeparator,
  StepperTitle,
  StepperTrigger,
} from "@/components/ui/stepper";
import {
  AGENT_CONSOLE_THEME_CLAUDE,
  AgentConsole,
  AgentConsoleBody,
  AgentConsoleChrome,
  AgentConsoleCursor,
  AgentConsoleGlyph,
  AgentConsoleHeading,
  AgentConsoleHint,
  AgentConsoleIdentity,
  AgentConsoleInput,
  AgentConsoleMedia,
  AgentConsolePanel,
  AgentConsolePath,
  AgentConsoleProduct,
  AgentConsolePrompt,
  AgentConsoleSubtitle,
  AgentConsoleTrafficLights,
  AgentConsoleTyping,
  AgentConsoleWindowTitle,
  type AgentConsoleTheme,
} from "@/components/ui/agent-console";
import TokenCreateForm from "@/components/TokenCreateForm";
import {
  CheckIcon,
  ChevronDownIcon,
  ChevronLeftIcon,
  ChevronRightIcon,
  CopyIcon,
  EyeIcon,
  EyeOffIcon,
} from "@/components/icons";
import { useBuildSession } from "@/hooks/use-build-session";
import { useDocumentTitle } from "@/hooks/use-document-title";
import {
  useIntegrationsQuery,
  useInvalidateIntegrations,
  useInvalidateTokens,
  useTokensQuery,
} from "@/hooks/use-server-queries";
import {
  type APIToken,
  type Integration,
} from "@/lib/api";
import {
  BUILD_CREATE_NEW_TOKEN_ID,
  BUILD_EXEMPLARS,
  BUILD_STEPS,
  buildAuthorizeSelectionReady,
  companionAppLabel,
  connectedAppIds,
  DEFAULT_BUILD_TOKEN_NAME,
  firstIncompleteStepId,
  getExemplar,
  isBuildStepId,
  resolveExemplarOpenPath,
  type BuildExemplar,
  type BuildExemplarId,
  type BuildStep,
  type BuildStepId,
  type BuildWorkspaceSnapshot,
} from "@/lib/buildPaths";
import { cn } from "@/lib/cn";
import { BUILD_PATH, DOCS_PATH } from "@/lib/constants";

/** `/build` → first incomplete step. */
export function BuildIndexRedirect() {
  useDocumentTitle("Build");
  const session = useBuildSession();
  const integrationsQuery = useIntegrationsQuery();
  const tokensQuery = useTokensQuery();

  const tokensReady = !tokensQuery.isPending;
  const integrationsReady = !integrationsQuery.isPending;

  if (!tokensReady || !integrationsReady) {
    return (
      <Container as="main" className="py-12">
        <div className="mx-auto w-full max-w-4xl">
          <p className="text-sm text-faint">Loading Build…</p>
        </div>
      </Container>
    );
  }

  const snapshot: BuildWorkspaceSnapshot = {
    integrations: integrationsQuery.data ?? [],
    tokens: tokensQuery.data ?? [],
    activeExemplarId: session.activeExemplarId,
    mcpInstalled: session.mcpInstalled,
    apiToken: session.apiToken,
    tokenName: session.tokenName,
    selectedTokenId: session.selectedTokenId,
    introSeen: session.introSeen,
  };

  const stepId = firstIncompleteStepId(snapshot, (step) =>
    isStepDone(step, snapshot, tokensReady, integrationsReady),
  );

  return <Navigate to="/build/$stepId" params={{ stepId }} replace />;
}

export default function BuildStepPage() {
  const { stepId: rawStepId } = useParams({ strict: false }) as {
    stepId?: string;
  };
  const navigate = useNavigate();
  const session = useBuildSession();
  const integrationsQuery = useIntegrationsQuery();
  const tokensQuery = useTokensQuery();
  const invalidateTokens = useInvalidateTokens();
  const stepId = rawStepId && isBuildStepId(rawStepId) ? rawStepId : null;
  const currentStep = stepId
    ? BUILD_STEPS.find((s) => s.id === stepId)!
    : null;
  useDocumentTitle(
    currentStep ? `${currentStep.title} · Build` : "Build",
  );

  if (!stepId || !currentStep) {
    return <Navigate to="/build" replace />;
  }

  const tokensReady = !tokensQuery.isPending;
  const integrationsReady = !integrationsQuery.isPending;

  const snapshot: BuildWorkspaceSnapshot = {
    integrations: integrationsQuery.data ?? [],
    tokens: tokensQuery.data ?? [],
    activeExemplarId: session.activeExemplarId,
    mcpInstalled: session.mcpInstalled,
    apiToken: session.apiToken,
    tokenName: session.tokenName,
    selectedTokenId: session.selectedTokenId,
    introSeen: session.introSeen,
  };

  const error =
    integrationsQuery.error != null
      ? errorMessage(integrationsQuery.error)
      : tokensQuery.error != null
        ? errorMessage(tokensQuery.error)
        : null;

  const activeExemplar = getExemplar(session.activeExemplarId);
  const connected = connectedAppIds(snapshot.integrations);

  function goToStep(id: BuildStepId) {
    void navigate({ to: "/build/$stepId", params: { stepId: id } });
  }

  async function refreshTokens() {
    await invalidateTokens();
  }

  return (
    <Container as="main" className="py-12">
      <div className="mx-auto w-full max-w-4xl">
        {error && (
          <p className="mb-8 text-sm text-ember-500">{error}</p>
        )}

        <div
          data-testid="build-step-nav"
          className="animate-fade-in-up"
        >
          <Stepper
            value={stepId}
            onValueChange={(next) => {
              if (isBuildStepId(next)) goToStep(next);
            }}
            activationMode="jump"
          >
            <StepperList aria-label="Build steps">
              {BUILD_STEPS.map((step) => (
                <StepperItem
                  key={step.id}
                  value={step.id}
                  data-testid={`build-nav-${step.id}`}
                >
                  <StepperSeparator />
                  <StepperTrigger>
                    <StepperIndicator />
                    <StepperTitle>{step.title}</StepperTitle>
                  </StepperTrigger>
                </StepperItem>
              ))}
            </StepperList>
          </Stepper>
        </div>

        <PageHeader
          className={cn(
            "mt-10 animate-fade-in-up [animation-delay:40ms]",
            stepId === "authorize" && "hidden",
          )}
        >
          <PageHeaderContent size="lg">
            {currentStep.eyebrow ? (
              <Eyebrow>{currentStep.eyebrow}</Eyebrow>
            ) : null}
            <PageHeaderTitle>{currentStep.title}</PageHeaderTitle>
            <PageHeaderDescription>
              {currentStep.description}
            </PageHeaderDescription>
          </PageHeaderContent>
        </PageHeader>

        <BuildStepPanel
          step={currentStep}
          tokensReady={tokensReady}
          integrationsReady={integrationsReady}
          connected={connected}
          integrations={snapshot.integrations}
          tokens={snapshot.tokens}
          activeExemplar={activeExemplar}
          activeExemplarId={session.activeExemplarId}
          onSelectExemplar={session.setActiveExemplarId}
          apiToken={session.apiToken}
          onApiToken={session.setApiToken}
          tokenName={session.tokenName}
          onTokenName={session.setTokenName}
          selectedTokenId={session.selectedTokenId}
          onSelectedTokenId={session.setSelectedTokenId}
          onRefreshTokens={refreshTokens}
          onMarkMcpInstalled={session.markMcpInstalled}
          onMarkIntroSeen={session.markIntroSeen}
          onGoToStep={goToStep}
        />
      </div>
    </Container>
  );
}

function isStepDone(
  step: BuildStep,
  snapshot: BuildWorkspaceSnapshot,
  tokensReady: boolean,
  integrationsReady: boolean,
): boolean {
  if (!step.isComplete(snapshot)) return false;
  switch (step.id) {
    case "intro":
      return true;
    case "authorize":
      return buildAuthorizeSelectionReady(snapshot);
    case "install":
      return true;
    case "connect":
      return integrationsReady;
    case "invoke":
      return integrationsReady && buildAuthorizeSelectionReady(snapshot);
    default:
      return true;
  }
}

function gestaltMcpBaseUrl(): string {
  if (typeof window === "undefined") return "https://your-gestalt-host";
  const { origin, hostname } = window.location;
  if (hostname === "localhost" || hostname === "127.0.0.1") {
    return "https://valon.tools";
  }
  return origin;
}

function cursorMcpInstallHref(mcpUrl: string, apiToken: string): string {
  const config = {
    url: mcpUrl,
    headers: {
      Authorization: `Bearer ${apiToken}`,
    },
  };
  const json = JSON.stringify(config);
  const base64 = btoa(json);
  return `cursor://anysphere.cursor-deeplink/mcp/install?name=${encodeURIComponent("gestalt")}&config=${encodeURIComponent(base64)}`;
}

function ClaudePixelIcon({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 80 80"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className={className}
      aria-hidden
    >
      <path
        d="M70 33H80V47H70V60H65V74H60V60H55V74H50V60H30V74H25V60H20V74H15V60H10V47H0V33H10V6H70V33ZM20 19V33H25V19H20ZM55 33H60V19H55V33Z"
        fill="currentColor"
      />
    </svg>
  );
}

/**
 * Build-page skin ids — composition variants, not an AgentConsole prop.
 * Palettes match Registry `agent-console.stories` (THEME_CODEX / THEME_CURSOR).
 * Prefer promoting story themes to Registry exports if other apps need them.
 */
type BuildAgentSkin = "claude" | "codex" | "cursor";

const BUILD_AGENT_SKINS: BuildAgentSkin[] = ["claude", "codex", "cursor"];

const BUILD_AGENT_THEMES: Record<BuildAgentSkin, AgentConsoleTheme> = {
  claude: AGENT_CONSOLE_THEME_CLAUDE,
  // OpenAI Codex CLI — near-black charcoal, white caret, green prompt glyph.
  codex: {
    background: "#121212",
    accent: "#f0f0f0",
    traffic: "#3a3a3a",
    foreground: "#f8f8f8",
    muted: "rgba(255,255,255,0.45)",
    glyph: "#b0d8a8",
  },
  // Cursor Agent terminal — near-black charcoal, white caret.
  cursor: {
    background: "#141414",
    accent: "#f5f5f5",
    traffic: "#3a3a3a",
    foreground: "#e8e8e8",
    muted: "rgba(255,255,255,0.45)",
    glyph: "rgba(255,255,255,0.85)",
  },
};

function pickBuildAgentSkin(exclude?: BuildAgentSkin): BuildAgentSkin {
  const options =
    exclude == null
      ? BUILD_AGENT_SKINS
      : BUILD_AGENT_SKINS.filter((id) => id !== exclude);
  return options[Math.floor(Math.random() * options.length)]!;
}

/** Codex CLI status-panel highlights — Registry AgentConsole Codex story. */
const CODEX_HL = {
  model: "#90b8b0",
  command: "#a890b8",
  path: "#f0e0b8",
  permission: "#a890b8",
} as const;

function BuildAgentConsolePreview({
  variant,
  prompt,
  reply,
  cwd,
}: {
  variant: BuildAgentSkin;
  prompt: string;
  /** When set, sequence: type prompt → think → type reply. */
  reply?: string;
  cwd: string;
}) {
  const theme = BUILD_AGENT_THEMES[variant];
  const [phase, setPhase] = useState<"prompt" | "thinking" | "reply">(
    reply ? "prompt" : "prompt",
  );

  // Reset the turn when the exemplar prompt/reply changes.
  useEffect(() => {
    setPhase("prompt");
  }, [prompt, reply]);

  useEffect(() => {
    if (!reply || phase !== "thinking") return;
    const timer = window.setTimeout(() => setPhase("reply"), 1600);
    return () => window.clearTimeout(timer);
  }, [phase, reply]);

  const promptComplete = !reply || phase === "thinking" || phase === "reply";

  const promptLine = (
    <AgentConsolePrompt
      className={
        variant === "codex"
          ? reply
            ? "mt-0 -mx-1 border-transparent bg-white/[0.06] px-2 py-1.5"
            : "mt-auto -mx-1 border-transparent bg-white/[0.06] px-2 py-1.5"
          : variant === "cursor"
            ? "border-transparent py-1"
            : undefined
      }
    >
      <AgentConsoleGlyph>
        {variant === "cursor" ? "→" : "❯"}
      </AgentConsoleGlyph>
      <AgentConsoleInput measureText={prompt}>
        {reply ? (
          <>
            {promptComplete ? (
              <span className="whitespace-pre-wrap">{prompt}</span>
            ) : (
              <AgentConsoleTyping
                text={prompt}
                onComplete={() => setPhase("thinking")}
              />
            )}
            {!promptComplete ? <AgentConsoleCursor /> : null}
          </>
        ) : (
          <>
            <AgentConsoleTyping text={prompt} />
            <AgentConsoleCursor />
          </>
        )}
      </AgentConsoleInput>
    </AgentConsolePrompt>
  );

  const replyBlock =
    reply && phase === "thinking" ? (
      <AgentConsoleHint
        className="motion-safe:animate-pulse"
        data-testid="build-agent-thinking"
      >
        Thinking…
      </AgentConsoleHint>
    ) : reply && phase === "reply" ? (
      <AgentConsolePanel
        className="whitespace-pre-wrap text-[length:inherit] leading-relaxed"
        data-testid="build-agent-reply"
      >
        <AgentConsoleTyping text={reply} delayMs={120} />
      </AgentConsolePanel>
    ) : null;

  if (variant === "codex") {
    return (
      <AgentConsole theme={theme} className="h-full w-full max-w-full">
        <AgentConsoleChrome>
          <AgentConsoleTrafficLights />
          <AgentConsoleWindowTitle>codex</AgentConsoleWindowTitle>
        </AgentConsoleChrome>
        <AgentConsoleBody className="min-h-0 flex-1 gap-4">
          <AgentConsolePanel className="space-y-0.5">
            <p className="text-[var(--agent-console-fg)]">
              <span className="text-[var(--agent-console-muted)]">{">_ "}</span>
              OpenAI Codex
            </p>
            <p>
              <span className="text-[var(--agent-console-muted)]">model: </span>
              <span style={{ color: CODEX_HL.model }}>gpt-5.6-sol max</span>{" "}
              <span style={{ color: CODEX_HL.command }}>/model</span>{" "}
              <span className="text-[var(--agent-console-muted)]">
                to change
              </span>
            </p>
            <p>
              <span className="text-[var(--agent-console-muted)]">
                directory:{" "}
              </span>
              <span style={{ color: CODEX_HL.path }}>{cwd}</span>
            </p>
            <p>
              <span className="text-[var(--agent-console-muted)]">
                permissions:{" "}
              </span>
              <span style={{ color: CODEX_HL.permission }}>YOLO mode</span>
            </p>
          </AgentConsolePanel>
          {promptLine}
          {replyBlock}
        </AgentConsoleBody>
      </AgentConsole>
    );
  }

  if (variant === "cursor") {
    return (
      <AgentConsole theme={theme} className="h-full w-full max-w-full">
        <AgentConsoleChrome>
          <AgentConsoleTrafficLights />
          <AgentConsoleWindowTitle>cursor</AgentConsoleWindowTitle>
        </AgentConsoleChrome>
        <AgentConsoleBody className="min-h-0 flex-1">
          <AgentConsoleIdentity className="gap-0">
            <AgentConsoleHeading>
              <AgentConsoleProduct>Cursor Agent</AgentConsoleProduct>
              <AgentConsoleSubtitle className="text-[var(--agent-console-muted)]">
                Building with Gestalt
              </AgentConsoleSubtitle>
            </AgentConsoleHeading>
          </AgentConsoleIdentity>
          <div
            className={
              reply ? "flex flex-col gap-3" : "mt-auto flex flex-col gap-3"
            }
          >
            {promptLine}
            {replyBlock}
            <AgentConsoleHint className="text-[var(--agent-console-fg)]">
              {cwd} · origin/main
            </AgentConsoleHint>
          </div>
        </AgentConsoleBody>
      </AgentConsole>
    );
  }

  return (
    <AgentConsole theme={theme} className="h-full w-full max-w-full">
      <AgentConsoleChrome>
        <AgentConsoleTrafficLights />
        <AgentConsoleWindowTitle>claude</AgentConsoleWindowTitle>
      </AgentConsoleChrome>
      <AgentConsoleBody className="min-h-0 flex-1">
        <AgentConsoleIdentity>
          <AgentConsoleMedia>
            <ClaudePixelIcon className="size-16" />
          </AgentConsoleMedia>
          <AgentConsoleHeading>
            <AgentConsoleProduct>Claude Code</AgentConsoleProduct>
            <AgentConsoleSubtitle>Building with Gestalt</AgentConsoleSubtitle>
            <AgentConsolePath>{cwd}</AgentConsolePath>
          </AgentConsoleHeading>
        </AgentConsoleIdentity>
        <div
          className={
            reply ? "flex flex-col gap-3" : "mt-auto flex flex-col gap-3"
          }
        >
          {promptLine}
          {replyBlock}
          {!reply ? (
            <AgentConsoleHint>? for shortcuts</AgentConsoleHint>
          ) : null}
        </div>
      </AgentConsoleBody>
    </AgentConsole>
  );
}

/** Registry ChoiceCards — vertical rail (features-14 spine); use {@link choiceCardClassName} only. */

function IntroStepActions({
  activeExemplarId,
  onSelectExemplar,
  onMarkIntroSeen,
  onGoToStep,
}: {
  activeExemplarId: BuildExemplarId;
  onSelectExemplar: (id: BuildExemplarId) => void;
  onMarkIntroSeen: () => void;
  onGoToStep: (id: BuildStepId) => void;
}) {
  const exemplar = getExemplar(activeExemplarId);
  const [agentSkin, setAgentSkin] = useState<BuildAgentSkin>("claude");
  const cwd = `~/${exemplar.department.toLowerCase().replace(/\s+/g, "-")}`;

  function handleSelectExemplar(id: BuildExemplarId) {
    onSelectExemplar(id);
    setAgentSkin((current) => pickBuildAgentSkin(current));
  }

  function handleContinue() {
    onMarkIntroSeen();
    onGoToStep("authorize");
  }

  return (
    <div className="space-y-6" data-testid="build-intro">
      {/*
        features-14 spine: ~⅓ pick rail + ~⅔ preview panel.
        Selection = RadioGroup ChoiceCards (pick-one outcomes); panel = AgentConsole slot.
      */}
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-[minmax(0,1fr)_minmax(0,2fr)] lg:items-stretch lg:gap-8">
        <div data-testid="build-outcome-toggle" className="min-w-0">
          <RadioGroup
            value={activeExemplarId}
            onValueChange={(value) =>
              handleSelectExemplar(value as BuildExemplarId)
            }
            className="grid grid-cols-1 gap-2"
            aria-label="What to build"
          >
            {BUILD_EXEMPLARS.map((item) => {
              const inputId = `build-outcome-${item.id}`;
              return (
                <Label
                  key={item.id}
                  htmlFor={inputId}
                  className={cn(choiceCardClassName)}
                  data-testid={`build-outcome-card-${item.id}`}
                >
                  <RadioGroupItem
                    value={item.id}
                    id={inputId}
                    className="absolute end-3 top-3"
                    aria-label={item.outcomeTitle}
                  />
                  <Eyebrow
                    data-testid={
                      item.id === activeExemplarId
                        ? "build-outcome-department"
                        : undefined
                    }
                  >
                    {item.department}
                  </Eyebrow>
                  <span
                    data-choice-title
                    className="text-base font-medium text-foreground"
                  >
                    {item.outcomeTitle}
                  </span>
                </Label>
              );
            })}
          </RadioGroup>
        </div>

        <div
          data-testid="build-agent-console"
          className="flex min-h-0 min-w-0"
        >
          <BuildAgentConsolePreview
            key={`${exemplar.id}-${agentSkin}`}
            variant={agentSkin}
            prompt={exemplar.llmPrompt}
            cwd={cwd}
          />
        </div>
      </div>

      <nav
        aria-label="Continue"
        className="mt-2 flex justify-end border-t border-alpha pt-6"
      >
        <button
          type="button"
          data-testid="build-intro-continue"
          onClick={handleContinue}
          className="group flex w-fit max-w-xs flex-col gap-1 rounded-xl bg-neutral-hover px-5 py-5 text-left transition-[background-color] duration-hover-out ease-out-quart hover:bg-neutral-dark-hover hover:duration-hover-in active:bg-neutral-dark-pressed focus-ring sm:items-end sm:text-right"
        >
          <span className="text-2xs font-medium uppercase tracking-[0.08em] text-muted-foreground">
            Next
          </span>
          <span className="mt-1 flex items-baseline gap-1.5 font-heading text-xl font-normal leading-tight text-foreground sm:flex-row-reverse">
            <ChevronRightIcon
              tight
              strokeWidth={1.5}
              className="size-[1ex] shrink-0 text-muted-foreground transition-colors duration-hover-out group-hover:text-foreground"
            />
            {BUILD_STEPS.find((step) => step.id === "authorize")?.title}
          </span>
        </button>
      </nav>
    </div>
  );
}

function McpInstallPanel({
  apiToken,
  onMarkMcpInstalled,
}: {
  apiToken: string;
  onMarkMcpInstalled: () => void;
}) {
  const mcpBase = gestaltMcpBaseUrl();
  const mcpUrl = `${mcpBase}/mcp`;
  const tokenForSnippets = apiToken || "gst_api_YOUR_TOKEN";
  const hasToken = apiToken.length > 0;
  const cursorInstallHref = hasToken
    ? cursorMcpInstallHref(mcpUrl, apiToken)
    : null;

  const cursorConfig = `{
  "mcpServers": {
    "gestalt": {
      "url": "${mcpUrl}",
      "headers": {
        "Authorization": "Bearer ${tokenForSnippets}"
      }
    }
  }
}`;

  const claudeCommand = `claude mcp add --transport http --scope project \\
  --header "Authorization: Bearer ${tokenForSnippets}" \\
  gestalt "${mcpUrl}"`;

  const codexCommand = `export GESTALT_API_KEY=${tokenForSnippets}
codex mcp add gestalt --url "${mcpUrl}" --bearer-token-env-var GESTALT_API_KEY`;

  return (
    <div className="space-y-4">
      <p className="text-sm text-muted-foreground">
        Install Gestalt in your AI client. See{" "}
        <UiLink asChild className="text-sm">
          <Link to={`${DOCS_PATH}/mcp`}>Use with MCP</Link>
        </UiLink>{" "}
        for full setup notes.
      </p>

      <div className="space-y-3">
        <p className="text-sm text-muted-foreground">
          In Cursor, one-click install adds this workspace as an MCP server
          using your API token.
        </p>
        {cursorInstallHref ? (
          <Button asChild>
            <a
              href={cursorInstallHref}
              data-testid="build-add-to-cursor"
              onClick={() => onMarkMcpInstalled()}
            >
              Add to Cursor
            </a>
          </Button>
        ) : (
          <Button type="button" disabled data-testid="build-add-to-cursor">
            Add to Cursor
          </Button>
        )}
        <details
          className="group rounded-md border border-alpha bg-base-white dark:bg-surface"
          data-testid="build-cursor-manual-config"
        >
          <summary className="flex cursor-pointer list-none items-center gap-2 px-3 py-2 text-sm font-medium text-foreground marker:content-none [&::-webkit-details-marker]:hidden">
            <ChevronDownIcon className="size-4 shrink-0 text-muted-foreground transition-transform duration-hover-out ease-out-quart group-open:rotate-180" />
            <span>
              Or paste into{" "}
              <code className="font-mono text-xs">.cursor/mcp.json</code>
            </span>
          </summary>
          <div className="space-y-2 border-t border-alpha px-3 py-3">
            <p className="text-sm text-muted-foreground">
              Skip one-click and add this MCP server block to your project’s
              Cursor config, then reload MCP in Cursor.
            </p>
            <CodeBlock
              code={cursorConfig}
              language="json"
              filename=".cursor/mcp.json"
            />
          </div>
        </details>
      </div>

      {/* Registry LanguageTabsCodeBlock — Tabs over syntax fence; do not fork. */}
      <div className="space-y-2" data-testid="build-mcp-install-tabs">
        <p className="text-sm text-muted-foreground">
          Or copy a snippet for Claude Code or Codex:
        </p>
        <LanguageTabsCodeBlock
          tabs={[
            {
              id: "claude",
              label: "Claude Code",
              filename: "Terminal",
              language: "bash",
              code: claudeCommand,
            },
            {
              id: "codex",
              label: "Codex",
              filename: "Terminal",
              language: "bash",
              code: codexCommand,
            },
          ]}
        />
      </div>
    </div>
  );
}

function BuildStepPanel({
  step,
  tokensReady,
  integrationsReady,
  connected,
  integrations,
  tokens,
  activeExemplar,
  activeExemplarId,
  onSelectExemplar,
  apiToken,
  onApiToken,
  tokenName,
  onTokenName,
  selectedTokenId,
  onSelectedTokenId,
  onRefreshTokens,
  onMarkMcpInstalled,
  onMarkIntroSeen,
  onGoToStep,
}: {
  step: BuildStep;
  tokensReady: boolean;
  integrationsReady: boolean;
  connected: Set<string>;
  integrations: Integration[];
  tokens: APIToken[];
  activeExemplar: BuildExemplar;
  activeExemplarId: BuildExemplarId;
  onSelectExemplar: (id: BuildExemplarId) => void;
  apiToken: string;
  onApiToken: (token: string) => void;
  tokenName: string;
  onTokenName: (name: string) => void;
  selectedTokenId: string;
  onSelectedTokenId: (id: string) => void;
  onRefreshTokens: () => void | Promise<void>;
  onMarkMcpInstalled: () => void;
  onMarkIntroSeen: () => void;
  onGoToStep: (id: BuildStepId) => void;
}) {
  const authorizeReady = buildAuthorizeSelectionReady({
    apiToken,
    selectedTokenId,
    tokens,
  });

  return (
    <section
      data-testid="build-step-panel"
      className="mt-10 space-y-3 animate-fade-in-up [animation-delay:60ms]"
      aria-busy={
        (step.id === "authorize" && !tokensReady) ||
        (step.id === "connect" && !integrationsReady) ||
        (step.id === "invoke" && (!tokensReady || !integrationsReady))
      }
    >
      {step.id === "intro" ? (
        <IntroStepActions
          activeExemplarId={activeExemplarId}
          onSelectExemplar={onSelectExemplar}
          onMarkIntroSeen={onMarkIntroSeen}
          onGoToStep={onGoToStep}
        />
      ) : null}

      {step.id === "authorize" ? (
        <AuthorizeStepActions
          title={step.title}
          description={step.description}
          tokens={tokens}
          tokensLoaded={tokensReady}
          integrations={integrations}
          apiToken={apiToken}
          onApiToken={onApiToken}
          tokenName={tokenName}
          onTokenName={onTokenName}
          selectedTokenId={selectedTokenId}
          onSelectedTokenId={onSelectedTokenId}
          onTokensChanged={onRefreshTokens}
        />
      ) : null}

      {step.id === "install" ? (
        <InstallStepActions
          apiToken={apiToken}
          onApiToken={onApiToken}
          onMarkMcpInstalled={onMarkMcpInstalled}
        />
      ) : null}

      {step.id === "connect" ? (
        <ConnectStepActions
          exemplar={activeExemplar}
          integrations={integrations}
          connected={connected}
          catalogReady={integrationsReady}
        />
      ) : null}

      {step.id === "invoke" ? (
        <InvokeStepActions
          exemplar={activeExemplar}
          integrations={integrations}
        />
      ) : null}

      {step.id !== "intro" ? (
        <BuildStepPager
          stepId={step.id}
          onGoToStep={(id) => {
            if (step.id === "install") {
              const from = BUILD_STEPS.findIndex((s) => s.id === step.id);
              const to = BUILD_STEPS.findIndex((s) => s.id === id);
              if (to > from) onMarkMcpInstalled();
            }
            onGoToStep(id);
          }}
          nextDisabled={
            (step.id === "authorize" && !authorizeReady) ||
            (step.id === "connect" &&
              activeExemplar.companionAppIds.some(
                (appId) => !connected.has(appId),
              ))
          }
          nextDisabledTitle={
            step.id === "authorize"
              ? "Pick an existing token or create a new one before continuing"
              : step.id === "connect"
                ? "Connect every required app before continuing"
                : undefined
          }
        />
      ) : null}
    </section>
  );
}

function BuildStepPager({
  stepId,
  onGoToStep,
  nextDisabled = false,
  nextDisabledTitle,
}: {
  stepId: BuildStepId;
  onGoToStep: (id: BuildStepId) => void;
  /** When true, the Next control is shown but not actionable. */
  nextDisabled?: boolean;
  nextDisabledTitle?: string;
}) {
  const index = BUILD_STEPS.findIndex((step) => step.id === stepId);
  const prev = index > 0 ? BUILD_STEPS[index - 1] : null;
  const next =
    index >= 0 && index < BUILD_STEPS.length - 1
      ? BUILD_STEPS[index + 1]
      : null;
  if (!prev && !next) return null;

  const cardClass =
    // Registry Card solid (bg-secondary ≈ neutral-hover) + Neutral dark hover/press.
    "group flex w-fit max-w-xs flex-col gap-1 rounded-xl bg-neutral-hover px-5 py-5 text-left transition-[background-color] duration-hover-out ease-out-quart hover:bg-neutral-dark-hover hover:duration-hover-in active:bg-neutral-dark-pressed focus-ring disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:bg-neutral-hover disabled:active:bg-neutral-hover";

  return (
    <nav
      aria-label="Build step navigation"
      data-testid="build-step-pager"
      className="mt-8 flex flex-wrap items-stretch justify-between gap-3 border-t border-alpha pt-6"
    >
      {prev ? (
        <button
          type="button"
          data-testid="build-step-prev"
          onClick={() => onGoToStep(prev.id)}
          className={cardClass}
        >
          <span className="text-2xs font-medium uppercase tracking-[0.08em] text-muted-foreground">
            Previous
          </span>
          <span className="mt-1 flex items-baseline gap-1.5 font-heading text-xl font-normal leading-tight text-foreground">
            <ChevronLeftIcon
              tight
              strokeWidth={1.5}
              className="size-[1ex] shrink-0 text-muted-foreground transition-colors duration-hover-out group-hover:text-foreground"
            />
            {prev.title}
          </span>
        </button>
      ) : (
        <span className="hidden sm:block" aria-hidden />
      )}
      {next ? (
        <button
          type="button"
          data-testid="build-step-next"
          onClick={() => onGoToStep(next.id)}
          disabled={nextDisabled}
          aria-disabled={nextDisabled}
          title={nextDisabled ? nextDisabledTitle : undefined}
          className={cn(cardClass, "ms-auto items-end text-right")}
        >
          <span className="text-2xs font-medium uppercase tracking-[0.08em] text-muted-foreground">
            Next
          </span>
          <span className="mt-1 flex items-baseline gap-1.5 font-heading text-xl font-normal leading-tight text-foreground flex-row-reverse">
            <ChevronRightIcon
              tight
              strokeWidth={1.5}
              className="size-[1ex] shrink-0 text-muted-foreground transition-colors duration-hover-out group-hover:text-foreground"
            />
            {next.title}
          </span>
        </button>
      ) : null}
    </nav>
  );
}

function tokenChoiceTitle(token: APIToken): string {
  const name = token.name?.trim();
  if (name && name !== token.id) return name;
  return token.id;
}

function scopeAppId(scope: string): string {
  const [appId] = scope.split(":");
  return appId.trim();
}

function appScopeDisplayName(
  appId: string,
  integrations: Integration[],
): string {
  const match = integrations.find((item) => item.name === appId);
  if (match?.displayName?.trim()) return match.displayName.trim();
  return companionAppLabel(appId);
}

function tokenScopeLabel(token: APIToken, integrations: Integration[]): string {
  if (!token.scopes?.length) return "Scope: all";
  const labels = [
    ...new Set(
      token.scopes.map((scope) =>
        appScopeDisplayName(scopeAppId(scope), integrations),
      ),
    ),
  ];
  return `Scope: ${labels.join(", ")}`;
}

function AuthorizeStepActions({
  title,
  description,
  tokens,
  tokensLoaded,
  integrations,
  apiToken,
  onApiToken,
  tokenName,
  onTokenName,
  selectedTokenId,
  onSelectedTokenId,
  onTokensChanged,
}: {
  title: string;
  description: string;
  tokens: APIToken[];
  tokensLoaded: boolean;
  integrations: Integration[];
  apiToken: string;
  onApiToken: (token: string) => void;
  tokenName: string;
  onTokenName: (name: string) => void;
  selectedTokenId: string;
  onSelectedTokenId: (id: string) => void;
  onTokensChanged: () => void | Promise<void>;
}) {
  const hasTokens = tokens.length > 0;
  const createSelected =
    !hasTokens || selectedTokenId === BUILD_CREATE_NEW_TOKEN_ID;

  // When the account has no tokens yet, treat authorize as the create path.
  const radioValue = hasTokens
    ? selectedTokenId || undefined
    : BUILD_CREATE_NEW_TOKEN_ID;

  async function handleTokenCreated(
    plaintext: string,
    created: { id: string; name: string },
  ) {
    onApiToken(plaintext);
    onTokenName(created.name);
    onSelectedTokenId(created.id);
    await onTokensChanged();
  }

  function selectExistingToken(token: APIToken) {
    onSelectedTokenId(token.id);
    onTokenName(tokenChoiceTitle(token));
  }

  function selectCreateNew() {
    const wasCreating =
      selectedTokenId === BUILD_CREATE_NEW_TOKEN_ID || !hasTokens;
    onSelectedTokenId(BUILD_CREATE_NEW_TOKEN_ID);
    if (!wasCreating || !tokenName.trim()) {
      onTokenName(DEFAULT_BUILD_TOKEN_NAME);
    }
  }

  return (
    <div className="grid grid-cols-1 gap-8 lg:grid-cols-[minmax(0,1fr)_min(100%,360px)] lg:items-start">
      <div className="min-w-0 space-y-4">
        <PageHeader>
          <PageHeaderContent size="lg">
            <PageHeaderTitle>{title}</PageHeaderTitle>
            <PageHeaderDescription>{description}</PageHeaderDescription>
          </PageHeaderContent>
        </PageHeader>

        {!tokensLoaded ? (
          <p className="text-sm text-faint">Loading tokens…</p>
        ) : null}

        {tokensLoaded && createSelected ? (
          <div className="space-y-2">
            {hasTokens ? null : (
              <p className="text-sm text-muted-foreground">
                Name this token for the rest of Build — you can change it before
                creating.
              </p>
            )}
            <TokenCreateForm
              name={tokenName}
              onNameChange={onTokenName}
              defaultName={DEFAULT_BUILD_TOKEN_NAME}
              onCreated={handleTokenCreated}
            />
          </div>
        ) : null}
      </div>

      {tokensLoaded && hasTokens ? (
        <div className="w-full max-w-[360px] space-y-2 lg:justify-self-end">
          <RadioGroup
            value={radioValue}
            onValueChange={(value) => {
              if (value === BUILD_CREATE_NEW_TOKEN_ID) {
                selectCreateNew();
                return;
              }
              const token = tokens.find((item) => item.id === value);
              if (token) selectExistingToken(token);
            }}
            className="gap-2"
            data-testid="build-token-radio"
            aria-label="Choose an API token"
          >
            {tokens.map((token) => {
              const inputId = `build-token-${token.id}`;
              const tokenTitle = tokenChoiceTitle(token);
              const showMonospace = tokenTitle === token.id;
              const created = token.createdAt
                ? new Date(token.createdAt).toLocaleDateString()
                : null;
              return (
                <Label
                  key={token.id}
                  htmlFor={inputId}
                  className={cn(choiceCardClassName)}
                >
                  <RadioGroupItem
                    value={token.id}
                    id={inputId}
                    className="absolute end-3 top-3"
                    aria-label={tokenTitle}
                  />
                  <span
                    data-choice-title
                    className={cn(
                      "text-sm font-medium text-foreground",
                      showMonospace && "truncate font-mono font-normal",
                    )}
                    title={showMonospace ? tokenTitle : undefined}
                  >
                    {tokenTitle}
                  </span>
                  <span
                    data-choice-desc
                    className="text-sm font-normal text-muted-foreground"
                  >
                    {tokenScopeLabel(token, integrations)}
                    {created ? ` · Created ${created}` : null}
                  </span>
                </Label>
              );
            })}

            <Label
              htmlFor="build-token-create"
              className={cn(choiceCardClassName)}
            >
              <RadioGroupItem
                value={BUILD_CREATE_NEW_TOKEN_ID}
                id="build-token-create"
                className="absolute end-3 top-3"
                aria-label="Create a new token"
              />
              <span
                data-choice-title
                className="text-sm font-medium text-foreground"
              >
                Create a new token
              </span>
              <span
                data-choice-desc
                className="text-sm font-normal text-muted-foreground"
              >
                Name it for this Build path — the secret is only shown once.
              </span>
            </Label>
          </RadioGroup>

          <UiLink asChild className="text-sm">
            <Link to="/settings" hash="authorization">
              Manage tokens
            </Link>
          </UiLink>
        </div>
      ) : null}
    </div>
  );
}

function InstallStepActions({
  apiToken,
  onApiToken,
  onMarkMcpInstalled,
}: {
  apiToken: string;
  onApiToken: (token: string) => void;
  onMarkMcpInstalled: () => void;
}) {
  const [secretVisible, setSecretVisible] = useState(false);
  const [secretCopied, setSecretCopied] = useState(false);

  return (
    <div className="space-y-4">
      <Field className="max-w-xl">
        <FieldLabel htmlFor="build-api-token">
          {apiToken
            ? "API token secret"
            : "Paste an API token secret"}
        </FieldLabel>
        <InputGroup>
          <InputGroupInput
            id="build-api-token"
            type={secretVisible ? "text" : "password"}
            autoComplete="off"
            spellCheck={false}
            placeholder="gst_api_…"
            value={apiToken}
            onChange={(event) => onApiToken(event.target.value.trim())}
            className="font-mono text-sm"
          />
          <InputGroupAddon align="inline-end">
            <InputGroupButton
              size="icon-xs"
              aria-label={secretVisible ? "Hide token" : "Show token"}
              title={secretVisible ? "Hide" : "Show"}
              aria-pressed={secretVisible}
              onClick={() => setSecretVisible((prev) => !prev)}
            >
              {secretVisible ? (
                <EyeOffIcon className="size-3.5" />
              ) : (
                <EyeIcon className="size-3.5" />
              )}
            </InputGroupButton>
            <InputGroupButton
              size="icon-xs"
              aria-label={secretCopied ? "Copied" : "Copy token"}
              title={secretCopied ? "Copied" : "Copy"}
              disabled={!apiToken}
              onClick={() => {
                if (!apiToken) return;
                void navigator.clipboard.writeText(apiToken).then(() => {
                  setSecretCopied(true);
                  window.setTimeout(() => setSecretCopied(false), 2000);
                });
              }}
            >
              {secretCopied ? (
                <CheckIcon className="size-3.5" />
              ) : (
                <CopyIcon className="size-3.5" />
              )}
            </InputGroupButton>
          </InputGroupAddon>
        </InputGroup>
        <FieldDescription>
          {apiToken
            ? "Token ready — install Gestalt below, or paste a different secret."
            : "Secrets aren't shown again after create. Paste a token secret to unlock one-click install."}
        </FieldDescription>
      </Field>

      <McpInstallPanel
        apiToken={apiToken}
        onMarkMcpInstalled={onMarkMcpInstalled}
      />
    </div>
  );
}

function InvokeStepActions({
  exemplar,
  integrations,
}: {
  exemplar: BuildExemplar;
  integrations: Integration[];
}) {
  const integration = integrations.find((item) => item.name === exemplar.id);
  const open = resolveExemplarOpenPath(exemplar, integration);
  const displayName = integration?.displayName?.trim() || exemplar.label;
  const invokeOp = `${exemplar.invokeAppId}.${exemplar.operationId}`;
  const [agentSkin] = useState<BuildAgentSkin>(() =>
    pickBuildAgentSkin("claude"),
  );
  const [promptCopied, setPromptCopied] = useState(false);
  const cwd = `~/${exemplar.department.toLowerCase().replace(/\s+/g, "-")}`;

  return (
    <div className="space-y-5" data-testid="build-first-call">
      <div className="space-y-5" data-testid="build-golden-prompt">
        <p className="text-sm text-muted-foreground text-pretty">
          Prompt your favorite LLM with{" "}
          <span className="inline-flex max-w-full items-center gap-1 align-middle">
            <Code>{exemplar.llmPrompt}</Code>
            <Button
              type="button"
              size="icon-xs"
              variant="ghost"
              className="shrink-0 text-muted-foreground"
              aria-label={promptCopied ? "Copied prompt" : "Copy prompt"}
              onClick={() => {
                void navigator.clipboard.writeText(exemplar.llmPrompt).then(() => {
                  setPromptCopied(true);
                  window.setTimeout(() => setPromptCopied(false), 2000);
                });
              }}
            >
              {promptCopied ? (
                <CheckIcon className="size-3.5" />
              ) : (
                <CopyIcon className="size-3.5" />
              )}
            </Button>
          </span>{" "}
          and it should reply like in this example below.
        </p>

        <div
          className="min-h-[16rem] w-full"
          data-testid="build-agent-console-reply"
        >
          <BuildAgentConsolePreview
            variant={agentSkin}
            prompt={exemplar.llmPrompt}
            reply={exemplar.expectedResult}
            cwd={cwd}
          />
        </div>

        <p className="text-sm text-muted-foreground text-pretty">
          Behind the scenes this calls{" "}
          <Code>{invokeOp}</Code>.
        </p>

        {/* Registry Card Collapsible — compose cardVariants; do not restyle chrome. */}
        <Collapsible
          defaultOpen
          className={cn(cardVariants({ variant: "outline" }), "w-full")}
          data-testid="build-cli-alert"
        >
          <CollapsibleTrigger className="rounded-t-xl p-4 data-[state=closed]:rounded-b-xl">
            How to do it with the CLI
            <ChevronDownIcon className="size-4 shrink-0 text-muted-foreground transition-transform duration-overshoot ease-out-back" />
          </CollapsibleTrigger>
          <CollapsibleContent className="space-y-3 rounded-b-xl border-t border-border px-4 py-3">
            <p className="text-sm text-muted-foreground text-pretty">
              If you want to use the CLI instead, do it this way:
            </p>
            <CodeBlock
              code={exemplar.invokeRecipe}
              language="bash"
              filename="Terminal"
            />
          </CollapsibleContent>
        </Collapsible>
      </div>

      <div className="space-y-3" data-testid="build-shipped-app">
        <SectionHeader>
          <SectionHeaderContent>
            <SectionHeaderTitle size="sm">Already shipped</SectionHeaderTitle>
            <SectionHeaderDescription>
              <span className="text-foreground">{exemplar.builderNote}</span>{" "}
              already shipped{" "}
              <span className="text-foreground">{displayName}</span>. It&apos;s a
              custom App that answers just what you asked and more.
            </SectionHeaderDescription>
          </SectionHeaderContent>
        </SectionHeader>
        <div className="max-w-md">
          <BuildStoreAppCard
            name={exemplar.id}
            label={displayName}
            description={
              integration?.description?.trim() ||
              exemplar.need
            }
            iconSvg={integration?.iconSvg}
            href={open.href}
            testId="build-open-exemplar"
          />
        </div>
      </div>

      <div className="space-y-3" data-testid="build-related-apps">
        <SectionHeader>
          <SectionHeaderContent>
            <SectionHeaderTitle size="sm">Related apps</SectionHeaderTitle>
            <SectionHeaderDescription>
              More apps that fit this outcome — open one, or browse the full
              store.
            </SectionHeaderDescription>
          </SectionHeaderContent>
        </SectionHeader>
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
          {exemplar.relatedAppIds.map((appId) => {
            const related = integrations.find((item) => item.name === appId);
            const label =
              related?.displayName?.trim() || companionAppLabel(appId);
            const href = related?.mountedPath?.trim()
              ? related.mountedPath.trim()
              : `/apps/${encodeURIComponent(appId)}`;
            return (
              <BuildStoreAppCard
                key={appId}
                name={appId}
                label={label}
                description={
                  related?.description?.trim() ||
                  `Open ${label} in Gestalt.`
                }
                iconSvg={related?.iconSvg}
                href={href}
              />
            );
          })}
        </div>
        <UiLink asChild className="inline-flex w-fit text-sm">
          <Link to="/apps">See all apps</Link>
        </UiLink>
      </div>
    </div>
  );
}

/** Catalog-style solid card — opens the app in a new tab (no connect chrome). */
function BuildStoreAppCard({
  name,
  label,
  description,
  iconSvg,
  href,
  testId,
}: {
  name: string;
  label: string;
  description: string;
  iconSvg?: string;
  href: string;
  testId?: string;
}) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      data-testid={testId ?? `build-open-app-${name}`}
      className={cn(
        "flex items-start gap-4 rounded-xl bg-neutral-hover p-4 text-foreground",
        "transition-[background-color] duration-hover-out ease-out-quart",
        "hover:bg-neutral-dark-hover hover:duration-hover-in active:bg-neutral-dark-pressed",
        "focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-gold-400 focus-visible:ring-offset-2 focus-visible:ring-offset-background",
      )}
    >
      <IntegrationIcon iconSvg={iconSvg} size="xl" />
      <span className="min-w-0">
        <span className="block text-base font-heading text-foreground">
          {label}
        </span>
        <span className="mt-1 block line-clamp-2 text-sm text-muted-foreground">
          {description}
        </span>
      </span>
    </a>
  );
}

function ConnectStepActions({
  exemplar,
  integrations,
  catalogReady,
}: {
  exemplar: BuildExemplar;
  integrations: Integration[];
  connected: Set<string>;
  catalogReady: boolean;
}) {
  const invalidateIntegrations = useInvalidateIntegrations();
  const returnPath = `${BUILD_PATH}/connect`;

  async function refreshIntegrations() {
    await invalidateIntegrations();
  }

  if (!catalogReady) {
    return <p className="text-sm text-faint">Loading apps…</p>;
  }

  const companionIntegrations = exemplar.companionAppIds.map((appId) => {
    const integration = integrations.find((item) => item.name === appId);
    return { appId, integration };
  });
  const missingFromCatalog = companionIntegrations.filter(
    (item) => !item.integration,
  );

  return (
    <div className="flex flex-col gap-6" data-testid="build-connect-apps">
      <div className="grid grid-cols-1 gap-6 sm:grid-cols-2">
        {companionIntegrations.map(({ appId, integration }) => {
          if (!integration) {
            return (
              <div
                key={appId}
                className="rounded-xl bg-neutral-hover p-4 text-foreground"
                data-testid={`build-connect-app-${appId}`}
              >
                <div className="flex items-start gap-3">
                  <IntegrationIcon size="md" />
                  <div className="min-w-0 flex-1">
                    <p className="text-sm font-medium text-foreground">
                      {companionAppLabel(appId)}
                    </p>
                    <p className="mt-1 text-sm text-muted-foreground">
                      This app is not available in your workspace yet.
                    </p>
                    <Badge variant="warning" size="sm" className="mt-2">
                      Not in workspace
                    </Badge>
                  </div>
                </div>
              </div>
            );
          }

          return (
            <IntegrationCard
              key={appId}
              integration={integration}
              returnPath={returnPath}
              onConnected={() => void refreshIntegrations()}
              onDisconnected={() => void refreshIntegrations()}
            />
          );
        })}
      </div>

      {missingFromCatalog.length > 0 ? (
        <p className="text-sm text-muted-foreground">
          Ask an admin to add missing apps to this workspace before you
          continue.
        </p>
      ) : null}
    </div>
  );
}

function errorMessage(reason: unknown): string {
  if (reason instanceof Error) return reason.message;
  if (typeof reason === "string") return reason;
  return "Failed to load workspace status";
}
