import { useState } from "react";
import {
  Link,
  Navigate,
  useNavigate,
  useParams,
} from "@tanstack/react-router";
import { Badge } from "@/components/Badge";
import Container from "@/components/Container";
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
import { SegmentedControl } from "@/components/ui/segmented-control";
import { CodeBlock } from "@/components/ui/code-block";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
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
  AgentConsolePath,
  AgentConsoleProduct,
  AgentConsolePrompt,
  AgentConsoleSubtitle,
  AgentConsoleTrafficLights,
  AgentConsoleTyping,
  AgentConsoleWindowTitle,
  AGENT_CONSOLE_THEME_CLAUDE,
} from "@/components/ui/agent-console";
import ShikiCode from "@/components/ShikiCode";
import TokenCreateForm from "@/components/TokenCreateForm";
import {
  CheckIcon,
  ChevronLeftIcon,
  ChevronRightIcon,
  ClaudeIcon,
  CodexIcon,
  CopyIcon,
  CursorIcon,
} from "@/components/icons";
import { useBuildSession } from "@/hooks/use-build-session";
import { useDocumentTitle } from "@/hooks/use-document-title";
import {
  useIntegrationsQuery,
  useInvalidateTokens,
  useTokensQuery,
} from "@/hooks/use-server-queries";
import {
  startIntegrationOAuth,
  type APIToken,
  type Integration,
} from "@/lib/api";
import {
  BUILD_EXEMPLARS,
  BUILD_STEPS,
  companionAppLabel,
  connectedAppIds,
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
import { primaryConnectLabel } from "@/lib/catalogFilters";
import { cn } from "@/lib/cn";
import { normalizeIntegrationStatus } from "@/lib/integrationStatus";
import {
  BUILD_PATH,
  CONNECTION_RETURN_PATH_STORAGE_KEY,
  DOCS_PATH,
} from "@/lib/constants";

type McpClient = "cursor" | "claude" | "codex";

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
        <div className="mx-auto w-full max-w-5xl">
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
    introSeen: session.introSeen,
  };

  const stepId = firstIncompleteStepId(snapshot, (step) =>
    isStepDone(step, snapshot, tokensReady, integrationsReady),
  );

  return <Navigate to="/build/$stepId" params={{ stepId }} replace />;
}

export default function BuildStepPage() {
  useDocumentTitle("Build");
  const { stepId: rawStepId } = useParams({ strict: false }) as {
    stepId?: string;
  };
  const navigate = useNavigate();
  const session = useBuildSession();
  const integrationsQuery = useIntegrationsQuery();
  const tokensQuery = useTokensQuery();
  const invalidateTokens = useInvalidateTokens();

  if (!rawStepId || !isBuildStepId(rawStepId)) {
    return <Navigate to="/build" replace />;
  }
  const stepId = rawStepId;

  const tokensReady = !tokensQuery.isPending;
  const integrationsReady = !integrationsQuery.isPending;

  const snapshot: BuildWorkspaceSnapshot = {
    integrations: integrationsQuery.data ?? [],
    tokens: tokensQuery.data ?? [],
    activeExemplarId: session.activeExemplarId,
    mcpInstalled: session.mcpInstalled,
    apiToken: session.apiToken,
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
  const currentStep = BUILD_STEPS.find((s) => s.id === stepId)!;

  function goToStep(id: BuildStepId) {
    void navigate({ to: "/build/$stepId", params: { stepId: id } });
  }

  async function refreshTokens() {
    await invalidateTokens();
  }

  return (
    <Container as="main" className="py-12">
      <div className="mx-auto w-full max-w-5xl">
        <PageHeader className="animate-fade-in-up">
          <PageHeaderContent>
            <div className="flex flex-col gap-3">
              <Eyebrow>Get started</Eyebrow>
              <PageHeaderTitle size="lg">Build</PageHeaderTitle>
            </div>
            <PageHeaderDescription className="max-w-2xl">
              Choose what someone on a team would ask an agent to do — then
              create a token, install Gestalt, connect companions, and make
              your first call.
            </PageHeaderDescription>
          </PageHeaderContent>
        </PageHeader>

        {error && (
          <p className="mt-8 text-sm text-ember-500">{error}</p>
        )}

        <div
          data-testid="build-step-nav"
          className="mt-8 animate-fade-in-up [animation-delay:40ms]"
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
          onRefreshTokens={refreshTokens}
          onMarkMcpInstalled={session.markMcpInstalled}
          onMarkIntroSeen={session.markIntroSeen}
          onGoToStep={goToStep}
        />

        <p className="mt-10 text-sm text-muted-foreground animate-fade-in-up [animation-delay:120ms]">
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
      return snapshot.apiToken.trim().length > 0 || tokensReady;
    case "install":
      return true;
    case "connect":
      return integrationsReady;
    case "invoke":
      return (
        integrationsReady &&
        (snapshot.apiToken.trim().length > 0 || tokensReady)
      );
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

/** Registry ChoiceCardsGrid — use shared {@link choiceCardClassName} only. */

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

  function handleContinue() {
    onMarkIntroSeen();
    onGoToStep("authorize");
  }

  return (
    <div className="space-y-6" data-testid="build-intro">
      <div data-testid="build-outcome-toggle" className="w-full">
        <RadioGroup
          value={activeExemplarId}
          onValueChange={(value) =>
            onSelectExemplar(value as BuildExemplarId)
          }
          className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4"
          aria-label="What to build"
        >
          {BUILD_EXEMPLARS.map((item) => {
            const inputId = `build-outcome-${item.id}`;
            return (
              <Label
                key={item.id}
                htmlFor={inputId}
                className={cn(choiceCardClassName, "h-full")}
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

      <div data-testid="build-agent-console" className="max-w-xl">
        <AgentConsole
          key={exemplar.id}
          theme={AGENT_CONSOLE_THEME_CLAUDE}
          className="w-full max-w-full"
        >
          <AgentConsoleChrome>
            <AgentConsoleTrafficLights />
            <AgentConsoleWindowTitle>claude</AgentConsoleWindowTitle>
          </AgentConsoleChrome>
          <AgentConsoleBody>
            <AgentConsoleIdentity>
              <AgentConsoleMedia>
                <ClaudePixelIcon className="size-16" />
              </AgentConsoleMedia>
              <AgentConsoleHeading>
                <AgentConsoleProduct>Claude Code</AgentConsoleProduct>
                <AgentConsoleSubtitle>
                  Building with Gestalt
                </AgentConsoleSubtitle>
                <AgentConsolePath>
                  ~/
                  {exemplar.department.toLowerCase().replace(/\s+/g, "-")}
                </AgentConsolePath>
              </AgentConsoleHeading>
            </AgentConsoleIdentity>
            <AgentConsolePrompt>
              <AgentConsoleGlyph>❯</AgentConsoleGlyph>
              <AgentConsoleInput measureText={exemplar.llmPrompt}>
                <AgentConsoleTyping text={exemplar.llmPrompt} />
                <AgentConsoleCursor />
              </AgentConsoleInput>
            </AgentConsolePrompt>
            <AgentConsoleHint>? for shortcuts</AgentConsoleHint>
          </AgentConsoleBody>
        </AgentConsole>
      </div>

      <nav
        aria-label="Continue"
        className="mt-2 flex justify-end border-t border-alpha pt-6"
      >
        <button
          type="button"
          data-testid="build-intro-continue"
          onClick={handleContinue}
          className="group flex flex-col gap-1 rounded-xl border border-alpha bg-background px-5 py-5 text-left transition-[color,background-color,border-color] duration-hover-out ease-out-quart hover:border-alpha-strong hover:bg-neutral-hover hover:duration-hover-in focus-ring sm:items-end sm:text-right"
        >
          <span className="text-xs font-medium uppercase tracking-[0.08em] text-muted-foreground">
            Next
          </span>
          <span className="mt-1 flex items-center gap-2 font-heading text-xl leading-tight tracking-tight text-foreground sm:flex-row-reverse">
            <ChevronRightIcon className="size-6 shrink-0 text-muted-foreground transition-colors duration-hover-out group-hover:text-foreground" />
            Create a token
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
  const [mcpClient, setMcpClient] = useState<McpClient>("cursor");
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
    <div className="space-y-3">
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
        <div className="space-y-3 text-sm text-muted-foreground">
          <p>
            One-click install opens Cursor and adds this workspace as an MCP
            server (uses your API token).
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
          <details className="rounded-md border border-alpha bg-base-white dark:bg-surface">
            <summary className="cursor-pointer list-none px-3 py-2 text-sm font-medium text-foreground marker:content-none [&::-webkit-details-marker]:hidden">
              Prefer editing{" "}
              <code className="font-mono text-xs">.cursor/mcp.json</code>{" "}
              manually
            </summary>
            <div className="space-y-2 border-t border-alpha px-3 py-3">
              <CodeBlock
                code={cursorConfig}
                language="json"
                filename=".cursor/mcp.json"
              />
            </div>
          </details>
        </div>
      ) : null}
      {mcpClient === "claude" ? (
        <div className="space-y-2 text-sm text-muted-foreground">
          <p>
            Run this in a terminal (uses your API token), or see{" "}
            <UiLink asChild className="text-sm">
              <Link to={`${DOCS_PATH}/mcp`}>Use with MCP</Link>
            </UiLink>
            .
          </p>
          <CodeBlock
            code={claudeCommand}
            language="bash"
            filename="Terminal"
          />
        </div>
      ) : null}
      {mcpClient === "codex" ? (
        <div className="space-y-2 text-sm text-muted-foreground">
          <p>Register this workspace with the Codex CLI:</p>
          <CodeBlock
            code={codexCommand}
            language="bash"
            filename="Terminal"
          />
        </div>
      ) : null}
      <p className="text-sm text-muted-foreground">
        Endpoint:{" "}
        <code className="font-mono text-xs text-foreground">{mcpUrl}</code>
      </p>
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
  onRefreshTokens: () => void | Promise<void>;
  onMarkMcpInstalled: () => void;
  onMarkIntroSeen: () => void;
  onGoToStep: (id: BuildStepId) => void;
}) {
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
      {step.id !== "intro" ? (
        <>
          <h2 className="font-heading text-xl leading-none tracking-tight text-foreground">
            {step.title}
          </h2>
          <p className="text-sm text-muted-foreground">{step.description}</p>
        </>
      ) : null}

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
          tokens={tokens}
          tokensLoaded={tokensReady}
          apiToken={apiToken}
          onApiToken={onApiToken}
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
        <BuildStepPager stepId={step.id} onGoToStep={onGoToStep} />
      ) : null}
    </section>
  );
}

function BuildStepPager({
  stepId,
  onGoToStep,
}: {
  stepId: BuildStepId;
  onGoToStep: (id: BuildStepId) => void;
}) {
  const index = BUILD_STEPS.findIndex((step) => step.id === stepId);
  const prev = index > 0 ? BUILD_STEPS[index - 1] : null;
  const next =
    index >= 0 && index < BUILD_STEPS.length - 1
      ? BUILD_STEPS[index + 1]
      : null;
  if (!prev && !next) return null;

  const cardClass =
    "group flex w-full flex-col gap-1 rounded-xl border border-alpha bg-background px-5 py-5 text-left transition-[color,background-color,border-color] duration-hover-out ease-out-quart hover:border-alpha-strong hover:bg-neutral-hover hover:duration-hover-in focus-ring";

  return (
    <nav
      aria-label="Build step navigation"
      data-testid="build-step-pager"
      className="mt-8 grid gap-3 border-t border-alpha pt-6 sm:grid-cols-2"
    >
      {prev ? (
        <button
          type="button"
          data-testid="build-step-prev"
          onClick={() => onGoToStep(prev.id)}
          className={cardClass}
        >
          <span className="text-xs font-medium uppercase tracking-[0.08em] text-muted-foreground">
            Previous
          </span>
          <span className="mt-1 flex items-center gap-2 font-heading text-xl leading-tight tracking-tight text-foreground">
            <ChevronLeftIcon className="size-6 shrink-0 text-muted-foreground transition-colors duration-hover-out group-hover:text-foreground" />
            {prev.title}
          </span>
        </button>
      ) : (
        <div className="hidden sm:block" aria-hidden />
      )}
      {next ? (
        <button
          type="button"
          data-testid="build-step-next"
          onClick={() => onGoToStep(next.id)}
          className={cn(cardClass, "sm:items-end sm:text-right")}
        >
          <span className="text-xs font-medium uppercase tracking-[0.08em] text-muted-foreground">
            Next
          </span>
          <span className="mt-1 flex items-center gap-2 font-heading text-xl leading-tight tracking-tight text-foreground sm:flex-row-reverse">
            <ChevronRightIcon className="size-6 shrink-0 text-muted-foreground transition-colors duration-hover-out group-hover:text-foreground" />
            {next.title}
          </span>
        </button>
      ) : null}
    </nav>
  );
}

function AuthorizeStepActions({
  tokens,
  tokensLoaded,
  apiToken,
  onApiToken,
  onTokensChanged,
}: {
  tokens: APIToken[];
  tokensLoaded: boolean;
  apiToken: string;
  onApiToken: (token: string) => void;
  onTokensChanged: () => void | Promise<void>;
}) {
  const [addingAnother, setAddingAnother] = useState(false);
  const hasTokens = tokens.length > 0;
  const showCreateForm = !hasTokens || addingAnother;

  async function handleTokenCreated(plaintext: string) {
    onApiToken(plaintext);
    setAddingAnother(false);
    await onTokensChanged();
  }

  return (
    <div className="space-y-3">
      {!tokensLoaded ? (
        <p className="text-sm text-faint">Loading tokens…</p>
      ) : null}

      {tokensLoaded && hasTokens ? (
        <div
          className="overflow-hidden rounded-xl border border-alpha bg-base-100 dark:bg-surface"
          data-testid="build-token-list"
        >
          <ul className="divide-y divide-alpha">
            {tokens.map((token) => (
              <li
                key={token.id}
                className="flex flex-wrap items-baseline justify-between gap-2 px-4 py-3"
              >
                <div className="min-w-0">
                  <p className="truncate text-sm font-medium text-foreground">
                    {token.name?.trim() || token.id}
                  </p>
                  <p className="mt-0.5 font-mono text-xs text-faint">
                    {token.id}
                  </p>
                </div>
                <div className="flex flex-wrap items-center gap-3 text-xs text-muted-foreground">
                  <span>
                    {token.scopes?.length
                      ? token.scopes.join(" ")
                      : "all scopes"}
                  </span>
                  <span className="font-mono">
                    {new Date(token.createdAt).toLocaleDateString()}
                  </span>
                  {token.expiresAt ? (
                    <span className="font-mono">
                      expires{" "}
                      {new Date(token.expiresAt).toLocaleDateString()}
                    </span>
                  ) : (
                    <span>no expiry</span>
                  )}
                </div>
              </li>
            ))}
          </ul>
          <div className="border-t border-alpha px-4 py-2">
            <UiLink asChild className="text-sm">
              <Link to="/settings" hash="authorization">
                Manage tokens
              </Link>
            </UiLink>
          </div>
        </div>
      ) : null}

      {tokensLoaded && hasTokens && !showCreateForm ? (
        <Button
          type="button"
          variant="outline"
          data-testid="build-add-another-token"
          onClick={() => setAddingAnother(true)}
        >
          Add another token
        </Button>
      ) : null}

      {tokensLoaded && showCreateForm ? (
        <div className="space-y-2">
          {hasTokens ? (
            <p className="text-sm text-muted-foreground">
              Add another token — the secret is only shown once.
            </p>
          ) : null}
          <TokenCreateForm onCreated={handleTokenCreated} />
          {hasTokens && addingAnother && !apiToken ? (
            <UiLink asChild className="text-sm">
              <button type="button" onClick={() => setAddingAnother(false)}>
                Cancel
              </button>
            </UiLink>
          ) : null}
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
  return (
    <div className="space-y-4">
      <Field className="max-w-xl">
        <FieldLabel htmlFor="build-api-token">
          {apiToken
            ? "API token secret"
            : "Paste an API token secret"}
        </FieldLabel>
        <Input
          id="build-api-token"
          type="password"
          autoComplete="off"
          spellCheck={false}
          placeholder="gst_api_…"
          value={apiToken}
          onChange={(event) => onApiToken(event.target.value.trim())}
          className="font-mono"
        />
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

      <Button
        type="button"
        variant="outline"
        data-testid="build-mark-mcp-installed"
        onClick={onMarkMcpInstalled}
      >
        I&apos;ve installed Gestalt
      </Button>
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

  return (
    <div className="space-y-5" data-testid="build-first-call">
      <div
        className="space-y-3 rounded-lg border border-grove-200 bg-grove-50 px-4 py-4 dark:border-grove-600 dark:bg-grove-700/15"
        data-testid="build-golden-prompt"
      >
        <div className="space-y-1">
          <Eyebrow>Golden first call</Eyebrow>
          <p className="text-sm font-medium text-foreground">
            {exemplar.label}
          </p>
          <p className="text-sm text-muted-foreground">{exemplar.need}</p>
          <p className="font-mono text-xs text-muted-foreground">
            {exemplar.invokeAppId}.{exemplar.operationId}
          </p>
        </div>
        <p className="text-sm text-muted-foreground">
          Paste this prompt in your AI client (MCP wired above) — or use the CLI
          recipe below.
        </p>
        <RecipeCodeBlock code={exemplar.llmPrompt} language="text" />
        <details className="group rounded-md border border-alpha bg-base-white open:pb-3 dark:bg-surface">
          <summary className="cursor-pointer list-none px-3 py-2 text-sm font-medium text-foreground marker:content-none [&::-webkit-details-marker]:hidden">
            Prefer the CLI instead
          </summary>
          <div className="space-y-2 border-t border-alpha px-3 pt-2">
            <RecipeCodeBlock
              code={exemplar.invokeRecipe}
              language="shellscript"
            />
          </div>
        </details>
      </div>

      <div
        role="note"
        className="space-y-3 rounded-lg border border-alpha bg-alpha-5 px-4 py-4 dark:bg-surface-raised"
        data-testid="build-exemplar-cta"
      >
        <p className="text-sm font-heading text-foreground">
          Congratulations — you&apos;re ready to build
        </p>
        <p className="text-sm text-muted-foreground">
          You should see something like this:
        </p>
        <RecipeCodeBlock code={exemplar.expectedResult} language="text" />
        <p className="text-sm text-muted-foreground">
          By the way — <span className="text-foreground">{exemplar.builderNote}</span>{" "}
          already shipped <span className="text-foreground">{displayName}</span>.
          Open it, or find this and more relevant apps in the store.
        </p>
        <div className="flex flex-wrap gap-4">
          {open.kind === "mount" ? (
            <Button asChild>
              <a href={open.href} data-testid="build-open-exemplar">
                Open {displayName}
              </a>
            </Button>
          ) : (
            <Button asChild>
              <Link
                to="/apps/$appName"
                params={{ appName: exemplar.id }}
                data-testid="build-open-exemplar"
              >
                Open {displayName}
              </Link>
            </Button>
          )}
          <UiLink asChild className="text-sm">
            <Link to="/apps">Browse app store</Link>
          </UiLink>
          <UiLink asChild className="text-sm">
            <Link to="/identities">Agent identities</Link>
          </UiLink>
          <UiLink asChild className="text-sm">
            <Link to={`${DOCS_PATH}/getting-started`}>Read the docs</Link>
          </UiLink>
        </div>
      </div>

      <UiLink asChild className="inline-flex w-fit text-sm">
        <Link to={`${DOCS_PATH}/mcp`}>Full MCP setup</Link>
      </UiLink>
    </div>
  );
}

function ConnectStepActions({
  exemplar,
  integrations,
  connected,
  catalogReady,
}: {
  exemplar: BuildExemplar;
  integrations: Integration[];
  connected: Set<string>;
  catalogReady: boolean;
}) {
  const [connectingAppId, setConnectingAppId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const returnPath = `${BUILD_PATH}/connect`;

  async function handleConnect(appId: string) {
    setConnectingAppId(appId);
    setError(null);
    try {
      window.sessionStorage.setItem(
        CONNECTION_RETURN_PATH_STORAGE_KEY,
        returnPath,
      );
      const { url } = await startIntegrationOAuth(
        appId,
        undefined,
        undefined,
        undefined,
        undefined,
        returnPath,
      );
      window.location.href = url;
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to start connect");
      setConnectingAppId(null);
    }
  }

  if (!catalogReady) {
    return <p className="text-sm text-faint">Loading apps…</p>;
  }

  if (exemplar.companionAppIds.length === 0) {
    return (
      <div className="space-y-3" data-testid="build-connect-self-contained">
        <p className="text-sm text-muted-foreground">
          <span className="font-medium text-foreground">{exemplar.label}</span>{" "}
          is self-contained — no companion apps to connect. Continue to make
          your first call, then open the app in the store.
        </p>
        <UiLink asChild className="text-sm">
          <Link to="/apps/$appName" params={{ appName: exemplar.id }}>
            View {exemplar.label} in the store
          </Link>
        </UiLink>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-start gap-3">
      <p className="text-sm text-muted-foreground">
        Connect companions for{" "}
        <span className="font-medium text-foreground">{exemplar.label}</span> —
        they unlock the golden prompt.
      </p>
      <ul className="flex w-full flex-col gap-2">
        {exemplar.companionAppIds.map((appId) => {
          const integration = integrations.find((item) => item.name === appId);
          const status = integration
            ? normalizeIntegrationStatus(integration)
            : null;
          const alreadyConnected = Boolean(
            status?.connected && status.tone === "success",
          );
          const actionLabel =
            (integration ? primaryConnectLabel(integration) : null) ??
            (alreadyConnected ? null : "Connect");
          const label = companionAppLabel(appId);
          const connecting = connectingAppId === appId;
          const inCatalog = connected.has(appId) || Boolean(integration);

          return (
            <li
              key={appId}
              className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-alpha bg-base-100 px-4 py-3 dark:bg-surface"
              data-testid={`build-connect-app-${appId}`}
            >
              <div className="flex min-w-0 items-center gap-3">
                <IntegrationIcon iconSvg={integration?.iconSvg} size="sm" />
                <span className="text-sm font-medium text-foreground">
                  {label}
                </span>
                {alreadyConnected ? (
                  <Badge variant="success" size="sm">
                    Connected
                  </Badge>
                ) : null}
                {!inCatalog ? (
                  <Badge variant="warning" size="sm">
                    Not in workspace
                  </Badge>
                ) : null}
              </div>
              {alreadyConnected ? null : (
                <Button
                  type="button"
                  onClick={() => handleConnect(appId)}
                  disabled={connecting || !actionLabel || !integration}
                >
                  {connecting
                    ? "Connecting…"
                    : `${actionLabel ?? "Connect"} ${label}`}
                </Button>
              )}
            </li>
          );
        })}
      </ul>
      <Link
        to="/apps"
        className="text-sm text-muted-foreground underline-offset-2 transition-colors duration-150 hover:text-foreground hover:underline"
      >
        See all apps
      </Link>
      {error && <p className="text-sm text-ember-500">{error}</p>}
    </div>
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
        className="absolute right-3 top-3 rounded-md p-1.5 text-muted-foreground opacity-0 transition-all duration-150 hover:bg-alpha-5 hover:text-foreground group-hover:opacity-100"
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
