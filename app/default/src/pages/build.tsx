import { useEffect, useMemo, useState, type ReactNode } from "react";
import { Lightbulb } from "lucide-react";
import { toast } from "sonner";
import {
  Link,
  Navigate,
  useNavigate,
  useParams,
} from "@tanstack/react-router";
import { Badge } from "@/components/ui/badge";
import Container from "@/components/Container";
import IntegrationCard from "@/components/IntegrationCard";
import SeeMoreAppsTrigger from "@/components/SeeMoreAppsTrigger";
import { InvokeOperationReference } from "@/components/InvokeOperationReference";
import IntegrationIcon from "@/components/IntegrationIcon";
import { Link as UiLink } from "@/components/ui/link";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import {
  choiceCardContentNoIndicatorClassName,
  choiceCardFormFieldsClassName,
  choiceCardFormShellClassName,
  choiceCardHoverClassName,
  choiceCardNoIndicatorClassName,
  choiceCardRadioHiddenClassName,
  radioLabelWrappedDisabledClassName,
} from "@/lib/choice-card-chrome";
import { Eyebrow } from "@/components/ui/eyebrow";
import {
  Alert,
  AlertActions,
  AlertCollapsibleContent,
  AlertDescription,
  AlertTitle,
  AlertTrigger,
} from "@/components/ui/alert";
import {
  StepPager,
  StepPagerNext,
  StepPagerPrevious,
  StepPagerStartSpacer,
} from "@/components/ui/step-pager";
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
import { CodeBlock } from "@/components/ui/code-block";
import { CopyableCode } from "@/components/ui/copyable-code";
import {
  Collapsible,
  CollapsibleContent,
} from "@/components/ui/collapsible";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { Button } from "@/components/ui/button";
import { ChipGroup, ChipGroupItem } from "@/components/ui/chip-group";
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
  AGENT_CONSOLE_THEME_CODEX,
  AGENT_CONSOLE_THEME_CURSOR,
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
} from "@/components/ui/agent-console";
import ErrorNotice from "@/components/ErrorNotice";
import TokenCreateForm from "@/components/TokenCreateForm";
import {
  ClaudeIcon,
  CodexIcon,
  CursorIcon,
  MoreHorizontalIcon,
  SpinnerIcon,
} from "@/components/icons";
import { useBuildSession } from "@/hooks/use-build-session";
import { useDocumentTitle } from "@/hooks/use-document-title";
import {
  useIntegrationsQuery,
  useInvalidateIntegrations,
  useInvalidateTokens,
  useTokensQuery,
} from "@/lib/queries";
import { PAGE_LAYOUT_READING_COLUMN_CLASS } from "@/lib/page-layout-content-top";
import {
  type APIToken,
  type Integration,
} from "@/lib/api";
import {
  BUILD_CREATE_NEW_TOKEN_ID,
  BUILD_USE_EXISTING_TOKEN_ID,
  BUILD_STEPS,
  buildInstallAgentSelected,
  buildAuthorizeSelectionReady,
  buildMcpCredentialReady,
  buildStepDescription,
  buildStepTitle,
  canNavigateToBuildStep,
  companionAppLabel,
  DEFAULT_BUILD_TOKEN_NAME,
  firstIncompleteStepId,
  getExemplar,
  isBuildComplete,
  isBuildStepId,
  isLegacySetupConnectStepId,
  resolveExemplarOpenPath,
  SETUP_PRODUCT_NAME,
  SETUP_JOURNEY_EYEBROW,
  isSetupDataSourceApp,
  setupAppsConnected,
  setupAppsHasConnectable,
  setupDataSourceIntegrations,
  writeSetupSkipped,
  type BuildExemplar,
  type BuildExemplarId,
  type BuildInstallAgentId,
  type BuildStep,
  type BuildStepId,
  type BuildWorkspaceSnapshot,
} from "@/lib/buildPaths";
import { cn } from "@/lib/cn";
import { DOCS_PATH, SETUP_PATH } from "@/lib/constants";
import { userFacingError } from "@/lib/user-facing-error";
import { getIntegrationLabel } from "@/lib/integrationSearch";
import {
  catalogBucketIdFor,
  catalogBucketsPresentIn,
  CATALOG_BUCKETS,
} from "@/lib/catalogBuckets";
import {
  SETUP_APPS_CATEGORY_ALL,
  SETUP_APPS_GRID_CLASS,
  SETUP_APPS_PAGE_SIZE,
} from "@/lib/setupSeeMore";

function buildSnapshot(
  session: ReturnType<typeof useBuildSession>,
  integrations: Integration[],
  tokens: APIToken[],
): BuildWorkspaceSnapshot {
  return {
    integrations,
    tokens,
    activeExemplarId: session.activeExemplarId,
    mcpInstalled: session.mcpInstalled,
    apiToken: session.apiToken,
    apiTokenGrantId: session.apiTokenGrantId,
    tokenName: session.tokenName,
    selectedTokenId: session.selectedTokenId,
    installAgentId: session.selectedInstallAgent,
    welcomeSeen: session.welcomeSeen,
    trySeen: session.trySeen,
  };
}

/** `/setup` → overview when complete, else first incomplete step. */
export function BuildIndexRedirect() {
  useDocumentTitle("Setup");
  const session = useBuildSession();
  const integrationsQuery = useIntegrationsQuery();
  const tokensQuery = useTokensQuery();

  const tokensReady = !tokensQuery.isPending;
  const integrationsReady = !integrationsQuery.isPending;

  if (!tokensReady || !integrationsReady) {
    return (
      <Container as="main" className="py-12">
        <div className="mx-auto flex min-h-[50vh] w-full max-w-4xl items-center justify-center">
          <p className="flex items-center gap-2 text-sm text-faint">
            <SpinnerIcon className="size-3.5 animate-spin" aria-hidden />
            Loading setup…
          </p>
        </div>
      </Container>
    );
  }

  const snapshot = buildSnapshot(
    session,
    integrationsQuery.data ?? [],
    tokensQuery.data ?? [],
  );

  if (isBuildComplete(snapshot)) {
    return <SetupOverview snapshot={snapshot} />;
  }

  const stepId = firstIncompleteStepId(snapshot, (step) =>
    isStepDone(step, snapshot, tokensReady, integrationsReady),
  );

  return <Navigate to="/setup/$stepId" params={{ stepId }} replace />;
}

function SetupStepperList({
  titleForStep,
  itemTestId,
  listTestId,
}: {
  titleForStep: (step: BuildStep) => string;
  itemTestId?: (id: BuildStepId) => string;
  listTestId?: string;
}) {
  return (
    <StepperList aria-label="Setup steps" data-testid={listTestId}>
      {BUILD_STEPS.map((step) => (
        <StepperItem
          key={step.id}
          value={step.id}
          data-testid={itemTestId?.(step.id)}
        >
          <StepperSeparator />
          <StepperTrigger>
            <StepperIndicator />
            <StepperTitle>{titleForStep(step)}</StepperTitle>
          </StepperTrigger>
        </StepperItem>
      ))}
    </StepperList>
  );
}

function SetupPageChrome({
  value,
  onValueChange,
  titleForStep,
  itemTestId,
  listTestId,
  children,
}: {
  value: string;
  onValueChange: (next: string) => void;
  titleForStep: (step: BuildStep) => string;
  itemTestId?: (id: BuildStepId) => string;
  listTestId?: string;
  children: ReactNode;
}) {
  return (
    <Container className="pb-12 pt-16">
      <div className={PAGE_LAYOUT_READING_COLUMN_CLASS}>
        <Stepper
          value={value}
          onValueChange={onValueChange}
          orientation="horizontal"
          activationMode="jump"
          size="sm"
          completedVariant="success"
          connectorVariant="primary"
        >
          <SetupStepperList
            titleForStep={titleForStep}
            itemTestId={itemTestId}
            listTestId={listTestId}
          />
        </Stepper>
        <div className="mt-8">{children}</div>
      </div>
    </Container>
  );
}

function SetupOverview({ snapshot }: { snapshot: BuildWorkspaceSnapshot }) {
  const navigate = useNavigate();
  const lastStepId = BUILD_STEPS[BUILD_STEPS.length - 1]!.id;

  return (
    <SetupPageChrome
      value={lastStepId}
      onValueChange={(next) => {
        if (!isBuildStepId(next)) return;
        void navigate({ to: "/setup/$stepId", params: { stepId: next } });
      }}
      titleForStep={(step) => buildStepTitle(step, snapshot.installAgentId)}
      itemTestId={(id) => `build-overview-${id}`}
    >
      <div className="space-y-8" data-testid="build-setup-overview">
        <PageHeader>
          <PageHeaderContent size="lg">
            <Eyebrow tone="accent">{SETUP_JOURNEY_EYEBROW}</Eyebrow>
            <PageHeaderTitle>You&apos;re all set</PageHeaderTitle>
            <PageHeaderDescription>
              Your assistant is connected and your workspace apps are ready to
              use.
            </PageHeaderDescription>
          </PageHeaderContent>
        </PageHeader>

        <StepPager variant="ghost" aria-label="After setup">
          <StepPagerPrevious asChild title="Run setup again">
            <Link to="/setup/$stepId" params={{ stepId: "welcome" }} />
          </StepPagerPrevious>
          <StepPagerNext asChild title="Browse apps">
            <Link to="/apps" />
          </StepPagerNext>
        </StepPager>
      </div>
    </SetupPageChrome>
  );
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
  const legacyConnect = Boolean(
    rawStepId && isLegacySetupConnectStepId(rawStepId),
  );
  const stepId =
    !legacyConnect && rawStepId && isBuildStepId(rawStepId) ? rawStepId : null;
  const currentStep = stepId
    ? BUILD_STEPS.find((s) => s.id === stepId)!
    : null;
  const stepTitle = currentStep
    ? buildStepTitle(currentStep, session.selectedInstallAgent)
    : "Setup";
  useDocumentTitle(currentStep ? `${stepTitle} · Setup` : "Setup");

  if (legacyConnect) {
    return (
      <Navigate to="/setup/$stepId" params={{ stepId: "token" }} replace />
    );
  }

  if (!stepId || !currentStep) {
    return <Navigate to="/setup" replace />;
  }

  const tokensReady = !tokensQuery.isPending;
  const integrationsReady = !integrationsQuery.isPending;

  const snapshot = buildSnapshot(
    session,
    integrationsQuery.data ?? [],
    tokensQuery.data ?? [],
  );

  const error =
    integrationsQuery.error != null
      ? userFacingError(
          integrationsQuery.error,
          "Couldn't load this workspace. Try again.",
        )
      : tokensQuery.error != null
        ? userFacingError(
            tokensQuery.error,
            "Couldn't load this workspace. Try again.",
          )
        : null;

  const activeExemplar = getExemplar(session.activeExemplarId);

  useEffect(() => {
    if (currentStep?.id === "try") session.markTrySeen();
  }, [currentStep?.id, session.markTrySeen]);

  function goToStep(id: BuildStepId) {
    void navigate({ to: "/setup/$stepId", params: { stepId: id } });
  }

  const stepIsDone = (step: BuildStep) =>
    isStepDone(step, snapshot, tokensReady, integrationsReady);

  function tryGoToStep(id: BuildStepId) {
    if (!stepId || !canNavigateToBuildStep(id, stepId, stepIsDone)) return;
    goToStep(id);
  }

  async function refreshTokens() {
    await invalidateTokens();
  }

  return (
    <SetupPageChrome
      value={stepId}
      onValueChange={(next) => {
        if (isBuildStepId(next)) tryGoToStep(next);
      }}
      titleForStep={(step) => step.title}
      itemTestId={(id) => `build-nav-${id}`}
      listTestId="build-step-nav"
    >
      {error ? (
        <ErrorNotice
          className="mb-8"
          message={error}
          onRetry={() => {
            void integrationsQuery.refetch();
            void tokensQuery.refetch();
          }}
          retrying={integrationsQuery.isFetching || tokensQuery.isFetching}
        />
      ) : null}

      <div className="space-y-8">
        <PageHeader className={cn(stepId === "welcome" && "hidden")}>
          <PageHeaderContent size="lg">
            <Eyebrow tone="accent">{currentStep.eyebrow}</Eyebrow>
            <PageHeaderTitle>
              {buildStepTitle(currentStep, session.selectedInstallAgent)}
            </PageHeaderTitle>
            <PageHeaderDescription>
              {buildStepDescription(currentStep, session.selectedInstallAgent)}
            </PageHeaderDescription>
          </PageHeaderContent>
        </PageHeader>

        <BuildStepPanel
          step={currentStep}
          tokensReady={tokensReady}
          integrationsReady={integrationsReady}
          integrations={snapshot.integrations}
          tokens={snapshot.tokens}
          activeExemplar={activeExemplar}
          activeExemplarId={session.activeExemplarId}
          onSelectExemplar={session.setActiveExemplarId}
          apiToken={session.apiToken}
          apiTokenGrantId={session.apiTokenGrantId}
          onApiToken={session.setApiToken}
          tokenName={session.tokenName}
          onTokenName={session.setTokenName}
          selectedTokenId={session.selectedTokenId}
          onSelectedTokenId={session.setSelectedTokenId}
          selectedInstallAgent={session.selectedInstallAgent}
          onSelectedInstallAgent={session.setSelectedInstallAgent}
          onRefreshTokens={refreshTokens}
          onMarkMcpInstalled={session.markMcpInstalled}
          onMarkWelcomeSeen={session.markWelcomeSeen}
          onGoToStep={goToStep}
        />
      </div>
    </SetupPageChrome>
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
    case "welcome":
      return true;
    case "assistant":
      return buildInstallAgentSelected(snapshot.installAgentId);
    case "token":
      return tokensReady;
    case "install":
      return true;
    case "apps":
      return integrationsReady;
    case "try":
      return integrationsReady;
    default:
      return true;
  }
}

function gestaltMcpBaseUrl(): string {
  const configured = import.meta.env.VITE_GESTALT_PUBLIC_ORIGIN?.trim();
  if (configured) return configured.replace(/\/$/, "");
  if (typeof window === "undefined") return "https://your-gestalt-host";
  const { origin, hostname } = window.location;
  if (hostname === "localhost" || hostname === "127.0.0.1") {
    return "https://your-gestalt-host";
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
 * Setup-page skin ids — composition variants, not an AgentConsole prop.
 * Palettes are Registry exports (`AGENT_CONSOLE_THEME_*`).
 */
type BuildAgentSkin = "claude" | "codex" | "cursor";

function buildAgentSkin(
  installAgentId: BuildInstallAgentId | "",
): BuildAgentSkin {
  if (installAgentId === "codex" || installAgentId === "cursor") {
    return installAgentId;
  }
  return "claude";
}

const BUILD_AGENT_THEMES = {
  claude: AGENT_CONSOLE_THEME_CLAUDE,
  codex: AGENT_CONSOLE_THEME_CODEX,
  cursor: AGENT_CONSOLE_THEME_CURSOR,
} as const;

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

  // Reset the turn when the exemplar prompt/reply or assistant skin changes.
  useEffect(() => {
    setPhase("prompt");
  }, [prompt, reply, variant]);

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
                Gestalt workspace
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
            <AgentConsoleSubtitle>Gestalt workspace</AgentConsoleSubtitle>
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

function WelcomeStorytelling({
  onMarkWelcomeSeen,
  onGoToStep,
}: {
  onMarkWelcomeSeen: () => void;
  onGoToStep: (id: BuildStepId) => void;
}) {
  const navigate = useNavigate();

  function handleContinue() {
    onMarkWelcomeSeen();
    onGoToStep("assistant");
  }

  function handleSkip() {
    writeSetupSkipped(true);
    void navigate({ to: "/apps" });
  }

  return (
    <div className="space-y-8" data-testid="build-welcome">
      <PageHeader>
        <PageHeaderContent size="lg">
          <Eyebrow tone="accent">{SETUP_JOURNEY_EYEBROW}</Eyebrow>
          <PageHeaderTitle>
            Your AI assistant, wired into your work
          </PageHeaderTitle>
          <PageHeaderDescription>
            Generic AI doesn&apos;t know how your company runs.{" "}
            {SETUP_PRODUCT_NAME} connects the assistant you already use to the
            company apps your teams rely on — with your permission.
          </PageHeaderDescription>
        </PageHeaderContent>
      </PageHeader>

      <ul className="max-w-xl space-y-3 text-body-lg text-muted-foreground">
        <li className="flex gap-2">
          <span className="text-foreground" aria-hidden>
            ·
          </span>
          <span>Ask in plain English in Cursor, Claude, or Codex</span>
        </li>
        <li className="flex gap-2">
          <span className="text-foreground" aria-hidden>
            ·
          </span>
          <span>Answers from real company systems — not generic web results</span>
        </li>
        <li className="flex gap-2">
          <span className="text-foreground" aria-hidden>
            ·
          </span>
          <span>You choose which apps to connect</span>
        </li>
      </ul>

      <p className="text-sm text-muted-foreground">About 5 minutes</p>

      <StepPager variant="ghost" aria-label="Continue">
        <button
          type="button"
          data-testid="build-welcome-skip"
          onClick={handleSkip}
          className="self-center text-sm text-muted-foreground underline-offset-4 hover:text-foreground hover:underline"
        >
          Skip for now
        </button>
        <StepPagerNext asChild title="Choose your assistant">
          <button
            type="button"
            data-testid="build-welcome-continue"
            onClick={handleContinue}
          />
        </StepPagerNext>
      </StepPager>
    </div>
  );
}

function AssistantPickerStepActions({
  selectedAgent,
  onSelectedAgent,
}: {
  selectedAgent: BuildInstallAgentId | "";
  onSelectedAgent: (id: BuildInstallAgentId) => void;
}) {
  // Third-party product marks (not tenant palette). Cursor is monochrome.
  const agents: {
    id: BuildInstallAgentId;
    label: string;
    testId: string;
    icon: ReactNode;
  }[] = [
    {
      id: "cursor",
      label: "Cursor",
      testId: "build-install-card-cursor",
      icon: <CursorIcon className="size-12 shrink-0 text-foreground" />,
    },
    {
      id: "claude",
      label: "Claude Code",
      testId: "build-install-card-claude",
      icon: <ClaudeIcon className="size-12 shrink-0 text-[#D97757]" />,
    },
    {
      id: "codex",
      label: "Codex",
      testId: "build-install-card-codex",
      icon: <CodexIcon className="size-12 shrink-0" />,
    },
    {
      id: "other",
      label: "Other",
      testId: "build-install-card-other",
      icon: (
        <span className="flex size-12 shrink-0 items-center justify-center">
          <MoreHorizontalIcon className="size-6 text-muted-foreground" />
        </span>
      ),
    },
  ];

  return (
    <RadioGroup
      value={selectedAgent || undefined}
      onValueChange={(value) => onSelectedAgent(value as BuildInstallAgentId)}
      className="grid grid-cols-2 gap-3 sm:grid-cols-4"
      data-testid="build-install-radio"
      aria-label="Choose your assistant"
    >
      {agents.map((agent) => {
        const inputId = `build-assistant-${agent.id}`;
        return (
          <Label
            key={agent.id}
            htmlFor={inputId}
            data-testid={agent.testId}
            className={cn(
              choiceCardNoIndicatorClassName,
              choiceCardHoverClassName,
              "h-full items-center text-center",
            )}
          >
            <RadioGroupItem
              focusRing="none"
              value={agent.id}
              id={inputId}
              className={choiceCardRadioHiddenClassName}
              aria-label={agent.label}
            />
            <div
              className={cn(
                choiceCardContentNoIndicatorClassName,
                "items-center gap-2.5",
              )}
            >
              {agent.icon}
              <span
                data-choice-title
                className="text-sm font-medium text-foreground"
              >
                {agent.label}
              </span>
            </div>
          </Label>
        );
      })}
    </RadioGroup>
  );
}

function SingleAgentMcpInstall({
  agent,
  apiToken,
  hasMcpCredential,
  onMarkMcpInstalled,
}: {
  agent: BuildInstallAgentId;
  apiToken: string;
  hasMcpCredential: boolean;
  onMarkMcpInstalled: () => void;
}) {
  const mcpBase = gestaltMcpBaseUrl();
  const mcpUrl = `${mcpBase}/mcp`;
  const tokenForSnippets = hasMcpCredential ? apiToken : "gst_api_YOUR_TOKEN";
  const cursorInstallHref = hasMcpCredential
    ? cursorMcpInstallHref(mcpUrl, apiToken)
    : null;
  const [cursorMethod, setCursorMethod] = useState<"open" | "paste">(
    hasMcpCredential ? "open" : "paste",
  );

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
    <div className="w-full space-y-4" data-testid="build-mcp-install-single">
      {agent === "cursor" ? (
        <div className="space-y-4">
          {!hasMcpCredential ? (
            <Alert variant="info" data-testid="build-install-token-needed">
              <AlertTitle>Create a token first</AlertTitle>
              <AlertDescription>
                We can only add Gestalt in Cursor with a token created in this
                session. Existing tokens cannot be shown again.
              </AlertDescription>
              <AlertActions>
                <Button asChild variant="secondary" size="sm">
                  <Link to="/setup/$stepId" params={{ stepId: "token" }}>
                    Create an API token
                  </Link>
                </Button>
              </AlertActions>
            </Alert>
          ) : null}

          <RadioGroup
            value={cursorMethod}
            onValueChange={(value) =>
              setCursorMethod(value as "open" | "paste")
            }
            className="flex max-w-xl flex-col gap-2"
            data-testid="build-install-cursor-method"
            aria-label="How to add Gestalt in Cursor"
          >
            <div className={choiceCardFormShellClassName}>
              <Label
                htmlFor="build-install-open-cursor"
                className={cn(
                  "flex cursor-pointer flex-col gap-1 p-4 leading-normal",
                  radioLabelWrappedDisabledClassName,
                )}
              >
                <RadioGroupItem
                  focusRing="none"
                  value="open"
                  id="build-install-open-cursor"
                  disabled={!hasMcpCredential}
                  className={choiceCardRadioHiddenClassName}
                  aria-label="Open Cursor"
                />
                <div className={choiceCardContentNoIndicatorClassName}>
                  <span
                    data-choice-title
                    className="text-sm font-medium text-foreground"
                  >
                    Open Cursor
                  </span>
                  <span
                    data-choice-desc
                    className="text-sm font-normal text-muted-foreground"
                  >
                    Adds Gestalt in one click.
                  </span>
                </div>
              </Label>
              <Collapsible open={cursorMethod === "open"}>
                <CollapsibleContent
                  className={choiceCardFormFieldsClassName}
                  onPointerDown={(event) => event.stopPropagation()}
                >
                  {cursorInstallHref ? (
                    <Button asChild>
                      <a
                        href={cursorInstallHref}
                        data-testid="build-add-to-cursor"
                        onClick={() => onMarkMcpInstalled()}
                      >
                        Add in Cursor
                      </a>
                    </Button>
                  ) : (
                    <Button
                      type="button"
                      disabled
                      data-testid="build-add-to-cursor"
                      title="Create a token first so Cursor can sign in."
                    >
                      Add in Cursor
                    </Button>
                  )}
                </CollapsibleContent>
              </Collapsible>
            </div>

            <div className={choiceCardFormShellClassName}>
              <Label
                htmlFor="build-install-paste-cursor"
                className={cn(
                  "flex cursor-pointer flex-col gap-1 p-4 leading-normal",
                  radioLabelWrappedDisabledClassName,
                )}
              >
                <RadioGroupItem
                  focusRing="none"
                  value="paste"
                  id="build-install-paste-cursor"
                  className={choiceCardRadioHiddenClassName}
                  aria-label="Paste the config yourself"
                />
                <div className={choiceCardContentNoIndicatorClassName}>
                  <span
                    data-choice-title
                    className="text-sm font-medium text-foreground"
                  >
                    Paste the config yourself
                  </span>
                  <span
                    data-choice-desc
                    className="text-sm font-normal text-muted-foreground"
                  >
                    Copy this into Cursor if you cannot use the button.
                  </span>
                </div>
              </Label>
              <Collapsible open={cursorMethod === "paste"}>
                <CollapsibleContent
                  className={choiceCardFormFieldsClassName}
                  onPointerDown={(event) => event.stopPropagation()}
                >
                  <CodeBlock
                    variant="outline"
                    code={cursorConfig}
                    language="json"
                    filename=".cursor/mcp.json"
                  />
                </CollapsibleContent>
              </Collapsible>
            </div>
          </RadioGroup>
        </div>
      ) : null}

      {agent === "claude" ? (
        <div data-testid="build-install-claude-snippet">
          <CodeBlock
            variant="outline"
            chrome="inset"
            code={claudeCommand}
            language="bash"
          />
        </div>
      ) : null}

      {agent === "codex" ? (
        <div data-testid="build-install-codex-snippet">
          <CodeBlock
            variant="outline"
            chrome="inset"
            code={codexCommand}
            language="bash"
          />
        </div>
      ) : null}

      {agent === "other" ? (
        <div className="space-y-2">
          <CopyableCode value={mcpUrl} tooltip="Copy connection URL" />
        </div>
      ) : null}

      <p className="flex items-start gap-2 text-sm text-muted-foreground text-pretty">
        <Lightbulb className="mt-0.5 size-4 shrink-0" aria-hidden />
        <span>
          This lets your assistant talk to {SETUP_PRODUCT_NAME} with your
          token. See the{" "}
          <UiLink asChild>
            <Link to={`${DOCS_PATH}/mcp`}>setup notes</Link>
          </UiLink>{" "}
          if you need more detail.
        </span>
      </p>
    </div>
  );
}

function BuildStepPanel({
  step,
  tokensReady,
  integrationsReady,
  integrations,
  tokens,
  activeExemplar,
  activeExemplarId,
  onSelectExemplar,
  apiToken,
  apiTokenGrantId,
  onApiToken,
  tokenName,
  onTokenName,
  selectedTokenId,
  onSelectedTokenId,
  selectedInstallAgent,
  onSelectedInstallAgent,
  onRefreshTokens,
  onMarkMcpInstalled,
  onMarkWelcomeSeen,
  onGoToStep,
}: {
  step: BuildStep;
  tokensReady: boolean;
  integrationsReady: boolean;
  integrations: Integration[];
  tokens: APIToken[];
  activeExemplar: BuildExemplar;
  activeExemplarId: BuildExemplarId;
  onSelectExemplar: (id: BuildExemplarId) => void;
  apiToken: string;
  apiTokenGrantId: string;
  onApiToken: (token: string, grantId?: string) => void;
  tokenName: string;
  onTokenName: (name: string) => void;
  selectedTokenId: string;
  onSelectedTokenId: (id: string) => void;
  selectedInstallAgent: BuildInstallAgentId | "";
  onSelectedInstallAgent: (id: BuildInstallAgentId | "") => void;
  onRefreshTokens: () => void | Promise<void>;
  onMarkMcpInstalled: () => void;
  onMarkWelcomeSeen: () => void;
  onGoToStep: (id: BuildStepId) => void;
}) {
  const authorizeReady = buildAuthorizeSelectionReady({
    apiToken,
    apiTokenGrantId,
    selectedTokenId,
  });
  const mcpCredentialReady = buildMcpCredentialReady({
    apiToken,
    apiTokenGrantId,
    selectedTokenId,
  });
  const installReady = buildInstallAgentSelected(selectedInstallAgent);

  function handleStepNext(id: BuildStepId) {
    if (step.id === "install") {
      const from = BUILD_STEPS.findIndex((s) => s.id === step.id);
      const to = BUILD_STEPS.findIndex((s) => s.id === id);
      if (to > from) onMarkMcpInstalled();
    }
    onGoToStep(id);
  }

  return (
    <section
      data-testid="build-step-panel"
      className="space-y-3"
      aria-busy={
        (step.id === "token" && !tokensReady) ||
        ((step.id === "apps" || step.id === "try") &&
          (!tokensReady || !integrationsReady))
      }
    >
      {step.id === "welcome" ? (
        <WelcomeStorytelling
          onMarkWelcomeSeen={onMarkWelcomeSeen}
          onGoToStep={onGoToStep}
        />
      ) : null}

      {step.id === "assistant" ? (
        <AssistantPickerStepActions
          selectedAgent={selectedInstallAgent}
          onSelectedAgent={onSelectedInstallAgent}
        />
      ) : null}

      {step.id === "token" ? (
        <AuthorizeStepActions
          tokens={tokens}
          tokensLoaded={tokensReady}
          tokenName={tokenName}
          onTokenName={onTokenName}
          selectedTokenId={selectedTokenId}
          onSelectedTokenId={onSelectedTokenId}
          onApiToken={onApiToken}
          onTokensChanged={onRefreshTokens}
        />
      ) : null}

      {step.id === "install" &&
      buildInstallAgentSelected(selectedInstallAgent) ? (
        <SingleAgentMcpInstall
          agent={selectedInstallAgent as BuildInstallAgentId}
          apiToken={apiToken}
          hasMcpCredential={mcpCredentialReady}
          onMarkMcpInstalled={onMarkMcpInstalled}
        />
      ) : null}

      {step.id === "apps" ? (
        <ConnectStepActions
          exemplar={activeExemplar}
          integrations={integrations}
          catalogReady={integrationsReady}
        />
      ) : null}

      {step.id === "try" ? (
        <InvokeStepActions
          exemplar={activeExemplar}
          integrations={integrations}
          installAgentId={selectedInstallAgent}
        />
      ) : null}

      {step.id !== "welcome" ? (
        <BuildStepPager
          stepId={step.id}
          installAgentId={selectedInstallAgent}
          onGoToStep={(id) => {
            void handleStepNext(id);
          }}
          terminalNext={
            step.id === "try"
              ? { label: "Browse apps", to: "/apps" }
              : undefined
          }
          nextDisabled={
            (step.id === "assistant" && !installReady) ||
            (step.id === "token" && !authorizeReady) ||
            (step.id === "apps" &&
              !setupAppsConnected({ integrations }) &&
              setupAppsHasConnectable({ integrations }))
          }
          nextDisabledTitle={
            step.id === "assistant"
              ? "Choose your assistant before continuing"
              : step.id === "token"
                ? "Create a token so we can add it in your assistant"
                : step.id === "apps"
                  ? "Connect at least one app to continue"
                  : undefined
          }
        />
      ) : null}
    </section>
  );
}

function BuildStepPager({
  stepId,
  installAgentId,
  onGoToStep,
  terminalNext,
  nextDisabled = false,
  nextDisabledTitle,
}: {
  stepId: BuildStepId;
  installAgentId: string;
  onGoToStep: (id: BuildStepId) => void;
  /** Last-step exit CTA in the Next slot when there is no following build step. */
  terminalNext?: { label: string; to: string };
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
  if (!prev && !next && !terminalNext) return null;
  const prevTitle = prev ? buildStepTitle(prev, installAgentId) : "";
  const nextTitle = next ? buildStepTitle(next, installAgentId) : "";

  return (
    <StepPager
      variant="ghost"
      aria-label="Setup step navigation"
      data-testid="build-step-pager"
      className="mt-8"
    >
      {prev ? (
        <StepPagerPrevious
          asChild
          title={prevTitle}
          data-testid="build-step-prev"
        >
          <button type="button" onClick={() => onGoToStep(prev.id)} />
        </StepPagerPrevious>
      ) : (
        <StepPagerStartSpacer />
      )}
      {next ? (
        <StepPagerNext
          asChild
          title={nextTitle}
          data-testid="build-step-next"
        >
          <button
            type="button"
            onClick={() => onGoToStep(next.id)}
            disabled={nextDisabled}
            aria-disabled={nextDisabled}
            title={nextDisabled ? nextDisabledTitle : undefined}
          />
        </StepPagerNext>
      ) : terminalNext ? (
        <StepPagerNext
          asChild
          title={terminalNext.label}
          data-testid="build-step-next"
        >
          <Link to={terminalNext.to} />
        </StepPagerNext>
      ) : null}
    </StepPager>
  );
}

function tokenChoiceTitle(token: APIToken, knownName?: string): string {
  const name = token.name?.trim() || knownName?.trim();
  if (name && name !== token.id) return name;
  return token.id;
}

function isListedTokenId(value: string, tokens: APIToken[]): boolean {
  const trimmed = value.trim();
  return tokens.some((token) => token.id === trimmed);
}

function createDraftTokenName(current: string, tokens: APIToken[]): string {
  if (!current.trim() || isListedTokenId(current, tokens)) {
    return DEFAULT_BUILD_TOKEN_NAME;
  }
  return current;
}

function tokenRecencyMs(token: APIToken): number {
  const ms = Date.parse(token.createdAt);
  return Number.isFinite(ms) ? ms : 0;
}

function sortTokensByRecency(tokens: APIToken[]): APIToken[] {
  return [...tokens].sort((a, b) => tokenRecencyMs(b) - tokenRecencyMs(a));
}

const BUILD_EXISTING_TOKEN_PREVIEW = 5;
const BUILD_MORE_TOKENS_ACCORDION_VALUE = "more-tokens";

function ExistingTokenRadioRow({
  token,
  knownName,
}: {
  token: APIToken;
  knownName?: string;
}) {
  const inputId = `build-token-${token.id}`;
  const title = tokenChoiceTitle(token, knownName);
  const showId = title !== token.id;

  return (
    <label
      htmlFor={inputId}
      className="flex cursor-pointer items-start gap-3"
    >
      <RadioGroupItem
        value={token.id}
        id={inputId}
        className="mt-0.5"
        aria-label={showId ? `${title} (${token.id})` : token.id}
      />
      <span className="min-w-0">
        <span
          className="block truncate text-sm font-medium text-foreground"
          title={title}
        >
          {title}
        </span>
        {showId ? (
          <span
            className="block truncate font-mono text-xs text-muted-foreground"
            title={token.id}
          >
            {token.id}
          </span>
        ) : null}
      </span>
    </label>
  );
}

function AuthorizeStepActions({
  tokens,
  tokensLoaded,
  onApiToken,
  tokenName,
  onTokenName,
  selectedTokenId,
  onSelectedTokenId,
  onTokensChanged,
}: {
  tokens: APIToken[];
  tokensLoaded: boolean;
  onApiToken: (token: string, grantId?: string) => void;
  tokenName: string;
  onTokenName: (name: string) => void;
  selectedTokenId: string;
  onSelectedTokenId: (id: string) => void;
  onTokensChanged: () => void | Promise<void>;
}) {
  const [moreTokensOpen, setMoreTokensOpen] = useState<string | undefined>(
    undefined,
  );
  const hasTokens = tokens.length > 0;
  const sortedTokens = useMemo(() => sortTokensByRecency(tokens), [tokens]);
  const previewTokens = useMemo(
    () => sortedTokens.slice(0, BUILD_EXISTING_TOKEN_PREVIEW),
    [sortedTokens],
  );
  const overflowTokens = useMemo(
    () => sortedTokens.slice(BUILD_EXISTING_TOKEN_PREVIEW),
    [sortedTokens],
  );

  const authorizeMode = !hasTokens
    ? BUILD_CREATE_NEW_TOKEN_ID
    : selectedTokenId === BUILD_CREATE_NEW_TOKEN_ID
      ? BUILD_CREATE_NEW_TOKEN_ID
      : selectedTokenId === BUILD_USE_EXISTING_TOKEN_ID ||
          tokens.some((token) => token.id === selectedTokenId)
        ? BUILD_USE_EXISTING_TOKEN_ID
        : undefined;

  useEffect(() => {
    if (!tokensLoaded || hasTokens) return;
    if (selectedTokenId === BUILD_CREATE_NEW_TOKEN_ID) return;
    onSelectedTokenId(BUILD_CREATE_NEW_TOKEN_ID);
    onTokenName(createDraftTokenName(tokenName, tokens));
  }, [
    tokensLoaded,
    hasTokens,
    selectedTokenId,
    tokenName,
    tokens,
    onSelectedTokenId,
    onTokenName,
  ]);

  useEffect(() => {
    if (!tokensLoaded) return;
    if (authorizeMode !== BUILD_CREATE_NEW_TOKEN_ID) return;
    const nextName = createDraftTokenName(tokenName, tokens);
    if (nextName !== tokenName) onTokenName(nextName);
  }, [
    tokensLoaded,
    authorizeMode,
    tokenName,
    tokens,
    onTokenName,
  ]);

  const selectedExistingTokenId = tokens.some(
    (token) => token.id === selectedTokenId,
  )
    ? selectedTokenId
    : undefined;

  useEffect(() => {
    if (authorizeMode !== BUILD_USE_EXISTING_TOKEN_ID) {
      setMoreTokensOpen(undefined);
    }
  }, [authorizeMode]);

  useEffect(() => {
    if (
      selectedExistingTokenId &&
      overflowTokens.some((token) => token.id === selectedExistingTokenId)
    ) {
      setMoreTokensOpen(BUILD_MORE_TOKENS_ACCORDION_VALUE);
    }
  }, [selectedExistingTokenId, overflowTokens]);

  async function handleTokenCreated(
    plaintext: string,
    created: { id: string; name: string },
  ) {
    onSelectedTokenId(created.id);
    onApiToken(plaintext, created.id);
    onTokenName(created.name);
    toast.success("Token created.");
    await onTokensChanged();
  }

  function selectExistingMode() {
    const first = sortedTokens[0];
    if (first) {
      selectExistingToken(first);
      return;
    }
    onSelectedTokenId(BUILD_USE_EXISTING_TOKEN_ID);
  }

  function selectCreateMode() {
    onSelectedTokenId(BUILD_CREATE_NEW_TOKEN_ID);
    onTokenName(createDraftTokenName(tokenName, tokens));
  }

  function selectExistingToken(token: APIToken) {
    onSelectedTokenId(token.id);
  }

  function knownNameFor(token: APIToken): string | undefined {
    if (token.id !== selectedExistingTokenId) return undefined;
    if (!tokenName.trim() || tokenName.trim() === token.id) return undefined;
    return tokenName;
  }

  return (
    <div className="space-y-8">
      {!tokensLoaded ? (
        <p className="text-sm text-faint">Loading tokens…</p>
      ) : (
        <div className="max-w-xl">
          <RadioGroup
            value={authorizeMode}
            onValueChange={(value) => {
              if (value === BUILD_CREATE_NEW_TOKEN_ID) {
                selectCreateMode();
                return;
              }
              selectExistingMode();
            }}
            className="flex flex-col gap-2"
            data-testid="build-token-radio"
            aria-label="Choose how to authorize"
          >
            {hasTokens ? (
              <div className={choiceCardFormShellClassName}>
                <Label
                  htmlFor="build-authorize-existing"
                  className={cn(
                    "flex cursor-pointer flex-col gap-1 p-4 leading-normal",
                    radioLabelWrappedDisabledClassName,
                  )}
                >
                  <RadioGroupItem
                    focusRing="none"
                    value={BUILD_USE_EXISTING_TOKEN_ID}
                    id="build-authorize-existing"
                    className={choiceCardRadioHiddenClassName}
                    aria-label="Use existing token"
                  />
                  <div className={choiceCardContentNoIndicatorClassName}>
                    <span
                      data-choice-title
                      className="text-sm font-medium text-foreground"
                    >
                      Use existing token
                    </span>
                  </div>
                </Label>
                <Collapsible
                  open={authorizeMode === BUILD_USE_EXISTING_TOKEN_ID}
                >
                  <CollapsibleContent
                    className={choiceCardFormFieldsClassName}
                    onPointerDown={(event) => event.stopPropagation()}
                  >
                    <div className="space-y-2">
                      <RadioGroup
                        value={selectedExistingTokenId}
                        onValueChange={(value) => {
                          const token = tokens.find((item) => item.id === value);
                          if (token) selectExistingToken(token);
                        }}
                        className="flex flex-col gap-2"
                        data-testid="build-existing-token-list"
                        aria-label="Existing tokens"
                      >
                        {previewTokens.map((token) => (
                          <ExistingTokenRadioRow
                            key={token.id}
                            token={token}
                            knownName={knownNameFor(token)}
                          />
                        ))}
                        {overflowTokens.length > 0 ? (
                          <Accordion
                            type="single"
                            collapsible
                            value={moreTokensOpen}
                            onValueChange={setMoreTokensOpen}
                            className="w-full"
                          >
                            <AccordionItem
                              value={BUILD_MORE_TOKENS_ACCORDION_VALUE}
                              className="border-none"
                            >
                              <AccordionTrigger
                                className="px-0 py-1 text-sm font-normal text-muted-foreground hover:text-foreground"
                                data-testid="build-existing-token-expand"
                              >
                                Show {overflowTokens.length} more token
                                {overflowTokens.length === 1 ? "" : "s"}
                              </AccordionTrigger>
                              <AccordionContent className="flex flex-col gap-2 px-0 pb-0">
                                {overflowTokens.map((token) => (
                                  <ExistingTokenRadioRow
                                    key={token.id}
                                    token={token}
                                    knownName={knownNameFor(token)}
                                  />
                                ))}
                              </AccordionContent>
                            </AccordionItem>
                          </Accordion>
                        ) : null}
                      </RadioGroup>
                    </div>
                  </CollapsibleContent>
                </Collapsible>
              </div>
            ) : null}

            <div className={choiceCardFormShellClassName}>
              <Label
                htmlFor="build-authorize-create"
                className={cn(
                  "flex cursor-pointer flex-col gap-1 p-4 leading-normal",
                  radioLabelWrappedDisabledClassName,
                )}
              >
                <RadioGroupItem
                  focusRing="none"
                  value={BUILD_CREATE_NEW_TOKEN_ID}
                  id="build-authorize-create"
                  className={choiceCardRadioHiddenClassName}
                  aria-label="Create new token"
                />
                <div className={choiceCardContentNoIndicatorClassName}>
                  <span
                    data-choice-title
                    className="text-sm font-medium text-foreground"
                  >
                    Create new token
                  </span>
                </div>
              </Label>
              <Collapsible open={authorizeMode === BUILD_CREATE_NEW_TOKEN_ID}>
                <CollapsibleContent
                  className={choiceCardFormFieldsClassName}
                  onPointerDown={(event) => event.stopPropagation()}
                >
                  <TokenCreateForm
                    name={tokenName}
                    onNameChange={onTokenName}
                    defaultName={DEFAULT_BUILD_TOKEN_NAME}
                    onCreated={handleTokenCreated}
                    showPlaintextResult={false}
                    fieldOrientation="horizontal"
                  />
                </CollapsibleContent>
              </Collapsible>
            </div>
          </RadioGroup>
        </div>
      )}
    </div>
  );
}

function InvokeStepActions({
  exemplar,
  integrations,
  installAgentId,
}: {
  exemplar: BuildExemplar;
  integrations: Integration[];
  installAgentId: BuildInstallAgentId | "";
}) {
  const integration = integrations.find((item) => item.name === exemplar.id);
  const open = resolveExemplarOpenPath(exemplar, integration);
  const displayName = integration?.displayName?.trim() || exemplar.label;
  const invokeAppLabel =
    integrations.find((item) => item.name === exemplar.invokeAppId)
      ?.displayName?.trim() || exemplar.invokeAppId;
  const agentSkin = buildAgentSkin(installAgentId);
  const cwd = `~/${exemplar.department.toLowerCase().replace(/\s+/g, "-")}`;

  return (
    <div className="space-y-16" data-testid="build-first-call">
      <div className="space-y-5" data-testid="build-golden-prompt">
        <p className="text-body-lg font-normal text-muted-foreground text-pretty">
          Prompt your favorite LLM with{" "}
          <CopyableCode value={exemplar.llmPrompt} tooltip="Copy prompt" />{" "}
          and it should reply like in this example below.
        </p>

        <div
          className="min-h-[16rem] w-full"
          data-testid="build-agent-console-reply"
          data-agent-skin={agentSkin}
        >
          <BuildAgentConsolePreview
            variant={agentSkin}
            prompt={exemplar.llmPrompt}
            reply={exemplar.expectedResult}
            cwd={cwd}
          />
        </div>

        <p className="text-body-lg font-normal text-muted-foreground text-pretty">
          Behind the scenes this calls{" "}
          <InvokeOperationReference
            appId={exemplar.invokeAppId}
            operationId={exemplar.operationId}
            appLabel={invokeAppLabel}
          />
          .
        </p>

        <Alert
          collapsible
          defaultOpen
          variant="outline"
          className="w-full"
          data-testid="build-cli-alert"
        >
          <AlertTrigger>
            <AlertTitle>How to do it with the CLI</AlertTitle>
          </AlertTrigger>
          <AlertCollapsibleContent>
            <AlertDescription>
              If you want to use the CLI instead, do it this way:
            </AlertDescription>
            <CodeBlock
              variant="outline"
              code={exemplar.invokeRecipe}
              language="cli"
              filename="Terminal"
            />
          </AlertCollapsibleContent>
        </Alert>
      </div>

      <div className="space-y-6" data-testid="build-shipped-app">
        <SectionHeader>
          <SectionHeaderContent>
            <SectionHeaderTitle>Already shipped</SectionHeaderTitle>
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

      <div className="space-y-6" data-testid="build-related-apps">
        <SectionHeader>
          <SectionHeaderContent>
            <SectionHeaderTitle>Related apps</SectionHeaderTitle>
            <SectionHeaderDescription>
              More apps that fit this outcome — open one, or browse the full store.
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
        "focus-ring rounded-xl",
      )}
    >
      <IntegrationIcon
        iconSvg={iconSvg}
        name={name}
        displayName={label}
        size="xl"
        variant="bare"
      />
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
  catalogReady: boolean;
}) {
  const invalidateIntegrations = useInvalidateIntegrations();
  const [visibleMoreCount, setVisibleMoreCount] = useState(SETUP_APPS_PAGE_SIZE);
  const [categoryFilter, setCategoryFilter] = useState(SETUP_APPS_CATEGORY_ALL);
  const returnPath = `${SETUP_PATH}/apps`;

  async function refreshIntegrations() {
    await invalidateIntegrations();
  }

  function selectCategory(next: string) {
    setCategoryFilter(next || SETUP_APPS_CATEGORY_ALL);
    setVisibleMoreCount(SETUP_APPS_PAGE_SIZE);
  }

  if (!catalogReady) {
    return <p className="text-sm text-faint">Loading apps…</p>;
  }

  const companionIds = new Set(exemplar.companionAppIds);
  const suggested = exemplar.companionAppIds.flatMap((appId) => {
    const integration = integrations.find((item) => item.name === appId);
    if (integration && !isSetupDataSourceApp(integration)) return [];
    return [{ appId, integration }];
  });
  const more = setupDataSourceIntegrations(integrations)
    .filter((integration) => !companionIds.has(integration.name))
    .sort((a, b) =>
      getIntegrationLabel(a).localeCompare(getIntegrationLabel(b)),
    );
  const categoryChips = catalogBucketsPresentIn(more);
  const effectiveCategory =
    categoryFilter === SETUP_APPS_CATEGORY_ALL ||
    categoryChips.some((bucket) => bucket.id === categoryFilter)
      ? categoryFilter
      : SETUP_APPS_CATEGORY_ALL;
  const activeCategory =
    effectiveCategory === SETUP_APPS_CATEGORY_ALL
      ? null
      : (CATALOG_BUCKETS.find((bucket) => bucket.id === effectiveCategory) ??
        null);
  const filteredMore =
    effectiveCategory === SETUP_APPS_CATEGORY_ALL
      ? more
      : more.filter(
          (integration) => catalogBucketIdFor(integration) === effectiveCategory,
        );
  const visibleMore = filteredMore.slice(0, visibleMoreCount);
  const remainingMore = filteredMore.slice(visibleMoreCount);
  const missingFromCatalog = suggested.filter((item) => !item.integration);
  const moreSectionTitle = activeCategory?.label ?? "More apps";

  function renderConnectCard(appId: string, integration: Integration | undefined) {
    if (!integration) {
      return (
        <div
          key={appId}
          className="rounded-xl bg-neutral-hover p-3 text-foreground"
          data-testid={`build-connect-app-${appId}`}
        >
          <div className="flex items-start gap-3">
            <IntegrationIcon
              name={appId}
              displayName={companionAppLabel(appId)}
              size="md"
              variant="bare"
            />
            <div className="min-w-0 flex-1">
              <p className="text-sm font-heading text-foreground">
                {companionAppLabel(appId)}
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
      <div key={appId} data-testid={`build-connect-app-${appId}`}>
        <IntegrationCard
          integration={integration}
          returnPath={returnPath}
          onConnected={() => void refreshIntegrations()}
          onDisconnected={() => void refreshIntegrations()}
          connectionEntry="modal"
          density="compact"
        />
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-10" data-testid="build-connect-apps">
      {suggested.length > 0 ? (
        <div className="space-y-6">
          <SectionHeader>
            <SectionHeaderContent>
              <SectionHeaderTitle>Suggested</SectionHeaderTitle>
            </SectionHeaderContent>
          </SectionHeader>
          <div className={SETUP_APPS_GRID_CLASS}>
            {suggested.map(({ appId, integration }) =>
              renderConnectCard(appId, integration),
            )}
          </div>
        </div>
      ) : null}

      {more.length > 0 ? (
        <div className="space-y-6">
          {categoryChips.length > 0 ? (
            <ChipGroup
              type="single"
              size="sm"
              value={effectiveCategory}
              onValueChange={selectCategory}
              aria-label="Filter apps by category"
              data-testid="build-apps-category-chips"
            >
              <ChipGroupItem
                value={SETUP_APPS_CATEGORY_ALL}
                data-testid="build-apps-category-all"
              >
                All
              </ChipGroupItem>
              {categoryChips.map((bucket) => (
                <ChipGroupItem
                  key={bucket.id}
                  value={bucket.id}
                  data-testid={`build-apps-category-${bucket.id}`}
                >
                  {bucket.label}
                </ChipGroupItem>
              ))}
            </ChipGroup>
          ) : null}

          <SectionHeader>
            <SectionHeaderContent>
              <SectionHeaderTitle>{moreSectionTitle}</SectionHeaderTitle>
            </SectionHeaderContent>
          </SectionHeader>

          {filteredMore.length === 0 ? (
            <div className="space-y-3" data-testid="build-apps-category-empty">
              <p className="text-sm font-medium text-foreground">
                No apps in this category
              </p>
              <p className="text-sm text-muted-foreground">
                Try another category, or choose All to see every app.
              </p>
              <Button
                type="button"
                variant="secondary"
                size="sm"
                onClick={() => selectCategory(SETUP_APPS_CATEGORY_ALL)}
              >
                Show all apps
              </Button>
            </div>
          ) : (
            <>
              <div className={SETUP_APPS_GRID_CLASS}>
                {visibleMore.map((integration) =>
                  renderConnectCard(integration.name, integration),
                )}
              </div>
              {remainingMore.length > 0 ? (
                <SeeMoreAppsTrigger
                  remaining={remainingMore}
                  onSeeMore={() =>
                    setVisibleMoreCount((count) => count + SETUP_APPS_PAGE_SIZE)
                  }
                />
              ) : null}
            </>
          )}
        </div>
      ) : null}

      {missingFromCatalog.length > 0 ? (
        <p className="text-sm text-muted-foreground">
          Ask an admin to add missing apps to this workspace before you
          continue.
        </p>
      ) : null}
    </div>
  );
}

