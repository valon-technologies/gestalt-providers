import { useEffect, useId, useState, type ReactNode } from "react";
import { Clock, RotateCcw } from "lucide-react";
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
import {
  Alert,
  AlertCollapsibleContent,
  AlertDescription,
  AlertTitle,
  AlertTrigger,
} from "@/components/ui/alert";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  StepPager,
  StepPagerNext,
  StepPagerPrevious,
  StepPagerStartSpacer,
} from "@/components/ui/step-pager";
import {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderDescription,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import { Eyebrow } from "@/components/ui/eyebrow";
import { CodeBlock } from "@/components/ui/code-block";
import { CopyableCode } from "@/components/ui/copyable-code";
import {
  TimelineSteps,
  TimelineStepsContent,
  TimelineStepsHeader,
  TimelineStepsIcon,
  TimelineStepsItem,
  TimelineStepsTitle,
} from "@/components/ui/timeline-steps";
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
  SETUP_TYPESET_CHROME_CLASS,
  SETUP_TYPESET_CLASS,
  SETUP_TYPESET_NESTED_CHROME_CLASS,
} from "@/features/setup/setup-typeset";
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
import { SpinnerIcon } from "@/components/icons";
import {
  AssistantPickerStepActions,
  SingleAgentMcpInstall,
} from "@/features/setup/assistant-install";
import { SetupOverlapCallout } from "@/features/setup/overlap-callout";
import {
  SETUP_TOKEN_CREATED_CONTENT_CLASS,
  SETUP_TOKEN_CREATE_CONTENT_CLASS,
  SETUP_TOKEN_CREATE_TRACK,
} from "@/features/setup/token-create-layout";
import { useBuildSession } from "@/hooks/use-build-session";
import { useDocumentTitle } from "@/hooks/use-document-title";
import {
  appsCatalogQueryStatus,
  connectionOverlayKnown,
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
  BUILD_STEPS,
  buildMcpCredentialReady,
  buildStepDescription,
  buildStepTitle,
  buildWorkspaceSnapshotFromSession,
  canNavigateToBuildStep,
  catalogLoadStateFromQuery,
  companionAppLabel,
  DEFAULT_BUILD_TOKEN_NAME,
  firstIncompleteStepId,
  getExemplar,
  isBuildStepUnlocked,
  isBuildComplete,
  isBuildStepId,
  isLegacySetupConnectStepId,
  resolveCatalogApp,
  resolveExemplarOpenPath,
  SETUP_PRODUCT_NAME,
  isSetupDataSourceApp,
  setupAppsContinueBlockedReason,
  setupDataSourceIntegrations,
  tryStepCatalogApp,
  writeSetupSkipped,
  type BuildExemplar,
  type BuildStep,
  type BuildStepId,
  type BuildWorkspaceSnapshot,
  type CatalogLoadState,
} from "@/lib/buildPaths";
import {
  CONNECT_ANOTHER_ASSISTANT_LABEL,
  SETUP_TOKEN_CREATE_DIFFERENT,
  SETUP_TOKEN_CREATE_ITEM_TITLE,
  SETUP_TOKEN_CREATED_ITEM_TITLE,
  SETUP_TOKEN_CREATED_LEAD,
  SETUP_TOKEN_CREATED_TAIL,
  SETUP_TOKEN_NEXT_DISABLED_TITLE,
  WELCOME_ASSISTANT_EXAMPLES,
} from "@/lib/assistantConnectionCopy";
import {
  assistantHostById,
  isBuildInstallAgentId,
  type AssistantHostConsoleSkin,
  type BuildInstallAgentId,
} from "@/lib/assistantHosts";
import { SETUP_PATH } from "@/lib/constants";
import {
  APPS_CATALOG_UNAVAILABLE,
  CONNECTION_STATUS_UNAVAILABLE,
  TOKENS_UNAVAILABLE,
  userFacingError,
} from "@/lib/user-facing-error";
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
  catalogQuery: { isPending: boolean; isError: boolean },
): BuildWorkspaceSnapshot {
  return buildWorkspaceSnapshotFromSession(
    session,
    integrations,
    tokens,
    catalogLoadStateFromQuery(catalogQuery),
  );
}

/** `/setup` → overview when complete, else first incomplete step. */
export function BuildIndexRedirect() {
  useDocumentTitle("Setup");
  const session = useBuildSession();
  const integrationsQuery = useIntegrationsQuery();
  const tokensQuery = useTokensQuery();
  const catalog = appsCatalogQueryStatus(integrationsQuery);

  const tokensReady = !tokensQuery.isPending;
  const catalogSettled = catalog.status !== "loading";

  // Token setup does not need the apps catalog. Waiting on GET /api/v1/apps
  // here is what turned a hung catalog into an infinite "Loading Build…".
  if (tokensQuery.isPending && !tokensQuery.data) {
    return (
      <Container as="main">
        <div className="mx-auto flex min-h-[50vh] w-full max-w-4xl items-center justify-center">
          <p className="flex items-center gap-2 text-sm text-faint">
            <SpinnerIcon className="size-3.5 animate-spin" aria-hidden />
            Loading setup…
          </p>
        </div>
      </Container>
    );
  }

  if (tokensQuery.isError && !tokensQuery.data) {
    return (
      <Container as="main">
        <div className="mx-auto flex min-h-[50vh] w-full max-w-4xl items-center justify-center">
          <ErrorNotice
            message={userFacingError(tokensQuery.error, TOKENS_UNAVAILABLE)}
            retrying={tokensQuery.isFetching}
            onRetry={() => {
              void tokensQuery.refetch();
            }}
          />
        </div>
      </Container>
    );
  }

  const snapshot = buildSnapshot(
    session,
    catalog.integrations,
    tokensQuery.data ?? [],
    {
      isPending: catalog.status === "loading",
      isError: catalog.status === "unavailable",
    },
  );

  if (isBuildComplete(snapshot)) {
    return <SetupOverview snapshot={snapshot} />;
  }

  const stepId = firstIncompleteStepId(snapshot, (step) =>
    isStepDone(step, snapshot, tokensReady, catalogSettled),
  );

  return <Navigate to="/setup/$stepId" params={{ stepId }} replace />;
}

function SetupStepperList({
  titleForStep,
  itemTestId,
  listTestId,
  isStepReachable,
}: {
  titleForStep: (step: BuildStep) => string;
  itemTestId?: (id: BuildStepId) => string;
  listTestId?: string;
  isStepReachable?: (id: BuildStepId) => boolean;
}) {
  return (
    <StepperList aria-label="Setup steps" data-testid={listTestId}>
      {BUILD_STEPS.map((step) => (
        <StepperItem
          key={step.id}
          value={step.id}
          disabled={isStepReachable ? !isStepReachable(step.id) : false}
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
  isStepReachable,
  children,
}: {
  value: string;
  onValueChange: (next: string) => void;
  titleForStep: (step: BuildStep) => string;
  itemTestId?: (id: BuildStepId) => string;
  listTestId?: string;
  isStepReachable?: (id: BuildStepId) => boolean;
  children: ReactNode;
}) {
  return (
    <Container as="main">
      <div className={PAGE_LAYOUT_READING_COLUMN_CLASS}>
        <Stepper
          value={value}
          onValueChange={onValueChange}
          orientation="horizontal"
          activationMode="linear"
          size="default"
        >
          <SetupStepperList
            titleForStep={titleForStep}
            itemTestId={itemTestId}
            listTestId={listTestId}
            isStepReachable={isStepReachable}
          />
        </Stepper>
        <div className={`${SETUP_TYPESET_CLASS} mt-8`}>{children}</div>
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
      <div
        className={`${SETUP_TYPESET_CHROME_CLASS} space-y-8`}
        data-testid="build-setup-overview"
      >
        <PageHeader>
          <PageHeaderContent size="md">
            <PageHeaderTitle>You&apos;re all set</PageHeaderTitle>
            <PageHeaderDescription>
              Your assistant is connected and your workspace apps are ready to
              use.
            </PageHeaderDescription>
          </PageHeaderContent>
        </PageHeader>

        <StepPager variant="ghost" aria-label="After setup">
          <StepPagerPrevious asChild title={CONNECT_ANOTHER_ASSISTANT_LABEL}>
            <Link to="/setup/$stepId" params={{ stepId: "assistant" }} />
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
    ? buildStepTitle(currentStep, session.installAgentId)
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
  const catalog = appsCatalogQueryStatus(integrationsQuery);
  const catalogSettled = catalog.status !== "loading";
  const catalogHasApps = catalog.integrations.length > 0;

  const snapshot = buildSnapshot(
    session,
    catalog.integrations,
    tokensQuery.data ?? [],
    {
      isPending: catalog.status === "loading",
      isError: catalog.status === "unavailable",
    },
  );

  const tokensError = tokensQuery.error
    ? userFacingError(tokensQuery.error, TOKENS_UNAVAILABLE)
    : null;
  const catalogError =
    catalog.status === "unavailable"
      ? userFacingError(catalog.error, APPS_CATALOG_UNAVAILABLE)
      : null;
  const overlayKnown = connectionOverlayKnown(
    integrationsQuery.overlayEnabled,
    integrationsQuery.overlayPending,
    integrationsQuery.overlayError,
  );
  const overlayError = integrationsQuery.overlayError
    ? userFacingError(
        integrationsQuery.overlayError,
        CONNECTION_STATUS_UNAVAILABLE,
      )
    : null;

  const stepUnlocked = isBuildStepUnlocked(stepId, (step) =>
    step.isComplete(snapshot),
  );
  const catalogGate = stepId === "apps" || stepId === "try";
  if (
    !stepUnlocked &&
    (!catalogGate || (tokensReady && catalogSettled))
  ) {
    const dest = firstIncompleteStepId(snapshot);
    if (dest !== stepId) {
      return (
        <Navigate to="/setup/$stepId" params={{ stepId: dest }} replace />
      );
    }
  }

  const activeExemplar = getExemplar(session.activeExemplarId);

  function goToStep(id: BuildStepId) {
    void navigate({ to: "/setup/$stepId", params: { stepId: id } });
  }

  const stepIsDone = (step: BuildStep) =>
    isStepDone(step, snapshot, tokensReady, catalogSettled);

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
      isStepReachable={(id) =>
        Boolean(stepId && canNavigateToBuildStep(id, stepId, stepIsDone))
      }
    >
      {tokensError ? (
        <div className={SETUP_TYPESET_CHROME_CLASS}>
          <ErrorNotice
            className="mb-8"
            message={tokensError}
            onRetry={() => {
              void tokensQuery.refetch();
            }}
            retrying={tokensQuery.isFetching}
          />
        </div>
      ) : null}

      {stepId !== "welcome" ? (
        <div className={SETUP_TYPESET_CHROME_CLASS}>
          <PageHeader>
            <PageHeaderContent size="md">
              <PageHeaderTitle>
                {buildStepTitle(currentStep, session.installAgentId)}
              </PageHeaderTitle>
              <PageHeaderDescription>
                {buildStepDescription(currentStep, session.installAgentId)}
              </PageHeaderDescription>
            </PageHeaderContent>
          </PageHeader>
        </div>
      ) : null}

        <BuildStepPanel
          step={currentStep}
          tokensReady={tokensReady}
          catalogSettled={catalogSettled}
          catalogHasApps={catalogHasApps}
          catalogError={catalogError}
          catalogRetrying={integrationsQuery.isFetching}
          onRetryCatalog={() => {
            void integrationsQuery.refetchDirectory();
          }}
          overlayKnown={overlayKnown}
          overlayPending={integrationsQuery.overlayPending}
          overlayError={overlayError}
          overlayRetrying={integrationsQuery.overlayFetching}
          onRetryOverlay={() => {
            void integrationsQuery.refetchOverlay();
          }}
          integrations={snapshot.integrations}
          catalogLoadState={snapshot.catalogLoadState}
          activeExemplar={activeExemplar}
          apiToken={session.apiToken}
          apiTokenGrantId={session.apiTokenGrantId}
          onApiToken={session.setApiToken}
          tokenName={session.tokenName}
          onTokenName={session.setTokenName}
          installAgentId={session.installAgentId}
          onInstallAgentId={session.setInstallAgentId}
          onRefreshTokens={refreshTokens}
          onMarkMcpInstalled={session.markMcpInstalled}
          onMarkWelcomeSeen={session.markWelcomeSeen}
          onMarkTrySeen={session.markTrySeen}
          onGoToStep={goToStep}
        />
    </SetupPageChrome>
  );
}

function isStepDone(
  step: BuildStep,
  snapshot: BuildWorkspaceSnapshot,
  tokensReady: boolean,
  catalogSettled: boolean,
): boolean {
  if (!step.isComplete(snapshot)) return false;
  switch (step.id) {
    case "welcome":
      return true;
    case "assistant":
      return isBuildInstallAgentId(snapshot.installAgentId);
    case "token":
      return true;
    case "install":
      return true;
    case "apps":
      return catalogSettled;
    case "try":
      return catalogSettled;
    default:
      return true;
  }
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
function buildAgentSkin(
  installAgentId: BuildInstallAgentId | "",
): AssistantHostConsoleSkin {
  return assistantHostById(installAgentId)?.consoleSkin ?? "claude";
}

function buildAgentProductLabel(
  installAgentId: BuildInstallAgentId | "",
): string {
  return assistantHostById(installAgentId)?.label ?? "Claude";
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
  productLabel,
  prompt,
  reply,
  cwd,
}: {
  variant: AssistantHostConsoleSkin;
  productLabel: string;
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
              {productLabel}
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
            <AgentConsoleProduct>{productLabel}</AgentConsoleProduct>
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
    <div data-testid="build-welcome">
      <div className={SETUP_TYPESET_CHROME_CLASS}>
        <PageHeader>
          <PageHeaderContent size="md">
            <PageHeaderTitle>
              Your AI assistant, wired into your work
            </PageHeaderTitle>
            <PageHeaderDescription>
              Generic AI doesn&apos;t know how your company runs.{" "}
              {SETUP_PRODUCT_NAME} connects the assistant you already use to the
              company apps your teams rely on, with your permission.
            </PageHeaderDescription>
          </PageHeaderContent>
        </PageHeader>
      </div>

      <ul>
        <li>{WELCOME_ASSISTANT_EXAMPLES}</li>
        <li>Answers from real company systems, not generic web results</li>
        <li>You choose which apps to connect</li>
        <li>Switch assistants anytime from Setup in the top nav</li>
      </ul>

      <p
        className={`${SETUP_TYPESET_NESTED_CHROME_CLASS} flex items-center gap-1.5 text-sm text-muted-foreground-soft`}
      >
        <Clock className="size-3.5" aria-hidden />
        About 5 minutes
      </p>

      <div className={SETUP_TYPESET_NESTED_CHROME_CLASS}>
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
    </div>
  );
}


function BuildStepPanel({
  step,
  tokensReady,
  catalogSettled,
  catalogHasApps,
  catalogError,
  catalogRetrying,
  onRetryCatalog,
  overlayKnown,
  overlayPending,
  overlayError,
  overlayRetrying,
  onRetryOverlay,
  integrations,
  catalogLoadState,
  activeExemplar,
  apiToken,
  apiTokenGrantId,
  onApiToken,
  tokenName,
  onTokenName,
  installAgentId,
  onInstallAgentId,
  onRefreshTokens,
  onMarkMcpInstalled,
  onMarkWelcomeSeen,
  onMarkTrySeen,
  onGoToStep,
}: {
  step: BuildStep;
  tokensReady: boolean;
  catalogSettled: boolean;
  catalogHasApps: boolean;
  catalogError: string | null;
  catalogRetrying: boolean;
  onRetryCatalog: () => void;
  overlayKnown: boolean;
  overlayPending: boolean;
  overlayError: string | null;
  overlayRetrying: boolean;
  onRetryOverlay: () => void;
  integrations: Integration[];
  catalogLoadState: CatalogLoadState;
  activeExemplar: BuildExemplar;
  apiToken: string;
  apiTokenGrantId: string;
  onApiToken: (token: string, grantId?: string) => void;
  tokenName: string;
  onTokenName: (name: string) => void;
  installAgentId: BuildInstallAgentId | "";
  onInstallAgentId: (id: BuildInstallAgentId | "") => void;
  onRefreshTokens: () => void | Promise<void>;
  onMarkMcpInstalled: () => void;
  onMarkWelcomeSeen: () => void;
  onMarkTrySeen: () => void;
  onGoToStep: (id: BuildStepId) => void;
}) {
  const mcpCredentialReady = buildMcpCredentialReady({
    apiToken,
    apiTokenGrantId,
  });
  const installReady = isBuildInstallAgentId(installAgentId);
  const appsContinueBlocked = setupAppsContinueBlockedReason({
    integrations,
    catalogLoadState,
  });

  function handleStepNext(id: BuildStepId) {
    if (step.id === "install") {
      if (!mcpCredentialReady) return;
      const from = BUILD_STEPS.findIndex((s) => s.id === step.id);
      const to = BUILD_STEPS.findIndex((s) => s.id === id);
      if (to > from) onMarkMcpInstalled();
    }
    if (step.id === "apps" && appsContinueBlocked) return;
    onGoToStep(id);
  }

  return (
    <section
      data-testid="build-step-panel"
      data-typeset-chrome
      className="space-y-3"
      aria-busy={
        (step.id === "token" && !tokensReady) ||
        ((step.id === "apps" || step.id === "try") &&
          (!tokensReady || !catalogSettled || overlayPending))
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
          selectedAgent={installAgentId}
          onSelectedAgent={onInstallAgentId}
        />
      ) : null}

      {step.id === "token" ? (
        <AuthorizeStepActions
          apiToken={apiToken}
          apiTokenGrantId={apiTokenGrantId}
          tokenName={tokenName}
          onTokenName={onTokenName}
          onApiToken={onApiToken}
          onTokensChanged={onRefreshTokens}
        />
      ) : null}

      {step.id === "install" && isBuildInstallAgentId(installAgentId) ? (
        <SingleAgentMcpInstall
          agent={installAgentId}
          apiToken={apiToken}
          hasMcpCredential={mcpCredentialReady}
          onMarkMcpInstalled={onMarkMcpInstalled}
        />
      ) : null}

      {step.id === "apps" ? (
        <ConnectStepActions
          exemplar={activeExemplar}
          integrations={integrations}
          catalogSettled={catalogSettled}
          catalogHasApps={catalogHasApps}
          catalogError={catalogError}
          catalogRetrying={catalogRetrying}
          onRetryCatalog={onRetryCatalog}
          overlayKnown={overlayKnown}
          overlayError={overlayError}
          overlayRetrying={overlayRetrying}
          onRetryOverlay={onRetryOverlay}
          installAgentId={installAgentId}
        />
      ) : null}

      {step.id === "try" ? (
        catalogError && !catalogHasApps ? (
          <div className={SETUP_TYPESET_CHROME_CLASS}>
            <ErrorNotice
              message={catalogError}
              retrying={catalogRetrying}
              onRetry={onRetryCatalog}
            />
          </div>
        ) : (
          <>
            {catalogError ? (
              <div className={SETUP_TYPESET_CHROME_CLASS}>
                <ErrorNotice
                  className="mb-4"
                  message={catalogError}
                  retrying={catalogRetrying}
                  onRetry={onRetryCatalog}
                />
              </div>
            ) : null}
            {overlayError ? (
              <div className={SETUP_TYPESET_CHROME_CLASS}>
                <ErrorNotice
                  className="mb-4"
                  message={overlayError}
                  retrying={overlayRetrying}
                  onRetry={onRetryOverlay}
                />
              </div>
            ) : null}
            <InvokeStepActions
              exemplar={activeExemplar}
              integrations={integrations}
              installAgentId={installAgentId}
              overlayKnown={overlayKnown}
              onMarkTrySeen={onMarkTrySeen}
            />
          </>
        )
      ) : null}

      {step.id !== "welcome" ? (
        <div className={SETUP_TYPESET_CHROME_CLASS}>
          <BuildStepPager
            stepId={step.id}
            installAgentId={installAgentId}
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
              (step.id === "token" && !mcpCredentialReady) ||
              (step.id === "install" && !mcpCredentialReady) ||
              (step.id === "apps" && appsContinueBlocked !== null)
            }
            nextDisabledTitle={
              step.id === "assistant"
                ? "Choose your assistant before continuing"
                : step.id === "token" || step.id === "install"
                  ? SETUP_TOKEN_NEXT_DISABLED_TITLE
                  : step.id === "apps"
                    ? (appsContinueBlocked ?? undefined)
                    : undefined
            }
          />
        </div>
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
            aria-label={
              nextDisabled && nextDisabledTitle
                ? `${nextTitle}. ${nextDisabledTitle}`
                : undefined
            }
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

function AuthorizeStepActions({
  apiToken,
  apiTokenGrantId,
  onApiToken,
  tokenName,
  onTokenName,
  onTokensChanged,
}: {
  apiToken: string;
  apiTokenGrantId: string;
  onApiToken: (token: string, grantId?: string) => void;
  tokenName: string;
  onTokenName: (name: string) => void;
  onTokensChanged: () => void | Promise<void>;
}) {
  const credentialReady = buildMcpCredentialReady({
    apiToken,
    apiTokenGrantId,
  });

  useEffect(() => {
    if (credentialReady) return;
    if (!tokenName.trim()) {
      onTokenName(DEFAULT_BUILD_TOKEN_NAME);
    }
  }, [credentialReady, tokenName, onTokenName]);

  async function handleTokenCreated(
    plaintext: string,
    created: { id: string; name: string },
  ) {
    onApiToken(plaintext, created.id);
    onTokenName(created.name);
    await onTokensChanged();
  }

  const createStatus = credentialReady ? "completed" : "current";

  return (
    <TimelineSteps
      orientation="vertical"
      size="sm"
      completedChrome="outcome"
      className={`${SETUP_TYPESET_CHROME_CLASS} w-full max-w-xl`}
      data-testid="build-token-setup"
      aria-label="Create a token"
    >
      <TimelineStepsItem
        status={createStatus}
        index={0}
        data-testid="build-token-create-item"
      >
        <TimelineStepsHeader>
          <TimelineStepsIcon />
          <TimelineStepsTitle>
            {credentialReady ? (
              tokenName.trim() ? (
                <>
                  {SETUP_TOKEN_CREATED_LEAD}{" "}
                  <span className="font-semibold">{tokenName.trim()}</span>{" "}
                  {SETUP_TOKEN_CREATED_TAIL}
                </>
              ) : (
                SETUP_TOKEN_CREATED_ITEM_TITLE
              )
            ) : (
              SETUP_TOKEN_CREATE_ITEM_TITLE
            )}
          </TimelineStepsTitle>
        </TimelineStepsHeader>
        <TimelineStepsContent
          className={
            credentialReady
              ? SETUP_TOKEN_CREATED_CONTENT_CLASS
              : SETUP_TOKEN_CREATE_CONTENT_CLASS
          }
        >
          {credentialReady ? (
            <div className="space-y-3">
              <div
                role="group"
                aria-label="API token"
                data-testid="build-token-created-secret"
              >
                <CopyableCode
                  value={apiToken}
                  size="lg"
                  className="w-fit max-w-full"
                  tooltip="Copy token"
                  sensitive
                  revealLabel="Show token"
                  hideLabel="Hide token"
                />
              </div>
              <Button
                type="button"
                variant="ghost"
                size="sm"
                data-testid="build-token-create-different"
                onClick={() => onApiToken("")}
              >
                <RotateCcw aria-hidden />
                {SETUP_TOKEN_CREATE_DIFFERENT}
              </Button>
            </div>
          ) : (
            <TokenCreateForm
              name={tokenName}
              onNameChange={onTokenName}
              defaultName={DEFAULT_BUILD_TOKEN_NAME}
              onCreated={handleTokenCreated}
              showPlaintextResult={false}
              fieldOrientation="horizontal"
              className={SETUP_TOKEN_CREATE_TRACK.form}
              controlsClassName={SETUP_TOKEN_CREATE_TRACK.controls}
              actionsClassName={SETUP_TOKEN_CREATE_TRACK.actions}
            />
          )}
        </TimelineStepsContent>
      </TimelineStepsItem>
    </TimelineSteps>
  );
}

function SetupAppNotInWorkspaceNotice({ appId }: { appId: string }) {
  const missingLabel = companionAppLabel(appId);
  return (
    <div className="h-full rounded-xl bg-neutral-hover p-3 text-foreground">
      <div className="flex items-start gap-3">
        <IntegrationIcon name={appId} displayName={missingLabel} size="md" />
        <div className="min-w-0 flex-1">
          <p className="text-sm font-heading text-foreground">{missingLabel}</p>
          <Badge variant="warning" size="sm" className="mt-2">
            Not in workspace
          </Badge>
        </div>
      </div>
    </div>
  );
}

function InvokeStepActions({
  exemplar,
  integrations,
  installAgentId,
  overlayKnown,
  onMarkTrySeen,
}: {
  exemplar: BuildExemplar;
  integrations: Integration[];
  installAgentId: BuildInstallAgentId | "";
  overlayKnown: boolean;
  onMarkTrySeen: () => void;
}) {
  useEffect(() => {
    onMarkTrySeen();
  }, [onMarkTrySeen]);
  const integration = resolveCatalogApp(
    integrations,
    exemplar.id,
    exemplar.knownMountPath,
  );
  const open = resolveExemplarOpenPath(exemplar, integration);
  const displayName = integration?.displayName?.trim() || exemplar.label;
  const featuredApp = integration
    ? tryStepCatalogApp({
        catalog: integration,
        label: displayName,
        description: integration.description?.trim() || exemplar.need,
        mountedPath: open.kind === "mount" ? open.href : undefined,
      })
    : null;
  const tryReturnPath = `${SETUP_PATH}/try`;
  const invokeApp = resolveCatalogApp(
    integrations,
    exemplar.invokeAppId,
    exemplar.knownMountPath,
  );
  const invokeAppId = invokeApp?.name ?? exemplar.invokeAppId;
  const invokeAppLabel =
    invokeApp?.displayName?.trim() || companionAppLabel(exemplar.invokeAppId);
  const agentSkin = buildAgentSkin(installAgentId);
  const productLabel = buildAgentProductLabel(installAgentId);
  const cwd = `~/${exemplar.department.toLowerCase().replace(/\s+/g, "-")}`;

  return (
    <div className="space-y-16" data-testid="build-first-call">
      <div data-testid="build-golden-prompt">
        <p>
          Prompt your favorite LLM with{" "}
          <span className={SETUP_TYPESET_CHROME_CLASS}>
            <CopyableCode value={exemplar.llmPrompt} tooltip="Copy prompt" />
          </span>{" "}
          and it should reply like in this example below.
        </p>

        <div
          className={`${SETUP_TYPESET_NESTED_CHROME_CLASS} min-h-[16rem] w-full`}
          data-testid="build-agent-console-reply"
          data-agent-skin={agentSkin}
        >
          <BuildAgentConsolePreview
            variant={agentSkin}
            productLabel={productLabel}
            prompt={exemplar.llmPrompt}
            reply={exemplar.expectedResult}
            cwd={cwd}
          />
        </div>

        <p>
          Behind the scenes this calls{" "}
          <span className={SETUP_TYPESET_CHROME_CLASS}>
            <InvokeOperationReference
              appId={invokeAppId}
              operationId={exemplar.operationId}
              appLabel={invokeAppLabel}
            />
          </span>
          .
        </p>

        <Alert
          collapsible
          defaultOpen
          variant="outline"
          className={`${SETUP_TYPESET_NESTED_CHROME_CLASS} w-full`}
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

      <div
        className={`${SETUP_TYPESET_CHROME_CLASS} space-y-6`}
        data-testid="build-shipped-app"
      >
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
        <div className="w-full" data-testid="build-open-exemplar">
          {featuredApp ? (
            <IntegrationCard
              integration={featuredApp}
              returnPath={tryReturnPath}
              connectionStatusKnown={overlayKnown}
              actions="launch"
            />
          ) : (
            <SetupAppNotInWorkspaceNotice appId={exemplar.id} />
          )}
        </div>
      </div>

      <div
        className={`${SETUP_TYPESET_CHROME_CLASS} space-y-6`}
        data-testid="build-related-apps"
      >
        <SectionHeader>
          <SectionHeaderContent>
            <SectionHeaderTitle>Related apps</SectionHeaderTitle>
            <SectionHeaderDescription>
              More apps that fit this outcome. Open one, or browse the full store.
            </SectionHeaderDescription>
          </SectionHeaderContent>
        </SectionHeader>
        <div className="grid grid-cols-1 items-stretch gap-6 sm:grid-cols-2">
          {exemplar.relatedAppIds.map((appId) => {
            const related = resolveCatalogApp(integrations, appId);
            const label =
              related?.displayName?.trim() || companionAppLabel(appId);
            return (
              <div
                key={appId}
                className="h-full"
                data-testid={`build-open-app-${appId}`}
              >
                {related ? (
                  <IntegrationCard
                    integration={tryStepCatalogApp({
                      catalog: related,
                      label,
                      description:
                        related.description?.trim() ||
                        `Open ${label} in Gestalt.`,
                      mountedPath: related.mountedPath?.trim(),
                    })}
                    returnPath={tryReturnPath}
                    connectionStatusKnown={overlayKnown}
                    actions="launch"
                  />
                ) : (
                  <SetupAppNotInWorkspaceNotice appId={appId} />
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function ConnectStepActions({
  exemplar,
  integrations,
  catalogSettled,
  catalogHasApps,
  catalogError,
  catalogRetrying,
  onRetryCatalog,
  overlayKnown,
  overlayError,
  overlayRetrying,
  onRetryOverlay,
  installAgentId,
}: {
  exemplar: BuildExemplar;
  integrations: Integration[];
  catalogSettled: boolean;
  catalogHasApps: boolean;
  catalogError: string | null;
  catalogRetrying: boolean;
  onRetryCatalog: () => void;
  overlayKnown: boolean;
  overlayError: string | null;
  overlayRetrying: boolean;
  onRetryOverlay: () => void;
  installAgentId: BuildInstallAgentId | "";
}) {
  const invalidateIntegrations = useInvalidateIntegrations();
  const [visibleMoreCount, setVisibleMoreCount] = useState(SETUP_APPS_PAGE_SIZE);
  const [categoryFilter, setCategoryFilter] = useState(SETUP_APPS_CATEGORY_ALL);
  const suggestedLabelId = useId();
  const moreLabelId = useId();
  const returnPath = `${SETUP_PATH}/apps`;

  async function refreshIntegrations() {
    await invalidateIntegrations();
  }

  function selectCategory(next: string) {
    setCategoryFilter(next || SETUP_APPS_CATEGORY_ALL);
    setVisibleMoreCount(SETUP_APPS_PAGE_SIZE);
  }

  const catalogNotice = catalogError ? (
    <ErrorNotice
      message={catalogError}
      retrying={catalogRetrying}
      onRetry={onRetryCatalog}
    />
  ) : null;
  const overlayNotice = overlayError ? (
    <ErrorNotice
      message={overlayError}
      retrying={overlayRetrying}
      onRetry={onRetryOverlay}
    />
  ) : null;

  if (catalogError && !catalogHasApps) {
    return (
      <div className={SETUP_TYPESET_CHROME_CLASS}>{catalogNotice}</div>
    );
  }

  if (!catalogSettled && !catalogHasApps) {
    return <p className={`${SETUP_TYPESET_CHROME_CLASS} text-sm text-faint`}>Loading apps…</p>;
  }

  const companionIds = new Set(exemplar.companionAppIds);
  const suggested = exemplar.companionAppIds.flatMap((appId) => {
    const integration = resolveCatalogApp(integrations, appId);
    if (integration && !isSetupDataSourceApp(integration)) return [];
    return [{ appId, integration }];
  });
  const suggestedCatalogNames = new Set(
    suggested.flatMap((item) => (item.integration ? [item.integration.name] : [])),
  );
  const more = setupDataSourceIntegrations(integrations)
    .filter(
      (integration) =>
        !companionIds.has(integration.name) &&
        !suggestedCatalogNames.has(integration.name),
    )
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
        <div key={appId} className="h-full" data-testid={`build-connect-app-${appId}`}>
          <SetupAppNotInWorkspaceNotice appId={appId} />
        </div>
      );
    }

    return (
      <div key={appId} className="h-full" data-testid={`build-connect-app-${appId}`}>
        <IntegrationCard
          integration={integration}
          returnPath={returnPath}
          connectionStatusKnown={overlayKnown}
          onConnected={() => void refreshIntegrations()}
          onDisconnected={() => void refreshIntegrations()}
          actions="connect"
        />
      </div>
    );
  }

  return (
    <div
      className={`${SETUP_TYPESET_CHROME_CLASS} flex flex-col gap-8`}
      data-testid="build-connect-apps"
    >
      {catalogNotice}
      {overlayNotice}
      <SetupOverlapCallout agentId={installAgentId} />
      <div className="flex flex-col gap-10">
        {suggested.length > 0 ? (
          <section
            className="space-y-3"
            aria-labelledby={suggestedLabelId}
            data-testid="build-connect-apps-suggested"
          >
            <Eyebrow id={suggestedLabelId} className="block">
              Suggested
            </Eyebrow>
            <div className={SETUP_APPS_GRID_CLASS}>
              {suggested.map(({ appId, integration }) =>
                renderConnectCard(appId, integration),
              )}
            </div>
          </section>
        ) : null}

        {more.length > 0 ? (
          <section
            className="space-y-3"
            aria-labelledby={moreLabelId}
            data-testid="build-connect-apps-more"
          >
            <Eyebrow id={moreLabelId} className="block">
              {moreSectionTitle}
            </Eyebrow>
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
          </section>
        ) : null}

        {missingFromCatalog.length > 0 ? (
          <p className="text-sm text-muted-foreground">
            Ask an admin to add missing apps to this workspace before you
            continue.
          </p>
        ) : null}
      </div>
    </div>
  );
}
