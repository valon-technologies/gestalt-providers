import type { APIToken, Integration } from "@/lib/api";
import {
  ASSISTANT_PICKER_DESCRIPTION,
  TOKEN_STEP_DESCRIPTION,
} from "@/lib/assistantConnectionCopy";
import {
  assistantHostById,
  isBuildInstallAgentId,
  normalizeStoredInstallAgentId,
  type BuildInstallAgentId,
} from "@/lib/assistantHosts";
import {
  appShowsCredentialSurface,
  catalogInstallState,
} from "@/lib/catalogFilters";
import { SETUP_PATH } from "@/lib/constants";
import { normalizeIntegrationStatus } from "@/lib/integrationStatus";

export type BuildStepId =
  | "welcome"
  | "assistant"
  | "token"
  | "install"
  | "apps"
  | "try";

export type BuildExemplarId =
  | "aiSpendTracker"
  | "oncall"
  | "ashby"
  | "servicingQuiz";

/**
 * Access-safe mounted app behind a Setup journey.
 * Try step faces a **department + outcome**; invoke reveals the **app**.
 */
export type BuildExemplar = {
  id: BuildExemplarId;
  /** Product name — reveal only on try / store CTA. */
  label: string;
  /** Rippling-style department for the intro eyebrow. */
  department: string;
  /** Short outcome title on the try promo. */
  outcomeTitle: string;
  /** One-line need under the toggle. */
  need: string;
  /** AgentConsole typewriter — need-shaped, no product spoiler. */
  llmPrompt: string;
  /**
   * Catalog apps the Try step should connect.
   * Every exemplar requires at least one — Setup always teaches connect.
   */
  companionAppIds: readonly [string, ...string[]];
  /** App used for the first-call proof (often the exemplar itself). */
  invokeAppId: string;
  operationId: string;
  invokeRecipe: string;
  expectedResult: string;
  /** Curated attribution — not from the Integration API. */
  builderNote: string;
  /** Known mount path when catalog omits mountedPath. */
  knownMountPath: string;
  /** Other catalog apps to promote under the shipped exemplar. */
  relatedAppIds: readonly string[];
};

/** Outcome of loading the workspace app catalog. Empty `integrations` is not enough. */
export type CatalogLoadState = "pending" | "ready" | "failed";

export interface BuildWorkspaceSnapshot {
  integrations: Integration[];
  tokens: APIToken[];
  /**
   * Whether {@link integrations} is a successful catalog load.
   * Pending and failed loads must not be treated as “nothing to connect.”
   */
  catalogLoadState: CatalogLoadState;
  activeExemplarId: BuildExemplarId;
  mcpInstalled: boolean;
  apiToken: string;
  /** Grant id the plaintext {@link apiToken} was issued for — empty when unset. */
  apiTokenGrantId: string;
  /** Display name for the token chosen or drafted in this Setup session. */
  tokenName: string;
  /**
   * Grant id for the session-minted secret, or a leftover radio sentinel from
   * older Setup (`new` / `existing`). Empty before the first mint.
   */
  selectedTokenId: string;
  /** Host assistant chosen on the Assistant step. */
  installAgentId: string;
  welcomeSeen: boolean;
  /** Try step was opened — last-step completion, independent of app connects. */
  trySeen: boolean;
}

export interface BuildStep {
  id: BuildStepId;
  title: string;
  /** Plain-English support line under the title — must not restate the title. */
  description: string;
  ctaLabel: string;
  to: string;
  isComplete: (snapshot: BuildWorkspaceSnapshot) => boolean;
}

export const BUILD_EXEMPLARS: BuildExemplar[] = [
  {
    id: "aiSpendTracker",
    label: "AI Spend Tracker",
    department: "Engineering",
    outcomeTitle: "Monitor spending",
    need: "See personal and org AI coding spend without asking Finance.",
    llmPrompt: "How much did I spend on AI last week?",
    companionAppIds: ["slack"],
    invokeAppId: "aiSpendTracker",
    operationId: "getMyUsage",
    invokeRecipe: "gestalt apps invoke aiSpendTracker getMyUsage",
    expectedResult: `Your AI spend (last 7 days): $12.40
Prior week: $9.10 (+36%)
Eng percentile: top 40%

(Slack digest preview ready for weekday morning.)`,
    builderNote: "Jon",
    knownMountPath: "/ai-spend",
    relatedAppIds: ["oncall", "modelProviderBillingMetrics"],
  },
  {
    id: "oncall",
    label: "Oncall",
    department: "Engineering",
    outcomeTitle: "Check oncall schedule",
    need: "See who’s on call and what’s in the queue for your eng team.",
    llmPrompt: "Who’s on call right now?",
    companionAppIds: ["pagerduty", "linear", "slack"],
    invokeAppId: "oncall",
    operationId: "get_me",
    invokeRecipe: "gestalt apps invoke oncall get_me",
    expectedResult: `On call now: Alex (primary), Sam (secondary)
Open queue: 3 pages
Next rotation: Thu 09:00`,
    builderNote: "Platform team",
    knownMountPath: "/oncall",
    relatedAppIds: ["incident_io", "datadog"],
  },
  {
    id: "ashby",
    label: "Ashby",
    department: "People",
    outcomeTitle: "Check hiring pipeline",
    need: "See which candidates need a follow-up this week.",
    llmPrompt: "Which candidates need a follow-up?",
    companionAppIds: ["ashby", "slack"],
    invokeAppId: "ashby",
    operationId: "listFollowUps",
    invokeRecipe: "gestalt apps invoke ashby listFollowUps",
    expectedResult: `Follow-ups due:
• Jordan Lee — onsite debrief (Slack reminder drafted)
• Priya Shah — offer packet review

3 candidates waiting more than 5 days.`,
    builderNote: "People Ops",
    knownMountPath: "/apps/ashby",
    relatedAppIds: ["rippling", "talentTeam"],
  },
  {
    id: "servicingQuiz",
    label: "Servicing Quiz",
    department: "Training",
    outcomeTitle: "Practice servicing knowledge",
    need: "Self-check mortgage servicing knowledge.",
    llmPrompt: "Am I ready for another servicing quiz?",
    companionAppIds: ["slack"],
    invokeAppId: "servicingQuiz",
    operationId: "getHistory",
    invokeRecipe: "gestalt apps invoke servicingQuiz getHistory",
    expectedResult: `Last attempt: 78% (passed)
Topics to review: ETD timing, MI cancellation
Ready for a new attempt when you are.`,
    builderNote: "Training team",
    knownMountPath: "/servicing-quiz",
    relatedAppIds: ["learnPortal", "trainingCurriculum"],
  },
];

/** Tenant-neutral product noun for Setup copy (deployments may override later). */
export const SETUP_PRODUCT_NAME = "Gestalt";

export const BUILD_STEPS: BuildStep[] = [
  {
    id: "welcome",
    title: "Welcome",
    description:
      "Connect the assistant you already use to this workspace so it can use your company’s apps, with your permission.",
    ctaLabel: "Choose your assistant",
    to: `${SETUP_PATH}/welcome`,
    isComplete: (snapshot) => snapshot.welcomeSeen,
  },
  {
    id: "assistant",
    title: "Choose your assistant",
    description: ASSISTANT_PICKER_DESCRIPTION,
    ctaLabel: "Continue",
    to: `${SETUP_PATH}/assistant`,
    isComplete: (snapshot) => buildInstallAgentSelected(snapshot.installAgentId),
  },
  {
    id: "token",
    title: "Create a token",
    description: TOKEN_STEP_DESCRIPTION,
    ctaLabel: "Continue",
    to: `${SETUP_PATH}/token`,
    isComplete: (snapshot) => buildMcpCredentialReady(snapshot),
  },
  {
    id: "install",
    title: `Add ${SETUP_PRODUCT_NAME}`,
    description: `Add ${SETUP_PRODUCT_NAME} so your assistant can reach this workspace.`,
    ctaLabel: "Continue",
    to: `${SETUP_PATH}/install`,
    isComplete: (snapshot) => snapshot.mcpInstalled,
  },
  {
    id: "apps",
    title: "Connect apps",
    description:
      "Pick the apps your assistant can use. Connect at least one to continue.",
    ctaLabel: "Continue",
    to: `${SETUP_PATH}/apps`,
    isComplete: (snapshot) => setupAppsStepComplete(snapshot),
  },
  {
    id: "try",
    title: "Try it",
    description:
      "Paste a test prompt in your assistant and see it use this workspace.",
    ctaLabel: "Browse apps",
    to: `${SETUP_PATH}/try`,
    isComplete: (snapshot) => snapshot.trySeen,
  },
];

/** Legacy radio value; still rejected as a grant id in stored sessions. */
export const BUILD_USE_EXISTING_TOKEN_ID = "existing";

/** Legacy radio value; still rejected as a grant id in stored sessions. */
export const BUILD_CREATE_NEW_TOKEN_ID = "new";

/** True when `id` is a real grant, not a token-step radio sentinel. */
export function isSetupTokenGrantId(id: string): boolean {
  const trimmed = id.trim();
  return (
    trimmed.length > 0 &&
    trimmed !== BUILD_CREATE_NEW_TOKEN_ID &&
    trimmed !== BUILD_USE_EXISTING_TOKEN_ID
  );
}

/** Pre-split Connect URL — redirect to the token step. */
export const LEGACY_SETUP_CONNECT_STEP_ID = "connect";

/** Demo name prefilled when drafting a Setup token. */
export const DEFAULT_BUILD_TOKEN_NAME = "Gestalt";

export const SETUP_TOKEN_CREATE_ITEM_TITLE = "Create a token";

export const SETUP_TOKEN_SELECTED_ITEM_TITLE = "Token ready";

export const SETUP_TOKEN_CREATE_DONE =
  "Continue to copy your token in the next step.";

export const SETUP_TOKEN_SELECTED_PENDING = "Create a token first.";

export const SETUP_TOKEN_NEXT_DISABLED_TITLE = "Create a token before continuing";

export function setupTokenSelectedReadyCopy(name: string): string {
  const label = name.trim() || "Your token";
  return `${label} is ready. Continue to add Gestalt.`;
}

/**
 * Token step is done when this session holds a minted secret bound to the
 * selected grant. A filled create-token name is not enough. Listed grants
 * cannot be reused: the API never returns the secret after mint.
 */
export function buildMcpCredentialReady(
  snapshot: Pick<
    BuildWorkspaceSnapshot,
    "apiToken" | "apiTokenGrantId" | "selectedTokenId"
  >,
): boolean {
  const token = snapshot.apiToken.trim();
  const grantId = snapshot.apiTokenGrantId.trim();
  const selected = snapshot.selectedTokenId.trim();
  return token.length > 0 && grantId.length > 0 && grantId === selected;
}

export function canNavigateToBuildStep(
  targetId: BuildStepId,
  currentId: BuildStepId,
  isStepDone: (step: BuildStep) => boolean,
): boolean {
  const targetIdx = BUILD_STEPS.findIndex((step) => step.id === targetId);
  const currentIdx = BUILD_STEPS.findIndex((step) => step.id === currentId);
  if (targetIdx === -1 || currentIdx === -1) return false;
  if (targetIdx <= currentIdx) return true;
  return BUILD_STEPS.slice(0, targetIdx).every(isStepDone);
}

const BUILD_STEP_IDS = new Set<string>(BUILD_STEPS.map((step) => step.id));

export function isBuildStepId(value: string): value is BuildStepId {
  return BUILD_STEP_IDS.has(value);
}

export function getExemplar(
  id: BuildExemplarId | string | null | undefined,
): BuildExemplar {
  return (
    BUILD_EXEMPLARS.find((exemplar) => exemplar.id === id) ??
    BUILD_EXEMPLARS[0]!
  );
}

/** True when every companion for the active exemplar is connected. */
export function exemplarCompanionsConnected(
  snapshot: BuildWorkspaceSnapshot,
): boolean {
  const exemplar = getExemplar(snapshot.activeExemplarId);
  const connected = connectedAppIds(snapshot.integrations);
  return exemplar.companionAppIds.every((appId) => connected.has(appId));
}

export function connectedAppIds(integrations: Integration[]): Set<string> {
  return new Set(
    integrations
      .filter(
        (integration) => normalizeIntegrationStatus(integration).connected,
      )
      .map((integration) => integration.name),
  );
}

/**
 * Setup Connect apps — only catalog rows that connect this workspace to an
 * external data source (OAuth / API key). Native / mount-only products stay
 * in the store, not on this step.
 */
export function isSetupDataSourceApp(integration: Integration): boolean {
  return appShowsCredentialSurface(integration);
}

export function setupDataSourceIntegrations(
  integrations: Integration[],
): Integration[] {
  return integrations.filter(isSetupDataSourceApp);
}

/** True when the user has connected at least one external data-source app. */
export function setupAppsConnected(
  snapshot: Pick<BuildWorkspaceSnapshot, "integrations">,
): boolean {
  return setupDataSourceIntegrations(snapshot.integrations).some(
    (integration) => catalogInstallState(integration) === "connected",
  );
}

/** True when a data-source app is still waiting to be connected. */
export function setupAppsHasConnectable(
  snapshot: Pick<BuildWorkspaceSnapshot, "integrations">,
): boolean {
  return setupDataSourceIntegrations(snapshot.integrations).some(
    (integration) => {
      const state = catalogInstallState(integration);
      return state === "not_connected" || state === "needs_attention";
    },
  );
}

/**
 * Connect apps is done when at least one data-source app is connected, or
 * after a successful catalog load that has nothing left to connect.
 * Pending and failed loads stay incomplete even if the list is empty.
 */
export function setupAppsStepComplete(
  snapshot: Pick<BuildWorkspaceSnapshot, "integrations" | "catalogLoadState">,
): boolean {
  if (setupAppsConnected(snapshot)) return true;
  return (
    snapshot.catalogLoadState === "ready" &&
    !setupAppsHasConnectable(snapshot)
  );
}

/** Map a catalog query onto {@link CatalogLoadState}. */
export function catalogLoadStateFromQuery(query: {
  isPending: boolean;
  isError: boolean;
}): CatalogLoadState {
  if (query.isPending) return "pending";
  if (query.isError) return "failed";
  return "ready";
}

/**
 * Session plaintext is valid only while it is bound to the current grant
 * selection. Empty bound ids never match, so stale secrets are dropped.
 */
export function sessionApiTokenBoundToSelection(
  boundGrantId: string,
  selectedId: string,
): boolean {
  const bound = boundGrantId.trim();
  const selected = selectedId.trim();
  return bound.length > 0 && bound === selected && isSetupTokenGrantId(selected);
}

/** Assemble the Setup snapshot from session + catalog/token queries. */
export function buildWorkspaceSnapshotFromSession(
  session: {
    activeExemplarId: BuildExemplarId;
    mcpInstalled: boolean;
    apiToken: string;
    apiTokenGrantId: string;
    tokenName: string;
    selectedTokenId: string;
    selectedInstallAgent: string;
    welcomeSeen: boolean;
    trySeen: boolean;
  },
  integrations: Integration[],
  tokens: APIToken[],
  catalogLoadState: CatalogLoadState,
): BuildWorkspaceSnapshot {
  return {
    integrations,
    tokens,
    catalogLoadState,
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

export function isBuildComplete(snapshot: BuildWorkspaceSnapshot): boolean {
  return BUILD_STEPS.every((step) => step.isComplete(snapshot));
}

export function firstIncompleteStepId(
  snapshot: BuildWorkspaceSnapshot,
  isStepDone: (step: BuildStep) => boolean = (step) =>
    step.isComplete(snapshot),
): BuildStepId {
  const first = BUILD_STEPS.find((step) => !isStepDone(step));
  return first?.id ?? BUILD_STEPS[BUILD_STEPS.length - 1]!.id;
}

/** Warm workspace: already has a token or a connected app — never auto-prompt Setup. */
export function isWorkspaceWarm(
  tokens: APIToken[],
  integrations: Integration[],
): boolean {
  if (tokens.length > 0) return true;
  return connectedAppIds(integrations).size > 0;
}

/**
 * Soft-force Setup only for empty net-new accounts that have not skipped.
 * Warm / complete / skipped users are never redirected.
 */
export function isActivationDue(args: {
  tokens: APIToken[];
  integrations: Integration[];
  skipped: boolean;
  complete?: boolean;
}): boolean {
  if (args.skipped) return false;
  if (args.complete) return false;
  if (isWorkspaceWarm(args.tokens, args.integrations)) return false;
  return true;
}

export function companionAppLabel(appId: string): string {
  switch (appId) {
    case "slack":
      return "Slack";
    case "pagerduty":
      return "PagerDuty";
    case "linear":
      return "Linear";
    case "ashby":
      return "Ashby";
    case "intercom":
      return "Intercom";
    case "aiSpendTracker":
      return "AI Spend Tracker";
    case "oncall":
      return "Oncall";
    case "servicingQuiz":
      return "Servicing Quiz";
    case "learnPortal":
      return "Learn Portal";
    case "trainingCurriculum":
      return "Training Curriculum";
    case "modelProviderBillingMetrics":
      return "Model provider billing";
    case "incident_io":
      return "incident.io";
    case "datadog":
      return "Datadog";
    case "rippling":
      return "Rippling";
    case "talentTeam":
      return "Talent Team";
    default:
      return appId;
  }
}

export function resolveExemplarOpenPath(
  exemplar: BuildExemplar,
  integration: Integration | undefined,
): { href: string; kind: "mount" | "store" } {
  const mounted = integration?.mountedPath?.trim();
  if (mounted) {
    return { href: mounted, kind: "mount" };
  }
  if (exemplar.knownMountPath) {
    return { href: exemplar.knownMountPath, kind: "mount" };
  }
  return {
    href: `/apps/${encodeURIComponent(exemplar.id)}/admin`,
    kind: "store",
  };
}

export const MCP_INSTALLED_STORAGE_KEY = "gestalt.build.mcpInstalled";
export const MCP_INSTALLED_AGENTS_STORAGE_KEY =
  "gestalt.build.mcpInstalledAgents";
export const BUILD_EXEMPLAR_STORAGE_KEY = "gestalt.build.activeExemplarId";
/** Welcome-seen flag (legacy key `introSeen` retained for in-flight sessions). */
export const BUILD_INTRO_SEEN_STORAGE_KEY = "gestalt.build.introSeen";
export const BUILD_TRY_SEEN_STORAGE_KEY = "gestalt.build.trySeen";
export const BUILD_API_TOKEN_STORAGE_KEY = "gestalt.build.apiToken";
export const BUILD_API_TOKEN_GRANT_ID_STORAGE_KEY =
  "gestalt.build.apiTokenGrantId";
export const BUILD_TOKEN_NAME_STORAGE_KEY = "gestalt.build.tokenName";
export const BUILD_SELECTED_TOKEN_ID_STORAGE_KEY =
  "gestalt.build.selectedTokenId";
export const BUILD_INSTALL_AGENT_STORAGE_KEY = "gestalt.build.installAgent.v2";
const LEGACY_BUILD_INSTALL_AGENT_STORAGE_KEY = "gestalt.build.installAgent";
export const SETUP_SKIPPED_STORAGE_KEY = "gestalt.setup.skipped";
export const SETUP_RESUME_BANNER_DISMISSED_KEY =
  "gestalt.setup.resumeBannerDismissed";

export function buildInstallAgentSelected(installAgentId: string): boolean {
  return isBuildInstallAgentId(installAgentId);
}

export function buildInstallStepTitle(installAgentId: string): string {
  const host = assistantHostById(installAgentId);
  if (!host || host.id === "other") {
    return `Add ${SETUP_PRODUCT_NAME} to your assistant`;
  }
  return `Add ${SETUP_PRODUCT_NAME} in ${host.label}`;
}

export function buildInstallStepDescription(installAgentId: string): string {
  return (
    assistantHostById(installAgentId)?.installDescription ??
    assistantHostById("other")!.installDescription
  );
}

export function buildStepTitle(
  step: BuildStep,
  installAgentId: string,
): string {
  if (step.id === "install") return buildInstallStepTitle(installAgentId);
  return step.title;
}

export function buildStepDescription(
  step: BuildStep,
  installAgentId: string,
): string {
  if (step.id === "install" && isBuildInstallAgentId(installAgentId)) {
    return buildInstallStepDescription(installAgentId);
  }
  return step.description;
}

export function isLegacySetupConnectStepId(value: string): boolean {
  return value === LEGACY_SETUP_CONNECT_STEP_ID;
}

export function mcpInstalledForAgent(
  installedAgents: readonly string[],
  agentId: string,
): boolean {
  return isBuildInstallAgentId(agentId) && installedAgents.includes(agentId);
}

export function addMcpInstalledAgent(
  installedAgents: readonly BuildInstallAgentId[],
  agentId: string,
): BuildInstallAgentId[] {
  if (!isBuildInstallAgentId(agentId)) return [...installedAgents];
  if (installedAgents.includes(agentId)) return [...installedAgents];
  return [...installedAgents, agentId];
}

export function readMcpInstalledAgents(): BuildInstallAgentId[] {
  if (typeof window === "undefined") return [];
  try {
    const raw = window.sessionStorage.getItem(MCP_INSTALLED_AGENTS_STORAGE_KEY);
    if (raw) {
      const parsed: unknown = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        return parsed.filter(isBuildInstallAgentId);
      }
    }
    if (readSessionFlag(MCP_INSTALLED_STORAGE_KEY)) {
      const agent = readStoredInstallAgent();
      return agent ? [agent] : [];
    }
    return [];
  } catch {
    return [];
  }
}

export function writeMcpInstalledAgents(ids: readonly BuildInstallAgentId[]): void {
  if (typeof window === "undefined") return;
  try {
    window.sessionStorage.setItem(
      MCP_INSTALLED_AGENTS_STORAGE_KEY,
      JSON.stringify(ids),
    );
    window.sessionStorage.removeItem(MCP_INSTALLED_STORAGE_KEY);
  } catch {
    /* ignore */
  }
}

export function readMcpInstalledFlag(): boolean {
  return readMcpInstalledAgents().length > 0;
}

export function writeMcpInstalledFlag(value: boolean): void {
  if (!value) {
    writeMcpInstalledAgents([]);
    return;
  }
  const agent = readStoredInstallAgent();
  writeMcpInstalledAgents(agent ? [agent] : []);
}

export function readIntroSeenFlag(): boolean {
  return readSessionFlag(BUILD_INTRO_SEEN_STORAGE_KEY);
}

export function writeIntroSeenFlag(value: boolean): void {
  writeSessionFlag(BUILD_INTRO_SEEN_STORAGE_KEY, value);
}

export function readTrySeenFlag(): boolean {
  return readSessionFlag(BUILD_TRY_SEEN_STORAGE_KEY);
}

export function writeTrySeenFlag(value: boolean): void {
  writeSessionFlag(BUILD_TRY_SEEN_STORAGE_KEY, value);
}

export function readSetupSkipped(): boolean {
  if (typeof window === "undefined") return false;
  try {
    return window.localStorage.getItem(SETUP_SKIPPED_STORAGE_KEY) === "1";
  } catch {
    return false;
  }
}

export function writeSetupSkipped(value: boolean): void {
  if (typeof window === "undefined") return;
  try {
    if (value) {
      window.localStorage.setItem(SETUP_SKIPPED_STORAGE_KEY, "1");
    } else {
      window.localStorage.removeItem(SETUP_SKIPPED_STORAGE_KEY);
    }
  } catch {
    /* ignore */
  }
}

export function readResumeBannerDismissed(): boolean {
  if (typeof window === "undefined") return false;
  try {
    return window.localStorage.getItem(SETUP_RESUME_BANNER_DISMISSED_KEY) === "1";
  } catch {
    return false;
  }
}

export function writeResumeBannerDismissed(value: boolean): void {
  if (typeof window === "undefined") return;
  try {
    if (value) {
      window.localStorage.setItem(SETUP_RESUME_BANNER_DISMISSED_KEY, "1");
    } else {
      window.localStorage.removeItem(SETUP_RESUME_BANNER_DISMISSED_KEY);
    }
  } catch {
    /* ignore */
  }
}

export function readActiveExemplarId(): BuildExemplarId {
  if (typeof window === "undefined") return BUILD_EXEMPLARS[0]!.id;
  try {
    const raw = window.sessionStorage.getItem(BUILD_EXEMPLAR_STORAGE_KEY);
    if (raw && BUILD_EXEMPLARS.some((exemplar) => exemplar.id === raw)) {
      return raw as BuildExemplarId;
    }
  } catch {
    /* ignore */
  }
  return BUILD_EXEMPLARS[0]!.id;
}

export function writeActiveExemplarId(id: BuildExemplarId): void {
  if (typeof window === "undefined") return;
  try {
    window.sessionStorage.setItem(BUILD_EXEMPLAR_STORAGE_KEY, id);
  } catch {
    /* ignore */
  }
}

export function readStoredApiToken(): string {
  if (typeof window === "undefined") return "";
  try {
    return window.sessionStorage.getItem(BUILD_API_TOKEN_STORAGE_KEY) ?? "";
  } catch {
    return "";
  }
}

export function writeStoredApiToken(token: string): void {
  if (typeof window === "undefined") return;
  try {
    if (token) {
      window.sessionStorage.setItem(BUILD_API_TOKEN_STORAGE_KEY, token);
    } else {
      window.sessionStorage.removeItem(BUILD_API_TOKEN_STORAGE_KEY);
    }
  } catch {
    /* ignore */
  }
}

export function readStoredApiTokenGrantId(): string {
  if (typeof window === "undefined") return "";
  try {
    return (
      window.sessionStorage.getItem(BUILD_API_TOKEN_GRANT_ID_STORAGE_KEY) ?? ""
    );
  } catch {
    return "";
  }
}

export function writeStoredApiTokenGrantId(grantId: string): void {
  if (typeof window === "undefined") return;
  try {
    if (grantId) {
      window.sessionStorage.setItem(
        BUILD_API_TOKEN_GRANT_ID_STORAGE_KEY,
        grantId,
      );
    } else {
      window.sessionStorage.removeItem(BUILD_API_TOKEN_GRANT_ID_STORAGE_KEY);
    }
  } catch {
    /* ignore */
  }
}

export function readStoredTokenName(): string {
  if (typeof window === "undefined") return DEFAULT_BUILD_TOKEN_NAME;
  try {
    const raw = window.sessionStorage.getItem(BUILD_TOKEN_NAME_STORAGE_KEY);
    if (raw == null) return DEFAULT_BUILD_TOKEN_NAME;
    return raw;
  } catch {
    return DEFAULT_BUILD_TOKEN_NAME;
  }
}

export function writeStoredTokenName(name: string): void {
  if (typeof window === "undefined") return;
  try {
    window.sessionStorage.setItem(BUILD_TOKEN_NAME_STORAGE_KEY, name);
  } catch {
    /* ignore */
  }
}

export function readStoredSelectedTokenId(): string {
  if (typeof window === "undefined") return "";
  try {
    return (
      window.sessionStorage.getItem(BUILD_SELECTED_TOKEN_ID_STORAGE_KEY) ?? ""
    );
  } catch {
    return "";
  }
}

export function writeStoredSelectedTokenId(id: string): void {
  if (typeof window === "undefined") return;
  try {
    if (id) {
      window.sessionStorage.setItem(BUILD_SELECTED_TOKEN_ID_STORAGE_KEY, id);
    } else {
      window.sessionStorage.removeItem(BUILD_SELECTED_TOKEN_ID_STORAGE_KEY);
    }
  } catch {
    /* ignore */
  }
}

export function readStoredInstallAgent(): BuildInstallAgentId | "" {
  if (typeof window === "undefined") return "";
  try {
    const current =
      window.sessionStorage.getItem(BUILD_INSTALL_AGENT_STORAGE_KEY) ?? "";
    const fromCurrent = normalizeStoredInstallAgentId(current, "current");
    if (fromCurrent) return fromCurrent;

    const legacy =
      window.sessionStorage.getItem(LEGACY_BUILD_INSTALL_AGENT_STORAGE_KEY) ??
      "";
    const fromLegacy = normalizeStoredInstallAgentId(legacy, "legacy");
    if (fromLegacy) {
      writeStoredInstallAgent(fromLegacy);
      window.sessionStorage.removeItem(LEGACY_BUILD_INSTALL_AGENT_STORAGE_KEY);
      return fromLegacy;
    }
    return "";
  } catch {
    return "";
  }
}

export function writeStoredInstallAgent(id: string): void {
  if (typeof window === "undefined") return;
  try {
    if (id && isBuildInstallAgentId(id)) {
      window.sessionStorage.setItem(BUILD_INSTALL_AGENT_STORAGE_KEY, id);
      window.sessionStorage.removeItem(LEGACY_BUILD_INSTALL_AGENT_STORAGE_KEY);
    } else {
      window.sessionStorage.removeItem(BUILD_INSTALL_AGENT_STORAGE_KEY);
      window.sessionStorage.removeItem(LEGACY_BUILD_INSTALL_AGENT_STORAGE_KEY);
    }
  } catch {
    /* ignore */
  }
}

function readSessionFlag(key: string): boolean {
  if (typeof window === "undefined") return false;
  try {
    return window.sessionStorage.getItem(key) === "1";
  } catch {
    return false;
  }
}

function writeSessionFlag(key: string, value: boolean): void {
  if (typeof window === "undefined") return;
  try {
    if (value) {
      window.sessionStorage.setItem(key, "1");
    } else {
      window.sessionStorage.removeItem(key);
    }
  } catch {
    /* ignore */
  }
}
