import type { APIToken, Integration } from "@/lib/api";
import { normalizeIntegrationStatus } from "@/lib/integrationStatus";

export type BuildStepId =
  | "intro"
  | "authorize"
  | "install"
  | "connect"
  | "invoke";

export type BuildExemplarId =
  | "aiSpendTracker"
  | "oncall"
  | "ashby"
  | "valonSats";

/**
 * Access-safe mounted Valon app behind a Build journey.
 * Intro faces a **department + outcome**; invoke reveals the **app**.
 */
export type BuildExemplar = {
  id: BuildExemplarId;
  /** Product name — reveal only on invoke / store CTA. */
  label: string;
  /** Rippling-style department for the intro eyebrow. */
  department: string;
  /** Short outcome title on the intro toggle. */
  outcomeTitle: string;
  /** One-line need under the toggle. */
  need: string;
  /** AgentConsole typewriter — need-shaped, no product spoiler. */
  llmPrompt: string;
  /**
   * Catalog apps the Connect step must link before Next.
   * Every exemplar requires at least one — Build always teaches connect.
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

export interface BuildWorkspaceSnapshot {
  integrations: Integration[];
  tokens: APIToken[];
  activeExemplarId: BuildExemplarId;
  mcpInstalled: boolean;
  apiToken: string;
  /** Display name for the token chosen or drafted in this Build session. */
  tokenName: string;
  /**
   * Radio selection on authorize: an existing token id, {@link BUILD_CREATE_NEW_TOKEN_ID},
   * or empty when nothing chosen yet.
   */
  selectedTokenId: string;
  introSeen: boolean;
}

export interface BuildStep {
  id: BuildStepId;
  title: string;
  /** Optional PageHeader eyebrow — omit when the step title stands alone. */
  eyebrow?: string;
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
    builderNote: "Valon Engineering",
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
    id: "valonSats",
    label: "Valon SATs",
    department: "Default Servicing",
    outcomeTitle: "Practice servicing knowledge",
    need: "Self-check mortgage servicing knowledge.",
    llmPrompt: "Am I ready for another servicing quiz?",
    companionAppIds: ["slack"],
    invokeAppId: "valonSats",
    operationId: "getHistory",
    invokeRecipe: "gestalt apps invoke valonSats getHistory",
    expectedResult: `Last attempt: 78% (passed)
Topics to review: ETD timing, MI cancellation
Ready for a new attempt when you are.`,
    builderNote: "Valon Servicing",
    knownMountPath: "/valon-sats",
    relatedAppIds: ["valonLearn", "trainingCurriculum"],
  },
];

export const BUILD_STEPS: BuildStep[] = [
  {
    id: "intro",
    title: "Pick what to build",
    description:
      "Choose a team outcome, then watch how an agent would ask for it.",
    ctaLabel: "Continue",
    to: "/build/intro",
    isComplete: (snapshot) => snapshot.introSeen,
  },
  {
    id: "authorize",
    title: "Choose a token",
    description:
      "Your agent needs a key to use Gestalt. Make a new one here, or pick one you already have.",
    ctaLabel: "Create API token",
    to: "/build/authorize",
    isComplete: (snapshot) => buildAuthorizeSelectionReady(snapshot),
  },
  {
    id: "install",
    title: "Install Gestalt",
    description:
      "Add Gestalt to Cursor, Claude Code, or Codex so the agent can reach your workspace.",
    ctaLabel: "Open MCP docs",
    to: "/build/install",
    isComplete: (snapshot) => snapshot.mcpInstalled,
  },
  {
    id: "connect",
    title: "Connect apps",
    description:
      "Link the apps this path needs before you make your first call.",
    ctaLabel: "See all apps",
    to: "/build/connect",
    isComplete: (snapshot) => exemplarCompanionsConnected(snapshot),
  },
  {
    id: "invoke",
    title: "Make your first call",
    description:
      "Paste the prompt into your agent and confirm you get a real answer.",
    ctaLabel: "Open Invoke docs",
    to: "/build/invoke",
    isComplete: (snapshot) =>
      buildAuthorizeSelectionReady(snapshot) &&
      snapshot.mcpInstalled &&
      exemplarCompanionsConnected(snapshot),
  },
];

/** Radio value for “create a new token” on the authorize step. */
export const BUILD_CREATE_NEW_TOKEN_ID = "new";

/** Demo name prefilled when drafting a Build token. */
export const DEFAULT_BUILD_TOKEN_NAME = "Gestalt Build";

/**
 * Authorize is ready when the user picked an existing token or created/pasted
 * a secret for this session — not merely because tokens exist in the account.
 */
export function buildAuthorizeSelectionReady(
  snapshot: Pick<
    BuildWorkspaceSnapshot,
    "apiToken" | "selectedTokenId" | "tokens"
  >,
): boolean {
  if (snapshot.apiToken.trim().length > 0) return true;
  const selected = snapshot.selectedTokenId.trim();
  if (!selected || selected === BUILD_CREATE_NEW_TOKEN_ID) return false;
  return snapshot.tokens.some((token) => token.id === selected);
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
    case "valonSats":
      return "Valon SATs";
    case "valonLearn":
      return "Valon Learn";
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
    href: `/apps/${encodeURIComponent(exemplar.id)}`,
    kind: "store",
  };
}

export const MCP_INSTALLED_STORAGE_KEY = "gestalt.build.mcpInstalled";
export const BUILD_EXEMPLAR_STORAGE_KEY = "gestalt.build.activeExemplarId";
export const BUILD_INTRO_SEEN_STORAGE_KEY = "gestalt.build.introSeen";
export const BUILD_API_TOKEN_STORAGE_KEY = "gestalt.build.apiToken";
export const BUILD_TOKEN_NAME_STORAGE_KEY = "gestalt.build.tokenName";
export const BUILD_SELECTED_TOKEN_ID_STORAGE_KEY =
  "gestalt.build.selectedTokenId";

export function readMcpInstalledFlag(): boolean {
  return readSessionFlag(MCP_INSTALLED_STORAGE_KEY);
}

export function writeMcpInstalledFlag(value: boolean): void {
  writeSessionFlag(MCP_INSTALLED_STORAGE_KEY, value);
}

export function readIntroSeenFlag(): boolean {
  return readSessionFlag(BUILD_INTRO_SEEN_STORAGE_KEY);
}

export function writeIntroSeenFlag(value: boolean): void {
  writeSessionFlag(BUILD_INTRO_SEEN_STORAGE_KEY, value);
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
