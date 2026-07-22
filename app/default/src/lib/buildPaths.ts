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
  | "valonLearn"
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
  /** Companion catalog apps to connect (empty = self-contained). */
  companionAppIds: string[];
  /** App used for the first-call proof (often the exemplar itself). */
  invokeAppId: string;
  operationId: string;
  invokeRecipe: string;
  expectedResult: string;
  /** Curated attribution — not from the Integration API. */
  builderNote: string;
  /** Known mount path when catalog omits mountedPath. */
  knownMountPath: string;
};

export interface BuildWorkspaceSnapshot {
  integrations: Integration[];
  tokens: APIToken[];
  activeExemplarId: BuildExemplarId;
  mcpInstalled: boolean;
  apiToken: string;
  introSeen: boolean;
}

export interface BuildStep {
  id: BuildStepId;
  title: string;
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
  },
  {
    id: "valonLearn",
    label: "Valon Learn",
    department: "People",
    outcomeTitle: "Continue onboarding",
    need: "Know which onboarding courses to take next.",
    llmPrompt: "What’s next in my onboarding?",
    companionAppIds: [],
    invokeAppId: "valonLearn",
    operationId: "listMyProgress",
    invokeRecipe: "gestalt apps invoke valonLearn listMyProgress",
    expectedResult: `In progress:
• Servicing Knowledge — 62%
• Valon OS basics — not started

Next: complete the Escrow quiz in Servicing Knowledge.`,
    builderNote: "Kaitlyn Schiffhauer",
    knownMountPath: "/learn",
  },
  {
    id: "valonSats",
    label: "Valon SATs",
    department: "Default Servicing",
    outcomeTitle: "Practice servicing knowledge",
    need: "Self-check mortgage servicing knowledge.",
    llmPrompt: "Am I ready for another servicing quiz?",
    companionAppIds: [],
    invokeAppId: "valonSats",
    operationId: "getHistory",
    invokeRecipe: "gestalt apps invoke valonSats getHistory",
    expectedResult: `Last attempt: 78% (passed)
Topics to review: ETD timing, MI cancellation
Ready for a new attempt when you are.`,
    builderNote: "Valon Servicing",
    knownMountPath: "/valon-sats",
  },
];

export const BUILD_STEPS: BuildStep[] = [
  {
    id: "intro",
    title: "Pick what to build",
    description:
      "See what someone on that team would ask an agent to do.",
    ctaLabel: "Continue",
    to: "/build/intro",
    isComplete: (snapshot) => snapshot.introSeen,
  },
  {
    id: "authorize",
    title: "Create a token",
    description:
      "Create a personal API token (or reuse one you already have). Secrets are only shown once.",
    ctaLabel: "Create API token",
    to: "/build/authorize",
    isComplete: (snapshot) =>
      snapshot.tokens.length > 0 || snapshot.apiToken.trim().length > 0,
  },
  {
    id: "install",
    title: "Install Gestalt",
    description:
      "Add Gestalt as an MCP server in Cursor, Claude Code, or Codex using your token.",
    ctaLabel: "Open MCP docs",
    to: "/build/install",
    isComplete: (snapshot) => snapshot.mcpInstalled,
  },
  {
    id: "connect",
    title: "Connect apps",
    description:
      "Connect companion apps this outcome needs — skip ahead when none are required.",
    ctaLabel: "See all apps",
    to: "/build/connect",
    isComplete: (snapshot) => exemplarCompanionsConnected(snapshot),
  },
  {
    id: "invoke",
    title: "Make your first call",
    description:
      "Paste the golden prompt into your AI client and confirm a real result.",
    ctaLabel: "Open Invoke docs",
    to: "/build/invoke",
    isComplete: (snapshot) =>
      (snapshot.tokens.length > 0 || snapshot.apiToken.trim().length > 0) &&
      snapshot.mcpInstalled &&
      exemplarCompanionsConnected(snapshot),
  },
];

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

/** True when every companion for the active exemplar is connected (or none required). */
export function exemplarCompanionsConnected(
  snapshot: BuildWorkspaceSnapshot,
): boolean {
  const exemplar = getExemplar(snapshot.activeExemplarId);
  if (exemplar.companionAppIds.length === 0) return true;
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
    case "aiSpendTracker":
      return "AI Spend Tracker";
    case "oncall":
      return "Oncall";
    case "valonLearn":
      return "Valon Learn";
    case "valonSats":
      return "Valon SATs";
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
