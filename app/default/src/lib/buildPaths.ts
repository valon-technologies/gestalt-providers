import type { Integration } from "@/lib/api";
import { normalizeIntegrationStatus } from "@/lib/integrationStatus";

export type BuildStepId = "connect" | "authorize" | "invoke";

export interface BuildWorkspaceSnapshot {
  integrations: Integration[];
  tokens: number;
}

export interface StarterAha {
  id: "google_calendar" | "slack" | "notion";
  appId: string;
  label: string;
  priority: "primary" | "alternate";
  ahaTitle: string;
  ahaDescription: string;
  /** Paste into Claude, Cursor, ChatGPT, etc. once MCP/Gestalt is connected. */
  llmPrompt: string;
  /** CLI fallback for the same aha. */
  invokeRecipe: string;
  /** Illustrative success output for the Build congrats callout. */
  expectedResult: string;
}

export interface BuildStep {
  id: BuildStepId;
  title: string;
  description: string;
  ctaLabel: string;
  to: string;
  isComplete: (snapshot: BuildWorkspaceSnapshot) => boolean;
}

export const STARTER_AHAS: StarterAha[] = [
  {
    id: "google_calendar",
    appId: "google_calendar",
    label: "Google Calendar",
    priority: "primary",
    ahaTitle: "See what's next",
    ahaDescription: "List the next events on your primary calendar.",
    llmPrompt:
      "Using Gestalt, list the next 5 events on my primary Google Calendar.",
    invokeRecipe:
      "gestalt apps invoke google_calendar events.list -p calendarId=primary -p maxResults=5",
    expectedResult: `Here are your next events on primary:

1. Design review — Tomorrow 10:00–10:30 AM
2. 1:1 with Alex — Tomorrow 2:00–2:30 PM
3. Sprint planning — Wed 11:00 AM–12:00 PM`,
  },
  {
    id: "slack",
    appId: "slack",
    label: "Slack",
    priority: "alternate",
    ahaTitle: "Post a hello",
    ahaDescription: "Send a message to a channel and see it appear in Slack.",
    llmPrompt:
      "Using Gestalt, post “Hello from Gestalt” to my Slack channel CHANNEL_ID.",
    invokeRecipe:
      "gestalt apps invoke slack chat.postMessage -p channel=CHANNEL_ID -p text='Hello from Gestalt'",
    expectedResult: `Posted to #general:

Hello from Gestalt

(message ts: 1710000000.000100)`,
  },
  {
    id: "notion",
    appId: "notion",
    label: "Notion",
    priority: "alternate",
    ahaTitle: "Find a page",
    ahaDescription: "Search your workspace for a familiar page title.",
    llmPrompt:
      "Using Gestalt, search my Notion workspace for pages about meetings.",
    invokeRecipe: "gestalt apps invoke notion search -p query=meeting",
    expectedResult: `Found 3 pages matching “meeting”:

• Weekly product meeting notes
• Meeting agenda template
• Customer meeting follow-ups`,
  },
];

export const BUILD_STEPS: BuildStep[] = [
  {
    id: "connect",
    title: "Connect an app",
    description:
      "Start with Google Calendar, or try Slack or Notion if you already use them.",
    // Primary CTA is selection-scoped Connect in the page; catalog is a soft link.
    ctaLabel: "See all apps",
    to: "/apps",
    isComplete: (snapshot) => connectedAppIds(snapshot.integrations).size > 0,
  },
  {
    id: "authorize",
    title: "Grant access",
    description:
      "Create an API token so you can invoke operations from the CLI or other clients.",
    ctaLabel: "Create API token",
    to: "/settings",
    isComplete: (snapshot) => snapshot.tokens > 0,
  },
  {
    id: "invoke",
    title: "Make your first call",
    description:
      "Wire this workspace into your AI client over MCP, paste the prompt, and get a real result from the app you connected.",
    ctaLabel: "Open Invoke docs",
    to: "/docs/invoke",
    // Prerequisites only — the recipe panel stays expanded as the last step
    // when the journey is otherwise complete (see Build page focus model).
    isComplete: (snapshot) =>
      connectedAppIds(snapshot.integrations).size > 0 && snapshot.tokens > 0,
  },
];

/** Apps that are actually connected (not merely present in the catalog). */
export function connectedAppIds(integrations: Integration[]): Set<string> {
  return new Set(
    integrations
      .filter(
        (integration) => normalizeIntegrationStatus(integration).connected,
      )
      .map((integration) => integration.name),
  );
}

/** Prefer Calendar; otherwise the first connected starter among Slack, Notion. */
export function selectActiveStarter(
  integrations: Integration[],
): StarterAha {
  const connected = connectedAppIds(integrations);
  const calendar = STARTER_AHAS.find((aha) => aha.id === "google_calendar")!;
  if (connected.has(calendar.appId)) {
    return calendar;
  }
  for (const aha of STARTER_AHAS) {
    if (aha.priority === "alternate" && connected.has(aha.appId)) {
      return aha;
    }
  }
  return calendar;
}

export function isBuildComplete(snapshot: BuildWorkspaceSnapshot): boolean {
  return BUILD_STEPS.every((step) => step.isComplete(snapshot));
}
