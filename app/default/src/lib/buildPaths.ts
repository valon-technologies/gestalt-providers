import type { Integration } from "@/lib/api";

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
  invokeRecipe: string;
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
    invokeRecipe:
      "gestalt apps invoke google_calendar events.list -p calendarId=primary -p maxResults=5",
  },
  {
    id: "slack",
    appId: "slack",
    label: "Slack",
    priority: "alternate",
    ahaTitle: "Post a hello",
    ahaDescription: "Send a message to a channel and see it appear in Slack.",
    invokeRecipe:
      "gestalt apps invoke slack chat.postMessage -p channel=CHANNEL_ID -p text='Hello from Gestalt'",
  },
  {
    id: "notion",
    appId: "notion",
    label: "Notion",
    priority: "alternate",
    ahaTitle: "Find a page",
    ahaDescription: "Search your workspace for a familiar page title.",
    invokeRecipe: "gestalt apps invoke notion search -p query=meeting",
  },
];

export const BUILD_STEPS: BuildStep[] = [
  {
    id: "connect",
    title: "Connect an app",
    description:
      "Start with Google Calendar, or try Slack or Notion if you already use them.",
    ctaLabel: "Open Apps",
    to: "/apps",
    isComplete: (snapshot) => snapshot.integrations.length > 0,
  },
  {
    id: "authorize",
    title: "Grant access",
    description:
      "Create an API token so you can invoke operations from the CLI or other clients.",
    ctaLabel: "Open Authorization",
    to: "/authorization",
    isComplete: (snapshot) => snapshot.tokens > 0,
  },
  {
    id: "invoke",
    title: "Make your first call",
    description:
      "Run the recipe below to see a real result from the app you connected.",
    ctaLabel: "Invoke docs",
    to: "/docs/invoke",
    isComplete: (snapshot) =>
      snapshot.integrations.length > 0 && snapshot.tokens > 0,
  },
];

export function connectedAppIds(integrations: Integration[]): Set<string> {
  return new Set(integrations.map((integration) => integration.name));
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
