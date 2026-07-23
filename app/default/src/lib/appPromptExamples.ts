/**
 * Curated agent prompt examples for app detail pages — ChatGPT plugin–style
 * “try this” chips (`@App …`).
 *
 * Interim SoT until examples land on the app manifest / API.
 */

export type AppPromptExample = {
  /** Catalog display name used as the `@…` handle (may include spaces). */
  displayName: string;
  /** Prompt body after the mention. */
  body: string;
  /** Full copyable prompt: `@DisplayName body`. */
  prompt: string;
};

/**
 * Build `@DisplayName …` mention text. Spaces in the label stay (matches
 * catalog display names; ChatGPT short handles are single tokens when possible).
 */
export function formatAppPromptMention(
  displayName: string,
  body: string,
): string {
  const handle = displayName.trim() || "App";
  return `@${handle} ${body.trim()}`;
}

const APP_PROMPT_BODIES: Readonly<Record<string, readonly string[]>> = {
  linear: [
    "Triage or update relevant issues for this task with clear next actions",
  ],
  aiSpendTracker: ["How much did I spend on AI last week?"],
  oncall: ["Who’s on call right now?"],
  slack: ["Summarize unread mentions in #eng from today"],
  pagerduty: ["What incidents are open for my team right now?"],
  github: ["Open PRs waiting on my review"],
  jira: ["List my in-progress tickets and blockers"],
  ashby: ["Which candidates need a follow-up this week?"],
  valonSats: ["Am I ready for another servicing quiz?"],
  valonLearn: ["What’s next in my onboarding?"],
  intercom: ["What support threads need me today?"],
  frontPorch: ["What borrower threads need a human today?"],
  gmail: ["Draft a short reply to my latest unread email"],
  google_calendar: ["What’s on my calendar for the rest of today?"],
  notion: ["Find the latest notes on our Q3 roadmap"],
  datadog: ["Any error spikes in production in the last hour?"],
  incident_io: ["Summarize the active incidents"],
  bigquery: ["What’s our 30-day delinquency rate?"],
  hex: ["Open the latest loan portfolio report"],
  ramp: ["Show my recent card spend this week"],
  rippling: ["Who’s out of office this week?"],
};

/**
 * Prompt examples for an app detail promo. Falls back to a generic ask when
 * the app has no curated body yet.
 */
export function getAppPromptExamples(
  appName: string,
  displayName: string,
): AppPromptExample[] {
  const name = displayName.trim() || appName;
  const bodies = APP_PROMPT_BODIES[appName];
  const resolved =
    bodies && bodies.length > 0
      ? bodies
      : (["What can you help me with in this workspace?"] as const);

  return resolved.map((body) => ({
    displayName: name,
    body,
    prompt: formatAppPromptMention(name, body),
  }));
}
