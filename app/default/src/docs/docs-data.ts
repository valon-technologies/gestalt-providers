import { ASSISTANT_OVERLAP_TITLE } from "@/lib/assistantConnectionCopy";
import { DOCS_PATH } from "@/lib/constants";
import { SETTINGS_TOKENS_PATH } from "@/lib/managed-identity-paths";

/**
 * Docs information architecture — single source of truth for:
 * - left-rail destinations (grouped by client posture)
 * - on-this-page subsections
 * - audience (user vs admin)
 * - next/prev journey edges used by page footers
 *
 * Two setups: assistants (browser connect, token, MCP; no Gestalt CLI) and
 * terminal (install CLI, then CLI connect / invoke / workflows). The left
 * rail lists both. The journey footer walks `docsJourneyTracks`, not the
 * full sidebar, so finishing MCP Clients does not send people to the CLI.
 *
 * Product surfaces that mint personal API tokens live under Settings
 * (`SETTINGS_TOKENS_PATH`). Never label that UI "Authorization". That word is
 * reserved for granting app access (`/docs/authorization`).
 */

export const DOCS_GETTING_STARTED_PATH = `${DOCS_PATH}/getting-started` as const;
export const DOCS_CONNECT_PATH = `${DOCS_PATH}/connect` as const;
export const DOCS_INVOKE_PATH = `${DOCS_PATH}/invoke` as const;
export const DOCS_CLI_PATH = `${DOCS_PATH}/cli` as const;
export const DOCS_WORKFLOWS_PATH = `${DOCS_PATH}/workflows` as const;
export const DOCS_TOKENS_PATH = `${DOCS_PATH}/tokens` as const;
export const DOCS_AUTHORIZATION_PATH = `${DOCS_PATH}/authorization` as const;
export const DOCS_MCP_PATH = `${DOCS_PATH}/mcp` as const;
export const DOCS_TROUBLESHOOTING_PATH = `${DOCS_PATH}/troubleshooting` as const;

/** UI where users mint personal API tokens (not grant docs). */
export const DOCS_SETTINGS_TOKENS_HREF = SETTINGS_TOKENS_PATH;

export type DocsAudience = "user" | "admin";

export type DocsNavGroupId =
  | "setup"
  | "assistants"
  | "terminal"
  | "administer"
  | "help";

export interface DocsSubsection {
  id: string;
  label: string;
}

export interface DocsNavLink {
  href: string;
  label: string;
}

export interface DocsNavItem {
  id: string;
  href: string;
  label: string;
  group: DocsNavGroupId;
  /** Who the page is written for. Admin pages need an audience callout in body. */
  audience: DocsAudience;
  subsections: DocsSubsection[];
}

export const DOCS_NAV_GROUPS: ReadonlyArray<{
  id: DocsNavGroupId;
  label: string;
}> = [
  { id: "setup", label: "Setup" },
  { id: "assistants", label: "Assistants" },
  { id: "terminal", label: "Terminal" },
  { id: "administer", label: "Administer" },
  { id: "help", label: "Help" },
];

export const docsNavItems: DocsNavItem[] = [
  {
    id: "getting-started",
    href: DOCS_GETTING_STARTED_PATH,
    label: "Getting Started",
    group: "setup",
    audience: "user",
    subsections: [
      { id: "connect-apps", label: "Connect Apps" },
      { id: "create-token", label: "Create an API token" },
      { id: "next-steps", label: "Next steps" },
    ],
  },
  {
    id: "tokens",
    href: DOCS_TOKENS_PATH,
    label: "API Tokens",
    group: "setup",
    audience: "user",
    subsections: [
      { id: "tokens-use", label: "What to do with the token" },
      { id: "tokens-cli", label: "Create from the terminal" },
    ],
  },
  {
    id: "mcp",
    href: DOCS_MCP_PATH,
    label: "MCP Clients",
    group: "assistants",
    audience: "user",
    // MCP destination tabs stay hash-backed (no TOC ids). Other clients is
    // a heading because it is not a dest tab.
    subsections: [
      { id: "mcp-connect", label: "Choose your assistant" },
      { id: "mcp-overlap", label: ASSISTANT_OVERLAP_TITLE },
      { id: "mcp-env", label: "Store the token on your computer" },
      { id: "mcp-other", label: "Other clients" },
      { id: "mcp-cloud", label: "Configure cloud environments" },
      { id: "mcp-verify", label: "Verify your tools" },
    ],
  },
  {
    id: "cli",
    href: DOCS_CLI_PATH,
    label: "Gestalt CLI",
    group: "terminal",
    audience: "user",
    subsections: [
      { id: "cli-install", label: "Install the CLI" },
      { id: "cli-point", label: "Point the CLI at this workspace" },
      { id: "cli-authenticate", label: "Authenticate from the terminal" },
    ],
  },
  {
    id: "connect",
    href: DOCS_CONNECT_PATH,
    label: "Connect Apps",
    group: "terminal",
    audience: "user",
    subsections: [
      { id: "connect-browser", label: "Connect Apps in the browser" },
      { id: "connect-cli", label: "Connect from the terminal" },
    ],
  },
  {
    id: "invoke",
    href: DOCS_INVOKE_PATH,
    label: "Invoke Operations",
    group: "terminal",
    audience: "user",
    // Option switchers use hash values without matching DOM ids (sticky chrome).
    subsections: [],
  },
  {
    id: "workflows",
    href: DOCS_WORKFLOWS_PATH,
    label: "Inspect Workflows",
    group: "terminal",
    audience: "user",
    subsections: [
      { id: "wf-help", label: "Start with help" },
      { id: "wf-runs", label: "Inspect runs" },
    ],
  },
  {
    id: "authorization",
    href: DOCS_AUTHORIZATION_PATH,
    label: "Grant App Access",
    group: "administer",
    audience: "admin",
    subsections: [
      { id: "authz-plugin-access", label: "Grant app access" },
      { id: "authz-service-accounts", label: "Grant service account access" },
      { id: "authz-admins", label: "Grant built-in admin access" },
      { id: "authz-inspect", label: "Inspect grants" },
    ],
  },
  {
    id: "troubleshooting",
    href: DOCS_TROUBLESHOOTING_PATH,
    label: "Troubleshooting",
    group: "help",
    audience: "user",
    subsections: [
      { id: "ts-not-authenticated", label: "The CLI says you are not authenticated" },
      { id: "ts-multiple-connections", label: "An app has multiple connections" },
      { id: "ts-empty-tools", label: "The MCP endpoint is mounted, but the tool list is empty" },
      { id: "ts-forbidden", label: "Access denied after grant" },
      {
        id: "ts-overlap",
        label: "The same app appears twice in my assistant",
      },
      {
        id: "ts-codex-desktop-tools",
        label: "Codex Desktop does not list Gestalt tools",
      },
    ],
  },
];

/** Heading / TOC label for a subsection id. DocsContent must not restate titles. */
export function docsSubsectionLabel(id: string): string {
  for (const item of docsNavItems) {
    const subsection = item.subsections.find((entry) => entry.id === id);
    if (subsection) return subsection.label;
  }
  throw new Error(`Unknown docs subsection id: ${id}`);
}

export function getActiveDocsNavItem(pathname: string): DocsNavItem {
  if (pathname === DOCS_PATH || pathname === DOCS_GETTING_STARTED_PATH) {
    return docsNavItems[0];
  }

  return (
    docsNavItems.find(
      (item) => pathname === item.href || pathname.startsWith(`${item.href}/`),
    ) ?? docsNavItems[0]
  );
}

export function docsNavItemsByGroup(
  group: DocsNavGroupId,
): DocsNavItem[] {
  return docsNavItems.filter((item) => item.group === group);
}

/**
 * Named reader tracks for the journey footer. Each track is one or more left-rail
 * groups, in the order people should walk them. Setup then Assistants is the
 * browser path. Terminal is the CLI path. Administer and Help stand alone.
 */
export const docsJourneyTracks = {
  assistants: ["setup", "assistants"],
  terminal: ["terminal"],
  administer: ["administer"],
  help: ["help"],
} as const satisfies Record<string, readonly DocsNavGroupId[]>;

export type DocsJourneyTrackId = keyof typeof docsJourneyTracks;

export function docsJourneyTrackItems(
  trackId: DocsJourneyTrackId,
): DocsNavItem[] {
  return docsJourneyTracks[trackId].flatMap((group) =>
    docsNavItemsByGroup(group),
  );
}

function docsJourneyTrackIdFor(
  item: DocsNavItem,
): DocsJourneyTrackId | undefined {
  const tracks = Object.entries(docsJourneyTracks) as Array<
    [DocsJourneyTrackId, readonly DocsNavGroupId[]]
  >;
  return tracks.find(([, groups]) => groups.includes(item.group))?.[0];
}

/**
 * Next/Previous for StepPager. Edges stay inside one journey track so the
 * left-rail listing is not treated as one walkthrough. Soft prose
 * prerequisites are not modeled on nav items.
 */
export function getDocsJourneyEdges(item: DocsNavItem): {
  previous: DocsNavLink | null;
  next: DocsNavLink | null;
} {
  const trackId = docsJourneyTrackIdFor(item);
  if (!trackId) {
    return { previous: null, next: null };
  }
  const track = docsJourneyTrackItems(trackId);
  const index = track.findIndex((candidate) => candidate.id === item.id);
  const previousItem = index > 0 ? track[index - 1] : undefined;
  const nextItem =
    index >= 0 && index < track.length - 1 ? track[index + 1] : undefined;
  return {
    previous: previousItem
      ? { href: previousItem.href, label: previousItem.label }
      : null,
    next: nextItem ? { href: nextItem.href, label: nextItem.label } : null,
  };
}
