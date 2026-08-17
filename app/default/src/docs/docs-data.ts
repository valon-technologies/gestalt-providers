import { MCP_DOCS_TITLE, MCP_OVERLAP_HEADING } from "@/lib/assistantConnectionCopy";
import { DOCS_PATH } from "@/lib/constants";
import { SETTINGS_TOKENS_PATH } from "@/lib/managed-identity-paths";

/**
 * Docs information architecture — single source of truth for:
 * - left-rail destinations (grouped by journey)
 * - on-this-page subsections
 * - audience (user vs admin)
 * - next/prev journey edges used by page footers
 *
 * Product surfaces that mint personal API tokens live under Settings
 * (`SETTINGS_TOKENS_PATH`). Never label that UI "Authorization" — that word is
 * reserved for granting app access (`/docs/authorization`).
 */

export const DOCS_GETTING_STARTED_PATH = `${DOCS_PATH}/getting-started` as const;
export const DOCS_CONNECT_PATH = `${DOCS_PATH}/connect` as const;
export const DOCS_INVOKE_PATH = `${DOCS_PATH}/invoke` as const;
export const DOCS_WORKFLOWS_PATH = `${DOCS_PATH}/workflows` as const;
export const DOCS_TOKENS_PATH = `${DOCS_PATH}/tokens` as const;
export const DOCS_AUTHORIZATION_PATH = `${DOCS_PATH}/authorization` as const;
export const DOCS_MCP_PATH = `${DOCS_PATH}/mcp` as const;
export const DOCS_TROUBLESHOOTING_PATH = `${DOCS_PATH}/troubleshooting` as const;

/** UI where users mint personal API tokens (not grant docs). */
export const DOCS_SETTINGS_TOKENS_HREF = SETTINGS_TOKENS_PATH;

export type DocsAudience = "user" | "admin";

export type DocsNavGroupId = "setup" | "automate" | "administer";

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
  { id: "automate", label: "Automate" },
  { id: "administer", label: "Administer" },
];

export const docsNavItems: DocsNavItem[] = [
  {
    id: "getting-started",
    href: DOCS_GETTING_STARTED_PATH,
    label: "Getting Started",
    group: "setup",
    audience: "user",
    subsections: [
      { id: "install", label: "Install" },
      { id: "point-cli", label: "Point the CLI at this workspace" },
      { id: "authenticate", label: "Authenticate" },
      { id: "authorization", label: "Grant App Access" },
      { id: "agent-environments", label: "Configure cloud environments" },
      { id: "workflows", label: "Inspect Workflows" },
    ],
  },
  {
    id: "connect",
    href: DOCS_CONNECT_PATH,
    label: "Connect Apps",
    group: "setup",
    audience: "user",
    subsections: [],
  },
  {
    id: "invoke",
    href: DOCS_INVOKE_PATH,
    label: "Invoke Operations",
    group: "setup",
    audience: "user",
    // Option switchers use hash values without matching DOM ids (sticky chrome).
    subsections: [],
  },
  {
    id: "tokens",
    href: DOCS_TOKENS_PATH,
    label: "Manage API Tokens",
    group: "automate",
    audience: "user",
    subsections: [],
  },
  {
    id: "mcp",
    href: DOCS_MCP_PATH,
    label: MCP_DOCS_TITLE,
    group: "automate",
    audience: "user",
    // Client options are hash-backed SegmentedControl values, not headings.
    subsections: [
      {
        id: "mcp-overlap",
        label: MCP_OVERLAP_HEADING,
      },
    ],
  },
  {
    id: "workflows",
    href: DOCS_WORKFLOWS_PATH,
    label: "Inspect Workflows",
    group: "automate",
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
    group: "administer",
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
 * Linear journey edges for StepPager. Forward/back follow sidebar order so the
 * nav array is the only sequence to maintain. Soft prose prerequisites are not
 * modeled on nav items — journey edges and hand-authored copy stay separate.
 */
export function getDocsJourneyEdges(item: DocsNavItem): {
  previous: DocsNavLink | null;
  next: DocsNavLink | null;
} {
  const index = docsNavItems.findIndex((candidate) => candidate.id === item.id);
  const previousItem = index > 0 ? docsNavItems[index - 1] : undefined;
  const nextItem =
    index >= 0 && index < docsNavItems.length - 1
      ? docsNavItems[index + 1]
      : undefined;
  return {
    previous: previousItem
      ? { href: previousItem.href, label: previousItem.label }
      : null,
    next: nextItem ? { href: nextItem.href, label: nextItem.label } : null,
  };
}
