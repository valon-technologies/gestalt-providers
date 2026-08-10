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
  /** Primary next step in the same journey (rendered as page footer). */
  next?: DocsNavLink;
  prerequisites?: DocsNavLink[];
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
      { id: "point-cli", label: "Point the CLI" },
      { id: "authenticate", label: "Authenticate" },
      { id: "authorization", label: "Grant App Access" },
      { id: "agent-environments", label: "Configure cloud environments" },
      { id: "workflows", label: "Inspect Workflows" },
    ],
    next: { href: DOCS_CONNECT_PATH, label: "Connect Apps" },
  },
  {
    id: "connect",
    href: DOCS_CONNECT_PATH,
    label: "Connect Apps",
    group: "setup",
    audience: "user",
    subsections: [],
    prerequisites: [
      { href: DOCS_GETTING_STARTED_PATH, label: "Getting Started" },
    ],
    next: { href: DOCS_INVOKE_PATH, label: "Invoke Operations" },
  },
  {
    id: "invoke",
    href: DOCS_INVOKE_PATH,
    label: "Invoke Operations",
    group: "setup",
    audience: "user",
    // Option switchers use hash values without matching DOM ids (sticky chrome).
    subsections: [],
    prerequisites: [{ href: DOCS_CONNECT_PATH, label: "Connect Apps" }],
    next: { href: DOCS_TOKENS_PATH, label: "Manage API Tokens" },
  },
  {
    id: "tokens",
    href: DOCS_TOKENS_PATH,
    label: "Manage API Tokens",
    group: "automate",
    audience: "user",
    subsections: [],
    prerequisites: [
      { href: DOCS_GETTING_STARTED_PATH, label: "Getting Started" },
    ],
    next: { href: DOCS_MCP_PATH, label: "Use With MCP" },
  },
  {
    id: "mcp",
    href: DOCS_MCP_PATH,
    label: "Use With MCP",
    group: "automate",
    audience: "user",
    // MCP client options are hash-backed SegmentedControl values, not headings.
    subsections: [],
    prerequisites: [{ href: DOCS_TOKENS_PATH, label: "Manage API Tokens" }],
    next: { href: DOCS_WORKFLOWS_PATH, label: "Inspect Workflows" },
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
    prerequisites: [
      { href: DOCS_GETTING_STARTED_PATH, label: "Getting Started" },
    ],
    next: { href: DOCS_TROUBLESHOOTING_PATH, label: "Troubleshooting" },
  },
  {
    id: "authorization",
    href: DOCS_AUTHORIZATION_PATH,
    label: "Grant App Access",
    group: "administer",
    audience: "admin",
    subsections: [
      { id: "authz-plugin-access", label: "Grant app access" },
      { id: "authz-service-accounts", label: "Service accounts" },
      { id: "authz-admins", label: "Built-in admins" },
      { id: "authz-inspect", label: "Inspect grants" },
    ],
    prerequisites: [
      { href: DOCS_GETTING_STARTED_PATH, label: "Getting Started" },
    ],
    next: { href: DOCS_TROUBLESHOOTING_PATH, label: "Troubleshooting" },
  },
  {
    id: "troubleshooting",
    href: DOCS_TROUBLESHOOTING_PATH,
    label: "Troubleshooting",
    group: "administer",
    audience: "user",
    subsections: [
      { id: "ts-not-authenticated", label: "Not authenticated" },
      { id: "ts-multiple-connections", label: "Multiple connections" },
      { id: "ts-empty-tools", label: "Empty MCP tool list" },
      { id: "ts-forbidden", label: "Access denied after grant" },
    ],
  },
];

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
