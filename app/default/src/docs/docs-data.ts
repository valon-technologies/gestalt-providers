export interface DocsSubsection {
  id: string;
  label: string;
}

export interface DocsNavItem {
  id: string;
  href: string;
  label: string;
  subsections: DocsSubsection[];
}

export const docsNavItems: DocsNavItem[] = [
  {
    id: "getting-started",
    href: "/docs/getting-started",
    label: "Getting Started",
    subsections: [
      { id: "install", label: "Install" },
      { id: "point-cli", label: "Point the CLI" },
      { id: "authenticate", label: "Authenticate" },
      { id: "authorization", label: "Grant authorization" },
      { id: "agent-environments", label: "Cloud environments" },
      { id: "workflows", label: "Inspect workflows" },
    ],
  },
  { id: "connect", href: "/docs/connect", label: "Connect Apps", subsections: [] },
  { id: "invoke", href: "/docs/invoke", label: "Invoke Operations", subsections: [] },
  {
    id: "workflows",
    href: "/docs/workflows",
    label: "Manage Workflows",
    subsections: [
      { id: "wf-help", label: "Start with help" },
      { id: "wf-runs", label: "Inspect runs" },
    ],
  },
  { id: "tokens", href: "/docs/tokens", label: "Manage API Tokens", subsections: [] },
  {
    id: "authorization",
    href: "/docs/authorization",
    label: "Grant Authorization",
    subsections: [
      { id: "authz-lookup-subject", label: "Look up subject ID" },
      { id: "authz-grant-app-access", label: "Grant app access" },
      { id: "authz-operation-roles", label: "Operation roles" },
      { id: "authz-nested-apps", label: "Nested app grants" },
      { id: "authz-admins", label: "Gestalt admin access" },
      { id: "authz-deploy", label: "Deploy changes" },
      { id: "authz-inspect", label: "Inspect grants" },
    ],
  },
  { id: "mcp", href: "/docs/mcp", label: "Use With MCP", subsections: [] },
  {
    id: "troubleshooting",
    href: "/docs/troubleshooting",
    label: "Troubleshooting",
    subsections: [
      { id: "ts-not-authenticated", label: "Not authenticated" },
      { id: "ts-multiple-connections", label: "Multiple connections" },
      { id: "ts-empty-tools", label: "Empty MCP tool list" },
    ],
  },
];

export function getActiveDocsNavItem(pathname: string): DocsNavItem {
  if (pathname === "/docs" || pathname === "/docs/getting-started") {
    return docsNavItems[0];
  }

  return (
    docsNavItems.find(
      (item) => pathname === item.href || pathname.startsWith(`${item.href}/`),
    ) ?? docsNavItems[0]
  );
}
