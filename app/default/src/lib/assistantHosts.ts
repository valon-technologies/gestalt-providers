/**
 * Canonical catalog of Setup host products (the app the human already uses).
 *
 * `offered` hosts appear in Choose your assistant and in MCP docs. Claude
 * (web / desktop) and ChatGPT stay in this catalog so an in-progress Setup
 * session can still finish their install recipe. They are not offered.
 *
 * `claude` is Claude on the web / desktop. `claude-code` is the terminal.
 * `cursor` is the Cursor app. `cursor-agent` is Cursor Agent (same
 * `.cursor/mcp.json` as Cursor, no one-click IDE button).
 * Session storage v1 used `claude` for Claude Code; readers migrate that.
 */

export type AssistantHostId =
  | "claude"
  | "claude-code"
  | "chatgpt"
  | "codex"
  | "cursor"
  | "cursor-agent"
  | "other";

export type BuildInstallAgentId = AssistantHostId;

export type AssistantHostIconKey =
  | "claude"
  | "claude-code"
  | "chatgpt"
  | "cursor"
  | "codex"
  | "other";

/** Setup AgentConsole composition skin — not an AgentConsole prop. */
export type AssistantHostConsoleSkin = "claude" | "codex" | "cursor";

export type McpDocsHash =
  | "mcp-claude"
  | "mcp-chatgpt"
  | "mcp-cursor"
  | "mcp-claude-code"
  | "mcp-codex"
  | "mcp-other";

export type AssistantHost = {
  id: AssistantHostId;
  label: string;
  docsHash: McpDocsHash;
  docsTabLabel: string;
  testId: string;
  installDescription: string;
  iconKey: AssistantHostIconKey;
  consoleSkin: AssistantHostConsoleSkin;
  /**
   * Shown in Choose your assistant and MCP docs. False keeps the install
   * recipe for in-progress sessions only.
   */
  offered: boolean;
};

export const ASSISTANT_HOST_PICKER_GRID_CLASS =
  "grid grid-cols-2 gap-3 sm:grid-cols-4";

export const ASSISTANT_HOSTS: readonly AssistantHost[] = [
  {
    id: "claude",
    label: "Claude",
    docsHash: "mcp-claude",
    docsTabLabel: "Claude",
    testId: "build-install-card-claude",
    installDescription:
      "Add a custom connector in Claude on the web or in the Claude desktop app.",
    iconKey: "claude",
    consoleSkin: "claude",
    offered: false,
  },
  {
    id: "claude-code",
    label: "Claude Code",
    docsHash: "mcp-claude-code",
    docsTabLabel: "Claude Code",
    testId: "build-install-card-claude-code",
    installDescription:
      "Run this command in your terminal from a project folder.",
    iconKey: "claude-code",
    consoleSkin: "claude",
    offered: true,
  },
  {
    id: "chatgpt",
    label: "ChatGPT",
    docsHash: "mcp-chatgpt",
    docsTabLabel: "ChatGPT",
    testId: "build-install-card-chatgpt",
    installDescription:
      "Add Gestalt as a developer-mode app in ChatGPT.",
    iconKey: "chatgpt",
    consoleSkin: "codex",
    offered: false,
  },
  {
    id: "codex",
    label: "Codex",
    docsHash: "mcp-codex",
    docsTabLabel: "Codex",
    testId: "build-install-card-codex",
    installDescription:
      "Run these commands in Terminal on the Mac where Codex is installed.",
    iconKey: "codex",
    consoleSkin: "codex",
    offered: true,
  },
  {
    id: "cursor",
    label: "Cursor",
    docsHash: "mcp-cursor",
    docsTabLabel: "Cursor",
    testId: "build-install-card-cursor",
    installDescription: "Connect Cursor so it can use your Gestalt apps.",
    iconKey: "cursor",
    consoleSkin: "cursor",
    offered: true,
  },
  {
    id: "cursor-agent",
    label: "Cursor Agent",
    docsHash: "mcp-cursor",
    docsTabLabel: "Cursor",
    testId: "build-install-card-cursor-agent",
    installDescription:
      "Paste this into .cursor/mcp.json. Cursor Agent reads the same MCP config as Cursor.",
    iconKey: "cursor",
    consoleSkin: "cursor",
    offered: true,
  },
  {
    id: "other",
    label: "Others",
    docsHash: "mcp-other",
    docsTabLabel: "Other clients",
    testId: "build-install-card-other",
    installDescription:
      "Use these MCP settings in any client that accepts a URL and an Authorization header.",
    iconKey: "other",
    consoleSkin: "claude",
    offered: true,
  },
];

const HOST_BY_ID = new Map(
  ASSISTANT_HOSTS.map((host) => [host.id, host] as const),
);

export const ASSISTANT_HOSTS_OFFERED: readonly AssistantHost[] =
  ASSISTANT_HOSTS.filter((host) => host.offered);

export const ASSISTANT_HOSTS_IN_PICKER = ASSISTANT_HOSTS_OFFERED;

export function isBuildInstallAgentId(
  value: string,
): value is BuildInstallAgentId {
  return HOST_BY_ID.has(value as BuildInstallAgentId);
}

export function assistantHostById(
  id: string,
): AssistantHost | undefined {
  return HOST_BY_ID.get(id as BuildInstallAgentId);
}

/**
 * URL hash for "open docs for this host".
 *
 * `docsHash` still names the config-file recipe tab (`mcp-codex`, …).
 * Claude and ChatGPT no longer have recipe tabs; their walkthroughs live
 * under `dest-*`. Setup and other deep links must use this landing hash,
 * not `docsHash`, or `#mcp-chatgpt` falls through to Claude.
 */
export function assistantDocsLandingHash(
  host: AssistantHost | undefined,
): string {
  if (!host) return "mcp-other";
  if (host.id === "claude") return "dest-claude";
  if (host.id === "chatgpt") return "dest-chatgpt";
  return host.docsHash;
}

/** Old recipe hashes that now select a dest-* walkthrough. */
export const ASSISTANT_DOCS_LANDING_HASH_ALIASES: Readonly<
  Record<string, string>
> = Object.fromEntries(
  ASSISTANT_HOSTS.flatMap((host) => {
    const landing = assistantDocsLandingHash(host);
    return landing === host.docsHash ? [] : [[host.docsHash, landing]];
  }),
);

export const MCP_CLIENT_TABS: ReadonlyArray<{
  id: McpDocsHash;
  label: string;
}> = (() => {
  const seen = new Set<McpDocsHash>();
  const tabs: Array<{ id: McpDocsHash; label: string }> = [];
  for (const host of ASSISTANT_HOSTS_OFFERED) {
    if (seen.has(host.docsHash)) continue;
    seen.add(host.docsHash);
    tabs.push({ id: host.docsHash, label: host.docsTabLabel });
  }
  return tabs;
})();

/**
 * v1 stored `claude` for Claude Code. Map that onto `claude-code` so an
 * in-progress Setup session does not land on the Claude chat recipe.
 */
export function normalizeStoredInstallAgentId(
  raw: string,
  source: "current" | "legacy" = "current",
): BuildInstallAgentId | "" {
  if (source === "legacy" && raw === "claude") return "claude-code";
  return isBuildInstallAgentId(raw) ? raw : "";
}
