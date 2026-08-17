/**
 * Canonical catalog of Setup host products (the app the human already uses).
 *
 * `claude` is Claude on the web / desktop. `claude-code` is the CLI.
 * Session storage v1 used `claude` for Claude Code; readers migrate that.
 */

export type BuildInstallAgentId =
  | "claude"
  | "chatgpt"
  | "cursor"
  | "claude-code"
  | "codex"
  | "other";

export type AssistantHostGroupId = "chat" | "coding" | "other";

export type AssistantHostIconKey =
  | "claude"
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
  id: BuildInstallAgentId;
  label: string;
  group: AssistantHostGroupId;
  docsHash: McpDocsHash;
  docsTabLabel: string;
  testId: string;
  installDescription: string;
  iconKey: AssistantHostIconKey;
  consoleSkin: AssistantHostConsoleSkin;
};

export const ASSISTANT_HOST_GROUPS: ReadonlyArray<{
  id: AssistantHostGroupId;
  label: string;
}> = [
  { id: "chat", label: "Chat" },
  { id: "coding", label: "Coding tools" },
  { id: "other", label: "Other" },
];

export const ASSISTANT_HOSTS: readonly AssistantHost[] = [
  {
    id: "claude",
    label: "Claude",
    group: "chat",
    docsHash: "mcp-claude",
    docsTabLabel: "Claude",
    testId: "build-install-card-claude",
    installDescription:
      "Add a custom connector in Claude on the web or in the Claude desktop app.",
    iconKey: "claude",
    consoleSkin: "claude",
  },
  {
    id: "chatgpt",
    label: "ChatGPT",
    group: "chat",
    docsHash: "mcp-chatgpt",
    docsTabLabel: "ChatGPT",
    testId: "build-install-card-chatgpt",
    installDescription:
      "Add Gestalt as a developer-mode app in ChatGPT.",
    iconKey: "chatgpt",
    consoleSkin: "codex",
  },
  {
    id: "cursor",
    label: "Cursor",
    group: "coding",
    docsHash: "mcp-cursor",
    docsTabLabel: "Cursor",
    testId: "build-install-card-cursor",
    installDescription: "Connect Cursor so it can use your Gestalt apps.",
    iconKey: "cursor",
    consoleSkin: "cursor",
  },
  {
    id: "claude-code",
    label: "Claude Code",
    group: "coding",
    docsHash: "mcp-claude-code",
    docsTabLabel: "Claude Code",
    testId: "build-install-card-claude-code",
    installDescription:
      "Run this command in your terminal from a project folder.",
    iconKey: "claude",
    consoleSkin: "claude",
  },
  {
    id: "codex",
    label: "Codex Desktop",
    group: "coding",
    docsHash: "mcp-codex",
    docsTabLabel: "Codex",
    testId: "build-install-card-codex",
    installDescription:
      "Run these commands in Terminal on the Mac where Codex Desktop is installed.",
    iconKey: "codex",
    consoleSkin: "codex",
  },
  {
    id: "other",
    label: "Other",
    group: "other",
    docsHash: "mcp-other",
    docsTabLabel: "Other clients",
    testId: "build-install-card-other",
    installDescription:
      "Paste this address into your assistant with your token.",
    iconKey: "other",
    consoleSkin: "claude",
  },
];

const HOST_BY_ID = new Map(
  ASSISTANT_HOSTS.map((host) => [host.id, host] as const),
);

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

export function assistantHostsInGroup(
  group: AssistantHostGroupId,
): readonly AssistantHost[] {
  return ASSISTANT_HOSTS.filter((host) => host.group === group);
}

export function assistantHostDocsHash(id: string): McpDocsHash {
  return assistantHostById(id)?.docsHash ?? "mcp-other";
}

export const MCP_CLIENT_TABS: ReadonlyArray<{
  id: McpDocsHash;
  label: string;
}> = ASSISTANT_HOSTS.map((host) => ({
  id: host.docsHash,
  label: host.docsTabLabel,
}));

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
