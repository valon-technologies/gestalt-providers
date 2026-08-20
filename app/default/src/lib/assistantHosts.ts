/**
 * Canonical catalog of Setup host products (the app the human already uses).
 *
 * `offered` hosts appear in Setup's Choose your assistant. Docs MCP Clients
 * uses the same named products (minus Others) for destination tabs. Claude
 * (web / desktop) stays in this catalog so an in-progress Setup session can
 * still finish its install recipe. It is not offered.
 *
 * `claude` is Claude on the web / desktop. `claude-code` is the terminal.
 * `chatgpt` is the ChatGPT app (custom MCP). `codex` is Codex Desktop.
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

/** Optional walkthrough video for a host install recipe. Paths are app-absolute. */
export type AssistantInstallDemo = {
  src: string;
  poster: string;
};

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
  testId: string;
  installDescription: string;
  iconKey: AssistantHostIconKey;
  consoleSkin: AssistantHostConsoleSkin;
  /**
   * Shown in Choose your assistant and MCP docs. False keeps the install
   * recipe for in-progress sessions only.
   */
  offered: boolean;
  installDemo?: AssistantInstallDemo;
};

export const ASSISTANT_HOST_PICKER_GRID_CLASS =
  "grid grid-cols-2 gap-3 sm:grid-cols-4";

export const ASSISTANT_HOSTS: readonly AssistantHost[] = [
  {
    id: "claude",
    label: "Claude",
    docsHash: "mcp-claude",
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
    testId: "build-install-card-chatgpt",
    installDescription:
      "Add Gestalt as a custom MCP in the ChatGPT app. You will paste a URL and a token.",
    iconKey: "chatgpt",
    consoleSkin: "codex",
    offered: true,
    installDemo: {
      src: "/setup/chatgpt-install.mp4",
      poster: "/setup/chatgpt-install.jpg",
    },
  },
  {
    id: "codex",
    label: "Codex",
    docsHash: "mcp-codex",
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
    testId: "build-install-card-cursor-agent",
    installDescription:
      "Connect Cursor Agent so it can use your Gestalt apps.",
    iconKey: "cursor",
    consoleSkin: "cursor",
    offered: true,
  },
  {
    id: "other",
    label: "Others",
    docsHash: "mcp-other",
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
 * MCP Clients hash for a host. Named products (including leftover Claude on
 * the web) use `dest-${id}`. Others stays the Other clients heading.
 */
export function assistantDestinationHash(
  hostId: Exclude<AssistantHostId, "other">,
): `dest-${Exclude<AssistantHostId, "other">}` {
  return `dest-${hostId}`;
}

/**
 * URL hash for "open docs for this host".
 *
 * Offered named products land on Choose your assistant (`dest-*`).
 * Claude on the web keeps leftover `dest-claude` (not a tab). Others still
 * uses the config-file heading `mcp-other`. Do not alias `mcp-cursor` onto
 * Cursor Agent: Cursor and Cursor Agent share that leftover hash, and it
 * must stay Cursor.
 */
export function assistantDocsLandingHash(
  host: AssistantHost | undefined,
): string {
  if (!host || host.id === "other") return "mcp-other";
  return assistantDestinationHash(host.id);
}

/** Leftover `mcp-*` hashes that now select a dest walkthrough. */
export const ASSISTANT_DOCS_LANDING_HASH_ALIASES: Readonly<
  Record<string, string>
> = Object.fromEntries(
  ASSISTANT_HOSTS.filter(
    (
      host,
    ): host is AssistantHost & {
      id: Exclude<AssistantHostId, "other" | "cursor-agent">;
    } => host.id !== "other" && host.id !== "cursor-agent",
  ).map((host) => [host.docsHash, assistantDestinationHash(host.id)]),
);

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
