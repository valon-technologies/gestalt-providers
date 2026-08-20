/**
 * Where a personal API token is saved in each offered assistant.
 *
 * Visible tabs are the named products Setup offers (not Others). Hash ids stay
 * `dest-*` so they do not collide with leftover `mcp-*` recipe hashes.
 *
 * `dest-claude` is hash-only: Claude.ai leftover links. It is not a tab.
 * ChatGPT and Codex are separate products.
 */
export const assistantDestinationTabs = [
  { id: "dest-claude-code", hostId: "claude-code", label: "Claude Code" },
  { id: "dest-chatgpt", hostId: "chatgpt", label: "ChatGPT" },
  { id: "dest-codex", hostId: "codex", label: "Codex" },
  { id: "dest-cursor", hostId: "cursor", label: "Cursor" },
  { id: "dest-cursor-agent", hostId: "cursor-agent", label: "Cursor Agent" },
] as const;

export const assistantDestinationLegacyIds = ["dest-claude"] as const;

export type AssistantDestinationTabId =
  (typeof assistantDestinationTabs)[number]["id"];

export type AssistantDestinationLegacyId =
  (typeof assistantDestinationLegacyIds)[number];

export type AssistantDestinationId =
  | AssistantDestinationTabId
  | AssistantDestinationLegacyId;

export const assistantDestinationTabIds = assistantDestinationTabs.map(
  (tab) => tab.id,
);

export const assistantDestinationIds: readonly AssistantDestinationId[] = [
  ...assistantDestinationTabIds,
  ...assistantDestinationLegacyIds,
];

export const defaultAssistantDestinationId: AssistantDestinationTabId =
  "dest-claude-code";

export const ASSISTANT_DESTINATION_SWITCHER_LABEL = "Choose your assistant";

export const assistantDestinationMedia = {
  "dest-claude": {
    video: "/docs/add-token-claude.mp4",
    poster: "/docs/add-token-claude.jpg",
    caption: "Claude connector form. Paste the token in Request headers.",
  },
} as const;

export const DOCS_TOKEN_PLACEHOLDER = "gst_api_your_token_here";
