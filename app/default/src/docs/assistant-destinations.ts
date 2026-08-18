/**
 * Where a personal API token is saved in each assistant.
 * Hash ids are distinct from MCP config-file recipes (`mcp-claude-code`, …)
 * so both switchers can share a page without stealing each other's hash.
 */
export const assistantDestinationTabs = [
  { id: "dest-claude", label: "Claude" },
  { id: "dest-chatgpt", label: "ChatGPT" },
  { id: "dest-cursor", label: "Cursor" },
] as const;

export type AssistantDestinationId =
  (typeof assistantDestinationTabs)[number]["id"];

export const assistantDestinationIds = assistantDestinationTabs.map(
  (tab) => tab.id,
);

export const defaultAssistantDestinationId: AssistantDestinationId =
  "dest-claude";

export const ASSISTANT_DESTINATION_SWITCHER_LABEL = "Choose your assistant";

export const assistantDestinationMedia = {
  "dest-claude": {
    video: "/docs/add-token-claude.mp4",
    poster: "/docs/add-token-claude.jpg",
    caption: "Claude connector form. Paste the token in Request headers.",
  },
  "dest-chatgpt": {
    video: "/docs/add-token-chatgpt.mp4",
    poster: "/docs/add-token-chatgpt.jpg",
    caption: "ChatGPT connector form. Paste the token in the Token field.",
  },
} as const;
