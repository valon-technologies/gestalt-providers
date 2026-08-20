/**
 * Where a personal API token is saved in each offered assistant.
 *
 * Visible tabs are Setup's offered named products (not Others). Tab ids are
 * `dest-${host.id}` so they do not collide with leftover `mcp-*` hashes.
 *
 * `dest-claude` is hash-only: Claude.ai leftover links. It is not a tab.
 * ChatGPT and Codex are separate products.
 */
import {
  ASSISTANT_HOSTS_OFFERED,
  assistantDestinationHash,
  type AssistantHost,
  type AssistantHostId,
} from "@/lib/assistantHosts";

export type AssistantDestinationTabHostId = Exclude<
  AssistantHostId,
  "claude" | "other"
>;

export type AssistantDestinationTabId =
  `dest-${AssistantDestinationTabHostId}`;

export const assistantDestinationLegacyIds = ["dest-claude"] as const;

export type AssistantDestinationLegacyId =
  (typeof assistantDestinationLegacyIds)[number];

export type AssistantDestinationId =
  | AssistantDestinationTabId
  | AssistantDestinationLegacyId;

function isDestinationTabHost(
  host: AssistantHost,
): host is AssistantHost & { id: AssistantDestinationTabHostId } {
  return host.id !== "other" && host.id !== "claude";
}

export const assistantDestinationTabs = ASSISTANT_HOSTS_OFFERED.filter(
  isDestinationTabHost,
).map((host) => ({
  id: assistantDestinationHash(host.id),
  hostId: host.id,
  label: host.label,
}));

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
