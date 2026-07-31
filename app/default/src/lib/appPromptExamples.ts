import type { Integration } from "@/lib/api";

const genericMcpPrompt = "What can you help me with in this workspace?";

/**
 * Returns the first configured prompt for an app. The generic MCP prompt is a
 * UI affordance only; app-specific content must come from Gestalt config.
 */
export function getAppPromptExample(
  integration: Pick<Integration, "prompts">,
  hasMcpSurface: boolean,
): string | undefined {
  const configured = integration.prompts
    ?.map((prompt) => prompt.text.trim())
    .find(Boolean);
  return configured ?? (hasMcpSurface ? genericMcpPrompt : undefined);
}
