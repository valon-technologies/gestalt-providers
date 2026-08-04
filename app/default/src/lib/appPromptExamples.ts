import type { Integration } from "@/lib/api";

const genericMcpPrompt = "What can you help me with in this workspace?";

export type AppPromptExample = {
  id: string;
  text: string;
};

/**
 * Returns configured prompts for an app. The generic MCP prompt is a UI
 * affordance only; app-specific content must come from Gestalt config.
 */
export function getAppPromptExamples(
  integration: Pick<Integration, "prompts">,
  hasMcpSurface: boolean,
): AppPromptExample[] {
  const configured =
    integration.prompts
      ?.map((prompt) => ({
        id: prompt.id.trim(),
        text: prompt.text.trim(),
      }))
      .filter((prompt) => prompt.id && prompt.text) ?? [];
  if (configured.length > 0) return configured;
  return hasMcpSurface
    ? [{ id: "generic-mcp", text: genericMcpPrompt }]
    : [];
}
