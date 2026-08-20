import { FINISH_SETUP_LABEL } from "./setupJourneyCopy";

/** Canonical copy for Gestalt MCP vs native plugins vs assistant skills. */

export const MCP_DOCS_TITLE = "MCP setup";

export const WELCOME_ASSISTANT_EXAMPLES =
  "Ask in plain English in ChatGPT, Claude Code, Cursor, or Codex";

export const ASSISTANT_PICKER_DESCRIPTION =
  "Pick the assistant you already use. You can add more later.";

export const TOKEN_STEP_DESCRIPTION =
  "Your assistant uses this token to reach Gestalt. Setup fills it into the install commands for you.";

export const SETUP_RESUME_BANNER_TITLE = FINISH_SETUP_LABEL;

export const SETUP_RESUME_BANNER_BODY = "Pick up where you left off.";

export const CLAUDE_CONNECTOR_SETTINGS_HREF =
  "https://claude.ai/settings/connectors";

export const CLAUDE_BLOCKED_TITLE = "Custom MCP connectors are disabled";

export const CLAUDE_BLOCKED_BODY =
  "Your administrator disabled custom MCP connectors in Claude on Enterprise. Gestalt cannot change that. Claude Code is the closest option that works with Gestalt MCP.";

export const CLAUDE_BLOCKED_CHOOSE_CLAUDE_CODE = "Choose Claude Code";

export const CLAUDE_BLOCKED_NEXT_TITLE =
  "Claude cannot finish setup here. Pick another assistant.";

export const CHATGPT_INSTALL_DEMO_LABEL =
  "Watch how to add Gestalt as a custom MCP in ChatGPT";

export const CHATGPT_INSTALL_PREAMBLE =
  "Follow the video in the ChatGPT app. Paste the values below when ChatGPT asks.";

export const CHATGPT_INSTALL_OPEN =
  "In ChatGPT, open **Settings**, then **Plugins**. Choose **Add**, then **Add MCP server**.";

export const CHATGPT_INSTALL_NAME_TYPE =
  "Name the server **Gestalt**. Set type to **Streamable HTTP**, not STDIO.";

export const CHATGPT_INSTALL_URL = "Paste this MCP server URL.";

export const CHATGPT_INSTALL_TOKEN =
  "Paste this token in **Bearer token env var** (the secret, not a variable name).";

export const CHATGPT_INSTALL_SAVE = "Choose **Save**.";

export const CLAUDE_INSTALL_OPEN =
  "Open Claude on the web or in the Claude desktop app.";

export const CLAUDE_INSTALL_OPEN_CONNECTORS =
  "Go to Customize, then Connectors. On a Team or Enterprise plan, an owner adds the connector first under Organization settings, then Connectors.";

export const CLAUDE_INSTALL_ADD_CONNECTOR =
  "Choose Add custom connector. Name it Gestalt and paste this URL.";

export const CLAUDE_INSTALL_REQUEST_HEADER =
  "Open Request headers. Add a header named Authorization. Paste this value exactly, including the word Bearer and the space.";

export const CLAUDE_INSTALL_ENABLE =
  "Save the connector. In a new chat, open +, then Connectors, and turn Gestalt on.";

export const CLAUDE_INSTALL_HEADERS_NOTE =
  "If you do not see Request headers, that setting may still be rolling out. Go back and choose Claude Code instead.";

export const ASSISTANT_OVERLAP_TITLE = "One path per app";

export const ASSISTANT_OVERLAP_SHORT =
  "Use Gestalt MCP for workspace apps your company linked here. Skip other connectors to the same app (for example Notion). Turn those on only when you need something Gestalt does not expose.";

export const ASSISTANT_OVERLAP_CODEX =
  "Use Gestalt MCP for workspace apps your company linked here. Skip Codex native plugins and assistant skills that link to the same app (for example Notion). Turn those on only when you need something Gestalt does not expose.";

export const ASSISTANT_OVERLAP_CHATGPT =
  "Use Gestalt MCP for workspace apps your company linked here. Skip other ChatGPT plugins and MCP servers for the same app (for example Notion). Turn those on only when you need something Gestalt does not expose.";

export const SETUP_ANOTHER_ASSISTANT_LABEL = "Set up another assistant";

export const CODEX_INSTALL_PREAMBLE =
  "Codex reads MCP servers from your local Codex config. Paste the commands below into Terminal (not the Codex chat). They save your API token in the shell session, then register this workspace as an MCP server named gestalt.";

export const CODEX_INSTALL_POSTAMBLE =
  "Restart Codex if Gestalt tools do not show up.";

export const CURSOR_AGENT_INSTALL_PREAMBLE =
  "Paste this config into .cursor/mcp.json in your project.";

export const MCP_SETUP_DOCS_LINK_LABEL = "MCP Clients";

export const SETUP_TOKEN_CREATE_ITEM_TITLE = "Create a token";

export const SETUP_TOKEN_CREATED_ITEM_TITLE = "Token created";

export const SETUP_TOKEN_CREATED_LEAD = "Token";

export const SETUP_TOKEN_CREATED_TAIL = "created";

/** Completed timeline title: "Token {name} created". */
export function setupTokenCreatedItemTitle(tokenName: string): string {
  const name = tokenName.trim();
  return name
    ? `${SETUP_TOKEN_CREATED_LEAD} ${name} ${SETUP_TOKEN_CREATED_TAIL}`
    : SETUP_TOKEN_CREATED_ITEM_TITLE;
}

export const SETUP_TOKEN_CREATE_DIFFERENT = "Create a different token";

export const SETUP_TOKEN_NEXT_DISABLED_TITLE = "Create a token before continuing";

/** Codex and ChatGPT name their own plugins. Other assistants keep the generic body. */
export function assistantOverlapBody(agentId: string): string {
  if (agentId === "codex") return ASSISTANT_OVERLAP_CODEX;
  if (agentId === "chatgpt") return ASSISTANT_OVERLAP_CHATGPT;
  return ASSISTANT_OVERLAP_SHORT;
}
