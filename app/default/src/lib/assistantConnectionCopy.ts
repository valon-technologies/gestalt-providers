/** Canonical copy for Gestalt MCP vs native plugins vs assistant skills. */

export const MCP_DOCS_TITLE = "MCP setup";

export const WELCOME_ASSISTANT_EXAMPLES =
  "Ask in plain English in Claude Code, Cursor, or Codex";

export const ASSISTANT_PICKER_DESCRIPTION =
  "Pick the assistant you already use. You can add more later.";

export const TOKEN_STEP_DESCRIPTION =
  "Your assistant uses this token to reach Gestalt. Add Gestalt fills it into the commands for you.";

export const CLAUDE_CONNECTOR_SETTINGS_HREF =
  "https://claude.ai/settings/connectors";

export const CHATGPT_PLUGINS_HREF = "https://chatgpt.com/plugins";

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

export const CHATGPT_INSTALL_DEVELOPER_MODE =
  "In ChatGPT, open Settings, then Security and login, and turn on Developer mode. This needs a paid plan. A workspace admin may need to allow it.";

export const CHATGPT_INSTALL_CREATE_APP =
  "Open Settings, then Plugins, or go to chatgpt.com/plugins. Create a developer-mode app.";

export const CHATGPT_INSTALL_URL =
  "Name it Gestalt and paste this MCP server URL.";

export const CHATGPT_INSTALL_TOKEN =
  "If ChatGPT asks how to authenticate, choose Token or API key (not OAuth) and copy the token below.";

export const CHATGPT_INSTALL_ENABLE =
  "Start a new chat. In the composer, open +, then Developer mode, and select Gestalt.";

export const CHATGPT_INSTALL_AUTH_NOTE =
  "Gestalt signs requests with this token. If ChatGPT only offers OAuth, go back and choose Claude or another assistant that accepts a token in the Authorization header.";

export const ASSISTANT_OVERLAP_TITLE = "One path per app";

export const ASSISTANT_OVERLAP_SHORT =
  "Use Gestalt MCP for workspace apps your company connected here. Skip other connectors to the same app (for example Notion). Turn those on only when you need something Gestalt does not expose.";

export const ASSISTANT_OVERLAP_CODEX =
  "Use Gestalt MCP for workspace apps your company connected here. Skip Codex native plugins and assistant skills that connect to the same app (for example Notion). Turn those on only when you need something Gestalt does not expose.";

export const CONNECT_ANOTHER_ASSISTANT_LABEL = "Connect another assistant";

export const CODEX_INSTALL_PREAMBLE =
  "Codex reads MCP servers from your local Codex config. Paste the commands below into Terminal (not the Codex chat). They save your API token in the shell session, then register this workspace as an MCP server named gestalt.";

export const CODEX_INSTALL_POSTAMBLE =
  "Restart Codex if Gestalt tools do not show up.";

export const CURSOR_AGENT_INSTALL_PREAMBLE =
  "Cursor Agent reads MCP servers from .cursor/mcp.json in your project. Paste this config, then start Agent from that folder.";

export const MCP_SETUP_DOCS_LINK_LABEL = "MCP setup docs";

export const SETUP_TOKEN_CREATE_ITEM_TITLE = "Create a token";

export const SETUP_TOKEN_CREATED_ITEM_TITLE = "Token created";

export const SETUP_TOKEN_CREATE_DONE = "Your token is saved.";

export const SETUP_TOKEN_CREATE_DIFFERENT = "Create a different token";

export const SETUP_TOKEN_NEXT_DISABLED_TITLE = "Create a token before continuing";

/** Codex names native plugins. Other assistants keep the generic overlap body. */
export function assistantOverlapBody(agentId: string): string {
  return agentId === "codex" ? ASSISTANT_OVERLAP_CODEX : ASSISTANT_OVERLAP_SHORT;
}
