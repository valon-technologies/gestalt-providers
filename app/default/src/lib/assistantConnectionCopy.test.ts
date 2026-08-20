import { describe, expect, test } from "vitest";
import {
  ASSISTANT_OVERLAP_CHATGPT,
  ASSISTANT_OVERLAP_CODEX,
  ASSISTANT_OVERLAP_SHORT,
  CHATGPT_INSTALL_DEMO_LABEL,
  CHATGPT_INSTALL_NAME_TYPE,
  CHATGPT_INSTALL_OPEN,
  CHATGPT_INSTALL_PREAMBLE,
  CHATGPT_INSTALL_SAVE,
  CHATGPT_INSTALL_TOKEN,
  CHATGPT_INSTALL_URL,
  CLAUDE_BLOCKED_BODY,
  CLAUDE_BLOCKED_CHOOSE_CLAUDE_CODE,
  CLAUDE_BLOCKED_NEXT_TITLE,
  CLAUDE_BLOCKED_TITLE,
  CURSOR_AGENT_INSTALL_PREAMBLE,
  SETUP_RESUME_BANNER_BODY,
  SETUP_TOKEN_CREATED_ITEM_TITLE,
  TOKEN_STEP_DESCRIPTION,
  WELCOME_ASSISTANT_EXAMPLES,
  assistantOverlapBody,
  setupTokenCreatedItemTitle,
} from "./assistantConnectionCopy";

describe("setup operator copy", () => {
  test("token step says Setup fills the install commands", () => {
    expect(TOKEN_STEP_DESCRIPTION).toBe(
      "Your assistant uses this token to reach Gestalt. Setup fills it into the install commands for you.",
    );
    expect(TOKEN_STEP_DESCRIPTION).not.toMatch(/Add Gestalt fills/);
  });

  test("completed token step names the grant this setup is using", () => {
    expect(setupTokenCreatedItemTitle("ci-pipeline")).toBe(
      "Token ci-pipeline created",
    );
    expect(setupTokenCreatedItemTitle("  Workspace  ")).toBe(
      "Token Workspace created",
    );
    expect(setupTokenCreatedItemTitle("")).toBe(
      SETUP_TOKEN_CREATED_ITEM_TITLE,
    );
    expect(setupTokenCreatedItemTitle("ci-pipeline")).not.toMatch(
      /is ready\. Continue to add Gestalt/,
    );
  });

  test("resume banner is stage-agnostic", () => {
    expect(SETUP_RESUME_BANNER_BODY).toBe("Pick up where you left off.");
    expect(SETUP_RESUME_BANNER_BODY).not.toMatch(/assistant|install/i);
  });

  test("welcome examples name ChatGPT with the other offered assistants", () => {
    expect(WELCOME_ASSISTANT_EXAMPLES).toBe(
      "Ask in plain English in ChatGPT, Claude Code, Cursor, or Codex",
    );
  });

  test("Claude blocked notice names Enterprise admin and Claude Code", () => {
    expect(CLAUDE_BLOCKED_TITLE).toBe("Custom MCP connectors are disabled");
    expect(CLAUDE_BLOCKED_BODY).toMatch(/administrator/);
    expect(CLAUDE_BLOCKED_BODY).toMatch(/Enterprise/);
    expect(CLAUDE_BLOCKED_BODY).toMatch(/Claude Code/);
    expect(CLAUDE_BLOCKED_CHOOSE_CLAUDE_CODE).toBe("Choose Claude Code");
    expect(CLAUDE_BLOCKED_NEXT_TITLE).toBe(
      "Claude cannot finish setup here. Pick another assistant.",
    );
  });

  test("ChatGPT install names the ChatGPT app, Streamable HTTP, and token paste", () => {
    expect(CHATGPT_INSTALL_DEMO_LABEL).toMatch(/custom MCP/);
    expect(CHATGPT_INSTALL_PREAMBLE).toMatch(/ChatGPT app/);
    expect(CHATGPT_INSTALL_OPEN).toBe(
      "In ChatGPT, open **Settings**, then **Plugins**. Choose **Add**, then **Add MCP server**.",
    );
    expect(CHATGPT_INSTALL_NAME_TYPE).toBe(
      "Name the server **Gestalt**. Set type to **Streamable HTTP**, not STDIO.",
    );
    expect(CHATGPT_INSTALL_URL).toMatch(/MCP server URL/);
    expect(CHATGPT_INSTALL_TOKEN).toBe(
      "Paste this token in **Bearer token env var** (the secret, not a variable name).",
    );
    expect(CHATGPT_INSTALL_SAVE).toBe("Choose **Save**.");
    expect(CHATGPT_INSTALL_OPEN.split("**").length % 2).toBe(1);
    expect(CHATGPT_INSTALL_NAME_TYPE.split("**").length % 2).toBe(1);
  });

  test("Cursor Agent install tells people to paste the project config", () => {
    expect(CURSOR_AGENT_INSTALL_PREAMBLE).toBe(
      "Paste this config into .cursor/mcp.json in your project.",
    );
    expect(CURSOR_AGENT_INSTALL_PREAMBLE).not.toMatch(/start Agent/i);
  });
});

describe("assistantOverlapBody", () => {
  test("names Codex native plugins only when Codex is the selected assistant", () => {
    expect(assistantOverlapBody("codex")).toBe(ASSISTANT_OVERLAP_CODEX);
    expect(assistantOverlapBody("codex")).toMatch(/Codex native plugins/);
  });

  test("names ChatGPT plugins when ChatGPT is the selected assistant", () => {
    expect(assistantOverlapBody("chatgpt")).toBe(ASSISTANT_OVERLAP_CHATGPT);
    expect(assistantOverlapBody("chatgpt")).toMatch(/ChatGPT plugins/);
  });

  test("keeps generic overlap copy for other assistants", () => {
    expect(assistantOverlapBody("cursor")).toBe(ASSISTANT_OVERLAP_SHORT);
    expect(assistantOverlapBody("claude-code")).not.toMatch(/Codex/);
    expect(assistantOverlapBody("other")).not.toMatch(/Codex/);
  });
});
