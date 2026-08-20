import { describe, expect, test } from "vitest";
import {
  ASSISTANT_OVERLAP_CODEX,
  ASSISTANT_OVERLAP_SHORT,
  CHATGPT_INSTALL_DEMO_LABEL,
  CHATGPT_INSTALL_NAME_TYPE,
  CHATGPT_INSTALL_OPEN,
  CHATGPT_INSTALL_PREAMBLE,
  CHATGPT_INSTALL_SAVE,
  CHATGPT_INSTALL_TOKEN,
  CHATGPT_INSTALL_URL,
  SETUP_RESUME_BANNER_BODY,
  TOKEN_STEP_DESCRIPTION,
  assistantOverlapBody,
} from "./assistantConnectionCopy";

describe("setup operator copy", () => {
  test("token step says Setup fills the install commands", () => {
    expect(TOKEN_STEP_DESCRIPTION).toBe(
      "Your assistant uses this token to reach Gestalt. Setup fills it into the install commands for you.",
    );
    expect(TOKEN_STEP_DESCRIPTION).not.toMatch(/Add Gestalt fills/);
  });

  test("resume banner is stage-agnostic", () => {
    expect(SETUP_RESUME_BANNER_BODY).toBe("Pick up where you left off.");
    expect(SETUP_RESUME_BANNER_BODY).not.toMatch(/assistant|install/i);
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
});

describe("assistantOverlapBody", () => {
  test("names Codex native plugins only when Codex is the selected assistant", () => {
    expect(assistantOverlapBody("codex")).toBe(ASSISTANT_OVERLAP_CODEX);
    expect(assistantOverlapBody("codex")).toMatch(/Codex native plugins/);
  });

  test("keeps generic overlap copy for other assistants", () => {
    expect(assistantOverlapBody("cursor")).toBe(ASSISTANT_OVERLAP_SHORT);
    expect(assistantOverlapBody("claude-code")).not.toMatch(/Codex/);
    expect(assistantOverlapBody("other")).not.toMatch(/Codex/);
  });
});
