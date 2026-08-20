import { describe, expect, it } from "vitest";
import {
  SETTINGS_TOKEN_CREATE_CONTINUE,
  SETTINGS_TOKEN_CREATE_DONE,
  SETTINGS_TOKEN_CREATE_TITLE,
  SETTINGS_TOKEN_CREATED_DESCRIPTION,
  SETTINGS_TOKEN_PLAINTEXT_DESCRIPTION,
  SETTINGS_TOKENS_CREATE_CTA,
  SETTINGS_TOKENS_EMPTY_TITLE,
  SETTINGS_TOKENS_LIST_TITLE,
  SETTINGS_TOKENS_SCOPES_SHOW_LESS,
  SETTINGS_TOKENS_UNNAMED_LABEL,
  settingsTokensScopesMoreLabel,
} from "./tokens-copy";

describe("settings tokens copy", () => {
  it("keeps Settings terminology on Create token (not New/Add)", () => {
    expect(SETTINGS_TOKENS_CREATE_CTA).toBe("Create token");
    expect(SETTINGS_TOKEN_CREATE_TITLE).toBe("Create token");
    expect(SETTINGS_TOKENS_LIST_TITLE).toBe("API tokens");
    expect(SETTINGS_TOKENS_EMPTY_TITLE).toBe("No API tokens yet.");
    expect(SETTINGS_TOKENS_UNNAMED_LABEL).toBe("No name");
    expect(settingsTokensScopesMoreLabel(1)).toBe("Show 1 more scope");
    expect(settingsTokensScopesMoreLabel(42)).toBe("Show 42 more scopes");
    expect(SETTINGS_TOKENS_SCOPES_SHOW_LESS).toBe("Show less");
  });

  it("uses settings-native one-time secret copy (not Build tutorial framing)", () => {
    expect(SETTINGS_TOKEN_PLAINTEXT_DESCRIPTION).toMatch(/won't show the full value again/i);
    expect(SETTINGS_TOKEN_PLAINTEXT_DESCRIPTION).not.toMatch(/Open MCP Clients/i);
    expect(SETTINGS_TOKEN_PLAINTEXT_DESCRIPTION).not.toMatch(/this example/i);
    expect(SETTINGS_TOKEN_CREATED_DESCRIPTION).toMatch(/open MCP Clients/i);
    expect(SETTINGS_TOKEN_CREATED_DESCRIPTION).not.toMatch(/Copy the secret/i);
    expect(SETTINGS_TOKEN_CREATE_CONTINUE).toBe("Open MCP Clients");
    expect(SETTINGS_TOKEN_CREATE_DONE).toBe("Back to tokens");
  });
});
