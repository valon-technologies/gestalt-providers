import { describe, expect, it } from "bun:test";
import {
  SETTINGS_TOKEN_CREATE_TITLE,
  SETTINGS_TOKEN_PLAINTEXT_DESCRIPTION,
  SETTINGS_TOKENS_CREATE_CTA,
  SETTINGS_TOKENS_EMPTY_TITLE,
  SETTINGS_TOKENS_LIST_TITLE,
} from "./tokens-copy";

describe("settings tokens copy", () => {
  it("keeps Settings terminology on Create token (not New/Add)", () => {
    expect(SETTINGS_TOKENS_CREATE_CTA).toBe("Create token");
    expect(SETTINGS_TOKEN_CREATE_TITLE).toBe("Create token");
    expect(SETTINGS_TOKENS_LIST_TITLE).toBe("Your API tokens");
    expect(SETTINGS_TOKENS_EMPTY_TITLE).toBe("No API tokens yet.");
  });

  it("uses settings-native one-time secret copy (not Build tutorial framing)", () => {
    expect(SETTINGS_TOKEN_PLAINTEXT_DESCRIPTION).toMatch(/won't show the full value again/i);
    expect(SETTINGS_TOKEN_PLAINTEXT_DESCRIPTION).not.toMatch(/this example/i);
  });
});
