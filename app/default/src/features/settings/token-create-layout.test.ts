import { describe, expect, test } from "vitest";
import { SETTINGS_TOKEN_CREATE_TRACK } from "./token-create-layout";

describe("settings token create layout", () => {
  test("caps name and expiration at half the form, picker and actions span the form", () => {
    expect(SETTINGS_TOKEN_CREATE_TRACK.form).toContain("@container");
    expect(SETTINGS_TOKEN_CREATE_TRACK.controls).toContain("max-w-[50cqi]");
    expect(SETTINGS_TOKEN_CREATE_TRACK.appAccessPanel).toBe("w-full min-w-0");
    expect(SETTINGS_TOKEN_CREATE_TRACK.actions).toBe("w-full");
    expect(SETTINGS_TOKEN_CREATE_TRACK.form).not.toContain("calc(2/8");
    expect(SETTINGS_TOKEN_CREATE_TRACK.controls).not.toContain("calc(2/8");
  });
});
