import { describe, expect, test } from "vitest";
import {
  SETUP_TOKEN_CREATE_CONTENT_CLASS,
  SETUP_TOKEN_CREATE_TRACK,
} from "./token-create-layout";

describe("setup token create layout", () => {
  test("caps name and expiration at half the form, actions span the form", () => {
    expect(SETUP_TOKEN_CREATE_TRACK.form).toContain("@container");
    expect(SETUP_TOKEN_CREATE_TRACK.controls).toContain("max-w-[50cqi]");
    expect(SETUP_TOKEN_CREATE_TRACK.actions).toBe("w-full");
    expect(SETUP_TOKEN_CREATE_CONTENT_CLASS).toBe("mt-5");
  });
});
