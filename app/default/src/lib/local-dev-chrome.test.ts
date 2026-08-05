import { describe, expect, it } from "vitest";
import { isLocalDevChrome } from "./local-dev-chrome";

describe("isLocalDevChrome", () => {
  it("is true in Vite DEV regardless of runtime flag", () => {
    expect(isLocalDevChrome({ DEV: true }, false)).toBe(true);
    expect(isLocalDevChrome({ DEV: true }, null)).toBe(true);
  });

  it("is true when the local-dev adapter injects the runtime flag", () => {
    expect(isLocalDevChrome({ DEV: false }, true)).toBe(true);
  });

  it("is false for production builds without the runtime flag", () => {
    expect(isLocalDevChrome({ DEV: false }, false)).toBe(false);
    expect(isLocalDevChrome({ DEV: false }, null)).toBe(false);
  });
});
