// @vitest-environment happy-dom
import { afterEach, describe, expect, it, vi } from "vitest";
import { isElementStuck } from "./workflow-runs-sticky-stuck";

describe("isElementStuck", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("is false while the node still sits below its sticky offset", () => {
    const el = document.createElement("div");
    vi.spyOn(window, "getComputedStyle").mockReturnValue({
      top: "114px",
    } as CSSStyleDeclaration);
    vi.spyOn(el, "getBoundingClientRect").mockReturnValue({
      top: 200,
    } as DOMRect);

    expect(isElementStuck(el)).toBe(false);
  });

  it("is true once the node has docked at its sticky offset", () => {
    const el = document.createElement("div");
    vi.spyOn(window, "getComputedStyle").mockReturnValue({
      top: "114px",
    } as CSSStyleDeclaration);
    vi.spyOn(el, "getBoundingClientRect").mockReturnValue({
      top: 114,
    } as DOMRect);

    expect(isElementStuck(el)).toBe(true);
  });

  it("is false when computed top is not a length", () => {
    const el = document.createElement("div");
    vi.spyOn(window, "getComputedStyle").mockReturnValue({
      top: "auto",
    } as CSSStyleDeclaration);
    vi.spyOn(el, "getBoundingClientRect").mockReturnValue({
      top: 0,
    } as DOMRect);

    expect(isElementStuck(el)).toBe(false);
  });
});
