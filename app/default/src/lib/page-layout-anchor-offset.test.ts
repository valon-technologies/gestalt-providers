// @vitest-environment happy-dom
import { afterEach, describe, expect, test } from "vitest";
import { readPageLayoutAnchorOffsetPx } from "./page-layout-anchor-offset";

describe("readPageLayoutAnchorOffsetPx", () => {
  afterEach(() => {
    document.documentElement.style.removeProperty(
      "--page-layout-anchor-offset",
    );
    document.body.replaceChildren();
  });

  test("probes the scoped custom property, not :root", () => {
    document.documentElement.style.setProperty(
      "--page-layout-anchor-offset",
      "10px",
    );
    const scope = document.createElement("div");
    scope.style.setProperty("--page-layout-anchor-offset", "48px");
    document.body.append(scope);

    expect(readPageLayoutAnchorOffsetPx(0, document.documentElement)).toBe(10);
    expect(readPageLayoutAnchorOffsetPx(0, scope)).toBe(48);
  });
});
