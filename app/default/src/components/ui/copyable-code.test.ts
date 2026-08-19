// @vitest-environment happy-dom
import { afterEach, beforeAll, describe, expect, test } from "vitest";
import { act, createElement } from "react";
import { createRoot, type Root } from "react-dom/client";

import { CopyableCode, maskCopyableValue, maskSecretsInText } from "./copyable-code";

describe("maskCopyableValue", () => {
  test("keeps a letter bookend and same-length middle stars", () => {
    const token = "gst_api_created_once_secret";
    const longToken = "dXRxw2bOB0Aw-PJrv0JD7R66t62X8TBG1eZEp0UHh_8";
    expect(maskCopyableValue(token)).toBe(
      `gst_${"*".repeat(token.length - 8)}cret`,
    );
    expect(maskCopyableValue(longToken)).toBe(
      `dXRx${"*".repeat(longToken.length - 8)}Hh_8`,
    );
    expect(maskCopyableValue(token).length).toBe(token.length);
    expect(maskCopyableValue(longToken).length).toBe(longToken.length);
  });

  test("does not leak the middle of the secret", () => {
    const secret = "gst_api_created_once_secret";
    const masked = maskCopyableValue(secret);
    expect(masked.startsWith("gst_")).toBe(true);
    expect(masked.endsWith("cret")).toBe(true);
    expect(masked).not.toContain("api_created_once");
    expect(masked).not.toBe(secret);
  });

  test("keeps empty and short values stable", () => {
    expect(maskCopyableValue("")).toBe("");
    expect(maskCopyableValue("a")).toBe("*");
    expect(maskCopyableValue("ab")).toBe("a*");
    expect(maskCopyableValue("abcd")).toBe("a**d");
  });
});

describe("maskSecretsInText", () => {
  test("masks each secret in place and keeps surrounding text", () => {
    const token = "gst_api_created_once_secret";
    const json = `"Authorization": "Bearer ${token}"`;
    expect(maskSecretsInText(json, [token])).toBe(
      `"Authorization": "Bearer ${maskCopyableValue(token)}"`,
    );
    expect(maskSecretsInText(json, [token])).not.toContain(token);
  });

  test("ignores empty secrets so split does not shred the text", () => {
    expect(maskSecretsInText("Bearer token", [""])).toBe("Bearer token");
  });
});

describe("CopyableCode sensitive", () => {
  let root: Root | null = null;
  let node: HTMLDivElement | null = null;

  beforeAll(() => {
    globalThis.IS_REACT_ACT_ENVIRONMENT = true;
  });

  afterEach(() => {
    act(() => {
      root?.unmount();
    });
    node?.remove();
    root = null;
    node = null;
  });

  function renderSensitive() {
    node = document.createElement("div");
    document.body.append(node);
    root = createRoot(node);
    act(() => {
      root!.render(
        createElement(CopyableCode, {
          value: "gst_api_created_once_secret",
          tooltip: "Copy token",
          sensitive: true,
          revealLabel: "Show token",
          hideLabel: "Hide token",
        }),
      );
    });
  }

  test("masks the visible text and keeps copy off the tooltip", () => {
    renderSensitive();
    const chip = node!.querySelector('[data-slot="copyable-code"]');
    expect(chip?.getAttribute("data-sensitive")).toBe("true");
    expect(chip?.getAttribute("data-revealed")).toBe("false");
    expect(chip?.textContent).toContain(maskCopyableValue("gst_api_created_once_secret"));
    expect(chip?.textContent).not.toContain("gst_api_created_once_secret");
    expect(node!.querySelector('[data-slot="copyable-code-reveal"]')).not.toBeNull();
  });

  test("show then hide toggles the full value in the chip", () => {
    renderSensitive();
    const reveal = node!.querySelector(
      '[data-slot="copyable-code-reveal"]',
    ) as HTMLButtonElement;
    act(() => {
      reveal.click();
    });
    const chip = node!.querySelector('[data-slot="copyable-code"]');
    expect(chip?.getAttribute("data-revealed")).toBe("true");
    expect(chip?.textContent).toContain("gst_api_created_once_secret");
    expect(reveal.getAttribute("aria-label")).toBe("Hide token");
    act(() => {
      reveal.click();
    });
    expect(chip?.getAttribute("data-revealed")).toBe("false");
    expect(chip?.textContent).toContain(maskCopyableValue("gst_api_created_once_secret"));
    expect(chip?.textContent).not.toContain("gst_api_created_once_secret");
  });

  test("masks listed secrets inside a bearer value", () => {
    const token = "gst_api_created_once_secret";
    node = document.createElement("div");
    document.body.append(node);
    root = createRoot(node);
    act(() => {
      root!.render(
        createElement(CopyableCode, {
          value: `Bearer ${token}`,
          tooltip: "Copy Authorization value",
          sensitive: true,
          secrets: [token],
          revealLabel: "Show token",
          hideLabel: "Hide token",
        }),
      );
    });
    const chip = node.querySelector('[data-slot="copyable-code"]');
    expect(chip?.textContent).toContain("Bearer ");
    expect(chip?.textContent).toContain(maskCopyableValue(token));
    expect(chip?.textContent).not.toContain(token);
  });
});
