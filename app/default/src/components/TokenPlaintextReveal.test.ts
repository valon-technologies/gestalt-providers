// @vitest-environment happy-dom
import { afterEach, beforeAll, describe, expect, test } from "vitest";
import { act, createElement } from "react";
import { createRoot, type Root } from "react-dom/client";

import { TokenPlaintextReveal } from "./TokenPlaintextReveal";

describe("TokenPlaintextReveal", () => {
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

  test("renders a named copy-only chip, not a text field or live alert", () => {
    node = document.createElement("div");
    document.body.append(node);
    root = createRoot(node);

    act(() => {
      root!.render(
        createElement(TokenPlaintextReveal, {
          plaintext: "gst_test_token",
          description:
            "This token is ready. Copy it now — it will not be shown again.",
        }),
      );
    });

    const group = node.querySelector('[role="group"][aria-label="API token"]');
    expect(group).not.toBeNull();
    expect(group?.querySelector('[data-slot="copyable-code"]')).not.toBeNull();
    expect(node.querySelector('[role="textbox"]')).toBeNull();
    expect(node.querySelector("input")).toBeNull();
    expect(node.querySelector('[role="alert"]')).toBeNull();
    expect(node.textContent).toContain("gst_test_token");
  });
});
