// @vitest-environment happy-dom
import { afterEach, beforeAll, describe, expect, test } from "vitest";
import { act, createElement } from "react";
import { createRoot, type Root } from "react-dom/client";

import { SegmentedControl } from "./segmented-control";

describe("SegmentedControl unmatched value", () => {
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

  test("does not mark any radio checked when value is not an option", () => {
    node = document.createElement("div");
    document.body.append(node);
    root = createRoot(node);

    act(() => {
      root!.render(
        createElement(SegmentedControl, {
          label: "Choose your assistant",
          value: "dest-claude",
          onValueChange: () => {},
          options: [
            { value: "dest-claude-code", label: "Claude Code" },
            { value: "dest-chatgpt", label: "ChatGPT" },
          ],
          showLabels: true,
          tooltips: false,
        }),
      );
    });

    const radios = node.querySelectorAll('[role="radio"]');
    expect(radios).toHaveLength(2);
    for (const radio of radios) {
      expect(radio.getAttribute("aria-checked")).toBe("false");
    }
    expect(radios[0]?.getAttribute("tabindex")).toBe("0");
    expect(radios[1]?.getAttribute("tabindex")).toBe("-1");
  });

  test("hides the pill when a matched value becomes unmatched", () => {
    node = document.createElement("div");
    document.body.append(node);
    root = createRoot(node);

    const options = [
      { value: "dest-claude-code", label: "Claude Code" },
      { value: "dest-chatgpt", label: "ChatGPT" },
    ];

    act(() => {
      root!.render(
        createElement(SegmentedControl, {
          label: "Choose your assistant",
          value: "dest-claude-code",
          onValueChange: () => {},
          options,
          showLabels: true,
          tooltips: false,
        }),
      );
    });

    act(() => {
      root!.render(
        createElement(SegmentedControl, {
          label: "Choose your assistant",
          value: "dest-claude",
          onValueChange: () => {},
          options,
          showLabels: true,
          tooltips: false,
        }),
      );
    });

    const radios = node.querySelectorAll('[role="radio"]');
    for (const radio of radios) {
      expect(radio.getAttribute("aria-checked")).toBe("false");
    }
    const pill = node.querySelector('[aria-hidden="true"]');
    expect(pill).not.toBeNull();
    expect((pill as HTMLElement).style.opacity).toBe("0");
    expect((pill as HTMLElement).className).not.toContain("transition-[left,top,width,height]");
  });
});
