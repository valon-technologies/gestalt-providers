import { describe, expect, test } from "vitest";

import { codeBlockLowlight } from "@/components/ui/code-block-lowlight";

describe("CodeBlock lowlight grammars", () => {
  test("registers the curated display set plus cli", () => {
    for (const lang of [
      "bash",
      "javascript",
      "json",
      "typescript",
      "yaml",
      "cli",
    ]) {
      expect(codeBlockLowlight.registered(lang)).toBe(true);
    }
  });

  test("does not register the full highlight.js catalog", () => {
    expect(codeBlockLowlight.registered("python")).toBe(false);
    expect(codeBlockLowlight.registered("go")).toBe(false);
  });
});
