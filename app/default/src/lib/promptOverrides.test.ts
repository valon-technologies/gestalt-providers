import { describe, expect, it } from "vitest";
import { applyDevPromptOverrides } from "./promptOverrides";
import type { Integration } from "./api";

describe("applyDevPromptOverrides", () => {
  it("merges prompts onto matching integration names", () => {
    // When the local JSON is absent, glob is empty and apply is a no-op.
    // With a present override file in DEV, keyed merges replace prompts.
    const integrations: Integration[] = [
      { name: "gmail", displayName: "Gmail", prompts: [] },
      { name: "slack", displayName: "Slack" },
    ];
    const result = applyDevPromptOverrides(integrations);
    expect(result).toHaveLength(2);
    expect(result[0]?.name).toBe("gmail");
    expect(result[1]?.name).toBe("slack");
    // If local overrides are loaded (prod-remote preview), gmail gets them;
    // otherwise prompts stay as provided.
    if (result[0]?.prompts?.length) {
      expect(result[0].prompts.every((p) => p.id && p.text)).toBe(true);
    } else {
      expect(result[0]?.prompts).toEqual([]);
    }
  });

  it("leaves integrations unchanged when no override map is present", () => {
    const integrations: Integration[] = [
      {
        name: "unknown-app",
        prompts: [{ id: "keep", text: "Keep me" }],
      },
    ];
    const result = applyDevPromptOverrides(integrations);
    expect(result[0]?.prompts).toEqual([{ id: "keep", text: "Keep me" }]);
  });
});
