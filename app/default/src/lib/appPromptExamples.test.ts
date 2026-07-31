import { describe, expect, it } from "vitest";
import { getAppPromptExample } from "./appPromptExamples";

describe("getAppPromptExample", () => {
  it("prefers the configured prompt even without an MCP surface", () => {
    expect(
      getAppPromptExample(
        { prompts: [{ id: "summarize-inbox", text: "  Summarize my inbox  " }] },
        false,
      ),
    ).toBe("Summarize my inbox");
  });

  it("skips blank projected prompts", () => {
    expect(
      getAppPromptExample(
        {
          prompts: [
            { id: "blank", text: "   " },
            { id: "useful", text: "  Show my open work  " },
          ],
        },
        false,
      ),
    ).toBe("Show my open work");
  });

  it("uses a generic affordance only for MCP apps without configured prompts", () => {
    expect(getAppPromptExample({ prompts: [] }, true)).toBe(
      "What can you help me with in this workspace?",
    );
    expect(getAppPromptExample({ prompts: [] }, false)).toBeUndefined();
  });
});
