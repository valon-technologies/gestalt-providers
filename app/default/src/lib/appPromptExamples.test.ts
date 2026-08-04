import { describe, expect, it } from "vitest";
import { getAppPromptExamples } from "./appPromptExamples";

describe("getAppPromptExamples", () => {
  it("returns every configured prompt in order", () => {
    expect(
      getAppPromptExamples(
        {
          prompts: [
            { id: "one", text: "  Draft a reply  " },
            { id: "two", text: "Find unread threads" },
          ],
        },
        false,
      ),
    ).toEqual([
      { id: "one", text: "Draft a reply" },
      { id: "two", text: "Find unread threads" },
    ]);
  });

  it("skips blank projected prompts", () => {
    expect(
      getAppPromptExamples(
        {
          prompts: [
            { id: "blank", text: "   " },
            { id: "useful", text: "  Show my open work  " },
          ],
        },
        false,
      ),
    ).toEqual([{ id: "useful", text: "Show my open work" }]);
  });

  it("uses a generic affordance only for MCP apps without configured prompts", () => {
    expect(getAppPromptExamples({ prompts: [] }, true)).toEqual([
      {
        id: "generic-mcp",
        text: "What can you help me with in this workspace?",
      },
    ]);
    expect(getAppPromptExamples({ prompts: [] }, false)).toEqual([]);
  });

  it("prefers configured prompts over the MCP fallback", () => {
    expect(
      getAppPromptExamples(
        { prompts: [{ id: "summarize-inbox", text: "  Summarize my inbox  " }] },
        true,
      ),
    ).toEqual([{ id: "summarize-inbox", text: "Summarize my inbox" }]);
  });
});
