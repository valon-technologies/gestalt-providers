import { describe, expect, test } from "vitest";
import { RecipeEmphasis } from "./recipe-emphasis";

describe("RecipeEmphasis", () => {
  test("leaves copy without markers unchanged", () => {
    expect(RecipeEmphasis({ text: "Paste this MCP server URL." })).toBe(
      "Paste this MCP server URL.",
    );
  });

  test("wraps paired markers in strong", () => {
    const nodes = RecipeEmphasis({
      text: "Set type to **Streamable HTTP**, not STDIO.",
    });
    expect(Array.isArray(nodes)).toBe(true);
    const [before, strong, after] = nodes as unknown[];
    expect(before).toBe("Set type to ");
    expect(after).toBe(", not STDIO.");
    expect(strong).toMatchObject({
      type: "strong",
      props: { children: "Streamable HTTP" },
    });
  });
});
