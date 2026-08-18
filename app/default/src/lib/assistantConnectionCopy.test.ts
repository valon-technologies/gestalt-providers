import { describe, expect, test } from "vitest";
import {
  ASSISTANT_OVERLAP_CODEX,
  ASSISTANT_OVERLAP_SHORT,
  assistantOverlapBody,
} from "./assistantConnectionCopy";

describe("assistantOverlapBody", () => {
  test("names Codex native plugins only when Codex is the selected assistant", () => {
    expect(assistantOverlapBody("codex")).toBe(ASSISTANT_OVERLAP_CODEX);
    expect(assistantOverlapBody("codex")).toMatch(/Codex native plugins/);
  });

  test("keeps generic overlap copy for other assistants", () => {
    expect(assistantOverlapBody("cursor")).toBe(ASSISTANT_OVERLAP_SHORT);
    expect(assistantOverlapBody("claude-code")).not.toMatch(/Codex/);
    expect(assistantOverlapBody("other")).not.toMatch(/Codex/);
  });
});
