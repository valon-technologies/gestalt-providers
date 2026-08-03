import { describe, expect, it } from "vitest";
import { readDevWorktreeName } from "./dev-worktree-name";

describe("readDevWorktreeName", () => {
  it("returns the trimmed name only in Vite DEV", () => {
    expect(
      readDevWorktreeName({
        DEV: true,
        VITE_GESTALT_WORKTREE_NAME: "  debug-banner  ",
      }),
    ).toBe("debug-banner");
    expect(
      readDevWorktreeName({
        DEV: false,
        VITE_GESTALT_WORKTREE_NAME: "debug-banner",
      }),
    ).toBeNull();
    expect(
      readDevWorktreeName({
        DEV: true,
        VITE_GESTALT_WORKTREE_NAME: "   ",
      }),
    ).toBeNull();
    expect(readDevWorktreeName({ DEV: true })).toBeNull();
  });
});
