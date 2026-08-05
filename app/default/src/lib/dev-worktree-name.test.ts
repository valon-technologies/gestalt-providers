import { describe, expect, it } from "vitest";
import { readDevWorktreeName } from "./dev-worktree-name";

describe("readDevWorktreeName", () => {
  it("returns the trimmed Vite name only in Vite DEV", () => {
    expect(
      readDevWorktreeName(
        {
          DEV: true,
          VITE_GESTALT_WORKTREE_NAME: "  debug-banner  ",
        },
        null,
      ),
    ).toBe("debug-banner");
    expect(
      readDevWorktreeName(
        {
          DEV: false,
          VITE_GESTALT_WORKTREE_NAME: "debug-banner",
        },
        null,
      ),
    ).toBeNull();
    expect(
      readDevWorktreeName(
        {
          DEV: true,
          VITE_GESTALT_WORKTREE_NAME: "   ",
        },
        null,
      ),
    ).toBeNull();
    expect(readDevWorktreeName({ DEV: true }, null)).toBeNull();
  });

  it("accepts a runtime adapter name outside Vite DEV", () => {
    expect(
      readDevWorktreeName(
        { DEV: false, VITE_GESTALT_WORKTREE_NAME: "ignored-prod-bake" },
        "  workflows  ",
      ),
    ).toBe("workflows");
    expect(readDevWorktreeName({ DEV: false }, "   ")).toBeNull();
    expect(readDevWorktreeName({ DEV: false }, null)).toBeNull();
  });

  it("prefers the Vite DEV name when both channels are set", () => {
    expect(
      readDevWorktreeName(
        { DEV: true, VITE_GESTALT_WORKTREE_NAME: "vite-name" },
        "runtime-name",
      ),
    ).toBe("vite-name");
  });
});
