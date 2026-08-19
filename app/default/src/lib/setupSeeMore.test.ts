import { describe, expect, test } from "vitest";
import type { Integration } from "@/lib/api";
import {
  SETUP_APPS_GRID_CLASS,
  setupSeeMoreLabel,
  setupSeeMorePreview,
} from "./setupSeeMore";

function stub(name: string, displayName: string): Integration {
  return {
    name,
    displayName,
    description: displayName,
  };
}

describe("SETUP_APPS_GRID_CLASS", () => {
  test("keeps connect cards on two columns so names stay readable", () => {
    expect(SETUP_APPS_GRID_CLASS).toContain("sm:grid-cols-2");
    expect(SETUP_APPS_GRID_CLASS).toContain("items-stretch");
    expect(SETUP_APPS_GRID_CLASS).not.toContain("grid-cols-3");
    expect(SETUP_APPS_GRID_CLASS).not.toContain("grid-cols-4");
  });
});

describe("setupSeeMoreLabel", () => {
  test("names one remaining app", () => {
    expect(setupSeeMoreLabel(["Gmail"])).toBe("See Gmail");
  });

  test("joins two remaining apps", () => {
    expect(setupSeeMoreLabel(["Gmail", "Slack"])).toBe("See Gmail and Slack");
  });

  test("names two and more when three or more remain", () => {
    expect(setupSeeMoreLabel(["Gmail", "Slack", "Notion"])).toBe(
      "See Gmail, Slack, and more",
    );
  });

  test("falls back when the remaining list is empty", () => {
    expect(setupSeeMoreLabel([])).toBe("See more");
  });
});

describe("setupSeeMorePreview", () => {
  test("shows three marks and ChatGPT-style copy", () => {
    const remaining = [
      stub("gmail", "Gmail"),
      stub("slack", "Slack"),
      stub("notion", "Notion"),
      stub("jira", "Jira"),
    ];
    const preview = setupSeeMorePreview(remaining);
    expect(preview.icons.map((app) => app.name)).toEqual([
      "gmail",
      "slack",
      "notion",
    ]);
    expect(preview.label).toBe("See Gmail, Slack, and more");
  });
});
