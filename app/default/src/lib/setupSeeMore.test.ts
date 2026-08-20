import { describe, expect, test } from "vitest";
import type { Integration } from "@/lib/api";
import {
  presentSetupConnectApps,
  SETUP_APPS_CATEGORY_ALL,
  SETUP_APPS_GRID_CLASS,
  SETUP_APPS_PAGE_SIZE,
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

describe("presentSetupConnectApps", () => {
  const more = [
    stub("ashby", "Ashby"),
    stub("figma", "Figma"),
    stub("github", "GitHub"),
    stub("gmail", "Gmail"),
    stub("intercom", "Intercom"),
    stub("jira", "Jira"),
    stub("looker", "Looker"),
    stub("notion", "Notion"),
    stub("zendesk", "Zendesk"),
  ];
  const suggested = [
    { appId: "slack", integration: stub("slack", "Slack") },
    { appId: "linear", integration: stub("linear", "Linear") },
  ];

  test("pages More apps until a query reveals every match", () => {
    const browsing = presentSetupConnectApps({
      suggested,
      more,
      query: "",
      category: SETUP_APPS_CATEGORY_ALL,
      visibleCount: SETUP_APPS_PAGE_SIZE,
      labelFor: (appId) => appId,
    });
    expect(browsing.visibleMore.map((app) => app.name)).toEqual([
      "ashby",
      "figma",
      "github",
      "gmail",
      "intercom",
      "jira",
      "looker",
      "notion",
    ]);
    expect(browsing.remainingMore.map((app) => app.name)).toEqual(["zendesk"]);
    expect(browsing.visibleSuggested.map((item) => item.appId)).toEqual([
      "slack",
      "linear",
    ]);

    const searching = presentSetupConnectApps({
      suggested,
      more,
      query: "Zendesk",
      category: SETUP_APPS_CATEGORY_ALL,
      visibleCount: SETUP_APPS_PAGE_SIZE,
      labelFor: (appId) => appId,
    });
    expect(searching.visibleMore.map((app) => app.name)).toEqual(["zendesk"]);
    expect(searching.remainingMore).toEqual([]);
    expect(searching.visibleSuggested).toEqual([]);
    expect(searching.moreSectionTitle).toBe("More apps");
  });

  test("keeps Suggested hits and hides unrelated More apps", () => {
    const presented = presentSetupConnectApps({
      suggested,
      more,
      query: "slack",
      category: SETUP_APPS_CATEGORY_ALL,
      visibleCount: SETUP_APPS_PAGE_SIZE,
      labelFor: (appId) => appId,
    });
    expect(presented.visibleSuggested.map((item) => item.appId)).toEqual([
      "slack",
    ]);
    expect(presented.filteredMore).toEqual([]);
  });

  test("ANDs search with category and prunes a chip that search emptied", () => {
    const presented = presentSetupConnectApps({
      suggested,
      more,
      query: "notion",
      category: "communication",
      visibleCount: SETUP_APPS_PAGE_SIZE,
      labelFor: (appId) => appId,
    });
    expect(presented.effectiveCategory).toBe(SETUP_APPS_CATEGORY_ALL);
    expect(presented.categoryChips.map((bucket) => bucket.id)).toEqual([
      "productivity",
    ]);
    expect(presented.visibleMore.map((app) => app.name)).toEqual(["notion"]);
    expect(presented.moreSectionTitle).toBe("More apps");
  });

  test("keeps a category chip that still has search hits", () => {
    const presented = presentSetupConnectApps({
      suggested,
      more,
      query: "mail",
      category: "communication",
      visibleCount: SETUP_APPS_PAGE_SIZE,
      labelFor: (appId) => appId,
    });
    expect(presented.effectiveCategory).toBe("communication");
    expect(presented.visibleMore.map((app) => app.name)).toEqual(["gmail"]);
    expect(presented.moreSectionTitle).toBe("Communication");
  });

  test("matches a missing suggested app by its label", () => {
    const presented = presentSetupConnectApps({
      suggested: [
        { appId: "not-installed", integration: undefined },
        { appId: "slack", integration: stub("slack", "Slack") },
      ],
      more,
      query: "Notion",
      category: SETUP_APPS_CATEGORY_ALL,
      visibleCount: SETUP_APPS_PAGE_SIZE,
      labelFor: (appId) =>
        appId === "not-installed" ? "Notion Calendar" : appId,
    });
    expect(presented.visibleSuggested.map((item) => item.appId)).toEqual([
      "not-installed",
    ]);
  });
});
