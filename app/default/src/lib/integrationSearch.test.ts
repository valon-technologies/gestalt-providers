import { describe, expect, test } from "vitest";
import type { Integration } from "@/lib/api";
import {
  catalogCardDescription,
  filterIntegrations,
  getIntegrationLabel,
  integrationMatchesQuery,
} from "./integrationSearch";

function stub(partial: Partial<Integration> & Pick<Integration, "name">): Integration {
  return partial;
}

describe("catalogCardDescription", () => {
  test("returns a distinct description", () => {
    expect(
      catalogCardDescription(
        stub({
          name: "slack",
          displayName: "Slack",
          description:
            "Read public and private conversations, DMs, and group DMs; send messages; and manage channels.",
        }),
      ),
    ).toBe(
      "Read public and private conversations, DMs, and group DMs; send messages; and manage channels.",
    );
  });

  test("omits empty and whitespace-only copy", () => {
    expect(catalogCardDescription(stub({ name: "slack" }))).toBeNull();
    expect(
      catalogCardDescription(stub({ name: "slack", description: "   " })),
    ).toBeNull();
  });

  test("omits a description that only repeats the label", () => {
    expect(
      catalogCardDescription(
        stub({ name: "slack", displayName: "Slack", description: "Slack" }),
      ),
    ).toBeNull();
    expect(
      catalogCardDescription(
        stub({ name: "slack", displayName: "Slack", description: "slack" }),
      ),
    ).toBeNull();
  });
});

describe("getIntegrationLabel", () => {
  test("prefers displayName", () => {
    expect(
      getIntegrationLabel(stub({ name: "slack", displayName: "Slack" })),
    ).toBe("Slack");
  });
});

describe("integrationMatchesQuery", () => {
  test("matches display name, plugin id, or description", () => {
    const notion = stub({
      name: "notion",
      displayName: "Notion",
      description: "Pages, databases, and MCP tools.",
    });
    expect(integrationMatchesQuery(notion, "Noti")).toBe(true);
    expect(integrationMatchesQuery(notion, "notion")).toBe(true);
    expect(integrationMatchesQuery(notion, "databases")).toBe(true);
    expect(integrationMatchesQuery(notion, "slack")).toBe(false);
  });
});

describe("filterIntegrations", () => {
  test("keeps apps that contain every query token", () => {
    const catalog = [
      stub({ name: "notion", displayName: "Notion" }),
      stub({ name: "slack", displayName: "Slack" }),
    ];
    expect(filterIntegrations(catalog, "not").map((app) => app.name)).toEqual([
      "notion",
    ]);
    expect(filterIntegrations(catalog, "").map((app) => app.name)).toEqual([
      "notion",
      "slack",
    ]);
  });
});
