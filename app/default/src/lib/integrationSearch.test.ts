import { describe, expect, test } from "vitest";
import type { Integration } from "@/lib/api";
import {
  catalogCardDescription,
  getIntegrationLabel,
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
