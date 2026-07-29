import { describe, expect, it } from "vitest";

import {
  extractSearchSnippet,
  searchTokensMissingFromText,
  splitSearchHighlightParts,
  textContainsAllSearchTokens,
} from "./search-highlight";

describe("search-highlight", () => {
  it("splits highlight parts for token-AND queries", () => {
    expect(splitSearchHighlightParts("Hello World", "wor")).toEqual([
      { text: "Hello ", highlight: false },
      { text: "Wor", highlight: true },
      { text: "ld", highlight: false },
    ]);
  });

  it("matches accent-insensitively", () => {
    expect(textContainsAllSearchTokens("José García", "jose")).toBe(true);
  });

  it("extracts description snippet around first token", () => {
    const snippet = extractSearchSnippet(
      "A long description that mentions deployment failures near the end",
      "deploy",
      8,
    );
    expect(snippet).toContain("deployment");
  });

  it("highlights ligature display text when query matches a partial expansion", () => {
    expect(splitSearchHighlightParts("ﬁx deployment", "f")).toEqual([
      { text: "ﬁ", highlight: true },
      { text: "x deployment", highlight: false },
    ]);
  });

  it("anchors snippet on tokens missing from the primary field", () => {
    const description =
      "Ship workflow plan for the quarterly roadmap. Later we added escrow automation.";
    const missing = searchTokensMissingFromText("Ship the release", "ship escrow");
    expect(missing).toEqual(["escrow"]);

    const snippet = extractSearchSnippet(description, "ship escrow", 16, missing);
    expect(snippet).toContain("escrow");
    expect(snippet).not.toContain("roadmap");
  });
});

describe("catalog list-search parity", () => {
  // integrationSearch.matchesSearchQuery delegates here — keep filter/highlight aligned.
  it("filter tokens highlight on accented display names", () => {
    const label = "Résumé Parser";
    const query = "resume";
    expect(textContainsAllSearchTokens(label, query)).toBe(true);
    const parts = splitSearchHighlightParts(label, query);
    expect(parts.some((part) => part.highlight)).toBe(true);
  });

  it("catalog haystack shapes match token-AND semantics", () => {
    expect(
      textContainsAllSearchTokens(
        "slack_v2 Slack Messaging Connect your workspace",
        "slack messaging",
      ),
    ).toBe(true);
    expect(
      textContainsAllSearchTokens(
        "zendesk Zendesk Support ticketing integration",
        "zendesk support",
      ),
    ).toBe(true);
  });
});
