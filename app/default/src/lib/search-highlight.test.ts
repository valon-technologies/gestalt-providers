import { describe, test } from "node:test";
import assert from "node:assert/strict";

import {
  extractSearchSnippet,
  searchTokensMissingFromText,
  splitSearchHighlightParts,
  textContainsAllSearchTokens,
} from "./search-highlight.ts";

describe("search-highlight", () => {
  test("splits highlight parts for token-AND queries", () => {
    assert.deepEqual(splitSearchHighlightParts("Hello World", "wor"), [
      { text: "Hello ", highlight: false },
      { text: "Wor", highlight: true },
      { text: "ld", highlight: false },
    ]);
  });

  test("matches accent-insensitively", () => {
    assert.equal(textContainsAllSearchTokens("José García", "jose"), true);
  });

  test("extracts description snippet around first token", () => {
    const snippet = extractSearchSnippet(
      "A long description that mentions deployment failures near the end",
      "deploy",
      8,
    );
    assert.ok(snippet?.includes("deployment"));
  });

  test("highlights ligature display text when query matches a partial expansion", () => {
    assert.deepEqual(splitSearchHighlightParts("ﬁx deployment", "f"), [
      { text: "ﬁ", highlight: true },
      { text: "x deployment", highlight: false },
    ]);
  });

  test("anchors snippet on tokens missing from the primary field", () => {
    const description =
      "Ship workflow plan for the quarterly roadmap. Later we added escrow automation.";
    const missing = searchTokensMissingFromText("Ship the release", "ship escrow");
    assert.deepEqual(missing, ["escrow"]);

    const snippet = extractSearchSnippet(description, "ship escrow", 16, missing);
    assert.ok(snippet?.includes("escrow"));
    assert.ok(!snippet?.includes("roadmap"));
  });
});

describe("catalog list-search parity", () => {
  // integrationSearch.matchesSearchQuery delegates here — keep filter/highlight aligned.
  test("filter tokens highlight on accented display names", () => {
    const label = "Résumé Parser";
    const query = "resume";
    assert.equal(textContainsAllSearchTokens(label, query), true);
    const parts = splitSearchHighlightParts(label, query);
    assert.ok(parts.some((part) => part.highlight));
  });

  test("catalog haystack shapes match token-AND semantics", () => {
    assert.equal(
      textContainsAllSearchTokens(
        "slack_v2 Slack Messaging Connect your workspace",
        "slack messaging",
      ),
      true,
    );
    assert.equal(
      textContainsAllSearchTokens(
        "zendesk Zendesk Support ticketing integration",
        "zendesk support",
      ),
      true,
    );
  });
});
