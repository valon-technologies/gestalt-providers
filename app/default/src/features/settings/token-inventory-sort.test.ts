import { describe, expect, it } from "vitest";
import type { APIToken } from "@/lib/api";
import {
  splitCollapsedTokenScopes,
  tokenCreatedAtMs,
  tokenExpiresAtMs,
  tokenNameSortKey,
  tokenScopeEntries,
  tokenScopesSortKey,
} from "./token-inventory-sort";

function token(partial: Partial<APIToken> & Pick<APIToken, "id" | "createdAt">): APIToken {
  return partial;
}

describe("token inventory sort keys", () => {
  it("sorts unnamed tokens with the visible No name label", () => {
    expect(tokenNameSortKey(token({ id: "a", createdAt: "2026-01-01T00:00:00Z" }))).toBe(
      "No name",
    );
    expect(
      tokenNameSortKey(
        token({ id: "a", createdAt: "2026-01-01T00:00:00Z", name: "  " }),
      ),
    ).toBe("No name");
    expect(
      tokenNameSortKey(
        token({ id: "a", createdAt: "2026-01-01T00:00:00Z", name: "CI" }),
      ),
    ).toBe("CI");
  });

  it("orders created timestamps newest first when compared descending", () => {
    const older = token({ id: "grant-z", createdAt: "2026-01-15T10:00:00Z" });
    const newer = token({ id: "grant-a", createdAt: "2026-02-20T14:30:00Z" });
    const ordered = [older, newer].sort(
      (a, b) => tokenCreatedAtMs(b) - tokenCreatedAtMs(a),
    );
    expect(ordered.map((row) => row.id)).toEqual(["grant-a", "grant-z"]);
  });

  it("sorts never-expiring tokens after dated expiries", () => {
    const dated = token({
      id: "a",
      createdAt: "2026-01-01T00:00:00Z",
      expiresAt: "2027-01-01T00:00:00Z",
    });
    const never = token({ id: "b", createdAt: "2026-01-01T00:00:00Z" });
    expect(tokenExpiresAtMs(dated)).toBeLessThan(tokenExpiresAtMs(never));
    expect(tokenExpiresAtMs(never)).toBe(Number.POSITIVE_INFINITY);
  });

  it("sorts unscoped tokens with the visible all label", () => {
    expect(
      tokenScopesSortKey(token({ id: "a", createdAt: "2026-01-01T00:00:00Z" })),
    ).toBe("all");
    expect(
      tokenScopesSortKey(
        token({
          id: "a",
          createdAt: "2026-01-01T00:00:00Z",
          scopeDetails: [{ scope: "slack", resources: ["workspace"] }],
        }),
      ),
    ).toBe("slack (workspace)");
  });
});

describe("collapsed token scopes", () => {
  it("keeps short lists fully visible", () => {
    const entries = tokenScopeEntries({
      scopes: ["g-issues:read", "g-issues:write", "slack", "github"],
    });
    expect(splitCollapsedTokenScopes(entries).rest).toEqual([]);
    expect(splitCollapsedTokenScopes(entries).preview).toHaveLength(4);
  });

  it("previews the first scopes and counts the rest when the list is long", () => {
    const entries = tokenScopeEntries({
      scopes: [
        "g-issues:attachments.create",
        "g-issues:contentRevisions.list",
        "g-issues:customers.delete",
        "g-issues:issues.list",
        "g-issues:issues.update",
      ],
    });
    const split = splitCollapsedTokenScopes(entries);
    expect(split.preview.map((entry) => entry.scope)).toEqual([
      "g-issues:attachments.create",
      "g-issues:contentRevisions.list",
      "g-issues:customers.delete",
    ]);
    expect(split.rest).toHaveLength(2);
  });
});
