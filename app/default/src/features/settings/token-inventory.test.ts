import { describe, expect, it } from "vitest";
import type { APIToken } from "@/lib/api";
import {
  splitCollapsedTokenScopes,
  tokenCreatedAtMs,
  tokenCreatedLabel,
  tokenDisplayName,
  tokenExpiresAtMs,
  tokenExpiresLabel,
  tokenScopeEntries,
  tokenScopesSortKey,
  tokenStoredName,
} from "./token-inventory";

function token(partial: Partial<APIToken> & Pick<APIToken, "id" | "createdAt">): APIToken {
  return partial;
}

describe("token inventory display", () => {
  it("uses the stored name for display and sort, and No name when none is stored", () => {
    const unnamed = token({ id: "a", createdAt: "2026-01-01T00:00:00Z" });
    const whitespace = token({
      id: "a",
      createdAt: "2026-01-01T00:00:00Z",
      name: "  ",
    });
    const named = token({
      id: "a",
      createdAt: "2026-01-01T00:00:00Z",
      name: "CI",
    });

    expect(tokenStoredName(unnamed)).toBeNull();
    expect(tokenDisplayName(unnamed)).toBe("No name");
    expect(tokenStoredName(whitespace)).toBeNull();
    expect(tokenDisplayName(whitespace)).toBe("No name");
    expect(tokenStoredName(named)).toBe("CI");
    expect(tokenDisplayName(named)).toBe("CI");
  });

  it("orders created timestamps newest first when compared descending", () => {
    const older = token({ id: "grant-z", createdAt: "2026-01-15T10:00:00Z" });
    const newer = token({ id: "grant-a", createdAt: "2026-02-20T14:30:00Z" });
    const ordered = [older, newer].sort(
      (a, b) => tokenCreatedAtMs(b) - tokenCreatedAtMs(a),
    );
    expect(ordered.map((row) => row.id)).toEqual(["grant-a", "grant-z"]);
  });

  it("formats created dates and leaves invalid createdAt blank", () => {
    const dated = token({ id: "a", createdAt: "2026-01-15T10:00:00Z" });
    expect(tokenCreatedLabel(dated)).toBe(
      new Date("2026-01-15T10:00:00Z").toLocaleDateString(),
    );
    expect(
      tokenCreatedLabel(token({ id: "b", createdAt: "not-a-date" })),
    ).toBe("");
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
    expect(
      tokenExpiresLabel(token({ id: "c", createdAt: "2026-01-01T00:00:00Z" })),
    ).toBe("Never");
    expect(
      tokenExpiresLabel(
        token({
          id: "d",
          createdAt: "2026-01-01T00:00:00Z",
          expiresAt: "not-a-date",
        }),
      ),
    ).toBe("Never");
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
          scopeDetails: [{ scope: "example-app", resources: ["workspace"] }],
        }),
      ),
    ).toBe("example-app (workspace)");
  });
});

describe("collapsed token scopes", () => {
  it("keeps short lists fully visible", () => {
    const entries = tokenScopeEntries({
      scopes: [
        "example-app:read",
        "example-app:write",
        "example-registry:read",
        "my-store:read",
      ],
    });
    expect(splitCollapsedTokenScopes(entries).rest).toEqual([]);
    expect(splitCollapsedTokenScopes(entries).preview).toHaveLength(4);
  });

  it("previews the first scopes and counts the rest when the list is long", () => {
    const entries = tokenScopeEntries({
      scopes: [
        "example-app:attachments.create",
        "example-app:contentRevisions.list",
        "example-app:customers.delete",
        "example-app:issues.list",
        "example-app:issues.update",
      ],
    });
    const split = splitCollapsedTokenScopes(entries);
    expect(split.preview.map((entry) => entry.scope)).toEqual([
      "example-app:attachments.create",
      "example-app:contentRevisions.list",
      "example-app:customers.delete",
    ]);
    expect(split.rest).toHaveLength(2);
  });
});
