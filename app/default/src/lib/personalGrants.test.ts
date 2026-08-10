import { describe, expect, it } from "vitest";
import {
  grantScopesToTokenScopeDetails,
  grantScopesToTokenScopes,
  identityGrantToAPIToken,
} from "./personalGrants";

describe("personalGrants", () => {
  it("preserves resource bindings on scopes", () => {
    const details = grantScopesToTokenScopeDetails([
      { scope: "apps.invoke", resource: ["slack", "github"] },
      { scope: "identity.read", resource: [] },
    ]);
    expect(details).toEqual([
      { scope: "apps.invoke", resources: ["slack", "github"] },
      { scope: "identity.read" },
    ]);
    expect(grantScopesToTokenScopes([
      { scope: "apps.invoke", resource: ["slack", "github"] },
    ])).toEqual(["apps.invoke:slack,github"]);
  });

  it("maps grants without inventing last-used metadata", () => {
    const token = identityGrantToAPIToken("grant-1", {
      scopes: [{ scope: "apps.invoke", resource: ["slack"] }],
      createdAt: 1_700_000_000,
      expiresAt: 0,
    });
    expect(token).toEqual({
      id: "grant-1",
      scopes: ["apps.invoke:slack"],
      scopeDetails: [{ scope: "apps.invoke", resources: ["slack"] }],
      createdAt: new Date(1_700_000_000 * 1000).toISOString(),
      expiresAt: undefined,
    });
    expect(token).not.toHaveProperty("lastUsedAt");
  });
});
