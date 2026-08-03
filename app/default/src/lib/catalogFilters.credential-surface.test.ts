import { describe, expect, test } from "vitest";

import { appShowsCredentialSurface } from "./catalogFilters";
import type { Integration } from "@/lib/api";

function stub(partial: Partial<Integration> & Pick<Integration, "name">): Integration {
  return {
    displayName: partial.name,
    description: "",
    ...partial,
  };
}

describe("appShowsCredentialSurface", () => {
  test("hides Connection for mount-only / no-credentials apps", () => {
    expect(
      appShowsCredentialSurface(
        stub({
          name: "g-issues",
          mountedPath: "/g-issues",
          status: "ready",
          credentialState: "not_required",
          connections: [],
        }),
      ),
    ).toBe(false);
  });

  test("hides Connection for not_required with only no-auth connection rows", () => {
    expect(
      appShowsCredentialSurface(
        stub({
          name: "public-api",
          status: "ready",
          credentialState: "not_required",
          connections: [
            {
              name: "default",
              status: "ready",
              credentialState: "not_required",
              authTypes: [],
            },
          ],
        }),
      ),
    ).toBe(false);
  });

  test("shows Connection when the user still needs to connect", () => {
    expect(
      appShowsCredentialSurface(
        stub({
          name: "ashby",
          status: "needs_user_connection",
          credentialState: "missing",
          connections: [
            {
              name: "default",
              status: "needs_user_connection",
              credentialState: "missing",
              authTypes: ["manual"],
              actions: ["connect"],
            },
          ],
        }),
      ),
    ).toBe(true);
  });

  test("shows Connection when a credential-bearing connection exists", () => {
    expect(
      appShowsCredentialSurface(
        stub({
          name: "github",
          status: "ready",
          credentialState: "connected",
          connections: [
            {
              name: "default",
              status: "ready",
              credentialState: "connected",
              authTypes: ["oauth"],
              actions: ["disconnect"],
            },
          ],
        }),
      ),
    ).toBe(true);
  });
});
