import { describe, expect, test } from "vitest";

import {
  appShowsCredentialSurface,
  overviewConnectionOutcomeStatus,
} from "./catalogFilters";
import { normalizeIntegrationStatus } from "./integrationStatus";
import type { Integration } from "@/lib/api";

function stub(partial: Partial<Integration> & Pick<Integration, "name">): Integration {
  return {
    displayName: partial.name,
    description: "",
    ...partial,
  };
}

describe("overviewConnectionOutcomeStatus", () => {
  test("Not connected / missing credentials use pending, not warning", () => {
    const status = normalizeIntegrationStatus(
      stub({
        name: "ashby",
        status: "needs_user_connection",
        credentialState: "missing",
        connections: [
          {
            name: "default",
            status: "needs_user_connection",
            credentialState: "missing",
            authTypes: ["oauth"],
          },
        ],
      }),
    );
    expect(status.summaryLabel).toBe("Not connected");
    expect(status.tone).toBe("warning");
    expect(overviewConnectionOutcomeStatus(status)).toBe("pending");
  });

  test("connected ready uses success", () => {
    const status = normalizeIntegrationStatus(
      stub({
        name: "gmail",
        status: "ready",
        credentialState: "connected",
        connections: [
          {
            name: "default",
            status: "ready",
            credentialState: "connected",
            authTypes: ["oauth"],
            connected: true,
          },
        ],
      }),
    );
    expect(overviewConnectionOutcomeStatus(status)).toBe("success");
  });
});

describe("appShowsCredentialSurface", () => {
  test("hides Connection for mount-only / no-credentials apps", () => {
    expect(
      appShowsCredentialSurface(
        stub({
          name: "example-app",
          mountedPath: "/example-app",
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
