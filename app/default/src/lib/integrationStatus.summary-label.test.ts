import { describe, expect, test } from "vitest";

import type { Integration } from "@/lib/api";
import {
  hasCredentialSurface,
  normalizeIntegrationStatus,
} from "./integrationStatus";

function stub(partial: Partial<Integration> & Pick<Integration, "name">): Integration {
  return {
    displayName: partial.name,
    description: "",
    ...partial,
  };
}

describe("integration summaryLabel — credential absence silent", () => {
  test("not_required + ready chrome is Ready, never absence copy", () => {
    const status = normalizeIntegrationStatus(
      stub({
        name: "g-issues",
        status: "ready",
        credentialState: "not_required",
        mountedPath: "/g-issues",
        connections: [
          {
            name: "default",
            status: "ready",
            credentialState: "not_required",
            authTypes: ["none"],
          },
        ],
      }),
      "current_user",
    );

    expect(status.summaryLabel).toBe("Ready");
    expect(status.summaryLabel).not.toMatch(/credential/i);
    expect(hasCredentialSurface(status)).toBe(false);
    expect(status.connections[0]?.summaryLabel).toBe("Ready");
    expect(status.connections[0]?.credentialLabel).toBe("");
  });

  test("mount-only ready apps use Ready without a credential surface", () => {
    const status = normalizeIntegrationStatus(
      stub({
        name: "docs",
        status: "ready",
        credentialState: "not_required",
        mountedPath: "/docs",
      }),
      "current_user",
    );

    expect(status.summaryLabel).toBe("Ready");
    expect(hasCredentialSurface(status)).toBe(false);
  });

  test("actionable auth still surfaces next-step labels", () => {
    const missing = normalizeIntegrationStatus(
      stub({
        name: "gmail",
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
      "current_user",
    );
    expect(missing.summaryLabel).toBe("Not connected");
    expect(hasCredentialSurface(missing)).toBe(true);

    const connected = normalizeIntegrationStatus(
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
      "current_user",
    );
    expect(connected.summaryLabel).toBe("Connected");
    expect(hasCredentialSurface(connected)).toBe(true);
  });
});
