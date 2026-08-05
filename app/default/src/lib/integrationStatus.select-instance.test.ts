import { describe, expect, test } from "vitest";

import { normalizeIntegrationStatus } from "./integrationStatus";
import type { Integration } from "@/lib/api";

function stub(partial: Partial<Integration> & Pick<Integration, "name">): Integration {
  return {
    displayName: partial.name,
    description: "",
    ...partial,
  };
}

describe("normalizeIntegrationStatus preferred-account actions", () => {
  test("keeps select_instance when multiple accounts are already linked", () => {
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
            instances: [
              { name: "work", preferred: true },
              { name: "personal", preferred: false },
            ],
          },
        ],
      }),
      "current_user",
    );

    expect(status.connections[0]?.canSelectInstance).toBe(true);
    expect(status.connections[0]?.actions).toEqual(
      expect.arrayContaining(["select_instance", "add_instance", "disconnect"]),
    );
  });

  test("does not expose select_instance for a single linked account", () => {
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
            instances: [{ name: "work", preferred: true }],
          },
        ],
      }),
      "current_user",
    );

    expect(status.connections[0]?.canSelectInstance).toBe(false);
    expect(status.connections[0]?.actions).not.toContain("select_instance");
  });
});
