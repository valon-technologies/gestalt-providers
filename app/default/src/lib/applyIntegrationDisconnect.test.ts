import { describe, expect, test } from "vitest";

import type { Integration } from "@/lib/api";
import { connectionSurfaceMode } from "@/features/app-workspace/connection-surface-copy";
import {
  applyDisconnectToIntegration,
  applyDisconnectToIntegrations,
} from "./applyIntegrationDisconnect";
import { normalizeIntegrationStatus } from "./integrationStatus";

function gmailLinked(
  instances: NonNullable<
    NonNullable<Integration["connections"]>[number]["instances"]
  >,
): Integration {
  return {
    name: "gmail",
    displayName: "Gmail",
    status: "ready",
    credentialState: "connected",
    connections: [
      {
        name: "default",
        authTypes: ["oauth"],
        connected: true,
        credentialState: "connected",
        status: "ready",
        healthState: "not_applicable",
        actions: ["disconnect", "add_instance"],
        instances,
      },
    ],
  };
}

describe("applyDisconnectToIntegration", () => {
  test("removes the last linked account and leaves the connection connectable", () => {
    const next = applyDisconnectToIntegration(
      gmailLinked([{ name: "work", connection: "default", preferred: true }]),
      { instance: "work", connection: "default" },
    );
    const connection = next.connections?.[0];
    const status = normalizeIntegrationStatus(next, "current_user");

    expect(connection?.instances).toEqual([]);
    expect(connection?.connected).toBe(false);
    expect(connection?.actions).toBeUndefined();
    expect(status.connections[0]?.instances).toEqual([]);
    expect(status.connected).toBe(false);
    expect(status.connections[0]?.canConnect).toBe(true);
    expect(status.connections[0]?.actions).toContain("connect");
    expect(connectionSurfaceMode(status)).toBe("connect");
  });

  test("keeps sibling accounts on the same connection", () => {
    const next = applyDisconnectToIntegration(
      gmailLinked([
        { name: "work", connection: "default", preferred: true },
        { name: "personal", connection: "default", preferred: false },
      ]),
      { instance: "work", connection: "default" },
    );
    const names = next.connections?.[0]?.instances?.map((instance) => instance.name);

    expect(names).toEqual(["personal"]);
    expect(next.connections?.[0]?.connected).toBe(true);
    expect(next.connections?.[0]?.actions).toBeUndefined();
    expect(normalizeIntegrationStatus(next).connections[0]?.actions).toEqual(
      expect.arrayContaining(["disconnect", "add_instance"]),
    );
    expect(
      normalizeIntegrationStatus(next).connections[0]?.actions,
    ).not.toContain("select_instance");
  });

  test("falls back to a remaining preferred account after removing the named preferred", () => {
    const linked = gmailLinked([
      { name: "work", connection: "default", preferred: true },
      { name: "personal", connection: "default", preferred: true },
    ]);
    linked.connections![0]!.preferredInstance = "work";
    const next = applyDisconnectToIntegration(linked, {
      instance: "work",
      connection: "default",
    });
    expect(next.connections?.[0]?.preferredInstance).toBe("personal");
  });

  test("clears every account when disconnecting the connection as a whole", () => {
    const next = applyDisconnectToIntegration(
      gmailLinked([
        { name: "work", connection: "default" },
        { name: "personal", connection: "default" },
      ]),
      { connection: "default" },
    );

    expect(next.connections?.[0]?.instances).toEqual([]);
    expect(next.connections?.[0]?.connected).toBe(false);
    expect(next.connections?.[0]?.actions).toBeUndefined();
    expect(
      normalizeIntegrationStatus(next).connections[0]?.actions,
    ).toContain("connect");
  });

  test("does not touch a different connection on the same app", () => {
    const integration: Integration = {
      name: "linear",
      connections: [
        {
          name: "ApiKey",
          authTypes: ["manual"],
          connected: false,
          credentialState: "missing",
          status: "needs_user_connection",
          actions: ["connect"],
          instances: [],
        },
        {
          name: "OAuth",
          authTypes: ["oauth"],
          connected: true,
          credentialState: "connected",
          status: "ready",
          actions: ["disconnect", "add_instance"],
          instances: [{ name: "default", preferred: true }],
        },
      ],
    };

    const next = applyDisconnectToIntegration(integration, {
      instance: "default",
      connection: "OAuth",
    });

    expect(next.connections?.[0]).toEqual(integration.connections?.[0]);
    expect(next.connections?.[1]?.instances).toEqual([]);
    expect(next.connections?.[1]?.connected).toBe(false);
    expect(normalizeIntegrationStatus(next).connected).toBe(false);
  });

  test("keeps not_required credentials when the last account is removed", () => {
    const next = applyDisconnectToIntegration(
      {
        name: "internal-docs",
        connections: [
          {
            name: "default",
            authTypes: ["oauth"],
            connected: true,
            credentialState: "not_required",
            status: "ready",
            healthState: "not_applicable",
            actions: ["disconnect"],
            instances: [{ name: "work", connection: "default" }],
          },
        ],
      },
      { instance: "work", connection: "default" },
    );
    const connection = next.connections?.[0];

    expect(connection?.credentialState).toBe("not_required");
    expect(connection?.connected).toBe(false);
    expect(connection?.actions).toBeUndefined();
  });

  test("keeps admin and unavailable status when the last account is removed", () => {
    for (const status of ["needs_admin_configuration", "unavailable"] as const) {
      const next = applyDisconnectToIntegration(
        {
          name: "locked-app",
          connections: [
            {
              name: "default",
              authTypes: ["oauth"],
              connected: true,
              credentialState: "connected",
              status,
              healthState: "not_applicable",
              actions: ["disconnect"],
              instances: [{ name: "work", connection: "default" }],
            },
          ],
        },
        { instance: "work", connection: "default" },
      );

      expect(next.connections?.[0]?.status).toBe(status);
      expect(next.connections?.[0]?.actions).toBeUndefined();
    }
  });

  test("keeps unhealthy health when the last account is removed", () => {
    const next = applyDisconnectToIntegration(
      {
        name: "gmail",
        connections: [
          {
            name: "default",
            authTypes: ["oauth"],
            connected: true,
            credentialState: "connected",
            status: "degraded",
            healthState: "unhealthy",
            actions: ["disconnect"],
            instances: [{ name: "work", connection: "default" }],
          },
        ],
      },
      { instance: "work", connection: "default" },
    );
    const connection = next.connections?.[0];

    expect(connection?.healthState).toBe("unhealthy");
    expect(connection?.status).toBe("needs_user_connection");
    expect(connection?.credentialState).toBe("missing");
    expect(connection?.actions).toBeUndefined();
  });
});

describe("applyDisconnectToIntegrations", () => {
  test("updates only the disconnected app in the catalog list", () => {
    const gmail = gmailLinked([{ name: "work", connection: "default" }]);
    const slack: Integration = {
      name: "slack",
      connections: [
        {
          name: "default",
          authTypes: ["oauth"],
          connected: true,
          instances: [{ name: "work", connection: "default" }],
        },
      ],
    };

    const next = applyDisconnectToIntegrations([gmail, slack], "gmail", {
      instance: "work",
      connection: "default",
    });

    expect(next[0]?.connections?.[0]?.instances).toEqual([]);
    expect(next[1]).toBe(slack);
  });
});
