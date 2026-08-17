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
    expect(connection?.actions).toEqual(["connect"]);
    expect(status.connections[0]?.instances).toEqual([]);
    expect(status.connected).toBe(false);
    expect(status.connections[0]?.canConnect).toBe(true);
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
    expect(next.connections?.[0]?.actions).toEqual(["disconnect", "add_instance"]);
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
