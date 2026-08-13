import { describe, expect, test } from "vitest";

import type { Integration } from "@/lib/api";
import {
  catalogInstallState,
  primaryConnectLabel,
} from "./catalogFilters";
import { connectionSurfaceMode } from "@/features/app-workspace/connection-surface-copy";
import { normalizeIntegrationStatus } from "./integrationStatus";

function stub(partial: Partial<Integration> & Pick<Integration, "name">): Integration {
  return {
    displayName: partial.name,
    description: "",
    ...partial,
  };
}

/** Production Linear shape: OAuth linked, unused API key, API still ANDs. */
function linearOauthLinked(): Integration {
  return stub({
    name: "linear",
    status: "needs_user_connection",
    credentialState: "missing",
    actions: ["connect"],
    connections: [
      {
        name: "ApiKey",
        displayName: "API key",
        authTypes: ["manual"],
        connected: false,
        credentialState: "missing",
        status: "needs_user_connection",
        healthState: "not_applicable",
        actions: ["connect"],
        instances: [],
      },
      {
        name: "OAuth",
        displayName: "OAuth",
        authTypes: ["oauth"],
        connected: true,
        credentialState: "connected",
        status: "ready",
        healthState: "not_applicable",
        actions: ["disconnect", "add_instance"],
        instances: [{ name: "default", preferred: true }],
      },
    ],
  });
}

describe("alternative auth methods are OR at the app level", () => {
  test("Linear OAuth in use is Connected even when API key is unused", () => {
    const integration = linearOauthLinked();
    const status = normalizeIntegrationStatus(integration, "current_user");

    expect(status.connected).toBe(true);
    expect(status.status).toBe("ready");
    expect(status.credentialState).toBe("connected");
    expect(status.summaryLabel).toBe("Connected");
    expect(status.tone).toBe("success");
    expect(status.connections.find((c) => c.key === "ApiKey")?.connected).toBe(
      false,
    );
    expect(status.connections.find((c) => c.key === "OAuth")?.connected).toBe(
      true,
    );
  });

  test("overview and catalog treat that Linear app as already connected", () => {
    const integration = linearOauthLinked();
    expect(primaryConnectLabel(integration)).toBeNull();
    expect(catalogInstallState(integration)).toBe("connected");
    expect(
      connectionSurfaceMode(normalizeIntegrationStatus(integration)),
    ).toBe("manage");
  });

  test("Notion REST OAuth in use is Connected while MCP/PAT stay optional", () => {
    const integration = stub({
      name: "notion",
      status: "needs_user_connection",
      credentialState: "missing",
      connections: [
        {
          name: "ApiKey",
          displayName: "API key",
          authTypes: ["manual"],
          connected: false,
          credentialState: "missing",
          status: "needs_user_connection",
          actions: ["connect"],
        },
        {
          name: "MCP",
          displayName: "MCP",
          authTypes: ["oauth"],
          connected: false,
          credentialState: "missing",
          status: "needs_user_connection",
          actions: ["connect"],
        },
        {
          name: "OAuth",
          displayName: "OAuth",
          authTypes: ["oauth"],
          connected: true,
          credentialState: "connected",
          status: "ready",
          actions: ["disconnect", "add_instance"],
          instances: [{ name: "default", preferred: true }],
        },
        {
          name: "PAT",
          displayName: "Personal access token",
          authTypes: ["manual"],
          connected: false,
          credentialState: "missing",
          status: "needs_user_connection",
          actions: ["connect"],
        },
      ],
    });
    const status = normalizeIntegrationStatus(integration);

    expect(status.connected).toBe(true);
    expect(status.summaryLabel).toBe("Connected");
    expect(primaryConnectLabel(integration)).toBeNull();
    expect(catalogInstallState(integration)).toBe("connected");
  });

  test("Linear stays Not connected when no method is linked", () => {
    const integration = stub({
      name: "linear",
      status: "needs_user_connection",
      credentialState: "missing",
      connections: [
        {
          name: "ApiKey",
          authTypes: ["manual"],
          connected: false,
          credentialState: "missing",
          status: "needs_user_connection",
          actions: ["connect"],
        },
        {
          name: "OAuth",
          authTypes: ["oauth"],
          connected: false,
          credentialState: "missing",
          status: "needs_user_connection",
          actions: ["connect"],
        },
      ],
    });
    const status = normalizeIntegrationStatus(integration);

    expect(status.connected).toBe(false);
    expect(status.summaryLabel).toBe("Not connected");
    expect(primaryConnectLabel(integration)).toBe("Connect");
    expect(catalogInstallState(integration)).toBe("not_connected");
  });
});
