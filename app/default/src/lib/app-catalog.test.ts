import type {
  AppCatalogConnection,
  AppCatalogEntry,
  AppConnectionOverlay,
  AppConnectionStatus,
  ConnectionDefInfo,
  Integration,
} from "@/lib/api";
import { mergeAppCatalogWithConnections } from "@/lib/app-catalog";

import { describe, expect, it } from "vitest";

const catalog: AppCatalogEntry[] = [
  {
    name: "slack",
    displayName: "Slack",
    iconUrl: "/api/v1/catalog/apps/slack/icon",
    connections: [
      {
        name: "default",
        mode: "subject",
        authTypes: ["oauth"],
      } satisfies AppCatalogConnection,
    ],
  },
];

const overlay: AppConnectionStatus[] = [
  {
    name: "slack",
    status: "ready",
    credentialState: "connected",
    healthState: "not_checked",
    actions: ["disconnect"],
    connected: true,
    connections: [
      {
        name: "default",
        status: "ready",
        credentialState: "connected",
        connected: true,
        instances: [{ name: "default" }],
      } satisfies AppConnectionOverlay,
    ],
  },
];

describe("mergeAppCatalogWithConnections", () => {
  it("paints catalog rows before overlay arrives", () => {
    const apps = mergeAppCatalogWithConnections(catalog);
    expect(apps).toEqual([
      {
        name: "slack",
        displayName: "Slack",
        iconUrl: "/api/v1/catalog/apps/slack/icon",
        connections: [
          {
            name: "default",
            mode: "subject",
            authTypes: ["oauth"],
          },
        ],
      },
    ] satisfies Integration[]);
    expect(apps[0]?.status).toBeUndefined();
    expect(apps[0]?.iconSvg).toBeUndefined();
  });

  it("fills subject connection status by app and connection name", () => {
    const apps = mergeAppCatalogWithConnections(catalog, overlay);
    expect(apps[0]?.status).toBe("ready");
    expect(apps[0]?.credentialState).toBe("connected");
    expect(apps[0]?.connected).toBe(true);
    const connection = apps[0]?.connections?.[0] as ConnectionDefInfo;
    expect(connection.connected).toBe(true);
    expect(connection.authTypes).toEqual(["oauth"]);
    expect(connection.instances).toEqual([{ name: "default" }]);
  });

  it("keeps overlay passthrough and instance status when zipping schema", () => {
    const apps = mergeAppCatalogWithConnections(
      [
        {
          name: "mcp",
          connections: [{ name: "MCP", displayName: "MCP", mcpPassthrough: true }],
        },
      ],
      [
        {
          name: "mcp",
          connections: [
            {
              name: "MCP",
              credentialState: "not_required",
              status: "ready",
            },
          ],
        },
      ],
    );
    expect(apps[0]?.connections?.[0]?.mcpPassthrough).toBe(true);
    expect(apps[0]?.connections?.[0]?.displayName).toBe("MCP");
  });

  it("paints advertised passthrough from catalog before overlay arrives", () => {
    const apps = mergeAppCatalogWithConnections([
      {
        name: "mcp",
        connections: [{ name: "MCP", displayName: "MCP", mcpPassthrough: true }],
      },
    ]);
    expect(apps[0]?.connections?.[0]?.mcpPassthrough).toBe(true);
  });

  it("keeps catalog schema when overlay names a different connection", () => {
    const apps = mergeAppCatalogWithConnections(catalog, [
      {
        name: "slack",
        status: "ready",
        connections: [
          {
            name: "other",
            connected: true,
            instances: [{ name: "work" }],
          },
        ],
      },
    ]);
    const connection = apps[0]?.connections?.[0] as ConnectionDefInfo;
    expect(connection.name).toBe("default");
    expect(connection.connected).toBeUndefined();
    expect(connection.instances).toBeUndefined();
  });

  it("copies the overlay app-level connected rollup instead of ORing rows", () => {
    const apps = mergeAppCatalogWithConnections(
      [
        {
          name: "mcp",
          connections: [{ name: "MCP", displayName: "MCP", mcpPassthrough: true }],
        },
      ],
      [
        {
          name: "mcp",
          status: "ready",
          credentialState: "not_required",
          connected: false,
          connections: [
            {
              name: "MCP",
              status: "ready",
              credentialState: "not_required",
              connected: false,
            },
          ],
        },
      ],
    );
    expect(apps[0]?.connected).toBe(false);
  });
});
