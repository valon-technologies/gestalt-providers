import { describe, expect, test } from "vitest";
import type { Integration } from "@/lib/api";
import { buildAuthActions, connectEntryPlan } from "./connectionAuthActions";
import { normalizeIntegrationStatus } from "./integrationStatus";

function stub(partial: Partial<Integration> & Pick<Integration, "name">): Integration {
  return {
    displayName: partial.name,
    description: "",
    ...partial,
  };
}

describe("connectEntryPlan", () => {
  test("starts OAuth when the only action is a param-free connect", () => {
    expect(
      connectEntryPlan(
        stub({
          name: "datadog",
          connections: [
            {
              name: "default",
              authTypes: ["oauth"],
              credentialState: "missing",
              status: "needs_user_connection",
            },
          ],
        }),
      ),
    ).toEqual({ kind: "oauth", connection: "default" });
  });

  test("opens the token form when the only action is a manual connect", () => {
    expect(
      connectEntryPlan(
        stub({
          name: "datadog",
          connections: [
            {
              name: "default",
              authTypes: ["manual"],
              credentialState: "missing",
              status: "needs_user_connection",
            },
          ],
        }),
      ),
    ).toEqual({ kind: "form", view: "token" });
  });

  test("keeps the chooser when OAuth and a token are both available", () => {
    expect(
      connectEntryPlan(
        stub({
          name: "linear",
          connections: [
            {
              name: "default",
              authTypes: ["oauth", "manual"],
              credentialState: "missing",
              status: "needs_user_connection",
            },
          ],
        }),
      ),
    ).toEqual({ kind: "chooser" });
  });

  test("keeps the chooser when two connections can connect", () => {
    expect(
      connectEntryPlan(
        stub({
          name: "github",
          connections: [
            {
              name: "oauth",
              displayName: "OAuth",
              authTypes: ["oauth"],
            },
            {
              name: "mcp",
              displayName: "MCP",
              authTypes: ["oauth"],
            },
          ],
        }),
      ),
    ).toEqual({ kind: "chooser" });
  });

  test("opens oauth params when the only OAuth connect needs extra fields", () => {
    expect(
      connectEntryPlan(
        stub({
          name: "jira",
          connections: [
            {
              name: "default",
              authTypes: ["oauth"],
              connectionParams: {
                site: { required: true, description: "Site" },
              },
            },
          ],
        }),
      ),
    ).toEqual({ kind: "form", view: "oauth_params" });
  });
});

describe("buildAuthActions", () => {
  test("leads with Reconnect and demotes other methods when a login is dead", () => {
    const status = normalizeIntegrationStatus(
      stub({
        name: "notion",
        connections: [
          {
            name: "OAuth",
            displayName: "OAuth",
            authTypes: ["oauth"],
            credentialState: "invalid",
            healthState: "unhealthy",
            status: "needs_user_connection",
            actions: ["reconnect", "disconnect", "add_instance"],
            instances: [{ name: "default" }],
          },
          {
            name: "ApiKey",
            displayName: "API key",
            authTypes: ["manual"],
            credentialState: "missing",
            status: "needs_user_connection",
            actions: ["connect"],
          },
        ],
      }),
    );
    const actions = buildAuthActions(status.connections);
    expect(actions[0]?.kind).toBe("reconnect");
    expect(actions[0]?.label).toMatch(/Reconnect/);
    expect(actions[0]?.variant).toBe("default");
    const other = actions.filter((action) => action.kind !== "reconnect");
    expect(other.length).toBeGreaterThan(0);
    expect(other.every((action) => action.variant === "secondary")).toBe(true);
  });
});
