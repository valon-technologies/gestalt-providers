import { describe, expect, test } from "vitest";

import { OTHER_SIGN_IN_METHODS_LABEL } from "@/lib/accountCopy";
import type { Integration } from "@/lib/api";
import { normalizeIntegrationStatus } from "@/lib/integrationStatus";
import {
  CONNECTION_SURFACE_TITLE,
  connectionSurfaceCopy,
  connectionSurfaceCopyForStatus,
  connectionSurfaceMode,
  humanizeConnectionName,
  overviewConnectionAttention,
  connectionPanelAttention,
  accountIdentityLines,
  accountInitials,
  accountRelationshipLabel,
  addAccountFormCopy,
  disconnectConfirmCopy,
  disconnectConfirmAccountLabel,
  USE_ACCOUNT_LABEL,
  DEFAULT_ACCOUNT_LABEL,
  IN_USE_LABEL,
  connectionMethodKind,
  connectionMethodTitle,
  connectionMethodPurpose,
  connectionDialogCopy,
  connectionEffectiveUseSummary,
  partitionConnectionMethods,
  shouldScopeInUseBadge,
  isInUseRelationship,
} from "./connection-surface-copy";

function stub(partial: Partial<Integration> & Pick<Integration, "name">): Integration {
  return { displayName: partial.name, description: "", ...partial };
}

describe("connection surface copy", () => {
  test("page title is always Connection", () => {
    expect(connectionSurfaceCopy("connect").title).toBe(CONNECTION_SURFACE_TITLE);
    expect(connectionSurfaceCopy("manage").title).toBe(CONNECTION_SURFACE_TITLE);
    expect(connectionSurfaceCopy("shared").title).toBe(CONNECTION_SURFACE_TITLE);
    expect(connectionSurfaceCopy("none").title).toBe(CONNECTION_SURFACE_TITLE);
  });

  test("manage mode for connected oauth apps", () => {
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
            actions: ["disconnect", "add_instance"],
          },
        ],
      }),
    );
    expect(connectionSurfaceMode(status)).toBe("manage");
    expect(connectionSurfaceCopy("manage").description).toMatch(/Only one is in use/);
    expect(connectionSurfaceCopy("manage").description).toMatch(/in use/i);
    expect(connectionSurfaceCopy("manage").trustNote).toMatch(/In use/);
    expect(connectionSurfaceCopy("manage").trustNote).not.toMatch(/privacy policy/i);
  });

  test("connect mode when credentials are missing", () => {
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
            authTypes: ["manual"],
            actions: ["connect"],
          },
        ],
      }),
    );
    expect(connectionSurfaceMode(status)).toBe("connect");
  });

  test("MCP passthrough stays a Connection surface", () => {
    const status = normalizeIntegrationStatus(
      stub({
        name: "mcp-passthrough-svc",
        status: "ready",
        credentialState: "not_required",
        connections: [
          {
            name: "MCP",
            displayName: "MCP",
            credentialMode: "none",
            credentialState: "not_required",
            status: "ready",
            mcpPassthrough: true,
          },
        ],
      }),
    );
    expect(connectionSurfaceMode(status)).toBe("shared");
     expect(connectionSurfaceCopy("shared").description).toMatch(/shared login/i);
  });

  test("humanize default machine names", () => {
    expect(humanizeConnectionName("default")).toBe("Account");
    expect(humanizeConnectionName("default", DEFAULT_ACCOUNT_LABEL)).toBe(
      "Account",
    );
    expect(humanizeConnectionName("work")).toBe("work");
  });

  test("overview attention for instance selection links to Connection", () => {
    const status = normalizeIntegrationStatus(
      stub({
        name: "gmail",
        status: "needs_instance_selection",
        credentialState: "connected",
        connections: [
          {
            name: "default",
            status: "needs_instance_selection",
            credentialState: "connected",
            connected: false,
            authTypes: ["oauth"],
            actions: ["select_instance"],
            instances: [
              { name: "work", connection: "default" },
              { name: "personal", connection: "default" },
            ],
          },
        ],
      }),
    );
    expect(status.connected).toBe(false);
    expect(status.connections[0]?.connected).toBe(false);
    const notice = overviewConnectionAttention(status);
    expect(notice?.title).toBe("Choose an account");
    expect(notice?.actionLabel).toBe("Choose an account");
    expect(notice?.description).toMatch(/no account in use/i);

    const panelNotice = connectionPanelAttention(status.connections[0]!);
    expect(panelNotice?.title).toBe("Choose an account");
    expect(panelNotice?.description).toMatch(/no account in use/i);

    const surface = connectionSurfaceCopyForStatus(status);
    expect(surface.trustNote).toMatch(/once you choose/i);
    expect(surface.trustNote).not.toMatch(/while connected/i);
    expect(accountRelationshipLabel({
      preferred: false,
      needsInstanceSelection: true,
    })).toBe("Available");
    expect(accountRelationshipLabel({
      preferred: true,
      needsInstanceSelection: true,
    })).toBe("In use");
  });

  test("disconnect confirm names the account when provided", () => {
    const copy = disconnectConfirmCopy({
      displayName: "Gmail",
      accountLabel: "hello",
    });
    expect(copy.heading).toBe("Disconnect hello?");
    expect(copy.body).toMatch(/hello/);
    expect(copy.body).toMatch(/Gmail/);
  });

  test("instance-scoped disconnect names a default account as Account", () => {
    expect(
      disconnectConfirmAccountLabel({ instanceName: "default" }),
    ).toBe(DEFAULT_ACCOUNT_LABEL);
    expect(
      disconnectConfirmCopy({
        displayName: "OAuth Service",
        accountLabel: disconnectConfirmAccountLabel({ instanceName: "default" }),
      }).heading,
    ).toBe("Disconnect Account?");
    expect(
      disconnectConfirmAccountLabel({
        identityPrimary: "ada@example.com",
        instanceName: "default",
      }),
    ).toBe("ada@example.com");
    expect(disconnectConfirmAccountLabel({})).toBeNull();
  });

  test("accountIdentityLines splits primary and additional facts", () => {
    const lines = accountIdentityLines({
      facts: [
        { kind: "display_name", value: "Ada" },
        { kind: "email", value: "ada@example.com", primary: true },
        { kind: "workspace", value: "Acme" },
      ],
    });
    expect(lines.primary).toEqual({
      kind: "email",
      value: "ada@example.com",
      primary: true,
    });
    expect(lines.additional.map((f) => f.kind)).toEqual([
      "display_name",
      "workspace",
    ]);
    expect(accountIdentityLines(undefined).primary).toBeNull();
  });

  test("overview attention is omitted for first-time connect", () => {
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
            authTypes: ["manual"],
            actions: ["connect"],
          },
        ],
      }),
    );
    expect(overviewConnectionAttention(status)).toBeNull();
  });

  test("dead preferred login stays manage and asks to reconnect", () => {
    const status = normalizeIntegrationStatus(
      stub({
        name: "notion",
        status: "needs_user_connection",
        credentialState: "invalid",
        healthState: "unhealthy",
        connections: [
          {
            name: "OAuth",
            displayName: "OAuth",
            status: "needs_user_connection",
            credentialState: "invalid",
            healthState: "unhealthy",
            connected: false,
            authTypes: ["oauth"],
            actions: ["reconnect", "disconnect"],
            instances: [{ name: "default" }],
          },
          {
            name: "ApiKey",
            displayName: "API key",
            status: "needs_user_connection",
            credentialState: "missing",
            authTypes: ["manual"],
            actions: ["connect"],
          },
        ],
      }),
    );
    expect(connectionSurfaceMode(status)).toBe("manage");
    expect(status.summaryLabel).toBe("Needs sign-in");
    const surface = connectionSurfaceCopyForStatus(status);
    expect(surface.description).toMatch(/needs a new sign-in/);
    expect(surface.trustNote).toMatch(/once access is restored/);
    const notice = overviewConnectionAttention(status);
    expect(notice?.title).toBe("Needs sign-in");
    expect(notice?.actionLabel).toBe("Sign in again on Connection");
    const panelNotice = connectionPanelAttention(status.connections[0]!);
    expect(panelNotice?.title).toBe("Needs sign-in");
    expect(panelNotice?.description).toMatch(/saved sign-in/);
    expect(
      accountRelationshipLabel({
        preferred: false,
        needsInstanceSelection: false,
        soleLinkedAccount: true,
      }),
    ).toBe("In use");
  });

  test("overview attention is omitted when ready", () => {
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
            actions: ["disconnect"],
          },
        ],
      }),
    );
    expect(overviewConnectionAttention(status)).toBeNull();
  });

  test("account relationship and add-account form copy", () => {
    expect(accountRelationshipLabel({
      preferred: false,
      needsInstanceSelection: false,
    })).toBe("Not in use");
    expect(accountRelationshipLabel({
      preferred: true,
      needsInstanceSelection: false,
    })).toBe("In use");

    const form = addAccountFormCopy({ appDisplayName: "GitHub" });
    expect(form.title).toBe("Connect GitHub");
    expect(form.label).toBe("Account label");
    expect(form.description).toMatch(/authenticate/i);
    expect(form.fieldDescription).toMatch(/tell this account apart/i);
    expect(form.placeholder).toMatch(/work, personal/);
    expect(
      addAccountFormCopy({
        appDisplayName: "GitHub",
        connectionKeyLabel: "OAuth",
      }).title,
    ).toBe("Connect another OAuth account");
  });

  test("account initials and use-account label", () => {
    expect(accountInitials("test")).toBe("TE");
    expect(accountInitials("Primary")).toBe("PR");
    expect(accountInitials("work email")).toBe("WE");
    expect(USE_ACCOUNT_LABEL).toBe("Use this account");
  });

  test("method titles and purpose copy replace credential chrome", () => {
    const status = normalizeIntegrationStatus(
      stub({
        name: "notion",
        connections: [
          {
            name: "ApiKey",
            displayName: "API key",
            authTypes: ["manual"],
            credentialState: "missing",
            status: "needs_user_connection",
            actions: ["connect"],
          },
          {
            name: "MCP",
            displayName: "MCP",
            authTypes: ["oauth"],
            credentialState: "missing",
            status: "needs_user_connection",
            actions: ["connect"],
          },
          {
            name: "OAuth",
            displayName: "OAuth",
            authTypes: ["oauth"],
            credentialState: "missing",
            status: "needs_user_connection",
            actions: ["connect"],
          },
          {
            name: "PAT",
            displayName: "Personal access token",
            authTypes: ["manual"],
            credentialState: "missing",
            status: "needs_user_connection",
            actions: ["connect"],
          },
        ],
      }),
    );
    const byKey = Object.fromEntries(
      status.connections.map((connection) => [connection.key, connection]),
    );
    expect(connectionMethodKind(byKey.ApiKey!)).toBe("api_key");
    expect(connectionMethodKind(byKey.MCP!)).toBe("mcp");
    expect(connectionMethodKind(byKey.OAuth!)).toBe("oauth");
    expect(connectionMethodKind(byKey.PAT!)).toBe("pat");
    expect(connectionMethodTitle(byKey.MCP!)).toBe("MCP");
    expect(connectionMethodTitle(byKey.OAuth!)).toBe("OAuth");
    expect(connectionMethodPurpose(byKey.ApiKey!)).toMatch(/Enter an API key/);
    expect(connectionMethodPurpose(byKey.MCP!)).toMatch(/assistants/);
    expect(connectionMethodPurpose(byKey.OAuth!)).toMatch(/API access/);
    expect(connectionMethodPurpose(byKey.PAT!)).toMatch(/user token/);
    expect(connectionMethodPurpose(byKey.ApiKey!)).not.toMatch(/credentials missing/i);

    const dialog = connectionDialogCopy(status, "Notion");
    expect(dialog.title).toBe("Connect Notion");
    expect(dialog.description).toMatch(/Methods are separate/);
    expect(dialog.description).not.toMatch(/User credentials missing/);
  });

  test("multi-method manage names the in-use accounts and scopes In use", () => {
    const status = normalizeIntegrationStatus(
      stub({
        name: "notion",
        status: "ready",
        credentialState: "connected",
        connections: [
          {
            name: "ApiKey",
            displayName: "API key",
            authTypes: ["manual"],
            credentialState: "missing",
            status: "needs_user_connection",
            actions: ["connect"],
          },
          {
            name: "MCP",
            displayName: "MCP",
            authTypes: ["oauth"],
            credentialState: "connected",
            status: "ready",
            actions: ["disconnect", "add_instance"],
            instances: [{ name: "default", preferred: true }],
          },
          {
            name: "OAuth",
            displayName: "OAuth",
            authTypes: ["oauth"],
            credentialState: "connected",
            status: "ready",
            actions: ["disconnect", "add_instance"],
            instances: [
              { name: "default", preferred: true },
              { name: "work", preferred: false },
            ],
          },
          {
            name: "PAT",
            displayName: "Personal access token",
            authTypes: ["manual"],
            credentialState: "missing",
            status: "needs_user_connection",
            actions: ["connect"],
          },
        ],
      }),
    );
    expect(shouldScopeInUseBadge(status.connections)).toBe(true);
    expect(
      accountRelationshipLabel({
        preferred: true,
        needsInstanceSelection: false,
        methodScope: "OAuth",
      }),
    ).toBe("In use for OAuth");
    expect(isInUseRelationship("In use for OAuth")).toBe(true);
    expect(isInUseRelationship(IN_USE_LABEL)).toBe(true);

    const summary = connectionEffectiveUseSummary(status);
    expect(summary).toMatch(/OAuth "Account" for API access/);
    expect(summary).toMatch(/MCP "Account"/);

    const surface = connectionSurfaceCopyForStatus(status);
    expect(surface.title).toBe(CONNECTION_SURFACE_TITLE);
    expect(surface.description).toMatch(/Each method can have its own/);
    expect(surface.trustNote).toMatch(/for that method/);

    const dialog = connectionDialogCopy(status, "Notion");
    expect(dialog.title).toBe("Connect Notion");
    expect(dialog.description).toMatch(/Methods are separate/);

    const partitioned = partitionConnectionMethods(status.connections);
    expect(partitioned.primary.map((c) => c.key)).toEqual(["MCP", "OAuth"]);
    expect(partitioned.other.map((c) => c.key)).toEqual(["ApiKey", "PAT"]);
    expect(OTHER_SIGN_IN_METHODS_LABEL).toBe("Other sign-in methods");
  });

  test("reconnect keeps unused methods visible instead of collapsing them", () => {
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
            actions: ["reconnect", "disconnect"],
            instances: [{ name: "default", preferred: true }],
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
    const partitioned = partitionConnectionMethods(status.connections);
    expect(partitioned.other).toEqual([]);
    expect(partitioned.primary.map((c) => c.key)).toEqual(["OAuth", "ApiKey"]);
    expect(shouldScopeInUseBadge(status.connections)).toBe(false);
    expect(
      accountRelationshipLabel({
        preferred: true,
        needsInstanceSelection: false,
        soleLinkedAccount: true,
      }),
    ).toBe("In use");
  });
});
