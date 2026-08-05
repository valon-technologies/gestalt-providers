import { describe, expect, test } from "vitest";

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
  ADD_ACCOUNT_LABEL,
  USE_ACCOUNT_LABEL,
  DEFAULT_ACCOUNT_LABEL,
} from "./connection-surface-copy";

function stub(partial: Partial<Integration> & Pick<Integration, "name">): Integration {
  return { displayName: partial.name, description: "", ...partial };
}

describe("connection surface copy", () => {
  test("page title is always Connection", () => {
    expect(connectionSurfaceCopy("connect").title).toBe(CONNECTION_SURFACE_TITLE);
    expect(connectionSurfaceCopy("manage").title).toBe(CONNECTION_SURFACE_TITLE);
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

  test("humanize default machine names", () => {
    expect(humanizeConnectionName("default")).toBe("Connection");
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
    expect(notice?.description).toMatch(/not connected/i);

    const panelNotice = connectionPanelAttention(status.connections[0]!);
    expect(panelNotice?.title).toBe("Choose an account");
    expect(panelNotice?.description).toMatch(/not connected/i);

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

    const form = addAccountFormCopy();
    expect(form.title).toBe("Add account");
    expect(form.label).toBe("Account label");
    expect(form.description).toMatch(/authenticate/i);
    expect(form.fieldDescription).toMatch(/tell this account apart/i);
    expect(form.placeholder).toMatch(/work, personal/);
    expect(ADD_ACCOUNT_LABEL).toBe("Add account");
  });

  test("account initials and use-account label", () => {
    expect(accountInitials("test")).toBe("TE");
    expect(accountInitials("Primary")).toBe("PR");
    expect(accountInitials("work email")).toBe("WE");
    expect(USE_ACCOUNT_LABEL).toBe("Use this account");
  });
});
