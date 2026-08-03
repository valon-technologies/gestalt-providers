import { describe, expect, test } from "vitest";

import type { Integration } from "@/lib/api";
import { normalizeIntegrationStatus } from "@/lib/integrationStatus";
import {
  CONNECTION_SURFACE_TITLE,
  connectionSurfaceCopy,
  connectionSurfaceMode,
  humanizeConnectionName,
  overviewConnectionAttention,
  connectionPanelAttention,
  accountInitials,
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
    expect(connectionSurfaceCopy("manage").description).toMatch(/Manage/);
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
    const notice = overviewConnectionAttention(status);
    expect(notice?.title).toBe("Choose an account");
    expect(notice?.actionLabel).toMatch(/Connection/);
    expect(notice?.description).toMatch(/account/i);

    const panelNotice = connectionPanelAttention(status.connections[0]!);
    expect(panelNotice?.title).toBe("Choose an account");
    expect(panelNotice?.description).toMatch(/Choose which one/i);
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

  test("account initials and use-account label", () => {
    expect(accountInitials("test")).toBe("TE");
    expect(accountInitials("Primary")).toBe("PR");
    expect(accountInitials("work email")).toBe("WE");
    expect(USE_ACCOUNT_LABEL).toBe("Use this account");
  });
});
