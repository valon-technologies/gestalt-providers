import { describe, expect, it } from "vitest";
import {
  SERVICE_ACCOUNTS_COPY,
  SERVICE_ACCOUNTS_ROUTE,
  toAgentIdentityRowView,
} from "./app-agent-identity-presentation";
import type { AppAdminIdentity } from "@/lib/api";

function identity(
  partial: Partial<AppAdminIdentity> & Pick<AppAdminIdentity, "subjectId">,
): AppAdminIdentity {
  return {
    displayName: "",
    role: "viewer",
    source: "static",
    mutable: false,
    effective: true,
    ...partial,
  };
}

describe("toAgentIdentityRowView", () => {
  it("shows display name and role only for a healthy grant", () => {
    expect(
      toAgentIdentityRowView(
        identity({
          subjectId: "service_account:slack-v2-smoke-test",
          displayName: "slack-v2-smoke-test",
          role: "viewer",
          source: "static",
          mutable: false,
          effective: true,
        }),
        0,
      ),
    ).toEqual({
      key: "service_account:slack-v2-smoke-test:viewer:static:0",
      title: "slack-v2-smoke-test",
      accountId: "slack-v2-smoke-test",
      showAccountId: false,
      roleLabel: "Viewer",
      exception: null,
    });
  });

  it("strips the wire prefix and never surfaces it as copy", () => {
    const row = toAgentIdentityRowView(
      identity({
        subjectId: "service_account:ci-runner",
        displayName: "CI Runner",
        role: "editor",
      }),
      1,
    );
    expect(row.accountId).toBe("ci-runner");
    expect(row.showAccountId).toBe(true);
    expect(row.title).toBe("CI Runner");
    expect(row.roleLabel).toBe("Editor");
    // Wire subject stays on `key` only; user-facing fields stay prefix-free.
    expect(row.title).not.toContain("service_account:");
    expect(row.accountId).not.toContain("service_account:");
  });

  it("falls back to the local account id when display name is empty", () => {
    expect(
      toAgentIdentityRowView(
        identity({
          subjectId: "service_account:bot",
          displayName: "  ",
        }),
        0,
      ),
    ).toMatchObject({
      title: "bot",
      accountId: "bot",
      showAccountId: false,
    });
  });

  it("surfaces an overridden exception instead of Effective/Shadowed badges", () => {
    expect(
      toAgentIdentityRowView(
        identity({
          subjectId: "service_account:old-bot",
          displayName: "old-bot",
          effective: false,
          shadowedBy: "static viewer grant",
        }),
        0,
      ).exception,
    ).toEqual({
      label: "Overridden",
      detail: "Not used — another grant takes priority",
    });
  });

  it("keeps product copy and route on the Service accounts term", () => {
    expect(SERVICE_ACCOUNTS_COPY.title).toBe("Service accounts");
    expect(SERVICE_ACCOUNTS_COPY.navLabel).toBe("Service accounts");
    expect(SERVICE_ACCOUNTS_COPY.docsLinkLabel).toBe(
      "How to create a service account",
    );
    expect(SERVICE_ACCOUNTS_ROUTE).toBe("/apps/$app/admin/service-accounts");
    expect(SERVICE_ACCOUNTS_COPY.listTestId).toBe("app-service-accounts-list");
  });
});
