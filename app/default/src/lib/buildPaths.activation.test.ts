import { describe, expect, test } from "vitest";
import type { APIToken, Integration } from "@/lib/api";
import {
  BUILD_CREATE_NEW_TOKEN_ID,
  BUILD_STEPS,
  BUILD_USE_EXISTING_TOKEN_ID,
  buildAuthorizeSelectionReady,
  buildInstallStepTitle,
  buildStepDescription,
  buildStepTitle,
  catalogLoadStateFromQuery,
  firstIncompleteStepId,
  isActivationDue,
  isBuildComplete,
  isSetupDataSourceApp,
  isSetupTokenGrantId,
  isWorkspaceWarm,
  sessionApiTokenBoundToSelection,
  setupAppsConnected,
  setupAppsHasConnectable,
  setupAppsStepComplete,
  setupDataSourceIntegrations,
  tokensIncludingSessionGrant,
  type BuildWorkspaceSnapshot,
} from "@/lib/buildPaths";

const token: APIToken = {
  id: "tok_1",
  name: "Test",
  scopes: ["api"],
  createdAt: "2026-01-01T00:00:00Z",
};

const connectedIntegration: Integration = {
  name: "slack",
  displayName: "Slack",
  description: "Slack",
  credentialState: "connected",
  status: "ready",
  connections: [
    {
      name: "slack",
      connected: true,
      credentialState: "connected",
      status: "ready",
    },
  ],
};

const disconnectedIntegration: Integration = {
  name: "pagerduty",
  displayName: "PagerDuty",
  description: "PagerDuty",
  credentialState: "missing",
  status: "needs_user_connection",
};

const mountOnlyIntegration: Integration = {
  name: "example-native",
  displayName: "Example native",
  description: "Example native",
  mountedPath: "/example-native",
  credentialState: "not_required",
  status: "ready",
  connections: [],
};

const noAuthIntegration: Integration = {
  name: "example-public",
  displayName: "Example public",
  description: "Example public",
  mountedPath: "/example-public",
  credentialState: "not_required",
  status: "ready",
  connections: [
    {
      name: "default",
      status: "ready",
      credentialState: "not_required",
      authTypes: [],
    },
  ],
};

describe("isWorkspaceWarm", () => {
  test("is warm when any API token exists", () => {
    expect(isWorkspaceWarm([token], [])).toBe(true);
  });

  test("is warm when any app is connected", () => {
    expect(isWorkspaceWarm([], [connectedIntegration])).toBe(true);
  });

  test("is cold with no tokens and no connected apps", () => {
    expect(isWorkspaceWarm([], [disconnectedIntegration])).toBe(false);
    expect(isWorkspaceWarm([], [])).toBe(false);
  });
});

describe("isActivationDue", () => {
  test("is due for empty cold accounts that have not skipped", () => {
    expect(
      isActivationDue({
        tokens: [],
        integrations: [disconnectedIntegration],
        skipped: false,
      }),
    ).toBe(true);
  });

  test("is not due when the user skipped setup", () => {
    expect(
      isActivationDue({
        tokens: [],
        integrations: [],
        skipped: true,
      }),
    ).toBe(false);
  });

  test("is not due for warm workspaces", () => {
    expect(
      isActivationDue({
        tokens: [token],
        integrations: [],
        skipped: false,
      }),
    ).toBe(false);
    expect(
      isActivationDue({
        tokens: [],
        integrations: [connectedIntegration],
        skipped: false,
      }),
    ).toBe(false);
  });

  test("is not due when setup is already complete", () => {
    expect(
      isActivationDue({
        tokens: [],
        integrations: [],
        skipped: false,
        complete: true,
      }),
    ).toBe(false);
  });
});

function completeSnapshot(
  overrides: Partial<BuildWorkspaceSnapshot> = {},
): BuildWorkspaceSnapshot {
  return {
    integrations: [connectedIntegration],
    tokens: [token],
    catalogLoadState: "ready",
    activeExemplarId: "aiSpendTracker",
    mcpInstalled: true,
    apiToken: "gst_x",
    apiTokenGrantId: "tok_1",
    tokenName: "Workspace assistant",
    selectedTokenId: "tok_1",
    installAgentId: "cursor",
    welcomeSeen: true,
    trySeen: true,
    ...overrides,
  };
}

describe("isBuildComplete", () => {
  test("is complete when every setup step is done", () => {
    expect(isBuildComplete(completeSnapshot())).toBe(true);
  });

  test("is complete when the catalog has nothing left to connect", () => {
    expect(
      isBuildComplete(
        completeSnapshot({
          integrations: [disconnectedIntegration],
        }),
      ),
    ).toBe(false);
    expect(
      isBuildComplete(
        completeSnapshot({
          integrations: [],
        }),
      ),
    ).toBe(true);
    expect(
      isBuildComplete(
        completeSnapshot({
          integrations: [mountOnlyIntegration, noAuthIntegration],
        }),
      ),
    ).toBe(true);
  });

  test("is incomplete until the try step is opened", () => {
    expect(isBuildComplete(completeSnapshot({ trySeen: false }))).toBe(false);
  });

  test("is incomplete without an MCP install ack", () => {
    expect(isBuildComplete(completeSnapshot({ mcpInstalled: false }))).toBe(
      false,
    );
  });

  test("is incomplete until a token is chosen", () => {
    expect(
      isBuildComplete(
        completeSnapshot({
          selectedTokenId: "",
          apiToken: "",
          apiTokenGrantId: "",
        }),
      ),
    ).toBe(false);
  });

  test("is incomplete until an assistant is chosen", () => {
    expect(isBuildComplete(completeSnapshot({ installAgentId: "" }))).toBe(
      false,
    );
  });

  test("is incomplete when the catalog has not loaded, even if the list is empty", () => {
    expect(
      isBuildComplete(
        completeSnapshot({
          integrations: [],
          catalogLoadState: "pending",
        }),
      ),
    ).toBe(false);
    expect(
      isBuildComplete(
        completeSnapshot({
          integrations: [],
          catalogLoadState: "failed",
        }),
      ),
    ).toBe(false);
  });
});

describe("firstIncompleteStepId", () => {
  test("starts at welcome until the intro is seen", () => {
    expect(firstIncompleteStepId(completeSnapshot({ welcomeSeen: false }))).toBe(
      "welcome",
    );
  });

  test("stops at assistant until an assistant is chosen", () => {
    expect(
      firstIncompleteStepId(
        completeSnapshot({
          installAgentId: "",
          selectedTokenId: "",
          apiToken: "",
          apiTokenGrantId: "",
          mcpInstalled: false,
        }),
      ),
    ).toBe("assistant");
  });

  test("stops at token until a token is chosen", () => {
    expect(
      firstIncompleteStepId(
        completeSnapshot({
          selectedTokenId: "",
          apiToken: "",
          apiTokenGrantId: "",
          mcpInstalled: false,
        }),
      ),
    ).toBe("token");
  });

  test("stops at install until MCP is acknowledged", () => {
    expect(firstIncompleteStepId(completeSnapshot({ mcpInstalled: false }))).toBe(
      "install",
    );
  });

  test("stops at apps until an app is connected", () => {
    expect(
      firstIncompleteStepId(
        completeSnapshot({
          integrations: [disconnectedIntegration],
          trySeen: false,
        }),
      ),
    ).toBe("apps");
  });

  test("stops at apps when the catalog failed to load", () => {
    expect(
      firstIncompleteStepId(
        completeSnapshot({
          integrations: [],
          catalogLoadState: "failed",
          trySeen: false,
        }),
      ),
    ).toBe("apps");
  });

  test("stops at try until the try step is opened", () => {
    expect(firstIncompleteStepId(completeSnapshot({ trySeen: false }))).toBe(
      "try",
    );
  });
});

describe("buildStepTitle", () => {
  test("names the install step after the chosen assistant", () => {
    const install = BUILD_STEPS.find((step) => step.id === "install")!;
    expect(buildStepTitle(install, "cursor")).toBe("Add Gestalt in Cursor");
    expect(buildStepTitle(install, "claude")).toBe(
      "Add Gestalt in Claude Code",
    );
    expect(buildInstallStepTitle("codex")).toBe("Add Gestalt in Codex");
    expect(buildStepDescription(install, "cursor")).toBe(
      "Connect Cursor so it can use your Gestalt apps.",
    );
  });

  test("keeps token and try titles static", () => {
    expect(BUILD_STEPS.map((step) => step.id)).toEqual([
      "welcome",
      "assistant",
      "token",
      "install",
      "apps",
      "try",
    ]);
    const token = BUILD_STEPS.find((step) => step.id === "token")!;
    expect(buildStepTitle(token, "cursor")).toBe("Create an API token");
  });
});

describe("setup data-source apps", () => {
  test("keeps OAuth / API-key apps and drops native products", () => {
    expect(isSetupDataSourceApp(connectedIntegration)).toBe(true);
    expect(isSetupDataSourceApp(disconnectedIntegration)).toBe(true);
    expect(isSetupDataSourceApp(mountOnlyIntegration)).toBe(false);
    expect(isSetupDataSourceApp(noAuthIntegration)).toBe(false);
    expect(
      setupDataSourceIntegrations([
        connectedIntegration,
        mountOnlyIntegration,
        noAuthIntegration,
        disconnectedIntegration,
      ]).map((integration) => integration.name),
    ).toEqual(["slack", "pagerduty"]);
  });

  test("does not treat native apps as connected or still-to-connect", () => {
    expect(
      setupAppsConnected({
        integrations: [mountOnlyIntegration, noAuthIntegration],
      }),
    ).toBe(false);
    expect(
      setupAppsHasConnectable({
        integrations: [mountOnlyIntegration, noAuthIntegration],
      }),
    ).toBe(false);
    expect(
      setupAppsHasConnectable({
        integrations: [disconnectedIntegration, mountOnlyIntegration],
      }),
    ).toBe(true);
    expect(
      setupAppsConnected({
        integrations: [connectedIntegration, mountOnlyIntegration],
      }),
    ).toBe(true);
  });

  test("does not complete Connect apps until a successful catalog load", () => {
    expect(
      setupAppsStepComplete({
        integrations: [],
        catalogLoadState: "pending",
      }),
    ).toBe(false);
    expect(
      setupAppsStepComplete({
        integrations: [],
        catalogLoadState: "failed",
      }),
    ).toBe(false);
    expect(
      setupAppsStepComplete({
        integrations: [],
        catalogLoadState: "ready",
      }),
    ).toBe(true);
    expect(
      setupAppsStepComplete({
        integrations: [connectedIntegration],
        catalogLoadState: "failed",
      }),
    ).toBe(true);
    expect(catalogLoadStateFromQuery({ isPending: true, isError: false })).toBe(
      "pending",
    );
    expect(catalogLoadStateFromQuery({ isPending: false, isError: true })).toBe(
      "failed",
    );
    expect(catalogLoadStateFromQuery({ isPending: false, isError: false })).toBe(
      "ready",
    );
  });
});

describe("buildAuthorizeSelectionReady", () => {
  test("a filled create-token name is not enough to continue", () => {
    expect(
      buildAuthorizeSelectionReady({
        apiToken: "",
        apiTokenGrantId: "",
        selectedTokenId: "new",
      }),
    ).toBe(false);
  });

  test("a minted secret bound to the new grant is enough", () => {
    expect(
      buildAuthorizeSelectionReady({
        apiToken: "gst_x",
        apiTokenGrantId: "tok_new",
        selectedTokenId: "tok_new",
      }),
    ).toBe(true);
  });

  test("picking a listed token is not enough without a session secret", () => {
    expect(
      buildAuthorizeSelectionReady({
        apiToken: "",
        apiTokenGrantId: "",
        selectedTokenId: "tok_1",
      }),
    ).toBe(false);
  });
});

describe("sessionApiTokenBoundToSelection", () => {
  test("keeps plaintext only when it is bound to the selected grant", () => {
    expect(sessionApiTokenBoundToSelection("tok_1", "tok_1")).toBe(true);
    expect(sessionApiTokenBoundToSelection("tok_1", "tok_2")).toBe(false);
    expect(sessionApiTokenBoundToSelection("tok_1", "new")).toBe(false);
  });

  test("never treats an unbound secret as matching a selection", () => {
    expect(sessionApiTokenBoundToSelection("", "tok_1")).toBe(false);
    expect(sessionApiTokenBoundToSelection("", "new")).toBe(false);
    expect(sessionApiTokenBoundToSelection("  ", "  ")).toBe(false);
  });
});

describe("tokensIncludingSessionGrant", () => {
  test("isSetupTokenGrantId rejects radio sentinels", () => {
    expect(isSetupTokenGrantId(BUILD_CREATE_NEW_TOKEN_ID)).toBe(false);
    expect(isSetupTokenGrantId(BUILD_USE_EXISTING_TOKEN_ID)).toBe(false);
    expect(isSetupTokenGrantId("")).toBe(false);
    expect(isSetupTokenGrantId("tok_new")).toBe(true);
  });

  test("prepends a session-minted grant the server list omitted", () => {
    const listed = tokensIncludingSessionGrant([token], {
      grantId: "tok_new",
      name: "Workspace assistant",
      createdAt: "2026-08-14T15:00:00Z",
    });
    expect(listed.map((item) => item.id)).toEqual(["tok_new", "tok_1"]);
    expect(listed[0]).toEqual({
      id: "tok_new",
      name: "Workspace assistant",
      createdAt: "2026-08-14T15:00:00Z",
    });
  });

  test("does not duplicate a grant the server already returned", () => {
    expect(
      tokensIncludingSessionGrant([token], {
        grantId: "tok_1",
        name: "Test",
      }),
    ).toEqual([token]);
  });

  test("does not inject radio sentinels or an unbound grant", () => {
    expect(
      tokensIncludingSessionGrant([token], {
        grantId: BUILD_CREATE_NEW_TOKEN_ID,
      }),
    ).toEqual([token]);
    expect(tokensIncludingSessionGrant([token], { grantId: "" })).toEqual([
      token,
    ]);
  });
});
