import { describe, expect, test } from "vitest";
import type { APIToken, Integration } from "@/lib/api";
import {
  BUILD_CREATE_NEW_TOKEN_ID,
  BUILD_STEPS,
  BUILD_USE_EXISTING_TOKEN_ID,
  buildInstallStepTitle,
  buildMcpCredentialReady,
  buildStepDescription,
  buildStepTitle,
  catalogLoadStateFromQuery,
  firstIncompleteStepId,
  isActivationDue,
  isBuildStepUnlocked,
  isBuildComplete,
  isSetupDataSourceApp,
  isSetupTokenGrantId,
  isWorkspaceWarm,
  mcpInstalledForAgent,
  setupAppsConnected,
  setupAppsContinueBlockedReason,
  setupAppsHasConnectable,
  setupAppsStepComplete,
  setupDataSourceIntegrations,
  tryStepCatalogApp,
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
    mcpInstalledAgents: ["cursor"],
    apiToken: "gst_x",
    apiTokenGrantId: "tok_1",
    tokenName: "Workspace assistant",
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
    expect(isBuildComplete(completeSnapshot({ mcpInstalledAgents: [] }))).toBe(
      false,
    );
  });

  test("is incomplete until a token is chosen", () => {
    expect(
      isBuildComplete(
        completeSnapshot({
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
          apiToken: "",
          apiTokenGrantId: "",
          mcpInstalledAgents: [],
        }),
      ),
    ).toBe("assistant");
  });

  test("stops at token until a token is chosen", () => {
    expect(
      firstIncompleteStepId(
        completeSnapshot({
          apiToken: "",
          apiTokenGrantId: "",
          mcpInstalledAgents: [],
        }),
      ),
    ).toBe("token");
  });

  test("stops at install until MCP is acknowledged", () => {
    expect(firstIncompleteStepId(completeSnapshot({ mcpInstalledAgents: [] }))).toBe(
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
    expect(buildStepTitle(install, "claude")).toBe("Add Gestalt in Claude");
    expect(buildStepTitle(install, "claude-code")).toBe(
      "Add Gestalt in Claude Code",
    );
    expect(buildStepTitle(install, "chatgpt")).toBe("Add Gestalt in ChatGPT");
    expect(buildInstallStepTitle("codex")).toBe("Add Gestalt in Codex");
    expect(buildStepDescription(install, "cursor")).toBe(
      "Connect Cursor so it can use your Gestalt apps.",
    );
    expect(buildStepDescription(install, "codex")).toBe(
      "Run these commands in Terminal on the Mac where Codex is installed.",
    );
    expect(buildStepDescription(install, "cursor-agent")).toBe(
      "Paste this into .cursor/mcp.json. Cursor Agent reads the same MCP config as Cursor.",
    );
    expect(buildStepDescription(install, "other")).toBe(
      "Use these MCP settings in any client that accepts a URL and an Authorization header.",
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
    expect(buildStepTitle(token, "cursor")).toBe("Create a token");
    expect(token.description).toContain("Your assistant uses this token");
    expect(token.description).toContain("Add Gestalt fills it into the commands");
    expect(token.description).not.toContain("We only show");
    expect(token.description).not.toContain("coding agent");
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

  test("disables Connect apps Next until the catalog load agrees with completion", () => {
    expect(
      setupAppsContinueBlockedReason({
        integrations: [],
        catalogLoadState: "pending",
      }),
    ).toBe("Loading apps…");
    expect(
      setupAppsContinueBlockedReason({
        integrations: [],
        catalogLoadState: "failed",
      }),
    ).toBe("Couldn't load apps. Try again.");
    expect(
      setupAppsContinueBlockedReason({
        integrations: [disconnectedIntegration],
        catalogLoadState: "ready",
      }),
    ).toBe("Connect at least one app to continue");
    expect(
      setupAppsContinueBlockedReason({
        integrations: [],
        catalogLoadState: "ready",
      }),
    ).toBeNull();
    expect(
      setupAppsContinueBlockedReason({
        integrations: [connectedIntegration],
        catalogLoadState: "failed",
      }),
    ).toBeNull();
  });
});

describe("buildMcpCredentialReady", () => {
  test("a filled create-token name is not enough to continue", () => {
    expect(
      buildMcpCredentialReady({
        apiToken: "",
        apiTokenGrantId: "",
      }),
    ).toBe(false);
  });

  test("a minted secret bound to a real grant is enough", () => {
    expect(
      buildMcpCredentialReady({
        apiToken: "gst_x",
        apiTokenGrantId: "tok_new",
      }),
    ).toBe(true);
  });

  test("a leftover radio sentinel is not a grant", () => {
    expect(
      buildMcpCredentialReady({
        apiToken: "gst_x",
        apiTokenGrantId: BUILD_CREATE_NEW_TOKEN_ID,
      }),
    ).toBe(false);
  });

  test("picking a listed token is not enough without a session secret", () => {
    expect(
      buildMcpCredentialReady({
        apiToken: "",
        apiTokenGrantId: "tok_1",
      }),
    ).toBe(false);
  });
});

describe("isBuildStepUnlocked", () => {
  test("lets people open a step only after earlier steps are done", () => {
    const missingToken = completeSnapshot({
      apiToken: "",
      apiTokenGrantId: "",
      mcpInstalledAgents: [],
    });
    const isDone = (step: (typeof BUILD_STEPS)[number]) =>
      step.isComplete(missingToken);
    expect(isBuildStepUnlocked("welcome", isDone)).toBe(true);
    expect(isBuildStepUnlocked("assistant", isDone)).toBe(true);
    expect(isBuildStepUnlocked("token", isDone)).toBe(true);
    expect(isBuildStepUnlocked("install", isDone)).toBe(false);
    expect(isBuildStepUnlocked("apps", isDone)).toBe(false);
  });
});

describe("isSetupTokenGrantId", () => {
  test("rejects radio sentinels", () => {
    expect(isSetupTokenGrantId(BUILD_CREATE_NEW_TOKEN_ID)).toBe(false);
    expect(isSetupTokenGrantId(BUILD_USE_EXISTING_TOKEN_ID)).toBe(false);
    expect(isSetupTokenGrantId("")).toBe(false);
    expect(isSetupTokenGrantId("tok_new")).toBe(true);
  });
});

describe("mcpInstalledForAgent", () => {
  test("install complete is scoped to the acknowledged assistant", () => {
    expect(mcpInstalledForAgent(["cursor"], "cursor")).toBe(true);
    expect(mcpInstalledForAgent(["cursor"], "chatgpt")).toBe(false);
    expect(mcpInstalledForAgent(["cursor"], "")).toBe(false);
    const otherAssistant = completeSnapshot({
      installAgentId: "chatgpt",
      mcpInstalledAgents: ["cursor"],
    });
    expect(isBuildComplete(otherAssistant)).toBe(false);
    expect(firstIncompleteStepId(otherAssistant)).toBe("install");
  });
});

describe("tryStepCatalogApp", () => {
  test("uses live catalog fields and fills mount and copy gaps", () => {
    expect(
      tryStepCatalogApp({
        appId: "aiSpendTracker",
        catalog: {
          name: "aiSpendTracker",
          displayName: "AI Spend Tracker",
          description: "Live description",
          iconSvg: "<svg></svg>",
          credentialState: "connected",
          status: "ready",
        },
        label: "Fallback",
        description: "Fallback copy",
        mountedPath: "/ai-spend",
      }),
    ).toMatchObject({
      name: "aiSpendTracker",
      displayName: "AI Spend Tracker",
      description: "Live description",
      iconSvg: "<svg></svg>",
      mountedPath: "/ai-spend",
    });
  });

  test("builds a catalog tile when the app is missing from the workspace", () => {
    expect(
      tryStepCatalogApp({
        appId: "example-app",
        label: "Example app",
        description: "Open Example app in Gestalt.",
        mountedPath: "/example-app",
      }),
    ).toEqual({
      name: "example-app",
      displayName: "Example app",
      description: "Open Example app in Gestalt.",
      mountedPath: "/example-app",
    });
  });
});
