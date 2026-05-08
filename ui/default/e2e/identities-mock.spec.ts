import { expect, mockAuthInfo, test } from "./fixtures";
import type {
  AccessPermission,
  APIToken,
  Integration,
  ManagedIdentity,
  ManagedIdentityGrant,
  ManagedIdentityMember,
} from "../src/lib/api";
import { CONNECTION_RETURN_PATH_STORAGE_KEY } from "../src/lib/constants";

type IdentityState = {
  identities: ManagedIdentity[];
  membersByIdentityID: Record<string, ManagedIdentityMember[]>;
  grantsByIdentityID: Record<string, ManagedIdentityGrant[]>;
  tokensByIdentityID: Record<string, APIToken[]>;
  visibleIntegrations: Integration[];
  managedIntegrationsByIdentityID: Record<string, Integration[]>;
};

function isoDate(days: number): string {
  const date = new Date(Date.UTC(2026, 3, 15 + days, 12, 0, 0));
  return date.toISOString();
}

async function wireIdentityRoutes(
  page: import("@playwright/test").Page,
  state: IdentityState,
  opts?: {
    onCreateToken?: (body: Record<string, unknown>) => void;
    onManagedConnect?: (body: Record<string, unknown>) => void;
    onManagedDisconnect?: (integration: string) => void;
    wrapGrantResponse?: boolean;
  },
) {
  await page.route("**/api/v1/integrations", async (route, request) => {
    if (request.method() === "GET") {
      await route.fulfill({ json: state.visibleIntegrations });
      return;
    }
    await route.fallback();
  });

  await page.route("**/api/v1/authorization/subjects", async (route, request) => {
    if (request.method() === "GET") {
      await route.fulfill({ json: state.identities });
      return;
    }
    if (request.method() === "POST") {
      const body = request.postDataJSON() as { id: string; subjectId?: string; displayName: string };
      const subjectId = body.subjectId || `service_account:${body.id}`;
      const localId = subjectId.replace(/^service_account:/, "");
      const identity: ManagedIdentity = {
        id: localId,
        subjectId,
        kind: "service_account",
        displayName: body.displayName,
        credentialSubjectId: subjectId,
        createdBySubjectId: "user:test@gestalt.dev",
        createdAt: isoDate(10 + state.identities.length),
        updatedAt: isoDate(10 + state.identities.length),
      };
      state.identities = [...state.identities, identity];
      state.membersByIdentityID[identity.subjectId] = [
        {
          subjectId: "user:test@gestalt.dev",
          email: "test@gestalt.dev",
          role: "admin",
        },
      ];
      state.grantsByIdentityID[identity.subjectId] = [];
      state.tokensByIdentityID[identity.subjectId] = [];
      state.managedIntegrationsByIdentityID[identity.subjectId] = createManagedIdentityIntegrations();
      await route.fulfill({ status: 201, json: identity });
      return;
    }
    await route.fallback();
  });

  await page.route("**/api/v1/authorization/subjects/**", async (route, request) => {
    const parts = new URL(request.url()).pathname.split("/").filter(Boolean);
    const identityID = decodeURIComponent(parts[4]);
    const identity = state.identities.find((item) => item.subjectId === identityID);
    if (!identity) {
      await route.fulfill({ status: 404, json: { error: "identity not found" } });
      return;
    }

    if (parts.length === 5) {
      if (request.method() === "GET") {
        await route.fulfill({ json: identity });
        return;
      }
      if (request.method() === "PATCH") {
        const body = request.postDataJSON() as { displayName: string };
        identity.displayName = body.displayName;
        identity.updatedAt = isoDate(30);
        await route.fulfill({ json: identity });
        return;
      }
      if (request.method() === "DELETE") {
        state.identities = state.identities.filter((item) => item.subjectId !== identityID);
        await route.fulfill({ json: { status: "deleted" } });
        return;
      }
    }

    if (parts[5] === "members") {
      if (parts.length === 6 && request.method() === "GET") {
        await route.fulfill({ json: state.membersByIdentityID[identityID] || [] });
        return;
      }
      if (parts.length === 6 && request.method() === "PUT") {
        const body = request.postDataJSON() as { email: string; role: ManagedIdentityMember["role"] };
        const nextMembers = [...(state.membersByIdentityID[identityID] || [])];
        const existingIndex = nextMembers.findIndex((member) => member.email === body.email);
        const nextMember: ManagedIdentityMember = {
          subjectId: `user:${body.email}`,
          email: body.email,
          role: body.role,
        };
        if (existingIndex >= 0) nextMembers.splice(existingIndex, 1, nextMember);
        else nextMembers.push(nextMember);
        state.membersByIdentityID[identityID] = nextMembers;
        await route.fulfill({ json: nextMember });
        return;
      }
      if (parts.length === 7 && request.method() === "DELETE") {
        const memberSubjectID = decodeURIComponent(parts[6]);
        state.membersByIdentityID[identityID] =
          (state.membersByIdentityID[identityID] || []).filter((member) => member.subjectId !== memberSubjectID);
        await route.fulfill({ json: { status: "deleted" } });
        return;
      }
    }

    if (parts[5] === "grants") {
      if (parts.length === 6 && request.method() === "GET") {
        await route.fulfill({ json: state.grantsByIdentityID[identityID] || [] });
        return;
      }
      if (parts.length === 7 && request.method() === "PUT") {
        const plugin = decodeURIComponent(parts[6]);
        const body = request.postDataJSON() as { role: ManagedIdentityGrant["role"] };
        const nextGrant: ManagedIdentityGrant = {
          plugin,
          role: body.role,
          source: "dynamic",
          mutable: true,
        };
        const nextGrants = [...(state.grantsByIdentityID[identityID] || []).filter((grant) => grant.plugin !== plugin), nextGrant];
        state.grantsByIdentityID[identityID] = nextGrants;
        await route.fulfill({
          status: opts?.wrapGrantResponse ? 202 : 200,
          json: opts?.wrapGrantResponse
            ? { status: "persisted_pending_reload", grant: nextGrant, reloaded: false }
            : nextGrant,
        });
        return;
      }
      if (parts.length === 7 && request.method() === "DELETE") {
        const plugin = decodeURIComponent(parts[6]);
        state.grantsByIdentityID[identityID] =
          (state.grantsByIdentityID[identityID] || []).filter((grant) => grant.plugin !== plugin);
        await route.fulfill({ json: { status: "deleted" } });
        return;
      }
    }

    if (parts[5] === "tokens") {
      if (parts.length === 6 && request.method() === "GET") {
        await route.fulfill({ json: state.tokensByIdentityID[identityID] || [] });
        return;
      }
      if (parts.length === 6 && request.method() === "POST") {
        const body = request.postDataJSON() as {
          name: string;
          permissions?: AccessPermission[];
        };
        opts?.onCreateToken?.(body as Record<string, unknown>);
        const token: APIToken = {
          id: `tok-${(state.tokensByIdentityID[identityID] || []).length + 1}`,
          name: body.name,
          permissions: body.permissions,
          createdAt: isoDate(60),
        };
        state.tokensByIdentityID[identityID] = [...(state.tokensByIdentityID[identityID] || []), token];
        await route.fulfill({
          status: 201,
          json: {
            id: token.id,
            name: token.name,
            token: "gst_api_identity_secret",
            permissions: token.permissions,
          },
        });
        return;
      }
      if (parts.length === 7 && request.method() === "DELETE") {
        const tokenID = decodeURIComponent(parts[6]);
        state.tokensByIdentityID[identityID] =
          (state.tokensByIdentityID[identityID] || []).filter((token) => token.id !== tokenID);
        await route.fulfill({ json: { status: "revoked" } });
        return;
      }
    }

    if (parts[5] === "integrations") {
      if (parts.length === 6 && request.method() === "GET") {
        await route.fulfill({ json: state.managedIntegrationsByIdentityID[identityID] || [] });
        return;
      }
      if (parts.length === 7 && request.method() === "DELETE") {
        const integration = decodeURIComponent(parts[6]);
        updateManagedIdentityIntegrationConnection(state, identityID, integration, false);
        opts?.onManagedDisconnect?.(integration);
        await route.fulfill({ json: { status: "deleted" } });
        return;
      }
    }

    if (parts[5] === "auth") {
      if (parts[6] === "start-oauth" && request.method() === "POST") {
        await route.fulfill({
          json: {
            state: "managed-oauth-state",
            url: "https://oauth.example.test/authorize",
          },
        });
        return;
      }
      if (parts[6] === "connect-manual" && request.method() === "POST") {
        const body = request.postDataJSON() as Record<string, unknown>;
        const integration = String(body.integration || "");
        updateManagedIdentityIntegrationConnection(state, identityID, integration, true);
        opts?.onManagedConnect?.(body);
        await route.fulfill({ json: { status: "connected", integration } });
        return;
      }
    }

    await route.fallback();
  });

}

function createManagedIdentityIntegrations(slackConnected = false): Integration[] {
  return [
    {
      name: "github",
      displayName: "GitHub",
      description: "Repository and workflow operations",
      status: "needs_user_connection",
      credentialState: "missing",
      healthState: "not_checked",
      actions: ["connect"],
      connections: [{
        name: "plugin",
        authTypes: ["manual"],
        credentialFields: [{ name: "token", label: "GitHub token" }],
        status: "needs_user_connection",
        credentialState: "missing",
        healthState: "not_checked",
        actions: ["connect"],
      }],
    },
    {
      name: "slack",
      displayName: "Slack",
      description: "Workspace chat integration",
      status: slackConnected ? "ready" : "needs_user_connection",
      credentialState: slackConnected ? "connected" : "missing",
      healthState: "not_checked",
      actions: slackConnected ? ["disconnect"] : ["connect"],
      connections: [{
        name: "plugin",
        authTypes: ["manual"],
        credentialFields: [{ name: "token", label: "Bot token" }],
        status: slackConnected ? "ready" : "needs_user_connection",
        credentialState: slackConnected ? "connected" : "missing",
        healthState: "not_checked",
        actions: slackConnected ? ["disconnect"] : ["connect"],
        instances: slackConnected ? [{ name: "default", connection: "plugin" }] : [],
      }],
    },
  ];
}

function updateManagedIdentityIntegrationConnection(
  state: IdentityState,
  identityID: string,
  integrationName: string,
  connected: boolean,
) {
  state.managedIntegrationsByIdentityID[identityID] =
    (state.managedIntegrationsByIdentityID[identityID] || []).map((integration) =>
      integration.name === integrationName
        ? {
            ...integration,
            status: connected ? "ready" : "needs_user_connection",
            credentialState: connected ? "connected" : "missing",
            actions: connected ? ["disconnect"] : ["connect"],
            connections: integration.connections?.map((connection) => ({
              ...connection,
              status: connected ? "ready" : "needs_user_connection",
              credentialState: connected ? "connected" : "missing",
              actions: connected ? ["disconnect"] : ["connect"],
              instances: connected ? [{ name: "default", connection: connection.name }] : [],
            })),
          }
        : integration,
    );
}

function createBaseState(role: ManagedIdentityMember["role"]): IdentityState {
  const subjectId = "service_account:agent-1";
  return {
    identities: [
      {
        id: "agent-1",
        subjectId,
        kind: "service_account",
        displayName: "Release Bot",
        credentialSubjectId: subjectId,
        createdBySubjectId: "user:test@gestalt.dev",
        createdAt: isoDate(0),
        updatedAt: isoDate(1),
      },
    ],
    membersByIdentityID: {
      [subjectId]: [
        {
          subjectId: "user:test@gestalt.dev",
          email: "test@gestalt.dev",
          role,
        },
      ],
    },
    grantsByIdentityID: {
      [subjectId]: [
        {
          plugin: "slack",
          role: "viewer",
          source: "dynamic",
          mutable: true,
        },
      ],
    },
    tokensByIdentityID: {
      [subjectId]: [],
    },
    visibleIntegrations: [
      {
        name: "github",
        displayName: "GitHub",
        description: "Repository and workflow operations",
      },
      {
        name: "slack",
        displayName: "Slack",
        description: "Workspace chat integration",
      },
    ],
    managedIntegrationsByIdentityID: {
      [subjectId]: createManagedIdentityIntegrations(),
    },
  };
}

test.describe("Managed identities", () => {
  test("lists identities and creates a new one", async ({ authenticatedPage: page }) => {
    const state = createBaseState("admin");
    await mockAuthInfo(page, { provider: "test-sso", displayName: "Test SSO" });
    await wireIdentityRoutes(page, state, { wrapGrantResponse: true });

    await page.goto("/identities");
    await expect(page.getByRole("heading", { name: "Agent Identities" })).toBeVisible();
    await expect(page.getByText("Release Bot")).toBeVisible();

    await page.getByLabel("Display name").fill("Deploy Bot");
    await page.getByRole("button", { name: "Create Identity" }).click();

    await expect(page).toHaveURL(/\/identities\?id=service_account%3Adeploy-bot$/);
    await expect(page.getByRole("heading", { name: "Deploy Bot" })).toBeVisible();
  });

  test("connects and disconnects managed identity plugin credentials", async ({ authenticatedPage: page }) => {
    const state = createBaseState("admin");
    const managedConnectBodies: Record<string, unknown>[] = [];
    const managedDisconnects: string[] = [];
    await mockAuthInfo(page, { provider: "test-sso", displayName: "Test SSO" });
    await wireIdentityRoutes(page, state, {
      onManagedConnect: (body) => managedConnectBodies.push(body),
      onManagedDisconnect: (integration) => managedDisconnects.push(integration),
    });

    await page.goto("/identities?id=agent-1");

    await expect(page.getByRole("heading", { name: "Release Bot" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Slack settings" })).toBeVisible();

    await page.getByRole("button", { name: "Slack settings" }).click();
    await page.getByRole("button", { name: "Connect" }).click();
    await page.getByRole("dialog").getByRole("textbox", { name: "Bot token" }).fill("xoxb-managed-identity");
    await page.getByRole("dialog").getByRole("button", { name: "Submit" }).click();

    await expect.poll(() => managedConnectBodies).toEqual([
      {
        integration: "slack",
        connection: "plugin",
        credential: "xoxb-managed-identity",
        returnPath: "/identities?id=service_account%3Aagent-1",
      },
    ]);
    await expect(page.getByLabel("Connected")).toBeVisible();

    await page.getByRole("button", { name: "Slack settings" }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByRole("button", { name: "Disconnect" }).click();
    await expect(dialog.getByRole("heading", { name: "Disconnect Slack?" })).toBeVisible();
    await dialog.getByRole("button", { name: "Disconnect" }).click();

    await expect.poll(() => managedDisconnects).toEqual(["slack"]);
    await page.getByRole("button", { name: "Slack settings" }).click();
    await expect(page.getByRole("dialog").getByRole("button", { name: "Connect" })).toBeVisible();
  });

  test("returns completed OAuth flows to the managed identity detail page", async ({ authenticatedPage: page }) => {
    const state = createBaseState("admin");
    await mockAuthInfo(page, { provider: "test-sso", displayName: "Test SSO" });
    await wireIdentityRoutes(page, state);

    await page.goto("/identities?id=agent-1");
    await page.evaluate(
      ([key, value]) => window.sessionStorage.setItem(key, value),
      [
        CONNECTION_RETURN_PATH_STORAGE_KEY,
        "/identities?id=service_account%3Aagent-1",
      ],
    );

    await page.goto("/integrations?connected=slack");

    await expect(page).toHaveURL(/\/identities\?id=service_account%3Aagent-1$/);
    await expect(page.getByRole("heading", { name: "Release Bot" })).toBeVisible();
  });

  test("renders a viewer detail page as read-only", async ({ authenticatedPage: page }) => {
    const state = createBaseState("viewer");
    state.tokensByIdentityID["service_account:agent-1"] = [
      {
        id: "tok-1",
        name: "viewer-token",
        permissions: [{ plugin: "slack", operations: ["channels.read"] }],
        createdAt: isoDate(3),
      },
      {
        id: "tok-2",
        name: "action-token",
        permissions: [{ plugin: "github", actions: ["provider_dev.attach"] }],
        createdAt: isoDate(4),
      },
      {
        id: "tok-3",
        name: "mixed-token",
        permissions: [{
          plugin: "github",
          operations: ["issues.read"],
          actions: ["provider_dev.attach"],
        }],
        createdAt: isoDate(5),
      },
    ];
    await mockAuthInfo(page, { provider: "test-sso", displayName: "Test SSO" });
    await wireIdentityRoutes(page, state);

    await page.goto("/identities?id=agent-1");
    await expect(page.getByRole("heading", { name: "Release Bot" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Create Token" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "Delete Identity" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "Add or Update Member" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "Set Grant" })).toHaveCount(0);
    await expect(page.getByRole("cell", { name: "github: actions: provider_dev.attach" })).toBeVisible();
    await expect(page.getByRole("cell", { name: "github: issues.read; actions: provider_dev.attach" })).toBeVisible();
    await page.getByRole("button", { name: "Slack settings" }).click();
    await expect(page.getByRole("dialog").getByRole("button", { name: "Connect" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "Revoke" })).toHaveCount(0);
  });

  test("lets an admin update sharing, grants, and tokens", async ({ authenticatedPage: page }) => {
    const state = createBaseState("admin");
    await mockAuthInfo(page, { provider: "test-sso", displayName: "Test SSO" });
    await wireIdentityRoutes(page, state);

    await page.goto("/identities?id=agent-1");

    await page.getByLabel("Display name").fill("Release Automation");
    await page.getByRole("button", { name: "Rename" }).click();
    await expect(page.getByRole("heading", { name: "Release Automation" })).toBeVisible();

    await page.getByLabel("User email").fill("viewer@example.test");
    await page.getByLabel("Role", { exact: true }).selectOption("viewer");
    await page.getByRole("button", { name: "Add or Update Member" }).click();
    await expect(page.getByRole("cell", { name: "viewer@example.test", exact: true })).toBeVisible();

    await page.getByRole("combobox", { name: "Plugin" }).fill("git");
    await page.getByRole("option", { name: /GitHub/ }).click();
    await page.getByLabel("Grant role").selectOption("viewer");
    await page.getByRole("button", { name: "Set Grant" }).click();
    await expect(page.getByRole("cell", { name: "github" })).toBeVisible();
    const githubGrantRow = page.getByRole("row").filter({
      has: page.getByRole("cell", { name: "github" }),
    });
    await expect(githubGrantRow.getByRole("cell", { name: "viewer" })).toBeVisible();

    await page.getByLabel("Token name").fill("release-token");
    await page.getByRole("radio", { name: /Restrict this token/ }).check();
    await page.getByLabel("Operations for slack").fill("channels.read");
    await page.getByRole("button", { name: "Create Token" }).click();
    await expect(page.getByText("Copy this token now")).toBeVisible();
    await expect(page.getByText("gst_api_identity_secret")).toBeVisible();
    await expect(page.getByText("release-token")).toBeVisible();
  });

  test("lets an admin remove members, grants, tokens, and identities", async ({ authenticatedPage: page }) => {
    const state = createBaseState("admin");
    state.membersByIdentityID["service_account:agent-1"] = [
      ...state.membersByIdentityID["service_account:agent-1"],
      {
        subjectId: "user:viewer@example.test",
        email: "viewer@example.test",
        role: "viewer",
      },
    ];
    state.tokensByIdentityID["service_account:agent-1"] = [
      {
        id: "tok-1",
        name: "cleanup-token",
        permissions: [{ plugin: "slack" }],
        createdAt: isoDate(3),
      },
    ];

    page.on("dialog", (dialog) => dialog.accept());
    await mockAuthInfo(page, { provider: "test-sso", displayName: "Test SSO" });
    await wireIdentityRoutes(page, state);

    await page.goto("/identities?id=agent-1");

    const memberRow = page.getByRole("row").filter({
      has: page.getByRole("cell", { name: "viewer@example.test", exact: true }),
    });
    await memberRow.getByRole("button", { name: "Remove" }).click();
    await expect(page.getByRole("cell", { name: "viewer@example.test", exact: true })).toHaveCount(0);

    const grantRow = page.getByRole("row").filter({
      has: page.getByRole("cell", { name: "slack", exact: true }),
    });
    await grantRow.getByRole("button", { name: "Remove" }).click();
    await expect(page.getByRole("row").filter({
      has: page.getByRole("cell", { name: "slack", exact: true }),
    })).toHaveCount(0);

    const tokenRow = page.getByRole("row").filter({
      has: page.getByRole("cell", { name: "cleanup-token", exact: true }),
    });
    await tokenRow.getByRole("button", { name: "Revoke" }).click();
    await expect(page.getByRole("cell", { name: "cleanup-token", exact: true })).toHaveCount(0);

    await page.getByRole("button", { name: "Delete Identity" }).click();
    await expect(page).toHaveURL(/\/identities$/);
    await expect(page.getByText("Release Bot")).toHaveCount(0);
  });

  test("creates all-authorized tokens without visible plugin grants", async ({ authenticatedPage: page }) => {
    const state = createBaseState("admin");
    state.grantsByIdentityID["service_account:agent-1"] = [];
    let createTokenBody: Record<string, unknown> | null = null;

    await mockAuthInfo(page, { provider: "test-sso", displayName: "Test SSO" });
    await wireIdentityRoutes(page, state, {
      onCreateToken: (body) => {
        createTokenBody = body;
      },
    });

    await page.goto("/identities?id=agent-1");
    await expect(page.getByRole("radio", { name: /All authorized access/ })).toBeChecked();
    await page.getByLabel("Token name").fill("brain-ingest");
    await page.getByRole("button", { name: "Create Token" }).click();

    await expect.poll(() => createTokenBody).toEqual({
      name: "brain-ingest",
    });
    const tokenRow = page.getByRole("row").filter({
      has: page.getByRole("cell", { name: "brain-ingest", exact: true }),
    });
    await expect(tokenRow.getByRole("cell", { name: "All authorized access" })).toBeVisible();
  });

  test("ignores stale token limits when all-authorized access is selected", async ({ authenticatedPage: page }) => {
    const state = createBaseState("admin");
    let createTokenBody: Record<string, unknown> | null = null;

    await mockAuthInfo(page, { provider: "test-sso", displayName: "Test SSO" });
    await wireIdentityRoutes(page, state, {
      onCreateToken: (body) => {
        createTokenBody = body;
      },
    });

    await page.goto("/identities?id=agent-1");
    await page.getByLabel("Token name").fill("all-token");
    await page.getByRole("radio", { name: /Restrict this token/ }).check();
    await page.getByLabel("Operations for slack").fill("channels.read");
    await page.getByRole("radio", { name: /All authorized access/ }).check();
    await page.getByRole("button", { name: "Create Token" }).click();

    await expect.poll(() => createTokenBody).toEqual({
      name: "all-token",
    });
  });

  test("does not post empty permissions in restricted mode", async ({ authenticatedPage: page }) => {
    const state = createBaseState("admin");
    let createTokenBody: Record<string, unknown> | null = null;

    await mockAuthInfo(page, { provider: "test-sso", displayName: "Test SSO" });
    await wireIdentityRoutes(page, state, {
      onCreateToken: (body) => {
        createTokenBody = body;
      },
    });

    await page.goto("/identities?id=agent-1");
    await page.getByLabel("Token name").fill("empty-restricted-token");
    await page.getByRole("radio", { name: /Restrict this token/ }).check();
    await page.getByRole("button", { name: "Create Token" }).click();

    await expect(page.getByText("Choose at least one token limit")).toBeVisible();
    expect(createTokenBody).toBeNull();
  });

  test("allows operation-scoped tokens from role-based plugin grants", async ({ authenticatedPage: page }) => {
    const state = createBaseState("admin");
    state.grantsByIdentityID["service_account:agent-1"] = [
      {
        plugin: "github",
        role: "viewer",
        source: "dynamic",
        mutable: true,
      },
    ];
    let createTokenBody: Record<string, unknown> | null = null;

    await mockAuthInfo(page, { provider: "test-sso", displayName: "Test SSO" });
    await wireIdentityRoutes(page, state, {
      onCreateToken: (body) => {
        createTokenBody = body;
      },
    });

    await page.goto("/identities?id=agent-1");
    await page.getByLabel("Token name").fill("github-token");
    await page.getByRole("radio", { name: /Restrict this token/ }).check();
    await page.getByLabel("Operations for github").fill("issues.read, repos.read");
    await page.getByRole("button", { name: "Create Token" }).click();

    await expect.poll(() => createTokenBody).toEqual({
      name: "github-token",
      permissions: [
        {
          plugin: "github",
          operations: ["issues.read", "repos.read"],
        },
      ],
    });
    await expect(page.getByText("gst_api_identity_secret")).toBeVisible();
  });

  test("allows plugin-level tokens from role-based plugin grants", async ({ authenticatedPage: page }) => {
    const state = createBaseState("admin");
    let createTokenBody: Record<string, unknown> | null = null;

    await mockAuthInfo(page, { provider: "test-sso", displayName: "Test SSO" });
    await wireIdentityRoutes(page, state, {
      onCreateToken: (body) => {
        createTokenBody = body;
      },
    });

    await page.goto("/identities?id=agent-1");
    await page.getByLabel("Token name").fill("slack-token");
    await page.getByRole("radio", { name: /Restrict this token/ }).check();
    await page.getByRole("checkbox", { name: "Limit this token to all authorized slack operations" }).check();
    await page.getByRole("button", { name: "Create Token" }).click();

    await expect.poll(() => createTokenBody).toEqual({
      name: "slack-token",
      permissions: [
        {
          plugin: "slack",
        },
      ],
    });
    await expect(page.getByText("gst_api_identity_secret")).toBeVisible();
  });
});
