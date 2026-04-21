import { expect, mockAuthInfo, test } from "./fixtures";
import type {
  APIToken,
  Integration,
  IntegrationOperation,
  ManagedIdentity,
  ManagedIdentityGrant,
  ManagedIdentityMember,
} from "../src/lib/api";

type IdentityState = {
  identities: ManagedIdentity[];
  membersByIdentityID: Record<string, ManagedIdentityMember[]>;
  grantsByIdentityID: Record<string, ManagedIdentityGrant[]>;
  tokensByIdentityID: Record<string, APIToken[]>;
  integrationsByIdentityID: Record<string, Integration[]>;
  visibleIntegrations: Integration[];
  operationsByIntegrationName: Record<string, IntegrationOperation[]>;
};

function isoDate(days: number): string {
  const date = new Date(Date.UTC(2026, 3, 15 + days, 12, 0, 0));
  return date.toISOString();
}

async function wireIdentityRoutes(
  page: import("@playwright/test").Page,
  state: IdentityState,
  opts?: {
    onStartOAuth?: (body: Record<string, unknown>) => void;
    onCreateToken?: (body: Record<string, unknown>) => void;
    integrationsRouteMode?: "json" | "html-fallback";
  },
) {
  await page.route("**/api/v1/integrations", async (route, request) => {
    if (request.method() === "GET") {
      await route.fulfill({ json: state.visibleIntegrations });
      return;
    }
    await route.fallback();
  });

  await page.route("**/api/v1/integrations/**", async (route, request) => {
    const url = new URL(request.url());
    const parts = url.pathname.split("/").filter(Boolean);
    if (parts.length === 5 && parts[4] === "operations" && request.method() === "GET") {
      const integration = decodeURIComponent(parts[3]);
      await route.fulfill({ json: state.operationsByIntegrationName[integration] || [] });
      return;
    }
    await route.fallback();
  });

  await page.route("**/api/v1/identities", async (route, request) => {
    if (request.method() === "GET") {
      await route.fulfill({ json: state.identities });
      return;
    }
    if (request.method() === "POST") {
      const body = request.postDataJSON() as { displayName: string };
      const identity: ManagedIdentity = {
        id: `agent-${state.identities.length + 1}`,
        displayName: body.displayName,
        role: "admin",
        createdAt: isoDate(10 + state.identities.length),
        updatedAt: isoDate(10 + state.identities.length),
      };
      state.identities = [...state.identities, identity];
      state.membersByIdentityID[identity.id] = [];
      state.grantsByIdentityID[identity.id] = [];
      state.tokensByIdentityID[identity.id] = [];
      state.integrationsByIdentityID[identity.id] = [];
      await route.fulfill({ status: 201, json: identity });
      return;
    }
    await route.fallback();
  });

  await page.route("**/api/v1/identities/**", async (route, request) => {
    const url = new URL(request.url());
    const parts = url.pathname.split("/").filter(Boolean);
    const identityID = parts[3];
    const identity = state.identities.find((item) => item.id === identityID);
    if (!identity) {
      await route.fulfill({ status: 404, json: { error: "identity not found" } });
      return;
    }

    if (parts.length === 4) {
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
        state.identities = state.identities.filter((item) => item.id !== identityID);
        await route.fulfill({ json: { status: "deleted" } });
        return;
      }
    }

    if (parts[4] === "members") {
      if (parts.length === 5 && request.method() === "GET") {
        await route.fulfill({ json: state.membersByIdentityID[identityID] || [] });
        return;
      }
      if (parts.length === 5 && request.method() === "PUT") {
        const body = request.postDataJSON() as { email: string; role: ManagedIdentityMember["role"] };
        const now = isoDate(40);
        const nextMembers = [...(state.membersByIdentityID[identityID] || [])];
        const existingIndex = nextMembers.findIndex((member) => member.email === body.email);
        const nextMember: ManagedIdentityMember = {
          userId: body.email,
          email: body.email,
          role: body.role,
          createdAt: existingIndex >= 0 ? nextMembers[existingIndex].createdAt : now,
          updatedAt: now,
        };
        if (existingIndex >= 0) nextMembers.splice(existingIndex, 1, nextMember);
        else nextMembers.push(nextMember);
        state.membersByIdentityID[identityID] = nextMembers;
        await route.fulfill({ json: nextMember });
        return;
      }
      if (parts.length === 6 && request.method() === "DELETE") {
        const email = decodeURIComponent(parts[5]);
        state.membersByIdentityID[identityID] =
          (state.membersByIdentityID[identityID] || []).filter((member) => member.email !== email);
        await route.fulfill({ json: { status: "deleted" } });
        return;
      }
    }

    if (parts[4] === "grants") {
      if (parts.length === 5 && request.method() === "GET") {
        await route.fulfill({ json: state.grantsByIdentityID[identityID] || [] });
        return;
      }
      if (parts.length === 6 && request.method() === "PUT") {
        const plugin = decodeURIComponent(parts[5]);
        const body = request.postDataJSON() as { operations?: string[] };
        const now = isoDate(50);
        const nextGrant: ManagedIdentityGrant = {
          plugin,
          operations: body.operations?.length ? body.operations : undefined,
          createdAt: now,
          updatedAt: now,
        };
        const nextGrants = [...(state.grantsByIdentityID[identityID] || []).filter((grant) => grant.plugin !== plugin), nextGrant];
        state.grantsByIdentityID[identityID] = nextGrants;
        await route.fulfill({ json: nextGrant });
        return;
      }
      if (parts.length === 6 && request.method() === "DELETE") {
        const plugin = decodeURIComponent(parts[5]);
        state.grantsByIdentityID[identityID] =
          (state.grantsByIdentityID[identityID] || []).filter((grant) => grant.plugin !== plugin);
        await route.fulfill({ json: { status: "deleted" } });
        return;
      }
    }

    if (parts[4] === "tokens") {
      if (parts.length === 5 && request.method() === "GET") {
        await route.fulfill({ json: state.tokensByIdentityID[identityID] || [] });
        return;
      }
      if (parts.length === 5 && request.method() === "POST") {
        const body = request.postDataJSON() as {
          name: string;
          permissions: { plugin: string; operations?: string[] }[];
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
      if (parts.length === 6 && request.method() === "DELETE") {
        const tokenID = decodeURIComponent(parts[5]);
        state.tokensByIdentityID[identityID] =
          (state.tokensByIdentityID[identityID] || []).filter((token) => token.id !== tokenID);
        await route.fulfill({ json: { status: "revoked" } });
        return;
      }
    }

    if (parts[4] === "integrations" && parts.length === 5 && request.method() === "GET") {
      if (opts?.integrationsRouteMode === "html-fallback") {
        await route.fulfill({
          contentType: "text/html; charset=utf-8",
          body: "<!DOCTYPE html><html><body><h1>fallback shell</h1></body></html>",
        });
        return;
      }
      await route.fulfill({ json: state.integrationsByIdentityID[identityID] || [] });
      return;
    }

    if (parts[4] === "integrations" && parts.length === 6 && request.method() === "DELETE") {
      await route.fulfill({ json: { status: "disconnected" } });
      return;
    }

    if (parts[4] === "auth" && parts[5] === "start-oauth" && request.method() === "POST") {
      const body = request.postDataJSON() as Record<string, unknown>;
      opts?.onStartOAuth?.(body);
      await route.fulfill({
        json: {
          url: "/oauth-handler",
          state: "oauth-state",
        },
      });
      return;
    }

    if (parts[4] === "auth" && parts[5] === "connect-manual" && request.method() === "POST") {
      await route.fulfill({ json: { status: "connected" } });
      return;
    }

    await route.fallback();
  });

  await page.route("**/oauth-handler", async (route) => {
    await route.fulfill({
      contentType: "text/html",
      body: "<html><body><h1>OAuth redirect</h1></body></html>",
    });
  });
}

function createBaseState(role: ManagedIdentity["role"]): IdentityState {
  return {
    identities: [
      {
        id: "agent-1",
        displayName: "Release Bot",
        role,
        createdAt: isoDate(0),
        updatedAt: isoDate(1),
      },
    ],
    membersByIdentityID: {
      "agent-1": [
        {
          userId: "admin@example.test",
          email: "admin@example.test",
          role: "admin",
          createdAt: isoDate(0),
          updatedAt: isoDate(1),
        },
      ],
    },
    grantsByIdentityID: {
      "agent-1": [
        {
          plugin: "slack",
          operations: ["channels.read", "users.read"],
          createdAt: isoDate(2),
          updatedAt: isoDate(2),
        },
      ],
    },
    tokensByIdentityID: {
      "agent-1": [],
    },
    integrationsByIdentityID: {
      "agent-1": [
        {
          name: "slack",
          displayName: "Slack",
          description: "Workspace chat integration",
          mountedPath: "/integrations/slack",
          authTypes: ["oauth"],
        },
      ],
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
    operationsByIntegrationName: {
      github: [
        { id: "issues.read", title: "List issues" },
        { id: "repos.read", title: "Read repository metadata" },
      ],
      slack: [
        { id: "channels.read", title: "List channels" },
        { id: "users.read", title: "List users" },
      ],
    },
  };
}

test.describe("Managed identities", () => {
  test("lists identities and creates a new one", async ({ authenticatedPage: page }) => {
    const state = createBaseState("admin");
    await mockAuthInfo(page, { provider: "test-sso", displayName: "Test SSO" });
    await wireIdentityRoutes(page, state);

    await page.goto("/identities");
    await expect(page.getByRole("heading", { name: "Agent Identities" })).toBeVisible();
    await expect(page.getByText("Release Bot")).toBeVisible();

    await page.getByLabel("Display name").fill("Deploy Bot");
    await page.getByRole("button", { name: "Create Identity" }).click();

    await expect(page).toHaveURL(/\/identities\?id=agent-2$/);
    await expect(page.getByRole("heading", { name: "Deploy Bot" })).toBeVisible();
  });

  test("creates an identity even when managed identity connection APIs are unavailable", async ({ authenticatedPage: page }) => {
    const state = createBaseState("admin");
    await mockAuthInfo(page, { provider: "test-sso", displayName: "Test SSO" });
    await wireIdentityRoutes(page, state, { integrationsRouteMode: "html-fallback" });

    await page.goto("/identities");
    await page.getByLabel("Display name").fill("Deploy Bot");
    await page.getByRole("button", { name: "Create Identity" }).click();

    await expect(page).toHaveURL(/\/identities\?id=agent-2$/);
    await expect(page.getByRole("heading", { name: "Deploy Bot" })).toBeVisible();
    await expect(page.getByText("Managed identity plugin connections are unavailable on this server.")).toBeVisible();
    await expect(page.getByText("Unexpected token '<'")).toHaveCount(0);
  });

  test("renders a viewer detail page as read-only except for token creation", async ({ authenticatedPage: page }) => {
    const state = createBaseState("viewer");
    state.tokensByIdentityID["agent-1"] = [
      {
        id: "tok-1",
        name: "viewer-token",
        permissions: [{ plugin: "slack", operations: ["channels.read"] }],
        createdAt: isoDate(3),
      },
    ];
    state.integrationsByIdentityID["agent-1"] = [
      {
        name: "slack",
        displayName: "Slack",
        description: "Workspace chat integration",
        connected: true,
        instances: [
          {
            name: "workspace-a",
            connection: "primary",
          },
        ],
        mountedPath: "/integrations/slack",
        authTypes: ["oauth"],
      },
    ];

    await mockAuthInfo(page, { provider: "test-sso", displayName: "Test SSO" });
    await wireIdentityRoutes(page, state);

    await page.goto("/identities?id=agent-1");
    await expect(page.getByRole("heading", { name: "Release Bot" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Create Token" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Delete Identity" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "Add or Update Member" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "Set Grant" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "Slack settings" })).toHaveCount(1);
    await expect(page.getByRole("button", { name: "Revoke" })).toHaveCount(0);
    await page.getByRole("button", { name: "Slack settings" }).click();
    await expect(page.getByText("workspace-a")).toBeVisible();
    await expect(page.getByText("primary")).toBeVisible();
    await expect(page.getByRole("button", { name: "Disconnect" })).toHaveCount(0);
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
    await page.getByLabel("Role").selectOption("viewer");
    await page.getByRole("button", { name: "Add or Update Member" }).click();
    await expect(page.getByText("viewer@example.test")).toBeVisible();

    await expect(page.getByLabel("Operations")).toBeDisabled();
    await page.getByLabel("Plugin").fill("git");
    await page.getByRole("option", { name: /GitHub/ }).click();
    await expect(page.getByLabel("Operations")).toBeEnabled();
    await page.getByLabel("Operations").click();
    await page.getByLabel("Filter operations").fill("read");
    await page.getByLabel("repos.read").check();
    await page.getByLabel("issues.read").check();
    await page.getByRole("button", { name: "Set Grant" }).click();
    await expect(page.getByRole("cell", { name: "github" })).toBeVisible();
    await expect(page.getByRole("cell", { name: "issues.read, repos.read" })).toBeVisible();

    await page.getByLabel("Token name").fill("release-token");
    await page.getByLabel("channels.read").check();
    await page.getByRole("button", { name: "Create Token" }).click();
    await expect(page.getByText("Copy this token now")).toBeVisible();
    await expect(page.getByText("gst_api_identity_secret")).toBeVisible();
    await expect(page.getByText("release-token")).toBeVisible();
  });

  test("starts identity-scoped OAuth with the identity detail return path", async ({ authenticatedPage: page }) => {
    const state = createBaseState("editor");
    let oauthBody: Record<string, unknown> | null = null;

    await mockAuthInfo(page, { provider: "test-sso", displayName: "Test SSO" });
    await wireIdentityRoutes(page, state, {
      onStartOAuth: (body) => {
        oauthBody = body;
      },
    });

    await page.goto("/identities?id=agent-1");
    await page.getByRole("button", { name: "Slack settings" }).click();
    await page.getByRole("button", { name: "Connect" }).click();

    await expect.poll(() => oauthBody?.returnPath).toBe("/identities?id=agent-1");
    await expect(page).toHaveURL(/\/oauth-handler$/);
  });

  test("does not navigate identity connections into user-scoped plugin pages", async ({ authenticatedPage: page }) => {
    const state = createBaseState("editor");

    await mockAuthInfo(page, { provider: "test-sso", displayName: "Test SSO" });
    await wireIdentityRoutes(page, state);

    await page.goto("/identities?id=agent-1");
    await page.getByTestId("integration-card-slack").click();

    await expect(page).toHaveURL(/\/identities\?id=agent-1$/);
    await expect(page.getByRole("heading", { name: "Release Bot" })).toBeVisible();
  });

  test("allows operation-scoped tokens from plugin-wide grants", async ({ authenticatedPage: page }) => {
    const state = createBaseState("viewer");
    state.grantsByIdentityID["agent-1"] = [
      {
        plugin: "github",
        createdAt: isoDate(4),
        updatedAt: isoDate(4),
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

  test("allows plugin-level tokens from operation-scoped grants", async ({ authenticatedPage: page }) => {
    const state = createBaseState("viewer");
    let createTokenBody: Record<string, unknown> | null = null;

    await mockAuthInfo(page, { provider: "test-sso", displayName: "Test SSO" });
    await wireIdentityRoutes(page, state, {
      onCreateToken: (body) => {
        createTokenBody = body;
      },
    });

    await page.goto("/identities?id=agent-1");
    await page.getByLabel("Token name").fill("slack-token");
    await page.getByLabel("Grant full access to slack").check();
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
