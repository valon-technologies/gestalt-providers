import {
  expect,
  mockAuthInfo,
  mockIntegrations,
  test,
} from "./fixtures";

type Integration = {
  name: string;
  displayName?: string;
  description?: string;
  managementPath?: string;
};

type AdminMember = {
  email?: string;
  role: string;
  source?: string;
  mutable?: boolean;
  effective?: boolean;
  selectorKind: string;
  selectorValue: string;
  subjectId?: string;
};

type AdminState = {
  defaultRole: string;
  membersByApp: Record<string, AdminMember[]>;
};

function app(name: string, displayName: string): Integration {
  return {
    name,
    displayName,
    description: displayName,
    managementPath: `/apps/${name}/admin`,
  };
}

function member(partial: AdminMember): AdminMember {
  return {
    source: "dynamic",
    mutable: true,
    effective: true,
    ...partial,
  };
}

function registryApp(name: string) {
  return {
    app: name,
    registry: "example-registry",
    desiredVersion: "1.0.0",
    rollout: {
      version: "1.0.0",
      state: "complete",
      createdAt: "2026-08-13T00:00:00Z",
      enrollmentEndsAt: "2026-08-13T00:05:00Z",
      deadline: "2026-08-13T00:10:00Z",
    },
    cohort: {
      acknowledged: 1,
      materialized: 1,
      restarted: 1,
      failed: 0,
    },
    fleetState: {
      state: "healthy",
      sourceVersion: "abc",
      desiredVersion: "1.0.0",
      minimumHealthyInstances: 1,
      liveInstances: 1,
      runningDesiredVersion: 1,
      mismatched: 0,
      errors: 0,
      heartbeatTtlSeconds: 45,
      evaluatedAt: "2026-08-13T00:00:00Z",
    },
  };
}

async function mockRegistryApps(
  page: import("@playwright/test").Page,
  apps: ReturnType<typeof registryApp>[],
) {
  await page.route(
    (url) => url.pathname === "/admin/api/v1/registry-apps",
    async (route, request) => {
      if (request.method() !== "GET") {
        await route.fallback();
        return;
      }
      await route.fulfill({ json: apps });
    },
  );
  await page.route(
    (url) => url.pathname.startsWith("/admin/api/v1/registry-apps/"),
    async (route, request) => {
      if (request.method() !== "GET") {
        await route.fallback();
        return;
      }
      const name = decodeURIComponent(new URL(request.url()).pathname.split("/").pop() ?? "");
      const match = apps.find((item) => item.app === name);
      if (!match) {
        await route.fulfill({ status: 404, json: { error: "not found" } });
        return;
      }
      await route.fulfill({
        json: {
          ...match,
          knownVersions: [],
          freshReplicas: [],
          staleReplicas: [],
        },
      });
    },
  );
}

async function mockGestaltAdmin(
  page: import("@playwright/test").Page,
  allowed: boolean,
) {
  await page.route(
    (url) => url.pathname === "/admin/api/v1/app-registries",
    async (route, request) => {
      if (request.method() !== "GET") {
        await route.fallback();
        return;
      }
      if (!allowed) {
        await route.fulfill({ status: 403, json: { error: "forbidden" } });
        return;
      }
      await route.fulfill({ json: [] });
    },
  );
}

async function wireAdminAccess(
  page: import("@playwright/test").Page,
  state: AdminState,
) {
  await mockGestaltAdmin(page, true);
  await page.route(
    (url) =>
      url.pathname === "/api/v2/authorization/models/active/resource-types",
    async (route, request) => {
      if (request.method() !== "GET") {
        await route.fallback();
        return;
      }
      await route.fulfill({
        json: {
          resourceTypes: [
            { name: "app", defaultRole: state.defaultRole },
          ],
        },
      });
    },
  );

  await page.route("**/api/v1/apps/*/admin/members", async (route, request) => {
    if (request.method() !== "GET") {
      await route.fallback();
      return;
    }
    const parts = new URL(request.url()).pathname.split("/").filter(Boolean);
    const appName = decodeURIComponent(parts[3] || "");
    await route.fulfill({ json: state.membersByApp[appName] ?? [] });
  });

  await page.route(
    "**/api/v2/authorization/relationships:delete",
    async (route, request) => {
      if (request.method() !== "POST") {
        await route.fallback();
        return;
      }
      const body = request.postDataJSON() as {
        relationshipTuple?: {
          resource?: { id?: string };
          target?: {
            subject?: { id?: string };
            subjectSet?: { resource?: { type?: string; id?: string }; relation?: string };
          };
        };
      };
      const tuple = body.relationshipTuple;
      const appName = tuple?.resource?.id ?? "";
      const subjectId = tuple?.target?.subject?.id;
      const groupId = tuple?.target?.subjectSet?.resource?.id;
      const groupType = tuple?.target?.subjectSet?.resource?.type;
      const groupRelation = tuple?.target?.subjectSet?.relation;
      const selector = subjectId
        ? subjectId
        : groupType && groupId
          ? `${groupType}:${groupId}${groupRelation ? `#${groupRelation}` : ""}`
          : "";
      state.membersByApp[appName] = (state.membersByApp[appName] ?? []).filter(
        (row) => row.selectorValue !== selector,
      );
      await route.fulfill({ json: {} });
    },
  );

  await page.route("**/api/v2/authorization/relationships", async (route, request) => {
    if (request.method() !== "POST") {
      await route.fallback();
      return;
    }
    const body = request.postDataJSON() as {
      relationship?: {
        tuple?: {
          resource?: { id?: string };
          relation?: string;
          target?: {
            subject?: { id?: string };
            subjectSet?: { resource?: { type?: string; id?: string }; relation?: string };
          };
        };
      };
    };
    const tuple = body.relationship?.tuple;
    const appName = tuple?.resource?.id ?? "";
    const role = tuple?.relation || "viewer";
    const subjectId = tuple?.target?.subject?.id;
    const group = tuple?.target?.subjectSet;
    let row: AdminMember;
    if (group?.resource?.id) {
      const selector = `${group.resource.type}:${group.resource.id}${
        group.relation ? `#${group.relation}` : ""
      }`;
      row = member({
        role,
        selectorKind: "subject_set",
        selectorValue: selector,
      });
    } else {
      const email = (subjectId || "").replace(/^user:/, "");
      row = member({
        role,
        selectorKind: "subject_id",
        selectorValue: subjectId || `user:${email}`,
        subjectId: subjectId || `user:${email}`,
        email,
      });
    }
    const existing = state.membersByApp[appName] ?? [];
    state.membersByApp[appName] = [
      ...existing.filter((item) => item.selectorValue !== row.selectorValue),
      row,
    ];
    await route.fulfill({ json: { tuple } });
  });
}

test.describe("Admin app access", () => {
  test("lists apps with group badges, people avatars, and No one", async ({
    authenticatedPage: page,
  }) => {
    const state: AdminState = {
      defaultRole: "",
      membersByApp: {
        slack: [
          member({
            role: "viewer",
            selectorKind: "subject_set",
            selectorValue: "group:eng#member",
          }),
          member({
            role: "viewer",
            selectorKind: "subject_id",
            selectorValue: "user:contractor@example.com",
            email: "contractor@example.com",
            subjectId: "user:contractor@example.com",
          }),
        ],
        httpbin: [],
      },
    };
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockIntegrations(page, [
      app("slack", "Slack"),
      app("httpbin", "HTTPBin"),
    ]);
    await wireAdminAccess(page, state);

    await page.goto("/admin");
    await expect(
      page.getByRole("heading", { name: "App access" }),
    ).toBeVisible();
    await expect(page.getByTestId("admin-nav-who-can-use")).toBeVisible();
    await expect(page.getByTestId("admin-nav-platform-admins")).toBeVisible();
    await expect(page.getByTestId("admin-nav-versions")).toBeVisible();
    await expect(page.getByTestId("admin-nav-metrics")).toBeVisible();
    await expect(
      page.getByText(
        "Set who can use each app: everyone, specific people and groups, or no one.",
      ),
    ).toBeVisible();
    await expect(page.getByTestId("admin-app-row-slack")).toContainText("Slack");
    await expect(
      page.getByTestId("admin-app-row-slack").getByTestId("app-mark"),
    ).toBeVisible();
    await expect(page.getByTestId("admin-app-row-slack")).toContainText("eng");
    await expect(page.getByTestId("admin-app-row-slack")).not.toContainText(
      "1 group",
    );
    await expect(page.getByTestId("admin-app-row-slack")).not.toContainText(
      "Specific people",
    );
    await expect(
      page.getByTestId("admin-app-row-slack").getByTestId("admin-access-status"),
    ).toBeVisible();
    await expect(
      page
        .getByTestId("admin-app-row-slack")
        .locator('[data-slot="avatar-group"]'),
    ).toBeVisible();
    await expect(page.getByTestId("admin-app-row-httpbin")).toContainText("No one");
    await expect(
      page.getByTestId("admin-app-row-httpbin").getByTestId("admin-access-status"),
    ).toHaveCount(0);

    await page.getByRole("searchbox", { name: "Search apps" }).fill("sla");
    await expect(page.getByTestId("admin-app-row-slack").getByRole("mark")).toHaveText(
      "Sla",
    );
    await expect(page.getByTestId("admin-app-row-httpbin")).toHaveCount(0);
  });

  test("opens an app, adds a group, then a person, and removes access", async ({
    authenticatedPage: page,
  }) => {
    const state: AdminState = {
      defaultRole: "",
      membersByApp: { slack: [] },
    };
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockIntegrations(page, [app("slack", "Slack")]);
    await wireAdminAccess(page, state);

    await page.goto("/admin");
    await page.getByTestId("admin-app-row-slack").click();
    await expect(
      page.getByRole("heading", { name: "Who can use Slack" }),
    ).toBeVisible();
    await expect(
      page.getByRole("radio", { name: /^No one/ }),
    ).toBeChecked();
    await page.getByTestId("admin-access-choice-specific").click();
    await expect(page.getByRole("heading", { name: "Groups" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "People" })).toBeVisible();
    await expect(page.getByText("No groups yet.")).toBeVisible();
    await expect(page.getByText("No individual people.")).toBeVisible();

    await page.getByRole("button", { name: "Add group" }).click();
    await page.getByRole("dialog").getByLabel("Group").fill("eng");
    await page.getByRole("dialog").getByRole("button", { name: "Add group" }).click();
    await expect(page.getByRole("dialog")).toHaveCount(0);
    await expect(
      page.getByTestId("admin-access-entry").filter({ hasText: "eng" }),
    ).toBeVisible();
    await expect(page.getByText("Slack is on for eng.")).toBeVisible();

    await page.getByRole("button", { name: "Add person" }).click();
    await page.getByRole("dialog").getByLabel("Email").fill("contractor@example.com");
    await page.getByRole("dialog").getByRole("button", { name: "Add person" }).click();
    await expect(
      page.getByTestId("admin-access-entry").filter({ hasText: "contractor@example.com" }),
    ).toBeVisible();

    await page
      .getByTestId("admin-access-entry")
      .filter({ hasText: "contractor@example.com" })
      .getByRole("button", { name: "Remove access" })
      .click();
    await expect(
      page.getByTestId("admin-access-entry").filter({ hasText: "contractor@example.com" }),
    ).toHaveCount(0);
  });

  test("locks config-backed people and Everyone from workspace config", async ({
    authenticatedPage: page,
  }) => {
    const state: AdminState = {
      defaultRole: "viewer",
      membersByApp: {
        slack: [
          member({
            role: "admin",
            source: "static",
            mutable: false,
            selectorKind: "subject_id",
            selectorValue: "user:seed@gestalt.dev",
            email: "seed@gestalt.dev",
            subjectId: "user:seed@gestalt.dev",
          }),
        ],
      },
    };
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockIntegrations(page, [app("slack", "Slack")]);
    await wireAdminAccess(page, state);

    await page.goto("/admin");
    await expect(page.getByTestId("admin-app-row-slack")).toContainText("Everyone");
    await page.getByTestId("admin-app-row-slack").click();
    await expect(
      page.getByRole("radio", { name: /^Everyone in the workspace/ }),
    ).toBeChecked();
    await expect(
      page.getByTestId("admin-access-choice-specific").locator(
        '[data-slot="radio-group-item"]',
      ),
    ).toBeDisabled();
    await expect(
      page.getByTestId("admin-access-choice-no_one").locator(
        '[data-slot="radio-group-item"]',
      ),
    ).toBeDisabled();
    await expect(page.getByRole("button", { name: "Add group" })).toHaveCount(0);
  });

  test("keeps config-locked rows from being removed", async ({
    authenticatedPage: page,
  }) => {
    const state: AdminState = {
      defaultRole: "",
      membersByApp: {
        slack: [
          member({
            role: "viewer",
            source: "static",
            mutable: false,
            selectorKind: "subject_set",
            selectorValue: "group:eng#member",
          }),
        ],
      },
    };
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockIntegrations(page, [app("slack", "Slack")]);
    await wireAdminAccess(page, state);

    await page.goto("/admin/apps/slack");
    await expect(
      page.getByTestId("admin-access-entry").filter({ hasText: "eng" }),
    ).toBeVisible();
    await expect(
      page.getByTestId("admin-access-entry").filter({ hasText: "eng" }).getByText(
        "Set in workspace config. Can’t change here.",
      ),
    ).toBeVisible();
    await expect(page.getByRole("button", { name: "Remove access" })).toBeDisabled();
  });

  test("keeps Admin section nav in sync with the center column", async ({
    authenticatedPage: page,
  }) => {
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockIntegrations(page, [app("slack", "Slack")]);
    await wireAdminAccess(page, { defaultRole: "", membersByApp: { slack: [] } });
    await page.route(
      (url) => url.pathname === "/admin/api/v1/app-registries",
      async (route, request) => {
        if (request.method() !== "GET") {
          await route.fallback();
          return;
        }
        await new Promise((resolve) => setTimeout(resolve, 1500));
        await route.fulfill({ json: [] });
      },
    );
    await page.route("**/admin/api/v1/platform-admins", async (route, request) => {
      if (request.method() !== "GET") {
        await route.fallback();
        return;
      }
      await route.fulfill({
        json: {
          resource: { type: "gestaltAdmin", id: "gestaltAdmin" },
          role: "admin",
          members: [],
        },
      });
    });

    await page.goto("/admin");
    await expect(page.getByRole("heading", { name: "App access" })).toBeVisible();

    await page.getByTestId("admin-nav-platform-admins").click();
    await expect(
      page.getByRole("heading", { name: "Platform admins" }),
    ).toBeVisible({ timeout: 1000 });
    await expect(page.getByTestId("admin-nav-platform-admins")).toHaveAttribute(
      "data-selected",
      "",
    );
    await expect(page.getByTestId("admin-nav-who-can-use")).not.toHaveAttribute(
      "data-selected",
    );
  });

  test("sends people who are not Gestalt admins back to Apps", async ({
    authenticatedPage: page,
  }) => {
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockIntegrations(page, [
      {
        name: "slack",
        displayName: "Slack",
        description: "Slack",
        managementPath: "/apps/slack/admin",
      },
    ]);
    await mockGestaltAdmin(page, false);

    await page.goto("/admin");
    await expect(page).toHaveURL(/\/apps/);
    await expect(page.getByRole("heading", { name: "Apps" })).toBeVisible();
    await expect(
      page.getByRole("navigation", { name: "Primary" }).getByRole("link", {
        name: "Admin",
      }),
    ).toHaveCount(0);
  });

  test("lists platform admins with config-locked rows", async ({
    authenticatedPage: page,
  }) => {
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockIntegrations(page, [app("slack", "Slack")]);
    await mockGestaltAdmin(page, true);
    await page.route("**/admin/api/v1/platform-admins", async (route, request) => {
      if (request.method() !== "GET") {
        await route.fallback();
        return;
      }
      await route.fulfill({
        json: {
          resource: { type: "gestaltAdmin", id: "gestaltAdmin" },
          role: "admin",
          members: [
            member({
              role: "admin",
              source: "static",
              mutable: false,
              selectorKind: "subject_id",
              selectorValue: "user:seed@gestalt.dev",
              email: "seed@gestalt.dev",
              subjectId: "user:seed@gestalt.dev",
            }),
          ],
        },
      });
    });

    await page.goto("/admin/platform-admins");
    await expect(
      page.getByRole("heading", { name: "Platform admins" }),
    ).toBeVisible();
    await expect(
      page.getByTestId("admin-platform-admin-entry").filter({ hasText: "seed@gestalt.dev" }),
    ).toBeVisible();
    await expect(page.getByRole("button", { name: "Remove access" })).toBeDisabled();
  });

  test("loads App versions and Metrics from Admin", async ({
    authenticatedPage: page,
  }) => {
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockIntegrations(page, [app("slack", "Slack")]);
    await mockGestaltAdmin(page, true);
    await page.route("**/admin/api/v1/registry-apps", async (route, request) => {
      if (request.method() !== "GET") {
        await route.fallback();
        return;
      }
      await route.fulfill({ json: [] });
    });
    await page.route("**/admin/api/v1/metrics", async (route, request) => {
      if (request.method() !== "GET") {
        await route.fallback();
        return;
      }
      await route.fulfill({
        contentType: "text/plain",
        body: "# HELP gestaltd_up 1\ngestaltd_up 1\n",
      });
    });

    await page.goto("/admin/versions");
    await expect(page.getByRole("heading", { name: "App versions" })).toBeVisible();
    await expect(page.getByRole("searchbox", { name: "Search apps" })).toBeVisible();
    await expect(page.getByText("No registry apps")).toBeVisible();

    await page.getByTestId("admin-nav-metrics").click();
    await expect(page.getByRole("heading", { name: "Metrics", exact: true })).toBeVisible();
    await expect(page.getByText("since it started")).toBeVisible();
    await expect(page.getByText("gestaltd_up 1")).toBeVisible();
  });

  test("filters App versions and highlights the matching app name", async ({
    authenticatedPage: page,
  }) => {
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockIntegrations(page, [app("slack", "Slack")]);
    await mockGestaltAdmin(page, true);
    await mockRegistryApps(page, [
      registryApp("example-viewer"),
      registryApp("example-tracker"),
    ]);

    await page.goto("/admin/versions");
    await expect(
      page.getByTestId("admin-versions-row-example-viewer"),
    ).toBeVisible();
    await expect(
      page.getByTestId("admin-versions-row-example-tracker"),
    ).toBeVisible();

    await page.getByRole("searchbox", { name: "Search apps" }).fill("viewer");
    await expect(
      page
        .getByTestId("admin-versions-row-example-viewer")
        .getByRole("link", { name: /example-viewer/ })
        .getByRole("mark"),
    ).toHaveText("viewer");
    await expect(
      page.getByTestId("admin-versions-row-example-tracker"),
    ).toHaveCount(0);
  });

  test("opens an app from the versions row", async ({
    authenticatedPage: page,
  }) => {
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockIntegrations(page, [app("slack", "Slack")]);
    await mockGestaltAdmin(page, true);
    await mockRegistryApps(page, [
      registryApp("example-viewer"),
      registryApp("example-tracker"),
    ]);

    await page.goto("/admin/versions");
    const row = page.getByTestId("admin-versions-row-example-viewer");
    await expect(row).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "App" })).toBeVisible();

    await row.getByRole("link", { name: "example-viewer" }).click();
    await expect(page).toHaveURL(/\/admin\/versions\/example-viewer$/);
    await expect(
      page.getByRole("heading", { name: "example-viewer" }),
    ).toBeVisible();
  });
});
