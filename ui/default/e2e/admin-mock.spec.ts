import { expect, test } from "./fixtures";

type AdminAuthorizationPlugin = {
  name: string;
  authorizationPolicy: string;
  mountedUiPath?: string;
};

type AdminMemberRow = {
  plugin?: string;
  role: string;
  source: "static" | "dynamic";
  effective: boolean;
  mutable: boolean;
  selectorKind: string;
  selectorValue: string;
  email?: string;
  shadowedBy?: string;
};

type AdminState = {
  plugins: AdminAuthorizationPlugin[];
  membersByPlugin: Record<string, AdminMemberRow[]>;
  adminMembers: AdminMemberRow[];
};

function metricsFixture(): string {
  return [
    '# HELP gestaltd_operation_count_total Total Gestalt operations.',
    '# TYPE gestaltd_operation_count_total counter',
    'gestaltd_operation_count_total{gestalt_provider="github"} 90',
    'gestaltd_operation_count_total{gestalt_provider="slack"} 30',
    '# HELP gestaltd_operation_error_count_total Total failed Gestalt operations.',
    '# TYPE gestaltd_operation_error_count_total counter',
    'gestaltd_operation_error_count_total{gestalt_provider="github"} 3',
    'gestaltd_operation_error_count_total{gestalt_provider="slack"} 1',
    '# HELP gestaltd_operation_duration_seconds Gestalt operation latency.',
    '# TYPE gestaltd_operation_duration_seconds histogram',
    'gestaltd_operation_duration_seconds_bucket{le="0.05"} 30',
    'gestaltd_operation_duration_seconds_bucket{le="0.1"} 60',
    'gestaltd_operation_duration_seconds_bucket{le="0.25"} 90',
    'gestaltd_operation_duration_seconds_bucket{le="0.5"} 110',
    'gestaltd_operation_duration_seconds_bucket{le="1"} 120',
    'gestaltd_operation_duration_seconds_bucket{le="+Inf"} 120',
    'gestaltd_operation_duration_seconds_sum 21',
    'gestaltd_operation_duration_seconds_count 120',
  ].join("\n");
}

async function wireAdminRoutes(
  page: import("@playwright/test").Page,
  state: AdminState,
) {
  await page.route("**/metrics", async (route, request) => {
    if (request.method() !== "GET") {
      await route.fallback();
      return;
    }
    await route.fulfill({
      contentType: "text/plain; version=0.0.4",
      body: metricsFixture(),
    });
  });

  await page.route("**/admin/api/v1/authorization/plugins", async (route, request) => {
    if (request.method() !== "GET") {
      await route.fallback();
      return;
    }
    await route.fulfill({ json: state.plugins });
  });

  await page.route("**/admin/api/v1/authorization/plugins/**", async (route, request) => {
    const url = new URL(request.url());
    const parts = url.pathname.split("/").filter(Boolean);
    const plugin = decodeURIComponent(parts[5] || "");
    if (!plugin) {
      await route.fulfill({ status: 400, json: { error: "plugin is required" } });
      return;
    }

    if (parts.length === 7 && parts[6] === "members" && request.method() === "GET") {
      await route.fulfill({ json: state.membersByPlugin[plugin] || [] });
      return;
    }

    if (parts.length === 7 && parts[6] === "members" && request.method() === "PUT") {
      const body = (request.postDataJSON() as { email?: string; role?: string }) ?? {};
      const email = (body.email || "").trim();
      const role = (body.role || "").trim();
      const row: AdminMemberRow = {
        plugin,
        role,
        source: "dynamic",
        effective: true,
        mutable: true,
        selectorKind: "subject_id",
        selectorValue: `user:${email}`,
        email,
      };
      const rows = (state.membersByPlugin[plugin] || []).filter((item) => item.email !== email);
      state.membersByPlugin[plugin] = [...rows, row];
      await route.fulfill({
        json: {
          status: "ok",
          persisted: true,
          reloaded: true,
          membership: row,
        },
      });
      return;
    }

    if (parts.length === 8 && parts[6] === "members" && request.method() === "DELETE") {
      const subjectID = decodeURIComponent(parts[7] || "");
      state.membersByPlugin[plugin] =
        (state.membersByPlugin[plugin] || []).filter((item) => item.selectorValue !== subjectID);
      await route.fulfill({
        json: {
          status: "deleted",
          persisted: true,
          reloaded: true,
        },
      });
      return;
    }

    await route.fallback();
  });

  await page.route("**/admin/api/v1/authorization/admins/members", async (route, request) => {
    if (request.method() === "GET") {
      await route.fulfill({
        headers: { "X-Gestalt-Can-Write": "true" },
        json: state.adminMembers,
      });
      return;
    }

    if (request.method() === "PUT") {
      const body = (request.postDataJSON() as { email?: string; role?: string }) ?? {};
      const email = (body.email || "").trim();
      const role = (body.role || "").trim();
      const row: AdminMemberRow = {
        role,
        source: "dynamic",
        effective: true,
        mutable: true,
        selectorKind: "subject_id",
        selectorValue: `user:${email}`,
        email,
      };
      state.adminMembers = [...state.adminMembers.filter((item) => item.email !== email), row];
      await route.fulfill({
        json: {
          status: "ok",
          persisted: true,
          reloaded: true,
          membership: row,
        },
      });
      return;
    }

    await route.fallback();
  });

  await page.route("**/admin/api/v1/authorization/admins/members/**", async (route, request) => {
    if (request.method() !== "DELETE") {
      await route.fallback();
      return;
    }
    const url = new URL(request.url());
    const parts = url.pathname.split("/").filter(Boolean);
    const subjectID = decodeURIComponent(parts[6] || "");
    state.adminMembers = state.adminMembers.filter((item) => item.selectorValue !== subjectID);
    await route.fulfill({
      json: {
        status: "deleted",
        persisted: true,
        reloaded: true,
      },
    });
  });
}

function baseState(): AdminState {
  return {
    plugins: [
      {
        name: "sample_plugin",
        authorizationPolicy: "workspace",
        mountedUiPath: "/sample",
      },
    ],
    membersByPlugin: {
      sample_plugin: [
        {
          plugin: "sample_plugin",
          role: "admin",
          source: "static",
          effective: true,
          mutable: false,
          selectorKind: "subject_id",
          selectorValue: "user:seed@gestalt.dev",
          email: "seed@gestalt.dev",
        },
      ],
    },
    adminMembers: [
      {
        role: "admin",
        source: "static",
        effective: true,
        mutable: false,
        selectorKind: "subject_id",
        selectorValue: "user:admin@gestalt.dev",
        email: "admin@gestalt.dev",
      },
      {
        role: "admin",
        source: "dynamic",
        effective: true,
        mutable: true,
        selectorKind: "subject_id",
        selectorValue: "user:removable-admin@gestalt.dev",
        email: "removable-admin@gestalt.dev",
      },
    ],
  };
}

test.describe("Admin Shell", () => {
  test("loads admin metrics and authorization workspaces", async ({ page }) => {
    const state = baseState();
    await wireAdminRoutes(page, state);

    await page.goto("/admin/index.html");

    await expect(page.getByRole("heading", { name: "Control surface" })).toBeVisible();
    await expect(page.locator("#summary-requests")).toHaveText("120");
    await expect(page.locator("#summary-errors")).toHaveText("4");

    await page.getByRole("button", { name: "Authorization" }).click();
    await expect(page.getByRole("heading", { name: "Plugin members" })).toBeVisible();
    await expect(page.locator("#authorization-plugin-select")).toHaveValue("sample_plugin");
    await expect(page.locator("#authorization-members-body")).toContainText("seed@gestalt.dev");

    await page.getByRole("button", { name: "Admins" }).click();
    await expect(page.getByRole("heading", { name: "Built-in admin members" })).toBeVisible();
    await expect(page.locator("#admin-members-body")).toContainText("admin@gestalt.dev");
  });

  test("creates and removes plugin and built-in admin grants through the admin APIs", async ({
    page,
  }) => {
    const state = baseState();
    await wireAdminRoutes(page, state);

    await page.goto("/admin/index.html");

    await page.getByRole("button", { name: "Authorization" }).click();
    await page.locator("#authorization-email-input").fill("writer@gestalt.dev");
    await page.locator("#authorization-role-input").fill("writer");
    await page.locator("#authorization-submit-button").click();
    await expect(page.locator("#authorization-members-body")).toContainText("writer@gestalt.dev");

    await page
      .locator('#authorization-members-body button[data-subject-id="user:writer@gestalt.dev"]')
      .click();
    await expect(page.locator("#authorization-members-body")).not.toContainText("writer@gestalt.dev");

    await page.getByRole("button", { name: "Admins" }).click();
    await expect(page.locator("#admin-members-body")).toContainText("removable-admin@gestalt.dev");
    await page
      .locator('#admin-members-body button[data-subject-id="user:removable-admin@gestalt.dev"]')
      .click();
    await expect(page.locator("#admin-members-body")).not.toContainText("removable-admin@gestalt.dev");
    await page.locator("#admin-members-email-input").fill("ops@gestalt.dev");
    await page.locator("#admin-members-role-input").fill("operator");
    await page.locator("#admin-members-submit-button").click();
    await expect(page.locator("#admin-members-body")).toContainText("ops@gestalt.dev");
  });
});
