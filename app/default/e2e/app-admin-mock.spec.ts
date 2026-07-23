import {
  expect,
  mockAppAdminRegistry,
  mockAuthSession,
  mockIntegrations,
  test,
} from "./fixtures";
import type { AppAdminRegistryResponse, Integration } from "../src/lib/api";

const APP = "g-issues";

const PUBLISHED_NEW: AppAdminRegistryResponse["publishedVersions"][number] = {
  version: "0.0.0-snapshot.gdef456",
  publishedAt: "2026-07-22T15:00:00Z",
  platforms: ["linux/amd64"],
  sourceRef: "def456def456def456def456def456def456def4",
  sourceUrl:
    "https://github.com/valon-technologies/valon-tools/commit/def456def456def456def456def456def456def4",
  publication: {
    workflowRunUrl:
      "https://github.com/valon-technologies/valon-tools/actions/runs/123456789",
    triggerPullRequest: {
      number: 3251,
      url: "https://github.com/valon-technologies/valon-tools/pull/3251",
    },
  },
};

const PUBLISHED_LEGACY: AppAdminRegistryResponse["publishedVersions"][number] = {
  version: "0.0.0-snapshot.gabc123",
  publishedAt: "2026-07-21T12:00:00Z",
  platforms: ["linux/amd64"],
  sourceRef: "abc123abc123abc123abc123abc123abc123ab",
  sourceUrl:
    "https://github.com/valon-technologies/valon-tools/commit/abc123abc123abc123abc123abc123abc123ab",
};

const MANAGED_INTEGRATION: Integration = {
  name: APP,
  displayName: "G Issues",
  managementPath: `/apps/${APP}/admin`,
};

const UNMANAGED_INTEGRATION: Integration = {
  name: "slack",
  displayName: "Slack",
};

function installedRegistryState(): AppAdminRegistryResponse {
  return {
    app: APP,
    registry: "toolshed",
    desiredVersion: PUBLISHED_LEGACY.version,
    knownVersions: [
      {
        version: PUBLISHED_LEGACY.version,
        installedAt: "2026-07-21T13:00:00Z",
        installedBy: "user:alice",
      },
    ],
    publishedVersions: [PUBLISHED_NEW, PUBLISHED_LEGACY],
    rollout: {
      version: PUBLISHED_LEGACY.version,
      state: "complete",
    },
    selectionDisabled: false,
  };
}

const APP_ADMIN_FIXED_TIME = new Date("2026-07-23T15:00:00Z");

async function expectVersionSelectShows(
  page: import("@playwright/test").Page,
  version: string,
) {
  await expect(page.getByTestId("version-select")).toContainText(version);
}

async function selectPublishedVersion(
  page: import("@playwright/test").Page,
  version: string,
) {
  await page.getByTestId("version-select").click();
  const listbox = page.getByRole("listbox");
  await expect(listbox).toBeVisible();
  await listbox.getByRole("option", { name: new RegExp(version) }).click();
}

async function openVersionSelect(page: import("@playwright/test").Page) {
  await page.getByTestId("version-select").click();
  await expect(page.getByRole("listbox")).toBeVisible();
}

test.describe("app admin registry UI", () => {
  test.beforeEach(async ({ page }) => {
    await page.clock.install({ time: APP_ADMIN_FIXED_TIME });
    await page.addInitScript(() => {
      localStorage.setItem(
        "gestalt.auth.session",
        JSON.stringify({
          subjectId: "user:test@gestalt.dev",
          email: "test@gestalt.dev",
        }),
      );
    });
    await mockAuthSession(page);
  });

  test("shows Manage app only when managementPath is returned", async ({ page }) => {
    await mockIntegrations(page, [MANAGED_INTEGRATION, UNMANAGED_INTEGRATION]);
    await page.goto("/apps");

    await expect(page.getByTestId(`manage-app-${APP}`)).toBeVisible();
    await expect(page.getByTestId("manage-app-slack")).toHaveCount(0);
  });

  test("renders published versions newest first with desired version selected", async ({
    page,
  }) => {
    await mockAppAdminRegistry(page, APP, installedRegistryState());
    await page.goto(`/apps/${APP}/admin`);

    await expectVersionSelectShows(page, PUBLISHED_LEGACY.version);

    await openVersionSelect(page);
    const options = page.getByRole("listbox").getByRole("option");
    await expect(options).toHaveCount(2);
    await expect(options.nth(0)).toContainText(PUBLISHED_NEW.version);
    await expect(options.nth(0)).toContainText("PR #3251");
    await expect(options.nth(0)).toContainText("yesterday");
    await expect(options.nth(1)).toContainText(PUBLISHED_LEGACY.version);
    await expect(options.nth(1)).toContainText("2 days ago");
  });

  test("sorts published versions newest first even when API returns older first", async ({
    page,
  }) => {
    await mockAppAdminRegistry(page, APP, {
      ...installedRegistryState(),
      publishedVersions: [PUBLISHED_LEGACY, PUBLISHED_NEW],
    });
    await page.goto(`/apps/${APP}/admin`);

    await openVersionSelect(page);
    const options = page.getByRole("listbox").getByRole("option");
    await expect(options.nth(0)).toContainText(PUBLISHED_NEW.version);
    await expect(options.nth(1)).toContainText(PUBLISHED_LEGACY.version);
  });

  test("shows publication details and legacy not recorded fields", async ({ page }) => {
    await mockAppAdminRegistry(page, APP, installedRegistryState());
    await page.goto(`/apps/${APP}/admin`);

    await selectPublishedVersion(page, PUBLISHED_NEW.version);
    await expect(page.getByTestId("published-version-summary")).toContainText(
      "Published yesterday",
    );
    await expect(page.getByRole("link", { name: "Commit def456d" })).toBeVisible();
    await expect(page.getByRole("link", { name: "PR #3251" })).toBeVisible();
    await expect(page.getByRole("link", { name: "workflow run" })).toBeVisible();

    await selectPublishedVersion(page, PUBLISHED_LEGACY.version);
    await expect(page.getByTestId("published-version-pr")).toHaveText(
      "PR: not recorded",
    );
    await expect(page.getByTestId("published-version-workflow")).toHaveText(
      "workflow: not recorded",
    );
    await expect(page.getByRole("link", { name: "Commit abc123a" })).toBeVisible();
  });

  test("renders first-install copy when no desired version exists", async ({ page }) => {
    await mockAppAdminRegistry(page, APP, {
      app: APP,
      registry: "toolshed",
      knownVersions: [],
      publishedVersions: [PUBLISHED_NEW],
      selectionDisabled: false,
    });
    await page.goto(`/apps/${APP}/admin`);

    await expect(page.getByText(/No version is installed yet/i)).toBeVisible();
    await expectVersionSelectShows(page, PUBLISHED_NEW.version);
  });

  test("disables selector and submit during active rollout", async ({ page }) => {
    await mockAppAdminRegistry(page, APP, {
      ...installedRegistryState(),
      rollout: {
        version: PUBLISHED_NEW.version,
        state: "enrolling",
      },
      selectionDisabled: true,
      disabledReason: "rollout in progress",
    });
    await page.goto(`/apps/${APP}/admin`);

    await expect(page.getByTestId("rollout-active-banner")).toContainText(
      "Rollout enrolling",
    );
    await expect(page.getByTestId("version-select")).toBeDisabled();
    await expect(page.getByTestId("select-version-button")).toBeDisabled();
    await expect(page.getByTestId("selection-disabled-reason")).toHaveText(
      "rollout in progress",
    );
  });

  test("successful selection shows the new active rollout", async ({ page }) => {
    await mockAppAdminRegistry(page, APP, installedRegistryState(), {
      onSelectVersion: (version) => ({
        app: APP,
        registry: "toolshed",
        fromVersion: PUBLISHED_LEGACY.version,
        desiredVersion: version,
        rollout: {
          version,
          state: "enrolling",
        },
      }),
    });
    await page.goto(`/apps/${APP}/admin`);

    await selectPublishedVersion(page, PUBLISHED_NEW.version);
    await page.getByTestId("select-version-button").click();

    await expect(page.getByTestId("rollout-active-banner")).toContainText(
      PUBLISHED_NEW.version,
    );
    await expect(page.getByTestId("version-select")).toBeDisabled();
    await expect(page.getByTestId("rollout-badge")).toHaveText("enrolling");
  });

  test("409 after stale page disables controls after refresh", async ({ page }) => {
    await mockAppAdminRegistry(page, APP, installedRegistryState(), {
      onSelectVersion: (version, currentState) => ({
        status: 409,
        json: { error: "rollout already active" },
        nextState: {
          ...currentState,
          selectionDisabled: true,
          disabledReason: "rollout in progress",
          rollout: {
            version,
            state: "enrolling",
          },
        },
      }),
    });
    await page.goto(`/apps/${APP}/admin`);

    await selectPublishedVersion(page, PUBLISHED_NEW.version);
    await page.getByTestId("select-version-button").click();

    await expect(page.getByTestId("version-select")).toBeDisabled();
    await expect(page.getByTestId("select-version-button")).toBeDisabled();
  });

  test("403 renders access denied without registry metadata", async ({ page }) => {
    await page.route(`**/api/v1/apps/${APP}/admin/registry`, (route) => {
      route.fulfill({ status: 403, json: { error: "app access denied" } });
    });
    await page.goto(`/apps/${APP}/admin`);

    await expect(page.getByTestId("app-admin-access-denied")).toBeVisible();
    await expect(page.getByText("Access denied")).toBeVisible();
    await expect(page.getByTestId("version-select")).toHaveCount(0);
    await expect(page.getByText("toolshed")).toHaveCount(0);
  });
});
